from __future__ import annotations

import datetime
import os
import unittest
import warnings
from collections import deque
from types import SimpleNamespace

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=ResourceWarning)

# Self-contained, network-free test configuration. Assign instead of setdefault so
# a developer's real shell variables can never make tests call Telegram/OpenAI.
os.environ["API_ID"] = "123456"
os.environ["API_HASH"] = "test_hash"
os.environ["BOT_TOKEN"] = "123456:test_token"
os.environ["ADMIN_ID_LIST"] = "123"
os.environ["DB_PATH"] = "/tmp/tgblaster_v118_unittest.db"
os.environ["BOT_SESSION_PATH"] = "/tmp/tgblaster_v118_unittest_bot"
os.environ["MEDIA_DIR"] = "/tmp/tgblaster_v118_unittest_media"
os.environ["OPENAI_API_KEY"] = ""
os.environ["AI_DM_ENABLED"] = "true"
os.environ["AI_DM_DRY_RUN"] = "false"
os.environ["AI_REPLY_DELAY_MIN_SECONDS"] = "0"
os.environ["AI_REPLY_DELAY_MAX_SECONDS"] = "0"
os.environ["AI_BURST_DELAY_MIN_SECONDS"] = "0"
os.environ["AI_BURST_DELAY_MAX_SECONDS"] = "0"
os.environ["DM_DIALOG_ABANDON_HOURS"] = "72"
os.environ["DM_DIALOG_POST_LINK_COMPLETE_HOURS"] = "72"

from config import conn
from handlers.dm import dm_handlers
from services.ai_dialog_service import create_ai_tables, handle_private_incoming
from services.dm_contact_analytics import (
    chat_rows,
    chat_stats,
    clear_completed_for_chat,
    create_contact_tables,
    expire_stale_dialogs,
    is_completed_contact,
    is_contact_in_progress,
    mark_completed,
    mark_first_reply,
    mark_link_sent,
    mark_opted_out,
    overall_stats,
    record_first_dm,
    record_source_seen,
    release_first_dm_claim,
    try_claim_first_dm,
)
from services.dm_opt_out import add_opt_out, remove_opt_out
from utils.database.database import create_dm_tables


class DmContactAnalyticsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        create_dm_tables()
        create_ai_tables()
        create_contact_tables()
        with conn:
            conn.execute("DELETE FROM dm_opt_out_users")
            conn.execute("DELETE FROM ai_processed_messages")
            conn.execute("DELETE FROM ai_messages")
            conn.execute("DELETE FROM ai_dialogs")
            conn.execute("DELETE FROM dm_first_dm_claims")
            conn.execute("DELETE FROM dm_completed_contacts")
            conn.execute("DELETE FROM dm_contact_sources")
            conn.execute("DELETE FROM dm_contact_cycles")
            conn.execute("DELETE FROM dm_sent_log")
        dm_handlers.dm_send_queues.clear()
        dm_handlers.dm_source_chat_titles.clear()
        dm_handlers.dm_source_chat_ids.clear()

    def _cycle(
        self,
        *,
        account: int = 100,
        user: int = 500,
        chat: int = 777,
        title: str = "Test Chat",
    ) -> int:
        return record_first_dm(
            dm_task_id=1,
            account_user_id=account,
            target_user_id=user,
            source_chat_id=chat,
            source_chat_title=title,
        )

    async def test_completed_contact_blocks_all_accounts_and_clear_reopens(self) -> None:
        cycle = self._cycle()
        mark_completed(cycle, "natural_finish_after_link")
        self.assertTrue(is_completed_contact(100, 500))
        self.assertTrue(is_completed_contact(101, 500))
        self.assertEqual(clear_completed_for_chat(777), 1)
        self.assertFalse(is_completed_contact(100, 500))

    async def test_active_contact_blocks_other_accounts_until_abandoned(self) -> None:
        cycle = self._cycle(account=100, user=504)
        self.assertTrue(is_contact_in_progress(101, 504))
        self.assertIsNone(
            try_claim_first_dm(
                account_user_id=101,
                target_user_id=504,
                dm_task_id=2,
                source_chat_id=778,
            )
        )
        old = (
            datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(hours=80)
        ).isoformat()
        with conn:
            conn.execute(
                "UPDATE dm_contact_cycles SET last_activity_at=? WHERE id=?",
                (old, cycle),
            )
        self.assertFalse(is_contact_in_progress(101, 504))
        self.assertIsNotNone(
            try_claim_first_dm(
                account_user_id=101,
                target_user_id=504,
                dm_task_id=2,
                source_chat_id=778,
            )
        )

    async def test_post_link_timeout_completes_and_before_link_timeout_abandons(self) -> None:
        old = (
            datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(hours=80)
        ).isoformat()
        post_link = self._cycle(user=501)
        mark_link_sent(post_link)
        before_link = self._cycle(user=502)
        with conn:
            conn.execute(
                "UPDATE dm_contact_cycles SET last_activity_at=? WHERE id IN (?,?)",
                (old, post_link, before_link),
            )
        result = expire_stale_dialogs()
        self.assertEqual(result, {"abandoned": 1, "completed": 1})
        self.assertTrue(is_completed_contact(100, 501))
        self.assertFalse(is_completed_contact(100, 502))
        self.assertFalse(is_contact_in_progress(100, 502))

    async def test_first_reply_preserves_post_link_state(self) -> None:
        cycle = self._cycle(user=503)
        mark_link_sent(cycle)
        mark_first_reply(cycle)
        row = conn.execute(
            "SELECT status, first_reply_at FROM dm_contact_cycles WHERE id=?",
            (cycle,),
        ).fetchone()
        self.assertEqual(row[0], "post_link_active")
        self.assertIsNotNone(row[1])

    async def test_same_user_can_be_counted_in_multiple_source_chats(self) -> None:
        for chat, title in ((800, "Alpha"), (801, "Beta")):
            record_source_seen(
                account_user_id=100,
                target_user_id=600,
                source_chat_id=chat,
                source_chat_title=title,
            )
        self._cycle(user=600, chat=800, title="Alpha")
        rows = {row[0]: row for row in chat_rows()}
        self.assertEqual(rows[800][2:], (1, 1))
        self.assertEqual(rows[801][2:], (0, 1))
        self.assertEqual(chat_stats(801)["seen_recipients"], 1)
        self.assertEqual(chat_stats(801)["first_dms"], 0)

    async def test_overall_optout_is_global_and_not_lost_after_completed_cleanup(self) -> None:
        cycle = self._cycle(user=610)
        mark_completed(cycle, "natural_finish_after_link")
        add_opt_out(610, source_account_user_id=100)
        self.assertEqual(overall_stats()["opted_out"], 1)
        self.assertEqual(clear_completed_for_chat(777), 1)
        self.assertEqual(overall_stats()["opted_out"], 1)
        self.assertTrue(remove_opt_out(610))

    async def test_completed_contact_purges_all_account_live_queues(self) -> None:
        target = SimpleNamespace(id=620)
        dm_handlers.dm_send_queues[1] = deque([(620, target)])
        dm_handlers.dm_send_queues[2] = deque([(620, target)])
        dm_handlers.dm_source_chat_titles[(1, 620)] = "Alpha"
        dm_handlers.dm_source_chat_titles[(2, 620)] = "Beta"

        original_get_task = dm_handlers._get_task
        dm_handlers._get_task = lambda task_id: {
            "user_id": 100 if int(task_id) == 1 else 101
        }
        try:
            cycle = self._cycle(account=100, user=620, chat=800, title="Alpha")
            mark_completed(cycle, "natural_finish_after_link")
        finally:
            dm_handlers._get_task = original_get_task

        self.assertEqual(list(dm_handlers.dm_send_queues[1]), [])
        self.assertEqual(list(dm_handlers.dm_send_queues[2]), [])
        self.assertNotIn((1, 620), dm_handlers.dm_source_chat_titles)
        self.assertNotIn((2, 620), dm_handlers.dm_source_chat_titles)

        # Admin cleanup cannot resurrect an old queued first DM. A new group
        # message must be observed before the account can enqueue the user again.
        self.assertEqual(clear_completed_for_chat(800), 1)
        self.assertEqual(list(dm_handlers.dm_send_queues[1]), [])


    async def test_first_dm_claim_is_global_across_accounts(self) -> None:
        first = try_claim_first_dm(
            account_user_id=100,
            target_user_id=639,
            dm_task_id=1,
            source_chat_id=777,
        )
        self.assertIsNotNone(first)
        second = try_claim_first_dm(
            account_user_id=101,
            target_user_id=639,
            dm_task_id=2,
            source_chat_id=778,
        )
        self.assertIsNone(second)
        self.assertTrue(release_first_dm_claim(100, 639, first))

    async def test_persistent_first_dm_claim_is_atomic_and_consumed(self) -> None:
        token = try_claim_first_dm(
            account_user_id=100,
            target_user_id=640,
            dm_task_id=1,
            source_chat_id=777,
        )
        self.assertIsNotNone(token)
        self.assertIsNone(
            try_claim_first_dm(
                account_user_id=100,
                target_user_id=640,
                dm_task_id=2,
                source_chat_id=778,
            )
        )
        self.assertTrue(release_first_dm_claim(100, 640, token))

        second = try_claim_first_dm(
            account_user_id=100,
            target_user_id=640,
            dm_task_id=1,
            source_chat_id=777,
        )
        self.assertIsNotNone(second)
        cycle = record_first_dm(
            dm_task_id=1,
            account_user_id=100,
            target_user_id=640,
            source_chat_id=777,
            source_chat_title="Test Chat",
            claim_token=second,
        )
        self.assertTrue(is_contact_in_progress(100, 640))
        self.assertEqual(
            conn.execute(
                "SELECT COUNT(*) FROM dm_first_dm_claims "
                "WHERE account_user_id=100 AND target_user_id=640"
            ).fetchone()[0],
            0,
        )
        self.assertGreater(cycle, 0)


    async def test_global_optout_removes_pending_persistent_claims(self) -> None:
        token = try_claim_first_dm(
            account_user_id=100,
            target_user_id=650,
            dm_task_id=1,
            source_chat_id=777,
        )
        self.assertIsNotNone(token)
        add_opt_out(650, source_account_user_id=100)
        count = conn.execute(
            "SELECT COUNT(*) FROM dm_first_dm_claims WHERE target_user_id=650"
        ).fetchone()[0]
        self.assertEqual(count, 0)

    async def test_delivery_status_is_logged_once(self) -> None:
        dm_handlers._safe_log_event(1, 651, "sent")
        count = conn.execute(
            "SELECT COUNT(*) FROM dm_sent_log "
            "WHERE dm_task_id=1 AND target_user_id=651 AND status='sent'"
        ).fetchone()[0]
        self.assertEqual(count, 1)

    async def test_stale_first_dm_claim_is_recoverable(self) -> None:
        old = (
            datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(hours=2)
        ).isoformat()
        with conn:
            conn.execute(
                """
                INSERT INTO dm_first_dm_claims
                    (account_user_id,target_user_id,claim_token,dm_task_id,source_chat_id,claimed_at)
                VALUES (100,641,'stale',1,777,?)
                """,
                (old,),
            )
        token = try_claim_first_dm(
            account_user_id=100,
            target_user_id=641,
            dm_task_id=2,
            source_chat_id=778,
        )
        self.assertIsNotNone(token)
        self.assertNotEqual(token, "stale")

    async def test_recent_orphan_ai_dialog_blocks_duplicate_first_dm(self) -> None:
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with conn:
            conn.execute(
                """
                INSERT INTO ai_dialogs
                    (dm_task_id, account_user_id, target_user_id, stage, status,
                     message_count, created_at, updated_at)
                VALUES (1,100,645,'first_dm_sent','active',0,?,?)
                """,
                (now, now),
            )
        self.assertTrue(is_contact_in_progress(100, 645))
        self.assertIsNone(
            try_claim_first_dm(
                account_user_id=100,
                target_user_id=645,
                dm_task_id=2,
                source_chat_id=778,
            )
        )

    async def test_stale_orphan_ai_dialog_does_not_block_forever(self) -> None:
        old = (
            datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(hours=80)
        ).isoformat()
        with conn:
            conn.execute(
                """
                INSERT INTO ai_dialogs
                    (dm_task_id, account_user_id, target_user_id, stage, status,
                     message_count, created_at, updated_at)
                VALUES (1,100,647,'first_dm_sent','active',0,?,?)
                """,
                (old, old),
            )
        self.assertFalse(is_contact_in_progress(100, 647))
        token = try_claim_first_dm(
            account_user_id=100,
            target_user_id=647,
            dm_task_id=2,
            source_chat_id=778,
        )
        self.assertIsNotNone(token)

    async def test_completion_recovery_without_cycle_still_protects_user(self) -> None:
        mark_completed(
            None,
            "recovery_after_missing_cycle",
            account_user_id=100,
            target_user_id=646,
            source_chat_id=779,
            source_chat_title="Recovery Chat",
        )
        self.assertTrue(is_completed_contact(100, 646))
        row = conn.execute(
            """
            SELECT cycle_id, source_chat_id, completion_reason
            FROM dm_completed_contacts
            WHERE target_user_id=646
            """
        ).fetchone()
        self.assertIsNone(row[0])
        self.assertEqual(row[1], 779)
        self.assertEqual(row[2], "recovery_after_missing_cycle")
        self.assertEqual(clear_completed_for_chat(779), 1)
        self.assertFalse(is_completed_contact(100, 646))

    async def test_late_optout_preserves_historical_completion_counter(self) -> None:
        cycle = self._cycle(user=642)
        mark_link_sent(cycle)
        mark_completed(cycle, "natural_finish_after_link")
        before = overall_stats()
        self.assertEqual(before["completed"], 1)

        mark_opted_out(cycle, "late_explicit_stop")
        row = conn.execute(
            "SELECT status, dialog_completed_at FROM dm_contact_cycles WHERE id=?",
            (cycle,),
        ).fetchone()
        self.assertEqual(row[0], "opted_out")
        self.assertIsNotNone(row[1])
        self.assertEqual(overall_stats()["completed"], 1)
        self.assertTrue(is_completed_contact(100, 642))

    async def test_recent_activity_is_not_expired(self) -> None:
        cycle = self._cycle(user=643)
        mark_link_sent(cycle)
        result = expire_stale_dialogs()
        self.assertEqual(result, {"abandoned": 0, "completed": 0})
        row = conn.execute(
            "SELECT status FROM dm_contact_cycles WHERE id=?", (cycle,)
        ).fetchone()
        self.assertEqual(row[0], "post_link_active")

    async def test_reply_counter_works_when_ai_temporarily_disabled(self) -> None:
        cycle = self._cycle(account=100, user=630)
        sender = SimpleNamespace(id=630, username="u", first_name="U")
        previous = os.environ["AI_DM_ENABLED"]
        os.environ["AI_DM_ENABLED"] = "false"
        try:
            await handle_private_incoming(
                dm_task_id=1,
                account_user_id=100,
                client=SimpleNamespace(),
                sender=sender,
                text="Привет",
                message_id=9001,
            )
        finally:
            os.environ["AI_DM_ENABLED"] = previous
        row = conn.execute(
            "SELECT first_reply_at, status FROM dm_contact_cycles WHERE id=?",
            (cycle,),
        ).fetchone()
        self.assertIsNotNone(row[0])
        self.assertEqual(row[1], "active")


if __name__ == "__main__":
    unittest.main()
