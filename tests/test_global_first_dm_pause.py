from __future__ import annotations

import datetime as dt
import os
import unittest
from unittest.mock import patch

os.environ["API_ID"] = "123456"
os.environ["API_HASH"] = "test_hash"
os.environ["BOT_TOKEN"] = "123456:test_token"
os.environ["ADMIN_ID_LIST"] = "123"
os.environ["DB_PATH"] = "/tmp/tgblaster_v122b3_pause_unittest.db"
os.environ["BOT_SESSION_PATH"] = "/tmp/tgblaster_v122b3_pause_bot"
os.environ["MEDIA_DIR"] = "/tmp/tgblaster_v122b3_pause_media"
os.environ["OPENAI_API_KEY"] = ""

from config import conn
from handlers.dm import dm_handlers
from services.ai_dialog_service import create_ai_tables
from services.dm_contact_analytics import create_contact_tables
from services.dm_opt_out import create_opt_out_table
from services.dm_task_queue import (
    account_gate_wait_seconds,
    claim_pending,
    count_all_pending,
    enqueue_pending,
    get_due_pending,
    get_global_first_dm_state,
    is_global_first_dm_paused,
    pause_account,
    pause_all_first_dms,
    release_pending_claim,
    resume_all_first_dms,
)
from utils.database.database import create_dm_tables, create_table


class _FakeClient:
    def __init__(self) -> None:
        self.sent: list[tuple[object, str]] = []

    def is_connected(self) -> bool:
        return True

    async def send_message(self, peer, text: str) -> None:
        self.sent.append((peer, text))

    async def send_file(self, peer, path, caption=None) -> None:
        self.sent.append((peer, caption or ""))


class GlobalFirstDmPauseTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        create_table()
        create_dm_tables()
        create_ai_tables()
        create_contact_tables()
        create_opt_out_table()
        with conn:
            for table in (
                "ai_processed_messages",
                "ai_messages",
                "ai_dialogs",
                "dm_first_dm_claims",
                "dm_completed_contacts",
                "dm_contact_sources",
                "dm_contact_cycles",
                "dm_opt_out_users",
                "dm_pending_sources",
                "dm_pending_queue",
                "dm_account_dispatch",
                "dm_watched_chats",
                "dm_sent_log",
                "dm_tasks",
                "sessions",
            ):
                conn.execute(f"DELETE FROM {table}")
        resume_all_first_dms(123)
        dm_handlers.dm_monitor_clients.clear()
        dm_handlers.dm_monitor_tasks.clear()
        dm_handlers.dm_account_dispatcher_tasks.clear()

    def tearDown(self) -> None:
        resume_all_first_dms(123)
        dm_handlers.dm_monitor_clients.clear()
        dm_handlers.dm_monitor_tasks.clear()
        for task in list(dm_handlers.dm_account_dispatcher_tasks.values()):
            task.cancel()
        dm_handlers.dm_account_dispatcher_tasks.clear()

    def _task(self, task_id: int = 1, account: int = 100) -> None:
        session = f"session-{account}"
        with conn:
            conn.execute(
                "INSERT OR REPLACE INTO sessions(user_id,session_string) VALUES (?,?)",
                (account, session),
            )
            conn.execute(
                """
                INSERT INTO dm_tasks(
                    id,admin_id,user_id,session_string,post_text,photo_url,
                    interval_minutes,is_active,created_at,delay_min,delay_max
                ) VALUES (?,?,?,?,?,NULL,0,1,?,0,0)
                """,
                (
                    task_id,
                    1,
                    account,
                    session,
                    "Привет",
                    dt.datetime.now(dt.timezone.utc).isoformat(),
                ),
            )
            conn.execute(
                "INSERT INTO dm_watched_chats(dm_task_id,chat_id) VALUES (?,?)",
                (task_id, 1000 + task_id),
            )

    def _enqueue(self, task_id: int = 1, account: int = 100, user: int = 500) -> int:
        created, pending_id = enqueue_pending(
            dm_task_id=task_id,
            account_user_id=account,
            target_user_id=user,
            target_access_hash=900000 + user,
            target_username=f"u{user}",
            target_first_name="Test",
            target_last_name="User",
            source_chat_id=1000 + task_id,
            source_chat_title="Source",
            delay_min=0,
            delay_max=0,
        )
        self.assertTrue(created)
        return pending_id

    def test_pause_is_persistent_and_resume_clears_it(self) -> None:
        state = pause_all_first_dms(777)
        self.assertTrue(state.is_paused)
        self.assertEqual(state.paused_by_admin_id, 777)
        self.assertIsNotNone(state.paused_at)
        self.assertTrue(is_global_first_dm_paused())
        self.assertIsNone(account_gate_wait_seconds(100))

        state = resume_all_first_dms(888)
        self.assertFalse(state.is_paused)
        self.assertIsNone(state.paused_at)
        self.assertEqual(get_global_first_dm_state().paused_by_admin_id, 888)

    def test_users_continue_to_queue_while_globally_paused(self) -> None:
        self._task()
        pause_all_first_dms(123)
        pending_id = self._enqueue()
        self.assertEqual(count_all_pending(), 1)
        self.assertIsNone(get_due_pending(100))
        row = conn.execute(
            "SELECT status FROM dm_pending_queue WHERE id=?", (pending_id,)
        ).fetchone()
        self.assertEqual(row, ("pending",))

    async def test_paused_send_does_not_call_telegram_and_keeps_row_pending(self) -> None:
        self._task()
        pending_id = self._enqueue()
        client = _FakeClient()
        dm_handlers.dm_monitor_clients[1] = client
        row = get_due_pending(100)
        self.assertIsNotNone(row)
        pause_all_first_dms(123)

        with patch.object(dm_handlers, "_resolve_pending_target", return_value=object()):
            result = await dm_handlers._send_pending_row(row)

        self.assertEqual(result, "global_paused")
        self.assertEqual(client.sent, [])
        status = conn.execute(
            "SELECT status,claim_token FROM dm_pending_queue WHERE id=?", (pending_id,)
        ).fetchone()
        self.assertEqual(status, ("pending", None))

    def test_claim_can_be_released_without_retry_or_reschedule(self) -> None:
        self._task()
        pending_id = self._enqueue()
        original = conn.execute(
            "SELECT eligible_at,retry_count FROM dm_pending_queue WHERE id=?",
            (pending_id,),
        ).fetchone()
        token = claim_pending(pending_id)
        self.assertIsNotNone(token)
        self.assertTrue(release_pending_claim(pending_id, token, "global_pause"))
        current = conn.execute(
            "SELECT status,eligible_at,retry_count,claim_token FROM dm_pending_queue WHERE id=?",
            (pending_id,),
        ).fetchone()
        self.assertEqual(current, ("pending", original[0], original[1], None))

    def test_global_resume_does_not_clear_account_peerflood_pause(self) -> None:
        self._task()
        pause_account(100, "PeerFlood: ручное возобновление")
        pause_all_first_dms(123)
        resume_all_first_dms(123)
        state = dm_handlers.get_account_dispatch_state(100)
        self.assertTrue(state.is_paused)
        self.assertIn("PeerFlood", state.pause_reason or "")

    async def test_send_works_again_after_global_resume(self) -> None:
        self._task()
        pending_id = self._enqueue(user=501)
        client = _FakeClient()
        dm_handlers.dm_monitor_clients[1] = client
        pause_all_first_dms(123)
        self.assertIsNone(get_due_pending(100))
        resume_all_first_dms(123)
        row = get_due_pending(100)
        self.assertIsNotNone(row)

        with patch.object(dm_handlers, "_resolve_pending_target", return_value=object()):
            result = await dm_handlers._send_pending_row(row)

        self.assertEqual(result, "sent")
        self.assertEqual(len(client.sent), 1)
        status = conn.execute(
            "SELECT status FROM dm_pending_queue WHERE id=?", (pending_id,)
        ).fetchone()
        self.assertEqual(status, ("sent",))


if __name__ == "__main__":
    unittest.main()
