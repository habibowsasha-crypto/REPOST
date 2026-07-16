from __future__ import annotations

import asyncio
import datetime as dt
import os
import unittest
from unittest.mock import patch

from telethon.errors import FloodWaitError, PeerFloodError, UserPrivacyRestrictedError

os.environ["API_ID"] = "123456"
os.environ["API_HASH"] = "test_hash"
os.environ["BOT_TOKEN"] = "123456:test_token"
os.environ["ADMIN_ID_LIST"] = "123"
os.environ["DB_PATH"] = "/tmp/tgblaster_v118_unittest.db"
os.environ["BOT_SESSION_PATH"] = "/tmp/tgblaster_v118_unittest_bot"
os.environ["MEDIA_DIR"] = "/tmp/tgblaster_v118_unittest_media"
os.environ["OPENAI_API_KEY"] = ""

from config import conn
from handlers.dm import dm_handlers
from services.dm_contact_analytics import (
    create_contact_tables,
    is_contact_in_progress,
    try_claim_first_dm,
)
from services.dm_task_queue import (
    MAX_DELAY_SECONDS,
    account_gate_wait_seconds,
    claim_pending,
    clear_task_pending,
    count_clearable_pending,
    count_pending,
    enqueue_pending,
    get_account_dispatch_state,
    get_due_pending,
    list_pending_page,
    mark_account_send_completed,
    parse_iso,
    pause_account,
    prepare_tasks_for_deletion,
    recover_stale_queue,
    resume_account,
    set_account_cooldown,
    set_account_pacing,
    validate_delay_range,
)
from services.ai_dialog_service import create_ai_tables
from services.dm_opt_out import create_opt_out_table
from utils.database.database import create_dm_tables, create_table


class _FakeClient:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.sent: list[tuple[object, str]] = []

    def is_connected(self) -> bool:
        return True

    async def send_message(self, peer, text: str) -> None:
        if self.error is not None:
            raise self.error
        self.sent.append((peer, text))

    async def send_file(self, peer, path, caption=None) -> None:
        if self.error is not None:
            raise self.error
        self.sent.append((peer, caption or ""))


class _BlockingClient(_FakeClient):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def send_message(self, peer, text: str) -> None:
        self.started.set()
        await self.release.wait()
        self.sent.append((peer, text))


class _PartialFloodClient(_FakeClient):
    async def send_file(self, peer, path, caption=None) -> None:
        self.sent.append((peer, caption or ""))

    async def send_message(self, peer, text: str) -> None:
        raise FloodWaitError(None, 19)


class SafeQueueTests(unittest.IsolatedAsyncioTestCase):
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
        dm_handlers.dm_monitor_clients.clear()
        dm_handlers.dm_monitor_tasks.clear()
        dm_handlers.dm_account_dispatcher_tasks.clear()

    def tearDown(self) -> None:
        dm_handlers.dm_monitor_clients.clear()
        dm_handlers.dm_monitor_tasks.clear()
        for task in list(dm_handlers.dm_account_dispatcher_tasks.values()):
            task.cancel()
        dm_handlers.dm_account_dispatcher_tasks.clear()

    def _task(self, *, task_id: int, account: int, active: int = 1) -> None:
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
                ) VALUES (?,?,?,?,?,NULL,0,?,?,0,0)
                """,
                (task_id, 1, account, session, "Привет", active, dt.datetime.now(dt.timezone.utc).isoformat()),
            )
            conn.execute(
                "INSERT INTO dm_watched_chats(dm_task_id,chat_id) VALUES (?,?)",
                (task_id, 1000 + task_id),
            )

    def _enqueue(self, *, task_id: int, account: int, user: int, chat: int) -> tuple[bool, int]:
        return enqueue_pending(
            dm_task_id=task_id,
            account_user_id=account,
            target_user_id=user,
            target_access_hash=900000 + user,
            target_username=f"u{user}",
            target_first_name="Test",
            target_last_name="User",
            source_chat_id=chat,
            source_chat_title=f"Chat {chat}",
            delay_min=0,
            delay_max=0,
        )

    def test_account_wide_dedupe_keeps_multiple_sources(self) -> None:
        self._task(task_id=1, account=100)
        self._task(task_id=2, account=100)
        first = self._enqueue(task_id=1, account=100, user=500, chat=11)
        second = self._enqueue(task_id=2, account=100, user=500, chat=22)
        self.assertTrue(first[0])
        self.assertFalse(second[0])
        self.assertEqual(first[1], second[1])
        sources = conn.execute(
            "SELECT dm_task_id,source_chat_id FROM dm_pending_sources WHERE pending_id=? ORDER BY dm_task_id,source_chat_id",
            (first[1],),
        ).fetchall()
        self.assertEqual(sources, [(1, 11), (2, 22)])

    def test_due_query_does_not_scan_future_row_first(self) -> None:
        self._task(task_id=1, account=101)
        _, future_id = self._enqueue(task_id=1, account=101, user=501, chat=11)
        _, ready_id = self._enqueue(task_id=1, account=101, user=502, chat=11)
        future = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)).isoformat()
        with conn:
            conn.execute("UPDATE dm_pending_queue SET eligible_at=? WHERE id=?", (future, future_id))
        row = get_due_pending(101)
        self.assertIsNotNone(row)
        self.assertEqual(row["id"], ready_id)

    def test_claim_is_atomic(self) -> None:
        self._task(task_id=1, account=102)
        _, row_id = self._enqueue(task_id=1, account=102, user=503, chat=11)
        token = claim_pending(row_id)
        self.assertTrue(token)
        self.assertIsNone(claim_pending(row_id))

    def test_stale_claim_recovery_is_conservative(self) -> None:
        self._task(task_id=1, account=103)
        _, claimed_id = self._enqueue(task_id=1, account=103, user=504, chat=11)
        _, sending_id = self._enqueue(task_id=1, account=103, user=505, chat=11)
        old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=2)).isoformat()
        with conn:
            conn.execute(
                "UPDATE dm_pending_queue SET status='claimed',claimed_at=?,updated_at=? WHERE id=?",
                (old, old, claimed_id),
            )
            conn.execute(
                "UPDATE dm_pending_queue SET status='sending',send_started_at=?,updated_at=? WHERE id=?",
                (old, old, sending_id),
            )
        result = recover_stale_queue()
        self.assertEqual(result, {"claimed_recovered": 1, "sending_uncertain": 1})
        statuses = dict(conn.execute("SELECT id,status FROM dm_pending_queue"))
        self.assertEqual(statuses[claimed_id], "pending")
        self.assertEqual(statuses[sending_id], "uncertain_delivery")

    def test_delay_and_pacing_have_safe_bounds(self) -> None:
        self.assertEqual(validate_delay_range(0, MAX_DELAY_SECONDS), (0, MAX_DELAY_SECONDS))
        with self.assertRaises(ValueError):
            validate_delay_range(0, MAX_DELAY_SECONDS + 1)
        with self.assertRaises(ValueError):
            set_account_pacing(104, 0, 10)
        set_account_pacing(104, 10, 20)
        state = get_account_dispatch_state(104)
        self.assertEqual((state.pacing_min, state.pacing_max), (10, 20))

    def test_general_clear_preserves_uncertain_delivery_guard(self) -> None:
        self._task(task_id=1, account=105)
        _, pending_id = self._enqueue(task_id=1, account=105, user=506, chat=11)
        _, uncertain_id = self._enqueue(task_id=1, account=105, user=507, chat=11)
        with conn:
            conn.execute(
                "UPDATE dm_pending_queue SET status='uncertain_delivery' WHERE id=?",
                (uncertain_id,),
            )
        self.assertEqual(count_pending(1), 2)
        self.assertEqual(count_clearable_pending(1), 1)
        self.assertEqual(clear_task_pending(1), 1)
        rows = {row["id"]: row["status"] for row in list_pending_page(1, offset=0, limit=10)}
        self.assertEqual(rows, {uncertain_id: "uncertain_delivery"})
        self.assertNotIn(pending_id, rows)

    async def test_successful_send_sets_pacing_and_records_delivery(self) -> None:
        self._task(task_id=1, account=106)
        _, row_id = self._enqueue(task_id=1, account=106, user=508, chat=11)
        fake = _FakeClient()
        dm_handlers.dm_monitor_clients[1] = fake
        set_account_pacing(106, 10, 10)
        row = get_due_pending(106)
        self.assertIsNotNone(row)
        result = await dm_handlers._send_pending_row(row)
        self.assertEqual(result, "sent")
        self.assertEqual(len(fake.sent), 1)
        status = conn.execute("SELECT status FROM dm_pending_queue WHERE id=?", (row_id,)).fetchone()[0]
        self.assertEqual(status, "sent")
        self.assertGreater(account_gate_wait_seconds(106) or 0, 0)
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM dm_contact_cycles WHERE account_user_id=106 AND target_user_id=508").fetchone()[0],
            1,
        )

    async def test_privacy_failure_leaves_no_sending_row(self) -> None:
        self._task(task_id=1, account=107)
        _, row_id = self._enqueue(task_id=1, account=107, user=509, chat=11)
        dm_handlers.dm_monitor_clients[1] = _FakeClient(UserPrivacyRestrictedError(None))
        result = await dm_handlers._send_pending_row(get_due_pending(107))
        self.assertEqual(result, "known_failure")
        status = conn.execute("SELECT status FROM dm_pending_queue WHERE id=?", (row_id,)).fetchone()[0]
        self.assertEqual(status, "cancelled")

    async def test_floodwait_pauses_entire_account(self) -> None:
        self._task(task_id=1, account=108)
        self._task(task_id=2, account=108)
        _, row_id = self._enqueue(task_id=1, account=108, user=510, chat=11)
        dm_handlers.dm_monitor_clients[1] = _FakeClient(FloodWaitError(None, 17))
        result = await dm_handlers._send_pending_row(get_due_pending(108))
        self.assertEqual(result, "flood_wait")
        state = get_account_dispatch_state(108)
        cooldown = parse_iso(state.cooldown_until)
        self.assertIsNotNone(cooldown)
        self.assertGreater((cooldown - dt.datetime.now(dt.timezone.utc)).total_seconds(), 10)
        status = conn.execute("SELECT status FROM dm_pending_queue WHERE id=?", (row_id,)).fetchone()[0]
        self.assertEqual(status, "retry_wait")

    async def test_dispatch_loop_does_not_burst_two_ready_rows(self) -> None:
        self._task(task_id=1, account=115)
        self._task(task_id=2, account=115)
        self._enqueue(task_id=1, account=115, user=518, chat=11)
        self._enqueue(task_id=2, account=115, user=519, chat=22)
        first_client = _FakeClient()
        second_client = _FakeClient()
        dm_handlers.dm_monitor_clients[1] = first_client
        dm_handlers.dm_monitor_clients[2] = second_client
        set_account_pacing(115, 5, 5)
        dispatcher = asyncio.create_task(dm_handlers._account_dispatch_loop(115))
        try:
            for _ in range(100):
                if len(first_client.sent) + len(second_client.sent) >= 1:
                    break
                await asyncio.sleep(0.01)
            self.assertEqual(len(first_client.sent) + len(second_client.sent), 1)
            await asyncio.sleep(0.25)
            self.assertEqual(len(first_client.sent) + len(second_client.sent), 1)
        finally:
            dispatcher.cancel()
            await asyncio.gather(dispatcher, return_exceptions=True)

    async def test_account_pacing_is_shared_by_two_tasks(self) -> None:
        self._task(task_id=1, account=109)
        self._task(task_id=2, account=109)
        self._enqueue(task_id=1, account=109, user=511, chat=11)
        self._enqueue(task_id=2, account=109, user=512, chat=22)
        dm_handlers.dm_monitor_clients[1] = _FakeClient()
        dm_handlers.dm_monitor_clients[2] = _FakeClient()
        set_account_pacing(109, 20, 20)
        first = get_due_pending(109)
        self.assertEqual(await dm_handlers._send_pending_row(first), "sent")
        self.assertGreater(account_gate_wait_seconds(109) or 0, 15)
        remaining = get_due_pending(109)
        self.assertIsNotNone(remaining)
        self.assertNotEqual(remaining["target_user_id"], first["target_user_id"])

    async def test_peerflood_pauses_account_until_manual_resume(self) -> None:
        self._task(task_id=1, account=110)
        self._enqueue(task_id=1, account=110, user=513, chat=11)
        dm_handlers.dm_monitor_clients[1] = _FakeClient(PeerFloodError(None))
        result = await dm_handlers._send_pending_row(get_due_pending(110))
        self.assertEqual(result, "peer_flood")
        state = get_account_dispatch_state(110)
        self.assertTrue(state.is_paused)
        self.assertIn("PeerFlood", state.pause_reason or "")

    async def test_stop_waits_for_inflight_send_instead_of_cancelling_it(self) -> None:
        self._task(task_id=1, account=111)
        self._enqueue(task_id=1, account=111, user=514, chat=11)
        fake = _BlockingClient()
        dm_handlers.dm_monitor_clients[1] = fake
        send_task = asyncio.create_task(dm_handlers._send_pending_row(get_due_pending(111)))
        await asyncio.wait_for(fake.started.wait(), timeout=1)
        stop_task = asyncio.create_task(
            dm_handlers.stop_dm_task_runtime(1, preserve_queue=True)
        )
        await asyncio.sleep(0)
        self.assertFalse(stop_task.done())
        fake.release.set()
        self.assertEqual(await asyncio.wait_for(send_task, timeout=1), "sent")
        self.assertTrue(await asyncio.wait_for(stop_task, timeout=1))
        self.assertEqual(conn.execute("SELECT is_active FROM dm_tasks WHERE id=1").fetchone()[0], 0)
        self.assertEqual(conn.execute("SELECT status FROM dm_pending_queue").fetchone()[0], "sent")

    async def test_unknown_transport_result_is_never_auto_retried(self) -> None:
        self._task(task_id=1, account=112)
        _, row_id = self._enqueue(task_id=1, account=112, user=515, chat=11)
        dm_handlers.dm_monitor_clients[1] = _FakeClient(ConnectionError("connection lost"))
        result = await dm_handlers._send_pending_row(get_due_pending(112))
        self.assertEqual(result, "uncertain")
        status = conn.execute("SELECT status FROM dm_pending_queue WHERE id=?", (row_id,)).fetchone()[0]
        self.assertEqual(status, "uncertain_delivery")
        self.assertIsNone(get_due_pending(112))

    async def test_partial_photo_delivery_is_not_retried(self) -> None:
        self._task(task_id=1, account=113)
        long_text = "x" * 1100
        with conn:
            conn.execute(
                "UPDATE dm_tasks SET post_text=?, photo_url=? WHERE id=1",
                (long_text, "/tmp/photo.jpg"),
            )
        _, row_id = self._enqueue(task_id=1, account=113, user=516, chat=11)
        fake = _PartialFloodClient()
        dm_handlers.dm_monitor_clients[1] = fake
        with patch.object(dm_handlers, "choose_first_dm_text", return_value=long_text):
            result = await dm_handlers._send_pending_row(get_due_pending(113))
        self.assertEqual(result, "uncertain")
        self.assertEqual(len(fake.sent), 1)
        self.assertEqual(
            conn.execute("SELECT status FROM dm_pending_queue WHERE id=?", (row_id,)).fetchone()[0],
            "uncertain_delivery",
        )
        self.assertIsNone(get_due_pending(113))
        self.assertGreater(account_gate_wait_seconds(113) or 0, 0)

    async def test_client_not_ready_is_rescheduled_instead_of_hot_looping(self) -> None:
        self._task(task_id=1, account=116)
        _, row_id = self._enqueue(task_id=1, account=116, user=520, chat=11)
        result = await dm_handlers._send_pending_row(get_due_pending(116))
        self.assertEqual(result, "retry")
        status, eligible_at = conn.execute(
            "SELECT status,eligible_at FROM dm_pending_queue WHERE id=?", (row_id,)
        ).fetchone()
        self.assertEqual(status, "retry_wait")
        self.assertGreater(
            (parse_iso(eligible_at) - dt.datetime.now(dt.timezone.utc)).total_seconds(),
            20,
        )

    async def test_unresolved_peer_uses_backoff_not_busy_retry(self) -> None:
        self._task(task_id=1, account=117)
        _, row_id = enqueue_pending(
            dm_task_id=1, account_user_id=117, target_user_id=521,
            target_access_hash=None, target_username=None, target_first_name="No",
            target_last_name="Peer", source_chat_id=11, source_chat_title="Chat",
            delay_min=0, delay_max=0,
        )
        dm_handlers.dm_monitor_clients[1] = _FakeClient()
        result = await dm_handlers._send_pending_row(get_due_pending(117))
        self.assertEqual(result, "unresolved")
        status, attempts, eligible_at = conn.execute(
            "SELECT status,resolve_attempts,eligible_at FROM dm_pending_queue WHERE id=?",
            (row_id,),
        ).fetchone()
        self.assertEqual(status, "unresolved_peer")
        self.assertEqual(attempts, 1)
        self.assertGreater(
            (parse_iso(eligible_at) - dt.datetime.now(dt.timezone.utc)).total_seconds(),
            45,
        )

    async def test_stopped_task_keeps_pending_row(self) -> None:
        self._task(task_id=1, account=118)
        _, row_id = self._enqueue(task_id=1, account=118, user=522, chat=11)
        row = get_due_pending(118)
        with conn:
            conn.execute("UPDATE dm_tasks SET is_active=0 WHERE id=1")
        self.assertEqual(await dm_handlers._send_pending_row(row), "task_inactive")
        self.assertEqual(
            conn.execute("SELECT status FROM dm_pending_queue WHERE id=?", (row_id,)).fetchone()[0],
            "pending",
        )

    async def test_stop_reassigns_account_deduped_row_to_other_active_task(self) -> None:
        self._task(task_id=1, account=119)
        self._task(task_id=2, account=119)
        _, row_id = self._enqueue(task_id=1, account=119, user=523, chat=11)
        self._enqueue(task_id=2, account=119, user=523, chat=22)
        self.assertTrue(await dm_handlers.stop_dm_task_runtime(1, preserve_queue=True))
        owner, status = conn.execute(
            "SELECT dm_task_id,status FROM dm_pending_queue WHERE id=?", (row_id,)
        ).fetchone()
        self.assertEqual((owner, status), (2, "pending"))
        self.assertEqual(get_due_pending(119)["dm_task_id"], 2)

    async def test_restart_waits_for_inflight_send(self) -> None:
        self._task(task_id=1, account=120)
        self._enqueue(task_id=1, account=120, user=524, chat=11)
        fake = _BlockingClient()
        dm_handlers.dm_monitor_clients[1] = fake
        send_task = asyncio.create_task(dm_handlers._send_pending_row(get_due_pending(120)))
        await asyncio.wait_for(fake.started.wait(), timeout=1)
        with patch.object(dm_handlers, "_launch_monitor"):
            restart_task = asyncio.create_task(dm_handlers.restart_dm_task_runtime(1))
            await asyncio.sleep(0)
            self.assertFalse(restart_task.done())
            fake.release.set()
            self.assertEqual(await asyncio.wait_for(send_task, timeout=1), "sent")
            self.assertTrue(await asyncio.wait_for(restart_task, timeout=1))

    async def test_task_deletion_preserves_uncertain_duplicate_guard(self) -> None:
        self._task(task_id=1, account=121)
        _, row_id = self._enqueue(task_id=1, account=121, user=525, chat=11)
        with conn:
            conn.execute(
                "UPDATE dm_pending_queue SET status='uncertain_delivery' WHERE id=?",
                (row_id,),
            )
        self.assertTrue(await dm_handlers.delete_dm_task_runtime(1))
        status = conn.execute(
            "SELECT status FROM dm_pending_queue WHERE id=?", (row_id,)
        ).fetchone()[0]
        self.assertEqual(status, "uncertain_delivery")
        self.assertFalse(conn.execute("SELECT 1 FROM dm_tasks WHERE id=1").fetchone())

    def test_resume_does_not_bypass_existing_floodwait(self) -> None:
        set_account_cooldown(122, 120, "FloodWait")
        pause_account(122, "PeerFlood")
        before = parse_iso(get_account_dispatch_state(122).cooldown_until)
        resume_account(122)
        state = get_account_dispatch_state(122)
        self.assertFalse(state.is_paused)
        self.assertEqual(parse_iso(state.cooldown_until), before)
        self.assertGreater(account_gate_wait_seconds(122) or 0, 100)

    def test_shorter_floodwait_never_reduces_existing_cooldown(self) -> None:
        long_until = parse_iso(set_account_cooldown(123, 120, "FloodWait"))
        short_until = parse_iso(set_account_cooldown(123, 10, "FloodWait"))
        self.assertEqual(short_until, long_until)

    def test_pacing_change_recalculates_from_last_successful_send(self) -> None:
        set_account_pacing(124, 5, 5)
        mark_account_send_completed(124)
        set_account_pacing(124, 20, 20)
        state = get_account_dispatch_state(124)
        self.assertIsNotNone(state.last_send_at)
        self.assertGreater(account_gate_wait_seconds(124) or 0, 18)

    def test_prepare_delete_reassigns_row_with_alternative_source(self) -> None:
        self._task(task_id=1, account=125, active=1)
        self._task(task_id=2, account=125, active=1)
        _, row_id = self._enqueue(task_id=1, account=125, user=526, chat=11)
        self._enqueue(task_id=2, account=125, user=526, chat=22)
        with conn:
            conn.execute("UPDATE dm_tasks SET is_active=0 WHERE id=1")
        result = prepare_tasks_for_deletion([1])
        self.assertEqual(result["reassigned"], 1)
        self.assertEqual(
            conn.execute("SELECT dm_task_id FROM dm_pending_queue WHERE id=?", (row_id,)).fetchone()[0],
            2,
        )

    def test_recent_sent_log_guards_against_analytics_write_failure(self) -> None:
        self._task(task_id=1, account=114)
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        with conn:
            conn.execute(
                "INSERT INTO dm_sent_log(dm_task_id,target_user_id,sent_at,status) VALUES(1,517,?,'sent')",
                (now,),
            )
        self.assertTrue(is_contact_in_progress(114, 517))
        self.assertIsNone(
            try_claim_first_dm(
                account_user_id=114,
                target_user_id=517,
                dm_task_id=1,
                source_chat_id=11,
            )
        )


if __name__ == "__main__":
    unittest.main()
