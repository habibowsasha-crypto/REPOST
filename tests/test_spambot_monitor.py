from __future__ import annotations

import datetime as dt
import os
import unittest
from types import SimpleNamespace

os.environ.setdefault("API_ID", "123456")
os.environ.setdefault("API_HASH", "test_hash")
os.environ.setdefault("BOT_TOKEN", "123456:test_token")
os.environ.setdefault("ADMIN_ID_LIST", "123")
os.environ.setdefault("DB_PATH", "/tmp/tgblaster_spambot_monitor.db")
os.environ.setdefault("BOT_SESSION_PATH", "/tmp/tgblaster_spambot_monitor_bot")
os.environ.setdefault("MEDIA_DIR", "/tmp/tgblaster_spambot_monitor_media")
os.environ.setdefault("OPENAI_API_KEY", "")

from config import conn
from services.dm_task_queue import (
    ensure_account_settings,
    get_account_dispatch_state,
    pause_account,
)
from services.spambot_monitor import (
    check_spambot_account,
    get_spambot_monitor_state,
    is_spambot_free_response,
    parse_spambot_restriction_until,
    process_due_spambot_checks,
    set_spambot_auto_resume,
    set_spambot_monitor_enabled,
    trigger_peer_flood_monitor,
)
from utils.database.database import create_dm_tables, create_table


class _FakeConversation:
    def __init__(self, response_text: str) -> None:
        self.response_text = response_text
        self.sent: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def send_message(self, text: str):
        self.sent.append(text)

    async def get_response(self):
        return SimpleNamespace(raw_text=self.response_text)


class _FakeClient:
    def __init__(self, response_text: str) -> None:
        self.conversation_obj = _FakeConversation(response_text)

    def is_connected(self) -> bool:
        return True

    def conversation(self, *_args, **_kwargs):
        return self.conversation_obj


class SpamBotMonitorTests(unittest.IsolatedAsyncioTestCase):
    ACCOUNT = 990001

    def setUp(self) -> None:
        create_table()
        create_dm_tables()
        with conn:
            conn.execute("DELETE FROM dm_spambot_monitor")
            conn.execute("DELETE FROM dm_account_dispatch")
            conn.execute("DELETE FROM dm_pending_queue")
            conn.execute("DELETE FROM dm_unified_leads")
            conn.execute("DELETE FROM dm_tasks")
            conn.execute("DELETE FROM sessions")
            conn.execute(
                "INSERT INTO sessions(user_id,session_string) VALUES (?,?)",
                (self.ACCOUNT, "session"),
            )

    def test_free_response_is_recognized(self) -> None:
        self.assertTrue(
            is_spambot_free_response(
                "Ваш аккаунт свободен от каких-либо ограничений."
            )
        )
        self.assertFalse(is_spambot_free_response("Ваш аккаунт временно ограничен"))

    def test_restriction_timestamp_is_parsed_as_utc(self) -> None:
        parsed = parse_spambot_restriction_until(
            "Ограничения будут автоматически сняты 2 Aug 2026, 07:36 UTC."
        )
        self.assertEqual(
            parsed,
            dt.datetime(2026, 8, 2, 7, 36, tzinfo=dt.timezone.utc),
        )
        russian = parse_spambot_restriction_until(
            "Ограничения будут сняты 12 августа 2027, 10:05 UTC"
        )
        self.assertEqual(
            russian,
            dt.datetime(2027, 8, 12, 10, 5, tzinfo=dt.timezone.utc),
        )

    def test_enable_while_peerflood_paused_queues_check(self) -> None:
        pause_account(self.ACCOUNT, "PeerFlood: ручное возобновление")
        state = set_spambot_monitor_enabled(self.ACCOUNT, True)
        self.assertTrue(state.is_enabled)
        self.assertEqual(state.status, "pending")
        self.assertIsNotNone(state.next_check_at)

    def test_peerflood_trigger_does_nothing_when_disabled(self) -> None:
        self.assertFalse(trigger_peer_flood_monitor(self.ACCOUNT))
        state = get_spambot_monitor_state(self.ACCOUNT)
        self.assertFalse(state.is_enabled)
        self.assertIsNone(state.next_check_at)

    async def test_restricted_response_schedules_exact_time_plus_one_minute(self) -> None:
        pause_account(self.ACCOUNT, "PeerFlood: ручное возобновление")
        set_spambot_monitor_enabled(self.ACCOUNT, True)
        response = (
            "Ваш аккаунт временно ограничен. Ограничения будут автоматически "
            "сняты 2 Aug 2099, 07:36 UTC."
        )
        client = _FakeClient(response)
        result = await check_spambot_account(self.ACCOUNT, lambda _account: client)

        self.assertEqual(result.outcome, "restricted")
        self.assertEqual(client.conversation_obj.sent, ["/start"])
        self.assertEqual(
            dt.datetime.fromisoformat(result.next_check_at),
            dt.datetime(2099, 8, 2, 7, 37, tzinfo=dt.timezone.utc),
        )
        state = get_spambot_monitor_state(self.ACCOUNT)
        self.assertEqual(state.status, "restricted")
        self.assertEqual(
            dt.datetime.fromisoformat(state.restriction_until),
            dt.datetime(2099, 8, 2, 7, 36, tzinfo=dt.timezone.utc),
        )

    async def test_free_response_keeps_first_dms_paused_for_manual_resume(self) -> None:
        pause_account(self.ACCOUNT, "PeerFlood: ручное возобновление")
        set_spambot_monitor_enabled(self.ACCOUNT, True)
        client = _FakeClient("Ваш аккаунт свободен от каких-либо ограничений.")

        result = await check_spambot_account(self.ACCOUNT, lambda _account: client)

        self.assertEqual(result.outcome, "free_detected")
        monitor = get_spambot_monitor_state(self.ACCOUNT)
        self.assertEqual(monitor.status, "free_detected")
        self.assertIsNone(monitor.next_check_at)
        dispatch = get_account_dispatch_state(self.ACCOUNT)
        self.assertTrue(dispatch.is_paused)
        self.assertIn("PeerFlood", dispatch.pause_reason or "")


    async def test_auto_resume_clears_peerflood_pause_after_free(self) -> None:
        pause_account(self.ACCOUNT, "PeerFlood: ручное возобновление")
        set_spambot_monitor_enabled(self.ACCOUNT, True)
        set_spambot_auto_resume(self.ACCOUNT, True)
        client = _FakeClient("Ваш аккаунт свободен от каких-либо ограничений.")

        results = await process_due_spambot_checks(lambda _account: client)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].outcome, "free_detected")
        self.assertTrue(results[0].auto_resumed)
        monitor = get_spambot_monitor_state(self.ACCOUNT)
        self.assertEqual(monitor.status, "idle")
        self.assertTrue(monitor.auto_resume)
        dispatch = get_account_dispatch_state(self.ACCOUNT)
        self.assertFalse(dispatch.is_paused)

    async def test_manual_mode_keeps_pause_after_free(self) -> None:
        pause_account(self.ACCOUNT, "PeerFlood: ручное возобновление")
        set_spambot_monitor_enabled(self.ACCOUNT, True)
        set_spambot_auto_resume(self.ACCOUNT, False)
        client = _FakeClient("Ваш аккаунт свободен от каких-либо ограничений.")

        results = await process_due_spambot_checks(lambda _account: client)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].outcome, "free_detected")
        self.assertFalse(results[0].auto_resumed)
        dispatch = get_account_dispatch_state(self.ACCOUNT)
        self.assertTrue(dispatch.is_paused)
        self.assertIn("PeerFlood", dispatch.pause_reason or "")


    def test_repeated_peerflood_does_not_reset_active_restriction_schedule(self) -> None:
        pause_account(self.ACCOUNT, "PeerFlood: ручное возобновление")
        set_spambot_monitor_enabled(self.ACCOUNT, True)
        future = dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=3)
        with conn:
            conn.execute(
                """
                UPDATE dm_spambot_monitor
                   SET status='restricted', next_check_at=?, restriction_until=?
                 WHERE account_user_id=?
                """,
                (future.isoformat(), future.isoformat(), self.ACCOUNT),
            )

        self.assertFalse(trigger_peer_flood_monitor(self.ACCOUNT))
        state = get_spambot_monitor_state(self.ACCOUNT)
        self.assertEqual(state.status, "restricted")
        self.assertEqual(state.next_check_at, future.isoformat())

    def test_peerflood_after_free_without_confirmed_send_is_suppressed(self) -> None:
        pause_account(self.ACCOUNT, "PeerFlood: ручное возобновление")
        set_spambot_monitor_enabled(self.ACCOUNT, True)
        checked_at = dt.datetime.now(dt.timezone.utc)
        with conn:
            conn.execute(
                """
                UPDATE dm_spambot_monitor
                   SET status='idle', next_check_at=NULL, last_checked_at=?,
                       last_response_text=?, last_error=NULL
                 WHERE account_user_id=?
                """,
                (
                    checked_at.isoformat(),
                    "Ваш аккаунт свободен от каких-либо ограничений.",
                    self.ACCOUNT,
                ),
            )

        self.assertFalse(trigger_peer_flood_monitor(self.ACCOUNT))
        state = get_spambot_monitor_state(self.ACCOUNT)
        self.assertEqual(state.status, "free_detected")
        self.assertIsNone(state.next_check_at)
        self.assertIn("повторный /start не поставлен", state.last_error or "")

    def test_confirmed_legacy_send_rearms_next_peerflood_cycle(self) -> None:
        pause_account(self.ACCOUNT, "PeerFlood: ручное возобновление")
        set_spambot_monitor_enabled(self.ACCOUNT, True)
        checked_at = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=2)
        ensure_account_settings(self.ACCOUNT)
        with conn:
            conn.execute(
                """
                UPDATE dm_spambot_monitor
                   SET status='idle', next_check_at=NULL, last_checked_at=?,
                       last_response_text=?
                 WHERE account_user_id=?
                """,
                (
                    checked_at.isoformat(),
                    "Ваш аккаунт свободен от каких-либо ограничений.",
                    self.ACCOUNT,
                ),
            )
            conn.execute(
                """
                INSERT INTO dm_tasks(
                    admin_id, user_id, session_string, post_text,
                    delay_min, delay_max, is_active, created_at
                ) VALUES (123, ?, 'session', 'x', 1, 1, 1, ?)
                """,
                (self.ACCOUNT, checked_at.isoformat()),
            )
            task_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            conn.execute(
                """
                INSERT INTO dm_pending_queue(
                    dm_task_id, account_user_id, target_user_id, status,
                    enqueued_at, eligible_at, sent_at, updated_at
                ) VALUES (?, ?, 123456789, 'sent', ?, ?, ?, ?)
                """,
                (
                    task_id,
                    self.ACCOUNT,
                    checked_at.isoformat(),
                    checked_at.isoformat(),
                    (checked_at + dt.timedelta(minutes=1)).isoformat(),
                    (checked_at + dt.timedelta(minutes=1)).isoformat(),
                ),
            )

        self.assertTrue(trigger_peer_flood_monitor(self.ACCOUNT))
        state = get_spambot_monitor_state(self.ACCOUNT)
        self.assertEqual(state.status, "pending")
        self.assertIsNotNone(state.next_check_at)

    def test_confirmed_unified_send_rearms_next_peerflood_cycle(self) -> None:
        pause_account(self.ACCOUNT, "PeerFlood: ручное возобновление")
        set_spambot_monitor_enabled(self.ACCOUNT, True)
        checked_at = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=2)
        sent_at = checked_at + dt.timedelta(minutes=1)
        with conn:
            conn.execute(
                """
                UPDATE dm_spambot_monitor
                   SET status='idle', next_check_at=NULL, last_checked_at=?,
                       last_response_text=?
                 WHERE account_user_id=?
                """,
                (
                    checked_at.isoformat(),
                    "Good news, no limits are currently applied to your account. "
                    "You're free as a bird!",
                    self.ACCOUNT,
                ),
            )
            conn.execute(
                """
                INSERT INTO dm_unified_leads(
                    target_user_id, enqueued_at, eligible_at, status,
                    sent_at, sent_by_account_user_id, updated_at
                ) VALUES (987654321, ?, ?, 'sent', ?, ?, ?)
                """,
                (
                    checked_at.isoformat(),
                    checked_at.isoformat(),
                    sent_at.isoformat(),
                    self.ACCOUNT,
                    sent_at.isoformat(),
                ),
            )

        self.assertTrue(trigger_peer_flood_monitor(self.ACCOUNT))
        state = get_spambot_monitor_state(self.ACCOUNT)
        self.assertEqual(state.status, "pending")
        self.assertIsNotNone(state.next_check_at)

    def test_auto_resume_default_is_off(self) -> None:
        state = get_spambot_monitor_state(self.ACCOUNT)
        self.assertFalse(state.auto_resume)
        enabled = set_spambot_monitor_enabled(self.ACCOUNT, True)
        self.assertFalse(enabled.auto_resume)


if __name__ == "__main__":
    unittest.main()
