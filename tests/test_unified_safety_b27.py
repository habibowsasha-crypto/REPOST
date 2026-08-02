from __future__ import annotations

import datetime as dt
import os
import unittest
from unittest.mock import patch

os.environ.setdefault("API_ID", "1")
os.environ.setdefault("API_HASH", "h")
os.environ.setdefault("BOT_TOKEN", "1:t")
os.environ.setdefault("ADMIN_ID_LIST", "1")
os.environ.setdefault("DB_PATH", "/tmp/uq_b27_safety.db")
os.environ.setdefault("BOT_SESSION_PATH", "/tmp/uq_b27_safety_bot")
os.environ.setdefault("MEDIA_DIR", "/tmp/uq_b27_safety_media")

from config import conn
from services.dm_unified_queue import (
    MODE_UNIFIED,
    _ensure_pending_row_for_account,
    _iso,
    complete_global_send_window,
    ensure_unified_queue_schema,
    get_queue_runtime_state,
    prepare_unified_send_for_account,
    recover_stale_pending_claims,
    recover_stale_unified_reservations,
    release_global_send_lease,
    renew_global_send_lease,
    release_unified_lead_for_pending,
    set_queue_mode,
    try_claim_global_send_lease,
    utc_now,
)
from utils.database.database import create_dm_tables, create_table


class UnifiedSafetyB27Tests(unittest.TestCase):
    def setUp(self) -> None:
        create_table()
        create_dm_tables()
        ensure_unified_queue_schema()
        with conn:
            for table in (
                "dm_pending_sources",
                "dm_pending_queue",
                "dm_unified_lead_sources",
                "dm_unified_lead_accounts",
                "dm_unified_leads",
                "dm_tasks",
                "dm_account_dispatch",
                "sessions",
            ):
                try:
                    conn.execute(f"DELETE FROM {table}")
                except Exception:
                    pass
            conn.execute(
                "UPDATE dm_queue_runtime SET mode=?, next_global_send_at=NULL, "
                "last_global_send_at=NULL, lease_owner_account_user_id=NULL, "
                "lease_token=NULL, lease_expires_at=NULL WHERE id=1",
                (MODE_UNIFIED,),
            )
        set_queue_mode(MODE_UNIFIED, admin_id=1)

    def _task(self, task_id: int, account_id: int) -> None:
        with conn:
            conn.execute(
                "INSERT OR REPLACE INTO sessions(user_id,session_string) VALUES (?,?)",
                (account_id, f"s-{account_id}"),
            )
            conn.execute(
                """
                INSERT INTO dm_tasks (
                    id, admin_id, user_id, session_string, post_text,
                    interval_minutes, is_active, delay_min, delay_max, first_dm_module
                ) VALUES (?, 1, ?, ?, 'x', 0, 1, 1, 2, 'default')
                """,
                (task_id, account_id, f"s-{account_id}"),
            )
            now = _iso(utc_now())
            conn.execute(
                """
                INSERT OR REPLACE INTO dm_account_dispatch (
                    account_user_id, next_send_at, is_paused, updated_at
                ) VALUES (?, ?, 0, ?)
                """,
                (account_id, now, now),
            )

    def test_runtime_schema_migrates_lease_columns(self) -> None:
        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(dm_queue_runtime)").fetchall()
        }
        self.assertTrue(
            {"lease_owner_account_user_id", "lease_token", "lease_expires_at"}
            <= columns
        )

    def test_stale_owner_cannot_release_newer_lease(self) -> None:
        token_a = try_claim_global_send_lease(100, lease_seconds=30)
        self.assertTrue(token_a)
        past = _iso(utc_now() - dt.timedelta(seconds=1))
        with conn:
            conn.execute(
                "UPDATE dm_queue_runtime SET lease_expires_at=?, next_global_send_at=NULL WHERE id=1",
                (past,),
            )
        token_b = try_claim_global_send_lease(200, lease_seconds=60)
        self.assertTrue(token_b)
        self.assertNotEqual(token_a, token_b)
        self.assertFalse(release_global_send_lease(100, str(token_a), retry_seconds=0))
        state = get_queue_runtime_state()
        self.assertEqual(state.lease_owner_account_user_id, 200)
        self.assertEqual(state.lease_token, token_b)
        self.assertTrue(release_global_send_lease(200, str(token_b), retry_seconds=0))

    def test_only_owner_can_renew_live_lease(self) -> None:
        token = try_claim_global_send_lease(100, lease_seconds=60)
        self.assertTrue(token)
        before = get_queue_runtime_state().lease_expires_at
        self.assertFalse(renew_global_send_lease(200, str(token), lease_seconds=300))
        self.assertEqual(get_queue_runtime_state().lease_expires_at, before)
        self.assertTrue(renew_global_send_lease(100, str(token), lease_seconds=300))
        self.assertGreater(
            dt.datetime.fromisoformat(get_queue_runtime_state().lease_expires_at),
            dt.datetime.fromisoformat(before),
        )
        release_global_send_lease(100, str(token), retry_seconds=0)

    def test_old_owner_cannot_complete_newer_lease(self) -> None:
        token_a = try_claim_global_send_lease(100, lease_seconds=30)
        past = _iso(utc_now() - dt.timedelta(seconds=1))
        with conn:
            conn.execute(
                "UPDATE dm_queue_runtime SET lease_expires_at=?, next_global_send_at=NULL WHERE id=1",
                (past,),
            )
        token_b = try_claim_global_send_lease(200, lease_seconds=60)
        self.assertFalse(complete_global_send_window(100, str(token_a)))
        self.assertEqual(get_queue_runtime_state().lease_token, token_b)
        self.assertTrue(complete_global_send_window(200, str(token_b)))

    def test_uncertain_pending_is_never_reopened(self) -> None:
        self._task(1, 100)
        now = _iso(utc_now())
        with conn:
            conn.execute(
                """
                INSERT INTO dm_pending_queue (
                    id, dm_task_id, account_user_id, target_user_id,
                    enqueued_at, eligible_at, status, retry_count,
                    resolve_attempts, updated_at
                ) VALUES (10,1,100,777,?,?, 'uncertain_delivery',0,0,?)
                """,
                (now, now, now),
            )
            conn.execute(
                """
                INSERT INTO dm_unified_leads (
                    id, target_user_id, preferred_account_user_id,
                    source_account_user_id, dm_task_id, enqueued_at,
                    eligible_at, status, legacy_pending_id, updated_at
                ) VALUES (1,777,100,100,1,?,?,'pending',10,?)
                """,
                (now, now, now),
            )
        lead = {
            "id": 1,
            "target_user_id": 777,
            "preferred_account_user_id": 100,
            "source_account_user_id": 100,
            "target_access_hash": None,
            "target_username": None,
            "target_first_name": "X",
            "target_last_name": None,
            "source_chat_id": None,
            "source_chat_title": None,
            "source_chat_username": None,
            "source_message_id": None,
            "enqueued_at": now,
        }
        with conn:
            pending_id = _ensure_pending_row_for_account(lead, 100, 1)
        self.assertIsNone(pending_id)
        self.assertEqual(
            conn.execute("SELECT status FROM dm_pending_queue WHERE id=10").fetchone()[0],
            "uncertain_delivery",
        )
        self.assertEqual(
            conn.execute("SELECT status FROM dm_unified_leads WHERE id=1").fetchone()[0],
            "uncertain_delivery",
        )

    def test_stale_sending_becomes_uncertain_in_both_queues(self) -> None:
        self._task(1, 100)
        old = _iso(utc_now() - dt.timedelta(minutes=10))
        with conn:
            conn.execute(
                """
                INSERT INTO dm_pending_queue (
                    id, dm_task_id, account_user_id, target_user_id,
                    enqueued_at, eligible_at, status, claim_token,
                    claimed_at, send_started_at, retry_count,
                    resolve_attempts, updated_at
                ) VALUES (10,1,100,777,?,?, 'sending','c',?,?,0,0,?)
                """,
                (old, old, old, old, old),
            )
            conn.execute(
                """
                INSERT INTO dm_unified_leads (
                    id, target_user_id, preferred_account_user_id,
                    source_account_user_id, dm_task_id, enqueued_at,
                    eligible_at, status, reserved_by_account_user_id,
                    reserved_at, reserve_token, legacy_pending_id, updated_at
                ) VALUES (1,777,100,100,1,?,?,'reserved',100,?,'old',10,?)
                """,
                (old, old, old, old),
            )
        self.assertEqual(recover_stale_pending_claims(older_than_seconds=30), 1)
        self.assertEqual(recover_stale_unified_reservations(max_age_seconds=60), 1)
        self.assertEqual(
            conn.execute("SELECT status FROM dm_pending_queue WHERE id=10").fetchone()[0],
            "uncertain_delivery",
        )
        self.assertEqual(
            conn.execute("SELECT status FROM dm_unified_leads WHERE id=1").fetchone()[0],
            "uncertain_delivery",
        )

    def test_active_lease_prevents_live_sending_row_from_stale_recovery(self) -> None:
        self._task(1, 100)
        token = try_claim_global_send_lease(100, lease_seconds=300)
        self.assertTrue(token)
        old = _iso(utc_now() - dt.timedelta(minutes=10))
        with conn:
            conn.execute(
                """
                INSERT INTO dm_pending_queue (
                    id, dm_task_id, account_user_id, target_user_id,
                    enqueued_at, eligible_at, status, claim_token,
                    claimed_at, send_started_at, retry_count,
                    resolve_attempts, updated_at
                ) VALUES (10,1,100,777,?,?, 'sending','c',?,?,0,0,?)
                """,
                (old, old, old, old, old),
            )
            conn.execute(
                """
                INSERT INTO dm_unified_leads (
                    id, target_user_id, enqueued_at, eligible_at, status,
                    reserved_by_account_user_id, reserved_at, reserve_token,
                    legacy_pending_id, updated_at
                ) VALUES (1,777,?,?,'reserved',100,?,?,10,?)
                """,
                (old, old, old, str(token), old),
            )
        self.assertEqual(recover_stale_pending_claims(older_than_seconds=30), 0)
        self.assertEqual(
            conn.execute("SELECT status FROM dm_pending_queue WHERE id=10").fetchone()[0],
            "sending",
        )
        release_global_send_lease(100, str(token), retry_seconds=0)

    def test_active_lease_prevents_stale_reservation_recovery(self) -> None:
        token = try_claim_global_send_lease(100, lease_seconds=300)
        self.assertTrue(token)
        old = _iso(utc_now() - dt.timedelta(minutes=10))
        with conn:
            conn.execute(
                """
                INSERT INTO dm_unified_leads (
                    id, target_user_id, enqueued_at, eligible_at, status,
                    reserved_by_account_user_id, reserved_at, reserve_token, updated_at
                ) VALUES (1,777,?,?,'reserved',100,?,?,?)
                """,
                (old, old, old, str(token), old),
            )
        self.assertEqual(recover_stale_unified_reservations(max_age_seconds=60), 0)
        self.assertEqual(
            conn.execute("SELECT status FROM dm_unified_leads WHERE id=1").fetchone()[0],
            "reserved",
        )
        release_global_send_lease(100, str(token), retry_seconds=0)

    def test_cross_account_claim_does_not_copy_foreign_access_hash(self) -> None:
        self._task(1, 100)
        now = _iso(utc_now())
        with conn:
            conn.execute(
                """
                INSERT INTO dm_unified_leads (
                    id, target_user_id, preferred_account_user_id,
                    source_account_user_id, dm_task_id, target_access_hash,
                    target_username, target_first_name, enqueued_at,
                    eligible_at, status, updated_at
                ) VALUES (1,777,999,999,99,12345,NULL,'X',?,?,'pending',?)
                """,
                (now, now, now),
            )
            conn.execute(
                """
                INSERT INTO dm_unified_lead_accounts (
                    lead_id, account_user_id, is_preferred, access_hash, target_username
                ) VALUES (1,999,1,12345,NULL)
                """
            )
        row = prepare_unified_send_for_account(100)
        self.assertIsNotNone(row)
        assert row is not None
        self.assertIsNone(row["target_access_hash"])
        release_global_send_lease(100, str(row["_global_lease_token"]), retry_seconds=0)

    def test_permanent_failure_is_terminal_and_not_retried(self) -> None:
        now = _iso(utc_now())
        with conn:
            conn.execute(
                """
                INSERT INTO dm_pending_queue (
                    id, dm_task_id, account_user_id, target_user_id,
                    enqueued_at, eligible_at, status, retry_count,
                    resolve_attempts, updated_at
                ) VALUES (10,1,100,777,?,?, 'cancelled',0,0,?)
                """,
                (now, now, now),
            )
            conn.execute(
                """
                INSERT INTO dm_unified_leads (
                    id, target_user_id, enqueued_at, eligible_at, status,
                    legacy_pending_id, retry_count, updated_at
                ) VALUES (1,777,?,?,'reserved',10,0,?)
                """,
                (now, now, now),
            )
        release_unified_lead_for_pending(
            10, status="cancelled", error="known_failure", retry_seconds=None
        )
        status, retries = conn.execute(
            "SELECT status,retry_count FROM dm_unified_leads WHERE id=1"
        ).fetchone()
        self.assertEqual(status, "cancelled")
        self.assertEqual(retries, 0)

    def test_real_retry_increments_retry_count(self) -> None:
        now = _iso(utc_now())
        with conn:
            conn.execute(
                """
                INSERT INTO dm_pending_queue (
                    id, dm_task_id, account_user_id, target_user_id,
                    enqueued_at, eligible_at, status, retry_count,
                    resolve_attempts, updated_at
                ) VALUES (10,1,100,777,?,?, 'retry_wait',0,0,?)
                """,
                (now, now, now),
            )
            conn.execute(
                """
                INSERT INTO dm_unified_leads (
                    id, target_user_id, enqueued_at, eligible_at, status,
                    legacy_pending_id, retry_count, updated_at
                ) VALUES (1,777,?,?,'reserved',10,0,?)
                """,
                (now, now, now),
            )
        release_unified_lead_for_pending(
            10, status="retry_wait", error="flood_wait", retry_seconds=30
        )
        status, retries = conn.execute(
            "SELECT status,retry_count FROM dm_unified_leads WHERE id=1"
        ).fetchone()
        self.assertEqual(status, "retry_wait")
        self.assertEqual(retries, 1)


if __name__ == "__main__":
    unittest.main()
