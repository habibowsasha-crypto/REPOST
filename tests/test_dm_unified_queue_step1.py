from __future__ import annotations

import os
import unittest

os.environ.setdefault("API_ID", "123456")
os.environ.setdefault("API_HASH", "test_hash")
os.environ.setdefault("BOT_TOKEN", "123456:test_token")
os.environ.setdefault("ADMIN_ID_LIST", "123")
os.environ.setdefault("DB_PATH", "/tmp/tgblaster_unified_queue_step1.db")
os.environ.setdefault("BOT_SESSION_PATH", "/tmp/tgblaster_unified_queue_step1_bot")
os.environ.setdefault("MEDIA_DIR", "/tmp/tgblaster_unified_queue_step1_media")
os.environ.setdefault("OPENAI_API_KEY", "")

from config import conn
from services.dm_task_queue import enqueue_pending, pause_account, resume_account
from services.dm_unified_queue import (
    MODE_PER_ACCOUNT,
    MODE_UNIFIED,
    count_active_unified_leads,
    get_queue_runtime_state,
    is_unified_queue_mode,
    migrate_pending_queue_to_unified_pool,
    set_queue_mode,
    shadow_sync_pending_row,
)
from utils.database.database import create_dm_tables, create_table


class UnifiedQueueStep1Tests(unittest.TestCase):
    def setUp(self) -> None:
        create_table()
        create_dm_tables()
        with conn:
            for table in (
                "dm_pending_sources",
                "dm_pending_queue",
                "dm_unified_lead_sources",
                "dm_unified_lead_accounts",
                "dm_unified_leads",
                "dm_tasks",
                "sessions",
                "dm_account_dispatch",
            ):
                conn.execute(f"DELETE FROM {table}")
            conn.execute(
                "UPDATE dm_queue_runtime SET mode=?, updated_by_admin_id=NULL, "
                "next_global_send_at=NULL, last_global_send_at=NULL, "
                "lease_owner_account_user_id=NULL, lease_token=NULL, lease_expires_at=NULL "
                "WHERE id=1",
                (MODE_PER_ACCOUNT,),
            )
            conn.execute(
                "INSERT INTO sessions(user_id, session_string) VALUES (101, 's1'), (102, 's2')"
            )
            conn.execute(
                """
                INSERT INTO dm_tasks (
                    id, admin_id, user_id, session_string, post_text,
                    interval_minutes, is_active, delay_min, delay_max, first_dm_module
                ) VALUES
                (1, 123, 101, 's1', 'x', 0, 1, 30, 90, 'default'),
                (2, 123, 102, 's2', 'y', 0, 1, 30, 90, 'kirill_vip')
                """
            )

    def test_default_mode_is_per_account(self) -> None:
        state = get_queue_runtime_state()
        self.assertEqual(state.mode, MODE_PER_ACCOUNT)
        self.assertFalse(is_unified_queue_mode())

    def test_set_mode_persists_without_enabling_send_path(self) -> None:
        state = set_queue_mode(MODE_UNIFIED, admin_id=123)
        self.assertEqual(state.mode, MODE_UNIFIED)
        self.assertTrue(is_unified_queue_mode())
        # Step 1 must not change pause/resume semantics of account dispatch.
        pause_account(101, "PeerFlood: test")
        resume_account(101)
        again = get_queue_runtime_state()
        self.assertEqual(again.mode, MODE_UNIFIED)
        set_queue_mode(MODE_PER_ACCOUNT, admin_id=123)

    def test_enqueue_shadow_syncs_into_unified_pool(self) -> None:
        created, pending_id = enqueue_pending(
            dm_task_id=1,
            account_user_id=101,
            target_user_id=5001,
            target_access_hash=111,
            target_username="lead_user",
            target_first_name="Lead",
            target_last_name=None,
            source_chat_id=9001,
            source_chat_title="Chat A",
            delay_min=30,
            delay_max=30,
            source_chat_username="chat_a",
            source_message_id=77,
        )
        self.assertTrue(created)
        self.assertGreater(pending_id, 0)
        self.assertEqual(count_active_unified_leads(), 1)
        row = conn.execute(
            """
            SELECT target_user_id, preferred_account_user_id, legacy_pending_id,
                   source_chat_id, first_dm_module, status
              FROM dm_unified_leads
             WHERE target_user_id=5001
            """
        ).fetchone()
        self.assertEqual(row[0], 5001)
        self.assertEqual(row[1], 101)
        self.assertEqual(row[2], pending_id)
        self.assertEqual(row[3], 9001)
        self.assertEqual(row[4], "default")
        self.assertEqual(row[5], "pending")
        accounts = conn.execute(
            "SELECT account_user_id, is_preferred FROM dm_unified_lead_accounts WHERE lead_id IN "
            "(SELECT id FROM dm_unified_leads WHERE target_user_id=5001)"
        ).fetchall()
        self.assertEqual(accounts, [(101, 1)])

    def test_migration_merges_same_target_across_accounts(self) -> None:
        # Insert legacy rows without going through enqueue shadow path.
        with conn:
            conn.execute(
                """
                INSERT INTO dm_pending_queue (
                    id, dm_task_id, account_user_id, target_user_id,
                    target_access_hash, target_username, target_first_name,
                    target_last_name, source_chat_id, source_chat_title,
                    source_chat_username, source_message_id,
                    enqueued_at, eligible_at, status, retry_count,
                    resolve_attempts, updated_at
                ) VALUES
                (10, 1, 101, 7001, 1, 'u7001', 'A', NULL, 11, 'C1', 'c1', 1,
                 '2026-01-01T00:00:00+00:00', '2026-01-01T00:10:00+00:00',
                 'pending', 0, 0, '2026-01-01T00:00:00+00:00'),
                (11, 2, 102, 7001, 2, 'u7001', 'A', NULL, 22, 'C2', 'c2', 2,
                 '2026-01-01T00:01:00+00:00', '2026-01-01T00:05:00+00:00',
                 'pending', 0, 0, '2026-01-01T00:01:00+00:00')
                """
            )
            conn.execute(
                """
                INSERT INTO dm_pending_sources (
                    pending_id, dm_task_id, source_chat_id, source_chat_title,
                    source_chat_username, source_message_id, first_seen_at, last_seen_at
                ) VALUES
                (10, 1, 11, 'C1', 'c1', 1, '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00'),
                (11, 2, 22, 'C2', 'c2', 2, '2026-01-01T00:01:00+00:00', '2026-01-01T00:01:00+00:00')
                """
            )
        result = migrate_pending_queue_to_unified_pool()
        self.assertEqual(result["pending_rows_synced"], 2)
        self.assertEqual(result["active_unified_leads"], 1)
        lead = conn.execute(
            "SELECT id, preferred_account_user_id, eligible_at FROM dm_unified_leads WHERE target_user_id=7001"
        ).fetchone()
        self.assertIsNotNone(lead)
        # Earliest eligible_at from the two rows must win.
        self.assertEqual(lead[2], "2026-01-01T00:05:00+00:00")
        accounts = {
            int(r[0])
            for r in conn.execute(
                "SELECT account_user_id FROM dm_unified_lead_accounts WHERE lead_id=?",
                (int(lead[0]),),
            )
        }
        self.assertEqual(accounts, {101, 102})
        sources = conn.execute(
            "SELECT COUNT(*) FROM dm_unified_lead_sources WHERE lead_id=?",
            (int(lead[0]),),
        ).fetchone()
        self.assertEqual(int(sources[0]), 2)

    def test_migration_is_idempotent(self) -> None:
        enqueue_pending(
            dm_task_id=1,
            account_user_id=101,
            target_user_id=8001,
            target_access_hash=None,
            target_username=None,
            target_first_name="X",
            target_last_name=None,
            source_chat_id=55,
            source_chat_title="S",
            delay_min=10,
            delay_max=10,
        )
        first = migrate_pending_queue_to_unified_pool()
        second = migrate_pending_queue_to_unified_pool()
        self.assertEqual(count_active_unified_leads(), 1)
        self.assertEqual(first["active_unified_leads"], 1)
        self.assertEqual(second["active_unified_leads"], 1)


if __name__ == "__main__":
    unittest.main()


class UnifiedQueueStep2Tests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        create_table()
        create_dm_tables()
        with conn:
            for table in (
                "dm_pending_sources",
                "dm_pending_queue",
                "dm_unified_lead_sources",
                "dm_unified_lead_accounts",
                "dm_unified_leads",
                "dm_tasks",
                "sessions",
                "dm_account_dispatch",
            ):
                conn.execute(f"DELETE FROM {table}")
            conn.execute(
                "UPDATE dm_queue_runtime SET mode=?, next_global_send_at=NULL, "
                "last_global_send_at=NULL, lease_owner_account_user_id=NULL, "
                "lease_token=NULL, lease_expires_at=NULL, "
                "global_spacing_min=30, global_spacing_max=30, "
                "updated_by_admin_id=NULL WHERE id=1",
                (MODE_PER_ACCOUNT,),
            )
            conn.execute(
                "INSERT INTO sessions(user_id, session_string) VALUES (201, 's1'), (202, 's2')"
            )
            conn.execute(
                """
                INSERT INTO dm_tasks (
                    id, admin_id, user_id, session_string, post_text,
                    interval_minutes, is_active, delay_min, delay_max, first_dm_module
                ) VALUES
                (11, 123, 201, 's1', 'x', 0, 1, 30, 90, 'default'),
                (12, 123, 202, 's2', 'y', 0, 1, 30, 90, 'default')
                """
            )

    def test_prepare_returns_none_when_mode_per_account(self) -> None:
        from services.dm_unified_queue import prepare_unified_send_for_account

        enqueue_pending(
            dm_task_id=11,
            account_user_id=201,
            target_user_id=9001,
            target_access_hash=1,
            target_username="u9001",
            target_first_name="T",
            target_last_name=None,
            source_chat_id=1,
            source_chat_title="C",
            delay_min=0,
            delay_max=0,
        )
        self.assertIsNone(prepare_unified_send_for_account(201))

    def test_prepare_reserves_lead_and_materializes_pending(self) -> None:
        from services.dm_unified_queue import (
            complete_global_send_window,
            prepare_unified_send_for_account,
            set_queue_mode,
        )

        set_queue_mode(MODE_UNIFIED, admin_id=123)
        enqueue_pending(
            dm_task_id=11,
            account_user_id=201,
            target_user_id=9002,
            target_access_hash=5,
            target_username="u9002",
            target_first_name="T",
            target_last_name=None,
            source_chat_id=2,
            source_chat_title="C",
            delay_min=0,
            delay_max=0,
        )
        # Make eligible immediately
        with conn:
            conn.execute(
                "UPDATE dm_unified_leads SET eligible_at=? WHERE target_user_id=9002",
                ("2020-01-01T00:00:00+00:00",),
            )
            conn.execute(
                "UPDATE dm_pending_queue SET eligible_at=? WHERE target_user_id=9002",
                ("2020-01-01T00:00:00+00:00",),
            )
        row = prepare_unified_send_for_account(201)
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(int(row["account_user_id"]), 201)
        self.assertEqual(int(row["target_user_id"]), 9002)
        lead = conn.execute(
            "SELECT status, reserved_by_account_user_id FROM dm_unified_leads WHERE target_user_id=9002"
        ).fetchone()
        self.assertEqual(lead[0], "reserved")
        self.assertEqual(int(lead[1]), 201)
        # Second account cannot take global lease until released/completed
        self.assertIsNone(prepare_unified_send_for_account(202))
        complete_global_send_window(201, str(row["_global_lease_token"]))

    def test_other_account_can_take_lead_when_preferred_paused(self) -> None:
        from services.dm_unified_queue import (
            prepare_unified_send_for_account,
            release_global_send_lease,
            set_queue_mode,
        )

        set_queue_mode(MODE_UNIFIED, admin_id=123)
        enqueue_pending(
            dm_task_id=11,
            account_user_id=201,
            target_user_id=9003,
            target_access_hash=None,
            target_username="shared_user",
            target_first_name="S",
            target_last_name=None,
            source_chat_id=3,
            source_chat_title="C",
            delay_min=0,
            delay_max=0,
        )
        with conn:
            conn.execute(
                "UPDATE dm_unified_leads SET eligible_at=? WHERE target_user_id=9003",
                ("2020-01-01T00:00:00+00:00",),
            )
        pause_account(201, "PeerFlood: test")
        row = prepare_unified_send_for_account(202)
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(int(row["account_user_id"]), 202)
        self.assertEqual(int(row["target_user_id"]), 9003)
        # Preferred account pending should be cancelled
        status = conn.execute(
            "SELECT status FROM dm_pending_queue WHERE account_user_id=201 AND target_user_id=9003"
        ).fetchone()
        self.assertEqual(status[0], "cancelled")


class UnifiedQueueStep3Tests(unittest.TestCase):
    def setUp(self) -> None:
        create_table()
        create_dm_tables()
        with conn:
            for table in (
                "dm_pending_sources",
                "dm_pending_queue",
                "dm_unified_lead_sources",
                "dm_unified_lead_accounts",
                "dm_unified_leads",
                "dm_tasks",
                "sessions",
                "dm_account_dispatch",
            ):
                conn.execute(f"DELETE FROM {table}")
            conn.execute(
                "UPDATE dm_queue_runtime SET mode=?, next_global_send_at=NULL, "
                "last_global_send_at=NULL, lease_owner_account_user_id=NULL, "
                "lease_token=NULL, lease_expires_at=NULL, "
                "global_spacing_min=30, global_spacing_max=60 WHERE id=1",
                (MODE_PER_ACCOUNT,),
            )

    def test_stats_and_spacing_presets(self) -> None:
        from services.dm_unified_queue import set_global_spacing, unified_queue_stats

        stats = unified_queue_stats()
        self.assertEqual(stats["mode"], MODE_PER_ACCOUNT)
        self.assertEqual(stats["active_leads"], 0)
        self.assertIn("by_status", stats)
        state = set_global_spacing(120, 300)
        self.assertEqual(state.global_spacing_min, 120)
        self.assertEqual(state.global_spacing_max, 300)
        stats2 = unified_queue_stats()
        self.assertEqual(stats2["global_spacing_min"], 120)
        self.assertEqual(stats2["global_spacing_max"], 300)

    def test_invalid_spacing_rejected(self) -> None:
        from services.dm_unified_queue import set_global_spacing

        with self.assertRaises(ValueError):
            set_global_spacing(1, 2)
        with self.assertRaises(ValueError):
            set_global_spacing(100, 50)
