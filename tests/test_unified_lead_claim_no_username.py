
from __future__ import annotations

import os
import unittest

os.environ.setdefault("API_ID", "1")
os.environ.setdefault("API_HASH", "h")
os.environ.setdefault("BOT_TOKEN", "1:t")
os.environ.setdefault("ADMIN_ID_LIST", "1")
os.environ.setdefault("DB_PATH", "/tmp/uq_claim_nouser.db")
os.environ.setdefault("BOT_SESSION_PATH", "/tmp/uq_claim_nouser_bot")
os.environ.setdefault("MEDIA_DIR", "/tmp/uq_claim_nouser_media")

from config import conn
from services.dm_unified_queue import (
    MODE_UNIFIED,
    ensure_unified_queue_schema,
    prepare_unified_send_for_account,
    set_queue_mode,
    _iso,
    utc_now,
)
from utils.database.database import create_dm_tables, create_table


class UnifiedClaimNoUsernameTests(unittest.TestCase):
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
            ):
                try:
                    conn.execute(f"DELETE FROM {table}")
                except Exception:
                    pass
        set_queue_mode(MODE_UNIFIED, admin_id=1)
        with conn:
            conn.execute("UPDATE dm_queue_runtime SET next_global_send_at=NULL, last_global_send_at=NULL, lease_owner_account_user_id=NULL, lease_token=NULL, lease_expires_at=NULL WHERE id=1")
        now = _iso(utc_now())
        with conn:
            conn.execute(
                """
                INSERT INTO dm_tasks (
                    id, admin_id, user_id, session_string, post_text,
                    interval_minutes, is_active, delay_min, delay_max, first_dm_module
                ) VALUES (1, 1, 100, 'session', 'x', 0, 1, 1, 2, 'default')
                """
            )
            # Lead WITHOUT username and WITHOUT lead_accounts row — previously unclaimable.
            conn.execute(
                """
                INSERT INTO dm_unified_leads (
                    target_user_id, preferred_account_user_id, source_account_user_id,
                    dm_task_id, target_access_hash, target_username,
                    target_first_name, enqueued_at, eligible_at, status, updated_at
                ) VALUES (555, NULL, 999, 99, 12345, NULL, 'NoUser', ?, ?, 'pending', ?)
                """,
                (now, now, now),
            )
            conn.execute(
                """
                INSERT INTO dm_account_dispatch (
                    account_user_id, next_send_at, is_paused, updated_at
                ) VALUES (100, ?, 0, ?)
                """,
                (now, now),
            )

    def test_account_can_claim_lead_without_username(self) -> None:
        row = prepare_unified_send_for_account(100)
        self.assertIsNotNone(row)
        self.assertEqual(int(row["target_user_id"]), 555)
        self.assertEqual(int(row["account_user_id"]), 100)
        self.assertIsNone(row["target_access_hash"])


if __name__ == "__main__":
    unittest.main()
