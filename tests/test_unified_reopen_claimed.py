
from __future__ import annotations
import os, unittest
os.environ.setdefault("API_ID", "1")
os.environ.setdefault("API_HASH", "h")
os.environ.setdefault("BOT_TOKEN", "1:t")
os.environ.setdefault("ADMIN_ID_LIST", "1")
os.environ.setdefault("DB_PATH", "/tmp/uq_reopen.db")
os.environ.setdefault("BOT_SESSION_PATH", "/tmp/uq_reopen_bot")
os.environ.setdefault("MEDIA_DIR", "/tmp/uq_reopen_media")

from config import conn
from services.dm_unified_queue import (
    MODE_UNIFIED, ensure_unified_queue_schema, prepare_unified_send_for_account,
    set_queue_mode, _iso, utc_now, _ensure_pending_row_for_account,
)
from utils.database.database import create_table, create_dm_tables

class ReopenClaimedTests(unittest.TestCase):
    def setUp(self):
        create_table(); create_dm_tables(); ensure_unified_queue_schema()
        with conn:
            for t in ("dm_pending_queue","dm_unified_leads","dm_unified_lead_accounts","dm_tasks","dm_account_dispatch"):
                try: conn.execute(f"DELETE FROM {t}")
                except Exception: pass
        set_queue_mode(MODE_UNIFIED, admin_id=1)
        with conn:
            conn.execute("UPDATE dm_queue_runtime SET next_global_send_at=NULL, last_global_send_at=NULL, lease_owner_account_user_id=NULL, lease_token=NULL, lease_expires_at=NULL WHERE id=1")
        now = _iso(utc_now())
        with conn:
            conn.execute("""INSERT INTO dm_tasks (id, admin_id, user_id, session_string, post_text, interval_minutes, is_active, delay_min, delay_max, first_dm_module)
                VALUES (1,1,100,'s','x',0,1,1,2,'default')""")
            conn.execute("""INSERT INTO dm_pending_queue
                (id, dm_task_id, account_user_id, target_user_id, target_access_hash,
                 enqueued_at, eligible_at, status, claim_token, claimed_at, retry_count, resolve_attempts, updated_at)
                VALUES (10,1,100,777,1,?,?, 'claimed','tok',?,0,0,?)""", (now,now,now,now))
            conn.execute("""INSERT INTO dm_unified_leads
                (id, target_user_id, preferred_account_user_id, source_account_user_id, dm_task_id,
                 target_access_hash, enqueued_at, eligible_at, status, legacy_pending_id, updated_at)
                VALUES (1,777,100,100,1,1,?,?,'pending',10,?)""", (now,now,now))
            conn.execute("""INSERT INTO dm_account_dispatch (account_user_id, next_send_at, is_paused, updated_at)
                VALUES (100,?,0,?)""", (now,now))

    def test_reopen_claimed_pending(self):
        lead = {
            "target_user_id": 777,
            "target_access_hash": 1,
            "target_username": None,
            "target_first_name": "X",
            "target_last_name": None,
            "source_chat_id": None,
            "source_chat_title": None,
            "source_chat_username": None,
            "source_message_id": None,
            "enqueued_at": _iso(utc_now()),
            "preferred_account_user_id": 100,
        }
        with conn:
            pid = _ensure_pending_row_for_account(lead, 100, 1)
        self.assertEqual(pid, 10)
        st = conn.execute("SELECT status, claim_token FROM dm_pending_queue WHERE id=10").fetchone()
        self.assertEqual(st[0], "pending")
        self.assertIsNone(st[1])

    def test_prepare_returns_row(self):
        row = prepare_unified_send_for_account(100)
        self.assertIsNotNone(row)
        self.assertEqual(int(row["target_user_id"]), 777)
        self.assertEqual(row["status"], "pending")

if __name__ == "__main__":
    unittest.main()
