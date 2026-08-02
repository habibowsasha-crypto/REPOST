
from __future__ import annotations

import os
import unittest

os.environ.setdefault("API_ID", "1")
os.environ.setdefault("API_HASH", "h")
os.environ.setdefault("BOT_TOKEN", "1:t")
os.environ.setdefault("ADMIN_ID_LIST", "1")
os.environ.setdefault("DB_PATH", "/tmp/unified_purge.db")
os.environ.setdefault("BOT_SESSION_PATH", "/tmp/unified_purge_bot")
os.environ.setdefault("MEDIA_DIR", "/tmp/unified_purge_media")

from config import conn
from services.dm_task_queue import cancel_target_globally, enqueue_pending
from services.dm_unified_queue import (
    MODE_UNIFIED,
    is_unified_queue_mode,
    mark_unified_lead_sent,
    set_queue_mode,
    shadow_sync_pending_row,
)
from utils.database.database import create_dm_tables, create_table


class UnifiedGlobalPurgeTests(unittest.TestCase):
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
                "dm_account_dispatch",
            ):
                try:
                    conn.execute(f"DELETE FROM {table}")
                except Exception:
                    pass
        set_queue_mode(MODE_UNIFIED, admin_id=1)
        self.assertTrue(is_unified_queue_mode())
        with conn:
            for tid, uid in ((1, 100), (2, 200)):
                conn.execute(
                    """
                    INSERT INTO dm_tasks (
                        id, admin_id, user_id, session_string, post_text,
                        interval_minutes, is_active, delay_min, delay_max, first_dm_module
                    ) VALUES (?, 1, ?, "s", "x", 0, 1, 1, 2, "default")
                    """,
                    (tid, uid),
                )

    def test_cancel_other_accounts_keeps_except_id(self) -> None:
        a, _ = enqueue_pending(
            dm_task_id=1, account_user_id=100, target_user_id=55,
            target_access_hash=1, target_username="petya",
            target_first_name="P", target_last_name=None,
            source_chat_id=1, source_chat_title="C", delay_min=0, delay_max=0,
        )
        b, _ = enqueue_pending(
            dm_task_id=2, account_user_id=200, target_user_id=55,
            target_access_hash=1, target_username="petya",
            target_first_name="P", target_last_name=None,
            source_chat_id=1, source_chat_title="C", delay_min=0, delay_max=0,
        )
        self.assertTrue(a and b)
        id1 = conn.execute(
            "SELECT id FROM dm_pending_queue WHERE account_user_id=100 AND target_user_id=55"
        ).fetchone()[0]
        id2 = conn.execute(
            "SELECT id FROM dm_pending_queue WHERE account_user_id=200 AND target_user_id=55"
        ).fetchone()[0]
        removed = cancel_target_globally(55, "sent_via_unified_queue", except_pending_id=id2)
        self.assertGreaterEqual(removed, 1)
        st1 = conn.execute("SELECT status FROM dm_pending_queue WHERE id=?", (id1,)).fetchone()[0]
        st2 = conn.execute("SELECT status FROM dm_pending_queue WHERE id=?", (id2,)).fetchone()[0]
        self.assertEqual(st1, "cancelled")
        self.assertEqual(st2, "pending")

    def test_mark_unified_sent_purges_other_pending(self) -> None:
        enqueue_pending(
            dm_task_id=1, account_user_id=100, target_user_id=77,
            target_access_hash=1, target_username="p",
            target_first_name="P", target_last_name=None,
            source_chat_id=1, source_chat_title="C", delay_min=0, delay_max=0,
        )
        enqueue_pending(
            dm_task_id=2, account_user_id=200, target_user_id=77,
            target_access_hash=1, target_username="p",
            target_first_name="P", target_last_name=None,
            source_chat_id=1, source_chat_title="C", delay_min=0, delay_max=0,
        )
        id2 = conn.execute(
            "SELECT id FROM dm_pending_queue WHERE account_user_id=200 AND target_user_id=77"
        ).fetchone()[0]
        shadow_sync_pending_row(id2)
        mark_unified_lead_sent(id2, 200)
        st1 = conn.execute(
            "SELECT status FROM dm_pending_queue WHERE account_user_id=100 AND target_user_id=77"
        ).fetchone()[0]
        self.assertEqual(st1, "cancelled")


if __name__ == "__main__":
    unittest.main()
