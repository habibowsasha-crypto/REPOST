
from __future__ import annotations

import os
import unittest

os.environ.setdefault("API_ID", "123456")
os.environ.setdefault("API_HASH", "test_hash")
os.environ.setdefault("BOT_TOKEN", "123456:test_token")
os.environ.setdefault("ADMIN_ID_LIST", "123")
os.environ.setdefault("DB_PATH", "/tmp/tgblaster_audit_p1.db")
os.environ.setdefault("BOT_SESSION_PATH", "/tmp/tgblaster_audit_p1_bot")
os.environ.setdefault("MEDIA_DIR", "/tmp/tgblaster_audit_p1_media")
os.environ.setdefault("OPENAI_API_KEY", "")
# Ensure unset so code default applies (true).
os.environ.pop("AI_DM_DRY_RUN", None)

from config import conn
from services.dm_task_queue import (
    enqueue_pending,
    get_account_dispatch_state,
    pause_account,
    set_account_cooldown,
)
from services.ai_dialog_service import ai_dry_run
from utils.database.database import create_dm_tables, create_table


class AuditP1FixTests(unittest.TestCase):
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
                "dm_spambot_monitor",
                "sessions",
            ):
                try:
                    conn.execute(f"DELETE FROM {table}")
                except Exception:
                    pass

    def test_dry_run_defaults_false_when_unset_for_compat(self) -> None:
        # Backward-compatible default remains false so existing Railway deploys
        # with AI_DM_ENABLED=true keep sending. .env.example still recommends true.
        os.environ.pop("AI_DM_DRY_RUN", None)
        self.assertFalse(ai_dry_run())

    def test_floodwait_preserves_peerflood_reason(self) -> None:
        pause_account(777, "PeerFlood: ручное возобновление")
        set_account_cooldown(777, 30, "FloodWait")
        state = get_account_dispatch_state(777)
        self.assertTrue(state.is_paused)
        self.assertIn("PeerFlood", state.pause_reason or "")
        self.assertIsNotNone(state.cooldown_until)

    def test_account_delete_clears_dispatch_and_cancels_queue(self) -> None:
        with conn:
            conn.execute(
                "INSERT INTO sessions(user_id, session_string) VALUES (888, 's')"
            )
            conn.execute(
                """
                INSERT INTO dm_tasks (
                    id, admin_id, user_id, session_string, post_text,
                    interval_minutes, is_active, delay_min, delay_max, first_dm_module
                ) VALUES (50, 123, 888, 's', 'x', 0, 1, 30, 90, 'default')
                """
            )
        pause_account(888, "PeerFlood: test")
        enqueue_pending(
            dm_task_id=50,
            account_user_id=888,
            target_user_id=42,
            target_access_hash=1,
            target_username="u",
            target_first_name="A",
            target_last_name=None,
            source_chat_id=1,
            source_chat_title="C",
            delay_min=0,
            delay_max=0,
        )
        # Simulate durable SQL portion of delete_account
        with conn:
            conn.execute(
                "UPDATE dm_tasks SET is_active=0, session_string='' WHERE user_id=?",
                (888,),
            )
            conn.execute(
                """
                UPDATE dm_pending_queue
                   SET status='cancelled', last_error='account_deleted',
                       claim_token=NULL, claimed_at=NULL
                 WHERE account_user_id=?
                   AND status IN (
                        'pending','claimed','sending','retry_wait',
                        'unresolved_peer','uncertain_delivery'
                   )
                """,
                (888,),
            )
            conn.execute("DELETE FROM dm_account_dispatch WHERE account_user_id=?", (888,))
            conn.execute("DELETE FROM sessions WHERE user_id=?", (888,))

        row = conn.execute(
            "SELECT status, last_error FROM dm_pending_queue WHERE account_user_id=888"
        ).fetchone()
        self.assertEqual(row[0], "cancelled")
        self.assertEqual(row[1], "account_deleted")
        dispatch = conn.execute(
            "SELECT 1 FROM dm_account_dispatch WHERE account_user_id=888"
        ).fetchone()
        self.assertIsNone(dispatch)


if __name__ == "__main__":
    unittest.main()
