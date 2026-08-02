from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class GlobalCompletedMigrationTests(unittest.TestCase):
    def test_account_scoped_completed_and_claims_migrate_to_global(self) -> None:
        with tempfile.TemporaryDirectory(prefix="tgblaster-global-completed-") as tmp:
            db_path = Path(tmp) / "legacy.sqlite3"
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE dm_completed_contacts (
                    account_user_id INTEGER NOT NULL,
                    target_user_id INTEGER NOT NULL,
                    source_chat_id INTEGER,
                    source_chat_title TEXT,
                    cycle_id INTEGER,
                    completed_at TEXT NOT NULL,
                    completion_reason TEXT,
                    PRIMARY KEY (account_user_id, target_user_id)
                );
                CREATE INDEX idx_dm_completed_chat
                    ON dm_completed_contacts(source_chat_id);

                CREATE TABLE dm_first_dm_claims (
                    account_user_id INTEGER NOT NULL,
                    target_user_id INTEGER NOT NULL,
                    claim_token TEXT NOT NULL,
                    dm_task_id INTEGER,
                    source_chat_id INTEGER,
                    claimed_at TEXT NOT NULL,
                    PRIMARY KEY (account_user_id, target_user_id),
                    UNIQUE (claim_token)
                );
                CREATE INDEX idx_dm_first_claims_at
                    ON dm_first_dm_claims(claimed_at);
                """
            )
            conn.execute(
                "INSERT INTO dm_completed_contacts VALUES(100,500,700,'First',1,'2026-01-01T00:00:00+00:00','first')"
            )
            conn.execute(
                "INSERT INTO dm_completed_contacts VALUES(101,500,701,'Second',2,'2026-01-02T00:00:00+00:00','second')"
            )
            conn.execute(
                "INSERT INTO dm_first_dm_claims VALUES(100,600,'aaa',1,700,'2026-01-01T00:00:00+00:00')"
            )
            conn.execute(
                "INSERT INTO dm_first_dm_claims VALUES(101,600,'bbb',2,701,'2026-01-02T00:00:00+00:00')"
            )
            conn.commit()
            conn.close()

            env = os.environ.copy()
            env.update(
                {
                    "API_ID": "123456",
                    "API_HASH": "test_hash",
                    "BOT_TOKEN": "123456:test_token",
                    "ADMIN_ID_LIST": "123",
                    "DB_PATH": str(db_path),
                    "BOT_SESSION_PATH": str(Path(tmp) / "bot"),
                    "MEDIA_DIR": str(Path(tmp) / "media"),
                    "OPENAI_API_KEY": "",
                }
            )
            code = r'''
from config import conn
from services.dm_contact_analytics import (
    clear_completed_for_chat,
    create_contact_tables,
    is_completed_contact,
    try_claim_first_dm,
)
create_contact_tables(); create_contact_tables()
completed_pk = [
    row[1] for row in sorted(
        conn.execute('PRAGMA table_info(dm_completed_contacts)').fetchall(),
        key=lambda row: row[5] or 0,
    ) if row[5]
]
claim_pk = [
    row[1] for row in sorted(
        conn.execute('PRAGMA table_info(dm_first_dm_claims)').fetchall(),
        key=lambda row: row[5] or 0,
    ) if row[5]
]
assert completed_pk == ['target_user_id'], completed_pk
assert claim_pk == ['target_user_id'], claim_pk
completed = conn.execute(
    'SELECT target_user_id,account_user_id,source_chat_id,completion_reason FROM dm_completed_contacts'
).fetchall()
claims = conn.execute(
    'SELECT target_user_id,account_user_id,claim_token FROM dm_first_dm_claims'
).fetchall()
assert completed == [(500,100,700,'first')], completed
assert claims == [(600,100,'aaa')], claims
assert is_completed_contact(999,500)
assert try_claim_first_dm(account_user_id=999,target_user_id=500,dm_task_id=9,source_chat_id=900) is None
assert clear_completed_for_chat(700) == 1
assert not is_completed_contact(100,500)
assert not is_completed_contact(999,500)
'''
            result = subprocess.run(
                [sys.executable, "-c", code],
                cwd=Path(__file__).resolve().parents[1],
                env=env,
                text=True,
                capture_output=True,
                timeout=30,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
