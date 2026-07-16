from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class DmQueueMigrationTests(unittest.TestCase):
    def test_v121_queue_unique_constraint_is_rebuilt_without_data_loss(self) -> None:
        with tempfile.TemporaryDirectory(prefix="tgblaster-migrate-") as tmp:
            db_path = Path(tmp) / "legacy.sqlite3"
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE sessions(user_id INTEGER PRIMARY KEY, session_string TEXT NOT NULL);
                CREATE TABLE dm_tasks(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    admin_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    session_string TEXT NOT NULL,
                    post_text TEXT NOT NULL,
                    photo_url TEXT,
                    interval_minutes INTEGER NOT NULL,
                    is_active BOOLEAN DEFAULT 1,
                    created_at TEXT,
                    delay_min INTEGER DEFAULT 30,
                    delay_max INTEGER DEFAULT 90
                );
                CREATE TABLE dm_sent_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dm_task_id INTEGER NOT NULL,
                    target_user_id INTEGER NOT NULL,
                    sent_at TEXT NOT NULL,
                    status TEXT DEFAULT 'sent'
                );
                CREATE TABLE dm_watched_chats(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dm_task_id INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL
                );
                CREATE TABLE dm_pending_queue(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dm_task_id INTEGER NOT NULL,
                    target_user_id INTEGER NOT NULL,
                    target_access_hash INTEGER,
                    target_username TEXT,
                    target_first_name TEXT,
                    target_last_name TEXT,
                    source_chat_id INTEGER,
                    source_chat_title TEXT,
                    enqueued_at TEXT NOT NULL,
                    eligible_at TEXT NOT NULL,
                    UNIQUE(dm_task_id,target_user_id)
                );
                """
            )
            conn.execute("INSERT INTO sessions VALUES(100,'session')")
            for task_id in (1, 2):
                conn.execute(
                    """
                    INSERT INTO dm_tasks(
                        id,admin_id,user_id,session_string,post_text,
                        interval_minutes,is_active,created_at,delay_min,delay_max
                    ) VALUES(?,1,100,'session','Привет',0,1,'2026-01-01',0,0)
                    """,
                    (task_id,),
                )
                conn.execute(
                    "INSERT INTO dm_watched_chats(dm_task_id,chat_id) VALUES(?,?)",
                    (task_id, 10 + task_id),
                )
            conn.execute(
                """
                INSERT INTO dm_pending_queue(
                    dm_task_id,target_user_id,source_chat_id,source_chat_title,
                    enqueued_at,eligible_at
                ) VALUES(1,500,11,'A','2026-01-01','2026-01-01')
                """
            )
            conn.execute(
                """
                INSERT INTO dm_pending_queue(
                    dm_task_id,target_user_id,source_chat_id,source_chat_title,
                    enqueued_at,eligible_at
                ) VALUES(2,500,22,'B','2026-01-01','2026-01-01')
                """
            )
            conn.execute("UPDATE dm_tasks SET is_active=0 WHERE id=1")
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
            code = """
from utils.database.database import create_table, create_dm_tables
from config import conn
create_table(); create_dm_tables(); create_dm_tables()
rows = conn.execute('SELECT id,status,dm_task_id,source_chat_id FROM dm_pending_queue ORDER BY id').fetchall()
sources = conn.execute('SELECT dm_task_id,source_chat_id FROM dm_pending_sources WHERE pending_id=1 ORDER BY dm_task_id,source_chat_id').fetchall()
source_columns = conn.execute('PRAGMA table_info(dm_pending_sources)').fetchall()
source_pk = [row[1] for row in sorted(source_columns, key=lambda row: row[5] or 0) if row[5]]
schema = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='dm_pending_queue'").fetchone()[0]
assert rows == [(1,'pending',2,22),(2,'cancelled',2,22)], rows
assert sources == [(1,11),(2,22)], sources
assert source_pk == ['pending_id','dm_task_id','source_chat_id'], source_pk
assert 'UNIQUE(dm_task_id,target_user_id)' not in ''.join(schema.split()), schema
conn.execute("UPDATE dm_pending_queue SET status='cancelled' WHERE id=1")
conn.commit()
from services.dm_task_queue import enqueue_pending
created, pending_id = enqueue_pending(dm_task_id=1,account_user_id=100,target_user_id=500,target_access_hash=1,target_username='u',target_first_name=None,target_last_name=None,source_chat_id=33,source_chat_title='C',delay_min=0,delay_max=0)
assert created and pending_id == 3, (created, pending_id)
"""
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
