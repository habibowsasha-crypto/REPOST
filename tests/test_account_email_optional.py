from __future__ import annotations

import os
import sqlite3
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

os.environ.setdefault("API_ID", "123456")
os.environ.setdefault("API_HASH", "test_hash")
os.environ.setdefault("BOT_TOKEN", "123456:test_token")
os.environ.setdefault("ADMIN_ID_LIST", "123")
os.environ.setdefault("DB_PATH", "/tmp/tgblaster_account_email_optional.db")
os.environ.setdefault("BOT_SESSION_PATH", "/tmp/tgblaster_account_email_optional_bot")
os.environ.setdefault("MEDIA_DIR", "/tmp/tgblaster_account_email_optional_media")

from config import conn, email_waiting
from handlers.account.account_handlers import (
    _normalize_email,
    skip_account_email,
)
from utils.database import database as database_module
from utils.database.database import create_table


class OptionalAccountEmailTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        create_table()
        email_waiting.clear()
        with conn:
            conn.execute("DELETE FROM sessions")

    def test_schema_contains_nullable_account_email(self) -> None:
        columns = {
            row[1]: row for row in conn.execute("PRAGMA table_info(sessions)").fetchall()
        }
        self.assertIn("account_email", columns)
        self.assertEqual(columns["account_email"][3], 0)  # nullable

    def test_email_validation_is_strict_but_practical(self) -> None:
        self.assertEqual(_normalize_email(" User.Name+tag@Example.COM "), "user.name+tag@example.com")
        self.assertIsNone(_normalize_email("not-an-email"))
        self.assertIsNone(_normalize_email("a@localhost"))
        self.assertIsNone(_normalize_email(""))

    async def test_skip_keeps_existing_email_and_finishes_account(self) -> None:
        with conn:
            conn.execute(
                "INSERT INTO sessions(user_id,session_string,account_email) VALUES (?,?,?)",
                (90001, "session", "saved@example.com"),
            )
        email_waiting[123] = {"user_id": 90001, "waiting": True, "last_message_id": 1}
        event = SimpleNamespace(
            sender_id=123,
            data=b"account_email_skip_90001",
            answer=AsyncMock(),
            respond=AsyncMock(),
        )
        await skip_account_email(event)
        stored = conn.execute(
            "SELECT account_email FROM sessions WHERE user_id=90001"
        ).fetchone()[0]
        self.assertEqual(stored, "saved@example.com")
        self.assertNotIn(123, email_waiting)
        event.answer.assert_awaited()
        event.respond.assert_awaited()
        self.assertIn("saved@example.com", event.respond.await_args.args[0])

    def test_legacy_sessions_table_migrates_without_data_loss(self) -> None:
        old_conn = database_module.conn
        temp = sqlite3.connect(":memory:")
        try:
            temp.execute(
                "CREATE TABLE sessions(user_id INTEGER PRIMARY KEY, session_string TEXT NOT NULL)"
            )
            temp.execute("INSERT INTO sessions VALUES (1, 'legacy-session')")
            database_module.conn = temp
            database_module.create_table()
            row = temp.execute(
                "SELECT user_id,session_string,account_email FROM sessions"
            ).fetchone()
            self.assertEqual(row, (1, "legacy-session", None))
        finally:
            database_module.conn = old_conn
            temp.close()


if __name__ == "__main__":
    unittest.main()
