from __future__ import annotations

import asyncio
import os
import unittest

os.environ.setdefault("API_ID", "123456")
os.environ.setdefault("API_HASH", "test_hash")
os.environ.setdefault("BOT_TOKEN", "123456:test_token")
os.environ.setdefault("ADMIN_ID_LIST", "123")
os.environ.setdefault("DB_PATH", "/tmp/tgblaster_ai_first_dm.db")
os.environ.setdefault("BOT_SESSION_PATH", "/tmp/tgblaster_ai_first_dm_bot")
os.environ.setdefault("MEDIA_DIR", "/tmp/tgblaster_ai_first_dm_media")
os.environ.setdefault("OPENAI_API_KEY", "")

from config import conn
from services.first_dm_modules import (
    AI_FIRST_DM_MODULE,
    DEFAULT_FIRST_DM_MODULE,
    first_dm_module_label,
    normalize_first_dm_module,
)
from services.first_message_ai_generated import (
    MODULE_ID,
    _validate,
    choose_ai_generated_first_dm_text,
    choose_ai_generated_first_dm_text_sync,
    ensure_ai_first_dm_history_table,
)
from utils.database.database import create_dm_tables, create_table


class AIFirstDmModuleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        create_table()
        create_dm_tables()
        ensure_ai_first_dm_history_table()
        with conn:
            conn.execute("DELETE FROM ai_first_dm_history")

    def test_module_registered(self) -> None:
        self.assertEqual(MODULE_ID, "ai_first_dm")
        self.assertEqual(normalize_first_dm_module("ai_first_dm"), AI_FIRST_DM_MODULE)
        self.assertEqual(first_dm_module_label(AI_FIRST_DM_MODULE), "🤖 AI Первый DM")
        self.assertEqual(normalize_first_dm_module("nope"), DEFAULT_FIRST_DM_MODULE)

    def test_validate_rejects_links_and_promises(self) -> None:
        ok, reason = _validate("Смотри https://t.me/xxx", [])
        self.assertFalse(ok)
        ok, reason = _validate("Гарантированная прибыль 100%", [])
        self.assertFalse(ok)
        ok, reason = _validate("Привет, ты сейчас по чату?", [])
        self.assertTrue(ok)

    def test_sync_fallback_without_openai(self) -> None:
        text = choose_ai_generated_first_dm_text_sync()
        self.assertTrue(len(text) >= 8)
        ok, _ = _validate(text, [])
        self.assertTrue(ok)

    async def test_async_falls_back_without_api_key(self) -> None:
        text = await choose_ai_generated_first_dm_text(
            source_chat_title="Test Chat",
            target_first_name="Ivan",
        )
        self.assertTrue(len(text) >= 8)
        self.assertNotIn("http", text.lower())


if __name__ == "__main__":
    unittest.main()
