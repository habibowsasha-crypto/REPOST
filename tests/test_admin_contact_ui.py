from __future__ import annotations

import os
import unittest
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=ResourceWarning)

os.environ["API_ID"] = "123456"
os.environ["API_HASH"] = "test_hash"
os.environ["BOT_TOKEN"] = "123456:test_token"
os.environ["ADMIN_ID_LIST"] = "123"
os.environ["DB_PATH"] = "/tmp/tgblaster_v118_unittest.db"
os.environ["BOT_SESSION_PATH"] = "/tmp/tgblaster_v118_unittest_bot"
os.environ["MEDIA_DIR"] = "/tmp/tgblaster_v118_unittest_media"
os.environ["OPENAI_API_KEY"] = ""

from config import conn
from handlers.admin.dm_contact_handlers import _chat_buttons, _short_button_title
from services.dm_contact_analytics import create_contact_tables, record_source_seen


class AdminContactUiTests(unittest.TestCase):
    def setUp(self) -> None:
        create_contact_tables()
        with conn:
            conn.execute("DELETE FROM dm_contact_sources")
            conn.execute("DELETE FROM dm_contact_cycles")
            conn.execute("DELETE FROM dm_completed_contacts")

    def test_chat_buttons_accept_four_field_rows_and_paginate(self) -> None:
        for index in range(11):
            record_source_seen(
                account_user_id=1,
                target_user_id=1000 + index,
                source_chat_id=2000 + index,
                source_chat_title=f"Chat {index}",
            )
        buttons, page, pages = _chat_buttons(0)
        self.assertEqual(page, 0)
        self.assertEqual(pages, 2)
        self.assertEqual(len(buttons), 10)  # 8 chats + page row + refresh row

        last_buttons, last_page, last_pages = _chat_buttons(999)
        self.assertEqual(last_page, 1)
        self.assertEqual(last_pages, 2)
        self.assertGreaterEqual(len(last_buttons), 3)

    def test_button_title_is_compact_and_safe_for_layout(self) -> None:
        title = _short_button_title("  Very\nlong   chat   title " + "x" * 80)
        self.assertNotIn("\n", title)
        self.assertLessEqual(len(title), 34)
        self.assertTrue(title.endswith("…"))


if __name__ == "__main__":
    unittest.main()
