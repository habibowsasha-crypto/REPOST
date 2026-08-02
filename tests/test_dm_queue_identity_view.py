from __future__ import annotations

import datetime as dt
import os
import re
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ.setdefault("API_ID", "123456")
os.environ.setdefault("API_HASH", "test_hash")
os.environ.setdefault("BOT_TOKEN", "123456:test_token")
os.environ.setdefault("ADMIN_ID_LIST", "123")
os.environ.setdefault("DB_PATH", "/tmp/tgblaster_dm_queue_identity_test.db")
os.environ.setdefault("BOT_SESSION_PATH", "/tmp/tgblaster_dm_queue_identity_test_bot")
os.environ.setdefault("MEDIA_DIR", "/tmp/tgblaster_dm_queue_identity_test_media")
os.environ.setdefault("OPENAI_API_KEY", "")

from config import conn
from handlers.dm import dm_management_handlers as management
from services.dm_task_queue import enqueue_pending
from utils.database.database import create_dm_tables, create_table


class DmQueueIdentityViewTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        create_table()
        create_dm_tables()
        management._queue_view_snapshots.clear()
        with conn:
            for table in (
                "dm_pending_sources",
                "dm_pending_queue",
                "dm_account_dispatch",
                "dm_watched_chats",
                "dm_sent_log",
                "dm_tasks",
                "sessions",
            ):
                conn.execute(f"DELETE FROM {table}")

    def _create_task(self, task_id: int = 925, account_id: int = 880000001) -> None:
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        with conn:
            conn.execute(
                "INSERT INTO sessions(user_id, session_string) VALUES (?, ?)",
                (account_id, "test-session"),
            )
            conn.execute(
                """
                INSERT INTO dm_tasks(
                    id, admin_id, user_id, session_string, post_text, photo_url,
                    interval_minutes, is_active, created_at, delay_min, delay_max,
                    first_dm_module
                ) VALUES (?, 123, ?, 'test-session', 'Привет', NULL, 0, 1, ?, 0, 0, 'kirill_vip')
                """,
                (task_id, account_id, now),
            )
            conn.execute(
                "INSERT INTO dm_watched_chats(dm_task_id, chat_id) VALUES (?, ?)",
                (task_id, -100777),
            )

    def _enqueue(self, index: int, task_id: int = 925) -> int:
        created, pending_id = enqueue_pending(
            dm_task_id=task_id,
            account_user_id=880000001,
            target_user_id=100000000 + index,
            target_access_hash=700 + index,
            target_username=f"queue_user_{index}",
            target_first_name="Очередной",
            target_last_name="Пользователь",
            source_chat_id=777000,
            source_chat_title="Чат Кирилла",
            source_chat_username="kirill_public_chat",
            source_message_id=5000 + index,
            delay_min=0,
            delay_max=0,
        )
        self.assertTrue(created)
        return pending_id

    def test_queue_command_patterns_accept_supported_forms(self) -> None:
        slash = re.compile(management._DM_QUEUE_COMMAND_PATTERN)
        russian = re.compile(management._DM_QUEUE_RU_PATTERN)
        first = slash.fullmatch("/dm_queue 25")
        self.assertEqual(first.group(1), "25")
        self.assertIsNone(first.group(2))
        second = slash.fullmatch("/dm_queue 25 2")
        self.assertEqual(second.group(1), "25")
        self.assertEqual(second.group(2), "2")
        self.assertEqual(slash.fullmatch("/queue #26 3").group(2), "3")
        self.assertIsNone(slash.fullmatch("/dm_queue").group(1))
        self.assertEqual(russian.fullmatch("очередь 25").group(1), "25")
        self.assertEqual(russian.fullmatch("очередь 25 2").group(2), "2")
        self.assertEqual(russian.fullmatch("КТО В ОЧЕРЕДИ задача #26 3").group(2), "3")
        self.assertIsNone(russian.fullmatch("очередь").group(1))

    def test_page_size_is_twenty_five(self) -> None:
        self.assertEqual(management._QUEUE_PAGE_SIZE, 25)

    def test_target_formatter_preserves_username_and_full_numeric_id(self) -> None:
        rendered = management._format_queue_target_html(
            {
                "target_user_id": 987654321012345,
                "target_username": "very_long_valid_username_12345",
                "target_first_name": "Иван",
                "target_last_name": "Петров",
            }
        )
        self.assertIn("Лид:", rendered)
        self.assertIn("@very_long_valid_username_12345", rendered)
        self.assertIn("Айди:", rendered)
        self.assertIn("<code>987654321012345</code>", rendered)

    def test_target_formatter_marks_missing_username(self) -> None:
        rendered = management._format_queue_target_html(
            {
                "target_user_id": 444555666,
                "target_username": None,
                "target_first_name": "Без",
                "target_last_name": "Юзернейма",
            }
        )
        self.assertIn("без @username", rendered)
        self.assertIn("<code>444555666</code>", rendered)

    def test_source_message_links_support_public_and_private_chats(self) -> None:
        self.assertEqual(
            management._source_message_url(
                {
                    "source_chat_username": "public_chat",
                    "source_chat_id": 123,
                    "source_message_id": 77,
                }
            ),
            "https://t.me/public_chat/77",
        )
        self.assertEqual(
            management._source_message_url(
                {
                    "source_chat_username": None,
                    "source_chat_id": -1001234567890,
                    "source_message_id": 88,
                }
            ),
            "https://t.me/c/1234567890/88",
        )
        self.assertIsNone(
            management._source_message_url(
                {
                    "source_chat_username": None,
                    "source_chat_id": 123,
                    "source_message_id": None,
                }
            )
        )

    async def test_queue_page_shows_parser_card_and_does_not_mutate_queue(self) -> None:
        self._create_task()
        pending_id = self._enqueue(1)
        before = conn.execute(
            "SELECT status, eligible_at FROM dm_pending_queue WHERE id=?",
            (pending_id,),
        ).fetchone()

        fake_event = SimpleNamespace(sender_id=123)
        mocked_render = AsyncMock()
        with patch.object(management, "render_menu", mocked_render):
            shown = await management._show_queue_page(
                fake_event, 925, 0, restart_snapshot=True
            )

        self.assertTrue(shown)
        self.assertEqual(mocked_render.await_count, 1)
        text = mocked_render.await_args.args[1]
        self.assertIn("👤 <b>Лид:</b> @queue_user_1", text)
        self.assertIn("🆔 <b>Айди:</b> <code>100000001</code>", text)
        self.assertIn("Перейти к сообщению", text)
        self.assertIn("https://t.me/kirill_public_chat/5001", text)
        self.assertIn("📨 <b>Из чата:</b> Чат Кирилла", text)

        after = conn.execute(
            "SELECT status, eligible_at FROM dm_pending_queue WHERE id=?",
            (pending_id,),
        ).fetchone()
        self.assertEqual(before, after)

    async def test_callback_queue_page_sends_navigation_as_last_separate_message(self) -> None:
        self._create_task()
        for index in range(1, 31):
            self._enqueue(index)

        fake_event = SimpleNamespace(
            sender_id=123,
            query=object(),
            respond=AsyncMock(),
            edit=AsyncMock(),
        )
        shown = await management._show_queue_page(
            fake_event, 925, 0, restart_snapshot=True
        )

        self.assertTrue(shown)
        self.assertGreaterEqual(fake_event.respond.await_count, 2)
        final_call = fake_event.respond.await_args_list[-1]
        final_text = final_call.args[0]
        final_buttons = final_call.kwargs.get("buttons")
        self.assertIn("Навигация очереди #925", final_text)
        self.assertIn("/dm_queue 925 2", final_text)
        self.assertTrue(final_buttons)
        labels = [
            str(getattr(button, "text", ""))
            for row in final_buttons
            for button in row
        ]
        self.assertIn("Следующие 25 ➡️", labels)
        fake_event.edit.assert_awaited_once()
        self.assertIsNone(fake_event.edit.await_args.kwargs.get("buttons"))

    async def test_direct_second_page_command_target_has_positions_26_to_30(self) -> None:
        self._create_task()
        for index in range(1, 31):
            self._enqueue(index)

        fake_event = SimpleNamespace(sender_id=123)
        mocked_render = AsyncMock()
        with patch.object(management, "render_menu", mocked_render):
            await management._show_queue_page(
                fake_event, 925, 1, restart_snapshot=True
            )

        text = mocked_render.await_args.args[1]
        self.assertIn("Позиции снимка: <b>26–30</b> из <b>30</b>", text)
        for index in range(26, 31):
            self.assertIn(f"@queue_user_{index}", text)
        self.assertNotIn("@queue_user_25\n", text)

    async def test_snapshot_next_page_does_not_skip_when_first_page_is_processed(self) -> None:
        self._create_task()
        pending_ids = [self._enqueue(index) for index in range(1, 31)]
        fake_event = SimpleNamespace(sender_id=123)
        mocked_render = AsyncMock()

        with patch.object(management, "render_menu", mocked_render):
            await management._show_queue_page(
                fake_event, 925, 0, restart_snapshot=True
            )
            with conn:
                conn.execute(
                    "UPDATE dm_pending_queue SET status='sent' WHERE id IN (?,?,?,?,?)",
                    tuple(pending_ids[:5]),
                )
            await management._show_queue_page(fake_event, 925, 1)

        second_text = mocked_render.await_args.args[1]
        # Page two is the original snapshot positions 26–30, not a shifted OFFSET query.
        for index in range(26, 31):
            self.assertIn(f"@queue_user_{index}", second_text)
        self.assertNotIn("@queue_user_25\n", second_text)



    async def test_twenty_five_cards_fit_telegram_visible_text_limit(self) -> None:
        import html as html_module
        self._create_task()
        for index in range(1, 26):
            created, _ = enqueue_pending(
                dm_task_id=925,
                account_user_id=880000001,
                target_user_id=700000000 + index,
                target_access_hash=index,
                target_username="u" * 32,
                target_first_name="Имя" * 20,
                target_last_name="Фамилия" * 20,
                source_chat_id=777000,
                source_chat_title="Название очень длинного исходного чата " + str(index),
                source_chat_username="public_chat_name",
                source_message_id=9000 + index,
                delay_min=0,
                delay_max=0,
            )
            self.assertTrue(created)
        fake_event = SimpleNamespace(sender_id=123)
        mocked_render = AsyncMock()
        with patch.object(management, "render_menu", mocked_render):
            await management._show_queue_page(fake_event, 925, 0, restart_snapshot=True)
        raw = mocked_render.await_args.args[1]
        visible = html_module.unescape(re.sub(r"<[^>]+>", "", raw))
        self.assertLessEqual(len(visible), 4096)
        self.assertEqual(raw.count("👤 <b>Лид:</b>"), 25)

    def test_utf16_split_keeps_every_chunk_below_safe_limit(self) -> None:
        cards = [
            "😀" * 400 + f" карточка {index}"
            for index in range(25)
        ]
        chunks = management._split_html_lines(cards, limit=3500)
        self.assertGreater(len(chunks), 1)
        self.assertEqual(sum(chunk.count("карточка") for chunk in chunks), 25)
        self.assertTrue(all(management._utf16_units(chunk) <= 3500 for chunk in chunks))

    def test_task_specific_source_link_is_not_overwritten_by_another_task(self) -> None:
        self._create_task(task_id=925, account_id=880000001)
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        with conn:
            conn.execute(
                """
                INSERT INTO dm_tasks(
                    id, admin_id, user_id, session_string, post_text, photo_url,
                    interval_minutes, is_active, created_at, delay_min, delay_max,
                    first_dm_module
                ) VALUES (926, 123, 880000001, 'test-session', 'Привет', NULL, 0, 1, ?, 0, 0, 'default')
                """,
                (now,),
            )
        created, pending_id = enqueue_pending(
            dm_task_id=925,
            account_user_id=880000001,
            target_user_id=909090909,
            target_access_hash=1,
            target_username="same_target",
            target_first_name=None,
            target_last_name=None,
            source_chat_id=111,
            source_chat_title="Первый чат",
            source_chat_username="first_chat",
            source_message_id=10,
            delay_min=0,
            delay_max=0,
        )
        self.assertTrue(created)
        created_again, reused_id = enqueue_pending(
            dm_task_id=926,
            account_user_id=880000001,
            target_user_id=909090909,
            target_access_hash=1,
            target_username="same_target",
            target_first_name=None,
            target_last_name=None,
            source_chat_id=222,
            source_chat_title="Второй чат",
            source_chat_username="second_chat",
            source_message_id=20,
            delay_min=0,
            delay_max=0,
        )
        self.assertFalse(created_again)
        self.assertEqual(reused_id, pending_id)
        from services.dm_task_queue import list_queue_rows_by_ids
        row = list_queue_rows_by_ids(925, [pending_id])[0]
        self.assertEqual(row["source_chat_title"], "Первый чат")
        self.assertEqual(row["source_chat_username"], "first_chat")
        self.assertEqual(row["source_message_id"], 10)


if __name__ == "__main__":
    unittest.main()
