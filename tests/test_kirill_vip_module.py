from __future__ import annotations

import os
import datetime as dt
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("API_ID", "123456")
os.environ.setdefault("API_HASH", "test_hash")
os.environ.setdefault("BOT_TOKEN", "123456:test_token")
os.environ.setdefault("ADMIN_ID_LIST", "123")
os.environ.setdefault("DB_PATH", "/tmp/tgblaster_kirill_module_test.db")
os.environ.setdefault("BOT_SESSION_PATH", "/tmp/tgblaster_kirill_module_test_bot")
os.environ.setdefault("MEDIA_DIR", "/tmp/tgblaster_kirill_module_test_media")
os.environ["AI_DM_ENABLED"] = "true"
os.environ["AI_DM_DRY_RUN"] = "false"
os.environ["OPENAI_API_KEY"] = ""
os.environ["AI_REPLY_DELAY_MIN_SECONDS"] = "0"
os.environ["AI_REPLY_DELAY_MAX_SECONDS"] = "0"
os.environ["AI_BURST_DELAY_MIN_SECONDS"] = "0"
os.environ["AI_BURST_DELAY_MAX_SECONDS"] = "0"
os.environ["AI_LINK_HELP_DELAY_MIN_SECONDS"] = "0"
os.environ["AI_LINK_HELP_DELAY_MAX_SECONDS"] = "0"

from config import conn
from handlers.dm import dm_handlers
from services.ai_dialog_service import (
    _get_dialog_by_target,
    create_ai_tables,
    handle_private_incoming,
    record_first_dm,
)
from services.dm_contact_analytics import create_contact_tables
from services.dm_task_queue import enqueue_pending, get_due_pending
from services.first_dm_modules import (
    DEFAULT_FIRST_DM_MODULE,
    KIRILL_VIP_MODULE,
    first_dm_module_label,
    normalize_first_dm_module,
)
from services.first_message_kirill_vip import (
    KIRILL_VIP_TEMPLATES,
    choose_kirill_vip_first_dm_text,
)
from services.maxim_sales_funnel import PIRATE_VIP_LINK
from utils.database.database import create_dm_tables, create_table


class FakeClient:
    def __init__(self) -> None:
        self.sent: list[str] = []

    def is_connected(self) -> bool:
        return True

    async def send_message(self, target, text: str):
        self.sent.append(text)
        return SimpleNamespace(id=len(self.sent))


class KirillVipModuleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        create_table()
        create_dm_tables()
        create_ai_tables()
        create_contact_tables()
        with conn:
            for table in (
                "ai_processed_messages",
                "ai_link_help_usage",
                "ai_messages",
                "ai_dialogs",
                "dm_first_dm_claims",
                "dm_completed_contacts",
                "dm_contact_sources",
                "dm_contact_cycles",
                "dm_pending_sources",
                "dm_pending_queue",
                "dm_account_dispatch",
                "dm_watched_chats",
                "dm_sent_log",
                "dm_tasks",
                "sessions",
            ):
                conn.execute(f"DELETE FROM {table}")
        dm_handlers.dm_monitor_clients.clear()
        dm_handlers.dm_monitor_tasks.clear()
        dm_handlers.dm_account_dispatcher_tasks.clear()
        self.sender = SimpleNamespace(
            id=88001, username="kirill_test", first_name="Тест"
        )
        self.client = FakeClient()
        self.message_id = 900


    def tearDown(self) -> None:
        dm_handlers.dm_monitor_clients.clear()
        dm_handlers.dm_monitor_tasks.clear()
        for task in list(dm_handlers.dm_account_dispatcher_tasks.values()):
            task.cancel()
        dm_handlers.dm_account_dispatcher_tasks.clear()

    def create_task_and_pending(self, *, module: str, account: int = 99001, target: int = 88001) -> int:
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        with conn:
            conn.execute(
                "INSERT INTO sessions(user_id, session_string) VALUES (?, ?)",
                (account, f"session-{account}"),
            )
            conn.execute(
                """
                INSERT INTO dm_tasks(
                    id, admin_id, user_id, session_string, post_text, photo_url,
                    interval_minutes, is_active, created_at, delay_min, delay_max,
                    first_dm_module
                ) VALUES (77, 1, ?, ?, 'Старый текст', NULL, 0, 1, ?, 0, 0, ?)
                """,
                (account, f"session-{account}", now, module),
            )
            conn.execute(
                "INSERT INTO dm_watched_chats(dm_task_id, chat_id) VALUES (77, 70077)"
            )
        created, pending_id = enqueue_pending(
            dm_task_id=77,
            account_user_id=account,
            target_user_id=target,
            target_access_hash=123456789,
            target_username="kirill_test",
            target_first_name="Тест",
            target_last_name="Юзер",
            source_chat_id=70077,
            source_chat_title="Чат Кирилла",
            delay_min=0,
            delay_max=0,
        )
        self.assertTrue(created)
        dm_handlers.dm_monitor_clients[77] = self.client
        return pending_id

    def open_kirill_cycle(self, first_text: str = "Слушай, у тебя VIP Кирилла есть?") -> None:
        record_first_dm(
            dm_task_id=77,
            account_user_id=99001,
            target=self.sender,
            text=first_text,
            source_chat_title="Чат Кирилла",
            dialog_module=KIRILL_VIP_MODULE,
        )

    async def reply(self, text: str) -> None:
        self.message_id += 1
        await handle_private_incoming(
            dm_task_id=77,
            account_user_id=99001,
            client=self.client,
            sender=self.sender,
            text=text,
            message_id=self.message_id,
        )

    def test_module_registry_and_templates(self) -> None:
        self.assertEqual(normalize_first_dm_module("kirill_vip"), KIRILL_VIP_MODULE)
        self.assertEqual(normalize_first_dm_module("unknown"), DEFAULT_FIRST_DM_MODULE)
        self.assertIn("Кирилла", first_dm_module_label(KIRILL_VIP_MODULE))
        self.assertGreaterEqual(len(KIRILL_VIP_TEMPLATES), 20)
        self.assertEqual(len(KIRILL_VIP_TEMPLATES), len(set(KIRILL_VIP_TEMPLATES)))
        selected = choose_kirill_vip_first_dm_text()
        self.assertIn(selected, KIRILL_VIP_TEMPLATES)
        self.assertTrue(all("Кирилл" in item for item in KIRILL_VIP_TEMPLATES))

    def test_existing_tasks_default_to_original_module(self) -> None:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(dm_tasks)")}
        self.assertIn("first_dm_module", columns)
        with conn:
            conn.execute(
                """
                INSERT INTO dm_tasks(
                    admin_id, user_id, session_string, post_text, interval_minutes,
                    is_active, created_at, delay_min, delay_max
                ) VALUES (1, 2, 'session', 'Привет', 0, 0, 'now', 30, 60)
                """
            )
        value = conn.execute(
            "SELECT first_dm_module FROM dm_tasks ORDER BY id DESC LIMIT 1"
        ).fetchone()[0]
        self.assertEqual(value, DEFAULT_FIRST_DM_MODULE)


    async def test_dispatcher_uses_kirill_selector_only_for_kirill_task(self) -> None:
        self.create_task_and_pending(module=KIRILL_VIP_MODULE)
        row = get_due_pending(99001)
        self.assertIsNotNone(row)
        with patch.object(dm_handlers, "choose_first_dm_text", side_effect=AssertionError("default selector used")), \
             patch.object(dm_handlers, "choose_kirill_vip_first_dm_text", return_value="У тебя VIP Кирилла есть?"):
            result = await dm_handlers._send_pending_row(row)
        self.assertEqual(result, "sent")
        self.assertEqual(self.client.sent[0], "У тебя VIP Кирилла есть?")
        dialog = _get_dialog_by_target(99001, 88001)
        self.assertEqual(dialog.dialog_module, KIRILL_VIP_MODULE)

    async def test_dispatcher_preserves_original_selector_for_default_task(self) -> None:
        self.create_task_and_pending(module=DEFAULT_FIRST_DM_MODULE)
        row = get_due_pending(99001)
        self.assertIsNotNone(row)
        with patch.object(dm_handlers, "choose_first_dm_text", return_value="Оригинальный первый DM"), \
             patch.object(dm_handlers, "choose_kirill_vip_first_dm_text", side_effect=AssertionError("Kirill selector used")):
            result = await dm_handlers._send_pending_row(row)
        self.assertEqual(result, "sent")
        self.assertEqual(self.client.sent[0], "Оригинальный первый DM")
        dialog = _get_dialog_by_target(99001, 88001)
        self.assertEqual(dialog.dialog_module, DEFAULT_FIRST_DM_MODULE)

    async def test_yes_branch_sends_kirill_offer_and_link_immediately(self) -> None:
        self.open_kirill_cycle()
        dialog = _get_dialog_by_target(99001, self.sender.id)
        self.assertEqual(dialog.dialog_module, KIRILL_VIP_MODULE)

        await self.reply("Да, есть")
        combined = " ".join(self.client.sent)
        lowered = combined.lower()
        self.assertIn("в чате кирилла", lowered)
        self.assertIn("дорого обошлась", lowered)
        self.assertIn("отдельно покупать", lowered)
        self.assertIn("софт моментально копирует", lowered)
        self.assertIn("vip кирилла", lowered)
        self.assertIn(PIRATE_VIP_LINK, combined)
        self.assertTrue(any("Заблокировать / Добавить" in item for item in self.client.sent))

        dialog = _get_dialog_by_target(99001, self.sender.id)
        self.assertEqual(dialog.status, "active")
        self.assertEqual(dialog.stage, "post_link_active")

    async def test_no_branch_sends_free_alternative_and_link(self) -> None:
        self.open_kirill_cycle("Ты VIPку Кирилла покупал?")
        await self.reply("Нет, не покупал")
        combined = " ".join(self.client.sent)
        lowered = combined.lower()
        self.assertIn("в чате кирилла", lowered)
        self.assertIn("покупать его vip не нужно", lowered)
        self.assertIn("бесплатный telegram-канал", lowered)
        self.assertIn(PIRATE_VIP_LINK, combined)

    async def test_post_link_official_question_is_answered_honestly(self) -> None:
        self.open_kirill_cycle()
        await self.reply("Да, есть")
        before = len(self.client.sent)
        await self.reply("Это официальный VIP Кирилла?")
        final = self.client.sent[before:]
        self.assertEqual(len(final), 1)
        self.assertIn("не официальный доступ", final[0].lower())
        self.assertIn("копирует", final[0].lower())
        dialog = _get_dialog_by_target(99001, self.sender.id)
        self.assertEqual(dialog.status, "completed")

    async def test_soft_decline_does_not_send_link(self) -> None:
        self.open_kirill_cycle()
        await self.reply("Нет, спасибо")
        self.assertEqual(self.client.sent, ["Понял, без проблем. Не буду навязывать."])
        dialog = _get_dialog_by_target(99001, self.sender.id)
        self.assertEqual(dialog.status, "completed")


if __name__ == "__main__":
    unittest.main()
