from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("API_ID", "123456")
os.environ.setdefault("API_HASH", "test_hash")
os.environ.setdefault("BOT_TOKEN", "123456:test_token")
os.environ.setdefault("ADMIN_ID_LIST", "123")
os.environ.setdefault("DB_PATH", "/tmp/tgblaster_ai_quick_offer_test.db")
os.environ.setdefault("BOT_SESSION_PATH", "/tmp/tgblaster_ai_quick_offer_test_bot")
os.environ.setdefault("MEDIA_DIR", "/tmp/tgblaster_ai_quick_offer_test_media")
os.environ["AI_DM_ENABLED"] = "true"
os.environ["AI_DM_DRY_RUN"] = "false"
os.environ["AI_REPLY_DELAY_MIN_SECONDS"] = "0"
os.environ["AI_REPLY_DELAY_MAX_SECONDS"] = "0"
os.environ["AI_BURST_DELAY_MIN_SECONDS"] = "0"
os.environ["AI_BURST_DELAY_MAX_SECONDS"] = "0"

from config import conn
from services.ai_dialog_service import (
    _get_dialog_by_target,
    _recent_quick_offer_series,
    create_ai_tables,
    handle_private_incoming,
    record_first_dm,
)
from services.ai_quick_offer import (
    QuickOfferGenerationError,
    QuickOfferPlan,
    _parse_messages,
    build_local_quick_offer_plan,
    generate_quick_offer_plan,
    series_similarity,
    validate_quick_offer,
)
from services.dm_contact_analytics import create_contact_tables
from services.dm_opt_out import is_opted_out
from services.first_dm_modules import (
    AI_QUICK_OFFER_MODULE,
    DEFAULT_FIRST_DM_MODULE,
    first_dm_module_label,
    normalize_first_dm_module,
)
from services.first_message_ai_quick_offer import (
    AI_QUICK_OFFER_FIRST_DM_TEMPLATES,
    choose_ai_quick_offer_first_dm_text,
)
from services.maxim_sales_funnel import PIRATE_VIP_LINK
from utils.database.database import create_dm_tables


SERIES = [
    "Понял тебя. Тогда коротко покажу одну штуку.",
    "Есть бесплатный Telegram-канал: софт автоматически копирует туда новые публикации из платных закрытых VIP-каналов трейдеров.",
    f"Можешь просто посмотреть, подписываться необязательно: {PIRATE_VIP_LINK}",
    "Если ссылка не нажимается, закрой крестиком плашку «Заблокировать / Добавить». Если не поможет, скопируй ссылку и вставь её в Telegram.",
]


class FakeClient:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send_message(self, target, text: str):
        self.sent.append(text)
        return SimpleNamespace(id=len(self.sent))


class InterruptingClient(FakeClient):
    async def send_message(self, target, text: str):
        result = await super().send_message(target, text)
        if len(self.sent) == 1:
            with conn:
                conn.execute(
                    """
                    INSERT INTO ai_processed_messages
                        (account_user_id, target_user_id, telegram_message_id, processed_at)
                    VALUES (99101, 98101, 99999, 'now')
                    """
                )
        return result


class AiQuickOfferTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        create_dm_tables()
        create_ai_tables()
        create_contact_tables()
        with conn:
            for table in (
                "dm_opt_out_users",
                "ai_quick_offer_usage",
                "ai_processed_messages",
                "ai_link_help_usage",
                "ai_messages",
                "ai_dialogs",
                "dm_first_dm_claims",
                "dm_completed_contacts",
                "dm_contact_sources",
                "dm_contact_cycles",
            ):
                conn.execute(f"DELETE FROM {table}")
        self.sender = SimpleNamespace(
            id=98101, username="quick_test", first_name="Тест"
        )
        self.client = FakeClient()

    def open_cycle(self) -> None:
        record_first_dm(
            dm_task_id=91,
            account_user_id=99101,
            target=self.sender,
            text="Привет, можно короткий вопрос?",
            source_chat_title="Crypto Chat",
            dialog_module=AI_QUICK_OFFER_MODULE,
        )

    async def reply(self, text: str, message_id: int = 1) -> None:
        await handle_private_incoming(
            dm_task_id=91,
            account_user_id=99101,
            client=self.client,
            sender=self.sender,
            text=text,
            message_id=message_id,
        )

    def test_registry_and_first_dm_pool(self) -> None:
        self.assertEqual(
            normalize_first_dm_module(AI_QUICK_OFFER_MODULE),
            AI_QUICK_OFFER_MODULE,
        )
        self.assertEqual(normalize_first_dm_module("unknown"), DEFAULT_FIRST_DM_MODULE)
        self.assertIn("AI Быстрый оффер", first_dm_module_label(AI_QUICK_OFFER_MODULE))
        self.assertGreaterEqual(len(AI_QUICK_OFFER_FIRST_DM_TEMPLATES), 20)
        self.assertIn(
            choose_ai_quick_offer_first_dm_text(),
            AI_QUICK_OFFER_FIRST_DM_TEMPLATES,
        )

    def test_validator_requires_safe_complete_series(self) -> None:
        self.assertEqual(validate_quick_offer(SERIES, []), (True, "ok"))
        duplicate_ok, duplicate_reason = validate_quick_offer(SERIES, ["\n".join(SERIES)])
        self.assertFalse(duplicate_ok)
        self.assertIn("повтор", duplicate_reason)
        unsafe = list(SERIES)
        unsafe[1] += " Тут гарантирован высокий винрейт."
        self.assertFalse(validate_quick_offer(unsafe, [])[0])
        self.assertGreater(series_similarity("\n".join(SERIES), "\n".join(SERIES)), 0.99)

    async def test_first_reply_sends_one_ai_series_and_no_extra_help(self) -> None:
        self.open_cycle()
        generated = QuickOfferPlan(list(SERIES), tokens_used=321, model="test-model")
        with patch(
            "services.ai_dialog_service.generate_quick_offer_plan",
            return_value=generated,
        ) as mocked:
            await self.reply("Да, сам иногда торгую")

        mocked.assert_awaited_once()
        self.assertEqual(self.client.sent, SERIES)
        self.assertEqual(sum(PIRATE_VIP_LINK in item for item in self.client.sent), 1)
        self.assertEqual(len(_recent_quick_offer_series()), 1)
        dialog = _get_dialog_by_target(99101, self.sender.id)
        self.assertEqual(dialog.status, "active")
        self.assertEqual(dialog.stage, "post_link_active")

    async def test_polite_decline_does_not_generate_or_send_link(self) -> None:
        self.open_cycle()
        with patch(
            "services.ai_dialog_service.generate_quick_offer_plan",
            side_effect=AssertionError("AI must not run after a polite refusal"),
        ):
            await self.reply("Нет, спасибо")
        self.assertEqual(self.client.sent, ["Понял, без проблем. Не буду навязывать."])
        self.assertFalse(any(PIRATE_VIP_LINK in item for item in self.client.sent))
        dialog = _get_dialog_by_target(99101, self.sender.id)
        self.assertEqual(dialog.status, "completed")

    async def test_explicit_stop_keeps_permanent_opt_out(self) -> None:
        self.open_cycle()
        with patch(
            "services.ai_dialog_service.generate_quick_offer_plan",
            side_effect=AssertionError("AI must not run after explicit stop"),
        ):
            await self.reply("Больше не пиши")
        self.assertTrue(is_opted_out(self.sender.id))
        self.assertEqual(
            self.client.sent,
            ["Понял, извини, что побеспокоил. Больше писать не буду."],
        )
        dialog = _get_dialog_by_target(99101, self.sender.id)
        self.assertEqual(dialog.status, "closed_negative")

    async def test_generation_failure_uses_validated_local_fallback(self) -> None:
        self.open_cycle()
        with patch(
            "services.ai_dialog_service.generate_quick_offer_plan",
            side_effect=QuickOfferGenerationError("temporary failure"),
        ):
            await self.reply("Да", message_id=2)
        self.assertEqual(len(self.client.sent), 4)
        self.assertEqual(sum(PIRATE_VIP_LINK in item for item in self.client.sent), 1)
        self.assertEqual(validate_quick_offer(self.client.sent, []), (True, "ok"))
        dialog = _get_dialog_by_target(99101, self.sender.id)
        self.assertEqual(dialog.status, "active")
        self.assertEqual(dialog.stage, "post_link_active")

    async def test_new_reply_interrupts_remaining_series(self) -> None:
        self.client = InterruptingClient()
        self.open_cycle()
        generated = QuickOfferPlan(list(SERIES), tokens_used=50, model="test-model")
        with patch(
            "services.ai_dialog_service.generate_quick_offer_plan",
            return_value=generated,
        ):
            await self.reply("Да, расскажи", message_id=3)
        self.assertEqual(self.client.sent, SERIES[:1])
        dialog = _get_dialog_by_target(99101, self.sender.id)
        self.assertEqual(dialog.status, "active")
        self.assertEqual(dialog.stage, "quick_offer_interrupted")

    async def test_generator_without_api_key_uses_local_fallback(self) -> None:
        with patch.dict(os.environ, {"OPENAI_API_KEY": ""}):
            plan = await generate_quick_offer_plan(
                history=[("incoming", "Да")],
                source_chat_title="Crypto Chat",
                recent_series=[],
            )
        self.assertEqual(plan.model, "local_safe_fallback")
        self.assertEqual(validate_quick_offer(plan.messages, []), (True, "ok"))

    def test_parser_keeps_wrapped_message_lines(self) -> None:
        parsed = _parse_messages(
            "MESSAGE_1: Первая строка\n"
            "продолжение первой строки\n"
            "MESSAGE_2: Второе сообщение\n"
            "MESSAGE_3: Третье сообщение"
        )
        self.assertEqual(
            parsed,
            [
                "Первая строка продолжение первой строки",
                "Второе сообщение",
                "Третье сообщение",
            ],
        )

    def test_validator_rejects_duplicate_inside_one_series(self) -> None:
        duplicate = list(SERIES)
        duplicate[1] = duplicate[0]
        ok, reason = validate_quick_offer(duplicate, [])
        self.assertFalse(ok)
        self.assertIn("внутри одной серии", reason)

    async def test_invalid_openai_output_is_replaced_with_local_series(self) -> None:
        invalid = [
            "Понял.",
            "Есть какой-то канал.",
            f"Посмотри: {PIRATE_VIP_LINK}",
        ]
        with patch.dict(os.environ, {"OPENAI_API_KEY": "sk-test"}), patch(
            "services.ai_quick_offer._generate_once",
            return_value=(invalid, 10),
        ) as mocked:
            plan = await generate_quick_offer_plan(
                history=[("incoming", "Что?")],
                source_chat_title="Crypto Chat",
                recent_series=[],
            )
        self.assertEqual(mocked.await_count, 4)
        self.assertEqual(plan.model, "local_safe_fallback")
        self.assertEqual(validate_quick_offer(plan.messages, []), (True, "ok"))

    def test_local_fallback_avoids_recent_rolling_window(self) -> None:
        recent: list[str] = []
        for _ in range(32):
            plan = build_local_quick_offer_plan(
                history=[("incoming", "Что это?")],
                recent_series=recent,
            )
            self.assertEqual(validate_quick_offer(plan.messages, recent), (True, "ok"))
            recent.append("\n".join(plan.messages))
            recent = recent[-30:]


if __name__ == "__main__":
    unittest.main()
