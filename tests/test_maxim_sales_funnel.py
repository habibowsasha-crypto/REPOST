from __future__ import annotations

import os
import unittest
import warnings
from types import SimpleNamespace

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=ResourceWarning)

os.environ.setdefault("API_ID", "123456")
os.environ.setdefault("API_HASH", "test_hash")
os.environ.setdefault("BOT_TOKEN", "123456:test_token")
os.environ.setdefault("ADMIN_ID_LIST", "123")
os.environ.setdefault("DB_PATH", "/tmp/tgblaster_v114_unittest.db")
os.environ.setdefault("BOT_SESSION_PATH", "/tmp/tgblaster_v114_unittest_bot")
os.environ.setdefault("MEDIA_DIR", "/tmp/tgblaster_v114_unittest_media")
os.environ.setdefault("AI_DM_ENABLED", "true")
os.environ.setdefault("AI_DM_DRY_RUN", "false")
os.environ.setdefault("OPENAI_API_KEY", "")
os.environ.setdefault("AI_REPLY_DELAY_MIN_SECONDS", "0")
os.environ.setdefault("AI_REPLY_DELAY_MAX_SECONDS", "0")
os.environ.setdefault("AI_BURST_DELAY_MIN_SECONDS", "0")
os.environ.setdefault("AI_BURST_DELAY_MAX_SECONDS", "0")
os.environ.setdefault("AI_MAX_FOLLOWUP_MESSAGES", "7")

from config import conn
from services.ai_dialog_service import (
    _get_dialog_by_target,
    create_ai_tables,
    handle_private_incoming,
    record_first_dm,
)
from services.maxim_sales_funnel import PIRATE_VIP_LINK, is_explicit_stop


class FakeClient:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send_message(self, target, text: str):
        self.sent.append(text)
        return SimpleNamespace(id=len(self.sent))


class MaximSalesFunnelTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        create_ai_tables()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM ai_processed_messages")
        cursor.execute("DELETE FROM ai_messages")
        cursor.execute("DELETE FROM ai_dialogs")
        conn.commit()
        cursor.close()
        self.sender = SimpleNamespace(id=7001, username="test_user", first_name="Тест")
        self.client = FakeClient()
        self.message_id = 100

    def open_cycle(self, first_text: str = "Ты сам торгуешь или просто наблюдаешь?") -> None:
        record_first_dm(
            dm_task_id=15,
            account_user_id=9001,
            target=self.sender,
            text=first_text,
            source_chat_title="Crypto Chat",
        )

    async def reply(self, text: str) -> None:
        self.message_id += 1
        await handle_private_incoming(
            dm_task_id=15,
            account_user_id=9001,
            client=self.client,
            sender=self.sender,
            text=text,
            message_id=self.message_id,
        )

    async def test_full_context_funnel_reaches_link(self) -> None:
        self.open_cycle()
        await self.reply("Да, иногда торгую")
        self.assertIn("вип", self.client.sent[-1].lower())

        await self.reply("Да, пробовал пару раз")
        self.assertIn("жалко", self.client.sent[-1].lower())

        before = len(self.client.sent)
        await self.reply("Ну да, заранее не поймешь")
        self.assertEqual(len(self.client.sent) - before, 2)
        self.assertTrue(any("трафер" in message.lower() for message in self.client.sent[before:]))

        await self.reply("Понял")
        self.assertTrue(any(PIRATE_VIP_LINK in message for message in self.client.sent))
        dialog = _get_dialog_by_target(9001, self.sender.id)
        self.assertIsNotNone(dialog)
        self.assertEqual(dialog.status, "completed")

    async def test_benefit_question_gets_transparent_model_and_link(self) -> None:
        self.open_cycle()
        await self.reply("А тебе какая с этого выгода?")
        combined = " ".join(self.client.sent).lower()
        self.assertIn("трафер", combined)
        self.assertIn("50", combined)
        self.assertIn("снг", combined)
        self.assertIn("запад", combined)
        self.assertIn(PIRATE_VIP_LINK, " ".join(self.client.sent))

    async def test_scam_suspicion_is_not_auto_stop(self) -> None:
        self.open_cycle()
        await self.reply("Это наёб какой-то")
        self.assertEqual(len(self.client.sent), 1)
        self.assertIn("тебя никто не заставляет", self.client.sent[0].lower())
        self.assertNotIn(PIRATE_VIP_LINK, self.client.sent[0])
        dialog = _get_dialog_by_target(9001, self.sender.id)
        self.assertEqual(dialog.status, "active")
        self.assertEqual(dialog.stage, "scam_reassured")

        await self.reply("Ну и что там вообще?")
        self.assertTrue(any(PIRATE_VIP_LINK in message for message in self.client.sent))

    async def test_explicit_stop_closes_dialog(self) -> None:
        self.open_cycle()
        await self.reply("Не пиши мне больше")
        self.assertEqual(len(self.client.sent), 1)
        self.assertIn("больше писать не буду", self.client.sent[0].lower())
        dialog = _get_dialog_by_target(9001, self.sender.id)
        self.assertEqual(dialog.status, "closed_negative")

    async def test_payment_question_is_not_false_stop(self) -> None:
        self.open_cycle()
        await self.reply("А платить не надо?")
        dialog = _get_dialog_by_target(9001, self.sender.id)
        self.assertEqual(dialog.status, "active")
        self.assertGreaterEqual(len(self.client.sent), 1)
        self.assertFalse(is_explicit_stop("А платить не надо?", ["не надо"]))

    async def test_vip_first_dm_does_not_repeat_same_question(self) -> None:
        self.open_cycle("Слушай, ты через випки когда-нибудь торговал?")
        await self.reply("Нет, ни разу")
        self.assertIn("жалко", self.client.sent[-1].lower())
        self.assertNotIn("пробовал когда-нибудь", self.client.sent[-1].lower())


if __name__ == "__main__":
    unittest.main()
