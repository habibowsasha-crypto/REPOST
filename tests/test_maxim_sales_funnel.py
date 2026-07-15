from __future__ import annotations

import os
import unittest
import warnings
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=ResourceWarning)

os.environ["API_ID"] = "123456"
os.environ["API_HASH"] = "test_hash"
os.environ["BOT_TOKEN"] = "123456:test_token"
os.environ["ADMIN_ID_LIST"] = "123"
os.environ["DB_PATH"] = "/tmp/tgblaster_v118_unittest.db"
os.environ["BOT_SESSION_PATH"] = "/tmp/tgblaster_v118_unittest_bot"
os.environ["MEDIA_DIR"] = "/tmp/tgblaster_v118_unittest_media"
os.environ["AI_DM_ENABLED"] = "true"
os.environ["AI_DM_DRY_RUN"] = "false"
os.environ["OPENAI_API_KEY"] = ""
os.environ["AI_REPLY_DELAY_MIN_SECONDS"] = "0"
os.environ["AI_REPLY_DELAY_MAX_SECONDS"] = "0"
os.environ["AI_BURST_DELAY_MIN_SECONDS"] = "0"
os.environ["AI_BURST_DELAY_MAX_SECONDS"] = "0"
os.environ["AI_MAX_FOLLOWUP_MESSAGES"] = "7"

from config import conn
from services.ai_dialog_service import (
    _get_dialog_by_target,
    clear_opt_out_dialog_state_by_user,
    create_ai_tables,
    handle_private_incoming,
    record_first_dm,
)
from services.dm_contact_analytics import (
    create_contact_tables,
    record_first_dm as record_contact_first_dm,
)
from services.dm_opt_out import is_opted_out, remove_opt_out
from services.maxim_sales_funnel import (
    PIRATE_VIP_LINK,
    build_local_plan,
    is_explicit_stop,
    generate_post_link_plan,
    is_human_takeover_request,
)
from utils.database.database import create_dm_tables


class FakeClient:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send_message(self, target, text: str):
        self.sent.append(text)
        return SimpleNamespace(id=len(self.sent))


class MaximSalesFunnelTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        create_dm_tables()
        create_ai_tables()
        create_contact_tables()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM dm_opt_out_users")
        cursor.execute("DELETE FROM ai_processed_messages")
        cursor.execute("DELETE FROM ai_messages")
        cursor.execute("DELETE FROM ai_dialogs")
        cursor.execute("DELETE FROM dm_first_dm_claims")
        cursor.execute("DELETE FROM dm_completed_contacts")
        cursor.execute("DELETE FROM dm_contact_sources")
        cursor.execute("DELETE FROM dm_contact_cycles")
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
        self.assertTrue(any("зарплат" in message.lower() for message in self.client.sent[before:]))
        self.assertTrue(any("моментально" in message.lower() for message in self.client.sent[before:]))
        self.assertFalse(any("подборк" in message.lower() for message in self.client.sent[before:]))

        await self.reply("Понял")
        self.assertTrue(any(PIRATE_VIP_LINK in message for message in self.client.sent))
        dialog = _get_dialog_by_target(9001, self.sender.id)
        self.assertIsNotNone(dialog)
        self.assertEqual(dialog.status, "active")
        self.assertEqual(dialog.stage, "post_link_active")

        await self.reply("Хорошо, посмотрю")
        dialog = _get_dialog_by_target(9001, self.sender.id)
        self.assertIsNotNone(dialog)
        self.assertEqual(dialog.status, "completed")

        before_late_message = len(self.client.sent)
        await self.reply("А ещё вопрос")
        self.assertEqual(len(self.client.sent), before_late_message)

    async def test_benefit_question_gets_transparent_model_and_link(self) -> None:
        self.open_cycle()
        await self.reply("А тебе какая с этого выгода?")
        combined = " ".join(self.client.sent).lower()
        self.assertIn("привлекаю людей", combined)
        self.assertIn("зарплату", combined)
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
        self.assertTrue(is_opted_out(self.sender.id))

    async def test_payment_question_is_not_false_stop(self) -> None:
        self.open_cycle()
        await self.reply("А платить не надо?")
        dialog = _get_dialog_by_target(9001, self.sender.id)
        self.assertEqual(dialog.status, "active")
        self.assertGreaterEqual(len(self.client.sent), 1)
        self.assertFalse(is_explicit_stop("А платить не надо?", ["не надо"]))

    async def test_link_explanation_is_simple_specific_and_instant(self) -> None:
        self.open_cycle()
        await self.reply("Да, интересно")
        await self.reply("Не пробовал")
        await self.reply("Ну да")
        await self.reply("Понял")
        combined = " ".join(self.client.sent).lower()
        self.assertIn("бесплатная telegram-группа", combined)
        self.assertIn("платных закрытых vip-каналов", combined)
        self.assertIn("моментально", combined)
        self.assertIn("сотни долларов", combined)
        self.assertNotIn("почти моментально", combined)
        self.assertNotIn("бесплатная подборка", combined)
        self.assertNotIn("торговать проще", combined)

    async def test_manual_optout_removal_allows_future_cycle_only(self) -> None:
        self.open_cycle()
        await self.reply("Не пиши мне больше")
        self.assertTrue(is_opted_out(self.sender.id))
        self.assertTrue(remove_opt_out(self.sender.id))
        self.assertTrue(clear_opt_out_dialog_state_by_user(self.sender.id))
        dialog = _get_dialog_by_target(9001, self.sender.id)
        self.assertEqual(dialog.status, "completed")
        self.open_cycle("Привет, новый цикл")
        dialog = _get_dialog_by_target(9001, self.sender.id)
        self.assertEqual(dialog.status, "active")

    async def test_vip_first_dm_does_not_repeat_same_question(self) -> None:
        self.open_cycle("Слушай, ты через випки когда-нибудь торговал?")
        await self.reply("Нет, ни разу")
        self.assertIn("жалко", self.client.sent[-1].lower())
        self.assertNotIn("пробовал когда-нибудь", self.client.sent[-1].lower())

    async def test_ack_after_explanation_does_not_repeat_offer(self) -> None:
        self.open_cycle()
        await self.reply("Да, торговал")
        await self.reply("Нет, не понравилось")
        await self.reply("Ну да")
        before = len(self.client.sent)
        await self.reply("Понятно")
        new_messages = self.client.sent[before:]
        raw_combined = " ".join(new_messages)
        combined = raw_combined.lower()
        self.assertIn(PIRATE_VIP_LINK, raw_combined)
        self.assertNotIn("6 платных", combined)
        self.assertNotIn("моментально копирует", combined)
        self.assertNotIn("сотни долларов", combined)

    async def test_direct_question_is_answered_before_funnel(self) -> None:
        self.open_cycle()
        await self.reply("Ты откуда меня нашел?")
        self.assertEqual(len(self.client.sent), 1)
        self.assertIn("crypto chat", self.client.sent[0].lower())
        self.assertNotIn(PIRATE_VIP_LINK, self.client.sent[0])

    async def test_bot_question_is_answered_honestly(self) -> None:
        self.open_cycle()
        await self.reply("Ты бот что ли?")
        combined = " ".join(self.client.sent).lower()
        self.assertIn("автомат", combined)
        self.assertNotIn("я не бот", combined)

    async def test_profit_question_has_no_promise(self) -> None:
        self.open_cycle()
        await self.reply("Сколько я там заработаю?")
        combined = " ".join(self.client.sent).lower()
        self.assertIn("не обещаю", combined)
        self.assertNotIn("точно", combined)
        self.assertNotIn("гарант", combined)


    async def test_content_specific_refusals_do_not_create_global_optout(self) -> None:
        false_cases = (
            "Стоп-лосс какой?",
            "Ссылку не надо, расскажи словами",
            "Не пиши пока ссылку",
            "Удали сообщение",
            "Удали",
            "Жалоба куда?",
            "Жалоба",
            "Спам",
            "Пожалуюсь",
            "Не беспокойся, всё нормально",
            "Больше не пиши про это",
            "Мне не интересно покупать, но бесплатно посмотрю",
            "Не надо?",
            "Не интересно?",
        )
        for text in false_cases:
            with self.subTest(text=text):
                self.assertFalse(is_explicit_stop(text, ["стоп", "не пиши", "удали", "не надо"]))

        self.assertTrue(is_explicit_stop("Не пиши мне больше", ["не пиши"]))
        self.assertTrue(
            is_explicit_stop("Не пиши, иначе пожалуюсь", ["не пиши"])
        )
        self.assertTrue(is_explicit_stop("Неинтересно", ["не интересно"]))
        self.assertTrue(is_explicit_stop("Отстань", ["отстань"]))

    async def test_followup_cap_uses_one_final_link_reply(self) -> None:
        history = [
            ("outgoing", "Ты сам торгуешь или просто наблюдаешь?"),
            ("incoming", "Да"),
            ("outgoing", "А платные вип-каналы трейдеров раньше смотрел?"),
            ("incoming", "Нет"),
            (
                "outgoing",
                "Есть бесплатная Telegram-группа. Программа моментально копирует "
                "туда посты из 6 платных закрытых VIP-каналов известных трейдеров.",
            ),
            ("incoming", "Понятно"),
        ]
        plan = build_local_plan(
            stage="offer_explained",
            history=history,
            source_chat_title="Crypto Chat",
            followup_count=6,
            max_followups=7,
        )
        self.assertTrue(plan.close_after)
        self.assertEqual(plan.action, "concise_link")
        self.assertEqual(sum(PIRATE_VIP_LINK in item for item in plan.messages), 1)

    async def test_identity_question_gets_simple_maxim_answer(self) -> None:
        self.open_cycle()
        await self.reply("Ты кто вообще?")
        combined = " ".join(self.client.sent).lower()
        self.assertIn("максим", combined)
        self.assertIn("привлеч", combined)
        self.assertNotIn("увидел твоё сообщение", combined)

    async def test_human_request_and_bot_question_are_not_confused(self) -> None:
        self.assertTrue(is_human_takeover_request("Позови живого человека"))
        self.assertFalse(is_human_takeover_request("Ты живой человек?"))

        self.open_cycle()
        await self.reply("Ты живой человек?")
        dialog = _get_dialog_by_target(9001, self.sender.id)
        self.assertEqual(dialog.status, "active")
        self.assertIn("автомат", " ".join(self.client.sent).lower())

    async def test_explicit_human_request_sets_handoff_status(self) -> None:
        self.open_cycle()
        await self.reply("Позови оператора")
        dialog = _get_dialog_by_target(9001, self.sender.id)
        self.assertEqual(dialog.status, "human_needed")
        self.assertIn("передам человеку", " ".join(self.client.sent).lower())

    async def test_post_link_answers_specific_question_once_without_repeating_link(self) -> None:
        self.open_cycle()
        await self.reply("А тебе какая с этого выгода?")
        self.assertTrue(any(PIRATE_VIP_LINK in message for message in self.client.sent))
        before = len(self.client.sent)
        await self.reply("Там точно бесплатно?")
        final_messages = self.client.sent[before:]
        self.assertEqual(len(final_messages), 1)
        self.assertIn("бесплат", final_messages[0].lower())
        self.assertNotIn(PIRATE_VIP_LINK, final_messages[0])
        dialog = _get_dialog_by_target(9001, self.sender.id)
        self.assertEqual(dialog.status, "completed")


    async def test_explicit_stop_is_persisted_when_ai_is_disabled_and_dialog_missing(self) -> None:
        cycle_id = record_contact_first_dm(
            dm_task_id=15,
            account_user_id=9001,
            target_user_id=self.sender.id,
            source_chat_id=777,
            source_chat_title="Crypto Chat",
        )
        previous = os.environ["AI_DM_ENABLED"]
        os.environ["AI_DM_ENABLED"] = "false"
        try:
            await self.reply("Не пиши мне больше")
        finally:
            os.environ["AI_DM_ENABLED"] = previous

        self.assertTrue(is_opted_out(self.sender.id))
        self.assertEqual(len(self.client.sent), 1)
        self.assertIn("больше писать не буду", self.client.sent[0].lower())
        row = conn.execute(
            "SELECT status FROM dm_contact_cycles WHERE id=?", (cycle_id,)
        ).fetchone()
        self.assertEqual(row[0], "opted_out")

    async def test_post_link_unknown_question_gets_honest_final_answer(self) -> None:
        self.open_cycle()
        await self.reply("А тебе какая с этого выгода?")
        before = len(self.client.sent)
        await self.reply("Как давно эта группа вообще работает?")
        final_messages = self.client.sent[before:]
        self.assertEqual(len(final_messages), 1)
        self.assertIn("придумывать не хочу", final_messages[0].lower())
        self.assertNotIn(PIRATE_VIP_LINK, final_messages[0])
        dialog = _get_dialog_by_target(9001, self.sender.id)
        self.assertEqual(dialog.status, "completed")

    async def test_late_explicit_stop_after_completed_sends_only_one_apology(self) -> None:
        self.open_cycle()
        await self.reply("А тебе какая с этого выгода?")
        await self.reply("Хорошо, посмотрю")
        before = len(self.client.sent)
        await self.reply("Не пиши мне больше")
        self.assertEqual(len(self.client.sent) - before, 1)
        self.assertTrue(is_opted_out(self.sender.id))
        await self.reply("Не пиши")
        self.assertEqual(len(self.client.sent) - before, 1)


    async def test_post_link_openai_path_uses_context_without_network(self) -> None:
        previous_key = os.environ.get("OPENAI_API_KEY", "")
        os.environ["OPENAI_API_KEY"] = "test-only-key"
        mocked = AsyncMock(
            return_value=(
                [
                    "Да, бесплатную группу можно смотреть без оплаты. "
                    "Платный доступ брать не обязан."
                ],
                17,
            )
        )
        history = [
            ("outgoing", "Вот, глянь сам: " + PIRATE_VIP_LINK),
            ("incoming", "Там точно бесплатно?"),
        ]
        try:
            with patch(
                "services.maxim_sales_funnel._openai_generate", mocked
            ):
                plan = await generate_post_link_plan(
                    history=history, source_chat_title="Crypto Chat"
                )
        finally:
            os.environ["OPENAI_API_KEY"] = previous_key

        self.assertEqual(plan.model, "gpt-4o-mini")
        self.assertEqual(plan.tokens_used, 17)
        self.assertEqual(len(plan.messages), 1)
        self.assertIn("бесплат", plan.messages[0].lower())
        self.assertNotIn(PIRATE_VIP_LINK, plan.messages[0])
        mocked.assert_awaited_once()

    async def test_post_link_repeats_link_only_on_explicit_request(self) -> None:
        self.open_cycle()
        await self.reply("А тебе какая с этого выгода?")
        before = len(self.client.sent)
        await self.reply("Ссылка не открывается, скинь ещё раз")
        final_messages = self.client.sent[before:]
        self.assertEqual(sum(PIRATE_VIP_LINK in item for item in final_messages), 1)


if __name__ == "__main__":
    unittest.main()
