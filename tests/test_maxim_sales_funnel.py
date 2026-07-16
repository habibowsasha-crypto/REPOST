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
os.environ["DB_PATH"] = "/tmp/tgblaster_v120_unittest.db"
os.environ["BOT_SESSION_PATH"] = "/tmp/tgblaster_v120_unittest_bot"
os.environ["MEDIA_DIR"] = "/tmp/tgblaster_v120_unittest_media"
os.environ["AI_DM_ENABLED"] = "true"
os.environ["AI_DM_DRY_RUN"] = "false"
os.environ["OPENAI_API_KEY"] = ""
os.environ["AI_REPLY_DELAY_MIN_SECONDS"] = "0"
os.environ["AI_REPLY_DELAY_MAX_SECONDS"] = "0"
os.environ["AI_BURST_DELAY_MIN_SECONDS"] = "0"
os.environ["AI_BURST_DELAY_MAX_SECONDS"] = "0"
os.environ["AI_LINK_HELP_DELAY_MIN_SECONDS"] = "0"
os.environ["AI_LINK_HELP_DELAY_MAX_SECONDS"] = "0"
os.environ["AI_MAX_FOLLOWUP_MESSAGES"] = "7"

from config import conn
from services.ai_dialog_service import (
    _get_dialog_by_target,
    _select_link_help_variant,
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
    LINK_ACCESS_HELP_VARIANTS,
    build_local_plan,
    classify_intent,
    is_emoji_only_reaction,
    is_explicit_stop,
    is_soft_decline,
    generate_post_link_plan,
    is_human_takeover_request,
    make_media_reaction_text,
    validate_link_access_help,
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
        cursor.execute("DELETE FROM ai_link_help_usage")
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

    async def reply_media(self, media_kind: str) -> None:
        self.message_id += 1
        await handle_private_incoming(
            dm_task_id=15,
            account_user_id=9001,
            client=self.client,
            sender=self.sender,
            text="",
            message_id=self.message_id,
            media_kind=media_kind,
        )

    async def test_full_context_funnel_reaches_link(self) -> None:
        self.open_cycle()
        await self.reply("Да, иногда торгую")
        self.assertIn("вип", self.client.sent[-1].lower())

        await self.reply("Да, пробовал пару раз")
        self.assertIn("жалко", self.client.sent[-1].lower())

        before = len(self.client.sent)
        await self.reply("Ну да, заранее не поймешь")
        self.assertEqual(len(self.client.sent) - before, 1)
        self.assertTrue(any("моментально" in message.lower() for message in self.client.sent[before:]))
        self.assertTrue(any("софт" in message.lower() for message in self.client.sent[before:]))
        self.assertTrue(any("сливами vip-каналов" in message.lower() for message in self.client.sent[before:]))
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

    async def test_media_only_reply_advances_from_vip_first_dm(self) -> None:
        self.open_cycle("Привет. А ты знаешь, что такое VIP-каналы в трейдинге?")
        await self.reply_media("gif")
        combined = " ".join(self.client.sent).lower()
        self.assertIn("бесплатный telegram-канал", combined)
        self.assertIn("софт моментально копирует", combined)
        self.assertNotIn("вижу", combined)
        dialog = _get_dialog_by_target(9001, self.sender.id)
        self.assertEqual(dialog.status, "active")
        self.assertEqual(dialog.stage, "offer_explained")

    async def test_media_only_reply_advances_to_link_after_explanation(self) -> None:
        self.open_cycle("Ты сам торгуешь или просто наблюдаешь?")
        await self.reply("Да, сам")
        await self.reply_media("sticker")
        self.assertTrue(any("бесплатный Telegram-канал" in item for item in self.client.sent))
        before = len(self.client.sent)
        await self.reply_media("photo")
        new_messages = self.client.sent[before:]
        self.assertTrue(any(PIRATE_VIP_LINK in item for item in new_messages))
        dialog = _get_dialog_by_target(9001, self.sender.id)
        self.assertEqual(dialog.stage, "post_link_active")

    async def test_media_only_reply_after_link_gets_short_final_reply(self) -> None:
        self.open_cycle("Привет. А ты знаешь, что такое VIP-каналы в трейдинге?")
        await self.reply_media("gif")
        await self.reply("Понятно")
        self.assertTrue(any(PIRATE_VIP_LINK in item for item in self.client.sent))
        before = len(self.client.sent)
        await self.reply_media("voice")
        final_messages = self.client.sent[before:]
        self.assertEqual(len(final_messages), 1)
        self.assertIn("сам глянь", final_messages[0].lower())
        self.assertNotIn(PIRATE_VIP_LINK, final_messages[0])
        self.assertNotIn("голос", final_messages[0].lower())
        dialog = _get_dialog_by_target(9001, self.sender.id)
        self.assertEqual(dialog.status, "completed")

    async def test_emoji_only_reply_is_a_reaction_and_advances(self) -> None:
        self.assertTrue(is_emoji_only_reaction("😂🔥"))
        self.assertEqual(classify_intent("😂🔥"), "reaction")
        self.assertFalse(is_emoji_only_reaction("...?"))
        self.open_cycle("Привет. А ты знаешь, что такое VIP-каналы в трейдинге?")
        await self.reply("😂")
        self.assertTrue(any("бесплатный telegram-канал" in item.lower() for item in self.client.sent))

    async def test_media_marker_is_internal_reaction(self) -> None:
        marker = make_media_reaction_text("video")
        self.assertEqual(marker, "[[media_reaction:video]]")
        self.assertEqual(classify_intent(marker), "reaction")

    async def test_empty_text_without_media_is_still_ignored(self) -> None:
        self.open_cycle()
        before = len(self.client.sent)
        self.message_id += 1
        await handle_private_incoming(
            dm_task_id=15,
            account_user_id=9001,
            client=self.client,
            sender=self.sender,
            text="",
            message_id=self.message_id,
        )
        self.assertEqual(len(self.client.sent), before)

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
        self.assertIn("бесплатный telegram-канал", combined)
        self.assertIn("сливами vip-каналов", combined)
        self.assertIn("софт моментально копирует", combined)
        self.assertIn("платных закрытых vip-каналов", combined)
        self.assertIn("моментально", combined)
        self.assertNotIn("почти моментально", combined)
        self.assertNotIn("бесплатная подборка", combined)
        self.assertNotIn("бесплатная telegram-группа", combined)
        self.assertNotIn("бесплатный чат", combined)
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
                    "Да, бесплатный канал можно смотреть без оплаты. "
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

    async def test_post_link_not_clickable_explains_telegram_contact_banner(self) -> None:
        self.open_cycle()
        await self.reply("А тебе какая с этого выгода?")
        self.assertTrue(any(PIRATE_VIP_LINK in message for message in self.client.sent))

        before = len(self.client.sent)
        await self.reply("Перейти не могу, нажимаю по ссылке — не кликается")
        final_messages = self.client.sent[before:]

        self.assertEqual(len(final_messages), 1)
        answer = final_messages[0].lower()
        self.assertIn("заблокировать", answer)
        self.assertIn("добавить", answer)
        self.assertIn("крестик", answer)
        self.assertIn("скопируй", answer)
        self.assertIn("telegram", answer)
        self.assertNotIn("проблема в приложении", answer)
        self.assertNotIn("устройстве", answer)
        self.assertNotIn(PIRATE_VIP_LINK, final_messages[0])

        dialog = _get_dialog_by_target(9001, self.sender.id)
        self.assertEqual(dialog.status, "completed")

    async def test_post_link_vague_openai_access_answer_falls_back_to_exact_help(self) -> None:
        previous_key = os.environ.get("OPENAI_API_KEY", "")
        os.environ["OPENAI_API_KEY"] = "test-only-key"
        mocked = AsyncMock(
            side_effect=[
                (["Возможно, проблема в приложении или устройстве. Попробуй позже."], 11),
                (["Попробуй скопировать ссылку в браузер."], 9),
            ]
        )
        history = [
            ("outgoing", "Вот, глянь сам: " + PIRATE_VIP_LINK),
            ("incoming", "Не могу перейти, ссылка не нажимается"),
        ]
        try:
            with patch("services.maxim_sales_funnel._openai_generate", mocked):
                plan = await generate_post_link_plan(
                    history=history, source_chat_title="Crypto Chat"
                )
        finally:
            os.environ["OPENAI_API_KEY"] = previous_key

        self.assertEqual(plan.model, "local_post_link_final")
        self.assertEqual(plan.tokens_used, 0)
        self.assertEqual(len(plan.messages), 1)
        answer = plan.messages[0].lower()
        self.assertIn("заблокировать", answer)
        self.assertIn("добавить", answer)
        self.assertIn("крестик", answer)
        self.assertIn("скопируй", answer)
        self.assertNotIn(PIRATE_VIP_LINK, plan.messages[0])
        self.assertEqual(mocked.await_count, 0)

    async def test_post_link_repeats_link_only_on_explicit_request(self) -> None:
        self.open_cycle()
        await self.reply("А тебе какая с этого выгода?")
        before = len(self.client.sent)
        await self.reply("Ссылка не открывается, скинь ещё раз")
        final_messages = self.client.sent[before:]
        self.assertEqual(sum(PIRATE_VIP_LINK in item for item in final_messages), 1)


    async def test_every_initial_link_gets_an_immediate_varied_help_message(self) -> None:
        self.open_cycle()
        await self.reply("А тебе какая с этого выгода?")

        link_positions = [
            index for index, message in enumerate(self.client.sent)
            if PIRATE_VIP_LINK in message
        ]
        self.assertEqual(len(link_positions), 1)
        link_index = link_positions[0]
        self.assertLess(link_index + 1, len(self.client.sent))
        help_message = self.client.sent[link_index + 1]
        self.assertTrue(validate_link_access_help(help_message))
        self.assertNotIn(PIRATE_VIP_LINK, help_message)

        row = conn.execute(
            """
            SELECT variant_index FROM ai_link_help_usage
            WHERE account_user_id = ? AND target_user_id = ?
            ORDER BY id DESC LIMIT 1
            """,
            (9001, self.sender.id),
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(help_message, LINK_ACCESS_HELP_VARIANTS[int(row[0])])

    async def test_link_help_pool_has_more_than_ten_unique_safe_variants(self) -> None:
        self.assertGreater(len(LINK_ACCESS_HELP_VARIANTS), 10)
        self.assertEqual(len(LINK_ACCESS_HELP_VARIANTS), len(set(LINK_ACCESS_HELP_VARIANTS)))
        for message in LINK_ACCESS_HELP_VARIANTS:
            with self.subTest(message=message):
                self.assertTrue(validate_link_access_help(message))
                self.assertNotIn(PIRATE_VIP_LINK, message)

    async def test_link_help_rotation_avoids_recent_variants_persistently(self) -> None:
        self.open_cycle()
        dialog = _get_dialog_by_target(9001, self.sender.id)
        self.assertIsNotNone(dialog)

        chosen: list[int] = []
        for _ in range(13):
            variant_index, message = _select_link_help_variant(dialog)
            chosen.append(variant_index)
            self.assertEqual(message, LINK_ACCESS_HELP_VARIANTS[variant_index])

        self.assertEqual(len(chosen), len(set(chosen)))
        persisted = conn.execute(
            """
            SELECT variant_index FROM ai_link_help_usage
            WHERE account_user_id = ?
            ORDER BY id ASC
            """,
            (9001,),
        ).fetchall()
        self.assertEqual([int(row[0]) for row in persisted], chosen)

    async def test_explicit_link_resend_gets_a_new_help_variant(self) -> None:
        self.open_cycle()
        await self.reply("А тебе какая с этого выгода?")
        first_help_row = conn.execute(
            """
            SELECT variant_index FROM ai_link_help_usage
            WHERE account_user_id = ?
            ORDER BY id DESC LIMIT 1
            """,
            (9001,),
        ).fetchone()
        self.assertIsNotNone(first_help_row)

        before = len(self.client.sent)
        await self.reply("Скинь ссылку ещё раз")
        resent = self.client.sent[before:]
        self.assertEqual(sum(PIRATE_VIP_LINK in item for item in resent), 1)
        resent_link_index = next(
            index for index, message in enumerate(resent) if PIRATE_VIP_LINK in message
        )
        self.assertLess(resent_link_index + 1, len(resent))
        self.assertTrue(validate_link_access_help(resent[resent_link_index + 1]))

        second_help_row = conn.execute(
            """
            SELECT variant_index FROM ai_link_help_usage
            WHERE account_user_id = ?
            ORDER BY id DESC LIMIT 1
            """,
            (9001,),
        ).fetchone()
        self.assertIsNotNone(second_help_row)
        self.assertNotEqual(int(first_help_row[0]), int(second_help_row[0]))


    async def test_link_help_failure_does_not_turn_delivered_link_into_send_error(self) -> None:
        class LinkDeliveredHelpFailsClient(FakeClient):
            async def send_message(self, target, text: str):
                if validate_link_access_help(text):
                    raise RuntimeError("simulated optional help failure")
                return await super().send_message(target, text)

        self.client = LinkDeliveredHelpFailsClient()
        self.open_cycle()
        await self.reply("А тебе какая с этого выгода?")

        self.assertTrue(any(PIRATE_VIP_LINK in message for message in self.client.sent))
        dialog = _get_dialog_by_target(9001, self.sender.id)
        self.assertIsNotNone(dialog)
        self.assertEqual(dialog.status, "active")
        self.assertEqual(dialog.stage, "post_link_active")

    async def test_plain_language_reexplanation_matches_screenshot_case(self) -> None:
        self.open_cycle("Как ты к этому относишься?")
        await self.reply("Я не понимаю о чем ты, слишком сложно")
        self.assertEqual(len(self.client.sent), 1)
        answer = self.client.sent[0].lower()
        self.assertIn("бесплатный telegram-канал", answer)
        self.assertIn("сливами vip-каналов", answer)
        self.assertIn("софт моментально копирует", answer)
        self.assertIn("6 платных закрытых vip-каналов", answer)
        self.assertIn("каждый доступ", answer)
        self.assertNotIn("бесплатная группа", answer)
        self.assertNotIn(PIRATE_VIP_LINK, self.client.sent[0])

    async def test_exact_ne_perehodit_uses_deterministic_banner_help(self) -> None:
        self.open_cycle()
        await self.reply("А тебе какая с этого выгода?")
        before = len(self.client.sent)
        await self.reply("Не переходит")
        messages = self.client.sent[before:]
        self.assertEqual(len(messages), 1)
        answer = messages[0].lower()
        for marker in ("заблокировать", "добавить", "крестик", "скопируй", "telegram"):
            self.assertIn(marker, answer)
        self.assertNotIn("браузер", answer)
        self.assertNotIn("устройств", answer)

    async def test_soft_decline_closes_for_account_without_global_optout(self) -> None:
        self.open_cycle()
        self.assertTrue(is_soft_decline("Нет, спасибо"))
        self.assertEqual(classify_intent("Нет, спасибо"), "soft_decline")
        self.assertFalse(is_explicit_stop("Нет, спасибо", ["не надо"]))

        await self.reply("Нет, спасибо")
        self.assertEqual(self.client.sent, ["Понял, без проблем. Не буду навязывать."])
        self.assertFalse(is_opted_out(self.sender.id))
        dialog = _get_dialog_by_target(9001, self.sender.id)
        self.assertIsNotNone(dialog)
        self.assertEqual(dialog.status, "completed")
        row = conn.execute(
            """
            SELECT completion_reason FROM dm_completed_contacts
            WHERE account_user_id=? AND target_user_id=?
            """,
            (9001, self.sender.id),
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], "completed_no_interest")

    async def test_polite_no_thanks_after_link_closes_without_second_pitch(self) -> None:
        self.open_cycle()
        await self.reply("А тебе какая с этого выгода?")
        before = len(self.client.sent)
        await self.reply("Спасибо, не надо")
        final_messages = self.client.sent[before:]
        self.assertEqual(final_messages, ["Понял, без проблем. Не буду навязывать."])
        self.assertFalse(is_opted_out(self.sender.id))
        self.assertFalse(any(PIRATE_VIP_LINK in message for message in final_messages))

    async def test_automatic_help_does_not_consume_sales_followup_limit(self) -> None:
        self.open_cycle()
        await self.reply("А тебе какая с этого выгода?")
        dialog = _get_dialog_by_target(9001, self.sender.id)
        self.assertIsNotNone(dialog)
        cycle_start = conn.execute(
            """
            SELECT MAX(id) FROM ai_messages
            WHERE dialog_id = ? AND provider = 'dm_first'
            """,
            (dialog.id,),
        ).fetchone()[0]
        counted = conn.execute(
            """
            SELECT COUNT(*) FROM ai_messages
            WHERE dialog_id = ? AND id > ? AND direction = 'outgoing'
              AND provider <> 'dm_first'
              AND model NOT IN ('stop_reply', 'post_offer_apology', 'link_access_auto_help')
            """,
            (dialog.id, cycle_start),
        ).fetchone()[0]
        all_outgoing = conn.execute(
            """
            SELECT COUNT(*) FROM ai_messages
            WHERE dialog_id = ? AND id > ? AND direction = 'outgoing'
            """,
            (dialog.id, cycle_start),
        ).fetchone()[0]
        self.assertEqual(all_outgoing, counted + 1)


if __name__ == "__main__":
    unittest.main()
