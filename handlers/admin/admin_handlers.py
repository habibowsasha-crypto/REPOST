from services.menu_ui import render_menu
from loguru import logger
import re

from telethon import Button
from config import callback_message, callback_query, ADMIN_ID_LIST, New_Message, Query, bot
from services.admin_state import clear_admin_interaction_state, is_command_event


def _main_menu_buttons():
    return [
        [
            Button.inline("➕ Добавить аккаунт 👤", b"add_account"),
            Button.inline("➕ Добавить группу 👥", b"add_groups"),
        ],
        [
            Button.inline("👤 Мои аккаунты", b"my_accounts"),
            Button.inline("👥 Мои группы", b"my_groups"),
        ],
        [Button.inline("🔎 Группы аккаунтов", b"my_accounts")],
        [
            Button.inline("💬 Запустить DM", b"menu_dm_post"),
            Button.inline("📋 DM-задачи", b"menu_dm_list"),
        ],
        [Button.inline("🛑 Остановить DM-задачу", b"menu_dm_stop")],
        [Button.inline("🧹 Очистить неактуальные DM-задачи", b"menu_dm_cleanup")],
        [
            Button.inline("🤖 AI статус", b"menu_ai_status"),
            Button.inline("💬 AI-диалоги", b"menu_ai_dialogs"),
        ],
        [Button.inline("📝 Первые DM-шаблоны", b"menu_first_dm_templates")],
        [Button.inline("📨 Обычная рассылка во все аккаунты", b"broadcast_All_account")],
        [Button.inline("❌ Остановить обычную рассылку", b"Stop_Broadcast_All_account")],
        [Button.inline("🕗 История обычной рассылки", b"show_history")],
        [Button.inline("✖️ Сбросить текущий ввод", b"menu_cancel_flow")],
    ]


@bot.on(New_Message(func=lambda e: e.sender_id in ADMIN_ID_LIST and is_command_event(e)))
async def reset_stale_state_before_command(event: callback_message) -> None:
    """
    Any slash command starts outside old text/number wizards.

    Text-state handlers separately ignore slash commands, so this cleanup does not
    race with them and prevents the repeated "Некорректный формат числа" bug.
    """
    command = (event.raw_text or "").strip().split(maxsplit=1)[0].lower()
    if command != "/cancel":
        await clear_admin_interaction_state(event.sender_id)


async def _show_main_menu(event: callback_message, *, edit: bool = False) -> None:
    await clear_admin_interaction_state(event.sender_id)
    if edit:
        await render_menu(event, "👋 Добро пожаловать, Админ!", buttons=_main_menu_buttons())
    else:
        await event.respond("👋 Добро пожаловать, Админ!", buttons=_main_menu_buttons())


@bot.on(New_Message(pattern=re.compile(r"^\s*(?:/menu(?:@\w+)?|меню)\s*$", re.IGNORECASE)))
async def menu_command(event: callback_message) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    await _show_main_menu(event)


@bot.on(Query(data=b"menu_home"))
async def menu_home(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    await _show_main_menu(event, edit=True)
    await event.answer()


@bot.on(New_Message(pattern=r"^/start(?:@\w+)?$"))
async def start(event: callback_message) -> None:
    """Show the complete admin menu and reset unfinished setup dialogs."""
    logger.info("Нажата команда /start")
    if event.sender_id not in ADMIN_ID_LIST:
        await event.respond("⛔ Запрещено!")
        return

    await _show_main_menu(event)


@bot.on(New_Message(pattern=r"^/cancel(?:@\w+)?$"))
async def cancel_flow(event: callback_message) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    cleared = await clear_admin_interaction_state(event.sender_id)
    if cleared:
        await event.respond("✅ Текущий ввод отменён.", buttons=_main_menu_buttons())
    else:
        await event.respond("ℹ️ Активного ввода не было.", buttons=_main_menu_buttons())


@bot.on(Query(data=b"menu_cancel_flow"))
async def cancel_flow_button(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    cleared = await clear_admin_interaction_state(event.sender_id)
    text = "✅ Текущий ввод отменён." if cleared else "ℹ️ Активного ввода не было."
    await render_menu(event, text, buttons=_main_menu_buttons())
    await event.answer()
