"""Admin menu navigation (Step 2: full tree, section stubs)."""

from __future__ import annotations

from telethon import Button, events

from config import (
    ADMIN_ID_LIST,
    AI_AUTO_LINK_DELAY_MAX,
    AI_AUTO_LINK_DELAY_MIN,
    AI_DM_ENABLED,
    AI_MODEL,
    AI_REPLY_DELAY_MAX,
    AI_REPLY_DELAY_MIN,
    CHANNEL_LINK,
    CHANNEL_PITCH,
    DM_ACCOUNT_INTERVAL_MAX,
    DM_ACCOUNT_INTERVAL_MIN,
    DM_DAILY_LIMIT_PER_ACCOUNT,
    DM_GLOBAL_SPACING_MAX,
    DM_GLOBAL_SPACING_MIN,
    OPENAI_API_KEY,
    PEER_FLOOD_MIN_COOLDOWN_MINUTES,
    SPAMBOT_AUTO_RESUME,
    bot,
    is_admin,
)
from services.menu_ui import back_home_row, back_row, render_menu

# ---------------------------------------------------------------------------
# Main menu
# ---------------------------------------------------------------------------


def _main_menu_buttons() -> list[list[Button]]:
    return [
        [Button.inline("👤 Аккаунты", b"menu_accounts")],
        [Button.inline("🚀 Рассылка", b"menu_broadcast")],
        [Button.inline("🤖 AI", b"menu_ai")],
        [Button.inline("🚫 Opt-out", b"menu_optout")],
        [Button.inline("ℹ️ Помощь", b"menu_help")],
    ]


def _main_menu_text() -> str:
    link_line = CHANNEL_LINK if CHANNEL_LINK else "не задана (CHANNEL_LINK в Railway)"
    return (
        "**Channel DM Bot**\n"
        "v1.0.0\n\n"
        "Бот отвечает только админам из `ADMIN_ID_LIST`.\n"
        f"Ссылка канала: `{link_line}`\n\n"
        "Выберите раздел:"
    )


async def show_main_menu(event, *, edit: bool = True) -> None:
    await render_menu(event, _main_menu_text(), _main_menu_buttons(), edit=edit)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


@bot.on(events.NewMessage(pattern=r"^/start(?:@\w+)?$"))
async def cmd_start(event: events.NewMessage.Event) -> None:
    if not is_admin(event.sender_id):
        await event.respond("Нет доступа.")
        return
    await show_main_menu(event, edit=False)


@bot.on(events.NewMessage(pattern=r"^/menu(?:@\w+)?$"))
async def cmd_menu(event: events.NewMessage.Event) -> None:
    if not is_admin(event.sender_id):
        return
    await show_main_menu(event, edit=False)


@bot.on(events.NewMessage(pattern=r"(?i)^меню$"))
async def cmd_menu_ru(event: events.NewMessage.Event) -> None:
    """Open main menu on plain text: меню / Меню / МЕНЮ."""
    if not is_admin(event.sender_id):
        return
    await show_main_menu(event, edit=False)


@bot.on(events.NewMessage(pattern=r"^/ping(?:@\w+)?$"))
async def cmd_ping(event: events.NewMessage.Event) -> None:
    if not is_admin(event.sender_id):
        return
    await event.respond("pong - бот жив.")


@bot.on(events.NewMessage(pattern=r"^/status(?:@\w+)?$"))
async def cmd_status(event: events.NewMessage.Event) -> None:
    if not is_admin(event.sender_id):
        return
    await render_menu(
        event,
        _status_text(),
        [[Button.inline("◀️ Меню", b"menu_home")]],
        edit=False,
    )


@bot.on(events.CallbackQuery(data=b"menu_home"))
async def cb_home(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    await show_main_menu(event)
    await event.answer()


# 👤 Accounts handlers live in handlers/accounts.py (Step 3).

# ---------------------------------------------------------------------------
# 🚀 Broadcast / outreach submenu
# ---------------------------------------------------------------------------


def _broadcast_buttons() -> list[list[Button]]:
    return [
        [
            Button.inline("▶️ Старт", b"bc_start"),
            Button.inline("⏹ Стоп", b"bc_stop"),
        ],
        [Button.inline("📊 Статус", b"bc_status")],
        [Button.inline("📥 Очередь", b"bc_queue")],
        [Button.inline("⚙️ Темп", b"bc_pacing")],
        [Button.inline("🔗 Ссылка канала", b"bc_link")],
        back_home_row(),
    ]


@bot.on(events.CallbackQuery(data=b"menu_broadcast"))
async def cb_broadcast(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    await render_menu(
        event,
        "**🚀 Рассылка**\n\n"
        "Общая очередь лидов + random аккаунт.\n"
        "Старт/стоп воркера и настройки темпа.\n\n"
        "Темп: 10–15 мин / аккаунт, 90–180 сек глобально.",
        _broadcast_buttons(),
    )
    await event.answer()


# bc_start / bc_stop live in handlers/dispatcher_ui.py (Step 6).


def _status_text() -> str:
    from services import accounts as accounts_svc
    from services import monitor as monitor_svc
    from services import queue as queue_svc

    admins = (
        ", ".join(f"`{x}`" for x in ADMIN_ID_LIST)
        or "(пусто - задайте ADMIN_ID_LIST)"
    )
    link = CHANNEL_LINK or "(не задана)"
    total = accounts_svc.count_accounts()
    active = accounts_svc.count_participating()
    mon = monitor_svc.monitor_status()
    mon_txt = (
        f"вкл ({mon['connected_count']} акк.)"
        if mon["running"]
        else "выкл"
    )
    pending = queue_svc.count_by_status(queue_svc.STATUS_PENDING)
    from services import dialog_store as dialog_store_svc
    dialog_active = dialog_store_svc.count_active()
    first_dm_today = queue_svc.count_first_dm_today()
    from services import dispatcher as dispatcher_svc
    wst = dispatcher_svc.worker_status()
    w_txt = "вкл / цикл ок" if wst["enabled"] and wst["loop_running"] else (
        "вкл / цикл стоп" if wst["enabled"] else "выкл"
    )
    return (
        "**📊 Статус**\n\n"
        f"Админы: {admins}\n"
        f"Мониторинг: {mon_txt}\n"
        f"Воркер first DM: {w_txt}\n"
        f"Аккаунтов: {total} (участвуют: {active})\n"
        f"Очередь pending: {pending}\n"
        f"Активных диалогов: {dialog_active}\n"
        f"First DM сегодня: {first_dm_today}\n"
        f"Ссылка: `{link}`\n"
        f"AI: {'вкл' if AI_DM_ENABLED else 'выкл'} | модель `{AI_MODEL}`"
    )


@bot.on(events.CallbackQuery(data=b"bc_status"))
async def cb_bc_status(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    await render_menu(
        event,
        _status_text(),
        [back_row(b"menu_broadcast"), back_home_row()],
    )
    await event.answer()


# bc_queue UI lives in handlers/queue_ui.py (Step 5).


def _pacing_text() -> str:
    acc_min_m = DM_ACCOUNT_INTERVAL_MIN // 60
    acc_max_m = DM_ACCOUNT_INTERVAL_MAX // 60
    return (
        "**⚙️ Темп (из env)**\n\n"
        f"На аккаунт между first DM: **{acc_min_m}–{acc_max_m} мин** "
        f"({DM_ACCOUNT_INTERVAL_MIN}–{DM_ACCOUNT_INTERVAL_MAX} сек)\n"
        f"Глобально между first DM: **{DM_GLOBAL_SPACING_MIN}–{DM_GLOBAL_SPACING_MAX} сек**\n"
        f"Лимит first DM / аккаунт / сутки: **{DM_DAILY_LIMIT_PER_ACCOUNT}**\n"
        f"Задержка AI-ответа: **{AI_REPLY_DELAY_MIN}–{AI_REPLY_DELAY_MAX} сек**\n"
        f"Авто-ссылка при тишине: **{AI_AUTO_LINK_DELAY_MIN}–{AI_AUTO_LINK_DELAY_MAX} сек**\n"
        f"PeerFlood min cooldown: **{PEER_FLOOD_MIN_COOLDOWN_MINUTES} мин**\n"
        f"SpamBot auto-resume: **{'да' if SPAMBOT_AUTO_RESUME else 'нет'}**\n\n"
        "Сейчас только просмотр. Изменение через кнопки - позже; "
        "пока правьте Variables в Railway."
    )


@bot.on(events.CallbackQuery(data=b"bc_pacing"))
async def cb_bc_pacing(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    await render_menu(
        event,
        _pacing_text(),
        [back_row(b"menu_broadcast"), back_home_row()],
    )
    await event.answer()


@bot.on(events.CallbackQuery(data=b"bc_link"))
async def cb_bc_link(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    link = CHANNEL_LINK or "(не задана - укажите CHANNEL_LINK в Railway)"
    pitch = CHANNEL_PITCH or "(пусто)"
    await render_menu(
        event,
        "**🔗 Ссылка канала**\n\n"
        f"Ссылка:\n`{link}`\n\n"
        f"Описание (pitch):\n{pitch}\n\n"
        "Смена из меню - в следующих шагах. Сейчас задаётся через env.",
        [back_row(b"menu_broadcast"), back_home_row()],
    )
    await event.answer()


# ---------------------------------------------------------------------------
# 🤖 AI
# ---------------------------------------------------------------------------


@bot.on(events.CallbackQuery(data=b"menu_ai"))
async def cb_ai(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    key_ok = "задан" if OPENAI_API_KEY else "не задан"
    await render_menu(
        event,
        "**🤖 AI**\n\n"
        f"Ответы AI: **{'включены' if AI_DM_ENABLED else 'выключены'}** "
        f"(`AI_DM_ENABLED`)\n"
        f"Модель: `{AI_MODEL}`\n"
        f"OPENAI_API_KEY: {key_ok}\n\n"
        "First DM (шаг 8):\n"
        "- AI короткий вопрос без ссылки\n"
        "- validator: без t.me, без опросников, без длинного тире\n"
        "- антиповтор + fallback пул\n\n"
        "Диалог (шаг 9):\n"
        "- после ответа: объяснение канала\n"
        "- тишина 60–120 сек → авто-ссылка\n"
        "- «не пиши» / агрессия → opt-out",
        [back_home_row()],
    )
    await event.answer()


# 🚫 Opt-out handlers live in handlers/optout.py (Step 10).


# ---------------------------------------------------------------------------
# ℹ️ Help
# ---------------------------------------------------------------------------


@bot.on(events.CallbackQuery(data=b"menu_help"))
async def cb_help(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    await render_menu(
        event,
        "**ℹ️ Помощь**\n\n"
        "Бот для мягкой DM-рекламы канала:\n"
        "1. Аккаунты слушают выбранные группы\n"
        "2. Активные люди → одна общая очередь\n"
        "3. Random лид × random свободный аккаунт\n"
        "4. AI: короткий вопрос → объяснение → ссылка\n"
        "5. «Не пиши» → opt-out навсегда\n\n"
        "**Команды:** `/start` `/menu` `меню` `/ping` `/status` `/cancel`\n\n"
        "**Сейчас:** шаг 10 - меню Opt-out.\n"
        "**Дальше:** шаг 11 - тесты и релиз.\n\n"
        "Railway: Volume `/data`, переменные из `.env.example`.",
        [back_home_row()],
    )
    await event.answer()
