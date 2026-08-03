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
        [
            Button.inline("🔄 Обновить", b"menu_refresh"),
            Button.inline("📊 Статус", b"bc_status"),
        ],
        [Button.inline("👤 Аккаунты", b"menu_accounts")],
        [Button.inline("🚀 Рассылка", b"menu_broadcast")],
        [
            Button.inline("📥 Очередь", b"bc_queue"),
            Button.inline("🚫 Opt-out", b"menu_optout"),
        ],
        [Button.inline("🤖 AI", b"menu_ai")],
        [Button.inline("ℹ️ Помощь", b"menu_help")],
    ]


def _dashboard_text() -> str:
    """Live snapshot for the home screen."""
    from services import accounts as accounts_svc
    from services import dialog_store as dialog_store_svc
    from services import dispatcher as dispatcher_svc
    from services import monitor as monitor_svc
    from services import opt_out as opt_out_svc
    from services import queue as queue_svc

    total_acc = accounts_svc.count_accounts()
    active_acc = accounts_svc.count_participating()
    mon = monitor_svc.monitor_status()
    mon_n = mon.get("connected_count") or 0
    mon_txt = f"онлайн {mon_n}" if mon.get("running") else "выкл"

    wst = dispatcher_svc.worker_status()
    if wst.get("enabled") and wst.get("loop_running"):
        w_txt = "▶ работает"
    elif wst.get("enabled"):
        w_txt = "⏸ включён, цикл стоп"
    else:
        w_txt = "⏹ выкл"

    pending = queue_svc.count_by_status(queue_svc.STATUS_PENDING)
    claimed = queue_svc.count_by_status(queue_svc.STATUS_CLAIMED)
    sent = queue_svc.count_by_status(queue_svc.STATUS_SENT)
    cancelled = queue_svc.count_by_status(queue_svc.STATUS_CANCELLED)
    dialogs = dialog_store_svc.count_active()
    first_today = queue_svc.count_first_dm_today()
    optouts = opt_out_svc.count()

    link = CHANNEL_LINK or "⚠ не задана"
    if len(link) > 40:
        link_short = link[:37] + "..."
    else:
        link_short = link

    ai_txt = "вкл" if AI_DM_ENABLED else "выкл"
    if AI_DM_ENABLED and not OPENAI_API_KEY:
        ai_txt = "вкл, но нет ключа → шаблоны"

    return (
        "**Channel DM Bot** · v1.0.2\n\n"
        f"**Рассылка:** {w_txt}\n"
        f"**Мониторинг:** {mon_txt}\n"
        f"**Аккаунты:** {active_acc} участвуют / {total_acc} всего\n\n"
        f"**Очередь**\n"
        f"• ждут first DM: **{pending}**\n"
        f"• в работе (claimed): **{claimed}**\n"
        f"• уже написали (sent): **{sent}**\n"
        f"• отменены: **{cancelled}**\n\n"
        f"**Диалоги сейчас:** {dialogs}\n"
        f"**First DM сегодня:** {first_today}\n"
        f"**Opt-out (не писать):** {optouts}\n\n"
        f"**Канал:** `{link_short}`\n"
        f"**AI:** {ai_txt} · `{AI_MODEL}`\n\n"
        "Нажми **Обновить**, чтобы освежить цифры."
    )


async def show_main_menu(event, *, edit: bool = True) -> None:
    await render_menu(event, _dashboard_text(), _main_menu_buttons(), edit=edit)


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
    await show_main_menu(event, edit=True)
    await event.answer()


@bot.on(events.CallbackQuery(data=b"menu_refresh"))
async def cb_refresh(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    await show_main_menu(event, edit=True)
    await event.answer("Обновлено")


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
    from services import dispatcher as dispatcher_svc
    from services import queue as queue_svc

    wst = dispatcher_svc.worker_status()
    w = "▶ работает" if (wst.get("enabled") and wst.get("loop_running")) else (
        "⏸ стоп цикла" if wst.get("enabled") else "⏹ выкл"
    )
    pending = queue_svc.count_by_status(queue_svc.STATUS_PENDING)
    sent = queue_svc.count_by_status(queue_svc.STATUS_SENT)
    today = queue_svc.count_first_dm_today()
    await render_menu(
        event,
        "**🚀 Рассылка**\n\n"
        f"Воркер: **{w}**\n"
        f"В очереди: **{pending}** · уже написали: **{sent}**\n"
        f"First DM сегодня: **{today}**\n\n"
        "Старт — начать раздачу first DM.\n"
        "Стоп — остановить.\n"
        "Очередь — кто ждёт.\n"
        "Темп — паузы между сообщениями.",
        _broadcast_buttons(),
    )
    await event.answer()


# bc_start / bc_stop live in handlers/dispatcher_ui.py (Step 6).


def _status_text() -> str:
    from services import accounts as accounts_svc
    from services import dialog_store as dialog_store_svc
    from services import dispatcher as dispatcher_svc
    from services import monitor as monitor_svc
    from services import opt_out as opt_out_svc
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
        f"вкл, подключено {mon['connected_count']}"
        if mon["running"]
        else "выкл"
    )
    pending = queue_svc.count_by_status(queue_svc.STATUS_PENDING)
    claimed = queue_svc.count_by_status(queue_svc.STATUS_CLAIMED)
    sent = queue_svc.count_by_status(queue_svc.STATUS_SENT)
    cancelled = queue_svc.count_by_status(queue_svc.STATUS_CANCELLED)
    dialog_active = dialog_store_svc.count_active()
    first_dm_today = queue_svc.count_first_dm_today()
    optouts = opt_out_svc.count()
    wst = dispatcher_svc.worker_status()
    w_txt = "▶ работает" if wst["enabled"] and wst["loop_running"] else (
        "⏸ включён / цикл стоп" if wst["enabled"] else "⏹ выкл"
    )
    wait = wst.get("global_wait_sec") or 0
    return (
        "**📊 Статус**\n\n"
        f"**Воркер:** {w_txt}\n"
        f"Пауза до след. first DM: ~{wait:.0f} сек\n"
        f"**Мониторинг:** {mon_txt}\n"
        f"**Аккаунты:** {active} участвуют / {total} всего\n\n"
        f"**Очередь**\n"
        f"• ждут: **{pending}**\n"
        f"• claimed: **{claimed}**\n"
        f"• написали: **{sent}**\n"
        f"• отменены: **{cancelled}**\n\n"
        f"**Диалоги активны:** {dialog_active}\n"
        f"**First DM сегодня:** {first_dm_today}\n"
        f"**Opt-out:** {optouts}\n\n"
        f"Ссылка: `{link}`\n"
        f"AI: {'вкл' if AI_DM_ENABLED else 'выкл'} | `{AI_MODEL}`\n"
        f"Админы: {admins}"
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
        "**Команды:** `/start` `/menu` `меню` `/ping` `/status` `/cancel`\nНа главном экране — живые цифры очереди и рассылки.\n\n"
        "**Сейчас:** шаг 10 - меню Opt-out.\n"
        "**Дальше:** шаг 11 - тесты и релиз.\n\n"
        "Railway: Volume `/data`, переменные из `.env.example`.",
        [back_home_row()],
    )
    await event.answer()
