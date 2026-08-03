"""Admin main menu and sections — unified visual style."""

from __future__ import annotations

from telethon import events

from config import (
    app_version,
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
from services.menu_ui import render_menu
from services.ui import (
    DENIED,
    DIV,
    OFF,
    ON,
    UPDATED,
    WAIT,
    back_home_row,
    back_row,
    btn,
    bullets,
    join,
    kv,
    screen,
    section,
    title,
    tree,
)


def _main_menu_buttons():
    return [
        [btn("🔄 Обновить", b"menu_refresh"), btn("📊 Детали", b"bc_status")],
        [btn("👤 Аккаунты", b"menu_accounts"), btn("🚀 Рассылка", b"menu_broadcast")],
        [btn("📥 Очередь", b"bc_queue"), btn("🚫 Opt-out", b"menu_optout")],
        [btn("🤖 AI", b"menu_ai"), btn("ℹ️ Помощь", b"menu_help")],
    ]


def _dashboard_text() -> str:
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

    wst = dispatcher_svc.worker_status()
    if wst.get("enabled") and wst.get("loop_running"):
        w_icon, w_txt = ON, "работает"
    elif wst.get("enabled"):
        w_icon, w_txt = WAIT, "пауза цикла"
    else:
        w_icon, w_txt = OFF, "выключена"

    mon_icon = ON if mon.get("running") and mon_n else OFF
    mon_txt = f"{mon_n} онлайн" if mon.get("running") else "выкл"

    pending = queue_svc.count_by_status(queue_svc.STATUS_PENDING)
    claimed = queue_svc.count_by_status(queue_svc.STATUS_CLAIMED)
    sent = queue_svc.count_by_status(queue_svc.STATUS_SENT)
    cancelled = queue_svc.count_by_status(queue_svc.STATUS_CANCELLED)
    dialogs = dialog_store_svc.count_active()
    first_today = queue_svc.count_first_dm_today()
    optouts = opt_out_svc.count()

    link = CHANNEL_LINK or "не задана"
    link_short = link[:33] + "…" if len(link) > 36 else link

    if AI_DM_ENABLED and OPENAI_API_KEY:
        ai_line = f"{ON} AI · `{AI_MODEL}`"
    elif AI_DM_ENABLED:
        ai_line = f"{WAIT} AI без ключа · шаблоны"
    else:
        ai_line = f"{OFF} AI выкл"

    return screen(
        "✨",
        f"Channel DM Bot · v{app_version()}",
        join(
            kv("Рассылка", w_txt, icon=w_icon),
            kv("Мониторинг", mon_txt, icon=mon_icon),
            kv("Аккаунты", f"{active_acc} из {total_acc}", icon="👤"),
        ),
        section(
            "📬 Очередь",
            tree(
                [
                    ("⏳", "ждут DM", pending),
                    ("🔄", "в работе", claimed),
                    ("✅", "написали", sent),
                    ("🗑", "отменены", cancelled),
                ]
            ),
        ),
        join(
            kv("Диалоги", str(dialogs), icon="💬"),
            kv("First DM сегодня", str(first_today), icon="📨"),
            kv("Opt-out", str(optouts), icon="🚫"),
        ),
        join(f"🔗 `{link_short}`", ai_line),
        hint="👇 разделы ниже",
    )


async def show_main_menu(event, *, edit: bool = True) -> None:
    await render_menu(event, _dashboard_text(), _main_menu_buttons(), edit=edit)


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
    if not is_admin(event.sender_id):
        return
    await show_main_menu(event, edit=False)


@bot.on(events.NewMessage(pattern=r"^/ping(?:@\w+)?$"))
async def cmd_ping(event: events.NewMessage.Event) -> None:
    if not is_admin(event.sender_id):
        return
    await event.respond("pong · бот жив ✨")


@bot.on(events.NewMessage(pattern=r"^/status(?:@\w+)?$"))
async def cmd_status(event: events.NewMessage.Event) -> None:
    if not is_admin(event.sender_id):
        return
    await render_menu(
        event,
        _status_text(),
        [back_home_row()],
        edit=False,
    )


@bot.on(events.CallbackQuery(data=b"menu_home"))
async def cb_home(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    await show_main_menu(event, edit=True)
    await event.answer()


@bot.on(events.CallbackQuery(data=b"menu_refresh"))
async def cb_refresh(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    await show_main_menu(event, edit=True)
    await event.answer(UPDATED)


def _broadcast_buttons():
    return [
        [btn("▶️ Старт", b"bc_start"), btn("⏹ Стоп", b"bc_stop")],
        [btn("📊 Статус", b"bc_status")],
        [btn("📥 Очередь", b"bc_queue")],
        [btn("⚙️ Темп", b"bc_pacing")],
        [btn("🔗 Ссылка канала", b"bc_link")],
        back_home_row(),
    ]


@bot.on(events.CallbackQuery(data=b"menu_broadcast"))
async def cb_broadcast(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    from services import dispatcher as dispatcher_svc
    from services import queue as queue_svc

    wst = dispatcher_svc.worker_status()
    if wst.get("enabled") and wst.get("loop_running"):
        w_icon, w = ON, "работает"
    elif wst.get("enabled"):
        w_icon, w = WAIT, "пауза цикла"
    else:
        w_icon, w = OFF, "выкл"

    pending = queue_svc.count_by_status(queue_svc.STATUS_PENDING)
    sent = queue_svc.count_by_status(queue_svc.STATUS_SENT)
    today = queue_svc.count_first_dm_today()

    text = screen(
        "🚀",
        "Рассылка",
        join(
            kv("Статус", w, icon=w_icon),
            kv("В очереди", str(pending), icon="⏳"),
            kv("Уже написали", str(sent), icon="✅"),
            kv("Сегодня", str(today), icon="📨"),
        ),
        bullets(
            [
                "Старт — раздать first DM",
                "Стоп — остановить",
                "Очередь — кто ждёт",
                "Темп — паузы",
            ]
        ),
    )
    await render_menu(event, text, _broadcast_buttons())
    await event.answer()


def _status_text() -> str:
    from services import accounts as accounts_svc
    from services import dialog_store as dialog_store_svc
    from services import dispatcher as dispatcher_svc
    from services import monitor as monitor_svc
    from services import opt_out as opt_out_svc
    from services import queue as queue_svc

    admins = ", ".join(f"`{x}`" for x in ADMIN_ID_LIST) or "(пусто)"
    link = CHANNEL_LINK or "(не задана)"
    total = accounts_svc.count_accounts()
    active = accounts_svc.count_participating()
    mon = monitor_svc.monitor_status()
    mon_txt = f"{ON} вкл · {mon['connected_count']} акк." if mon["running"] else f"{OFF} выкл"
    pending = queue_svc.count_by_status(queue_svc.STATUS_PENDING)
    claimed = queue_svc.count_by_status(queue_svc.STATUS_CLAIMED)
    sent = queue_svc.count_by_status(queue_svc.STATUS_SENT)
    cancelled = queue_svc.count_by_status(queue_svc.STATUS_CANCELLED)
    dialog_active = dialog_store_svc.count_active()
    first_dm_today = queue_svc.count_first_dm_today()
    optouts = opt_out_svc.count()
    wst = dispatcher_svc.worker_status()
    if wst["enabled"] and wst["loop_running"]:
        w_txt = f"{ON} работает"
    elif wst["enabled"]:
        w_txt = f"{WAIT} цикл стоп"
    else:
        w_txt = f"{OFF} выкл"
    wait = wst.get("global_wait_sec") or 0

    return screen(
        "📊",
        "Подробный статус",
        join(
            f"Рассылка: {w_txt}",
            kv("Пауза до след. DM", f"~{wait:.0f}с"),
            f"Мониторинг: {mon_txt}",
            kv("Аккаунты", f"{active} / {total}"),
        ),
        section(
            "📬 Очередь",
            f"⏳ {pending}  ·  🔄 {claimed}  ·  ✅ {sent}  ·  🗑 {cancelled}",
        ),
        join(
            kv("Диалоги", str(dialog_active), icon="💬"),
            kv("Сегодня first DM", str(first_dm_today), icon="📨"),
            kv("Opt-out", str(optouts), icon="🚫"),
        ),
        join(
            f"🔗 `{link}`",
            f"🤖 AI: {'вкл' if AI_DM_ENABLED else 'выкл'} · `{AI_MODEL}`",
            f"👮 Админы: {admins}",
        ),
    )


@bot.on(events.CallbackQuery(data=b"bc_status"))
async def cb_bc_status(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    await render_menu(
        event,
        _status_text(),
        [back_row(b"menu_broadcast"), back_home_row()],
    )
    await event.answer()


def _pacing_text() -> str:
    acc_min_m = DM_ACCOUNT_INTERVAL_MIN // 60
    acc_max_m = DM_ACCOUNT_INTERVAL_MAX // 60
    return screen(
        "⚙️",
        "Темп",
        join(
            kv("На аккаунт", f"{acc_min_m}–{acc_max_m} мин между first DM"),
            kv("Глобально", f"{DM_GLOBAL_SPACING_MIN}–{DM_GLOBAL_SPACING_MAX} сек"),
            kv("Лимит / сутки", str(DM_DAILY_LIMIT_PER_ACCOUNT)),
            kv("Ответ AI", f"{AI_REPLY_DELAY_MIN}–{AI_REPLY_DELAY_MAX} сек"),
            kv("Авто-ссылка", f"{AI_AUTO_LINK_DELAY_MIN}–{AI_AUTO_LINK_DELAY_MAX} сек"),
            kv("PeerFlood cooldown", f"{PEER_FLOOD_MIN_COOLDOWN_MINUTES} мин"),
            kv("SpamBot auto-resume", "да" if SPAMBOT_AUTO_RESUME else "нет"),
        ),
        "Меняется через Variables в Railway.",
    )


@bot.on(events.CallbackQuery(data=b"bc_pacing"))
async def cb_bc_pacing(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
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
        await event.answer(DENIED, alert=True)
        return
    link = CHANNEL_LINK or "(не задана — CHANNEL_LINK в Railway)"
    pitch = CHANNEL_PITCH or "(пусто)"
    text = screen(
        "🔗",
        "Ссылка канала",
        f"`{link}`",
        kv("Pitch", pitch),
        "Сейчас только через env Railway.",
    )
    await render_menu(
        event,
        text,
        [back_row(b"menu_broadcast"), back_home_row()],
    )
    await event.answer()


@bot.on(events.CallbackQuery(data=b"menu_ai"))
async def cb_ai(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    key_ok = "есть" if OPENAI_API_KEY else "нет"
    ai_on = f"{ON} включён" if AI_DM_ENABLED else f"{OFF} выкл"
    text = screen(
        "🤖",
        "AI",
        join(
            kv("Статус", ai_on),
            kv("Ключ OpenAI", key_ok),
            f"Модель: `{AI_MODEL}`",
        ),
        section(
            "First DM",
            bullets(
                [
                    "короткий вопрос без ссылки",
                    "без t.me / опросов / тире —",
                    "антиповтор + запасной пул",
                ]
            ),
        ),
        section(
            "Диалог",
            bullets(
                [
                    "ответ → объяснение канала",
                    "тишина 60–120с → ссылка",
                    "«не пиши» → opt-out",
                ]
            ),
        ),
    )
    await render_menu(event, text, [back_home_row()])
    await event.answer()


@bot.on(events.CallbackQuery(data=b"menu_help"))
async def cb_help(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    text = screen(
        "ℹ️",
        "Помощь",
        bullets(
            [
                "Добавь аккаунт → включи участие",
                "Чаты → обнови → выбери / режим «все»",
                "Рассылка → Старт",
            ],
            mark="1.",
        ).replace("1. ", "1. ", 1),
        section(
            "Команды",
            join("`/start`  `/menu`  `меню`", "`/ping`  `/status`  `/cancel`"),
        ),
        "На главном экране — живые цифры очереди.",
    )
    # fix numbered list properly
    text = screen(
        "ℹ️",
        "Помощь",
        join(
            "1. Добавь аккаунт → включи участие",
            "2. Чаты → обнови → выбери / режим «все»",
            "3. Рассылка → Старт",
        ),
        section(
            "Команды",
            join("`/start`  `/menu`  `меню`", "`/ping`  `/status`  `/cancel`"),
        ),
        "На главном экране — живые цифры очереди.",
    )
    await render_menu(event, text, [back_home_row()])
    await event.answer()
