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
    from services import dispatcher as dispatcher_svc

    st = dispatcher_svc.worker_status()
    running = bool(st.get("enabled"))
    toggle = (
        btn("⏸ Пауза", b"bc_toggle")
        if running
        else btn("▶️ Запустить", b"bc_toggle")
    )
    return [
        [toggle, btn("🔄 Обновить", b"menu_refresh")],
        [btn("📊 Статус", b"bc_status")],
        [btn("👤 Аккаунты", b"menu_accounts"), btn("📬 Очередь", b"bc_queue")],
        [btn("⚙️ Настройки", b"menu_settings"), btn("🚫 Opt-out", b"menu_optout")],
        [btn("ℹ️ Помощь", b"menu_help")],
    ]


def _dashboard_text() -> str:
    from services import accounts as accounts_svc
    from services import dialog_store as dialog_store_svc
    from services import dispatcher as dispatcher_svc
    from services import monitor as monitor_svc
    from services import opt_out as opt_out_svc
    from services import queue as queue_svc
    from services import runtime as runtime_svc

    total_acc = accounts_svc.count_accounts()
    mon = monitor_svc.monitor_status()
    mon_n = int(mon.get("connected_count") or 0)

    wst = dispatcher_svc.worker_status()
    if wst.get("enabled") and wst.get("loop_running"):
        w_icon, w_txt = ON, "работает"
    elif wst.get("enabled"):
        w_icon, w_txt = WAIT, "пауза цикла"
    else:
        w_icon, w_txt = OFF, "выключена"

    pending = queue_svc.count_by_status(queue_svc.STATUS_PENDING)
    claimed = queue_svc.count_by_status(queue_svc.STATUS_CLAIMED)
    first_today = queue_svc.count_first_dm_today()
    dialogs = dialog_store_svc.count_active()
    optouts = opt_out_svc.count()

    link = CHANNEL_LINK or "не задана"
    link_short = link[:33] + "…" if len(link) > 36 else link

    if AI_DM_ENABLED and OPENAI_API_KEY:
        ai_line = f"{ON} AI · `{AI_MODEL}`"
    elif AI_DM_ENABLED:
        ai_line = f"{WAIT} AI без ключа · шаблоны"
    else:
        ai_line = f"{OFF} AI выкл"

    wait = float(wst.get("global_wait_sec") or 0)
    wait_txt = f"~{int(wait)}с" if wait else "сейчас"
    pf = runtime_svc.format_peer_flood_range()

    return screen(
        "✨",
        f"Channel DM Bot · v{app_version()}",
        join(
            f"{w_icon} Рассылка   **{w_txt}**",
            f"⏳ До след. DM   **{wait_txt}**",
        ),
        section(
            "📬 Очередь",
            tree(
                [
                    ("⏳", "ждут", pending),
                    ("🔄", "в работе", claimed),
                    ("✅", "сегодня", first_today),
                ]
            ),
        ),
        join(
            kv("Диалоги", str(dialogs), icon="💬"),
            kv("Opt-out", str(optouts), icon="🚫"),
            kv("В сети", f"{mon_n} / {total_acc} акк.", icon="👤"),
        ),
        join(f"🔗 `{link_short}`", ai_line, f"⚠️ PeerFlood  {pf}"),
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


def _settings_buttons():
    return [
        [btn("⏱ Темп рассылки", b"bc_pacing")],
        [btn("⚠️ После PeerFlood", b"bc_peerflood")],
        [btn("🔗 Канал", b"bc_link")],
        [btn("🤖 AI", b"menu_ai")],
        back_home_row(),
    ]


@bot.on(events.CallbackQuery(data=b"menu_settings"))
async def cb_settings(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    from services import runtime as runtime_svc

    pf = runtime_svc.format_peer_flood_range()
    ai = f"{ON} `{AI_MODEL}`" if AI_DM_ENABLED else f"{OFF} выкл"
    link = CHANNEL_LINK or "(не задана)"
    link_short = link if len(link) <= 40 else link[:37] + "…"
    acc_min_m = DM_ACCOUNT_INTERVAL_MIN // 60
    acc_max_m = DM_ACCOUNT_INTERVAL_MAX // 60
    text = screen(
        "⚙️",
        "Настройки",
        join(
            "Две разные вещи:",
            "• **Темп** — как часто писать, когда всё ок",
            "• **После PeerFlood** — сколько ждать после ограничения Telegram",
        ),
        join(
            kv("Темп (акк.)", f"{acc_min_m}–{acc_max_m} мин", icon="⏱"),
            kv("После PeerFlood", pf, icon="⚠️"),
            kv("AI", ai, icon="🤖"),
            f"🔗 `{link_short}`",
        ),
    )
    await render_menu(event, text, _settings_buttons())
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
        "Статус",
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
        [[btn("🔄 Обновить", b"bc_status")], back_home_row()],
    )
    await event.answer()


def _pacing_text() -> str:
    acc_min_m = DM_ACCOUNT_INTERVAL_MIN // 60
    acc_max_m = DM_ACCOUNT_INTERVAL_MAX // 60
    return screen(
        "⏱",
        "Темп рассылки",
        "Плановый ритм, пока аккаунт **не** в PeerFlood.",
        join(
            kv("Между DM одного аккаунта", f"{acc_min_m}–{acc_max_m} мин"),
            kv("Между любыми DM в боте", f"{DM_GLOBAL_SPACING_MIN}–{DM_GLOBAL_SPACING_MAX} сек"),
            kv("Макс. first DM / сутки", str(DM_DAILY_LIMIT_PER_ACCOUNT)),
            kv("Пауза перед ответом AI", f"{AI_REPLY_DELAY_MIN}–{AI_REPLY_DELAY_MAX} сек"),
            kv("Авто-ссылка при тишине", f"{AI_AUTO_LINK_DELAY_MIN}–{AI_AUTO_LINK_DELAY_MAX} сек"),
        ),
        join(
            "Эти значения задаются в **Railway Variables**.",
            "Здесь только просмотр — кнопками не меняются.",
        ),
    )


def _pacing_buttons():
    return [
        back_row(b"menu_settings"),
        back_home_row(),
    ]


@bot.on(events.CallbackQuery(data=b"bc_pacing"))
async def cb_bc_pacing(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    await render_menu(event, _pacing_text(), _pacing_buttons())
    await event.answer()


def _peerflood_screen() -> str:
    from services import runtime as runtime_svc

    pf = runtime_svc.format_peer_flood_range()
    return screen(
        "⚠️",
        "После PeerFlood",
        "Это **не** обычный темп.",
        join(
            "Когда Telegram кидает PeerFlood:",
            "• аккаунт на паузу",
            "• бот пишет @SpamBot",
            "• ждём случайное время из диапазона ниже",
        ),
        f"Сейчас: **{pf}** (рандом каждый раз)",
        "Пресет или свой `min max` в **секундах**:",
    )


def _peerflood_buttons():
    from services.ui import btn

    # data: pf_rng_{lo}_{hi}
    return [
        [
            btn("1–5 мин", b"pf_rng_60_300"),
            btn("3–10 мин", b"pf_rng_180_600"),
        ],
        [
            btn("5–15 мин", b"pf_rng_300_900"),
            btn("10–30 мин", b"pf_rng_600_1800"),
        ],
        [btn("✏️ Свой min max (сек)", b"pf_custom")],
        back_row(b"menu_settings"),
        back_home_row(),
    ]


@bot.on(events.CallbackQuery(data=b"bc_peerflood"))
async def cb_bc_peerflood(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    await render_menu(event, _peerflood_screen(), _peerflood_buttons())
    await event.answer()


@bot.on(events.CallbackQuery(pattern=rb"^pf_rng_(\d+)_(\d+)$"))
async def cb_pf_rng(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    from services import runtime as runtime_svc

    lo = int(event.pattern_match.group(1))
    hi = int(event.pattern_match.group(2))
    a, b = runtime_svc.set_peer_flood_range_seconds(lo, hi)
    label = runtime_svc.format_peer_flood_range()
    await render_menu(event, _peerflood_screen(), _peerflood_buttons())
    await event.answer(f"Диапазон: {label}")


# legacy fixed preset buttons still work → fixed range
@bot.on(events.CallbackQuery(pattern=rb"^pf_set_(\d+)$"))
async def cb_pf_set(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    from services import runtime as runtime_svc

    seconds = int(event.pattern_match.group(1))
    runtime_svc.set_peer_flood_range_seconds(seconds, seconds)
    label = runtime_svc.format_peer_flood_range()
    await render_menu(event, _peerflood_screen(), _peerflood_buttons())
    await event.answer(f"После PeerFlood: {label}")


@bot.on(events.CallbackQuery(data=b"pf_custom"))
async def cb_pf_custom(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    from services.admin_state import set_state
    from services import runtime as runtime_svc

    set_state(event.sender_id, flow="peerflood", step="custom_range")
    pf = runtime_svc.format_peer_flood_range()
    text = screen(
        "✏️",
        "Свой диапазон",
        f"Сейчас: **{pf}**",
        "Пришли **два числа** в секундах: `min max`",
        f"Допустимо: **{runtime_svc.PEER_FLOOD_MIN_ALLOWED_SEC}–{runtime_svc.PEER_FLOOD_MAX_ALLOWED_SEC}** сек.",
        "Примеры: `180 600` (=3–10 мин), `60 300` (=1–5 мин)",
        "Одно число = фиксированная пауза.",
        "Отмена: /cancel",
    )
    await render_menu(
        event,
        text,
        [back_row(b"bc_peerflood"), back_home_row()],
    )
    await event.answer()


def _is_peerflood_flow(event) -> bool:
    if not event.is_private:
        return False
    if not is_admin(event.sender_id):
        return False
    from services.admin_state import get_state

    st = get_state(int(event.sender_id)) or {}
    return st.get("flow") == "peerflood"


@bot.on(events.NewMessage(func=_is_peerflood_flow))
async def on_peerflood_custom(event: events.NewMessage.Event) -> None:
    from services.admin_state import clear_state, get_state
    from services import runtime as runtime_svc

    st = get_state(event.sender_id) or {}
    if st.get("step") != "custom_range":
        return
    raw = (event.raw_text or "").strip()
    if raw.startswith("/"):
        return
    parts = raw.replace(",", " ").split()
    if len(parts) == 1 and parts[0].isdigit():
        n = int(parts[0])
        a, b = runtime_svc.set_peer_flood_range_seconds(n, n)
    elif len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
        a, b = runtime_svc.set_peer_flood_range_seconds(int(parts[0]), int(parts[1]))
    else:
        await event.respond("Формат: `180 600` (min max в секундах) или одно число.")
        return
    label = runtime_svc.format_peer_flood_range()
    clear_state(event.sender_id)
    await event.respond(
        screen(
            "⚠️",
            "Пауза PeerFlood",
            f"Сохранено: **{label}**",
            f"`{a}`–`{b}` сек · рандом при каждом PeerFlood",
        ),
        buttons=[back_home_row()],
    )


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
        [back_row(b"menu_settings"), back_home_row()],
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
    await render_menu(
        event,
        text,
        [back_row(b"menu_settings"), back_home_row()],
    )
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
                "Аккаунты → добавить → включить участие",
                "Чаты → обновить → выбрать / режим «все»",
                "На главной → Запустить",
            ],
        ),
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
            "3. Главная → Запустить",
        ),
        section(
            "Команды",
            join("`/start`  `/menu`  `меню`", "`/ping`  `/status`  `/cancel`"),
        ),
        "На главном экране — живые цифры очереди.",
    )
    await render_menu(event, text, [back_home_row()])
    await event.answer()
