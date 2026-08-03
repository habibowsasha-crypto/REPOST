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
        [btn("📁 База", b"menu_audience"), btn("🚫 Opt-out", b"menu_optout")],
        [btn("⚙️ Настройки", b"menu_settings"), btn("ℹ️ Помощь", b"menu_help")],
    ]


def _dashboard_text() -> str:
    from services import accounts as accounts_svc
    from services import dialog_store as dialog_store_svc
    from services import dispatcher as dispatcher_svc
    from services import audience as audience_svc
    from services import opt_out as opt_out_svc
    from services import queue as queue_svc
    from services import runtime as runtime_svc

    wst = dispatcher_svc.worker_status()
    if wst.get("enabled") and wst.get("loop_running"):
        w_line = f"{ON} Рассылка работает"
    elif wst.get("enabled"):
        w_line = f"{WAIT} Рассылка пауза цикла"
    else:
        w_line = f"{OFF} Рассылка выкл"

    wait = float(wst.get("global_wait_sec") or 0)
    wait_txt = f"~{int(wait)}с" if wait else "сейчас"

    pending = queue_svc.count_by_status(queue_svc.STATUS_PENDING)
    claimed = queue_svc.count_by_status(queue_svc.STATUS_CLAIMED)
    first_today = queue_svc.count_first_dm_today()
    dialogs = dialog_store_svc.count_active()
    optouts = opt_out_svc.count()
    audience_n = audience_svc.count()

    link = CHANNEL_LINK or "не задана"
    # show full invite link if fits; Telegram allows long messages
    if len(link) > 48:
        link_show = link[:45] + "…"
    else:
        link_show = link

    if AI_DM_ENABLED and OPENAI_API_KEY:
        ai_line = f"{ON} AI · `{AI_MODEL}`"
    elif AI_DM_ENABLED:
        ai_line = f"{WAIT} AI без ключа · шаблоны"
    else:
        ai_line = f"{OFF} AI выкл"

    pf = runtime_svc.format_peer_flood_range()
    acc_block = accounts_svc.dashboard_accounts_block(limit=8)

    queue_block = tree(
        [
            ("⏳", "ждут", pending),
            ("🔄", "в работе", claimed),
            ("✅", "сегодня", first_today),
        ]
    )

    return join(
        f"✨ **Channel DM Bot · v{app_version()}**",
        DIV,
        w_line,
        f"⏳ До след. DM {wait_txt}",
        "📬 Очередь",
        queue_block,
        DIV,
        "👤 Аккаунты",
        acc_block,
        DIV,
        f"💬 Диалоги: {dialogs}",
        f"📁 База: {audience_n}",
        f"🚫 Opt-out: {optouts}",
        f"🔗 {link_show}",
        ai_line,
        f"⚠️ PeerFlood {pf}",
        DIV,
        "Таймер паузы · 🔄 Обновить",
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
    a_lo, a_hi = runtime_svc.get_account_interval_range()
    text = screen(
        "⚙️",
        "Настройки",
        join(
            "• **Темп** — как часто писать (меняется в меню)",
            "• **После PeerFlood** — пауза после ограничения",
        ),
        join(
            kv("Темп (акк.)", f"{a_lo // 60}–{a_hi // 60} мин", icon="⏱"),
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
    from services import runtime as runtime_svc

    a_lo, a_hi = runtime_svc.get_account_interval_range()
    g_lo, g_hi = runtime_svc.get_global_spacing_range()
    daily = runtime_svc.get_daily_limit()
    r_lo, r_hi = runtime_svc.get_ai_reply_delay_range()
    l_lo, l_hi = runtime_svc.get_auto_link_delay_range()

    return join(
        f"⏱ **Темп рассылки**",
        DIV,
        "Плановый ритм, пока нет PeerFlood.",
        "",
        f"👤 Между DM одного аккаунта",
        f"   **{a_lo // 60}–{a_hi // 60} мин**",
        "",
        f"🌐 Между любыми DM в боте",
        f"   **{g_lo}–{g_hi} сек**",
        "",
        f"📊 Макс. first DM / сутки",
        f"   **{daily}**",
        "",
        f"🤖 Пауза перед ответом AI",
        f"   **{r_lo}–{r_hi} сек**",
        "",
        f"🔗 Авто-ссылка при тишине",
        f"   **{l_lo}–{l_hi} сек**",
        DIV,
        "Меняется кнопками ниже (не Railway).",
    )


def _pacing_buttons():
    return [
        [btn("👤 Аккаунт 10–15м", b"pace_acc_600_900"), btn("15–25м", b"pace_acc_900_1500")],
        [btn("🌐 Global 90–180с", b"pace_glob_90_180"), btn("120–240с", b"pace_glob_120_240")],
        [btn("📊 Лимит 30", b"pace_daily_30"), btn("45", b"pace_daily_45"), btn("60", b"pace_daily_60")],
        [btn("🤖 AI 20–60с", b"pace_ai_20_60"), btn("30–90с", b"pace_ai_30_90")],
        [btn("🔗 Link 60–120с", b"pace_link_60_120"), btn("90–180с", b"pace_link_90_180")],
        [btn("✏️ Свой интервал", b"pace_custom")],
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


@bot.on(events.CallbackQuery(pattern=rb"^pace_acc_(\d+)_(\d+)$"))
async def cb_pace_acc(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    from services import runtime as runtime_svc

    m = event.pattern_match
    lo, hi = int(m.group(1)), int(m.group(2))
    runtime_svc.set_account_interval_range(lo, hi)
    await render_menu(event, _pacing_text(), _pacing_buttons())
    await event.answer(f"Аккаунт: {lo // 60}–{hi // 60} мин")


@bot.on(events.CallbackQuery(pattern=rb"^pace_glob_(\d+)_(\d+)$"))
async def cb_pace_glob(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    from services import runtime as runtime_svc

    m = event.pattern_match
    lo, hi = int(m.group(1)), int(m.group(2))
    runtime_svc.set_global_spacing_range(lo, hi)
    await render_menu(event, _pacing_text(), _pacing_buttons())
    await event.answer(f"Global: {lo}–{hi}с")


@bot.on(events.CallbackQuery(pattern=rb"^pace_daily_(\d+)$"))
async def cb_pace_daily(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    from services import runtime as runtime_svc

    n = int(event.pattern_match.group(1))
    runtime_svc.set_daily_limit(n)
    await render_menu(event, _pacing_text(), _pacing_buttons())
    await event.answer(f"Лимит: {n}/сутки")


@bot.on(events.CallbackQuery(pattern=rb"^pace_ai_(\d+)_(\d+)$"))
async def cb_pace_ai(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    from services import runtime as runtime_svc

    m = event.pattern_match
    lo, hi = int(m.group(1)), int(m.group(2))
    runtime_svc.set_ai_reply_delay_range(lo, hi)
    await render_menu(event, _pacing_text(), _pacing_buttons())
    await event.answer(f"AI: {lo}–{hi}с")


@bot.on(events.CallbackQuery(pattern=rb"^pace_link_(\d+)_(\d+)$"))
async def cb_pace_link(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    from services import runtime as runtime_svc

    m = event.pattern_match
    lo, hi = int(m.group(1)), int(m.group(2))
    runtime_svc.set_auto_link_delay_range(lo, hi)
    await render_menu(event, _pacing_text(), _pacing_buttons())
    await event.answer(f"Link: {lo}–{hi}с")


@bot.on(events.CallbackQuery(data=b"pace_custom"))
async def cb_pace_custom(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    from services.admin_state import set_state

    set_state(event.sender_id, flow="pacing", step="custom")
    text = join(
        "✏️ **Свой темп**",
        DIV,
        "Пришли одной строкой:",
        "`acc_min acc_max glob_min glob_max daily ai_min ai_max link_min link_max`",
        "",
        "Все в **секундах**, daily — число.",
        "Пример:",
        "`600 900 90 180 45 30 90 60 120`",
        "",
        "Отмена: /cancel",
    )
    await render_menu(
        event,
        text,
        [back_row(b"bc_pacing"), back_home_row()],
    )
    await event.answer()


def _is_pacing_custom(event) -> bool:
    if not getattr(event, "is_private", False):
        return False
    if not is_admin(event.sender_id):
        return False
    from services.admin_state import get_state

    st = get_state(int(event.sender_id)) or {}
    return st.get("flow") == "pacing" and st.get("step") == "custom"


@bot.on(events.NewMessage(func=_is_pacing_custom))
async def on_pacing_custom(event: events.NewMessage.Event) -> None:
    from services.admin_state import clear_state
    from services import runtime as runtime_svc

    raw = (event.raw_text or "").strip()
    if raw.startswith("/"):
        return
    parts = raw.replace(",", " ").split()
    if len(parts) != 9 or not all(p.lstrip("-").isdigit() for p in parts):
        await event.respond(
            notice(
                "warn",
                "Нужно 9 чисел. Пример: `600 900 90 180 45 30 90 60 120`",
            )
        )
        return
    vals = [int(x) for x in parts]
    clear_state(event.sender_id)
    runtime_svc.set_account_interval_range(vals[0], vals[1])
    runtime_svc.set_global_spacing_range(vals[2], vals[3])
    runtime_svc.set_daily_limit(vals[4])
    runtime_svc.set_ai_reply_delay_range(vals[5], vals[6])
    runtime_svc.set_auto_link_delay_range(vals[7], vals[8])
    await event.respond(
        join("⏱ **Темп сохранён**", DIV, _pacing_text()),
        buttons=[back_home_row()],
    )


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
