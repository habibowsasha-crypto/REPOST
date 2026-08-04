"""Admin main menu and sections - unified visual style."""

from __future__ import annotations

from telethon import events

from config import (
    app_version,
    ADMIN_ID_LIST,
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
    TELEGRAM_DIALOG_DELETE_DAYS,
    LOCAL_DIALOG_TEXT_RETENTION_DAYS,
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
    from services import accounts as accounts_svc
    from services import dispatcher as dispatcher_svc

    running = bool(dispatcher_svc.worker_status().get("enabled"))
    toggle = (
        btn("⏸ ПАУЗА FIRST DM", b"bc_toggle")
        if running
        else btn("▶️ ЗАПУСТИТЬ FIRST DM", b"bc_toggle")
    )
    rows = [
        [toggle, btn("🔄 ОБНОВИТЬ", b"menu_refresh")],
        [btn("👤 АККАУНТЫ", b"menu_accounts"), btn("💬 ДИАЛОГИ", b"menu_dialogs")],
    ]
    reauth = accounts_svc.count_reauth_required()
    if reauth:
        rows.append(
            [btn(f"🔴 НУЖЕН ВХОД · {reauth}", b"acc_problem_list")]
        )
    rows.extend(
        [
            [btn("📬 ОЧЕРЕДЬ", b"bc_queue"), btn("📁 БАЗА ЛЮДЕЙ", b"menu_audience")],
            [btn("🚫 НЕ ПИСАТЬ", b"menu_optout"), btn("📊 СТАТИСТИКА", b"bc_status")],
            [btn("⚙️ НАСТРОЙКИ", b"menu_settings"), btn("ℹ️ ПОМОЩЬ", b"menu_help")],
        ]
    )
    return rows


def _dashboard_text() -> str:
    from services import accounts as accounts_svc
    from services import dialog_store as dialog_store_svc
    from services import dispatcher as dispatcher_svc
    from services import monitor as monitor_svc
    from services import queue as queue_svc

    wst = dispatcher_svc.worker_status()
    running = bool(wst.get("enabled") and wst.get("loop_running"))
    if running:
        status_title = "🟢 **FIRST DM РАБОТАЮТ**"
        status_hint = "Новые сообщения отправляются\nАктивные диалоги продолжаются всегда"
    else:
        status_title = "⏸ **FIRST DM НА ПАУЗЕ**"
        status_hint = "Новые сообщения из очереди не отправляются\nАктивные диалоги продолжаются всегда"

    pending = queue_svc.count_by_status(queue_svc.STATUS_PENDING)
    claimed = queue_svc.count_by_status(queue_svc.STATUS_CLAIMED)
    first_today = queue_svc.count_first_dm_today()
    first_total = queue_svc.count_first_dm_total()
    dialogs = dialog_store_svc.count_active()
    waiting_reply = dialog_store_svc.count_by_stage(
        dialog_store_svc.STAGE_WAITING_REPLY,
    )
    closed_today = dialog_store_svc.count_closed_today()
    account_count = accounts_svc.count_accounts()
    acc_block = accounts_svc.dashboard_accounts_block(limit=8)
    reauth_count = accounts_svc.count_reauth_required()
    auth_warning = (
        f"⚠️ **{reauth_count} аккаунт требует повторного входа**"
        if reauth_count == 1
        else (
            f"⚠️ **{reauth_count} аккаунта требуют повторного входа**"
            if 1 < reauth_count < 5
            else (
                f"⚠️ **{reauth_count} аккаунтов требуют повторного входа**"
                if reauth_count
                else ""
            )
        )
    )

    if CHANNEL_LINK:
        link_line = "🔗 Канал: ✅ настроен"
    else:
        link_line = "🔗 Канал: ❌ не настроен"
    if AI_DM_ENABLED and OPENAI_API_KEY:
        ai_line = "🤖 AI: ✅ работает"
    elif AI_DM_ENABLED:
        ai_line = "🤖 AI: 🟡 локальные шаблоны"
    else:
        ai_line = "🤖 AI: 🔴 выключен"
    mon = monitor_svc.monitor_status()
    monitor_line = (
        f"📡 Мониторинг: ✅ активен · {mon['connected_count']} акк."
        if mon.get("running")
        else "📡 Мониторинг: ❌ остановлен"
    )

    return join(
        f"✨ **CHANNEL DM BOT · v{app_version()}**",
        DIV,
        status_title,
        status_hint,
        "",
        "📬 **ОЧЕРЕДЬ**",
        f"├ Ждут сообщения: **{pending}**",
        f"├ Сейчас отправляется: **{claimed}**",
        f"├ Отправлено сегодня: **{first_today}**",
        f"└ Отправлено всего: **{first_total}**",
        DIV,
        f"👤 **АККАУНТЫ · {account_count}**",
        acc_block,
        *([auth_warning] if auth_warning else []),
        DIV,
        "💬 **ДИАЛОГИ**",
        f"├ Активные: **{dialogs}**",
        f"├ Ждут ответа: **{waiting_reply}**",
        f"└ Завершено сегодня: **{closed_today}**",
        DIV,
        link_line,
        ai_line,
        monitor_line,
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
        [btn("🗑 Хранение и удаление", b"bc_retention")],
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
            "• **Темп** - как часто писать (меняется в меню)",
            "• **После PeerFlood** - пауза после ограничения",
        ),
        join(
            kv("Темп (акк.)", f"{a_lo // 60}-{a_hi // 60} мин", icon="⏱"),
            kv("После PeerFlood", pf, icon="⚠️"),
            kv("AI", ai, icon="🤖"),
            f"🔗 `{link_short}`",
            f"🗑 Telegram {TELEGRAM_DIALOG_DELETE_DAYS}д · база {LOCAL_DIALOG_TEXT_RETENTION_DAYS}д",
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
    first_dm_total = queue_svc.count_first_dm_total()
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
            kv("First DM сегодня", str(first_dm_today), icon="📨"),
            kv("First DM всего", str(first_dm_total), icon="📊"),
            kv("Не писать", str(optouts), icon="🚫"),
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


def _fmt_min_range(lo: int, hi: int) -> str:
    if lo >= 60 and hi >= 60:
        return f"{lo // 60}-{hi // 60} мин"
    return f"{lo}-{hi} сек"


def _pacing_text() -> str:
    from services import runtime as runtime_svc

    a_lo, a_hi = runtime_svc.get_account_interval_range()
    g_lo, g_hi = runtime_svc.get_global_spacing_range()
    daily = runtime_svc.get_daily_limit()
    r_lo, r_hi = runtime_svc.get_ai_reply_delay_range()
    l_lo, l_hi = runtime_svc.get_auto_link_delay_range()

    return join(
        "⏱ **Темп рассылки**",
        DIV,
        "Плановый ритм, пока нет PeerFlood.",
        "",
        f"👤 Аккаунт: **{_fmt_min_range(a_lo, a_hi)}**",
        f"🌐 Global: **{g_lo}-{g_hi} сек**",
        f"📊 Лимит: **{daily}**/сутки",
        f"🤖 AI-ответ: **{r_lo}-{r_hi} сек**",
        f"🙏 Извинение после рекламы: **{l_lo}-{l_hi} сек**",
        DIV,
        "Нажми параметр, чтобы изменить.",
    )


def _pacing_buttons():
    from services import runtime as runtime_svc

    a_lo, a_hi = runtime_svc.get_account_interval_range()
    g_lo, g_hi = runtime_svc.get_global_spacing_range()
    daily = runtime_svc.get_daily_limit()
    r_lo, r_hi = runtime_svc.get_ai_reply_delay_range()
    l_lo, l_hi = runtime_svc.get_auto_link_delay_range()
    return [
        [btn(f"👤 Аккаунт · {_fmt_min_range(a_lo, a_hi)}", b"pace_edit_acc")],
        [btn(f"🌐 Global · {g_lo}-{g_hi}с", b"pace_edit_glob")],
        [btn(f"📊 Лимит · {daily}/сутки", b"pace_edit_daily")],
        [btn(f"🤖 AI · {r_lo}-{r_hi}с", b"pace_edit_ai")],
        [btn(f"🙏 Извинение · {l_lo}-{l_hi}с", b"pace_edit_link")],
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


def _pace_edit_screen(kind: str):
    """Return (text, buttons) for one parameter editor."""
    from services import runtime as runtime_svc

    hints = {
        "acc": (
            "👤",
            "Между DM одного аккаунта",
            "Пауза после first DM у **одного** аккаунта, перед следующим.",
            "Пришли два числа в **минутах**: `мин макс`\nПример: `10 15`",
            "acc",
        ),
        "glob": (
            "🌐",
            "Между любыми DM в боте",
            "Минимальный зазор между **любыми** first DM (все аккаунты).",
            "Пришли два числа в **секундах**: `мин макс`\nПример: `90 180`",
            "glob",
        ),
        "daily": (
            "📊",
            "Макс. first DM / сутки",
            "Лимит first DM **на один аккаунт** за сутки (UTC).",
            "Пришли одно число:\nПример: `45`",
            "daily",
        ),
        "ai": (
            "🤖",
            "Пауза перед ответом AI",
            "Сколько ждать перед ответом в воронке (explain / link).",
            "Пришли два числа в **секундах**: `мин макс`\nПример: `20 60`",
            "ai",
        ),
        "link": (
            "🙏",
            "Извинение после рекламы",
            "Если юзер молчит после рекламы со ссылкой, через сколько отправить короткое извинение.",
            "Пришли два числа в **секундах**: `мин макс`\nПример: `5 60`",
            "link",
        ),
    }
    emoji, title, desc, how, step = hints[kind]
    if kind == "acc":
        lo, hi = runtime_svc.get_account_interval_range()
        cur = f"Сейчас: **{_fmt_min_range(lo, hi)}**"
    elif kind == "glob":
        lo, hi = runtime_svc.get_global_spacing_range()
        cur = f"Сейчас: **{lo}-{hi} сек**"
    elif kind == "daily":
        cur = f"Сейчас: **{runtime_svc.get_daily_limit()}**/сутки"
    elif kind == "ai":
        lo, hi = runtime_svc.get_ai_reply_delay_range()
        cur = f"Сейчас: **{lo}-{hi} сек**"
    else:
        lo, hi = runtime_svc.get_auto_link_delay_range()
        cur = f"Сейчас: **{lo}-{hi} сек**"

    text = join(
        f"{emoji} **{title}**",
        DIV,
        desc,
        "",
        cur,
        DIV,
        how,
        "",
        "Отмена: /cancel",
    )
    buttons = [back_row(b"bc_pacing"), back_home_row()]
    return text, buttons, step


@bot.on(events.CallbackQuery(pattern=rb"^pace_edit_(acc|glob|daily|ai|link)$"))
async def cb_pace_edit(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    from services.admin_state import set_state

    kind = event.pattern_match.group(1)
    if isinstance(kind, (bytes, bytearray)):
        kind = kind.decode()
    text, buttons, step = _pace_edit_screen(kind)
    set_state(event.sender_id, flow="pacing", step=step)
    await render_menu(event, text, buttons)
    await event.answer()


def _validate_apology_range_input(lo: int, hi: int) -> tuple[int, int]:
    """Reject admin values outside the approved 5-60 second interval."""
    a, b = int(lo), int(hi)
    if a > b:
        a, b = b, a
    if a < 5 or b > 60:
        raise ValueError("apology delay must be within 5-60 seconds")
    return a, b


def _is_pacing_edit(event) -> bool:
    if not getattr(event, "is_private", False):
        return False
    if not is_admin(event.sender_id):
        return False
    from services.admin_state import get_state

    st = get_state(int(event.sender_id)) or {}
    return st.get("flow") == "pacing" and st.get("step") in {
        "acc",
        "glob",
        "daily",
        "ai",
        "link",
    }


@bot.on(events.NewMessage(func=_is_pacing_edit))
async def on_pacing_edit(event: events.NewMessage.Event) -> None:
    from services.admin_state import clear_state, get_state
    from services import runtime as runtime_svc

    st = get_state(event.sender_id) or {}
    step = st.get("step")
    raw = (event.raw_text or "").strip()
    if raw.startswith("/"):
        return
    parts = raw.replace(",", " ").replace("-", " ").replace("-", " ").split()
    try:
        if step == "daily":
            if len(parts) != 1 or not parts[0].isdigit():
                raise ValueError("need 1 int")
            n = runtime_svc.set_daily_limit(int(parts[0]))
            msg = f"Лимит: **{n}**/сутки"
        else:
            if len(parts) != 2 or not all(p.isdigit() for p in parts):
                raise ValueError("need 2 ints")
            a, b = int(parts[0]), int(parts[1])
            if step == "acc":
                # minutes → seconds
                lo, hi = runtime_svc.set_account_interval_range(a * 60, b * 60)
                msg = f"Аккаунт: **{_fmt_min_range(lo, hi)}**"
            elif step == "glob":
                lo, hi = runtime_svc.set_global_spacing_range(a, b)
                msg = f"Global: **{lo}-{hi} сек**"
            elif step == "ai":
                lo, hi = runtime_svc.set_ai_reply_delay_range(a, b)
                msg = f"AI: **{lo}-{hi} сек**"
            elif step == "link":
                a, b = _validate_apology_range_input(a, b)
                lo, hi = runtime_svc.set_auto_link_delay_range(a, b)
                msg = f"Извинение: **{lo}-{hi} сек**"
            else:
                raise ValueError("bad step")
    except (TypeError, ValueError):
        await event.respond(
            notice("warn", "Неверный формат. Смотри пример на экране или /cancel")
        )
        return

    clear_state(event.sender_id)
    await event.respond(
        join("✅ " + msg, DIV, _pacing_text()),
        buttons=[
            [btn("⏱ Темп", b"bc_pacing")],
            back_home_row(),
        ],
    )


def _peerflood_screen() -> str:
    from services import runtime as runtime_svc

    pf = runtime_svc.format_peer_flood_range()
    burst_extra = runtime_svc.format_peer_flood_burst_extra()
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
        f"После **5 PeerFlood за 10 минут**: +**{burst_extra}**",
        "Пресет или свой `min max` в **секундах**:",
    )


def _peerflood_buttons():
    from services.ui import btn

    # data: pf_rng_{lo}_{hi}
    return [
        [
            btn("1-5 мин", b"pf_rng_60_300"),
            btn("3-10 мин", b"pf_rng_180_600"),
        ],
        [
            btn("5-15 мин", b"pf_rng_300_900"),
            btn("10-30 мин", b"pf_rng_600_1800"),
        ],
        [btn("✏️ Свой min max (сек)", b"pf_custom")],
        [btn("🔥 Доп. пауза после 5/10", b"pf_burst")],
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
        f"Допустимо: **{runtime_svc.PEER_FLOOD_MIN_ALLOWED_SEC}-{runtime_svc.PEER_FLOOD_MAX_ALLOWED_SEC}** сек.",
        "Примеры: `180 600` (=3-10 мин), `60 300` (=1-5 мин)",
        "Одно число = фиксированная пауза.",
        "Отмена: /cancel",
    )
    await render_menu(
        event,
        text,
        [back_row(b"bc_peerflood"), back_home_row()],
    )
    await event.answer()


def _peerflood_burst_screen() -> str:
    from services import runtime as runtime_svc

    extra = runtime_svc.format_peer_flood_burst_extra()
    return screen(
        "🔥",
        "Дополнительная пауза 5/10",
        "Срабатывает только если **один аккаунт** получает",
        "**5 PeerFlood за скользящие 10 минут**.",
        "",
        f"Сейчас добавляется: **{extra}**",
        "",
        "Обычный PeerFlood и все остальные настройки не меняются.",
    )


def _peerflood_burst_buttons():
    return [
        [
            btn("5 мин", b"pf_burst_set_300"),
            btn("10 мин", b"pf_burst_set_600"),
        ],
        [
            btn("15 мин", b"pf_burst_set_900"),
            btn("30 мин", b"pf_burst_set_1800"),
        ],
        [btn("✏️ Свое время (сек)", b"pf_burst_custom")],
        back_row(b"bc_peerflood"),
        back_home_row(),
    ]


@bot.on(events.CallbackQuery(data=b"pf_burst"))
async def cb_pf_burst(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    await render_menu(event, _peerflood_burst_screen(), _peerflood_burst_buttons())
    await event.answer()


@bot.on(events.CallbackQuery(pattern=rb"^pf_burst_set_(\d+)$"))
async def cb_pf_burst_set(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    from services import runtime as runtime_svc

    seconds = int(event.pattern_match.group(1))
    value = runtime_svc.set_peer_flood_burst_extra_seconds(seconds)
    await render_menu(event, _peerflood_burst_screen(), _peerflood_burst_buttons())
    await event.answer(
        f"Доп. пауза: {runtime_svc.format_duration(value)}"
    )


@bot.on(events.CallbackQuery(data=b"pf_burst_custom"))
async def cb_pf_burst_custom(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    from services.admin_state import set_state
    from services import runtime as runtime_svc

    set_state(event.sender_id, flow="peerflood", step="burst_extra")
    text = screen(
        "✏️",
        "Своя дополнительная пауза",
        f"Сейчас: **{runtime_svc.format_peer_flood_burst_extra()}**",
        "Пришли **одно число в секундах**.",
        f"Допустимо: **{runtime_svc.PEER_FLOOD_BURST_EXTRA_MIN_SEC}-"
        f"{runtime_svc.PEER_FLOOD_BURST_EXTRA_MAX_SEC}** сек.",
        "Пример: `600` (=10 минут)",
        "Отмена: /cancel",
    )
    await render_menu(
        event,
        text,
        [back_row(b"pf_burst"), back_home_row()],
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
    step = st.get("step")
    raw = (event.raw_text or "").strip()
    if raw.startswith("/"):
        return

    if step == "burst_extra":
        if not raw.isdigit():
            await event.respond("Формат: одно число в секундах, например `600`.")
            return
        value = runtime_svc.set_peer_flood_burst_extra_seconds(int(raw))
        clear_state(event.sender_id)
        await event.respond(
            screen(
                "🔥",
                "Дополнительная пауза 5/10",
                f"Сохранено: **{runtime_svc.format_duration(value)}**",
                "Она добавится только после 5 PeerFlood одного аккаунта за 10 минут.",
            ),
            buttons=[back_home_row()],
        )
        return

    if step != "custom_range":
        return
    parts = raw.replace(",", " ").split()
    if len(parts) == 1 and parts[0].isdigit():
        a, b = runtime_svc.set_peer_flood_range_seconds(int(parts[0]), int(parts[0]))
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
            f"`{a}`-`{b}` сек · рандом при каждом PeerFlood",
        ),
        buttons=[back_home_row()],
    )


@bot.on(events.CallbackQuery(data=b"bc_retention"))
async def cb_retention(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    from services import retention as retention_svc

    stats = retention_svc.retention_stats()
    text = screen(
        "🗑",
        "Хранение и удаление",
        join(
            f"📱 Telegram: удалить через **{TELEGRAM_DIALOG_DELETE_DAYS} дней**",
            "└ С First DM и до конца диалога · у обеих сторон",
            "",
            f"🧠 База бота: очистить тексты через **{LOCAL_DIALOG_TEXT_RETENTION_DAYS} дней**",
            "└ Статусы, статистика и «Не писать» сохраняются",
        ),
        join(
            f"⏳ Ждут удаления в Telegram: **{stats['telegram_pending']}**",
            f"⚠️ Уже готовы к удалению: **{stats['telegram_due']}**",
            f"🧹 Ждут очистки текста: **{stats['local_pending']}**",
            f"🚫 Невозможно удалить без сессии: **{stats['telegram_abandoned']}**",
        ),
        "Если аккаунт временно недоступен, бот повторит удаление позже.",
    )
    await render_menu(
        event,
        text,
        [[btn("🔄 Обновить", b"bc_retention")], back_row(b"menu_settings"), back_home_row()],
    )
    await event.answer()


@bot.on(events.CallbackQuery(data=b"bc_link"))
async def cb_bc_link(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    link = CHANNEL_LINK or "(не задана - CHANNEL_LINK в Railway)"
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
                    "без t.me / опросов / тире -",
                    "антиповтор + запасной пул",
                ]
            ),
        ),
        section(
            "Диалог",
            bullets(
                [
                    "ответ → объяснение канала",
                    "тишина 60-120с → ссылка",
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
        "На главном экране - живые цифры очереди.",
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
        join(
            "На главном экране - живые цифры очереди.",
            "Пауза останавливает только новые First DM. Активные диалоги продолжаются.",
        ),
    )
    await render_menu(event, text, [back_home_row()])
    await event.answer()
