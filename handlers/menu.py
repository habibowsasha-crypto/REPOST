"""Admin menu navigation."""

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


def _main_menu_buttons() -> list[list[Button]]:
    return [
        [
            Button.inline("🔄 Обновить", b"menu_refresh"),
            Button.inline("📊 Детали", b"bc_status"),
        ],
        [
            Button.inline("👤 Аккаунты", b"menu_accounts"),
            Button.inline("🚀 Рассылка", b"menu_broadcast"),
        ],
        [
            Button.inline("📥 Очередь", b"bc_queue"),
            Button.inline("🚫 Opt-out", b"menu_optout"),
        ],
        [
            Button.inline("🤖 AI", b"menu_ai"),
            Button.inline("ℹ️ Помощь", b"menu_help"),
        ],
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
        w_icon, w_txt = "🟢", "работает"
    elif wst.get("enabled"):
        w_icon, w_txt = "🟡", "пауза цикла"
    else:
        w_icon, w_txt = "🔴", "выключена"

    mon_icon = "🟢" if mon.get("running") and mon_n else "🔴"
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
        ai_line = f"🟢 AI · `{AI_MODEL}`"
    elif AI_DM_ENABLED:
        ai_line = "🟡 AI без ключа · шаблоны"
    else:
        ai_line = "🔴 AI выкл"

    return "\n".join(
        [
            "✨ **Channel DM Bot**",
            "──────────────",
            f"{w_icon} Рассылка: **{w_txt}**",
            f"{mon_icon} Мониторинг: **{mon_txt}**",
            f"👤 Аккаунты: **{active_acc}** из {total_acc}",
            "",
            "📬 **Очередь**",
            f"├ ⏳ ждут DM  **{pending}**",
            f"├ 🔄 в работе  **{claimed}**",
            f"├ ✅ написали  **{sent}**",
            f"└ 🗑 отменены  **{cancelled}**",
            "",
            f"💬 Диалоги: **{dialogs}**",
            f"📨 First DM сегодня: **{first_today}**",
            f"🚫 Opt-out: **{optouts}**",
            "",
            f"🔗 `{link_short}`",
            ai_line,
            "──────────────",
            "👇 разделы ниже",
        ]
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
    if wst.get("enabled") and wst.get("loop_running"):
        w = "🟢 работает"
    elif wst.get("enabled"):
        w = "🟡 пауза цикла"
    else:
        w = "🔴 выкл"
    pending = queue_svc.count_by_status(queue_svc.STATUS_PENDING)
    sent = queue_svc.count_by_status(queue_svc.STATUS_SENT)
    today = queue_svc.count_first_dm_today()
    text = "\n".join(
        [
            "🚀 **Рассылка**",
            "──────────────",
            f"Статус: **{w}**",
            f"⏳ В очереди: **{pending}**",
            f"✅ Уже написали: **{sent}**",
            f"📨 Сегодня: **{today}**",
            "──────────────",
            "▶️ Старт — раздать first DM",
            "⏹ Стоп — остановить",
            "📥 Очередь — кто ждёт",
            "⚙️ Темп — паузы",
        ]
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
    mon_txt = (
        f"🟢 вкл · {mon['connected_count']} акк."
        if mon["running"]
        else "🔴 выкл"
    )
    pending = queue_svc.count_by_status(queue_svc.STATUS_PENDING)
    claimed = queue_svc.count_by_status(queue_svc.STATUS_CLAIMED)
    sent = queue_svc.count_by_status(queue_svc.STATUS_SENT)
    cancelled = queue_svc.count_by_status(queue_svc.STATUS_CANCELLED)
    dialog_active = dialog_store_svc.count_active()
    first_dm_today = queue_svc.count_first_dm_today()
    optouts = opt_out_svc.count()
    wst = dispatcher_svc.worker_status()
    if wst["enabled"] and wst["loop_running"]:
        w_txt = "🟢 работает"
    elif wst["enabled"]:
        w_txt = "🟡 цикл стоп"
    else:
        w_txt = "🔴 выкл"
    wait = wst.get("global_wait_sec") or 0
    return "\n".join(
        [
            "📊 **Подробный статус**",
            "──────────────",
            f"Рассылка: {w_txt}",
            f"Пауза до след. DM: **~{wait:.0f}с**",
            f"Мониторинг: {mon_txt}",
            f"Аккаунты: **{active}** / {total}",
            "",
            "📬 **Очередь**",
            f"⏳ {pending}  ·  🔄 {claimed}  ·  ✅ {sent}  ·  🗑 {cancelled}",
            "",
            f"💬 Диалоги: **{dialog_active}**",
            f"📨 Сегодня first DM: **{first_dm_today}**",
            f"🚫 Opt-out: **{optouts}**",
            "",
            f"🔗 `{link}`",
            f"🤖 AI: {'вкл' if AI_DM_ENABLED else 'выкл'} · `{AI_MODEL}`",
            f"👮 Админы: {admins}",
        ]
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


def _pacing_text() -> str:
    acc_min_m = DM_ACCOUNT_INTERVAL_MIN // 60
    acc_max_m = DM_ACCOUNT_INTERVAL_MAX // 60
    return "\n".join(
        [
            "⚙️ **Темп**",
            "──────────────",
            f"На аккаунт: **{acc_min_m}–{acc_max_m} мин** между first DM",
            f"Глобально: **{DM_GLOBAL_SPACING_MIN}–{DM_GLOBAL_SPACING_MAX} сек**",
            f"Лимит / аккаунт / сутки: **{DM_DAILY_LIMIT_PER_ACCOUNT}**",
            f"Ответ AI: **{AI_REPLY_DELAY_MIN}–{AI_REPLY_DELAY_MAX} сек**",
            f"Авто-ссылка: **{AI_AUTO_LINK_DELAY_MIN}–{AI_AUTO_LINK_DELAY_MAX} сек**",
            f"PeerFlood cooldown: **{PEER_FLOOD_MIN_COOLDOWN_MINUTES} мин**",
            f"SpamBot auto-resume: **{'да' if SPAMBOT_AUTO_RESUME else 'нет'}**",
            "",
            "Меняется через Variables в Railway.",
        ]
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
    link = CHANNEL_LINK or "(не задана — CHANNEL_LINK в Railway)"
    pitch = CHANNEL_PITCH or "(пусто)"
    text = "\n".join(
        [
            "🔗 **Ссылка канала**",
            "──────────────",
            f"`{link}`",
            "",
            f"Pitch: {pitch}",
            "",
            "Сейчас только через env Railway.",
        ]
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
        await event.answer("Нет доступа", alert=True)
        return
    key_ok = "есть" if OPENAI_API_KEY else "нет"
    ai_on = "🟢 включён" if AI_DM_ENABLED else "🔴 выкл"
    text = "\n".join(
        [
            "🤖 **AI**",
            "──────────────",
            f"Статус: **{ai_on}**",
            f"Ключ OpenAI: **{key_ok}**",
            f"Модель: `{AI_MODEL}`",
            "",
            "**First DM**",
            "• короткий вопрос без ссылки",
            "• без t.me / опросов / тире —",
            "• антиповтор + запасной пул",
            "",
            "**Диалог**",
            "• ответ → объяснение канала",
            "• тишина 60–120с → ссылка",
            "• «не пиши» → opt-out",
        ]
    )
    await render_menu(event, text, [back_home_row()])
    await event.answer()


@bot.on(events.CallbackQuery(data=b"menu_help"))
async def cb_help(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    text = "\n".join(
        [
            "ℹ️ **Помощь**",
            "──────────────",
            "1. Добавь аккаунт → включи участие",
            "2. Чаты → обнови → выбери / режим «все»",
            "3. Рассылка → Старт",
            "",
            "**Команды**",
            "`/start`  `/menu`  `меню`",
            "`/ping`  `/status`  `/cancel`",
            "",
            "На главном экране — живые цифры очереди.",
        ]
    )
    await render_menu(event, text, [back_home_row()])
    await event.answer()
