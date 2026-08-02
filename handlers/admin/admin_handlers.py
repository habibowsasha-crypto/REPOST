import html
import math
from services.menu_ui import render_menu
from loguru import logger
import re

from telethon import Button
from config import (
    ADMIN_ID_LIST,
    New_Message,
    Query,
    bot,
    callback_message,
    callback_query,
    user_states,
)
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
        [Button.inline("📊 Контакты и диалоги", b"menu_dm_contacts")],
        [Button.inline("🌐 Режим очереди DM", b"menu_queue_mode")],
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



_UNIFIED_PAGE_SIZE = 25
_unified_lead_snapshots: dict[int, list[int]] = {}


def _format_queue_mode_screen() -> tuple[str, list]:
    from services.account_profiles import format_account_label
    from services.dm_unified_queue import (
        MODE_UNIFIED,
        list_unified_participating_accounts,
        unified_queue_stats,
    )

    stats = unified_queue_stats()
    enabled = stats["mode"] == MODE_UNIFIED
    if enabled:
        mode_line = "🟢 <b>Включена</b> — пишут только из общего пула"
    else:
        mode_line = "⚪ <b>Выключена</b> — каждый аккаунт из своей очереди"

    next_line = "—"
    if stats["next_global_send_in_seconds"] is not None:
        sec = int(stats["next_global_send_in_seconds"])
        if sec <= 0:
            next_line = "свободен"
        elif sec < 120:
            next_line = f"через {sec} сек"
        else:
            next_line = f"через {sec // 60} мин {sec % 60} сек"

    by = stats.get("by_status") or {}
    pending = int(by.get("pending", 0))
    retry = int(by.get("retry_wait", 0)) + int(by.get("unresolved_peer", 0))
    reserved = (
        int(by.get("reserved", 0))
        + int(by.get("claimed", 0))
        + int(by.get("sending", 0))
    )
    uncertain = int(by.get("uncertain_delivery", 0))
    ready = int(stats["ready_leads"])
    active = int(stats["active_leads"])
    sent = int(stats["sent_leads"])
    preferred_paused = int(stats["preferred_account_paused_leads"])
    spacing = f"{stats['global_spacing_min']}–{stats['global_spacing_max']} сек"

    accounts = list_unified_participating_accounts()
    if accounts:
        acc_lines = []
        for acc in accounts[:15]:
            try:
                label = html.escape(
                    format_account_label(
                        int(acc["account_user_id"]),
                        include_id=True,
                        max_length=40,
                    )
                )
            except Exception:
                label = str(acc["account_user_id"])
            if acc.get("is_paused"):
                flag = "⏸"
            else:
                flag = "🟢" if enabled else "⚪"
            acc_lines.append(
                f"{flag} {label} · задач: {acc['task_count']}"
            )
        if len(accounts) > 15:
            acc_lines.append(f"… и ещё {len(accounts) - 15}")
        accounts_block = "\n".join(acc_lines)
    else:
        accounts_block = "<i>Нет активных DM-задач с сессией</i>"

    if enabled:
        solo_line = (
            "📋 <b>Соло-отправка:</b> на паузе\n"
            "📡 <b>Мониторинг чатов:</b> работает (люди попадают в общий пул)"
        )
    else:
        solo_line = "📋 <b>Соло-отправка:</b> активна (каждый из своей очереди)"

    text = (
        "🌐 <b>Общая очередь первых DM</b>\n"
        f"{mode_line}\n\n"
        f"📬 <b>К отправке:</b> {ready}\n"
        f"⚙️ <b>В работе:</b> {reserved}\n"
        f"⚠️ <b>Спорных:</b> {uncertain}\n"
        f"⏳ <b>Ожидают / retry:</b> {pending + retry}\n"
        f"👥 <b>Всего в пуле:</b> {active}\n"
        f"✅ <b>Уже написали:</b> {sent}\n"
        f"⏸ <b>Preferred на паузе:</b> {preferred_paused}\n\n"
        f"⏱ <b>Общая пауза:</b> {spacing}\n"
        f"🚀 <b>Слот:</b> {next_line}\n\n"
        f"📦 <b>Модуль:</b> 🤖 AI Первый DM\n"
        f"{solo_line}\n"
        "🔁 После отправки человек снимается из очередей всех аккаунтов\n\n"
        f"<b>Аккаунты в общей очереди ({len(accounts)}):</b>\n"
        f"{accounts_block}"
    )
    toggle_label = (
        "↩️ Выключить общую очередь"
        if enabled
        else "✅ Включить общую очередь"
    )
    buttons = [
        [Button.inline("👥 Лиды пула", b"unified_leads_page_0")],
        [Button.inline(toggle_label, b"queue_mode_confirm")],
        [
            Button.inline("30–60 сек", b"queue_spacing_30_60"),
            Button.inline("60–120 сек", b"queue_spacing_60_120"),
        ],
        [
            Button.inline("2–5 мин", b"queue_spacing_120_300"),
            Button.inline("5–10 мин", b"queue_spacing_300_600"),
        ],
        [Button.inline("✏️ Своя пауза", b"queue_spacing_custom")],
        [
            Button.inline("🔄 Обновить", b"menu_queue_mode"),
            Button.inline("🏠 Меню", b"menu_home"),
        ],
    ]
    return text, buttons


def _format_unified_lead_card(row: dict, *, index: int) -> str:
    from services.account_profiles import format_account_label
    from services.first_dm_modules import first_dm_module_label

    username = " ".join(str(row.get("target_username") or "").split()).strip().lstrip("@")
    first_name = " ".join(str(row.get("target_first_name") or "").split()).strip()
    last_name = " ".join(str(row.get("target_last_name") or "").split()).strip()
    full_name = " ".join(p for p in (first_name, last_name) if p).strip()
    user_id = int(row["target_user_id"])
    if username:
        lead = f"@{html.escape(username[:32])}"
    elif full_name:
        lead = f"{html.escape(full_name[:32])} <i>без @</i>"
    else:
        lead = "<i>без username</i>"

    status = str(row.get("status") or "pending")
    status_map = {
        "pending": "⏳ к отправке",
        "retry_wait": "🔁 retry",
        "unresolved_peer": "❓ peer",
        "reserved": "🔒 резерв",
        "claimed": "🔒 claimed",
        "sending": "📤 sending",
        "uncertain_delivery": "⚠️ спорный",
    }
    status_label = status_map.get(status, status)
    chat = " ".join(str(row.get("source_chat_title") or "").split()) or "—"
    if len(chat) > 36:
        chat = chat[:35] + "…"
    preferred = row.get("preferred_account_user_id")
    preferred_line = "—"
    if preferred is not None:
        try:
            preferred_line = html.escape(
                format_account_label(int(preferred), include_id=True, max_length=36)
            )
        except Exception:
            preferred_line = str(preferred)
    return (
        f"<b>#{index}</b> · {lead}\n"
        f"🆔 <code>{user_id}</code> · {status_label}\n"
        f"💬 {html.escape(chat)}\n"
        f"⭐ {preferred_line}"
    )


async def _show_unified_leads_page(event, page: int, *, restart: bool = False) -> None:
    from services.dm_unified_queue import (
        list_active_unified_lead_ids,
        list_unified_lead_rows_by_ids,
    )

    admin_id = int(event.sender_id)
    if restart or admin_id not in _unified_lead_snapshots:
        _unified_lead_snapshots[admin_id] = list_active_unified_lead_ids()
    snapshot = _unified_lead_snapshots.get(admin_id) or []
    total = len(snapshot)
    pages = max(1, math.ceil(total / _UNIFIED_PAGE_SIZE) if total else 1)
    page = max(0, min(int(page), pages - 1))
    start = page * _UNIFIED_PAGE_SIZE
    page_ids = snapshot[start : start + _UNIFIED_PAGE_SIZE]
    rows = list_unified_lead_rows_by_ids(page_ids)

    if total:
        pos = f"{start + 1}–{start + len(page_ids)}"
        header = (
            f"👥 <b>Лиды общей очереди</b>\n"
            f"Всего: <b>{total}</b> · стр. <b>{page + 1}/{pages}</b> · поз. <b>{pos}</b>\n\n"
        )
    else:
        header = "👥 <b>Лиды общей очереди</b>\nПул пуст.\n\n"

    if not rows:
        body = "<i>Нет активных лидов в снимке.</i>"
    else:
        cards = []
        for i, row in enumerate(rows, start=start + 1):
            cards.append(_format_unified_lead_card(row, index=i))
        body = "\n\n".join(cards)

    buttons: list = []
    nav = []
    if page > 0:
        nav.append(Button.inline("⬅️", f"unified_leads_page_{page - 1}".encode()))
    if page + 1 < pages:
        nav.append(Button.inline("➡️", f"unified_leads_page_{page + 1}".encode()))
    if nav:
        buttons.append(nav)
    buttons.append(
        [
            Button.inline("🔄 Обновить", b"unified_leads_restart"),
            Button.inline("🌐 К режиму", b"menu_queue_mode"),
        ]
    )
    buttons.append([Button.inline("🏠 Меню", b"menu_home")])
    await render_menu(
        event,
        header + body,
        buttons=buttons,
        parse_mode="html",
        link_preview=False,
    )



@bot.on(Query(data=b"menu_queue_mode"))
async def menu_queue_mode(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    await clear_admin_interaction_state(event.sender_id)
    text, buttons = _format_queue_mode_screen()
    await render_menu(event, text, buttons=buttons, parse_mode="html")
    await event.answer()


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("unified_leads_page_")))
async def unified_leads_page(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    raw = event.data.decode(errors="ignore")
    try:
        page = int(raw.rsplit("_", 1)[1])
    except (ValueError, IndexError):
        page = 0
    await _show_unified_leads_page(event, page, restart=False)
    await event.answer()


@bot.on(Query(data=b"unified_leads_restart"))
async def unified_leads_restart(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    await _show_unified_leads_page(event, 0, restart=True)
    await event.answer("Список обновлён")


@bot.on(Query(data=b"queue_mode_confirm"))
async def queue_mode_confirm(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    from services.dm_unified_queue import (
        MODE_UNIFIED,
        get_queue_runtime_state,
        unified_queue_stats,
    )

    state = get_queue_runtime_state()
    stats = unified_queue_stats()
    if state.mode == MODE_UNIFIED:
        title = "Вернуть режим «По аккаунтам»?"
        body = (
            "Отправка снова пойдёт из очереди каждого аккаунта.\n"
            f"В общем пуле останется лидов: <b>{stats['active_leads']}</b> "
            "(данные не удаляются)."
        )
        yes = b"queue_mode_set_per_account"
        yes_label = "↩️ Да, по аккаунтам"
    else:
        title = "Включить «Общую очередь»?"
        body = (
            "Аккаунты будут брать лидов из одного пула с общей паузой "
            f"<b>{state.global_spacing_min}–{state.global_spacing_max} сек</b>.\n"
            f"Готовых лидов сейчас: <b>{stats['ready_leads']}</b>.\n"
            "PeerFlood/FloodWait по-прежнему действуют на аккаунт."
        )
        yes = b"queue_mode_set_unified"
        yes_label = "✅ Да, общая очередь"
    buttons = [
        [Button.inline(yes_label, yes)],
        [Button.inline("✖️ Отмена", b"menu_queue_mode")],
    ]
    await render_menu(event, f"🌐 <b>{title}</b>\n\n{body}", buttons=buttons)
    await event.answer()


@bot.on(Query(data=b"queue_mode_set_unified"))
async def queue_mode_set_unified(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    from services.dm_unified_queue import MODE_UNIFIED, set_queue_mode

    set_queue_mode(MODE_UNIFIED, admin_id=int(event.sender_id))
    await event.answer("Общая очередь включена")
    text, buttons = _format_queue_mode_screen()
    await render_menu(event, text, buttons=buttons, parse_mode="html")


@bot.on(Query(data=b"queue_mode_set_per_account"))
async def queue_mode_set_per_account(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    from services.dm_unified_queue import MODE_PER_ACCOUNT, set_queue_mode

    set_queue_mode(MODE_PER_ACCOUNT, admin_id=int(event.sender_id))
    await event.answer("Режим по аккаунтам")
    text, buttons = _format_queue_mode_screen()
    await render_menu(event, text, buttons=buttons, parse_mode="html")


def _is_queue_spacing_preset(data) -> bool:
    try:
        raw = data.decode(errors="ignore") if isinstance(data, (bytes, bytearray)) else str(data or "")
    except Exception:
        return False
    return raw.startswith("queue_spacing_") and raw != "queue_spacing_custom"


@bot.on(Query(data=_is_queue_spacing_preset))
async def queue_spacing_preset(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    from services.dm_unified_queue import set_global_spacing

    raw = event.data.decode(errors="ignore")
    # queue_spacing_30_60
    try:
        _, _, low_s, high_s = raw.split("_", 3)
        low, high = int(low_s), int(high_s)
    except (ValueError, IndexError):
        await event.answer("Некорректный пресет", alert=True)
        return
    try:
        set_global_spacing(low, high)
    except ValueError as exc:
        await event.answer(str(exc), alert=True)
        return
    await event.answer(f"Пауза {low}–{high} сек")
    text, buttons = _format_queue_mode_screen()
    await render_menu(event, text, buttons=buttons, parse_mode="html")


@bot.on(Query(data=b"queue_spacing_custom"))
async def queue_spacing_custom_start(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    await clear_admin_interaction_state(event.sender_id)
    user_states[int(event.sender_id)] = {
        "flow": "queue_spacing_custom",
        "step": "range",
    }
    await render_menu(
        event,
        "✏️ Введите общую паузу в секундах:\n"
        "<code>мин макс</code>\n\n"
        "Примеры: <code>30 60</code> или <code>120 300</code>\n"
        "Минимум 5 сек, максимум 30 дней.\n\n"
        "Отмена: /cancel",
        buttons=[[Button.inline("✖️ Отмена", b"menu_queue_mode")]],
    )
    await event.answer()


def _is_queue_spacing_custom_input(event) -> bool:
    if event.sender_id not in ADMIN_ID_LIST or not event.text or is_command_event(event):
        return False
    state = user_states.get(event.sender_id)
    return isinstance(state, dict) and state.get("flow") == "queue_spacing_custom"


@bot.on(New_Message(func=_is_queue_spacing_custom_input))
async def queue_spacing_custom_input(event: callback_message) -> None:
    from services.dm_unified_queue import set_global_spacing

    admin_id = int(event.sender_id)
    state = user_states.get(admin_id) or {}
    if state.get("flow") != "queue_spacing_custom":
        return
    parts = (event.text or "").replace(",", " ").split()
    if len(parts) != 2:
        await event.respond(
            "⚠ Нужно два числа: мин и макс в секундах.\nПример: <code>45 90</code>",
            parse_mode="html",
        )
        return
    try:
        low, high = int(parts[0]), int(parts[1])
        set_global_spacing(low, high)
    except ValueError as exc:
        await event.respond(f"⚠ {exc}")
        return
    user_states.pop(admin_id, None)
    text, buttons = _format_queue_mode_screen()
    # respond with fresh screen (not edit)
    await event.respond(
        f"✅ Общая пауза: {low}–{high} сек\n\n" + text.replace("<b>", "").replace("</b>", "").replace("<code>", "").replace("</code>", ""),
        buttons=buttons,
    )
