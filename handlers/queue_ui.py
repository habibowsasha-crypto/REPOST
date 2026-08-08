"""Queue admin UI - unified style."""

from __future__ import annotations

from telethon import Button, events

from config import bot, is_admin
from services import monitor as monitor_svc
from services import queue as queue_svc
from services.menu_ui import render_menu
from services.ui import DENIED, back_home_row, back_row, btn, join, screen

SOURCE_PAGE_SIZE = 10


def queue_screen_text() -> str:
    availability = queue_svc.dashboard_availability_counts()
    pending = availability["total_pending"]
    total_pending = queue_svc.count_by_status(queue_svc.STATUS_PENDING)
    available_enabled = availability["available_enabled"]
    waiting_account_enable = availability["waiting_account_enable"]
    no_available_account = availability["no_available_account"]
    claimed = queue_svc.count_by_status(queue_svc.STATUS_CLAIMED)
    sent_today = queue_svc.count_first_dm_today()
    sent_total = queue_svc.count_first_dm_total()
    cancelled = queue_svc.count_by_status(queue_svc.STATUS_CANCELLED)
    active_source = queue_svc.get_active_source_chat_id()
    source_label = queue_svc.source_chat_label(active_source)
    frozen = queue_svc.count_pending_outside_active_source()
    mon = monitor_svc.monitor_status()
    mon_line = (
        f"📡 Мониторинг: ✅ активен · **{mon['connected_count']} акк.**"
        if mon["running"]
        else "📡 Мониторинг: ❌ остановлен"
    )

    recent = queue_svc.list_recent(
        10,
        status=queue_svc.STATUS_PENDING,
        respect_source_filter=True,
    )
    if not recent:
        recent_block = "В выбранном источнике сейчас нет людей, ожидающих First DM."
    else:
        recent_block = join(
            "**Последние в выбранной очереди**",
            *[queue_svc.format_lead_line(lead) for lead in recent],
        )

    failures = queue_svc.list_recent_failures(5)
    if failures:
        failure_lines = []
        for lead in failures:
            label = queue_svc.format_target_label(lead)
            reason = str(lead.get("failure_reason") or "неизвестно")
            failure_lines.append(f"• {label} · `{reason}`")
        failure_block = join("**Последние конечные ошибки**", *failure_lines)
    else:
        failure_block = ""

    filter_block = join(
        f"🎯 Источник First DM: **{source_label}**",
        f"├ Уникальных пользователей в очереди: **{pending}**",
        f"├ Всего ждут во всех группах: **{total_pending}**",
        f"└ Остальные сейчас не отправляются: **{frozen}**",
    )

    return screen(
        "📬",
        "Очередь First DM",
        filter_block,
        join(
            f"├ Доступны включённым аккаунтам: **{available_enabled}**",
            f"├ Ждут включения аккаунта: **{waiting_account_enable}**",
            f"├ Нет доступного аккаунта: **{no_available_account}**",
            f"├ Сейчас отправляется: **{claimed}**",
            f"├ Отправлено сегодня: **{sent_today}**",
            f"├ Отправлено всего: **{sent_total}**",
            f"└ Отменено: **{cancelled}**",
        ),
        mon_line,
        recent_block,
        failure_block,
    )


def _queue_buttons():
    return [
        [btn("🎯 ИСТОЧНИК FIRST DM", b"bc_queue_source")],
        [btn("🔄 ОБНОВИТЬ", b"bc_queue")],
        [btn("🧹 ОЧИСТИТЬ ОЧЕРЕДЬ", b"bc_queue_clear")],
        back_home_row(),
    ]


def _source_menu_text() -> str:
    active = queue_svc.get_active_source_chat_id()
    groups = queue_svc.list_first_dm_source_groups()
    pending_all = queue_svc.count_by_status(queue_svc.STATUS_PENDING)
    current = queue_svc.source_chat_label(active)
    return screen(
        "🎯",
        "Источник First DM",
        join(
            f"Сейчас: **{current}**",
            f"Людей во всей очереди: **{pending_all}**",
            "",
            "Выбери группу. Люди из остальных групп не удаляются - они просто ждут.",
            "Если человек писал в нескольких группах, он доступен при выборе любой из них.",
            "" if groups else "Источники пока не найдены в очереди.",
        ),
    )


def _source_buttons(page: int = 0) -> list[list[Button]]:
    groups = queue_svc.list_first_dm_source_groups()
    active = queue_svc.get_active_source_chat_id()
    pages = max(1, (len(groups) + SOURCE_PAGE_SIZE - 1) // SOURCE_PAGE_SIZE)
    page = max(0, min(int(page), pages - 1))
    start = page * SOURCE_PAGE_SIZE
    chunk = groups[start : start + SOURCE_PAGE_SIZE]

    buttons: list[list[Button]] = []
    all_mark = "✅" if active is None else "⬜"
    buttons.append(
        [btn(f"{all_mark} Все группы", f"bc_qsrc_all_{page}".encode())]
    )
    for row in chunk:
        chat_id = int(row["source_chat_id"])
        pending = int(row.get("pending_count") or 0)
        title = str(row.get("title") or "").strip()
        username = str(row.get("username") or "").strip().lstrip("@")
        label = title or (f"@{username}" if username else f"чат {chat_id}")
        label = " ".join(label.split())
        if len(label) > 30:
            label = label[:29].rstrip() + "…"
        mark = "✅" if active == chat_id else "⬜"
        buttons.append(
            [
                btn(
                    f"{mark} {label} · {pending}",
                    f"bc_qsrc_set_{chat_id}_{page}".encode(),
                )
            ]
        )

    nav: list[Button] = []
    if page > 0:
        nav.append(btn("⬅️", f"bc_qsrc_page_{page - 1}".encode()))
    nav.append(btn(f"{page + 1}/{pages}", f"bc_qsrc_page_{page}".encode()))
    if page + 1 < pages:
        nav.append(btn("➡️", f"bc_qsrc_page_{page + 1}".encode()))
    buttons.append(nav)
    buttons.append(back_row(b"bc_queue", "◀️ К очереди"))
    buttons.append(back_home_row())
    return buttons


async def _show_source_menu(event, page: int = 0) -> None:
    await render_menu(event, _source_menu_text(), _source_buttons(page))


@bot.on(events.CallbackQuery(data=b"bc_queue"))
async def cb_bc_queue(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    await render_menu(event, queue_screen_text(), _queue_buttons())
    await event.answer()


@bot.on(events.CallbackQuery(data=b"bc_queue_source"))
async def cb_bc_queue_source(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    await _show_source_menu(event, 0)
    await event.answer()


@bot.on(events.CallbackQuery(pattern=rb"^bc_qsrc_page_(\d+)$"))
async def cb_bc_qsrc_page(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    page = int(event.pattern_match.group(1))
    await _show_source_menu(event, page)
    await event.answer()


@bot.on(events.CallbackQuery(pattern=rb"^bc_qsrc_all_(\d+)$"))
async def cb_bc_qsrc_all(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    page = int(event.pattern_match.group(1))
    queue_svc.set_active_source_chat_id(None)
    await _show_source_menu(event, page)
    await event.answer("First DM: все группы")


@bot.on(events.CallbackQuery(pattern=rb"^bc_qsrc_set_(-?\d+)_(\d+)$"))
async def cb_bc_qsrc_set(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    chat_id = int(event.pattern_match.group(1))
    page = int(event.pattern_match.group(2))
    queue_svc.set_active_source_chat_id(chat_id)
    await _show_source_menu(event, page)
    await event.answer(f"Выбрано: {queue_svc.source_chat_label(chat_id)}")


@bot.on(events.CallbackQuery(data=b"bc_queue_clear"))
async def cb_bc_queue_clear(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    n = queue_svc.clear_pending()
    await render_menu(event, queue_screen_text(), _queue_buttons())
    await event.answer(f"Очищено: {n}")
