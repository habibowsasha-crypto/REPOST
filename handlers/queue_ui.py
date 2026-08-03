"""Queue admin UI — unified style."""

from __future__ import annotations

from telethon import events

from config import bot, is_admin
from services import monitor as monitor_svc
from services import queue as queue_svc
from services.menu_ui import render_menu
from services.ui import (
    DENIED,
    OFF,
    ON,
    back_home_row,
    btn,
    join,
    kv,
    screen,
    section,
    tree,
)


def queue_screen_text() -> str:
    pending = queue_svc.count_by_status(queue_svc.STATUS_PENDING)
    claimed = queue_svc.count_by_status(queue_svc.STATUS_CLAIMED)
    sent = queue_svc.count_by_status(queue_svc.STATUS_SENT)
    cancelled = queue_svc.count_by_status(queue_svc.STATUS_CANCELLED)
    mon = monitor_svc.monitor_status()
    if mon["running"]:
        mon_line = kv("Мониторинг", f"{mon['connected_count']} акк. онлайн", icon=ON)
    else:
        mon_line = kv("Мониторинг", "выкл", icon=OFF)

    recent = queue_svc.list_recent(12, status=queue_svc.STATUS_PENDING)
    if not recent:
        recent_block = (
            "Pending пуст.\n"
            "Нужны: участие + чаты в мониторинге + сообщения в группах."
        )
    else:
        recent_block = join(
            "Последние «ждут»:",
            *[queue_svc.format_lead_line(lead) for lead in recent],
        )

    return screen(
        "📬",
        "Очередь",
        mon_line,
        section(
            "Сводка",
            tree(
                [
                    ("⏳", "ждут DM", pending),
                    ("🔄", "в работе", claimed),
                    ("✅", "написали", sent),
                    ("🗑", "отменены", cancelled),
                ]
            ),
        ),
        recent_block,
    )


def _queue_buttons():
    return [
        [btn("🔄 Обновить", b"bc_queue")],
        [btn("🧹 Очистить «ждут»", b"bc_queue_clear")],
        back_home_row(),
    ]


@bot.on(events.CallbackQuery(data=b"bc_queue"))
async def cb_bc_queue(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    await render_menu(event, queue_screen_text(), _queue_buttons())
    await event.answer()


@bot.on(events.CallbackQuery(data=b"bc_queue_clear"))
async def cb_bc_queue_clear(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    n = queue_svc.clear_pending()
    await render_menu(event, queue_screen_text(), _queue_buttons())
    await event.answer(f"Очищено: {n}")
