"""Admin UI for the common lead queue."""

from __future__ import annotations

from telethon import Button, events

from config import bot, is_admin
from services import monitor as monitor_svc
from services import queue as queue_svc
from services.menu_ui import back_home_row, back_row, render_menu


def queue_screen_text() -> str:
    pending = queue_svc.count_by_status(queue_svc.STATUS_PENDING)
    claimed = queue_svc.count_by_status(queue_svc.STATUS_CLAIMED)
    sent = queue_svc.count_by_status(queue_svc.STATUS_SENT)
    mon = monitor_svc.monitor_status()
    mon_line = (
        f"Мониторинг: **вкл** ({mon['connected_count']} акк.)"
        if mon["running"]
        else "Мониторинг: **выкл**"
    )
    lines = [
        "**📥 Очередь**\n",
        mon_line,
        f"Pending: **{pending}** | Claimed: {claimed} | Sent: {sent}\n",
    ]
    recent = queue_svc.list_recent(12, status=queue_svc.STATUS_PENDING)
    if not recent:
        lines.append("Pending пуст. Нужны: аккаунт с участием + выбранные чаты + сообщения в группах.")
    else:
        lines.append("Последние pending:")
        for lead in recent:
            lines.append(queue_svc.format_lead_line(lead))
    return "\n".join(lines)


@bot.on(events.CallbackQuery(data=b"bc_queue"))
async def cb_bc_queue(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    await render_menu(
        event,
        queue_screen_text(),
        [
            [Button.inline("🔄 Обновить", b"bc_queue")],
            [Button.inline("🧹 Очистить pending", b"bc_queue_clear")],
            [Button.inline("📡 Перезапуск мониторинга", b"bc_monitor_refresh")],
            back_row(b"menu_broadcast"),
            back_home_row(),
        ],
    )
    await event.answer()


@bot.on(events.CallbackQuery(data=b"bc_queue_clear"))
async def cb_bc_queue_clear(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    n = queue_svc.clear_pending()
    await render_menu(
        event,
        f"**🧹 Очистка**\n\nУдалено pending: **{n}**\n\n" + queue_screen_text(),
        [
            [Button.inline("🔄 Обновить", b"bc_queue")],
            back_row(b"menu_broadcast"),
            back_home_row(),
        ],
    )
    await event.answer(f"Удалено: {n}")


@bot.on(events.CallbackQuery(data=b"bc_monitor_refresh"))
async def cb_bc_monitor_refresh(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    await event.answer("Перезапуск…")
    try:
        await monitor_svc.refresh_monitor()
    except Exception as exc:
        await render_menu(
            event,
            f"⚠ Ошибка мониторинга: `{type(exc).__name__}`",
            [back_row(b"bc_queue"), back_home_row()],
        )
        return
    await render_menu(
        event,
        queue_screen_text(),
        [
            [Button.inline("🔄 Обновить", b"bc_queue")],
            [Button.inline("🧹 Очистить pending", b"bc_queue_clear")],
            [Button.inline("📡 Перезапуск мониторинга", b"bc_monitor_refresh")],
            back_row(b"menu_broadcast"),
            back_home_row(),
        ],
    )
