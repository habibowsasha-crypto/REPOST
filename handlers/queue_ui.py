"""Queue admin UI - unified style."""

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
    sent_today = queue_svc.count_first_dm_today()
    sent_total = queue_svc.count_first_dm_total()
    cancelled = queue_svc.count_by_status(queue_svc.STATUS_CANCELLED)
    mon = monitor_svc.monitor_status()
    mon_line = (
        f"📡 Мониторинг: ✅ активен · **{mon['connected_count']} акк.**"
        if mon["running"]
        else "📡 Мониторинг: ❌ остановлен"
    )

    recent = queue_svc.list_recent(10, status=queue_svc.STATUS_PENDING)
    if not recent:
        recent_block = "Очередь пуста. Новые люди появятся из выбранных групп или импорта."
    else:
        recent_block = join(
            "**Последние в очереди**",
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

    return screen(
        "📬",
        "Очередь First DM",
        join(
            f"├ Ждут сообщения: **{pending}**",
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
        [btn("🔄 ОБНОВИТЬ", b"bc_queue")],
        [btn("🧹 ОЧИСТИТЬ ОЧЕРЕДЬ", b"bc_queue_clear")],
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
