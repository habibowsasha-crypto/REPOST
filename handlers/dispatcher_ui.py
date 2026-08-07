"""Start / pause dispatcher worker UI - unified style.

Pause stops new First DM and every autonomous touch before the first reply.
Only dialogs proven by an incoming user message continue.
"""

from __future__ import annotations

from telethon import events

from config import bot, is_admin
from services import dispatcher as dispatcher_svc
from services import monitor as monitor_svc
from services import queue as queue_svc
from services.menu_ui import render_menu
from services.ui import (
    DENIED,
    OFF,
    ON,
    WAIT,
    back_home_row,
    btn,
    join,
    kv,
    screen,
)


def _worker_body() -> str:
    st = dispatcher_svc.worker_status()
    mon = monitor_svc.monitor_status()
    pending = queue_svc.count_by_status(queue_svc.STATUS_PENDING)
    if st["enabled"] and st["loop_running"]:
        flag, loop = f"{ON} вкл", f"{ON} работает"
    elif st["enabled"]:
        flag, loop = f"{ON} вкл", f"{WAIT} остановлен"
    else:
        flag, loop = f"{OFF} пауза", f"{OFF} остановлен"
    return join(
        f"Рассылка: **{flag}**",
        f"Цикл: **{loop}**",
        kv("Глобальная пауза", f"{st['global_wait_sec']} сек"),
        kv("Мониторинг (очередь)", f"{mon['connected_count']} акк.", icon="📡"),
        kv("Pending", str(pending), icon="⏳"),
        "",
        "Пауза = нет новых First DM и silence follow-up до первого ответа.",
        "Продолжаются только реальные диалоги после входящего сообщения.",
        "Сбор пользователей и мониторинг чатов продолжаются.",
    )


def _toggle_row():
    st = dispatcher_svc.worker_status()
    if st.get("enabled"):
        return [btn("⏸ ПАУЗА FIRST DM", b"bc_toggle")]
    return [btn("▶️ ЗАПУСТИТЬ FIRST DM", b"bc_toggle")]


@bot.on(events.CallbackQuery(data=b"bc_toggle"))
async def cb_bc_toggle(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    st = dispatcher_svc.worker_status()
    if st.get("enabled"):
        await dispatcher_svc.stop_worker()
        await event.answer("Пауза: рассылка остановлена")
    else:
        await dispatcher_svc.start_worker()
        await event.answer("Запущено")
    # Always return to main menu so the toggle label updates.
    from handlers.menu import show_main_menu

    await show_main_menu(event, edit=True)


# Keep legacy callbacks working (redirect to toggle behavior / status)
@bot.on(events.CallbackQuery(data=b"bc_start"))
async def cb_bc_start(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    await dispatcher_svc.start_worker()
    from handlers.menu import show_main_menu

    await show_main_menu(event, edit=True)
    await event.answer("Запущено")


@bot.on(events.CallbackQuery(data=b"bc_stop"))
async def cb_bc_stop(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    await dispatcher_svc.stop_worker()
    from handlers.menu import show_main_menu

    await show_main_menu(event, edit=True)
    await event.answer("Пауза")


@bot.on(events.CallbackQuery(data=b"bc_worker_status"))
async def cb_bc_worker_status(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    st = dispatcher_svc.worker_status()
    emoji = "🟢" if st.get("enabled") else "⏸"
    text = screen(emoji, "Рассылка", _worker_body())
    await render_menu(
        event,
        text,
        [
            _toggle_row(),
            [btn("🔄 Обновить", b"bc_worker_status")],
            back_home_row(),
        ],
    )
    await event.answer()
