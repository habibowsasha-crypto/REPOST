"""Start/stop dispatcher worker UI — unified style."""

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
        flag, loop = f"{OFF} выкл", f"{OFF} остановлен"
    return join(
        f"Флаг: **{flag}**",
        f"Цикл: **{loop}**",
        kv("Глобальная пауза", f"{st['global_wait_sec']} сек"),
        kv("Мониторинг", f"{mon['connected_count']} акк."),
        kv("Pending", str(pending), icon="⏳"),
    )


@bot.on(events.CallbackQuery(data=b"bc_start"))
async def cb_bc_start(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    await dispatcher_svc.start_worker()
    text = screen("▶️", "Старт", "Воркер включён — first DM идут.", _worker_body())
    await render_menu(
        event,
        text,
        [
            [btn("⏹ Стоп", b"bc_stop")],
            [btn("🔄 Обновить", b"bc_worker_status")],
            back_home_row(),
        ],
    )
    await event.answer("Воркер запущен")


@bot.on(events.CallbackQuery(data=b"bc_stop"))
async def cb_bc_stop(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    await dispatcher_svc.stop_worker()
    text = screen(
        "⏹",
        "Стоп",
        "Новые first DM не отправляются.",
        _worker_body(),
    )
    await render_menu(
        event,
        text,
        [
            [btn("▶️ Старт", b"bc_start")],
            [btn("🔄 Обновить", b"bc_worker_status")],
            back_home_row(),
        ],
    )
    await event.answer("Воркер остановлен")


@bot.on(events.CallbackQuery(data=b"bc_worker_status"))
async def cb_bc_worker_status(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    st = dispatcher_svc.worker_status()
    title = "Рассылка"
    if st["enabled"] and st["loop_running"]:
        emoji = "🟢"
    elif st["enabled"]:
        emoji = "🟡"
    else:
        emoji = "🔴"
    text = screen(emoji, title, _worker_body())
    await render_menu(
        event,
        text,
        [
            [btn("▶️ Старт", b"bc_start"), btn("⏹ Стоп", b"bc_stop")],
            [btn("🔄 Обновить", b"bc_worker_status")],
            back_home_row(),
        ],
    )
    await event.answer()
