"""Start/stop dispatcher worker from admin menu."""

from __future__ import annotations

from telethon import Button, events

from config import bot, is_admin
from services import dispatcher as dispatcher_svc
from services import monitor as monitor_svc
from services import queue as queue_svc
from services.menu_ui import back_home_row, back_row, render_menu


def _worker_screen() -> str:
    st = dispatcher_svc.worker_status()
    mon = monitor_svc.monitor_status()
    pending = queue_svc.count_by_status(queue_svc.STATUS_PENDING)
    enabled = "вкл" if st["enabled"] else "выкл"
    loop = "работает" if st["loop_running"] else "остановлен"
    return (
        f"**🚀 Воркер first DM**\n\n"
        f"Флаг: **{enabled}**\n"
        f"Цикл: **{loop}**\n"
        f"Глобальная пауза ещё: **{st['global_wait_sec']}** сек\n"
        f"Мониторинг: {mon['connected_count']} акк.\n"
        f"Pending лидов: **{pending}**\n\n"
        "Random lead × random свободный аккаунт.\n"
        "Текст first DM - из локального пула (AI - шаг 8)."
    )


@bot.on(events.CallbackQuery(data=b"bc_start"))
async def cb_bc_start(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    await dispatcher_svc.start_worker()
    await render_menu(
        event,
        "**▶️ Старт**\n\nВоркер включён.\n\n" + _worker_screen(),
        [
            [Button.inline("⏹ Стоп", b"bc_stop")],
            [Button.inline("🔄 Обновить", b"bc_worker_status")],
            back_row(b"menu_broadcast"),
            back_home_row(),
        ],
    )
    await event.answer("Воркер запущен")


@bot.on(events.CallbackQuery(data=b"bc_stop"))
async def cb_bc_stop(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    await dispatcher_svc.stop_worker()
    await render_menu(
        event,
        "**⏹ Стоп**\n\nНовые first DM не отправляются.\n\n" + _worker_screen(),
        [
            [Button.inline("▶️ Старт", b"bc_start")],
            back_row(b"menu_broadcast"),
            back_home_row(),
        ],
    )
    await event.answer("Воркер остановлен")


@bot.on(events.CallbackQuery(data=b"bc_worker_status"))
async def cb_bc_worker_status(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    await render_menu(
        event,
        _worker_screen(),
        [
            [Button.inline("▶️ Старт", b"bc_start"), Button.inline("⏹ Стоп", b"bc_stop")],
            back_row(b"menu_broadcast"),
            back_home_row(),
        ],
    )
    await event.answer()
