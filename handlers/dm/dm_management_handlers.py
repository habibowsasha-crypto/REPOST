from __future__ import annotations

import datetime as dt
import html
import math
import re
from typing import Any, Optional

from telethon import Button

from config import ADMIN_ID_LIST, New_Message, Query, bot, callback_message, callback_query, conn
from services.account_profiles import format_account_label
from services.admin_state import is_command_event
from services.dm_task_queue import (
    MAX_DELAY_SECONDS,
    MIN_PACING_SECONDS,
    clear_task_pending,
    count_clearable_pending,
    count_pending,
    format_pending_target,
    get_account_dispatch_state,
    list_pending_page,
    parse_iso,
    remove_chat_source,
    reschedule_task_pending,
    resume_account,
    set_account_pacing,
)
from services.menu_ui import render_menu
from .dm_handlers import (
    delete_dm_task_runtime,
    dm_monitor_tasks,
    ensure_account_dispatcher,
    get_dm_task_operation_lock,
    restart_dm_task_runtime,
    start_dm_task_runtime,
    stop_dm_task_runtime,
)


dm_manage_state: dict[int, dict[str, Any]] = {}
_QUEUE_PAGE_SIZE = 8


def _task_row(task_id: int) -> Optional[dict[str, Any]]:
    row = conn.execute(
        """
        SELECT id, user_id, post_text, photo_url, is_active, created_at,
               delay_min, delay_max,
               (SELECT COUNT(*) FROM dm_watched_chats WHERE dm_task_id=dm_tasks.id),
               (SELECT COUNT(*) FROM dm_sent_log WHERE dm_task_id=dm_tasks.id AND status='sent'),
               (SELECT COUNT(*) FROM dm_sent_log WHERE dm_task_id=dm_tasks.id AND status='privacy'),
               (SELECT COUNT(*) FROM dm_sent_log WHERE dm_task_id=dm_tasks.id AND status='error')
          FROM dm_tasks WHERE id=?
        """,
        (int(task_id),),
    ).fetchone()
    if not row:
        return None
    keys = (
        "id", "user_id", "post_text", "photo_url", "is_active", "created_at",
        "delay_min", "delay_max", "chat_count", "sent_count", "privacy_count",
        "error_count",
    )
    return dict(zip(keys, row))


def _parse_suffix(data: bytes, prefix: str) -> Optional[int]:
    try:
        value = data.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return None
    if not value.startswith(prefix):
        return None
    tail = value[len(prefix):]
    return int(tail) if tail.isdigit() else None


def _task_callback(data: bytes) -> bool:
    try:
        value = data.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return False
    return value.startswith("dm_task_") and value[len("dm_task_"):].isdigit()


def _short(text: Any, limit: int = 36) -> str:
    compact = " ".join(str(text or "").replace("\n", " ").split()).strip() or "Без названия"
    return compact if len(compact) <= limit else compact[: limit - 1].rstrip() + "…"


def _format_duration(seconds: int) -> str:
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds} сек"
    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes} мин" + (f" {sec} сек" if sec else "")
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours} ч" + (f" {minutes} мин" if minutes else "")
    days, hours = divmod(hours, 24)
    return f"{days} дн" + (f" {hours} ч" if hours else "")


def _watched_chat_rows(task_id: int) -> list[tuple[int, str]]:
    rows = conn.execute(
        """
        SELECT w.chat_id,
               COALESCE(d.title, d.username, g.group_username, CAST(w.chat_id AS TEXT))
          FROM dm_watched_chats AS w
          JOIN dm_tasks AS t ON t.id=w.dm_task_id
          LEFT JOIN discovered_groups AS d
            ON d.user_id=t.user_id AND d.group_id=w.chat_id
          LEFT JOIN groups AS g
            ON g.user_id=t.user_id AND g.group_id=w.chat_id
         WHERE w.dm_task_id=?
         ORDER BY lower(COALESCE(d.title, d.username, g.group_username, CAST(w.chat_id AS TEXT)))
        """,
        (int(task_id),),
    ).fetchall()
    return [(int(chat_id), str(title or chat_id)) for chat_id, title in rows]


def _available_chat_rows(task_id: int) -> list[tuple[int, str]]:
    task = _task_row(task_id)
    if not task:
        return []
    rows = conn.execute(
        """
        SELECT g.group_id,
               COALESCE(d.title, d.username, g.group_username, CAST(g.group_id AS TEXT))
          FROM groups AS g
          LEFT JOIN discovered_groups AS d
            ON d.user_id=g.user_id AND d.group_id=g.group_id
         WHERE g.user_id=?
           AND COALESCE(d.is_available, 1)=1
           AND NOT EXISTS (
               SELECT 1 FROM dm_watched_chats AS w
                WHERE w.dm_task_id=? AND w.chat_id=g.group_id
           )
         ORDER BY lower(COALESCE(d.title, d.username, g.group_username, CAST(g.group_id AS TEXT)))
        """,
        (int(task["user_id"]), int(task_id)),
    ).fetchall()
    return [(int(chat_id), str(title or chat_id)) for chat_id, title in rows]


async def _show_task(event, task_id: int) -> None:
    dm_manage_state.pop(event.sender_id, None)
    task = _task_row(task_id)
    if not task:
        await render_menu(
            event,
            "⚠ DM-задача не найдена.",
            buttons=[[Button.inline("📋 К списку задач", b"menu_dm_list")]],
        )
        return

    running_task = dm_monitor_tasks.get(int(task_id))
    running = bool(running_task and not running_task.done())
    active = bool(task["is_active"])
    status = (
        "🟢 активна"
        if active and running
        else ("🟡 ожидает запуска/нет чатов" if active else "🔴 остановлена")
    )
    account = html.escape(
        format_account_label(int(task["user_id"]), include_id=True, max_length=60)
    )
    dispatch = get_account_dispatch_state(int(task["user_id"]))
    queue_count = count_pending(int(task_id))
    photo = "да" if task["photo_url"] else "нет"
    created = html.escape(str(task["created_at"] or "")[:19])
    gate_lines: list[str] = []
    if dispatch.is_paused:
        gate_lines.append(
            f"⛔ Первые DM аккаунта: <b>на паузе</b> ({html.escape(dispatch.pause_reason or 'без причины')})"
        )
    else:
        cooldown = parse_iso(dispatch.cooldown_until)
        now = dt.datetime.now(dt.timezone.utc)
        if cooldown and cooldown > now:
            seconds = math.ceil((cooldown - now).total_seconds())
            gate_lines.append(
                f"⏳ Telegram cooldown: <b>{html.escape(_format_duration(seconds))}</b>"
            )
    gate_text = ("\n" + "\n".join(gate_lines)) if gate_lines else ""
    text = (
        f"📨 <b>DM-задача #{int(task_id)}</b>\n\n"
        f"👤 Аккаунт: <b>{account}</b>\n"
        f"📍 Статус: <b>{status}</b>\n"
        f"💬 Отслеживаемых чатов: <b>{int(task['chat_count'])}</b>\n"
        f"⏱ Задержка после сообщения: "
        f"<b>{int(task['delay_min'] or 0)}–{int(task['delay_max'] or 0)} сек</b>\n"
        f"🧭 Пауза аккаунта между первыми DM: "
        f"<b>{dispatch.pacing_min}–{dispatch.pacing_max} сек</b>{gate_text}\n"
        f"👥 Сейчас в очереди: <b>{queue_count}</b>\n"
        f"✅ Первых DM отправлено: <b>{int(task['sent_count'])}</b>\n"
        f"🔒 Закрытых ЛС: <b>{int(task['privacy_count'])}</b>\n"
        f"⚠ Ошибок отправки: <b>{int(task['error_count'])}</b>\n"
        f"📸 Фото: <b>{photo}</b>\n"
        f"📅 Создана: <code>{created}</code>\n\n"
        "Повторный контакт защищён активным диалогом, завершённым контактом и opt-out."
    )
    buttons = [
        [Button.inline("⏱ Изменить задержку задачи", f"dm_delay_{task_id}".encode())],
        [Button.inline("🧭 Пауза аккаунта между DM", f"dm_pacing_{task_id}".encode())],
        [Button.inline("💬 Управление чатами", f"dm_chats_manage_{task_id}".encode())],
        [Button.inline(f"📋 Посмотреть очередь ({queue_count})", f"dm_queue_{task_id}_0".encode())],
    ]
    if dispatch.is_paused:
        buttons.append(
            [Button.inline("▶️ Возобновить первые DM аккаунта", f"dm_account_resume_{task_id}".encode())]
        )
    if active:
        if not running:
            buttons.append(
                [Button.inline("🔄 Перезапустить задачу", f"dm_task_restart_{task_id}".encode())]
            )
        buttons.append(
            [Button.inline("⏸ Остановить и сохранить очередь", f"dm_task_stop_{task_id}".encode())]
        )
    else:
        buttons.append([Button.inline("▶️ Запустить задачу", f"dm_task_start_{task_id}".encode())])
    buttons.extend(
        [
            [Button.inline("🗑 Удалить задачу", f"dm_task_delete_ask_{task_id}".encode())],
            [Button.inline("◀️ К списку задач", b"menu_dm_list")],
            [Button.inline("🏠 Главное меню", b"menu_home")],
        ]
    )
    await render_menu(event, text, buttons=buttons, parse_mode="html")


@bot.on(Query(data=_task_callback))
async def dm_task_card(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    task_id = _parse_suffix(event.data, "dm_task_")
    if task_id is None:
        await event.answer("Некорректный ID", alert=True)
        return
    await _show_task(event, task_id)
    await event.answer()



@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dm_pacing_") and d.decode(errors="ignore")[10:].isdigit()))
async def dm_pacing_menu(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    task_id = _parse_suffix(event.data, "dm_pacing_")
    task = _task_row(task_id or -1)
    if task_id is None or not task:
        await event.answer("Задача не найдена", alert=True)
        return
    state = get_account_dispatch_state(int(task["user_id"]))
    dm_manage_state.pop(event.sender_id, None)
    await render_menu(
        event,
        f"🧭 <b>Пауза аккаунта между первыми DM</b>\n\n"
        f"Аккаунт: <b>{html.escape(format_account_label(int(task['user_id']), include_id=True, max_length=60))}</b>\n"
        f"Сейчас: <b>{state.pacing_min}–{state.pacing_max} секунд</b>\n\n"
        "Эта пауза общая для всех DM-задач данного Telegram-аккаунта. "
        "Она не заменяет задержку после сообщения пользователя.",
        buttons=[
            [
                Button.inline("10–20 сек", f"dm_pacing_set_{task_id}_10_20".encode()),
                Button.inline("30–60 сек", f"dm_pacing_set_{task_id}_30_60".encode()),
            ],
            [
                Button.inline("60–120 сек", f"dm_pacing_set_{task_id}_60_120".encode()),
                Button.inline("2–5 минут", f"dm_pacing_set_{task_id}_120_300".encode()),
            ],
            [Button.inline("✍️ Ввести вручную", f"dm_pacing_manual_{task_id}".encode())],
            [Button.inline("◀️ Назад", f"dm_task_{task_id}".encode())],
        ],
        parse_mode="html",
    )
    await event.answer()


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dm_pacing_set_")))
async def dm_pacing_set(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    try:
        tail = event.data.decode()[len("dm_pacing_set_"):]
        task_raw, low_raw, high_raw = tail.split("_", 2)
        task_id, low, high = int(task_raw), int(low_raw), int(high_raw)
    except (ValueError, IndexError):
        await event.answer("Некорректные данные", alert=True)
        return
    task = _task_row(task_id)
    if not task:
        await event.answer("Задача не найдена", alert=True)
        return
    try:
        set_account_pacing(int(task["user_id"]), low, high)
    except ValueError:
        await event.answer("Недопустимый диапазон", alert=True)
        return
    await event.answer("Пауза аккаунта обновлена")
    await _show_task(event, task_id)


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dm_pacing_manual_")))
async def dm_pacing_manual(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    task_id = _parse_suffix(event.data, "dm_pacing_manual_")
    if task_id is None or not _task_row(task_id):
        await event.answer("Задача не найдена", alert=True)
        return
    dm_manage_state[event.sender_id] = {"step": "pacing_manual", "task_id": task_id}
    await render_menu(
        event,
        f"✍️ <b>Пауза аккаунта для задачи #{task_id}</b>\n\n"
        "Введите минимум и максимум в секундах через пробел.\n"
        f"Допустимо: от {MIN_PACING_SECONDS} секунд до 30 дней.\n"
        "Пример: <code>30 60</code>.",
        buttons=[[Button.inline("❌ Отмена", f"dm_pacing_{task_id}".encode())]],
        parse_mode="html",
    )
    await event.answer()


@bot.on(New_Message(func=lambda e: e.sender_id in dm_manage_state and
                    dm_manage_state[e.sender_id].get("step") == "pacing_manual" and
                    not is_command_event(e)))
async def dm_pacing_manual_input(event: callback_message) -> None:
    state = dm_manage_state.get(event.sender_id) or {}
    task_id = int(state.get("task_id") or 0)
    task = _task_row(task_id)
    parts = [part for part in re.split(r"[\s,;]+", (event.raw_text or "").strip()) if part]
    try:
        if len(parts) != 2 or not task:
            raise ValueError
        low, high = int(parts[0]), int(parts[1])
        set_account_pacing(int(task["user_id"]), low, high)
    except ValueError:
        await event.respond(
            f"⚠ Нужны два целых числа: {MIN_PACING_SECONDS} ≤ минимум ≤ максимум ≤ {MAX_DELAY_SECONDS}."
        )
        return
    dm_manage_state.pop(event.sender_id, None)
    await event.respond(
        f"✅ Пауза аккаунта изменена на {low}–{high} секунд.",
        buttons=[[Button.inline("⚙️ К задаче", f"dm_task_{task_id}".encode())]],
    )


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dm_account_resume_")))
async def dm_account_resume(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    task_id = _parse_suffix(event.data, "dm_account_resume_")
    task = _task_row(task_id or -1)
    if task_id is None or not task:
        await event.answer("Задача не найдена", alert=True)
        return
    resume_account(int(task["user_id"]))
    ensure_account_dispatcher(int(task["user_id"]))
    await event.answer("Первые DM аккаунта возобновлены")
    await _show_task(event, task_id)


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dm_delay_") and d.decode(errors="ignore")[9:].isdigit()))
async def dm_delay_menu(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    task_id = _parse_suffix(event.data, "dm_delay_")
    dm_manage_state.pop(event.sender_id, None)
    task = _task_row(task_id or -1)
    if task_id is None or not task:
        await event.answer("Задача не найдена", alert=True)
        return
    text = (
        f"⏱ <b>Задержка первого DM задачи #{task_id}</b>\n\n"
        f"Сейчас: <b>{int(task['delay_min'] or 0)}–{int(task['delay_max'] or 0)} секунд</b>\n\n"
        "Задержка считается отдельно для каждого нового пользователя с момента "
        "его сообщения в отслеживаемом чате."
    )
    buttons = [
        [Button.inline("30–60 сек", f"dm_delay_preset_{task_id}_30_60".encode()), Button.inline("60–120 сек", f"dm_delay_preset_{task_id}_60_120".encode())],
        [Button.inline("2–5 минут", f"dm_delay_preset_{task_id}_120_300".encode()), Button.inline("5–15 минут", f"dm_delay_preset_{task_id}_300_900".encode())],
        [Button.inline("✍️ Ввести вручную", f"dm_delay_manual_{task_id}".encode())],
        [Button.inline("◀️ Назад", f"dm_task_{task_id}".encode())],
    ]
    await render_menu(event, text, buttons=buttons, parse_mode="html")
    await event.answer()


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dm_delay_preset_")))
async def dm_delay_preset(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    try:
        _, _, _, task_raw, low_raw, high_raw = event.data.decode().split("_")
        task_id, low, high = int(task_raw), int(low_raw), int(high_raw)
    except (ValueError, IndexError):
        await event.answer("Некорректные данные", alert=True)
        return
    if not _task_row(task_id):
        await event.answer("Задача не найдена", alert=True)
        return
    dm_manage_state[event.sender_id] = {
        "step": "delay_apply",
        "task_id": task_id,
        "delay_min": low,
        "delay_max": high,
    }
    await _show_delay_apply(event, task_id, low, high)
    await event.answer()


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dm_delay_manual_")))
async def dm_delay_manual(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    task_id = _parse_suffix(event.data, "dm_delay_manual_")
    if task_id is None or not _task_row(task_id):
        await event.answer("Задача не найдена", alert=True)
        return
    dm_manage_state[event.sender_id] = {"step": "delay_manual", "task_id": task_id}
    await render_menu(
        event,
        f"✍️ <b>Новая задержка задачи #{task_id}</b>\n\n"
        "Отправьте минимум и максимум в секундах через пробел.\n"
        "Примеры: <code>30 60</code> или <code>60 60</code>.\n\n"
        "Минимум не должен превышать максимум.",
        buttons=[[Button.inline("❌ Отмена", f"dm_delay_{task_id}".encode())]],
        parse_mode="html",
    )
    await event.answer()


@bot.on(New_Message(func=lambda e: e.sender_id in dm_manage_state and
                    dm_manage_state[e.sender_id].get("step") == "delay_manual" and
                    not is_command_event(e)))
async def dm_delay_manual_input(event: callback_message) -> None:
    state = dm_manage_state.get(event.sender_id) or {}
    task_id = int(state.get("task_id") or 0)
    parts = [part for part in re.split(r"[\s,;]+", (event.raw_text or "").strip()) if part]
    try:
        if len(parts) != 2:
            raise ValueError
        low, high = int(parts[0]), int(parts[1])
        if low < 0 or high < low or high > MAX_DELAY_SECONDS:
            raise ValueError
    except ValueError:
        await event.respond(
            "⚠ Нужны два целых числа: 0 ≤ минимум ≤ максимум ≤ 2 592 000. "
            "Например: `30 60`."
        )
        return
    state.update(step="delay_apply", delay_min=low, delay_max=high)
    dm_manage_state[event.sender_id] = state
    await _show_delay_apply(event, task_id, low, high)


async def _show_delay_apply(event, task_id: int, low: int, high: int) -> None:
    task = _task_row(task_id)
    if not task:
        await render_menu(event, "⚠ Задача не найдена.")
        return
    queued = count_pending(task_id)
    warning = ""
    if high > 86400:
        warning = (
            f"\n\n⚠ Максимальная задержка — {_format_duration(high)}. "
            "Проверьте, что это указано намеренно."
        )
    text = (
        f"⏱ <b>Изменить задержку задачи #{task_id}?</b>\n\n"
        f"Было: <b>{int(task['delay_min'] or 0)}–{int(task['delay_max'] or 0)} сек</b>\n"
        f"Станет: <b>{low}–{high} сек</b>\n"
        f"В текущей очереди: <b>{queued}</b> пользователей.{warning}\n\n"
        "Выберите, к кому применить новую задержку."
    )
    buttons = [
        [Button.inline("Только к новым пользователям", f"dm_delay_apply_new_{task_id}".encode())],
        [Button.inline(f"Также к текущей очереди ({queued})", f"dm_delay_apply_current_{task_id}".encode())],
        [Button.inline("❌ Отмена", f"dm_delay_{task_id}".encode())],
    ]
    await render_menu(event, text, buttons=buttons, parse_mode="html")


async def _apply_delay(event: callback_query, task_id: int, *, current_queue: bool) -> None:
    state = dm_manage_state.get(event.sender_id) or {}
    if state.get("step") != "delay_apply" or int(state.get("task_id") or 0) != int(task_id):
        await event.answer("Настройка устарела. Выберите задержку заново.", alert=True)
        return
    low = int(state["delay_min"])
    high = int(state["delay_max"])
    async with get_dm_task_operation_lock(task_id):
        with conn:
            cursor = conn.execute(
                "UPDATE dm_tasks SET delay_min=?, delay_max=? WHERE id=?",
                (low, high, int(task_id)),
            )
        if int(cursor.rowcount or 0) != 1:
            dm_manage_state.pop(event.sender_id, None)
            await event.answer("Задача не найдена", alert=True)
            return
        changed = (
            reschedule_task_pending(task_id, low, high)
            if current_queue
            else 0
        )
    dm_manage_state.pop(event.sender_id, None)
    suffix = (
        f" Время ожидания пересчитано для {changed} пользователей в очереди."
        if current_queue
        else " Текущая очередь сохранила прежнее время ожидания."
    )
    await event.answer("Задержка обновлена")
    await render_menu(
        event,
        f"✅ Задержка задачи #{task_id} изменена на {low}–{high} секунд.{suffix}",
        buttons=[[Button.inline("⚙️ К задаче", f"dm_task_{task_id}".encode())]],
    )


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dm_delay_apply_new_")))
async def dm_delay_apply_new(event: callback_query) -> None:
    task_id = _parse_suffix(event.data, "dm_delay_apply_new_")
    if event.sender_id not in ADMIN_ID_LIST or task_id is None:
        return
    await _apply_delay(event, task_id, current_queue=False)


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dm_delay_apply_current_")))
async def dm_delay_apply_current(event: callback_query) -> None:
    task_id = _parse_suffix(event.data, "dm_delay_apply_current_")
    if event.sender_id not in ADMIN_ID_LIST or task_id is None:
        return
    await _apply_delay(event, task_id, current_queue=True)


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dm_chats_manage_")))
async def dm_chats_manage(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    task_id = _parse_suffix(event.data, "dm_chats_manage_")
    task = _task_row(task_id or -1)
    if task_id is None or not task:
        await event.answer("Задача не найдена", alert=True)
        return
    rows = _watched_chat_rows(task_id)
    lines = [f"💬 <b>Чаты DM-задачи #{task_id}</b>", ""]
    if rows:
        for chat_id, title in rows:
            pending = count_pending(task_id, chat_id)
            lines.append(
                f"✅ {html.escape(_short(title, 45))} — в очереди: <b>{pending}</b>"
            )
    else:
        lines.append("Чаты не выбраны.")
    buttons = [
        [Button.inline("➕ Добавить чат", f"dm_chat_add_menu_{task_id}_0".encode())],
        [Button.inline("➖ Удалить чат", f"dm_chat_remove_menu_{task_id}_0".encode())],
        [Button.inline("◀️ К задаче", f"dm_task_{task_id}".encode())],
    ]
    await render_menu(event, "\n".join(lines), buttons=buttons, parse_mode="html")
    await event.answer()


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dm_chat_add_menu_")))
async def dm_chat_add_menu(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    try:
        tail = event.data.decode()[len("dm_chat_add_menu_"):]
        parts = tail.split("_", 1)
        task_id = int(parts[0])
        page = int(parts[1]) if len(parts) > 1 else 0
    except (ValueError, IndexError):
        await event.answer("Некорректные данные", alert=True)
        return
    if not _task_row(task_id):
        await event.answer("Задача не найдена", alert=True)
        return
    rows = _available_chat_rows(task_id)
    page_size = 10
    pages = max(1, math.ceil(len(rows) / page_size))
    page = max(0, min(page, pages - 1))
    selected = rows[page * page_size:(page + 1) * page_size]
    buttons = [
        [Button.inline(f"➕ {_short(title, 40)}", f"dm_chat_add_{task_id}_{chat_id}".encode())]
        for chat_id, title in selected
    ]
    if pages > 1:
        nav = []
        if page > 0:
            nav.append(Button.inline("⬅️", f"dm_chat_add_menu_{task_id}_{page - 1}".encode()))
        nav.append(Button.inline(f"{page + 1}/{pages}", f"dm_chat_add_menu_{task_id}_{page}".encode()))
        if page + 1 < pages:
            nav.append(Button.inline("➡️", f"dm_chat_add_menu_{task_id}_{page + 1}".encode()))
        buttons.append(nav)
    buttons.append([Button.inline("◀️ Назад", f"dm_chats_manage_{task_id}".encode())])
    text = (
        f"➕ <b>Добавить чат в задачу #{task_id}</b>\n\nВыберите чат:"
        if rows
        else f"✅ Все доступные чаты аккаунта уже добавлены в задачу #{task_id}."
    )
    await render_menu(event, text, buttons=buttons, parse_mode="html")
    await event.answer()


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dm_chat_add_") and not d.decode(errors="ignore").startswith("dm_chat_add_menu_")))
async def dm_chat_add(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    try:
        _, _, _, task_raw, chat_raw = event.data.decode().split("_", 4)
        task_id, chat_id = int(task_raw), int(chat_raw)
    except (ValueError, IndexError):
        await event.answer("Некорректные данные", alert=True)
        return
    task = _task_row(task_id)
    available = {chat for chat, _ in _available_chat_rows(task_id)}
    if not task or chat_id not in available:
        await event.answer("Чат уже добавлен или недоступен", alert=True)
        return
    with conn:
        conn.execute(
            "INSERT OR IGNORE INTO dm_watched_chats (dm_task_id, chat_id) VALUES (?, ?)",
            (task_id, chat_id),
        )
    if task["is_active"]:
        await restart_dm_task_runtime(task_id)
    await event.answer("Чат добавлен")
    await render_menu(
        event,
        f"✅ Чат добавлен в DM-задачу #{task_id}.",
        buttons=[[Button.inline("💬 К чатам задачи", f"dm_chats_manage_{task_id}".encode())]],
    )


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dm_chat_remove_menu_")))
async def dm_chat_remove_menu(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    try:
        tail = event.data.decode()[len("dm_chat_remove_menu_"):]
        parts = tail.split("_", 1)
        task_id = int(parts[0])
        page = int(parts[1]) if len(parts) > 1 else 0
    except (ValueError, IndexError):
        await event.answer("Некорректные данные", alert=True)
        return
    if not _task_row(task_id):
        await event.answer("Задача не найдена", alert=True)
        return
    rows = _watched_chat_rows(task_id)
    page_size = 10
    pages = max(1, math.ceil(len(rows) / page_size))
    page = max(0, min(page, pages - 1))
    selected = rows[page * page_size:(page + 1) * page_size]
    buttons = [
        [Button.inline(f"➖ {_short(title, 40)}", f"dm_chat_remove_ask_{task_id}_{chat_id}".encode())]
        for chat_id, title in selected
    ]
    if pages > 1:
        nav = []
        if page > 0:
            nav.append(Button.inline("⬅️", f"dm_chat_remove_menu_{task_id}_{page - 1}".encode()))
        nav.append(Button.inline(f"{page + 1}/{pages}", f"dm_chat_remove_menu_{task_id}_{page}".encode()))
        if page + 1 < pages:
            nav.append(Button.inline("➡️", f"dm_chat_remove_menu_{task_id}_{page + 1}".encode()))
        buttons.append(nav)
    buttons.append([Button.inline("◀️ Назад", f"dm_chats_manage_{task_id}".encode())])
    await render_menu(
        event,
        f"➖ <b>Удалить чат из задачи #{task_id}</b>\n\nВыберите чат:",
        buttons=buttons,
        parse_mode="html",
    )
    await event.answer()


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dm_chat_remove_ask_")))
async def dm_chat_remove_ask(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    try:
        prefix = "dm_chat_remove_ask_"
        task_raw, chat_raw = event.data.decode()[len(prefix):].split("_", 1)
        task_id, chat_id = int(task_raw), int(chat_raw)
    except (ValueError, IndexError):
        await event.answer("Некорректные данные", alert=True)
        return
    rows = _watched_chat_rows(task_id)
    titles = {gid: title for gid, title in rows}
    if chat_id not in titles:
        await event.answer("Чат уже удалён", alert=True)
        return
    if len(rows) <= 1:
        await event.answer(
            "Нельзя удалить последний чат. Сначала добавьте другой или удалите задачу.",
            alert=True,
        )
        return
    pending = count_pending(task_id, chat_id)
    title = html.escape(_short(titles[chat_id], 55))
    text = (
        f"➖ <b>Удалить «{title}» из задачи #{task_id}?</b>\n\n"
        f"В очереди из этого чата: <b>{pending}</b> пользователей.\n\n"
    )
    buttons = []
    if pending:
        text += "Выберите, что сделать с уже ожидающими пользователями."
        buttons.extend(
            [
                [Button.inline("Оставить их в очереди", f"dm_chat_remove_keep_{task_id}_{chat_id}".encode())],
                [Button.inline("Удалить их из очереди", f"dm_chat_remove_drop_{task_id}_{chat_id}".encode())],
            ]
        )
    else:
        text += "Ожидающих пользователей из этого чата нет."
        buttons.append([Button.inline("✅ Удалить чат", f"dm_chat_remove_keep_{task_id}_{chat_id}".encode())])
    buttons.append([Button.inline("❌ Отмена", f"dm_chats_manage_{task_id}".encode())])
    await render_menu(event, text, buttons=buttons, parse_mode="html")
    await event.answer()


async def _remove_chat(
    event: callback_query,
    task_id: int,
    chat_id: int,
    *,
    drop_queue: bool,
) -> None:
    task = _task_row(task_id)
    rows = _watched_chat_rows(task_id)
    if not task or chat_id not in {gid for gid, _ in rows}:
        await event.answer("Чат уже удалён", alert=True)
        return
    if len(rows) <= 1:
        await event.answer("Нельзя удалить последний чат", alert=True)
        return

    removed_pending = 0
    async with get_dm_task_operation_lock(task_id):
        if drop_queue:
            removed_pending = remove_chat_source(
                task_id,
                chat_id,
                cancel_orphans=True,
            )
        with conn:
            conn.execute(
                "DELETE FROM dm_watched_chats WHERE dm_task_id=? AND chat_id=?",
                (task_id, chat_id),
            )
    if task["is_active"]:
        await restart_dm_task_runtime(task_id)
    await event.answer("Чат удалён")
    extra = (
        f" Из очереди отменено записей без других источников: {removed_pending}."
        if drop_queue
        else " Уже ожидающие пользователи сохранены в очереди."
    )
    await render_menu(
        event,
        f"✅ Чат удалён из задачи #{task_id}.{extra}",
        buttons=[[Button.inline("💬 К чатам задачи", f"dm_chats_manage_{task_id}".encode())]],
    )


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dm_chat_remove_keep_")))
async def dm_chat_remove_keep(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    try:
        tail = event.data.decode()[len("dm_chat_remove_keep_"):]
        task_raw, chat_raw = tail.split("_", 1)
        task_id, chat_id = int(task_raw), int(chat_raw)
    except (ValueError, IndexError):
        await event.answer("Некорректные данные", alert=True)
        return
    await _remove_chat(event, task_id, chat_id, drop_queue=False)


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dm_chat_remove_drop_")))
async def dm_chat_remove_drop(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    try:
        tail = event.data.decode()[len("dm_chat_remove_drop_"):]
        task_raw, chat_raw = tail.split("_", 1)
        task_id, chat_id = int(task_raw), int(chat_raw)
    except (ValueError, IndexError):
        await event.answer("Некорректные данные", alert=True)
        return
    await _remove_chat(event, task_id, chat_id, drop_queue=True)


def _queue_parse(data: bytes) -> Optional[tuple[int, int]]:
    try:
        value = data.decode()
        tail = value[len("dm_queue_"):]
        task_raw, page_raw = tail.split("_", 1)
        return int(task_raw), int(page_raw)
    except (ValueError, IndexError, UnicodeDecodeError):
        return None


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dm_queue_") and not d.decode(errors="ignore").startswith("dm_queue_clear_")))
async def dm_queue_view(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    parsed = _queue_parse(event.data)
    if parsed is None:
        await event.answer("Некорректные данные", alert=True)
        return
    task_id, page = parsed
    if not _task_row(task_id):
        await event.answer("Задача не найдена", alert=True)
        return
    total = count_pending(task_id)
    pages = max(1, math.ceil(total / _QUEUE_PAGE_SIZE))
    page = max(0, min(page, pages - 1))
    rows = list_pending_page(
        task_id,
        offset=page * _QUEUE_PAGE_SIZE,
        limit=_QUEUE_PAGE_SIZE,
    )
    lines = [
        f"📋 <b>Очередь DM-задачи #{task_id}</b>",
        f"\nВсего ожидают: <b>{total}</b>",
    ]
    now = dt.datetime.now(dt.timezone.utc)
    status_names = {
        "pending": "ожидает",
        "claimed": "подготовка",
        "sending": "отправляется",
        "retry_wait": "повтор после Telegram-паузы",
        "unresolved_peer": "ожидает восстановления Telegram-получателя",
        "uncertain_delivery": "неизвестен результат отправки — без автоповтора",
    }
    for index, row in enumerate(rows, start=page * _QUEUE_PAGE_SIZE + 1):
        target = html.escape(_short(format_pending_target(row), 48))
        source = html.escape(
            _short(
                row.get("source_chat_title")
                or row.get("source_chat_id")
                or "Источник не определён",
                45,
            )
        )
        due_at = parse_iso(row.get("eligible_at"))
        if due_at is None or due_at <= now:
            due_text = "готов"
        else:
            due_text = f"через {_format_duration(math.ceil((due_at - now).total_seconds()))}"
        status = status_names.get(str(row.get("status")), str(row.get("status")))
        lines.append(
            f"\n<b>{index}.</b> {target}\n"
            f"Источник: {source}\n"
            f"Статус: <b>{html.escape(status)}</b> | {due_text}"
        )
    buttons = []
    if pages > 1:
        nav = []
        if page > 0:
            nav.append(Button.inline("⬅️", f"dm_queue_{task_id}_{page - 1}".encode()))
        nav.append(Button.inline(f"{page + 1}/{pages}", f"dm_queue_{task_id}_{page}".encode()))
        if page + 1 < pages:
            nav.append(Button.inline("➡️", f"dm_queue_{task_id}_{page + 1}".encode()))
        buttons.append(nav)
    if total:
        buttons.append([Button.inline("🧹 Очистить очередь", f"dm_queue_clear_ask_{task_id}".encode())])
    buttons.extend(
        [
            [Button.inline("🔄 Обновить", f"dm_queue_{task_id}_{page}".encode())],
            [Button.inline("◀️ К задаче", f"dm_task_{task_id}".encode())],
        ]
    )
    await render_menu(event, "\n".join(lines), buttons=buttons, parse_mode="html")
    await event.answer()


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dm_queue_clear_ask_")))
async def dm_queue_clear_ask(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    task_id = _parse_suffix(event.data, "dm_queue_clear_ask_")
    if task_id is None or not _task_row(task_id):
        await event.answer("Задача не найдена", alert=True)
        return
    total = count_pending(task_id)
    clearable = count_clearable_pending(task_id)
    protected = max(0, total - clearable)
    protected_note = (
        f"\nЗаписей с неизвестным результатом отправки, которые останутся для защиты "
        f"от дубля: <b>{protected}</b>."
        if protected
        else ""
    )
    await render_menu(
        event,
        f"⚠️ <b>Очистить очередь задачи #{task_id}?</b>\n\n"
        f"Будет отменено ещё не начатых первых DM: <b>{clearable}</b>."
        f"{protected_note}\n\n"
        "Это не добавит людей в opt-out, не закроет диалоги и не удалит статистику.",
        buttons=[
            [Button.inline("✅ Да, очистить", f"dm_queue_clear_yes_{task_id}".encode())],
            [Button.inline("❌ Отмена", f"dm_queue_{task_id}_0".encode())],
        ],
        parse_mode="html",
    )
    await event.answer()


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dm_queue_clear_yes_")))
async def dm_queue_clear_yes(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    task_id = _parse_suffix(event.data, "dm_queue_clear_yes_")
    if task_id is None or not _task_row(task_id):
        await event.answer("Задача не найдена", alert=True)
        return
    async with get_dm_task_operation_lock(task_id):
        removed = clear_task_pending(task_id, "admin_clear")
    await event.answer("Очередь очищена")
    await render_menu(
        event,
        f"✅ Очередь задачи #{task_id} очищена. Удалено: {removed}.\n\n"
        "Активные и завершённые AI-диалоги, статистика и opt-out не изменены.",
        buttons=[[Button.inline("⚙️ К задаче", f"dm_task_{task_id}".encode())]],
    )


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dm_task_stop_")))
async def dm_task_stop(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    task_id = _parse_suffix(event.data, "dm_task_stop_")
    if task_id is None or not _task_row(task_id):
        await event.answer("Задача не найдена", alert=True)
        return
    await stop_dm_task_runtime(task_id, preserve_queue=True)
    await event.answer("Задача остановлена")
    await _show_task(event, task_id)


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dm_task_start_")))
async def dm_task_start(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    task_id = _parse_suffix(event.data, "dm_task_start_")
    if task_id is None or not _task_row(task_id):
        await event.answer("Задача не найдена", alert=True)
        return
    if not await start_dm_task_runtime(task_id):
        await event.answer("Не удалось запустить: у задачи нет доступных чатов.", alert=True)
        return
    await event.answer("Задача запускается")
    await _show_task(event, task_id)


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dm_task_restart_")))
async def dm_task_restart(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    task_id = _parse_suffix(event.data, "dm_task_restart_")
    task = _task_row(task_id or -1)
    if task_id is None or not task:
        await event.answer("Задача не найдена", alert=True)
        return
    if not task["is_active"]:
        await event.answer("Сначала запустите остановленную задачу", alert=True)
        return
    if not await restart_dm_task_runtime(task_id):
        await event.answer("Не удалось перезапустить задачу", alert=True)
        return
    await event.answer("Задача перезапускается")
    await _show_task(event, task_id)


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dm_task_delete_ask_")))
async def dm_task_delete_ask(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    task_id = _parse_suffix(event.data, "dm_task_delete_ask_")
    task = _task_row(task_id or -1)
    if task_id is None or not task:
        await event.answer("Задача не найдена", alert=True)
        return
    account = html.escape(format_account_label(int(task["user_id"]), include_id=True, max_length=60))
    queued = count_pending(task_id)
    await render_menu(
        event,
        f"🗑 <b>Удалить DM-задачу #{task_id}?</b>\n\n"
        f"Аккаунт: <b>{account}</b>\n"
        f"Чатов: <b>{int(task['chat_count'])}</b>\n"
        f"В очереди: <b>{queued}</b>\n\n"
        "Задача будет удалена. Ожидающие строки будут безопасно отменены либо "
        "переданы другой активной задаче этого аккаунта, если пользователь был "
        "замечен и там. Записи с неизвестным результатом отправки останутся как "
        "защита от дубля. История контактов, AI-диалоги и opt-out сохранятся.",
        buttons=[
            [Button.inline("🗑 Удалить задачу безопасно", f"dm_task_delete_yes_{task_id}".encode())],
            [Button.inline("❌ Отмена", f"dm_task_{task_id}".encode())],
        ],
        parse_mode="html",
    )
    await event.answer()


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dm_task_delete_yes_")))
async def dm_task_delete_yes(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    task_id = _parse_suffix(event.data, "dm_task_delete_yes_")
    if task_id is None:
        await event.answer("Некорректный ID", alert=True)
        return
    if not await delete_dm_task_runtime(task_id):
        await event.answer("Задача уже удалена", alert=True)
        return
    await event.answer("Задача удалена")
    await render_menu(
        event,
        f"✅ DM-задача #{task_id} удалена. Ожидающие строки обработаны безопасно; "
        "защита от неизвестной доставки, история контактов и opt-out сохранены.",
        buttons=[[Button.inline("📋 К списку задач", b"menu_dm_list")]],
    )
