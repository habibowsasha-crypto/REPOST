from __future__ import annotations

import asyncio
import datetime as dt

from loguru import logger
from telethon import Button, TelegramClient
from telethon.sessions import StringSession

from config import ADMIN_ID_LIST, API_HASH, API_ID, Query, bot, callback_query, conn
from services.account_profiles import format_account_label, refresh_stale_account_profiles
from services.dm_task_queue import get_account_dispatch_state, resume_account
from services.menu_ui import render_menu
from services.spambot_monitor import (
    get_spambot_monitor_state,
    mark_spambot_manual_resume,
    set_spambot_auto_resume,
    set_spambot_monitor_enabled,
    spambot_status_label,
)
from utils.telegram import broadcast_status_emoji, get_active_broadcast_groups

_CONNECT_TIMEOUT = 15.0
_REQUEST_TIMEOUT = 10.0


def _is_admin(sender_id) -> bool:
    try:
        return int(sender_id) in ADMIN_ID_LIST
    except (TypeError, ValueError):
        return False


def _parse_account_id(data: bytes, prefix: str) -> int | None:
    try:
        value = data.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return None
    if not value.startswith(prefix):
        return None
    suffix = value[len(prefix):]
    return int(suffix) if suffix.isdigit() else None


def _format_utc(value: str | None) -> str | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return str(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc).strftime("%d.%m.%Y %H:%M UTC")


@bot.on(Query(data=b"my_accounts"))
async def my_accounts(event: callback_query) -> None:
    if not _is_admin(event.sender_id):
        await event.answer("Недоступно", alert=True)
        return
    cursor = conn.cursor()
    try:
        sessions = cursor.execute(
            "SELECT user_id, session_string FROM sessions ORDER BY user_id"
        ).fetchall()
    finally:
        cursor.close()

    if not sessions:
        await render_menu(
            event,
            "❌ У вас нет добавленных аккаунтов",
            buttons=[
                [Button.inline("➕ Добавить аккаунт", b"add_account")],
                [Button.inline("🏠 Главное меню", b"menu_home")],
            ],
        )
        return

    try:
        await refresh_stale_account_profiles(
            [(int(user_id), str(session_string)) for user_id, session_string in sessions],
            timeout_seconds=_REQUEST_TIMEOUT,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        # Profile refresh is display-only. Cached labels remain usable.
        logger.warning(f"Не удалось обновить список аккаунтов: {exc}")

    buttons = [
        [
            Button.inline(
                f"👤 {format_account_label(int(user_id), include_id=True, max_length=42)}",
                f"account_info_{int(user_id)}".encode(),
            )
        ]
        for user_id, _session_string in sessions
    ]
    buttons.append([Button.inline("🏠 Главное меню", b"menu_home")])
    await render_menu(event, "📱 **Список ваших аккаунтов:**", buttons=buttons)


async def _show_account_info(event: callback_query, user_id: int) -> None:
    cursor = conn.cursor()
    try:
        row = cursor.execute(
            "SELECT session_string, account_email FROM sessions WHERE user_id = ?", (user_id,)
        ).fetchone()
        group_rows = cursor.execute(
            """
            SELECT g.group_id, COALESCE(d.title, g.group_username), COALESCE(d.is_available, 1)
            FROM groups AS g
            LEFT JOIN discovered_groups AS d
              ON d.user_id = g.user_id AND d.group_id = g.group_id
            WHERE g.user_id = ?
            ORDER BY lower(COALESCE(d.title, g.group_username))
            """,
            (user_id,),
        ).fetchall()
    finally:
        cursor.close()

    if not row:
        await render_menu(
            event,
            "⚠ Не удалось найти аккаунт.",
            buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]],
        )
        return

    session_string, account_email = row
    monitor = get_spambot_monitor_state(user_id)
    dispatch = get_account_dispatch_state(user_id)
    client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
    try:
        await asyncio.wait_for(client.connect(), timeout=_CONNECT_TIMEOUT)
        authorized = await asyncio.wait_for(
            client.is_user_authorized(), timeout=_REQUEST_TIMEOUT
        )
        if not authorized:
            await render_menu(
                event,
                "⚠ Сессия аккаунта больше не авторизована.",
                buttons=[
                    [Button.inline("❌ Удалить аккаунт", f"delete_account_{user_id}".encode())],
                    [Button.inline("🏠 Главное меню", b"menu_home")],
                ],
            )
            return

        me = await asyncio.wait_for(client.get_me(), timeout=_REQUEST_TIMEOUT)
        name = me.first_name or me.username or "Без имени"
        phone = me.phone or "Не указан"
        email = str(account_email).strip() if account_email else "Не указана"
        active_gids = set(get_active_broadcast_groups(user_id))

        lines = []
        for group_id, title, available in group_rows:
            icon = broadcast_status_emoji(user_id, int(group_id))
            suffix = " ⚠ недоступна" if not available else ""
            lines.append(f"{icon} {title}{suffix}")
        group_list = "\n".join(lines) if lines else "Рабочих групп пока нет."
        mass_active = "🟢 ВКЛ" if active_gids else "🔴 ВЫКЛ"
        monitor_enabled = "🟢 ВКЛ" if monitor.is_enabled else "🔴 ВЫКЛ"
        first_dm_state = (
            f"⛔ пауза ({dispatch.pause_reason or 'без причины'})"
            if dispatch.is_paused
            else "🟢 запущены"
        )

        auto_resume_label = "🟢 ВКЛ" if monitor.auto_resume else "🔴 ВЫКЛ"
        monitor_details = [
            f"🛡 **Мониторинг @SpamBot:** {monitor_enabled}",
            f"▶️ **Автовозобновление DM:** {auto_resume_label}",
            f"📨 **Первые DM аккаунта:** {first_dm_state}",
        ]
        if monitor.is_enabled:
            monitor_details.append(
                f"🔎 **Статус проверки:** {spambot_status_label(monitor)}"
            )
            restriction_until = _format_utc(monitor.restriction_until)
            next_check = _format_utc(monitor.next_check_at)
            if restriction_until:
                monitor_details.append(
                    f"⏳ **Ограничение до:** `{restriction_until}`"
                )
            if next_check:
                monitor_details.append(
                    f"🕒 **Следующая проверка:** `{next_check}`"
                )
        monitor_text = "\n".join(monitor_details)

        toggle_label = (
            "🛡 Выключить мониторинг @SpamBot"
            if monitor.is_enabled
            else "🛡 Включить мониторинг @SpamBot"
        )
        auto_toggle_label = (
            "▶️ Выключить автовозобновление DM"
            if monitor.auto_resume
            else "▶️ Включить автовозобновление DM"
        )
        buttons = [
            [
                Button.inline(
                    toggle_label,
                    f"spambot_monitor_toggle_{user_id}".encode(),
                )
            ],
            [
                Button.inline(
                    auto_toggle_label,
                    f"spambot_auto_resume_toggle_{user_id}".encode(),
                )
            ],
        ]
        if monitor.is_enabled and monitor.status == "free_detected" and dispatch.is_paused:
            buttons.append(
                [
                    Button.inline(
                        "▶️ Возобновить первые DM",
                        f"spambot_monitor_resume_{user_id}".encode(),
                    )
                ]
            )
        buttons.extend(
            [
                [Button.inline("📧 Изменить почту", f"account_email_edit_{user_id}".encode())],
                [Button.inline("🔎 Найти группы аккаунта", f"sync_groups_{user_id}".encode())],
                [Button.inline("📋 Найденные группы", f"discovered_groups_{user_id}_0".encode())],
                [Button.inline("📋 Рабочий список групп", f"groups_{user_id}".encode())],
                [
                    Button.inline("🚀 Начать рассылку во все чаты", f"broadcastAll_{user_id}".encode()),
                    Button.inline("❌ Остановить общую рассылку", f"StopBroadcastAll_{user_id}".encode()),
                ],
                [Button.inline("❌ Удалить этот аккаунт", f"delete_account_{user_id}".encode())],
                [Button.inline("🏠 Главное меню", b"menu_home")],
            ]
        )
        await render_menu(
            event,
            f"📢 **Меню аккаунта {name}:**\n"
            f"🚀 **Массовая рассылка:** {mass_active}\n\n"
            f"📌 **Имя:** {name}\n"
            f"🆔 **Telegram ID:** `{int(user_id)}`\n"
            f"📞 **Номер:** `+{phone}`\n"
            f"📧 **Почта:** `{email}`\n\n"
            f"{monitor_text}\n\n"
            f"📝 **Рабочие группы:**\n{group_list}",
            buttons=buttons,
        )
    except asyncio.TimeoutError:
        logger.warning(f"Тайм-аут открытия аккаунта {user_id}")
        await render_menu(
            event,
            "⚠ Telegram временно не ответил. Сохранённые настройки аккаунта не изменены.",
            buttons=[
                [Button.inline("🔄 Повторить", f"account_info_{user_id}".encode())],
                [Button.inline("🏠 Главное меню", b"menu_home")],
            ],
        )
    except Exception as exc:
        logger.exception(f"Ошибка открытия аккаунта {user_id}: {exc}")
        await render_menu(
            event,
            f"⚠ Не удалось открыть аккаунт: {exc}",
            buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]],
        )
    finally:
        try:
            await asyncio.wait_for(client.disconnect(), timeout=_CONNECT_TIMEOUT)
        except Exception:
            pass


@bot.on(Query(data=lambda data: data.decode(errors="ignore").startswith("account_info_")))
async def handle_account_button(event: callback_query) -> None:
    if not _is_admin(event.sender_id):
        await event.answer("Недоступно", alert=True)
        return
    user_id = _parse_account_id(event.data, "account_info_")
    if user_id is None:
        await event.answer("Некорректный ID аккаунта", alert=True)
        return
    await _show_account_info(event, user_id)


@bot.on(
    Query(
        data=lambda data: data.decode(errors="ignore").startswith(
            "spambot_monitor_toggle_"
        )
    )
)
async def toggle_spambot_monitor(event: callback_query) -> None:
    if not _is_admin(event.sender_id):
        await event.answer("Недоступно", alert=True)
        return
    user_id = _parse_account_id(event.data, "spambot_monitor_toggle_")
    if user_id is None:
        await event.answer("Некорректный ID аккаунта", alert=True)
        return
    exists = conn.execute(
        "SELECT 1 FROM sessions WHERE user_id=?", (int(user_id),)
    ).fetchone()
    if not exists:
        await event.answer("Аккаунт не найден", alert=True)
        return

    current = get_spambot_monitor_state(user_id)
    updated = set_spambot_monitor_enabled(user_id, not current.is_enabled)
    if updated.is_enabled:
        message = "Мониторинг @SpamBot включён"
        if updated.next_check_at:
            message += ". Проверка поставлена в очередь"
    else:
        message = "Мониторинг @SpamBot выключен"
    await event.answer(message)
    await _show_account_info(event, user_id)


@bot.on(
    Query(
        data=lambda data: data.decode(errors="ignore").startswith(
            "spambot_auto_resume_toggle_"
        )
    )
)
async def toggle_spambot_auto_resume(event: callback_query) -> None:
    if not _is_admin(event.sender_id):
        await event.answer("Недоступно", alert=True)
        return
    user_id = _parse_account_id(event.data, "spambot_auto_resume_toggle_")
    if user_id is None:
        await event.answer("Некорректный ID аккаунта", alert=True)
        return
    exists = conn.execute(
        "SELECT 1 FROM sessions WHERE user_id=?", (int(user_id),)
    ).fetchone()
    if not exists:
        await event.answer("Аккаунт не найден", alert=True)
        return

    current = get_spambot_monitor_state(user_id)
    updated = set_spambot_auto_resume(user_id, not current.auto_resume)
    if updated.auto_resume:
        message = (
            "Автовозобновление включено: после ответа @SpamBot "
            "первые DM снимут паузу без ручного нажатия"
        )
    else:
        message = (
            "Автовозобновление выключено: после снятия ограничений "
            "нужна ручная кнопка возобновления"
        )
    await event.answer(message)
    await _show_account_info(event, user_id)


@bot.on(
    Query(
        data=lambda data: data.decode(errors="ignore").startswith(
            "spambot_monitor_resume_"
        )
    )
)
async def resume_first_dms_after_spambot(event: callback_query) -> None:
    if not _is_admin(event.sender_id):
        await event.answer("Недоступно", alert=True)
        return
    user_id = _parse_account_id(event.data, "spambot_monitor_resume_")
    if user_id is None:
        await event.answer("Некорректный ID аккаунта", alert=True)
        return

    monitor = get_spambot_monitor_state(user_id)
    if not monitor.is_enabled or monitor.status != "free_detected":
        await event.answer(
            "@SpamBot ещё не подтвердил отсутствие ограничений",
            alert=True,
        )
        return

    resume_account(user_id)
    mark_spambot_manual_resume(user_id)
    # Local import avoids a handlers.account -> handlers.dm import cycle at startup.
    from handlers.dm.dm_handlers import ensure_account_dispatcher

    ensure_account_dispatcher(user_id)
    await event.answer("Первые DM аккаунта возобновлены")
    await _show_account_info(event, user_id)
