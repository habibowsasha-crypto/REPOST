from __future__ import annotations

import asyncio
import re
from typing import Any

from loguru import logger
from telethon import Button, TelegramClient
from telethon.errors import (
    FloodWaitError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    SessionPasswordNeededError,
)
from telethon.sessions import StringSession

from config import (
    ADMIN_ID_LIST,
    API_HASH,
    API_ID,
    New_Message,
    Query,
    bot,
    broadcast_all_state,
    callback_message,
    callback_query,
    code_waiting,
    conn,
    email_waiting,
    password_waiting,
    phone_waiting,
    user_clients,
    user_states,
)
from services.account_profiles import save_account_profile
from services.admin_state import clear_admin_interaction_state, is_command_event
from services.menu_ui import render_menu

_AUTH_CONNECT_TIMEOUT = 20.0
_AUTH_REQUEST_TIMEOUT = 30.0
_AUTH_DISCONNECT_TIMEOUT = 10.0

_EMAIL_RE = re.compile(
    r"^(?=.{3,254}$)[A-Za-z0-9.!#$%&'*+/=?^_{|}~-]+@"
    r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+$"
)


def _is_admin(admin_id: Any) -> bool:
    try:
        return int(admin_id) in ADMIN_ID_LIST
    except (TypeError, ValueError):
        return False


def _mask_phone(phone: str) -> str:
    if len(phone) <= 6:
        return "***"
    return f"{phone[:3]}***{phone[-3:]}"


def _normalize_email(value: Any) -> str | None:
    email = str(value or "").strip().lower()
    if not _EMAIL_RE.fullmatch(email):
        return None
    return email


async def _delete_sensitive_message(event: Any) -> None:
    """Best-effort removal of login codes and 2FA passwords from admin chat."""
    delete = getattr(event, "delete", None)
    if not callable(delete):
        return
    try:
        await delete()
    except Exception as exc:
        logger.debug(f"Не удалось удалить чувствительное сообщение: {exc}")


async def _drop_temp_client(admin_id: int) -> None:
    client = user_clients.pop(admin_id, None)
    if client is None:
        return
    try:
        if client.is_connected():
            await asyncio.wait_for(
                client.disconnect(), timeout=_AUTH_DISCONNECT_TIMEOUT
            )
    except Exception as exc:
        logger.debug(f"Не удалось отключить временный Telegram-клиент {admin_id}: {exc}")


async def _save_authorized_account(admin_id: int, client: TelegramClient) -> int:
    """Persist the MTProto session without overwriting an existing account e-mail."""
    me = await asyncio.wait_for(
        client.get_me(), timeout=_AUTH_REQUEST_TIMEOUT
    )
    session_string = client.session.save()
    with conn:
        conn.execute(
            """
            INSERT INTO sessions (
                user_id, session_string, username, first_name, last_name,
                account_email, profile_updated_at
            )
            VALUES (?, ?, NULL, NULL, NULL, NULL, NULL)
            ON CONFLICT(user_id) DO UPDATE SET
                session_string = excluded.session_string
            """,
            (me.id, session_string),
        )
    save_account_profile(me)
    return int(me.id)


async def _prompt_optional_email(
    event: Any,
    admin_id: int,
    account_user_id: int,
    success_text: str,
) -> None:
    """Ask for optional account e-mail after Telegram authorization succeeds."""
    email_waiting[admin_id] = {
        "user_id": int(account_user_id),
        "waiting": True,
        "last_message_id": int(getattr(getattr(event, "message", None), "id", 0) or 0),
    }
    current = conn.execute(
        "SELECT account_email FROM sessions WHERE user_id=?",
        (int(account_user_id),),
    ).fetchone()
    current_email = str(current[0]).strip() if current and current[0] else ""
    existing_note = (
        f"\n\nСейчас сохранена почта: `{current_email}`. Можно отправить новую "
        "или пропустить, чтобы оставить прежнюю."
        if current_email
        else ""
    )
    await event.respond(
        f"{success_text}\n\n"
        "📧 Отправьте e-mail, к которому привязан этот Telegram-аккаунт.\n"
        "Это необязательный шаг — почта используется только как заметка администратора."
        f"{existing_note}",
        buttons=[
            [Button.inline("⏭ Пропустить", f"account_email_skip_{account_user_id}".encode())],
            [Button.inline("🏠 Главное меню", b"menu_home")],
        ],
    )


async def _show_account_ready(event: Any, account_user_id: int, email: str | None) -> None:
    email_text = email or "Не указана"
    await event.respond(
        "✅ Аккаунт полностью добавлен.\n\n"
        f"🆔 Telegram ID: `{int(account_user_id)}`\n"
        f"📧 Почта: `{email_text}`",
        buttons=[
            [Button.inline("🔎 Найти группы аккаунта", f"sync_groups_{account_user_id}".encode())],
            [Button.inline("👤 Мои аккаунты", b"my_accounts")],
            [Button.inline("🏠 Главное меню", b"menu_home")],
        ],
    )


@bot.on(Query(data=b"add_account"))
async def add_account(event: callback_query) -> None:
    if not _is_admin(event.sender_id):
        await event.answer("Недоступно", alert=True)
        return
    admin_id = int(event.sender_id)
    await clear_admin_interaction_state(admin_id)
    await _drop_temp_client(admin_id)
    phone_waiting[admin_id] = True
    await render_menu(
        event,
        "📲 Напишите номер телефона аккаунта в формате: `+xxxxxxxxxxx`",
        buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]],
    )


@bot.on(
    New_Message(
        func=lambda e: e.sender_id in phone_waiting
        and bool(e.text)
        and e.text.startswith("+")
        and e.text[1:].isdigit()
        and not is_command_event(e)
    )
)
async def send_code_for_phone(event: callback_message) -> None:
    if not _is_admin(event.sender_id):
        return
    admin_id = int(event.sender_id)
    phone_number = event.text.strip()
    logger.info(f"Отправляю код подтверждения для {_mask_phone(phone_number)}")

    await _drop_temp_client(admin_id)
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    user_clients[admin_id] = client
    try:
        await asyncio.wait_for(
            client.connect(), timeout=_AUTH_CONNECT_TIMEOUT
        )
        await event.respond("⏳ Отправляю код подтверждения...")
        await asyncio.wait_for(
            client.send_code_request(phone_number), timeout=_AUTH_REQUEST_TIMEOUT
        )
        code_waiting[admin_id] = phone_number
        phone_waiting.pop(admin_id, None)
        await event.respond(
            "✅ Код отправлен!\n\n"
            "⏰ Код действует ограниченное время.\n"
            "📱 Введите его сюда как можно скорее:"
        )
        logger.info("Код подтверждения отправлен")
    except FloodWaitError as exc:
        phone_waiting.pop(admin_id, None)
        code_waiting.pop(admin_id, None)
        await _drop_temp_client(admin_id)
        seconds = max(0, int(exc.seconds))
        hours, rem = divmod(seconds, 3600)
        minutes, seconds = divmod(rem, 60)
        message = (
            "⚠ Telegram временно ограничил повторную отправку кода. "
            f"Подождите {hours} ч {minutes} мин {seconds} сек."
        )
        logger.warning(message)
        await event.respond(message)
    except Exception as exc:
        phone_waiting.pop(admin_id, None)
        code_waiting.pop(admin_id, None)
        await _drop_temp_client(admin_id)
        logger.error(f"Ошибка отправки кода: {exc}")
        await event.respond(
            f"⚠ Не удалось отправить код: {exc}\n"
            "Начните заново через «Добавить аккаунт»."
        )


@bot.on(
    New_Message(
        func=lambda e: e.sender_id in code_waiting
        and bool(e.text)
        and e.text.isdigit()
        and e.sender_id not in broadcast_all_state
        and not is_command_event(e)
    )
)
async def get_code(event: callback_message) -> None:
    if not _is_admin(event.sender_id):
        return
    admin_id = int(event.sender_id)
    phone_number = code_waiting.get(admin_id)
    client = user_clients.get(admin_id)
    if not phone_number or client is None:
        code_waiting.pop(admin_id, None)
        await event.respond("⚠ Сессия авторизации потеряна. Начните добавление аккаунта заново.")
        return

    code = event.text.strip()
    message_id = int(getattr(getattr(event, "message", None), "id", 0) or 0)
    try:
        await asyncio.wait_for(
            client.sign_in(phone_number, code), timeout=_AUTH_REQUEST_TIMEOUT
        )
        await _delete_sensitive_message(event)
        account_user_id = await _save_authorized_account(admin_id, client)
        code_waiting.pop(admin_id, None)
        password_waiting.pop(admin_id, None)
        await _drop_temp_client(admin_id)
        await _prompt_optional_email(
            event,
            admin_id,
            account_user_id,
            "✅ Авторизация прошла успешно!",
        )
    except SessionPasswordNeededError:
        await _delete_sensitive_message(event)
        password_waiting[admin_id] = {
            "waiting": True,
            "last_message_id": message_id,
        }
        code_waiting.pop(admin_id, None)
        await event.respond("🔐 На аккаунте включён пароль 2FA. Отправьте пароль:")
    except PhoneCodeExpiredError:
        await _delete_sensitive_message(event)
        code_waiting.pop(admin_id, None)
        await _drop_temp_client(admin_id)
        await event.respond(
            "⏰ Код подтверждения истёк. Нажмите «Добавить аккаунт», чтобы получить новый."
        )
    except PhoneCodeInvalidError:
        await _delete_sensitive_message(event)
        await event.respond(
            "❌ Неверный код подтверждения. Проверьте код и введите его ещё раз."
        )
    except Exception as exc:
        await _delete_sensitive_message(event)
        code_waiting.pop(admin_id, None)
        password_waiting.pop(admin_id, None)
        await _drop_temp_client(admin_id)
        logger.error(f"Ошибка подтверждения кода: {exc}")
        await event.respond(
            f"❌ Ошибка авторизации: {exc}\n"
            "Начните добавление аккаунта заново."
        )


@bot.on(
    New_Message(
        func=lambda e: e.sender_id in password_waiting
        and bool(e.text)
        and not is_command_event(e)
        and e.sender_id not in user_states
        and e.sender_id not in broadcast_all_state
    )
)
async def get_password(event: callback_message) -> None:
    if not _is_admin(event.sender_id):
        return
    admin_id = int(event.sender_id)
    state = password_waiting.get(admin_id)
    client = user_clients.get(admin_id)
    if not state or client is None:
        password_waiting.pop(admin_id, None)
        await event.respond("⚠ Сессия авторизации потеряна. Начните добавление аккаунта заново.")
        return
    if not state.get("waiting") or event.message.id <= state.get("last_message_id", 0):
        return

    password = event.text.strip()
    try:
        await asyncio.wait_for(
            client.sign_in(password=password), timeout=_AUTH_REQUEST_TIMEOUT
        )
        await _delete_sensitive_message(event)
        account_user_id = await _save_authorized_account(admin_id, client)
        password_waiting.pop(admin_id, None)
        code_waiting.pop(admin_id, None)
        await _drop_temp_client(admin_id)
        await _prompt_optional_email(
            event,
            admin_id,
            account_user_id,
            "✅ Авторизация с паролем прошла успешно!",
        )
    except Exception as exc:
        await _delete_sensitive_message(event)
        logger.warning(f"Ошибка ввода 2FA-пароля: {exc}")
        await event.respond(
            f"⚠ Не удалось войти с этим паролем: {exc}\n"
            "Можно повторить пароль или открыть «Меню», чтобы отменить ввод."
        )


@bot.on(
    New_Message(
        func=lambda e: e.sender_id in email_waiting
        and bool(e.text)
        and not is_command_event(e)
    )
)
async def get_account_email(event: callback_message) -> None:
    if not _is_admin(event.sender_id):
        return
    admin_id = int(event.sender_id)
    state = email_waiting.get(admin_id)
    if not state or not state.get("waiting"):
        return
    message_id = int(getattr(getattr(event, "message", None), "id", 0) or 0)
    if message_id and message_id <= int(state.get("last_message_id", 0) or 0):
        return
    account_user_id = int(state["user_id"])
    email = _normalize_email(event.text)
    if email is None:
        await event.respond(
            "⚠ Не похоже на корректный e-mail. Пример: `name@example.com`\n\n"
            "Отправьте адрес ещё раз или пропустите этот шаг.",
            buttons=[
                [Button.inline("⏭ Пропустить", f"account_email_skip_{account_user_id}".encode())]
            ],
        )
        return
    with conn:
        cursor = conn.execute(
            "UPDATE sessions SET account_email=? WHERE user_id=?",
            (email, account_user_id),
        )
    if int(cursor.rowcount or 0) != 1:
        email_waiting.pop(admin_id, None)
        await event.respond("⚠ Аккаунт уже не найден в базе.")
        return
    email_waiting.pop(admin_id, None)
    await _show_account_ready(event, account_user_id, email)


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("account_email_skip_")))
async def skip_account_email(event: callback_query) -> None:
    if not _is_admin(event.sender_id):
        await event.answer("Недоступно", alert=True)
        return
    try:
        account_user_id = int(event.data.decode().rsplit("_", 1)[1])
    except (ValueError, IndexError):
        await event.answer("Некорректный ID аккаунта", alert=True)
        return
    exists = conn.execute(
        "SELECT account_email FROM sessions WHERE user_id=?", (account_user_id,)
    ).fetchone()
    if not exists:
        email_waiting.pop(int(event.sender_id), None)
        await event.answer("Аккаунт не найден", alert=True)
        return
    state = email_waiting.get(int(event.sender_id))
    if state and int(state.get("user_id", -1)) == account_user_id:
        email_waiting.pop(int(event.sender_id), None)
    email = str(exists[0]).strip() if exists[0] else None
    await event.answer("Шаг пропущен")
    await _show_account_ready(event, account_user_id, email)


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("account_email_edit_")))
async def edit_account_email(event: callback_query) -> None:
    if not _is_admin(event.sender_id):
        await event.answer("Недоступно", alert=True)
        return
    try:
        account_user_id = int(event.data.decode().rsplit("_", 1)[1])
    except (ValueError, IndexError):
        await event.answer("Некорректный ID аккаунта", alert=True)
        return
    exists = conn.execute(
        "SELECT 1 FROM sessions WHERE user_id=?", (account_user_id,)
    ).fetchone()
    if not exists:
        await event.answer("Аккаунт не найден", alert=True)
        return
    await clear_admin_interaction_state(int(event.sender_id))
    email_waiting[int(event.sender_id)] = {
        "user_id": account_user_id,
        "waiting": True,
        "last_message_id": int(getattr(getattr(event, "message", None), "id", 0) or 0),
    }
    await render_menu(
        event,
        "📧 Отправьте новый e-mail для аккаунта.\n\n"
        "Можно пропустить — текущая почта останется без изменений.",
        buttons=[
            [Button.inline("⏭ Пропустить", f"account_email_skip_{account_user_id}".encode())],
            [Button.inline("◀️ Назад", f"account_info_{account_user_id}".encode())],
        ],
    )
    await event.answer()
