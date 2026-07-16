from __future__ import annotations

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
    password_waiting,
    phone_waiting,
    user_clients,
    user_states,
)
from services.admin_state import clear_admin_interaction_state, is_command_event
from services.account_profiles import save_account_profile
from services.menu_ui import render_menu


def _mask_phone(phone: str) -> str:
    if len(phone) <= 6:
        return "***"
    return f"{phone[:3]}***{phone[-3:]}"


async def _drop_temp_client(admin_id: int) -> None:
    client = user_clients.pop(admin_id, None)
    if client is None:
        return
    try:
        if client.is_connected():
            await client.disconnect()
    except Exception as exc:
        logger.debug(f"Не удалось отключить временный Telegram-клиент {admin_id}: {exc}")


async def _save_authorized_account(admin_id: int, client: TelegramClient) -> int:
    me = await client.get_me()
    session_string = client.session.save()
    with conn:
        conn.execute(
            """
            INSERT INTO sessions (
                user_id, session_string, username, first_name, last_name,
                profile_updated_at
            )
            VALUES (?, ?, NULL, NULL, NULL, NULL)
            ON CONFLICT(user_id) DO UPDATE SET
                session_string = excluded.session_string
            """,
            (me.id, session_string),
        )
    save_account_profile(me)
    return int(me.id)


@bot.on(Query(data=b"add_account"))
async def add_account(event: callback_query) -> None:
    admin_id = event.sender_id
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
    admin_id = event.sender_id
    phone_number = event.text.strip()
    logger.info(f"Отправляю код подтверждения для {_mask_phone(phone_number)}")

    await _drop_temp_client(admin_id)
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    user_clients[admin_id] = client
    await client.connect()
    await event.respond("⏳ Отправляю код подтверждения...")

    try:
        await client.send_code_request(phone_number)
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
    admin_id = event.sender_id
    phone_number = code_waiting.get(admin_id)
    client = user_clients.get(admin_id)
    if not phone_number or client is None:
        code_waiting.pop(admin_id, None)
        await event.respond("⚠ Сессия авторизации потеряна. Начните добавление аккаунта заново.")
        return

    try:
        await client.sign_in(phone_number, event.text.strip())
        account_user_id = await _save_authorized_account(admin_id, client)
        code_waiting.pop(admin_id, None)
        password_waiting.pop(admin_id, None)
        await _drop_temp_client(admin_id)
        await event.respond(
            "✅ Авторизация прошла успешно!",
            buttons=[
                [Button.inline("🔎 Найти группы аккаунта", f"sync_groups_{account_user_id}".encode())],
                [Button.inline("🏠 Главное меню", b"menu_home")],
            ],
        )
    except SessionPasswordNeededError:
        password_waiting[admin_id] = {
            "waiting": True,
            "last_message_id": event.message.id,
        }
        code_waiting.pop(admin_id, None)
        await event.respond("🔐 На аккаунте включён пароль 2FA. Отправьте пароль:")
    except PhoneCodeExpiredError:
        code_waiting.pop(admin_id, None)
        await _drop_temp_client(admin_id)
        await event.respond(
            "⏰ Код подтверждения истёк. Нажмите «Добавить аккаунт», чтобы получить новый."
        )
    except PhoneCodeInvalidError:
        await event.respond(
            "❌ Неверный код подтверждения. Проверьте код и введите его ещё раз."
        )
    except Exception as exc:
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
        and not is_command_event(e)
        and e.sender_id not in user_states
        and e.sender_id not in broadcast_all_state
    )
)
async def get_password(event: callback_message) -> None:
    admin_id = event.sender_id
    state = password_waiting.get(admin_id)
    client = user_clients.get(admin_id)
    if not state or client is None:
        password_waiting.pop(admin_id, None)
        await event.respond("⚠ Сессия авторизации потеряна. Начните добавление аккаунта заново.")
        return
    if not state.get("waiting") or event.message.id <= state.get("last_message_id", 0):
        return

    try:
        await client.sign_in(password=event.text.strip())
        account_user_id = await _save_authorized_account(admin_id, client)
        password_waiting.pop(admin_id, None)
        code_waiting.pop(admin_id, None)
        await _drop_temp_client(admin_id)
        await event.respond(
            "✅ Авторизация с паролем прошла успешно!",
            buttons=[
                [Button.inline("🔎 Найти группы аккаунта", f"sync_groups_{account_user_id}".encode())],
                [Button.inline("🏠 Главное меню", b"menu_home")],
            ],
        )
    except Exception as exc:
        logger.warning(f"Ошибка ввода 2FA-пароля: {exc}")
        await event.respond(
            f"⚠ Не удалось войти с этим паролем: {exc}\n"
            "Можно повторить пароль или открыть «Меню», чтобы отменить ввод."
        )
