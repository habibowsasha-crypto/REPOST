"""Account management: login wizard, list, participate toggle, delete."""

from __future__ import annotations

import re

from loguru import logger
from telethon import Button, TelegramClient, events
from telethon.errors import (
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberInvalidError,
    SessionPasswordNeededError,
)
from telethon.sessions import StringSession

from config import API_HASH, API_ID, bot, is_admin
from services import accounts as accounts_svc
from services.admin_state import clear_state, get_state, set_state
from services.menu_ui import back_home_row, back_row, render_menu

_PHONE_RE = re.compile(r"^\+?[0-9]{10,15}$")


def _normalize_phone(raw: str) -> str | None:
    value = (raw or "").strip().replace(" ", "").replace("-", "")
    if not value:
        return None
    if not value.startswith("+") and value.isdigit():
        value = "+" + value
    if not _PHONE_RE.match(value):
        return None
    return value


async def _disconnect_client(client: TelegramClient | None) -> None:
    if client is None:
        return
    try:
        if client.is_connected():
            await client.disconnect()
    except Exception as exc:
        logger.debug("Temp client disconnect: {}", exc)


async def show_accounts_menu(event, *, edit: bool = True) -> None:
    total = accounts_svc.count_accounts()
    active = accounts_svc.count_participating()
    await render_menu(
        event,
        "**👤 Аккаунты**\n\n"
        f"Всего: **{total}** | Участвуют в рассылке: **{active}**\n\n"
        "Добавьте user-аккаунт Telegram и включите тумблер участия.",
        [
            [Button.inline("➕ Добавить аккаунт", b"acc_add")],
            [Button.inline("📋 Список аккаунтов", b"acc_list")],
            back_home_row(),
        ],
        edit=edit,
    )


async def show_account_list(event, *, edit: bool = True) -> None:
    rows = accounts_svc.list_accounts()
    if not rows:
        await render_menu(
            event,
            "**📋 Список аккаунтов**\n\nПока пусто. Нажмите «Добавить аккаунт».",
            [
                [Button.inline("➕ Добавить аккаунт", b"acc_add")],
                back_row(b"menu_accounts"),
                back_home_row(),
            ],
            edit=edit,
        )
        return

    lines = ["**📋 Список аккаунтов**\n"]
    buttons: list[list[Button]] = []
    for acc in rows:
        label = accounts_svc.format_account_label(acc)
        status = accounts_svc.account_status_line(acc)
        icon = "🟢" if acc.get("participates") and not acc.get("is_paused") else "🔴"
        lines.append(f"{icon} `{acc['user_id']}` | {label}\n{status}")
        short = accounts_svc.format_account_label(acc, include_id=False)[:28]
        buttons.append(
            [
                Button.inline(
                    f"{icon} {short}",
                    f"acc_card_{acc['user_id']}".encode(),
                )
            ]
        )
    buttons.append([Button.inline("➕ Добавить", b"acc_add")])
    buttons.append(back_row(b"menu_accounts"))
    buttons.append(back_home_row())
    await render_menu(event, "\n\n".join(lines), buttons, edit=edit)


async def show_account_card(event, user_id: int, *, edit: bool = True) -> None:
    acc = accounts_svc.get_account(user_id)
    if not acc:
        await render_menu(
            event,
            "⚠ Аккаунт не найден.",
            [back_row(b"acc_list"), back_home_row()],
            edit=edit,
        )
        return

    label = accounts_svc.format_account_label(acc)
    status = accounts_svc.account_status_line(acc)
    phone = (acc.get("phone") or "").strip() or "не указан"
    toggle_label = (
        "⏸ Выключить участие"
        if acc.get("participates")
        else "▶️ Включить участие"
    )
    from services import chats as chats_svc

    mode = chats_svc.get_chat_mode(user_id)
    mode_label = (
        "все + исключения"
        if mode == chats_svc.CHAT_MODE_ALL
        else "вручную"
    )
    watchable = chats_svc.count_watchable(user_id)
    from services import monitor as monitor_svc
    connected = int(user_id) in set(monitor_svc.connected_account_ids())
    mon_line = "online" if connected else "offline"
    discovered_n = len(chats_svc.list_discovered(user_id))
    from services import spambot as spambot_svc

    sb = spambot_svc.get_state(user_id)
    sb_status = sb.get("status") or "idle"
    sb_reply = (sb.get("last_reply") or "").strip()
    if len(sb_reply) > 120:
        sb_reply = sb_reply[:120] + "…"
    sb_next = (sb.get("next_check_at") or "")[:19] or "-"
    cooldown = (acc.get("cooldown_until") or "")[:19] or "-"
    text = (
        f"**👤 Аккаунт**\n\n"
        f"{label}\n"
        f"ID: `{acc['user_id']}`\n"
        f"Телефон: `{phone}`\n"
        f"Статус: {status}\n"
        f"Клиент монитора: **{mon_line}**\n"
        f"Cooldownoldown: `{cooldown}`\n"
        f"SpamBot: **{sb_status}** | next `{sb_next}`\n"
        f"{('Ответ: ' + sb_reply + chr(10)) if sb_reply else ''}"
        f"Чаты: режим **{mode_label}**, найдено {discovered_n}, "
        f"мониторинг **{watchable}**\n"
        f"Добавлен: {(acc.get('created_at') or '')[:19]}\n"
    )
    buttons = [
        [Button.inline(toggle_label, f"acc_toggle_{user_id}".encode())],
        [Button.inline("💬 Чаты", f"acc_chats_{user_id}".encode())],
        [Button.inline("🤖 Проверить @SpamBot", f"acc_spambot_{user_id}".encode())],
        [Button.inline("▶️ Снять паузу", f"acc_unpause_{user_id}".encode())],
        [Button.inline("🗑 Удалить", f"acc_del_ask_{user_id}".encode())],
        back_row(b"acc_list"),
        back_home_row(),
    ]
    await render_menu(event, text, buttons, edit=edit)


# ---------------------------------------------------------------------------
# Menu entry points
# ---------------------------------------------------------------------------


@bot.on(events.CallbackQuery(data=b"menu_accounts"))
async def cb_menu_accounts(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    await show_accounts_menu(event)
    await event.answer()


@bot.on(events.CallbackQuery(data=b"acc_list"))
async def cb_acc_list(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    await show_account_list(event)
    await event.answer()


@bot.on(events.CallbackQuery(pattern=rb"^acc_card_(\d+)$"))
async def cb_acc_card(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    user_id = int(event.pattern_match.group(1))
    await show_account_card(event, user_id)
    await event.answer()


@bot.on(events.CallbackQuery(pattern=rb"^acc_toggle_(\d+)$"))
async def cb_acc_toggle(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    user_id = int(event.pattern_match.group(1))
    acc = accounts_svc.get_account(user_id)
    if not acc:
        await event.answer("Не найден", alert=True)
        return
    new_value = not bool(acc.get("participates"))
    accounts_svc.set_participates(user_id, new_value)
    try:
        from services import monitor as monitor_svc
        await monitor_svc.refresh_monitor()
    except Exception as exc:
        logger.warning("monitor refresh after toggle: {}", exc)
    await show_account_card(event, user_id)
    await event.answer("Участвует" if new_value else "Выключен")


# acc_chats handler lives in handlers/chats.py (Step 4).


@bot.on(events.CallbackQuery(pattern=rb"^acc_del_ask_(\d+)$"))
async def cb_acc_del_ask(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    user_id = int(event.pattern_match.group(1))
    acc = accounts_svc.get_account(user_id)
    if not acc:
        await event.answer("Не найден", alert=True)
        return
    label = accounts_svc.format_account_label(acc)
    await render_menu(
        event,
        f"**🗑 Удалить аккаунт?**\n\n{label}\n\n"
        "Сессия будет удалена из бота. Очередь/диалоги затронем в следующих шагах.",
        [
            [Button.inline("✅ Да, удалить", f"acc_del_yes_{user_id}".encode())],
            [Button.inline("❌ Отмена", f"acc_card_{user_id}".encode())],
        ],
    )
    await event.answer()


@bot.on(events.CallbackQuery(pattern=rb"^acc_del_yes_(\d+)$"))
async def cb_acc_del_yes(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    user_id = int(event.pattern_match.group(1))
    ok = accounts_svc.delete_account(user_id)
    if ok:
        await event.answer("Удалён")
        await show_account_list(event)
    else:
        await event.answer("Не найден", alert=True)


# ---------------------------------------------------------------------------
# Login wizard
# ---------------------------------------------------------------------------


@bot.on(events.CallbackQuery(data=b"acc_add"))
async def cb_acc_add(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    admin_id = int(event.sender_id)
    # Cancel any previous in-progress login client.
    prev = get_state(admin_id)
    if prev and prev.get("client"):
        await _disconnect_client(prev.get("client"))
    clear_state(admin_id)
    set_state(admin_id, flow="login", step="phone")
    await render_menu(
        event,
        "**➕ Добавление аккаунта**\n\n"
        "Отправьте номер телефона в международном формате.\n"
        "Пример: `+79001234567`\n\n"
        "Код придёт в приложение Telegram / SMS.\n"
        "Отмена: /cancel",
        [[Button.inline("❌ Отмена", b"acc_cancel")]],
    )
    await event.answer()


@bot.on(events.CallbackQuery(data=b"acc_cancel"))
async def cb_acc_cancel(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    admin_id = int(event.sender_id)
    state = get_state(admin_id)
    if state and state.get("client"):
        await _disconnect_client(state.get("client"))
    clear_state(admin_id)
    await show_accounts_menu(event)
    await event.answer("Отменено")


@bot.on(events.NewMessage(pattern=r"^/cancel(?:@\w+)?$"))
async def cmd_cancel(event: events.NewMessage.Event) -> None:
    if not is_admin(event.sender_id):
        return
    admin_id = int(event.sender_id)
    state = get_state(admin_id)
    if not state:
        await event.respond("Нечего отменять.")
        return
    if state.get("client"):
        await _disconnect_client(state.get("client"))
    clear_state(admin_id)
    await event.respond("Ок, отменено.")
    await show_accounts_menu(event, edit=False)


@bot.on(
    events.NewMessage(
        func=lambda e: (
            is_admin(e.sender_id)
            and get_state(int(e.sender_id)) is not None
            and (get_state(int(e.sender_id)) or {}).get("flow") == "login"
            and not (e.raw_text or "").strip().startswith("/")
        )
    )
)
async def login_text_handler(event: events.NewMessage.Event) -> None:
    """Handle phone / code / 2FA password during account login."""
    admin_id = int(event.sender_id)
    state = get_state(admin_id)
    if not state:
        return
    step = state.get("step")
    text = (event.raw_text or "").strip()

    if step == "phone":
        phone = _normalize_phone(text)
        if not phone:
            await event.respond(
                "⚠ Неверный номер. Пример: `+79001234567`\nИли /cancel"
            )
            return
        client = TelegramClient(StringSession(), API_ID, API_HASH)
        try:
            await client.connect()
            await client.send_code_request(phone)
        except PhoneNumberInvalidError:
            await _disconnect_client(client)
            await event.respond("⚠ Telegram отклонил номер. Проверьте формат.")
            return
        except Exception as exc:
            await _disconnect_client(client)
            logger.exception("send_code_request failed: {}", exc)
            await event.respond(f"⚠ Не удалось отправить код: `{type(exc).__name__}`")
            return

        set_state(
            admin_id,
            flow="login",
            step="code",
            phone=phone,
            client=client,
        )
        await event.respond(
            f"Код отправлен на `{phone}`.\n\n"
            "Пришлите код из Telegram (только цифры).\n"
            "Отмена: /cancel"
        )
        return

    if step == "code":
        client: TelegramClient | None = state.get("client")
        phone = state.get("phone")
        if client is None or not phone:
            clear_state(admin_id)
            await event.respond("⚠ Сессия входа сброшена. Начните снова.")
            return
        code = text.replace(" ", "").replace("-", "")
        if not code.isdigit():
            await event.respond("⚠ Код должен быть числом. Или /cancel")
            return
        try:
            await client.sign_in(phone=phone, code=code)
        except SessionPasswordNeededError:
            set_state(admin_id, step="password")
            await event.respond(
                "Нужен пароль 2FA облачного пароля Telegram.\n"
                "Отправьте пароль одним сообщением.\n"
                "Отмена: /cancel"
            )
            return
        except PhoneCodeInvalidError:
            await event.respond("⚠ Неверный код. Попробуйте ещё раз или /cancel")
            return
        except PhoneCodeExpiredError:
            await _disconnect_client(client)
            clear_state(admin_id)
            await event.respond("⚠ Код истёк. Начните добавление заново.")
            return
        except Exception as exc:
            logger.exception("sign_in code failed: {}", exc)
            await event.respond(f"⚠ Ошибка входа: `{type(exc).__name__}`")
            return
        await _finish_login(event, admin_id, client, phone)
        return

    if step == "password":
        client = state.get("client")
        phone = state.get("phone")
        if client is None:
            clear_state(admin_id)
            await event.respond("⚠ Сессия входа сброшена. Начните снова.")
            return
        try:
            await client.sign_in(password=text)
        except Exception as exc:
            logger.warning("2FA failed: {}", exc)
            await event.respond(
                "⚠ Пароль не принят. Попробуйте ещё раз или /cancel"
            )
            return
        await _finish_login(event, admin_id, client, phone or "")
        return


async def _finish_login(
    event: events.NewMessage.Event,
    admin_id: int,
    client: TelegramClient,
    phone: str,
) -> None:
    try:
        me = await client.get_me()
        session_string = client.session.save()
        accounts_svc.upsert_account(
            user_id=int(me.id),
            session_string=session_string,
            phone=phone or None,
            username=getattr(me, "username", None),
            first_name=getattr(me, "first_name", None),
            last_name=getattr(me, "last_name", None),
        )
        label = accounts_svc.format_account_label(
            {
                "user_id": int(me.id),
                "username": getattr(me, "username", None),
                "first_name": getattr(me, "first_name", None),
                "last_name": getattr(me, "last_name", None),
            }
        )
        await event.respond(
            f"✅ Аккаунт сохранён: **{label}**\n\n"
            "По умолчанию **не участвует** в рассылке.\n"
            "Откройте карточку и нажмите «Включить участие».",
            buttons=[
                [Button.inline("👤 Открыть карточку", f"acc_card_{me.id}".encode())],
                [Button.inline("📋 К списку", b"acc_list")],
                back_home_row(),
            ],
        )
        logger.info("Account saved user_id={} by admin={}", me.id, admin_id)
    except Exception as exc:
        logger.exception("finish_login failed: {}", exc)
        await event.respond(f"⚠ Не удалось сохранить аккаунт: `{type(exc).__name__}`")
    finally:
        await _disconnect_client(client)
        clear_state(admin_id)



@bot.on(events.CallbackQuery(pattern=rb"^acc_spambot_(\d+)$"))
async def cb_acc_spambot(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    user_id = int(event.pattern_match.group(1))
    await event.answer("Проверяю @SpamBot…")
    from services import spambot as spambot_svc
    try:
        result = await spambot_svc.check_account(user_id, force=True)
    except Exception as exc:
        logger.exception("manual spambot check: {}", exc)
        await show_account_card(event, user_id)
        return
    await show_account_card(event, user_id)
    try:
        await event.answer(str(result.get("result") or "done")[:20])
    except Exception:
        pass


@bot.on(events.CallbackQuery(pattern=rb"^acc_unpause_(\d+)$"))
async def cb_acc_unpause(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    user_id = int(event.pattern_match.group(1))
    from services import spambot as spambot_svc
    await spambot_svc.resume_account(user_id, source="manual")
    await show_account_card(event, user_id)
    await event.answer("Пауза снята")
