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
from services.menu_ui import render_menu
from services.ui import (
    DENIED,
    DISABLED,
    ENABLED,
    OFF,
    ON,
    WARN,
    back_home_row,
    back_row,
    btn,
    join,
    kv,
    notice,
    screen,
)

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
    authorized = accounts_svc.count_authorized()
    reauth = accounts_svc.count_reauth_required()
    rows = accounts_svc.list_accounts()
    paused = sum(
        1
        for row in rows
        if row.get("is_paused") and not accounts_svc.is_reauth_required(row)
    )
    finishing = sum(
        1
        for row in rows
        if not row.get("participates")
        and not accounts_svc.is_reauth_required(row)
        and accounts_svc.dashboard_account_line(row).find("завершает") >= 0
    )
    text = screen(
        "👤",
        "Аккаунты",
        join(
            f"├ Всего: **{total}**",
            f"├ Авторизованы: **{authorized}**",
            f"├ Требуют повторного входа: **{reauth}**",
            f"├ First DM включены: **{active}**",
            f"├ Ограничены Telegram: **{paused}**",
            f"└ Завершают диалоги: **{finishing}**",
        ),
        "🟢 работает · 🟡 First DM отключены · 🔴 требуется вход",
    )
    buttons = [
        [btn("➕ ДОБАВИТЬ АККАУНТ", b"acc_add")],
        [btn("📋 ОТКРЫТЬ СПИСОК", b"acc_list")],
    ]
    if reauth:
        buttons.append(
            [btn(f"🔴 НУЖЕН ВХОД · {reauth}", b"acc_problem_list")]
        )
    buttons.append(back_home_row())
    await render_menu(event, text, buttons, edit=edit)


async def show_account_list(event, *, edit: bool = True) -> None:
    rows = accounts_svc.list_accounts()
    if not rows:
        await render_menu(
            event,
            screen("📋", "Список аккаунтов", "Аккаунты ещё не добавлены."),
            [
                [Button.inline("➕ ДОБАВИТЬ АККАУНТ", b"acc_add")],
                back_row(b"menu_accounts"),
                back_home_row(),
            ],
            edit=edit,
        )
        return

    body = "\n\n".join(accounts_svc.dashboard_account_line(acc) for acc in rows)
    buttons: list[list[Button]] = []
    for acc in rows:
        if accounts_svc.is_reauth_required(acc):
            icon = "🔴"
        elif acc.get("is_paused"):
            icon = "🟠"
        elif acc.get("participates"):
            icon = "🟢"
        else:
            icon = "🟡"
        short = accounts_svc.format_account_label(acc, include_id=False)[:28]
        buttons.append([btn(f"{icon} {short}", f"acc_card_{acc['user_id']}")])
    buttons.append([btn("➕ ДОБАВИТЬ АККАУНТ", b"acc_add")])
    buttons.append(back_row(b"menu_accounts"))
    buttons.append(back_home_row())
    await render_menu(
        event,
        screen("📋", "Список аккаунтов", body),
        buttons,
        edit=edit,
    )


async def show_problem_accounts(event, *, edit: bool = True) -> None:
    rows = accounts_svc.list_reauth_required()
    if not rows:
        await render_menu(
            event,
            screen("✅", "Проблемные аккаунты", "Все аккаунты авторизованы."),
            [back_row(b"menu_accounts"), back_home_row()],
            edit=edit,
        )
        return
    body = "\n\n".join(accounts_svc.dashboard_account_line(acc) for acc in rows)
    buttons = [
        [
            btn(
                f"🔴 {accounts_svc.format_account_label(acc, include_id=False)[:28]}",
                f"acc_card_{acc['user_id']}",
            )
        ]
        for acc in rows
    ]
    buttons.append(back_row(b"menu_accounts"))
    buttons.append(back_home_row())
    await render_menu(
        event,
        screen("🔴", "Требуется повторный вход", body),
        buttons,
        edit=edit,
    )


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
    phone = (acc.get("phone") or "").strip() or "не указан"
    from services import chats as chats_svc
    from services import dialog_store as dialog_store_svc
    from services import monitor as monitor_svc
    from services import spambot as spambot_svc

    active_dialogs = dialog_store_svc.count_open_for_account(user_id)
    retention_waiting = dialog_store_svc.count_retention_waiting_for_account(user_id)
    connected = int(user_id) in set(monitor_svc.connected_account_ids())
    auth_lost = accounts_svc.is_reauth_required(acc)

    if auth_lost:
        error = (acc.get("auth_error") or "session_not_authorized").strip()
        if len(error) > 110:
            error = error[:107] + "..."
        text = screen(
            "👤",
            label,
            join(
                "🔴 Состояние: **требуется повторный вход**",
                "📨 First DM: **остановлены**",
                f"💬 Активных диалогов: **{active_dialogs}**",
                f"🗑 Ожидают очистки: **{retention_waiting}**",
                "📡 Мониторинг: **остановлен**",
            ),
            join(
                "Telegram-сессия больше не действует.",
                "Новые сообщения с аккаунта не отправляются.",
                "Перезайдите в аккаунт или удалите его из бота.",
            ),
            join(
                f"🆔 ID: `{acc['user_id']}`",
                f"📱 Телефон: `{phone}`",
                f"⚠ Причина: `{error}`",
                f"🕒 Обнаружено: `{(acc.get('auth_lost_at') or '')[:19] or '-'}`",
            ),
        )
        buttons = [
            [btn("🔑 ПЕРЕЗАЙТИ", f"acc_relogin_{user_id}")],
            [btn("🗑 УДАЛИТЬ АККАУНТ", f"acc_del_ask_{user_id}")],
            [btn("🔄 ПРОВЕРИТЬ СНОВА", f"acc_auth_check_{user_id}")],
            back_row(b"acc_list"),
            back_home_row(),
        ]
        await render_menu(event, text, buttons, edit=edit)
        return

    toggle_label = (
        "⏸ ОТКЛЮЧИТЬ FIRST DM"
        if acc.get("participates")
        else "▶️ ВКЛЮЧИТЬ FIRST DM"
    )
    mode = chats_svc.get_chat_mode(user_id)
    mode_label = (
        "все + исключения" if mode == chats_svc.CHAT_MODE_ALL else "вручную"
    )
    watchable = chats_svc.count_watchable(user_id)
    sb = spambot_svc.get_state(user_id)
    sb_status = sb.get("status") or "idle"
    sb_reply = (sb.get("last_reply") or "").strip()
    if len(sb_reply) > 120:
        sb_reply = sb_reply[:120] + "..."
    sb_next = (sb.get("next_check_at") or "")[:19] or "-"
    cooldown = (acc.get("cooldown_until") or "")[:19] or "-"
    sb_block = f"Ответ: {sb_reply}" if sb_reply else ""

    if acc.get("is_paused"):
        state_line = f"🟠 Ограничен: **{acc.get('pause_reason') or 'пауза'}**"
    elif acc.get("participates"):
        state_line = "🟢 Состояние: **работает**"
    elif active_dialogs:
        state_line = "🟡 Состояние: **завершает активные диалоги**"
    else:
        state_line = "🟡 Состояние: **First DM отключены**"

    text = screen(
        "👤",
        label,
        join(
            state_line,
            f"📨 First DM: **{'включены' if acc.get('participates') else 'отключены'}**",
            f"💬 Активных диалогов: **{active_dialogs}**",
            f"🗑 Ожидают очистки: **{retention_waiting}**",
            f"📡 Мониторинг: **{'online' if connected else 'offline'}**",
        ),
        join(
            f"⏱ Интервал First DM: **{accounts_svc.format_dm_interval(acc)}**",
            f"📍 Чаты: **{mode_label} · {watchable} в мониторинге**",
            f"🤖 SpamBot: **{sb_status}**",
            *([f"⏳ Ограничение до: `{cooldown}`"] if cooldown != "-" else []),
            *([f"🔄 Следующая проверка: `{sb_next}`"] if sb_next != "-" else []),
            *([sb_block] if sb_block else []),
        ),
        join(
            f"🆔 ID: `{acc['user_id']}`",
            f"📱 Телефон: `{phone}`",
            f"📅 Добавлен: `{(acc.get('created_at') or '')[:19]}`",
        ),
    )
    buttons = [
        [btn(toggle_label, f"acc_toggle_{user_id}")],
        [btn("⏱ ИНТЕРВАЛ FIRST DM", f"acc_delay_{user_id}")],
        [btn("📡 ГРУППЫ", f"acc_chats_{user_id}")],
        [btn("🤖 ПРОВЕРИТЬ SPAMBOT", f"acc_spambot_{user_id}")],
        [btn("▶️ СНЯТЬ ПАУЗУ", f"acc_unpause_{user_id}")],
        [btn("🗑 УДАЛИТЬ АККАУНТ", f"acc_del_ask_{user_id}")],
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
        await event.answer(DENIED, alert=True)
        return
    await show_accounts_menu(event)
    await event.answer()


@bot.on(events.CallbackQuery(data=b"acc_list"))
async def cb_acc_list(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    await show_account_list(event)
    await event.answer()


@bot.on(events.CallbackQuery(data=b"acc_problem_list"))
async def cb_acc_problem_list(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    await show_problem_accounts(event)
    await event.answer()


@bot.on(events.CallbackQuery(pattern=rb"^acc_card_(\d+)$"))
async def cb_acc_card(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    user_id = int(event.pattern_match.group(1))
    await show_account_card(event, user_id)
    await event.answer()


@bot.on(events.CallbackQuery(pattern=rb"^acc_toggle_(\d+)$"))
async def cb_acc_toggle(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    user_id = int(event.pattern_match.group(1))
    acc = accounts_svc.get_account(user_id)
    if not acc:
        await event.answer("Не найден", alert=True)
        return
    new_value = not bool(acc.get("participates"))
    if new_value and accounts_svc.is_reauth_required(acc):
        await event.answer("Сначала выполните повторный вход", alert=True)
        await show_account_card(event, user_id)
        return
    accounts_svc.set_participates(user_id, new_value)
    try:
        from services import monitor as monitor_svc
        await monitor_svc.refresh_monitor()
    except Exception as exc:
        logger.warning("monitor refresh after toggle: {}", exc)
    await show_account_card(event, user_id)
    await event.answer(ENABLED if new_value else DISABLED)


# acc_chats handler lives in handlers/chats.py (Step 4).


@bot.on(events.CallbackQuery(pattern=rb"^acc_relogin_(\d+)$"))
async def cb_acc_relogin(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    user_id = int(event.pattern_match.group(1))
    acc = accounts_svc.get_account(user_id)
    if not acc:
        await event.answer("Не найден", alert=True)
        return

    admin_id = int(event.sender_id)
    previous = get_state(admin_id)
    if previous and previous.get("client"):
        await _disconnect_client(previous.get("client"))
    clear_state(admin_id)

    phone = (acc.get("phone") or "").strip()
    if not phone:
        set_state(
            admin_id,
            flow="login",
            login_mode="reauth",
            target_user_id=user_id,
            step="phone",
        )
        await render_menu(
            event,
            screen(
                "🔑",
                "Повторный вход",
                f"Аккаунт: **{accounts_svc.format_account_label(acc, include_id=False)}**",
                "Отправьте номер телефона в международном формате.",
                "Пример: `+79001234567`",
                "Отмена: /cancel",
            ),
            [[btn("❌ ОТМЕНА", b"acc_cancel")]],
        )
        await event.answer()
        return

    client = TelegramClient(StringSession(), API_ID, API_HASH)
    try:
        await client.connect()
        await client.send_code_request(phone)
    except Exception as exc:
        await _disconnect_client(client)
        logger.warning(
            "Re-login code request failed account={} error={}",
            user_id,
            type(exc).__name__,
        )
        await event.answer("Не удалось отправить код", alert=True)
        await show_account_card(event, user_id)
        return

    set_state(
        admin_id,
        flow="login",
        login_mode="reauth",
        target_user_id=user_id,
        step="code",
        phone=phone,
        client=client,
    )
    await render_menu(
        event,
        screen(
            "🔑",
            "Повторный вход",
            f"Аккаунт: **{accounts_svc.format_account_label(acc, include_id=False)}**",
            f"Код отправлен на `{phone}`.",
            "Пришлите код из Telegram только цифрами.",
            "Если включена 2FA, бот затем запросит пароль.",
            "Отмена: /cancel",
        ),
        [[btn("❌ ОТМЕНА", b"acc_cancel")]],
    )
    await event.answer("Код отправлен")


@bot.on(events.CallbackQuery(pattern=rb"^acc_auth_check_(\d+)$"))
async def cb_acc_auth_check(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    user_id = int(event.pattern_match.group(1))
    acc = accounts_svc.get_account(user_id)
    if not acc:
        await event.answer("Не найден", alert=True)
        return
    client = TelegramClient(StringSession(acc.get("session_string") or ""), API_ID, API_HASH)
    try:
        await client.connect()
        authorized = bool(await client.is_user_authorized())
    except Exception as exc:
        from services import account_auth

        if account_auth.is_auth_loss_error(exc):
            authorized = False
        else:
            logger.warning(
                "Manual auth check temporary failure account={} error={}",
                user_id,
                type(exc).__name__,
            )
            await event.answer("Временная ошибка проверки", alert=True)
            await _disconnect_client(client)
            return
    finally:
        await _disconnect_client(client)

    from services import account_auth
    from services import monitor as monitor_svc

    if authorized:
        account_auth.mark_authorized(user_id)
        await monitor_svc.refresh_monitor()
        await event.answer("Аккаунт авторизован")
    else:
        await account_auth.register_auth_loss(
            user_id, "session_not_authorized", notify=True
        )
        await monitor_svc.disconnect_account(user_id, cancel_tasks=True)
        await event.answer("Нужен повторный вход", alert=True)
    await show_account_card(event, user_id)


@bot.on(events.CallbackQuery(pattern=rb"^acc_del_ask_(\d+)$"))
async def cb_acc_del_ask(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    user_id = int(event.pattern_match.group(1))
    acc = accounts_svc.get_account(user_id)
    if not acc:
        await event.answer("Не найден", alert=True)
        return
    label = accounts_svc.format_account_label(acc)
    from services import retention as retention_svc

    pending_cleanup = retention_svc.count_pending_for_account(user_id)
    blocks = [
        f"👤 **{label}**",
        "Сессия будет удалена из бота и аккаунт сразу отключится.",
        "История и статистика останутся в базе как неактивные записи.",
    ]
    if pending_cleanup:
        blocks.append(
            f"⚠️ Ожидают удаления в Telegram: **{pending_cleanup}** диалогов.\n"
            "После удаления сессии бот уже не сможет очистить их у обеих сторон."
        )
    await render_menu(
        event,
        screen("🗑", "Удалить аккаунт?", *blocks),
        [
            [Button.inline("✅ ДА, УДАЛИТЬ", f"acc_del_yes_{user_id}".encode())],
            [Button.inline("❌ ОТМЕНА", f"acc_card_{user_id}".encode())],
        ],
    )
    await event.answer()


@bot.on(events.CallbackQuery(pattern=rb"^acc_del_yes_(\d+)$"))
async def cb_acc_del_yes(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    user_id = int(event.pattern_match.group(1))
    try:
        from services import monitor as monitor_svc
        await monitor_svc.disconnect_account(user_id, cancel_tasks=True)
    except Exception as exc:
        logger.warning("disconnect before account delete: {}", exc)
    ok = accounts_svc.delete_account(user_id)
    if ok:
        try:
            from services import monitor as monitor_svc
            await monitor_svc.refresh_monitor()
        except Exception as exc:
            logger.warning("monitor refresh after delete: {}", exc)
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
        await event.answer(DENIED, alert=True)
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
        await event.answer(DENIED, alert=True)
        return
    admin_id = int(event.sender_id)
    state = get_state(admin_id)
    target_user_id = int(state.get("target_user_id")) if state and state.get("target_user_id") else None
    if state and state.get("client"):
        await _disconnect_client(state.get("client"))
    clear_state(admin_id)
    if target_user_id is not None:
        await show_account_card(event, target_user_id)
    else:
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
    flow = state.get("flow")
    target_user_id = int(state.get("target_user_id")) if state.get("target_user_id") else None
    clear_state(admin_id)
    await event.respond("Ок, отменено.")
    if flow == "login" and target_user_id is not None:
        await show_account_card(event, target_user_id, edit=False)
    elif flow == "login":
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
    state = get_state(admin_id) or {}
    login_mode = str(state.get("login_mode") or "add")
    target_user_id = (
        int(state.get("target_user_id"))
        if state.get("target_user_id") is not None
        else None
    )
    try:
        me = await client.get_me()
        actual_user_id = int(me.id)
        if login_mode == "reauth" and target_user_id is not None:
            if actual_user_id != target_user_id:
                await event.respond(
                    "⚠ Вы вошли в другой Telegram-аккаунт.\n\n"
                    f"Ожидался ID: `{target_user_id}`\n"
                    f"Получен ID: `{actual_user_id}`\n\n"
                    "Сессия не сохранена. Нажмите «Перезайти» и используйте нужный номер.",
                    buttons=[
                        [
                            Button.inline(
                                "🔑 ПЕРЕЗАЙТИ",
                                f"acc_relogin_{target_user_id}".encode(),
                            )
                        ],
                        [
                            Button.inline(
                                "👤 К АККАУНТУ",
                                f"acc_card_{target_user_id}".encode(),
                            )
                        ],
                    ],
                )
                return

        session_string = client.session.save()
        accounts_svc.upsert_account(
            user_id=actual_user_id,
            session_string=session_string,
            phone=phone or None,
            username=getattr(me, "username", None),
            first_name=getattr(me, "first_name", None),
            last_name=getattr(me, "last_name", None),
        )
        label = accounts_svc.format_account_label(
            {
                "user_id": actual_user_id,
                "username": getattr(me, "username", None),
                "first_name": getattr(me, "first_name", None),
                "last_name": getattr(me, "last_name", None),
            }
        )

        from services import account_auth
        from services import monitor as monitor_svc

        account_auth.mark_authorized(actual_user_id)
        await monitor_svc.refresh_monitor()

        if login_mode == "reauth":
            await event.respond(
                "\n".join(
                    [
                        "✅ **АККАУНТ СНОВА ПОДКЛЮЧЁН**",
                        "━━━━━━━━━━━━━━━━━━",
                        "",
                        f"Аккаунт: **{label}**",
                        "",
                        "Мониторинг восстановлен.",
                        "Настройки First DM сохранены.",
                        "Диалоги и статистика сохранены.",
                    ]
                ),
                buttons=[
                    [
                        Button.inline(
                            "👤 К АККАУНТУ",
                            f"acc_card_{actual_user_id}".encode(),
                        )
                    ],
                    [Button.inline("📋 К АККАУНТАМ", b"acc_list")],
                    back_home_row(),
                ],
            )
            logger.info(
                "Account reauthorized user_id={} by admin={}",
                actual_user_id,
                admin_id,
            )
        else:
            await event.respond(
                f"✅ Аккаунт сохранён: **{label}**\n\n"
                "По умолчанию **не участвует** в рассылке.\n"
                "Откройте карточку и нажмите «Включить участие».",
                buttons=[
                    [
                        Button.inline(
                            "👤 Открыть карточку",
                            f"acc_card_{actual_user_id}".encode(),
                        )
                    ],
                    [Button.inline("📋 К списку", b"acc_list")],
                    back_home_row(),
                ],
            )
            logger.info("Account saved user_id={} by admin={}", actual_user_id, admin_id)
    except Exception as exc:
        logger.exception("finish_login failed: {}", exc)
        await event.respond(f"⚠ Не удалось сохранить аккаунт: `{type(exc).__name__}`")
    finally:
        await _disconnect_client(client)
        clear_state(admin_id)



@bot.on(events.CallbackQuery(pattern=rb"^acc_spambot_(\d+)$"))
async def cb_acc_spambot(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
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
    except Exception as exc:
        logger.debug("Second callback answer ignored: {}", exc)


@bot.on(events.CallbackQuery(pattern=rb"^acc_unpause_(\d+)$"))
async def cb_acc_unpause(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    user_id = int(event.pattern_match.group(1))
    from services import spambot as spambot_svc
    await spambot_svc.resume_account(user_id, source="manual")
    await show_account_card(event, user_id)
    await event.answer("Пауза снята")


@bot.on(events.CallbackQuery(pattern=rb"^acc_delay_(\d+)$"))
async def cb_acc_delay(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    user_id = int(event.pattern_match.group(1))
    acc = accounts_svc.get_account(user_id)
    if not acc:
        await event.answer("Не найден", alert=True)
        return
    label = accounts_svc.format_account_label(acc, include_id=False)
    cur = accounts_svc.format_dm_interval(acc)
    text = screen(
        "⏱",
        "Задержка DM",
        f"Аккаунт: **{label}**",
        f"Сейчас: {cur}",
        "",
        "По умолчанию - как в **Настройки → Темп**.",
        "Свой интервал только для этого аккаунта (между first DM).",
    )
    buttons = [
        [btn("Как в настройках", f"acc_delayset_{user_id}_0_0")],
        [
            btn("5-10 мин", f"acc_delayset_{user_id}_300_600"),
            btn("10-15 мин", f"acc_delayset_{user_id}_600_900"),
        ],
        [
            btn("15-25 мин", f"acc_delayset_{user_id}_900_1500"),
            btn("20-40 мин", f"acc_delayset_{user_id}_1200_2400"),
        ],
        [btn("30-60 мин", f"acc_delayset_{user_id}_1800_3600")],
        back_row(f"acc_card_{user_id}"),
        back_home_row(),
    ]
    await render_menu(event, text, buttons)
    await event.answer()


@bot.on(events.CallbackQuery(pattern=rb"^acc_delayset_(\d+)_(\d+)_(\d+)$"))
async def cb_acc_delayset(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    user_id = int(event.pattern_match.group(1))
    lo = int(event.pattern_match.group(2))
    hi = int(event.pattern_match.group(3))
    if lo == 0 and hi == 0:
        accounts_svc.set_dm_interval(user_id, None, None)
        msg = "как в настройках"
    else:
        accounts_svc.set_dm_interval(user_id, lo, hi)
        msg = f"{lo // 60}-{hi // 60} мин"
    await show_account_card(event, user_id)
    await event.answer(f"Задержка: {msg}")
