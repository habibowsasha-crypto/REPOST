"""Account chat mode and selection UI (Step 4)."""

from __future__ import annotations

from loguru import logger
from telethon import Button, events

from config import bot, is_admin
from services import accounts as accounts_svc
from services import chats as chats_svc
from services.menu_ui import render_menu
from services.ui import DENIED, WARN, back_home_row, back_row, btn, join, kv, notice, screen


def _mode_label(mode: str) -> str:
    if mode == chats_svc.CHAT_MODE_ALL:
        return "Все группы + исключения"
    return "Вручную"


async def show_chats_menu(event, account_user_id: int, page: int = 0, *, edit: bool = True) -> None:
    acc = accounts_svc.get_account(account_user_id)
    if not acc:
        await render_menu(
            event,
            "⚠ Аккаунт не найден.",
            [back_row(b"acc_list"), back_home_row()],
            edit=edit,
        )
        return

    mode = chats_svc.get_chat_mode(account_user_id)
    discovered = chats_svc.list_discovered(account_user_id)
    selected = chats_svc.list_selected_ids(account_user_id)
    excluded = chats_svc.list_excluded_ids(account_user_id)
    watchable = chats_svc.count_watchable(account_user_id)

    total = len(discovered)
    pages = max(1, (total + chats_svc.PAGE_SIZE - 1) // chats_svc.PAGE_SIZE)
    page = max(0, min(int(page), pages - 1))
    start = page * chats_svc.PAGE_SIZE
    chunk = discovered[start : start + chats_svc.PAGE_SIZE]

    label = accounts_svc.format_account_label(acc)
    body = join(
        f"Аккаунт: {label}",
        kv("Режим", _mode_label(mode)),
        kv("Найдено групп", str(total)),
        kv("Мониторится", str(watchable)),
    )
    if watchable == 0:
        body = join(body, notice("warn", "Ни один чат не мониторится - очередь пустая."))
    if mode == chats_svc.CHAT_MODE_MANUAL:
        body = join(body, kv("Выбрано вручную", str(len(selected))), "", "Отметь чаты для мониторинга:")
    else:
        body = join(body, kv("Исключено", str(len(excluded))), "", "Отметь чаты, которые **не** слушать:")
    if not discovered:
        body = join(body, "", "Список пуст. Нажми **Обновить группы**", "(аккаунт должен быть в группах).")
    text = screen("💬", "Чаты", body)

    buttons: list[list[Button]] = []
    other_mode = (
        chats_svc.CHAT_MODE_ALL
        if mode == chats_svc.CHAT_MODE_MANUAL
        else chats_svc.CHAT_MODE_MANUAL
    )
    other_label = (
        "🌐 Режим: все + исключения"
        if other_mode == chats_svc.CHAT_MODE_ALL
        else "🖐 Режим: вручную"
    )
    buttons.append(
        [btn(other_label, f"chat_mode_{account_user_id}_{other_mode}")]
    )
    buttons.append(
        [btn("🔄 Обновить группы", f"chat_refresh_{account_user_id}_{page}")]
    )

    for row in chunk:
        cid = int(row["chat_id"])
        title = chats_svc.short_title(row)
        if mode == chats_svc.CHAT_MODE_MANUAL:
            mark = "✅" if cid in selected else "⬜"
            buttons.append(
                [
                    Button.inline(
                        f"{mark} {title}",
                        f"chat_sel_{account_user_id}_{cid}_{page}".encode(),
                    )
                ]
            )
        else:
            mark = "🚫" if cid in excluded else "👁"
            buttons.append(
                [
                    Button.inline(
                        f"{mark} {title}",
                        f"chat_exc_{account_user_id}_{cid}_{page}".encode(),
                    )
                ]
            )

    nav: list[Button] = []
    if page > 0:
        nav.append(
            Button.inline("⬅️", f"chat_page_{account_user_id}_{page - 1}".encode())
        )
    nav.append(Button.inline(f"{page + 1}/{pages}", f"chat_page_{account_user_id}_{page}".encode()))
    if page + 1 < pages:
        nav.append(
            Button.inline("➡️", f"chat_page_{account_user_id}_{page + 1}".encode())
        )
    if nav:
        buttons.append(nav)

    buttons.append(back_row(f"acc_card_{account_user_id}".encode(), "◀️ К аккаунту"))
    buttons.append(back_home_row())
    await render_menu(event, text, buttons, edit=edit)


@bot.on(events.CallbackQuery(pattern=rb"^acc_chats_(\d+)$"))
async def cb_acc_chats(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    account_user_id = int(event.pattern_match.group(1))
    await show_chats_menu(event, account_user_id, 0)
    await event.answer()


@bot.on(events.CallbackQuery(pattern=rb"^chat_page_(\d+)_(\d+)$"))
async def cb_chat_page(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    account_user_id = int(event.pattern_match.group(1))
    page = int(event.pattern_match.group(2))
    await show_chats_menu(event, account_user_id, page)
    await event.answer()


@bot.on(events.CallbackQuery(pattern=rb"^chat_mode_(\d+)_(manual|all_with_exclusions)$"))
async def cb_chat_mode(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    account_user_id = int(event.pattern_match.group(1))
    mode = event.pattern_match.group(2).decode()
    ok = chats_svc.set_chat_mode(account_user_id, mode)
    if not ok:
        await event.answer("Ошибка режима", alert=True)
        return
    await show_chats_menu(event, account_user_id, 0)
    try:
        from services import monitor as monitor_svc
        await monitor_svc.refresh_monitor()
    except Exception as exc:
        logger.warning("Monitor refresh after chat mode change failed: {}", exc)
    await event.answer(_mode_label(mode))


@bot.on(events.CallbackQuery(pattern=rb"^chat_refresh_(\d+)_(\d+)$"))
async def cb_chat_refresh(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    account_user_id = int(event.pattern_match.group(1))
    page = int(event.pattern_match.group(2))
    await event.answer("Обновляю…")
    try:
        count = await chats_svc.refresh_discovered_chats(account_user_id)
    except RuntimeError as exc:
        reason = str(exc)
        if reason == "session_not_authorized":
            msg = "Сессия не авторизована. Добавьте аккаунт заново."
        elif reason == "account_not_found":
            msg = "Аккаунт не найден."
        else:
            msg = f"Ошибка: {reason}"
        await render_menu(
            event,
            screen("⚠️", "Обновление групп", msg),
            [
                back_row(f"acc_chats_{account_user_id}".encode()),
                back_home_row(),
            ],
        )
        return
    except Exception as exc:
        logger.exception("Chat discovery refresh failed account={}: {}", account_user_id, exc)
        await render_menu(
            event,
            screen("⚠️", "Обновление групп", f"Не удалось: `{type(exc).__name__}`"),
            [
                back_row(f"acc_chats_{account_user_id}".encode()),
                back_home_row(),
            ],
        )
        return

    await show_chats_menu(event, account_user_id, page)
    try:
        from services import monitor as monitor_svc
        await monitor_svc.refresh_monitor()
    except Exception as exc:
        logger.warning("Monitor refresh after chat discovery failed: {}", exc)
    # Second answer not always allowed; ignore if already answered.
    try:
        await event.answer(f"Найдено: {count}")
    except Exception as exc:
        logger.debug("Second chat refresh callback answer ignored: {}", exc)


@bot.on(events.CallbackQuery(pattern=rb"^chat_sel_(\d+)_(-?\d+)_(\d+)$"))
async def cb_chat_sel(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    account_user_id = int(event.pattern_match.group(1))
    chat_id = int(event.pattern_match.group(2))
    page = int(event.pattern_match.group(3))
    now_on = chats_svc.toggle_selected(account_user_id, chat_id)
    await show_chats_menu(event, account_user_id, page)
    try:
        from services import monitor as monitor_svc
        await monitor_svc.refresh_monitor()
    except Exception as exc:
        logger.warning("Monitor refresh after chat selection failed: {}", exc)
    await event.answer("Выбран" if now_on else "Снят")


@bot.on(events.CallbackQuery(pattern=rb"^chat_exc_(\d+)_(-?\d+)_(\d+)$"))
async def cb_chat_exc(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    account_user_id = int(event.pattern_match.group(1))
    chat_id = int(event.pattern_match.group(2))
    page = int(event.pattern_match.group(3))
    now_exc = chats_svc.toggle_excluded(account_user_id, chat_id)
    await show_chats_menu(event, account_user_id, page)
    try:
        from services import monitor as monitor_svc
        await monitor_svc.refresh_monitor()
    except Exception as exc:
        logger.warning("Monitor refresh after chat exclusion failed: {}", exc)
    await event.answer("Исключён" if now_exc else "Снова слушать")
