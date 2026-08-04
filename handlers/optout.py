"""Opt-out management UI — unified style."""

from __future__ import annotations

from loguru import logger
from telethon import events

from config import bot, is_admin
from services import opt_out as opt_out_svc
from services.admin_state import clear_state, get_state, set_state
from services.menu_ui import render_menu
from services.ui import (
    DENIED,
    back_home_row,
    back_row,
    btn,
    join,
    kv,
    notice,
    screen,
)


def _optout_text() -> str:
    total = opt_out_svc.count()
    rows = opt_out_svc.list_all(limit=20)
    if not rows:
        body = "Список пуст — запретов на отправку нет."
    else:
        lines = []
        for r in rows:
            tid = r.get("user_id")
            reason = (r.get("reason") or "")[:40]
            lines.append(f"• `{tid}`" + (f" — {reason}" if reason else ""))
        body = join(f"Показано {len(rows)} из {total}:", *lines)
    return screen(
        "🚫",
        "Кому не писать",
        f"🚫 Всего людей: **{total}**",
        body,
        "Этим людям бот больше не пишет и не продолжает активные диалоги.",
    )


def _optout_buttons():
    return [
        [btn("🔄 ОБНОВИТЬ", b"menu_optout")],
        [btn("➕ ДОБАВИТЬ", b"opt_add")],
        [btn("➖ УБРАТЬ", b"opt_remove")],
        back_home_row(),
    ]


@bot.on(events.CallbackQuery(data=b"menu_optout"))
async def cb_menu_optout(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    await render_menu(event, _optout_text(), _optout_buttons())
    await event.answer()


@bot.on(events.CallbackQuery(data=b"opt_add"))
async def cb_opt_add(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    set_state(event.sender_id, flow="optout", step="opt_add")
    text = screen(
        "➕",
        "Добавить в «Не писать»",
        "Пришли **числовой user id** одним сообщением.",
        "Отмена: /cancel",
    )
    await render_menu(event, text, [back_row(b"menu_optout"), back_home_row()])
    await event.answer()


@bot.on(events.CallbackQuery(data=b"opt_remove"))
async def cb_opt_remove(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    set_state(event.sender_id, flow="optout", step="opt_remove")
    text = screen(
        "➖",
        "Убрать из «Не писать»",
        "Пришли **числовой user id** одним сообщением.",
        "Отмена: /cancel",
    )
    await render_menu(event, text, [back_row(b"menu_optout"), back_home_row()])
    await event.answer()


@bot.on(
    events.NewMessage(
        func=lambda e: e.is_private
        and is_admin(e.sender_id)
        and (get_state(int(e.sender_id)) or {}).get("flow") == "optout"
    )
)
async def on_optout_text(event: events.NewMessage.Event) -> None:
    st = get_state(event.sender_id) or {}
    step = st.get("step")
    if step not in {"opt_add", "opt_remove"}:
        return
    raw = (event.raw_text or "").strip()
    if raw.startswith("/"):
        return
    if not raw.lstrip("-").isdigit():
        await event.respond(notice("warn", "Нужен числовой user id. /cancel — отмена."))
        return
    target_id = int(raw)
    clear_state(event.sender_id)
    if step == "opt_add":
        from services import dialog_store as dialog_store_svc

        dialog = dialog_store_svc.get_dialog(target_id)
        account_user_id = int(dialog.get("account_user_id") or 0) if dialog else 0
        opt_out_svc.add(target_id, reason="manual_admin")
        if account_user_id:
            try:
                from services import monitor as monitor_svc

                await monitor_svc.maybe_disconnect_inactive_account(account_user_id)
            except Exception as exc:
                logger.warning("Opt-out account cleanup failed account={}: {}", account_user_id, exc)
        msg = notice("ok", f"`{target_id}` добавлен в «Не писать». Активный диалог остановлен.")
    else:
        opt_out_svc.remove(target_id)
        msg = notice("ok", f"`{target_id}` убран из «Не писать».")
    await event.respond(
        screen("🚫", "Кому не писать", msg),
        buttons=[back_home_row()],
    )
