"""Opt-out management UI — unified style."""

from __future__ import annotations

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
        body = "Список пуст — никто не в opt-out."
    else:
        lines = []
        for r in rows:
            tid = r.get("user_id")
            reason = (r.get("reason") or "")[:40]
            lines.append(f"• `{tid}`" + (f" — {reason}" if reason else ""))
        body = join(f"Показано {len(rows)} из {total}:", *lines)
    return screen(
        "🚫",
        "Opt-out",
        kv("Всего", str(total)),
        body,
        "Этим людям бот больше не пишет.",
    )


def _optout_buttons():
    return [
        [btn("🔄 Обновить", b"menu_optout")],
        [btn("➕ Добавить по ID", b"opt_add")],
        [btn("➖ Снять по ID", b"opt_remove")],
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
        "Добавить в opt-out",
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
        "Снять opt-out",
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
        opt_out_svc.add(target_id, reason="manual_admin")
        msg = notice("ok", f"`{target_id}` добавлен в opt-out.")
    else:
        opt_out_svc.remove(target_id)
        msg = notice("ok", f"`{target_id}` снят с opt-out.")
    await event.respond(
        screen("🚫", "Opt-out", msg),
        buttons=[back_home_row()],
    )
