"""Admin Opt-out UI: list and remove bans."""

from __future__ import annotations

import re

from telethon import Button, events

from config import bot, is_admin
from services import opt_out as opt_out_svc
from services.admin_state import clear_state, get_state, set_state
from services.menu_ui import back_home_row, back_row, render_menu

_PAGE = 15


def _optout_home_text() -> str:
    total = opt_out_svc.count()
    return (
        "**🚫 Opt-out**\n\n"
        f"В списке: **{total}**\n\n"
        "Этим пользователям бот больше не пишет "
        "(после «не пиши» / агрессии или вручную).\n\n"
        "Можно снять запрет по кнопке в списке или вводом user id."
    )


async def show_optout_menu(event, *, edit: bool = True) -> None:
    await render_menu(
        event,
        _optout_home_text(),
        [
            [Button.inline("📋 Список", b"optout_list")],
            [Button.inline("✅ Снять запрет (ввести id)", b"optout_remove")],
            [Button.inline("➕ Добавить в opt-out", b"optout_add")],
            back_home_row(),
        ],
        edit=edit,
    )


async def show_optout_list(event, page: int = 0, *, edit: bool = True) -> None:
    rows = opt_out_svc.list_all(limit=200)
    total = len(rows)
    if total == 0:
        await render_menu(
            event,
            "**📋 Opt-out список**\n\nПока пусто.",
            [
                [Button.inline("➕ Добавить", b"optout_add")],
                back_row(b"menu_optout"),
                back_home_row(),
            ],
            edit=edit,
        )
        return

    pages = max(1, (total + _PAGE - 1) // _PAGE)
    page = max(0, min(int(page), pages - 1))
    chunk = rows[page * _PAGE : (page + 1) * _PAGE]

    lines = [f"**📋 Opt-out** (стр. {page + 1}/{pages}, всего {total})\n"]
    buttons: list[list[Button]] = []
    for row in chunk:
        uid = int(row["user_id"])
        reason = (row.get("reason") or "-")[:40]
        created = (row.get("created_at") or "")[:19]
        lines.append(f"`{uid}` | {reason} | {created}")
        buttons.append(
            [Button.inline(f"✅ Снять {uid}", f"optout_del_{uid}".encode())]
        )

    nav: list[Button] = []
    if page > 0:
        nav.append(Button.inline("⬅️", f"optout_page_{page - 1}".encode()))
    nav.append(Button.inline(f"{page + 1}/{pages}", f"optout_page_{page}".encode()))
    if page + 1 < pages:
        nav.append(Button.inline("➡️", f"optout_page_{page + 1}".encode()))
    if nav:
        buttons.append(nav)
    buttons.append(back_row(b"menu_optout"))
    buttons.append(back_home_row())
    await render_menu(event, "\n".join(lines), buttons, edit=edit)


@bot.on(events.CallbackQuery(data=b"menu_optout"))
async def cb_menu_optout(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    clear_state(int(event.sender_id))
    await show_optout_menu(event)
    await event.answer()


@bot.on(events.CallbackQuery(data=b"optout_list"))
async def cb_optout_list(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    await show_optout_list(event, 0)
    await event.answer()


@bot.on(events.CallbackQuery(pattern=rb"^optout_page_(\d+)$"))
async def cb_optout_page(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    page = int(event.pattern_match.group(1))
    await show_optout_list(event, page)
    await event.answer()


@bot.on(events.CallbackQuery(pattern=rb"^optout_del_(\d+)$"))
async def cb_optout_del(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    uid = int(event.pattern_match.group(1))
    ok = opt_out_svc.remove(uid)
    await show_optout_list(event, 0)
    await event.answer("Снято" if ok else "Не найден")


@bot.on(events.CallbackQuery(data=b"optout_remove"))
async def cb_optout_remove(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    set_state(int(event.sender_id), flow="optout", step="remove")
    await render_menu(
        event,
        "**✅ Снять запрет**\n\n"
        "Отправьте **user id** числом (например `123456789`).\n"
        "Отмена: /cancel",
        [[Button.inline("❌ Отмена", b"menu_optout")]],
    )
    await event.answer()


@bot.on(events.CallbackQuery(data=b"optout_add"))
async def cb_optout_add(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    set_state(int(event.sender_id), flow="optout", step="add")
    await render_menu(
        event,
        "**➕ Добавить в opt-out**\n\n"
        "Отправьте **user id** числом.\n"
        "Отмена: /cancel",
        [[Button.inline("❌ Отмена", b"menu_optout")]],
    )
    await event.answer()


@bot.on(
    events.NewMessage(
        func=lambda e: (
            is_admin(e.sender_id)
            and (get_state(int(e.sender_id)) or {}).get("flow") == "optout"
            and not (e.raw_text or "").strip().startswith("/")
        )
    )
)
async def optout_text_handler(event: events.NewMessage.Event) -> None:
    admin_id = int(event.sender_id)
    state = get_state(admin_id) or {}
    step = state.get("step")
    raw = (event.raw_text or "").strip()
    if not re.fullmatch(r"\d{5,15}", raw):
        await event.respond("⚠ Нужен числовой user id (5–15 цифр). Или /cancel")
        return
    uid = int(raw)

    if step == "remove":
        ok = opt_out_svc.remove(uid)
        clear_state(admin_id)
        if ok:
            await event.respond(f"✅ Запрет снят для `{uid}`.")
        else:
            await event.respond(f"В opt-out не было `{uid}`.")
        await show_optout_menu(event, edit=False)
        return

    if step == "add":
        opt_out_svc.add(uid, reason="manual_admin")
        clear_state(admin_id)
        await event.respond(f"✅ `{uid}` добавлен в opt-out.")
        await show_optout_menu(event, edit=False)
        return

    clear_state(admin_id)
