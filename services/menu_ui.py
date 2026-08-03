"""Shared helpers for admin inline menus."""

from __future__ import annotations

from typing import Any, Optional, Sequence, Union

from telethon import Button
from telethon.events.callbackquery import CallbackQuery
from telethon.events.newmessage import NewMessage

EventLike = Union[NewMessage.Event, CallbackQuery.Event, Any]


def back_home_row() -> list[Button]:
    return [Button.inline("◀️ Главное меню", b"menu_home")]


def back_row(data: bytes, label: str = "◀️ Назад") -> list[Button]:
    return [Button.inline(label, data)]


async def render_menu(
    event: EventLike,
    text: str,
    buttons: Optional[Sequence[Sequence[Button]]] = None,
    *,
    edit: bool = True,
) -> None:
    """
    Show a menu screen.

    - CallbackQuery: edit the message when possible.
    - NewMessage / fallback: respond with a new message.
    """
    btn_list = [list(row) for row in buttons] if buttons else None

    query = getattr(event, "query", None)
    if edit and query is not None:
        try:
            await event.edit(text, buttons=btn_list, link_preview=False)
            return
        except Exception:
            # Message not modified / can't edit - fall through to respond.
            pass

    respond = getattr(event, "respond", None)
    if callable(respond):
        await respond(text, buttons=btn_list, link_preview=False)
        return

    # Last resort for unusual event wrappers in tests.
    answer = getattr(event, "answer", None)
    if callable(answer):
        await answer(text[:200])
