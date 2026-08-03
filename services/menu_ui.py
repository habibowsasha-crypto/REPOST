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

    - CallbackQuery + edit=True: always try to edit the same message.
      If content is unchanged (Telegram "not modified") - stay on same message.
      Never spawn a duplicate menu from a button press.
    - NewMessage / edit=False: send a new message.
    """
    btn_list = [list(row) for row in buttons] if buttons else None

    is_callback = getattr(event, "query", None) is not None

    if edit and is_callback:
        try:
            await event.edit(text, buttons=btn_list, link_preview=False)
            return
        except Exception as exc:
            err = str(exc).lower()
            # Same text/buttons - Telegram rejects edit. Keep current message.
            if (
                "not modified" in err
                or "message is not modified" in err
                or "content of the message" in err
            ):
                return
            # Other edit failures: still do NOT open a second menu from a callback.
            # Admin can re-send /menu if the message was deleted.
            return

    respond = getattr(event, "respond", None)
    if callable(respond):
        await respond(text, buttons=btn_list, link_preview=False)
        return

    answer = getattr(event, "answer", None)
    if callable(answer):
        await answer(text[:200])
