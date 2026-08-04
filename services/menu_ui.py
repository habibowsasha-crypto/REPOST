"""Shared helpers for admin inline menus (render + nav)."""

from __future__ import annotations

from typing import Any, Optional, Sequence, Union

from loguru import logger
from telethon import Button
from telethon.events.callbackquery import CallbackQuery
from telethon.events.newmessage import NewMessage

from services.ui import back_home_row, back_row, btn  # noqa: F401 — re-export

EventLike = Union[NewMessage.Event, CallbackQuery.Event, Any]


async def render_menu(
    event: EventLike,
    text: str,
    buttons: Optional[Sequence[Sequence[Button]]] = None,
    *,
    edit: bool = True,
) -> None:
    """
    Show a menu screen.

    - CallbackQuery + edit=True: edit the same message (never spawn a duplicate).
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
            if (
                "not modified" in err
                or "message is not modified" in err
                or "content of the message" in err
            ):
                return
            # Other edit failures: do not open a second menu from a callback,
            # but keep the cause visible in logs.
            logger.warning("Menu edit failed: {}", exc)
            return

    respond = getattr(event, "respond", None)
    if callable(respond):
        await respond(text, buttons=btn_list, link_preview=False)
        return

    answer = getattr(event, "answer", None)
    if callable(answer):
        await answer(text[:200])
