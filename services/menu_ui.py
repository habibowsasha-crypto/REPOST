from __future__ import annotations

from typing import Any

from loguru import logger
from telethon.errors import MessageNotModifiedError


def _looks_like_html(text: str) -> bool:
    sample = str(text or "")
    return any(tag in sample for tag in ("<b>", "<i>", "<code>", "<a ", "</", "<u>"))


async def render_menu(event: Any, text: str, *args: Any, **kwargs: Any) -> Any:
    """
    Render callback-driven UI inside the same Telegram message.

    Callback events are edited in place and never create a replacement message.
    Ordinary NewMessage events receive a normal response.

    If the text contains HTML tags and parse_mode is not set, HTML is applied
    automatically so admin screens do not show raw <b> tags.
    """
    if "parse_mode" not in kwargs and _looks_like_html(text):
        kwargs["parse_mode"] = "html"
    if "link_preview" not in kwargs:
        kwargs["link_preview"] = False

    if getattr(event, "query", None) is not None:
        try:
            return await event.edit(text, *args, **kwargs)
        except MessageNotModifiedError:
            try:
                await event.answer()
            except Exception:
                pass
            return None
        except Exception as exc:
            logger.warning(f"Не удалось обновить сообщение меню: {exc}")
            try:
                await event.answer(
                    "Не удалось обновить меню. Отправьте «Меню» ещё раз.",
                    alert=True,
                )
            except Exception:
                pass
            return None

    return await event.respond(text, *args, **kwargs)
