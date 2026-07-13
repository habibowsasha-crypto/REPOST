from __future__ import annotations

from typing import Any

from loguru import logger
from telethon.errors import MessageNotModifiedError


async def render_menu(event: Any, text: str, *args: Any, **kwargs: Any) -> Any:
    """
    Render callback-driven UI inside the same Telegram message.

    Callback events are edited in place and never create a replacement message.
    Ordinary NewMessage events receive a normal response.
    """
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
