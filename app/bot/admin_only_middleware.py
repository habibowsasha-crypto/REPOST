"""Aiogram ingress gate for ADMIN_ONLY_MODE."""

from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable

from aiogram import BaseMiddleware
from aiogram.types import CallbackQuery, Message, TelegramObject

from app.config import get_settings
from app.services.admin_only_mode import (
    admin_only_enabled,
    admin_only_trusted_source_message_allowed,
    admin_only_user_allowed,
)

log = logging.getLogger(__name__)


class AdminOnlyAccessMiddleware(BaseMiddleware):
    """Keep Telegram interaction private to admins while preserving signal input.

    In ADMIN_ONLY_MODE an admin may interact with the bot only in a private chat.
    Trusted sender_chat posts from configured source chats are allowed as a
    read-only signal feed; handlers are separately wired not to answer back into
    those chats. Every other update is silently ignored.
    """

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        settings = get_settings()
        if not admin_only_enabled(settings):
            return await handler(event, data)

        if isinstance(event, Message):
            chat = getattr(event, "chat", None)
            chat_type = str(getattr(chat, "type", "") or "").lower()
            actor = getattr(event, "from_user", None)
            actor_id = getattr(actor, "id", None)

            if chat_type == "private" and admin_only_user_allowed(actor_id, settings):
                return await handler(event, data)

            if admin_only_trusted_source_message_allowed(
                chat_id=getattr(chat, "id", None),
                chat_type=chat_type,
                sender_chat_present=getattr(event, "sender_chat", None) is not None,
                settings=settings,
            ):
                return await handler(event, data)

            log.info(
                "ADMIN_ONLY_UPDATE_SUPPRESSED type=Message actor_present=%s chat_type=%s",
                int(actor_id is not None),
                chat_type or "unknown",
            )
            return None

        if isinstance(event, CallbackQuery):
            actor = getattr(event, "from_user", None)
            actor_id = getattr(actor, "id", None)
            callback_chat = getattr(getattr(event, "message", None), "chat", None)
            callback_chat_type = str(
                getattr(callback_chat, "type", "") or ""
            ).lower()
            if (
                callback_chat_type == "private"
                and admin_only_user_allowed(actor_id, settings)
            ):
                return await handler(event, data)
            log.info(
                "ADMIN_ONLY_UPDATE_SUPPRESSED type=CallbackQuery actor_present=%s chat_type=%s",
                int(actor_id is not None),
                callback_chat_type or "unknown",
            )
            return None

        log.info("ADMIN_ONLY_UPDATE_SUPPRESSED type=%s", type(event).__name__)
        return None
