"""Telegram bot handlers. Importing this package registers event handlers."""

from handlers import accounts  # noqa: F401
from handlers import chats  # noqa: F401
from handlers import dispatcher_ui  # noqa: F401
from handlers import dialogs_ui  # noqa: F401
from handlers import menu  # noqa: F401
from handlers import optout  # noqa: F401
from handlers import queue_ui  # noqa: F401
from handlers import audience_ui  # noqa: F401

__all__ = ["menu", "accounts", "chats", "queue_ui", "dispatcher_ui", "dialogs_ui", "optout", "audience_ui"]
