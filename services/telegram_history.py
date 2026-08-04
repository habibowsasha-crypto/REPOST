"""Helpers for reconciling ambiguous Telegram sends without fixed history limits."""

from __future__ import annotations

import datetime as dt
from typing import Any, AsyncIterator


def normalize_text(value: str | None) -> str:
    return (value or "").replace("\r\n", "\n").strip()


def normalize_date(value: Any) -> dt.datetime | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        parsed = value
    else:
        try:
            parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


async def _all_messages(client: Any, entity: Any) -> AsyncIterator[Any]:
    """Yield newest-to-oldest messages without a numeric scan cap.

    Real Telethon clients expose ``iter_messages``. Lightweight test doubles may
    only expose ``get_messages``; for those we request ``limit=None`` rather than
    silently introducing another 30/40/100-message cutoff.
    """
    iterator_factory = getattr(client, "iter_messages", None)
    if callable(iterator_factory):
        async for message in iterator_factory(entity):
            yield message
        return

    messages = await client.get_messages(entity, limit=None)
    for message in messages or []:
        yield message


async def find_outgoing_text_since(
    client: Any,
    entity: Any,
    expected_text: str,
    *,
    since: dt.datetime | None,
) -> Any | None:
    expected = normalize_text(expected_text)
    async for message in _all_messages(client, entity):
        message_date = normalize_date(getattr(message, "date", None))
        if since is not None and message_date is not None and message_date < since:
            # Telethon iterates newest to oldest, so older rows cannot match.
            break
        if not bool(getattr(message, "out", False)):
            continue
        if normalize_text(getattr(message, "message", None)) != expected:
            continue
        return message
    return None
