"""Central ADMIN_ONLY_MODE policy helpers.

When enabled, the bot is private to configured ADMIN_IDS for new trade execution,
interactive Telegram use and private notifications. Trusted signal-source posts
remain readable so the admin account can continue receiving automated entries.
Existing non-admin executions are intentionally not orphaned: monitor/recovery
workers may keep managing their already-open protective lifecycle until terminal.
"""

from __future__ import annotations

from typing import Any

from app.config import get_settings


def admin_only_enabled(settings: Any | None = None) -> bool:
    settings = settings or get_settings()
    return bool(getattr(settings, "ADMIN_ONLY_MODE", False))


def configured_admin_ids(settings: Any | None = None) -> frozenset[int]:
    settings = settings or get_settings()
    return frozenset(
        int(value)
        for value in getattr(settings, "admin_ids", [])
        if int(value) > 0
    )


def admin_only_user_allowed(user_id: int | None, settings: Any | None = None) -> bool:
    """Return whether a Telegram user may interact/receive/new-trade in current mode."""
    settings = settings or get_settings()
    if not admin_only_enabled(settings):
        return True
    try:
        uid = int(user_id or 0)
    except (TypeError, ValueError, OverflowError):
        return False
    return uid > 0 and uid in configured_admin_ids(settings)


def admin_only_trade_user_allowed(user_id: int | None, settings: Any | None = None) -> bool:
    """Alias kept explicit at trade boundaries for readable safety checks."""
    return admin_only_user_allowed(user_id, settings)


def admin_only_trusted_source_message_allowed(
    *,
    chat_id: int | None,
    chat_type: str | None,
    sender_chat_present: bool,
    settings: Any | None = None,
) -> bool:
    """Allow only the passive trusted-source feed while ADMIN_ONLY_MODE is active.

    This does not authorize a trade by itself. Existing source validation in
    handlers still checks trusted chat IDs and sender_chat identity/titles. The
    purpose here is only to let the signal-ingress handler *read* channel-origin
    posts while ignoring ordinary non-admin Telegram users.
    """
    settings = settings or get_settings()
    if not admin_only_enabled(settings):
        return True
    try:
        cid = int(chat_id or 0)
    except (TypeError, ValueError, OverflowError):
        return False
    normalized_type = str(chat_type or "").strip().lower()
    if normalized_type not in {"group", "supergroup", "channel"}:
        return False
    if cid not in set(getattr(settings, "allowed_source_chat_ids", []) or []):
        return False
    # In private-admin mode, ordinary group members must never gain an
    # interaction path merely because they are inside the trusted source chat.
    return bool(sender_chat_present)
