"""First-DM protection for administrators/owners of monitored source groups."""

from __future__ import annotations

import time
from typing import Any

# A short process-local cache avoids
# issuing one GetParticipant request for every message received by every account.
_ROLE_CACHE_TTL_SECONDS = 30 * 60
# Keep the process-local cache bounded on long-running deployments. Monitored
# groups can expose an effectively unbounded stream of unique user IDs; expired
# entries must therefore be pruned even when the same user never appears again.
_ROLE_CACHE_MAX_ENTRIES = 10_000
_ROLE_CACHE_PRUNE_EVERY = 256
# If Telegram explicitly says this account cannot inspect participants/admins in
# a monitored chat, do not repeat the same doomed permission request for every
# incoming message. This cooldown affects only the admin-role check; First DM
# remains fail-open and continues normally.
_CHAT_CHECK_UNAVAILABLE_TTL_SECONDS = 15 * 60
_CHAT_CHECK_UNAVAILABLE_MAX_ENTRIES = 5_000
_role_cache: dict[tuple[int, int], tuple[bool, float]] = {}
_unavailable_chat_checks: dict[tuple[int, int], float] = {}
_cache_put_count = 0


def clear_cache() -> None:
    """Test/maintenance helper; no production state is persisted here."""
    global _cache_put_count
    _role_cache.clear()
    _unavailable_chat_checks.clear()
    _cache_put_count = 0


def _prune_unavailable_chat_checks(*, now: float | None = None) -> None:
    current = time.monotonic() if now is None else float(now)
    expired = [
        key for key, expires_at in _unavailable_chat_checks.items()
        if expires_at <= current
    ]
    for key in expired:
        _unavailable_chat_checks.pop(key, None)

    if len(_unavailable_chat_checks) <= _CHAT_CHECK_UNAVAILABLE_MAX_ENTRIES:
        return
    overflow = len(_unavailable_chat_checks) - _CHAT_CHECK_UNAVAILABLE_MAX_ENTRIES
    oldest = sorted(_unavailable_chat_checks.items(), key=lambda item: item[1])[:overflow]
    for key, _expires_at in oldest:
        _unavailable_chat_checks.pop(key, None)


def is_chat_check_suppressed(account_user_id: int, chat_id: int) -> bool:
    """Return True while a known-unavailable account/chat check is cooling down."""
    key = (int(account_user_id), int(chat_id))
    expires_at = _unavailable_chat_checks.get(key)
    if expires_at is None:
        return False
    if time.monotonic() >= expires_at:
        _unavailable_chat_checks.pop(key, None)
        return False
    return True


def mark_chat_check_unavailable(
    account_user_id: int,
    chat_id: int,
    *,
    ttl_seconds: float | None = None,
) -> bool:
    """Cooldown repeated role checks; return True only when a new cooldown is set."""
    key = (int(account_user_id), int(chat_id))
    now = time.monotonic()
    current = _unavailable_chat_checks.get(key)
    if current is not None and current > now:
        return False
    ttl = (
        _CHAT_CHECK_UNAVAILABLE_TTL_SECONDS
        if ttl_seconds is None
        else max(1.0, float(ttl_seconds))
    )
    _unavailable_chat_checks[key] = now + ttl
    _prune_unavailable_chat_checks(now=now)
    return True


def clear_chat_check_unavailable(account_user_id: int, chat_id: int) -> None:
    _unavailable_chat_checks.pop((int(account_user_id), int(chat_id)), None)


def _prune_cache(*, now: float | None = None, enforce_limit: bool = False) -> None:
    """Drop expired role checks and keep the in-process cache bounded."""
    current = time.monotonic() if now is None else float(now)
    expired = [
        key
        for key, (_is_admin, expires_at) in _role_cache.items()
        if expires_at <= current
    ]
    for key in expired:
        _role_cache.pop(key, None)

    if not enforce_limit or len(_role_cache) <= _ROLE_CACHE_MAX_ENTRIES:
        return

    overflow = len(_role_cache) - _ROLE_CACHE_MAX_ENTRIES
    # Oldest-expiring valid entries are cheapest to evict. A later lookup simply
    # asks Telegram again; positive admin exclusions remain durable in SQLite.
    oldest = sorted(_role_cache.items(), key=lambda item: item[1][1])[:overflow]
    for key, _value in oldest:
        _role_cache.pop(key, None)


def _cache_get(chat_id: int, target_user_id: int) -> bool | None:
    key = (int(chat_id), int(target_user_id))
    row = _role_cache.get(key)
    if row is None:
        return None
    is_admin, expires_at = row
    if time.monotonic() >= expires_at:
        _role_cache.pop(key, None)
        return None
    return bool(is_admin)


def _cache_put(chat_id: int, target_user_id: int, is_admin: bool) -> None:
    global _cache_put_count
    now = time.monotonic()
    _role_cache[(int(chat_id), int(target_user_id))] = (
        bool(is_admin),
        now + _ROLE_CACHE_TTL_SECONDS,
    )
    _cache_put_count += 1
    periodic = _cache_put_count % max(1, int(_ROLE_CACHE_PRUNE_EVERY)) == 0
    over_limit = len(_role_cache) > _ROLE_CACHE_MAX_ENTRIES
    if periodic or over_limit:
        _prune_cache(now=now, enforce_limit=over_limit)


async def is_admin_or_owner(
    client: Any,
    *,
    chat_id: int,
    target_user_id: int,
    user_entity: Any | None = None,
    force_refresh: bool = False,
) -> bool:
    """Return True only for a confirmed administrator/creator.

    Exceptions are intentionally not swallowed. Callers decide how to handle
    an unavailable role check. Production First DM logic is fail-open: only a
    positively confirmed administrator/creator is excluded.
    """
    if not force_refresh:
        cached = _cache_get(chat_id, target_user_id)
        if cached is not None:
            return cached

    target = user_entity if user_entity is not None else int(target_user_id)
    permissions = await client.get_permissions(int(chat_id), target)
    is_admin = bool(
        getattr(permissions, "is_admin", False)
        or getattr(permissions, "is_creator", False)
    )
    _cache_put(chat_id, target_user_id, is_admin)
    return is_admin
