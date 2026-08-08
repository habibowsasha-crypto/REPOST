"""First-DM protection for administrators/owners of monitored source groups."""

from __future__ import annotations

import time
from typing import Any

# A short process-local cache avoids
# issuing one GetParticipant request for every message received by every account.
_ROLE_CACHE_TTL_SECONDS = 30 * 60
_role_cache: dict[tuple[int, int], tuple[bool, float]] = {}


def clear_cache() -> None:
    """Test/maintenance helper; no production state is persisted here."""
    _role_cache.clear()


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
    _role_cache[(int(chat_id), int(target_user_id))] = (
        bool(is_admin),
        time.monotonic() + _ROLE_CACHE_TTL_SECONDS,
    )


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
