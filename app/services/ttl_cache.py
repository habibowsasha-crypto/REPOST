"""Small process-local async caches for hot-path database and BingX reads.

v1.5.13 adds single-flight coalescing: if several coroutines ask for the same
key while it is cold, only one real database/exchange request is executed and
all callers await that task. This removes duplicate BingX metadata reads and
reduces load during rapid Telegram/menu activity.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Awaitable, Callable, TypeVar

T = TypeVar("T")


class TTLCache:
    """Generic async TTL cache with per-key single-flight fetches."""

    def __init__(self, ttl_seconds: float, *, max_entries: int = 5000) -> None:
        self._ttl = max(0.0, float(ttl_seconds))
        self._max_entries = max(32, int(max_entries))
        self._store: dict[Any, tuple[float, Any]] = {}
        self._inflight: dict[Any, asyncio.Task[Any]] = {}
        self._generation: dict[Any, int] = {}
        self._lock = asyncio.Lock()

    def _prune_unlocked(self) -> None:
        if len(self._store) <= self._max_entries:
            return
        overflow = len(self._store) - self._max_entries
        for key, _ in sorted(self._store.items(), key=lambda item: item[1][0])[
            :overflow
        ]:
            self._store.pop(key, None)
            if key not in self._inflight:
                self._generation.pop(key, None)

    @staticmethod
    def _consume_task_result(task: asyncio.Task[Any]) -> None:
        """Retrieve detached task failures after all callers were cancelled.

        ``asyncio.shield`` deliberately keeps the shared fetch alive when one
        Telegram handler is cancelled. If that was the last waiter and the
        fetch later fails, nobody awaits the task and asyncio otherwise emits
        ``Task exception was never retrieved``. Reading ``exception()`` here
        does not prevent active awaiters from receiving the same exception.
        """
        if task.cancelled():
            return
        try:
            task.exception()
        except (asyncio.CancelledError, Exception):
            pass

    async def get_or_fetch(self, key: Any, fetcher: Callable[[], Awaitable[T]]) -> T:
        """Return a fresh cached value or coalesce one in-flight fetch."""
        async with self._lock:
            now = time.monotonic()
            cached = self._store.get(key)
            if cached and (now - cached[0]) < self._ttl:
                return cached[1]

            task = self._inflight.get(key)
            if task is None:
                generation = self._generation.get(key, 0)

                async def runner() -> T:
                    try:
                        value = await fetcher()
                        async with self._lock:
                            if self._generation.get(key, 0) == generation:
                                self._store[key] = (time.monotonic(), value)
                                self._prune_unlocked()
                        return value
                    finally:
                        async with self._lock:
                            current = self._inflight.get(key)
                            if current is asyncio.current_task():
                                self._inflight.pop(key, None)
                                if key not in self._store:
                                    self._generation.pop(key, None)

                task = asyncio.create_task(runner(), name=f"ttl-cache:{key !r}"[:120])
                task.add_done_callback(self._consume_task_result)
                self._inflight[key] = task

                # A cancelled Telegram handler must not cancel the shared underlying
                # BingX/DB request that another caller is also awaiting.
        return await asyncio.shield(task)

    def invalidate(self, key: Any) -> None:
        """Remove a key and prevent an older in-flight fetch from re-caching it."""
        self._store.pop(key, None)
        if key in self._inflight:
            self._generation[key] = self._generation.get(key, 0) + 1
        else:
            # No runner can re-cache this key, so retaining a generation entry
            # would only leak metadata for historical users/symbols.
            self._generation.pop(key, None)

    def clear(self) -> None:
        """Clear cached values without cancelling in-flight network writes/reads."""
        self._store.clear()
        active_generations = {
            key: self._generation.get(key, 0) + 1 for key in self._inflight
        }
        self._generation.clear()
        self._generation.update(active_generations)

    def size(self) -> int:
        return len(self._store)


_instrument_info_cache = TTLCache(ttl_seconds=3600.0, max_entries=2000)
_api_key_cache = TTLCache(ttl_seconds=300.0, max_entries=5000)
_user_settings_cache = TTLCache(ttl_seconds=300.0, max_entries=5000)
_dashboard_cache = TTLCache(ttl_seconds=5.0, max_entries=5000)
# Very short public-price cache: coalesces one MARKET wave without reusing a
# stale price for the following wave of users.
_market_price_cache = TTLCache(ttl_seconds=0.35, max_entries=2000)


def get_instrument_info_cache() -> TTLCache:
    return _instrument_info_cache


def get_api_key_cache() -> TTLCache:
    return _api_key_cache


def get_user_settings_cache() -> TTLCache:
    return _user_settings_cache


def get_dashboard_cache() -> TTLCache:
    return _dashboard_cache


def get_market_price_cache() -> TTLCache:
    return _market_price_cache


def invalidate_user(user_id: int) -> None:
    """Drop API/settings/dashboard cache entries for one Telegram user."""
    uid = int(user_id)
    for cache in (_api_key_cache, _user_settings_cache, _dashboard_cache):
        keys_to_drop = [
            key
            for key in list(cache._store)
            if isinstance(key, tuple) and key and key[0] == uid
        ]
        keys_to_drop.extend(
            key
            for key in list(cache._inflight)
            if isinstance(key, tuple)
            and key
            and key[0] == uid
            and key not in keys_to_drop
        )
        for key in keys_to_drop:
            cache.invalidate(key)
