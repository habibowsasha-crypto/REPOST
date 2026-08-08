"""Кэш открытых позиций для одного цикла монитора.

Назначение: устранить дублирующиеся HTTP-запросы к BingX positions API.

BingX positions endpoint возвращает все открытые
позиции одним запросом, но текущий код вызывает fetch_open_positions(symbol)
для каждой execution_row отдельно. При 5 execution = 5 одинаковых HTTP-запросов.

Решение: per-cycle кэш с коротким TTL. При первом fetch в цикле — реальный
HTTP. Все последующие fetch других symbol/side — читают из кэша.

ВАЖНО: после операций которые меняют позицию (cancel order, create order,
emergency_close_market) кэш должен инвалидироваться через invalidate().
"""

from __future__ import annotations

import asyncio
import time
from typing import Any


class PositionsCache:
    """Локальный кэш позиций по (user_id, exchange) с TTL.

    Использование внутри одного prокса монитора:
        cache = PositionsCache(ttl_seconds=3.0)
        positions = await cache.get(adapter, user_id, exchange, symbol, side)
        # ... делаем cancel/create операции ...
        cache.invalidate(user_id, exchange)
        positions = await cache.get(adapter, user_id, exchange, symbol, side)
    """

    def __init__(self, ttl_seconds: float = 3.0, *, max_entries: int = 5000) -> None:
        self._cache: dict[tuple[int, str], tuple[float, list[dict]]] = {}
        self._ttl = max(0.0, float(ttl_seconds))
        self._max_entries = max(32, int(max_entries))
        # Per-key lock so two concurrent tasks for the same (user, exchange)
        # don't both fire a duplicate HTTP fetch when the cache is cold.
        self._locks: dict[tuple[int, str], asyncio.Lock] = {}

    def _drop_idle_lock(self, key: tuple[int, str]) -> None:
        lock = self._locks.get(key)
        if lock is not None and not lock.locked():
            self._locks.pop(key, None)

    def _prune(self, *, protected_key: tuple[int, str] | None = None) -> None:
        if len(self._cache) > self._max_entries:
            overflow = len(self._cache) - self._max_entries
            oldest = sorted(self._cache.items(), key=lambda item: item[1][0])[:overflow]
            for old_key, _ in oldest:
                self._cache.pop(old_key, None)
                self._drop_idle_lock(old_key)

        if len(self._locks) > self._max_entries:
            for old_key, old_lock in list(self._locks.items()):
                if len(self._locks) <= self._max_entries:
                    break
                if (
                    old_key != protected_key
                    and old_key not in self._cache
                    and not old_lock.locked()
                ):
                    self._locks.pop(old_key, None)
            for old_key, old_lock in list(self._locks.items()):
                if len(self._locks) <= self._max_entries:
                    break
                if old_key != protected_key and not old_lock.locked():
                    self._locks.pop(old_key, None)

    def _lock_for(self, key: tuple[int, str]) -> asyncio.Lock:
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
            self._prune(protected_key=key)
        return lock

    async def get(
        self,
        adapter: Any,
        user_id: int,
        exchange: str,
        symbol: str | None = None,
        side: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get open positions for user, optionally filtered by symbol/side.

        On first call for (user_id, exchange) — fetches ALL positions from
        the exchange. Subsequent calls within TTL filter from the cache.
        """
        key = (int(user_id), str(exchange))
        now = time.monotonic()
        cached = self._cache.get(key)
        if cached and (now - cached[0]) < self._ttl:
            all_positions = cached[1]
        else:
            # Lock per (user, exchange) so two concurrent monitor tasks for the
            # same user don't both fire the same HTTP request when cache is cold.
            async with self._lock_for(key):
                # Re-check: another task may have populated the cache while we waited.
                cached = self._cache.get(key)
                now = time.monotonic()
                if cached and (now - cached[0]) < self._ttl:
                    all_positions = cached[1]
                else:
                    try:
                        all_positions = await adapter.fetch_open_positions(None)
                    except TypeError:
                        # Some adapters require explicit None — try without arg
                        all_positions = await adapter.fetch_open_positions()
                    self._cache[key] = (time.monotonic(), all_positions)
                    self._prune()

                    # Apply local filter (same logic as adapter's per-symbol filter).
        if not symbol and not side:
            return list(all_positions)

        out: list[dict[str, Any]] = []
        symbol_u = symbol.upper() if symbol else None
        side_u = side.upper() if side else None
        for p in all_positions:
            if symbol_u and str(p.get("symbol", "")).upper() != symbol_u:
                continue
            if side_u:
                pside = str(p.get("side") or p.get("positionSide") or "").upper()
                if pside != side_u:
                    continue
            size = float(
                p.get("size") or p.get("availableSize") or p.get("positionAmt") or 0
            )
            if abs(size) > 0:
                out.append(p)
        return out

    def invalidate(self, user_id: int, exchange: str) -> None:
        """Drop cached positions for this user after a mutating operation."""
        key = (int(user_id), str(exchange))
        self._cache.pop(key, None)
        self._drop_idle_lock(key)

    def clear(self) -> None:
        """Drop cached positions and release all idle per-key locks."""
        self._cache.clear()
        for key in list(self._locks):
            self._drop_idle_lock(key)

            # Shared across BE and lifecycle workers. A two-second TTL is short enough for
            # protection logic while coalescing duplicate all-position reads started by the
            # two workers at nearly the same time.


_GLOBAL_POSITIONS_CACHE = PositionsCache(ttl_seconds=2.0)


def get_global_positions_cache() -> PositionsCache:
    return _GLOBAL_POSITIONS_CACHE
