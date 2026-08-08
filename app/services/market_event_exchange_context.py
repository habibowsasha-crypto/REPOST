from __future__ import annotations

import asyncio
import copy
import json
import time
from collections import Counter
from typing import Any, Awaitable, Callable


_READ_METHOD_TTLS = {
    "fetch_open_positions": 3.0,
    "fetch_entry_order_fill_status": 5.0,
    "fetch_protective_order_identity_detail": 5.0,
    "fetch_open_orders": 2.0,
    "fetch_open_algo_orders": 2.0,
    "fetch_position_tpsl_history": 5.0,
    "instrument_info": 60.0,
    "fetch_last_price": 1.0,
    "fetch_market_prices": 1.0,
}

_WRITE_PREFIXES = (
    "create_",
    "set_",
    "cancel_",
    "close_",
    "place_",
    "submit_",
    "replace_",
    "update_",
    "amend_",
    "delete_",
    "emergency_",
)


def _copy(value: Any) -> Any:
    try:
        return copy.deepcopy(value)
    except Exception:
        return value


def _stable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _stable(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [_stable(v) for v in value]
    if isinstance(value, set):
        return sorted((_stable(v) for v in value), key=lambda item: repr(item))
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return repr(value)


def _cache_key(account_key: tuple[int, str], method: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    payload = {
        "account": [int(account_key[0]), str(account_key[1]).lower()],
        "method": str(method),
        "args": _stable(args),
        "kwargs": _stable(kwargs),
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


class MarketEventExchangeContext:
    """One-event read coalescing and exchange-evidence snapshot boundary.

    The context is intentionally process-local and short-lived.  It is created
    for one market-event/user verification and is destroyed before the next
    durable attempt.  Only read methods are cached.  Any exchange write through
    the wrapped adapter invalidates all cached reads before and after the write,
    so post-write safety read-backs remain fresh.
    """

    def __init__(self, *, ttl_scale: float = 1.0) -> None:
        self._ttl_scale = max(0.0, float(ttl_scale or 1.0))
        self._cache: dict[str, tuple[float, Any]] = {}
        self._inflight: dict[str, tuple[int, asyncio.Task[Any]]] = {}
        self._tasks: set[asyncio.Task[Any]] = set()
        self._generation = 0
        self._lock = asyncio.Lock()
        self._stats: Counter[str] = Counter()

    def wrap_adapter(self, adapter: Any, account_key: tuple[int, str]) -> Any:
        if isinstance(adapter, _MarketEventAdapterProxy):
            return adapter
        return _MarketEventAdapterProxy(adapter, self, account_key)

    async def _cached_read(
        self,
        *,
        account_key: tuple[int, str],
        method: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        loader: Callable[[], Awaitable[Any]],
    ) -> Any:
        ttl = max(0.0, float(_READ_METHOD_TTLS.get(method, 0.0)) * self._ttl_scale)
        key = _cache_key(account_key, method, args, kwargs)
        self._stats[f"{method}_requests"] += 1
        now = time.monotonic()
        async with self._lock:
            generation = self._generation
            cached = self._cache.get(key)
            if cached is not None and ttl > 0 and now - cached[0] <= ttl:
                self._stats[f"{method}_hits"] += 1
                return _copy(cached[1])
            if cached is not None:
                self._cache.pop(key, None)
                self._stats[f"{method}_expirations"] += 1
            inflight = self._inflight.get(key)
            if inflight is None or inflight[0] != generation:
                task = asyncio.create_task(loader())
                self._inflight[key] = (generation, task)
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)
                self._stats[f"{method}_fetches"] += 1
            else:
                task = inflight[1]
                self._stats[f"{method}_singleflight_waits"] += 1

        try:
            value = await asyncio.shield(task)
        except asyncio.CancelledError:
            raise
        except Exception:
            async with self._lock:
                if self._inflight.get(key) == (generation, task):
                    self._inflight.pop(key, None)
                self._stats[f"{method}_errors"] += 1
            raise

        async with self._lock:
            if self._inflight.get(key) == (generation, task):
                self._inflight.pop(key, None)
            # A write can occur while a read is in flight. Never let that older
            # result repopulate the post-write cache, and never make a later
            # read join an in-flight request from an earlier generation.
            if ttl > 0 and generation == self._generation:
                self._cache[key] = (time.monotonic(), _copy(value))
            elif generation != self._generation:
                self._stats[f"{method}_stale_results_not_cached"] += 1
        return _copy(value)

    async def invalidate(self, *, reason: str = "exchange_write") -> None:
        async with self._lock:
            count = len(self._cache)
            self._generation += 1
            self._cache.clear()
            self._stats["invalidations"] += 1
            self._stats[f"invalidations_{str(reason or 'unknown')}"] += 1
            self._stats["invalidated_entries"] += count

    def stats(self) -> dict[str, int]:
        result = dict(self._stats)
        result.setdefault("invalidations", 0)
        result.setdefault("invalidated_entries", 0)
        result["cache_entries"] = len(self._cache)
        result["inflight"] = len(self._inflight)
        return dict(sorted((str(k), int(v)) for k, v in result.items()))

    async def close_adapters(self, adapter_cache: dict[tuple[int, str], Any]) -> None:
        async with self._lock:
            inflight = list(self._tasks)
            self._inflight.clear()
            self._cache.clear()
            self._generation += 1
        for task in inflight:
            if not task.done():
                task.cancel()
        if inflight:
            await asyncio.gather(*inflight, return_exceptions=True)

        seen: set[int] = set()
        for adapter in list(adapter_cache.values()):
            target = getattr(adapter, "_wrapped_adapter", adapter)
            ident = id(target)
            if ident in seen:
                continue
            seen.add(ident)
            close = getattr(target, "close", None)
            if callable(close):
                try:
                    result = close()
                    if asyncio.iscoroutine(result):
                        await result
                except Exception:
                    pass


class _MarketEventAdapterProxy:
    def __init__(
        self,
        adapter: Any,
        context: MarketEventExchangeContext,
        account_key: tuple[int, str],
    ) -> None:
        object.__setattr__(self, "_wrapped_adapter", adapter)
        object.__setattr__(self, "_market_event_context", context)
        object.__setattr__(self, "_market_event_account_key", (int(account_key[0]), str(account_key[1]).lower()))

    def __getattr__(self, name: str) -> Any:
        adapter = object.__getattribute__(self, "_wrapped_adapter")
        attr = getattr(adapter, name)
        if not callable(attr):
            return attr

        context = object.__getattribute__(self, "_market_event_context")
        account_key = object.__getattribute__(self, "_market_event_account_key")

        if name in _READ_METHOD_TTLS:
            async def cached(*args: Any, **kwargs: Any) -> Any:
                async def loader() -> Any:
                    result = attr(*args, **kwargs)
                    if asyncio.iscoroutine(result):
                        return await result
                    return result

                return await context._cached_read(
                    account_key=account_key,
                    method=name,
                    args=tuple(args),
                    kwargs=dict(kwargs),
                    loader=loader,
                )

            return cached

        if name.startswith(_WRITE_PREFIXES):
            async def write(*args: Any, **kwargs: Any) -> Any:
                await context.invalidate(reason=f"before_{name}")
                try:
                    result = attr(*args, **kwargs)
                    if asyncio.iscoroutine(result):
                        return await result
                    return result
                finally:
                    await context.invalidate(reason=f"after_{name}")

            return write

        return attr
