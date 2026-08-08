"""Process-wide BingX workload controls.

The bot can serve many accounts from one Railway process.  This module keeps
private/public HTTP load bounded across *all* signals and monitor services,
prioritises protection writes over new entries, and serialises mutating requests
for one API account.

It intentionally remains process-local.  Production must still use one Railway
replica because Telegram polling and in-memory queues are single-process.
"""

from __future__ import annotations

import asyncio
import contextvars
import hashlib
import heapq
import itertools
import logging
import time
from collections import OrderedDict
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator, Iterator

from app.config import get_settings
from app.services.execution_metrics import record_workload_request

log = logging.getLogger(__name__)

# Lower number = higher priority.
PRIORITY_EMERGENCY = 0
PRIORITY_STOP = 5
PRIORITY_CANCEL = 10
PRIORITY_MONITOR = 20
PRIORITY_TP = 30
PRIORITY_ENTRY = 50
# Deferred fee/PnL history reads must never overtake any trading request.
PRIORITY_FINANCIAL = 70

_request_priority: contextvars.ContextVar[int] = contextvars.ContextVar(
    "bingx_request_priority", default=PRIORITY_MONITOR
)
_request_label: contextvars.ContextVar[str] = contextvars.ContextVar(
    "bingx_request_label", default="background"
)
_request_user_id: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "bingx_request_user_id", default=None
)


class BingxWorkloadTimeout(RuntimeError):
    """A request could not enter the bounded BingX workload window in time."""


@contextmanager
def bingx_request_context(
    *,
    priority: int,
    label: str,
    user_id: int | None = None,
) -> Iterator[None]:
    """Set request priority for all adapter calls in the current task."""

    p_token = _request_priority.set(int(priority))
    l_token = _request_label.set(str(label or "operation"))
    resolved_user_id = _request_user_id.get() if user_id is None else int(user_id)
    u_token = _request_user_id.set(resolved_user_id)
    try:
        yield
    finally:
        _request_user_id.reset(u_token)
        _request_label.reset(l_token)
        _request_priority.reset(p_token)


@dataclass
class _GateWaiter:
    priority: int
    sequence: int
    future: asyncio.Future[None]


class _PriorityGate:
    """Fair priority-aware concurrency gate with bounded waiting."""

    def __init__(self, limit: int) -> None:
        self.limit = max(1, int(limit))
        self._active = 0
        self._lock = asyncio.Lock()
        self._waiters: list[tuple[int, int, asyncio.Future[None]]] = []
        self._sequence = itertools.count()
        self.peak_active = 0

    def _wake_locked(self) -> None:
        while self._active < self.limit and self._waiters:
            _, _, future = heapq.heappop(self._waiters)
            if future.cancelled() or future.done():
                continue
            self._active += 1
            self.peak_active = max(self.peak_active, self._active)
            future.set_result(None)

    async def acquire(self, priority: int, timeout: float) -> float:
        started = time.monotonic()
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        async with self._lock:
            heapq.heappush(
                self._waiters,
                (int(priority), next(self._sequence), future),
            )
            self._wake_locked()
        try:
            await asyncio.wait_for(asyncio.shield(future), timeout=max(0.1, timeout))
        except BaseException:
            # A cancellation can arrive just after _wake_locked granted the
            # permit. In that race the future is already done and the active
            # counter must be released here, otherwise one global HTTP slot is
            # leaked forever.
            granted = future.done() and not future.cancelled()
            async with self._lock:
                if granted:
                    self._active = max(0, self._active - 1)
                elif not future.done():
                    future.cancel()
                self._wake_locked()
            raise
        return max(0.0, time.monotonic() - started)

    async def release(self) -> None:
        async with self._lock:
            self._active = max(0, self._active - 1)
            self._wake_locked()

    @property
    def active(self) -> int:
        return self._active

    @property
    def waiting(self) -> int:
        return sum(1 for _, _, f in self._waiters if not f.done())


class _CompositePriorityGate:
    """Atomically reserves one global slot and, for writes, one account slot.

    A waiter is granted only when both resources are available. This prevents a
    low-priority TP/ENTRY from occupying a global slot while merely waiting for
    another request of the same account, and lets STOP/EMERGENCY jump ahead of
    not-yet-started lower-priority writes.
    """

    def __init__(self, limit: int) -> None:
        self.limit = max(1, int(limit))
        self._active = 0
        self._active_accounts: set[str] = set()
        self._lock = asyncio.Lock()
        self._waiters: list[tuple[int, int, str | None, asyncio.Future[None]]] = []
        self._sequence = itertools.count()
        self.peak_active = 0

    def _wake_locked(self) -> None:
        while self._active < self.limit and self._waiters:
            deferred: list[tuple[int, int, str | None, asyncio.Future[None]]] = []
            granted = False
            while self._waiters:
                item = heapq.heappop(self._waiters)
                _, _, account_key, future = item
                if future.cancelled() or future.done():
                    continue
                if account_key is not None and account_key in self._active_accounts:
                    deferred.append(item)
                    continue
                self._active += 1
                if account_key is not None:
                    self._active_accounts.add(account_key)
                self.peak_active = max(self.peak_active, self._active)
                future.set_result(None)
                granted = True
                break
            for item in deferred:
                heapq.heappush(self._waiters, item)
            if not granted:
                break

    async def acquire(
        self, priority: int, account_key: str | None, timeout: float
    ) -> float:
        started = time.monotonic()
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        async with self._lock:
            heapq.heappush(
                self._waiters,
                (int(priority), next(self._sequence), account_key, future),
            )
            self._wake_locked()
        try:
            await asyncio.wait_for(asyncio.shield(future), timeout=max(0.05, timeout))
        except BaseException:
            granted = future.done() and not future.cancelled()
            async with self._lock:
                if granted:
                    self._active = max(0, self._active - 1)
                    if account_key is not None:
                        self._active_accounts.discard(account_key)
                elif not future.done():
                    future.cancel()
                self._wake_locked()
            raise
        return max(0.0, time.monotonic() - started)

    async def release(self, account_key: str | None) -> None:
        async with self._lock:
            self._active = max(0, self._active - 1)
            if account_key is not None:
                self._active_accounts.discard(account_key)
            self._wake_locked()

    @property
    def active(self) -> int:
        return self._active

    @property
    def waiting(self) -> int:
        return sum(1 for _, _, _, f in self._waiters if not f.done())


class _TokenBucket:
    """Small async token bucket used for global and per-account pacing."""

    def __init__(self, rate: float, capacity: float) -> None:
        self.rate = max(0.0, float(rate))
        self.capacity = max(1.0, float(capacity))
        self._tokens = self.capacity
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, amount: float = 1.0, timeout: float | None = None) -> float:
        if self.rate <= 0:
            return 0.0
        deadline = None if timeout is None else time.monotonic() + max(0.0, timeout)
        requested = max(0.0001, float(amount))
        waited = 0.0
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = max(0.0, now - self._updated)
                self._tokens = min(
                    self.capacity,
                    self._tokens + elapsed * self.rate,
                )
                self._updated = now
                if self._tokens >= requested:
                    self._tokens -= requested
                    return waited
                delay = (requested - self._tokens) / self.rate
            delay = max(0.001, min(delay, 1.0))
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError
                delay = min(delay, remaining)
            before = time.monotonic()
            await asyncio.sleep(delay)
            waited += time.monotonic() - before


@dataclass
class _RateState:
    tokens: float
    updated: float


class _PriorityWorkloadGate:
    """Atomically grants priority, pacing, global concurrency and write locks.

    The previous governor consumed global/per-account rate tokens before a
    request entered the priority gate.  Under saturation, queued ENTRY tasks
    could therefore drain the pacing budget before a later STOP arrived.  This
    scheduler keeps every not-yet-started request in one heap and deducts rate
    tokens only when the same request also receives its global/account slot.

    Requests with a lower numeric priority always win over lower-importance
    work.  Within the best priority class, independent accounts may proceed in
    parallel when resources are available.  Lower classes never jump ahead of
    an already waiting STOP/EMERGENCY request.
    """

    def __init__(
        self,
        *,
        limit: int,
        global_rate: float,
        global_capacity: float,
        account_rate: float,
        account_capacity: float,
        max_accounts: int = 5000,
    ) -> None:
        self.limit = max(1, int(limit))
        self.global_rate = max(0.0, float(global_rate))
        self.global_capacity = max(1.0, float(global_capacity))
        self.account_rate = max(0.0, float(account_rate))
        self.account_capacity = max(1.0, float(account_capacity))
        now = time.monotonic()
        self._global = _RateState(self.global_capacity, now)
        self._accounts: OrderedDict[str, _RateState] = OrderedDict()
        self._max_accounts = max(1, int(max_accounts))
        self._active = 0
        self._active_write_accounts: set[str] = set()
        self._lock = asyncio.Lock()
        self._waiters: list[
            tuple[
                int,
                int,
                str | None,
                str | None,
                float,
                asyncio.Future[dict[str, float]],
            ]
        ] = []
        self._sequence = itertools.count()
        self._wake_task: asyncio.Task[None] | None = None
        self.peak_active = 0

    @staticmethod
    def _refill(state: _RateState, *, rate: float, capacity: float, now: float) -> None:
        elapsed = max(0.0, now - state.updated)
        state.tokens = min(capacity, state.tokens + elapsed * rate)
        state.updated = now

    def _account_state_locked(self, key: str, now: float) -> _RateState:
        state = self._accounts.get(key)
        if state is None:
            state = _RateState(self.account_capacity, now)
            self._accounts[key] = state
        else:
            self._accounts.move_to_end(key)
        while len(self._accounts) > self._max_accounts:
            self._accounts.popitem(last=False)
        return state

    def _rate_delay_locked(self, rate_account_key: str | None, now: float) -> float:
        self._refill(
            self._global,
            rate=self.global_rate,
            capacity=self.global_capacity,
            now=now,
        )
        delays: list[float] = []
        if self.global_rate > 0 and self._global.tokens < 1.0:
            delays.append((1.0 - self._global.tokens) / self.global_rate)
        if rate_account_key is not None:
            state = self._account_state_locked(rate_account_key, now)
            self._refill(
                state,
                rate=self.account_rate,
                capacity=self.account_capacity,
                now=now,
            )
            if self.account_rate > 0 and state.tokens < 1.0:
                delays.append((1.0 - state.tokens) / self.account_rate)
        return max(delays, default=0.0)

    def _has_rate_locked(self, rate_account_key: str | None, now: float) -> bool:
        return self._rate_delay_locked(rate_account_key, now) <= 1e-9

    def _consume_rate_locked(self, rate_account_key: str | None, now: float) -> None:
        self._refill(
            self._global,
            rate=self.global_rate,
            capacity=self.global_capacity,
            now=now,
        )
        if self.global_rate > 0:
            self._global.tokens = max(0.0, self._global.tokens - 1.0)
        if rate_account_key is not None:
            state = self._account_state_locked(rate_account_key, now)
            self._refill(
                state,
                rate=self.account_rate,
                capacity=self.account_capacity,
                now=now,
            )
            if self.account_rate > 0:
                state.tokens = max(0.0, state.tokens - 1.0)

    def _cancel_wake_locked(self) -> None:
        task = self._wake_task
        self._wake_task = None
        if task is not None and not task.done():
            task.cancel()

    def _schedule_wake_locked(self, delay: float) -> None:
        self._cancel_wake_locked()
        if not self._waiters:
            return
        self._wake_task = asyncio.create_task(
            self._wake_after(max(0.001, float(delay))),
            name="bingx-priority-rate-wake",
        )

    async def _wake_after(self, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
            async with self._lock:
                self._wake_task = None
                self._wake_locked()
        except asyncio.CancelledError:
            return

    def _wake_locked(self) -> None:
        self._cancel_wake_locked()
        while self._active < self.limit and self._waiters:
            # Discard completed/cancelled heap heads first.
            while self._waiters and self._waiters[0][5].done():
                heapq.heappop(self._waiters)
            if not self._waiters:
                return

            best_priority = self._waiters[0][0]
            deferred: list[
                tuple[
                    int,
                    int,
                    str | None,
                    str | None,
                    float,
                    asyncio.Future[dict[str, float]],
                ]
            ] = []
            grant_item = None
            minimum_rate_delay: float | None = None
            now = time.monotonic()

            # Only peers of the current highest priority may be considered.
            while self._waiters and self._waiters[0][0] == best_priority:
                item = heapq.heappop(self._waiters)
                _, _, rate_key, write_key, _, future = item
                if future.done():
                    continue
                if write_key is not None and write_key in self._active_write_accounts:
                    deferred.append(item)
                    continue
                delay = self._rate_delay_locked(rate_key, now)
                if delay > 1e-9:
                    minimum_rate_delay = (
                        delay
                        if minimum_rate_delay is None
                        else min(minimum_rate_delay, delay)
                    )
                    deferred.append(item)
                    continue
                grant_item = item
                break

            for item in deferred:
                heapq.heappush(self._waiters, item)

            if grant_item is None:
                # The highest-priority class is blocked.  Never let lower work
                # consume tokens ahead of it.  A write-account release or the
                # scheduled token refill will call us again.
                if minimum_rate_delay is not None:
                    self._schedule_wake_locked(minimum_rate_delay)
                return

            priority, _, rate_key, write_key, enqueued_at, future = grant_item
            self._consume_rate_locked(rate_key, now)
            self._active += 1
            if write_key is not None:
                self._active_write_accounts.add(write_key)
            self.peak_active = max(self.peak_active, self._active)
            total_wait = max(0.0, now - enqueued_at)
            future.set_result(
                {
                    "priority": float(priority),
                    "wait": total_wait,
                    "gate_wait": total_wait,
                    "global_rate_wait": total_wait,
                    "account_rate_wait": total_wait if rate_key else 0.0,
                }
            )

    async def acquire(
        self,
        *,
        priority: int,
        rate_account_key: str | None,
        write_account_key: str | None,
        timeout: float,
    ) -> dict[str, float]:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, float]] = loop.create_future()
        item = (
            int(priority),
            next(self._sequence),
            rate_account_key,
            write_account_key,
            time.monotonic(),
            future,
        )
        async with self._lock:
            heapq.heappush(self._waiters, item)
            self._wake_locked()
        try:
            return await asyncio.wait_for(
                asyncio.shield(future), timeout=max(0.05, float(timeout))
            )
        except BaseException:
            granted = future.done() and not future.cancelled()
            async with self._lock:
                if granted:
                    self._active = max(0, self._active - 1)
                    if write_account_key is not None:
                        self._active_write_accounts.discard(write_account_key)
                elif not future.done():
                    future.cancel()
                self._wake_locked()
            raise

    async def release(self, write_account_key: str | None) -> None:
        async with self._lock:
            self._active = max(0, self._active - 1)
            if write_account_key is not None:
                self._active_write_accounts.discard(write_account_key)
            self._wake_locked()

    @property
    def active(self) -> int:
        return self._active

    @property
    def waiting(self) -> int:
        return sum(1 for item in self._waiters if not item[5].done())


class _BingxRequestGovernor:
    def __init__(self) -> None:
        settings = get_settings()
        self._gate = _PriorityWorkloadGate(
            limit=settings.BINGX_GLOBAL_MAX_IN_FLIGHT,
            global_rate=settings.BINGX_GLOBAL_REQUESTS_PER_SECOND,
            global_capacity=settings.BINGX_GLOBAL_BURST_LIMIT,
            account_rate=settings.BINGX_PER_USER_REQUESTS_PER_SECOND,
            account_capacity=settings.BINGX_PER_USER_BURST_LIMIT,
        )
        # v1.6.60: /swap/v2/trade/order is shared by entry, STOP, TP and
        # market close writes.  Keep it below BingX's tighter trade-order
        # ceiling even when public reads/history are allowed to run faster.
        self._trade_order_bucket = _TokenBucket(
            rate=float(settings.BINGX_TRADE_ORDER_REQUESTS_PER_SECOND),
            capacity=float(settings.BINGX_TRADE_ORDER_BURST_LIMIT),
        )
        self._queue_timeout = float(settings.BINGX_REQUEST_QUEUE_TIMEOUT_SECONDS)

    @staticmethod
    def _account_key(api_key: str) -> str:
        # Never keep/log a raw API key as a dictionary key.
        return hashlib.sha256(str(api_key or "").encode("utf-8")).hexdigest()[:24]

    @staticmethod
    def _effective_priority(
        method: str,
        path: str,
        body: dict[str, Any] | None,
    ) -> int:
        priority = int(_request_priority.get())
        method_up = str(method or "").upper()
        path_l = str(path or "").lower()
        payload = body or {}
        label = _request_label.get().lower()

        # Explicit contexts always win.  Some legacy services still use labels
        # rather than endpoint metadata.
        if "emergency" in label or "panic" in label or "manual-close" in label:
            return PRIORITY_EMERGENCY
        if "stop" in label or "sl" in label:
            return min(priority, PRIORITY_STOP)
        if "cancel" in label:
            return min(priority, PRIORITY_CANCEL)

        if method_up == "DELETE" or "cancel" in path_l:
            return min(priority, PRIORITY_CANCEL)

        # BingX USDT-M uses one POST /openApi/swap/v2/trade/order endpoint for
        # entries, STOP, TP and market closes.  Classify by type and by whether
        # the side is closing the hedge positionSide.
        if method_up == "POST" and "/swap/v2/trade/order" in path_l:
            typ = str(payload.get("type") or "").upper()
            side = str(payload.get("side") or "").upper()
            pos_side = str(payload.get("positionSide") or "").upper()
            if typ == "STOP_MARKET":
                return min(priority, PRIORITY_STOP)
            if typ == "TAKE_PROFIT_MARKET":
                return priority if priority < PRIORITY_MONITOR else PRIORITY_TP
            close_side = (pos_side == "LONG" and side == "SELL") or (
                pos_side == "SHORT" and side == "BUY"
            )
            if close_side and typ in {"MARKET", "LIMIT"}:
                return min(priority, PRIORITY_EMERGENCY)
            if typ in {"MARKET", "LIMIT"}:
                return priority if priority < PRIORITY_MONITOR else PRIORITY_ENTRY

        # Legacy MEXC endpoint classifiers kept for compatibility aliases.
        side_code = str(payload.get("side") or "")
        if method_up == "POST" and "/order/create" in path_l and side_code in {"2", "4"}:
            return PRIORITY_EMERGENCY
        if "/stoporder/place" in path_l:
            if payload.get("stopLossPrice") not in (None, "", 0, "0"):
                return min(priority, PRIORITY_STOP)
            if payload.get("takeProfitPrice") not in (None, "", 0, "0"):
                return min(priority, PRIORITY_TP)

        return priority

    def stats(self) -> dict[str, int]:
        return {
            "active": int(self._gate.active),
            "queued": int(self._gate.waiting),
            "peak_active": int(self._gate.peak_active),
            "limit": int(self._gate.limit),
        }

    @asynccontextmanager
    async def permit(
        self,
        *,
        api_key: str,
        auth: bool,
        method: str,
        path: str,
        body: dict[str, Any] | None,
    ) -> AsyncIterator[dict[str, Any]]:
        priority = self._effective_priority(method, path, body)
        started = time.monotonic()
        deadline = started + self._queue_timeout

        def remaining() -> float:
            left = deadline - time.monotonic()
            if left <= 0:
                raise TimeoutError
            return left

        method_up = str(method or "").upper()
        is_write = bool(auth and api_key and method_up in {"POST", "DELETE"})
        rate_account_key = self._account_key(api_key) if auth and api_key else None
        write_account_key = self._account_key(api_key) if is_write else None
        global_rate_wait = 0.0
        account_rate_wait = 0.0
        gate_wait = 0.0
        granted = False
        try:
            grant = await self._gate.acquire(
                priority=priority,
                rate_account_key=rate_account_key,
                write_account_key=write_account_key,
                timeout=remaining(),
            )
            gate_wait = float(grant.get("gate_wait") or 0.0)
            global_rate_wait = float(grant.get("global_rate_wait") or 0.0)
            account_rate_wait = float(grant.get("account_rate_wait") or 0.0)
            granted = True

            trade_order_wait = 0.0
            if method_up == "POST" and "/swap/v2/trade/order" in str(path or "").lower():
                trade_order_wait = await self._trade_order_bucket.acquire(timeout=remaining())

            total_wait = max(0.0, time.monotonic() - started)
            if total_wait >= 1.0:
                log.warning(
                    "BingX workload wait priority=%s label=%s method=%s path=%s "
                    "wait_ms=%s active=%s queued=%s",
                    priority,
                    _request_label.get(),
                    method_up,
                    path,
                    int(total_wait * 1000),
                    self._gate.active,
                    self._gate.waiting,
                )
            metrics = {
                "priority": priority,
                "wait_ms": int(total_wait * 1000),
                "gate_wait_ms": int(gate_wait * 1000),
                "global_rate_wait_ms": int(global_rate_wait * 1000),
                "account_rate_wait_ms": int(account_rate_wait * 1000),
                "trade_order_wait_ms": int(trade_order_wait * 1000),
                "active": self._gate.active,
                "queued": self._gate.waiting,
                "user_id": _request_user_id.get(),
            }
            record_workload_request(metrics)
            yield metrics
        except TimeoutError as exc:
            raise BingxWorkloadTimeout(
                f"BingX request queue timeout after {self._queue_timeout:.1f}s "
                f"for {method_up} {path}"
            ) from exc
        finally:
            if granted:
                await self._gate.release(write_account_key)


_GOVERNOR: _BingxRequestGovernor | None = None
_GOVERNOR_LOCK = asyncio.Lock()


async def _get_governor() -> _BingxRequestGovernor:
    global _GOVERNOR
    if _GOVERNOR is not None:
        return _GOVERNOR
    async with _GOVERNOR_LOCK:
        if _GOVERNOR is None:
            _GOVERNOR = _BingxRequestGovernor()
    return _GOVERNOR


@asynccontextmanager
async def govern_bingx_request(
    *,
    api_key: str,
    auth: bool,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
) -> AsyncIterator[dict[str, Any]]:
    governor = await _get_governor()
    async with governor.permit(
        api_key=api_key,
        auth=bool(auth),
        method=method,
        path=path,
        body=body,
    ) as metrics:
        yield metrics


async def bingx_workload_stats() -> dict[str, int]:
    """Return process-local BingX governor depth for diagnostics."""

    governor = await _get_governor()
    return governor.stats()


async def reset_workload_manager_for_tests() -> None:
    """Reset process-local state; intended only for isolated regression tests."""

    global _GOVERNOR
    async with _GOVERNOR_LOCK:
        _GOVERNOR = None


# Backward-compatible aliases used by older core modules. In this BingX-only
# build they route to the BingX governor.
mexc_request_context = bingx_request_context
govern_mexc_request = govern_bingx_request
MexcWorkloadTimeout = BingxWorkloadTimeout
