"""One process-wide bounded dispatcher for all new trade entries."""

from __future__ import annotations

import asyncio
import itertools
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from app.config import get_settings
from app.services.models import ExecutionResult
from app.services.trade_notification_policy import mandatory_trade_warning_payload
from app.services.execution_metrics import record_trade_result
from app.services.workload_manager import PRIORITY_ENTRY, mexc_request_context

log = logging.getLogger(__name__)

DispatchFactory = Callable[[dict[str, Any]], Awaitable[ExecutionResult]]


@dataclass(order=True)
class _QueuedTrade:
    priority: int
    sequence: int
    factory: DispatchFactory = field(compare=False)
    future: asyncio.Future[ExecutionResult] = field(compare=False)
    user_id: int = field(compare=False)
    symbol: str = field(compare=False)
    enqueued_at: float = field(compare=False)
    signal_received_at: float = field(compare=False)
    queue_depth_at_submit: int = field(compare=False)
    max_wait_seconds: float = field(compare=False)


class TradeExecutionDispatcher:
    def __init__(self) -> None:
        settings = get_settings()
        self.worker_count = max(1, int(settings.TRADE_EXECUTION_WORKERS))
        self.max_queue_size = max(1, int(settings.TRADE_QUEUE_MAX_SIZE))
        self.market_max_wait_seconds = max(
            0.5, float(settings.MARKET_QUEUE_MAX_WAIT_SECONDS)
        )
        self.limit_max_wait_seconds = max(
            0.5, float(settings.LIMIT_QUEUE_MAX_WAIT_SECONDS)
        )
        self.shutdown_timeout_seconds = max(
            5.0, float(settings.TRADE_DISPATCHER_SHUTDOWN_TIMEOUT_SECONDS)
        )
        self._queue: asyncio.PriorityQueue[_QueuedTrade] = asyncio.PriorityQueue(
            maxsize=self.max_queue_size
        )
        self._workers: list[asyncio.Task[None]] = []
        self._sequence = itertools.count()
        self._start_lock = asyncio.Lock()
        self._state_lock = asyncio.Lock()
        self._stopping = False
        self._active = 0
        self._processing = 0
        self._peak_active = 0
        self._active_lock = asyncio.Lock()

    async def start(self) -> None:
        if self._workers or self._stopping:
            return
        async with self._start_lock:
            async with self._state_lock:
                if self._workers or self._stopping:
                    return
                for worker_id in range(1, self.worker_count + 1):
                    task = asyncio.create_task(
                        self._worker(worker_id),
                        name=f"trade-dispatcher:{worker_id}",
                    )
                    self._workers.append(task)
                log.info(
                    "Global trade dispatcher started workers=%s queue_max=%s wait_max=%.1fs",
                    self.worker_count,
                    self.max_queue_size,
                    self.market_max_wait_seconds,
                )

    async def submit(
        self,
        factory: DispatchFactory,
        *,
        user_id: int,
        symbol: str,
        signal_received_at: float | None = None,
        priority: int = PRIORITY_ENTRY,
        entry_type: str = "MARKET",
    ) -> ExecutionResult:
        await self.start()
        now = time.monotonic()
        received = float(signal_received_at or now)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[ExecutionResult] = loop.create_future()
        depth = self._queue.qsize()
        entry_type_up = str(entry_type or "MARKET").upper()
        max_wait_seconds = (
            self.limit_max_wait_seconds
            if entry_type_up == "LIMIT"
            else self.market_max_wait_seconds
        )
        item = _QueuedTrade(
            priority=int(priority),
            sequence=next(self._sequence),
            factory=factory,
            future=future,
            user_id=int(user_id),
            symbol=str(symbol).upper(),
            enqueued_at=now,
            signal_received_at=received,
            queue_depth_at_submit=depth,
            max_wait_seconds=max_wait_seconds,
        )
        # Admission and shutdown draining are protected by one state lock.
        # Without this, stop() could drain an empty queue between the stopping
        # check and put_nowait(), leaving a newly accepted future unresolved.
        async with self._state_lock:
            if self._stopping:
                return self._rejected(item, "dispatcher_stopping")
            try:
                self._queue.put_nowait(item)
            except asyncio.QueueFull:
                log.error(
                    "Trade queue full uid=%s symbol=%s depth=%s max=%s",
                    item.user_id,
                    item.symbol,
                    depth,
                    self.max_queue_size,
                )
                return self._rejected(item, "queue_full")

        # Do not cancel an accepted exchange job merely because the Telegram
        # message handler was cancelled. The dispatcher is stopped explicitly on
        # application shutdown; once a worker starts, the executor must finish
        # its safety/rollback path.
        return await asyncio.shield(future)

    def _rejected(self, item: _QueuedTrade, reason: str) -> ExecutionResult:
        reason_text = {
            "queue_full": "глобальная очередь сделок переполнена; вход безопасно пропущен",
            "queue_stale": "сигнал устарел до начала исполнения; вход безопасно пропущен",
            "dispatcher_stopping": "бот завершает работу; новый вход не принят",
        }.get(reason, reason)
        return ExecutionResult(
            item.user_id,
            "skipped",
            reason_text,
            mandatory_trade_warning_payload(
                reason,
                {
                    "dispatch": {
                        "rejected": reason,
                        "queue_depth_at_submit": item.queue_depth_at_submit,
                        "global_worker_limit": self.worker_count,
                    },
                    reason: True,
                },
            ),
        )

    async def _worker(self, worker_id: int) -> None:
        while True:
            try:
                item = await self._queue.get()
            except asyncio.CancelledError:
                return
            self._processing += 1
            try:
                if item.future.cancelled() or item.future.done():
                    continue
                started = time.monotonic()
                queue_wait = max(0.0, started - item.enqueued_at)
                signal_age = max(0.0, started - item.signal_received_at)
                # Expiry is based on total age from validated signal receipt,
                # not only time inside this queue. Telegram DM probing and batch
                # preparation can also make a MARKET signal stale before submit.
                if signal_age > item.max_wait_seconds:
                    result = self._rejected(item, "queue_stale")
                    result.payload["dispatch"].update(
                        {
                            "worker_id": worker_id,
                            "queue_wait_ms": int(queue_wait * 1000),
                            "signal_age_at_start_ms": int(signal_age * 1000),
                            "queue_max_wait_ms": int(item.max_wait_seconds * 1000),
                        }
                    )
                    if not item.future.done():
                        item.future.set_result(result)
                    continue

                async with self._active_lock:
                    self._active += 1
                    self._peak_active = max(self._peak_active, self._active)
                    active_at_start = self._active
                    peak = self._peak_active

                dispatch_context = {
                    "worker_id": worker_id,
                    "queue_wait_ms": int(queue_wait * 1000),
                    "signal_age_at_start_ms": int(signal_age * 1000),
                    "queue_depth_at_submit": item.queue_depth_at_submit,
                    "global_worker_limit": self.worker_count,
                    "active_at_start": active_at_start,
                    "peak_active": peak,
                }
                exec_started = time.monotonic()
                try:
                    with mexc_request_context(
                        priority=PRIORITY_ENTRY,
                        label="new_entry",
                        user_id=item.user_id,
                    ):
                        result = await item.factory(dict(dispatch_context))
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.exception(
                        "Global trade worker failed uid=%s symbol=%s",
                        item.user_id,
                        item.symbol,
                    )
                    result = ExecutionResult(
                        item.user_id,
                        "error",
                        f"internal dispatcher error: {type(exc).__name__}: {exc}",
                    )
                finally:
                    async with self._active_lock:
                        self._active = max(0, self._active - 1)

                finished = time.monotonic()
                metrics = dict(result.payload.get("dispatch") or {})
                metrics.update(dispatch_context)
                metrics.update(
                    {
                        "execution_ms": int((finished - exec_started) * 1000),
                        "signal_to_result_ms": int(
                            max(0.0, finished - item.signal_received_at) * 1000
                        ),
                        "queue_depth_after": self._queue.qsize(),
                    }
                )
                result.payload["dispatch"] = metrics
                record_trade_result(status=str(result.status), payload=result.payload)
                log_method = (
                    log.warning if metrics["signal_to_result_ms"] >= 15000 else log.info
                )
                log_method(
                    "trade dispatch uid=%s symbol=%s status=%s queue_ms=%s exec_ms=%s total_ms=%s worker=%s",
                    item.user_id,
                    item.symbol,
                    result.status,
                    metrics["queue_wait_ms"],
                    metrics["execution_ms"],
                    metrics["signal_to_result_ms"],
                    worker_id,
                )
                if not item.future.done():
                    item.future.set_result(result)
            except asyncio.CancelledError:
                if not item.future.done():
                    item.future.set_result(self._rejected(item, "dispatcher_stopping"))
                raise
            finally:
                self._processing = max(0, self._processing - 1)
                self._queue.task_done()

    async def stop(self) -> None:
        # Atomically close admission and drain jobs that have not started.
        # Queued entries are deliberately not replayed after a redeploy because
        # their market context may be stale.
        async with self._state_lock:
            self._stopping = True
            while True:
                try:
                    item = self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                try:
                    if not item.future.done():
                        item.future.set_result(
                            self._rejected(item, "dispatcher_stopping")
                        )
                finally:
                    self._queue.task_done()

        # Let already-started executions finish their STOP/rollback path before
        # cancelling worker loops. Abruptly cancelling an exchange write is more
        # dangerous than waiting a bounded grace period.
        deadline = time.monotonic() + self.shutdown_timeout_seconds
        while self._processing > 0 and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        if self._processing > 0:
            log.error(
                "Trade dispatcher shutdown grace expired processing=%s timeout=%.1fs",
                self._processing,
                self.shutdown_timeout_seconds,
            )

        workers = list(self._workers)
        self._workers.clear()
        for task in workers:
            task.cancel()
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)
        log.info("Global trade dispatcher stopped")

    def stats(self) -> dict[str, int]:
        return {
            "workers": self.worker_count,
            "active": self._active,
            "processing": self._processing,
            "peak_active": self._peak_active,
            "queued": self._queue.qsize(),
            "queue_max": self.max_queue_size,
        }


_DISPATCHER: TradeExecutionDispatcher | None = None


def trade_dispatcher_stats() -> dict[str, int]:
    """Return dispatcher diagnostics without instantiating runtime state."""
    dispatcher = _DISPATCHER
    if dispatcher is None:
        return {
            "workers": 0,
            "active": 0,
            "processing": 0,
            "peak_active": 0,
            "queued": 0,
            "queue_max": 0,
        }
    return dispatcher.stats()


def get_trade_dispatcher() -> TradeExecutionDispatcher:
    global _DISPATCHER
    if _DISPATCHER is None:
        _DISPATCHER = TradeExecutionDispatcher()
    return _DISPATCHER


async def start_trade_dispatcher() -> None:
    await get_trade_dispatcher().start()


async def stop_trade_dispatcher() -> None:
    global _DISPATCHER
    dispatcher = _DISPATCHER
    _DISPATCHER = None
    if dispatcher is not None:
        await dispatcher.stop()
