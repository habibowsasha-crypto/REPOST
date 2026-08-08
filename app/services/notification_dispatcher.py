"""Bounded Telegram delivery queue independent from BingX trade workers."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Awaitable, Callable

from app.config import get_settings
from app.services.telegram_delivery import (
    DeliveryOutcome,
    probe_private_chat,
    send_private_message,
)

log = logging.getLogger(__name__)

NotificationFactory = Callable[[], Awaitable[DeliveryOutcome]]


@dataclass
class _NotificationJob:
    factory: NotificationFactory
    future: asyncio.Future[DeliveryOutcome]
    user_id: int
    label: str
    enqueued_at: float


class TelegramNotificationDispatcher:
    def __init__(self) -> None:
        settings = get_settings()
        self.worker_count = max(1, int(settings.TELEGRAM_NOTIFICATION_WORKERS))
        self.max_wait_seconds = max(
            1.0, float(settings.TELEGRAM_NOTIFICATION_MAX_WAIT_SECONDS)
        )
        self._queue: asyncio.Queue[_NotificationJob] = asyncio.Queue(
            maxsize=max(1, int(settings.TELEGRAM_NOTIFICATION_QUEUE_MAX_SIZE))
        )
        self._workers: list[asyncio.Task[None]] = []
        self._start_lock = asyncio.Lock()
        self._state_lock = asyncio.Lock()
        self._stopping = False

    async def start(self) -> None:
        if self._workers or self._stopping:
            return
        async with self._start_lock:
            async with self._state_lock:
                if self._workers or self._stopping:
                    return
                for worker_id in range(1, self.worker_count + 1):
                    self._workers.append(
                        asyncio.create_task(
                            self._worker(worker_id),
                            name=f"telegram-dispatcher:{worker_id}",
                        )
                    )
                log.info(
                    "Telegram notification dispatcher started workers=%s queue_max=%s",
                    self.worker_count,
                    self._queue.maxsize,
                )

    async def submit(
        self,
        factory: NotificationFactory,
        *,
        user_id: int,
        label: str,
    ) -> DeliveryOutcome:
        await self.start()
        loop = asyncio.get_running_loop()
        future: asyncio.Future[DeliveryOutcome] = loop.create_future()
        job = _NotificationJob(
            factory=factory,
            future=future,
            user_id=int(user_id),
            label=str(label or "notification"),
            enqueued_at=time.monotonic(),
        )
        # Keep admission atomic with stop() queue draining so no notification
        # future can be enqueued after shutdown and remain unresolved.
        async with self._state_lock:
            if self._stopping:
                return DeliveryOutcome(False, "notification_dispatcher_stopping", "", 0)
            try:
                self._queue.put_nowait(job)
            except asyncio.QueueFull:
                log.error(
                    "Telegram notification queue full uid=%s label=%s depth=%s",
                    job.user_id,
                    job.label,
                    self._queue.qsize(),
                )
                return DeliveryOutcome(
                    False,
                    "notification_queue_full",
                    "Telegram notification queue is full",
                    0,
                )
        return await asyncio.shield(future)

    async def _worker(self, worker_id: int) -> None:
        while True:
            try:
                job = await self._queue.get()
            except asyncio.CancelledError:
                return
            try:
                if job.future.done():
                    continue
                wait = time.monotonic() - job.enqueued_at
                if wait > self.max_wait_seconds:
                    outcome = DeliveryOutcome(
                        False,
                        "notification_queue_stale",
                        f"waited {wait:.1f}s",
                        0,
                    )
                else:
                    try:
                        outcome = await job.factory()
                    except Exception as exc:
                        log.exception(
                            "Telegram notification worker failure uid=%s label=%s",
                            job.user_id,
                            job.label,
                        )
                        outcome = DeliveryOutcome(
                            False,
                            "notification_worker_error",
                            f"{type(exc).__name__}: {exc}",
                            0,
                        )
                if not job.future.done():
                    job.future.set_result(outcome)
                if wait >= 1.0:
                    log.warning(
                        "Telegram queue wait uid=%s label=%s wait_ms=%s worker=%s",
                        job.user_id,
                        job.label,
                        int(wait * 1000),
                        worker_id,
                    )
            except asyncio.CancelledError:
                if not job.future.done():
                    job.future.set_result(
                        DeliveryOutcome(
                            False, "notification_dispatcher_stopping", "", 0
                        )
                    )
                raise
            finally:
                self._queue.task_done()

    def stats(self) -> dict[str, int]:
        return {
            "workers": self.worker_count,
            "queued": self._queue.qsize(),
            "queue_max": self._queue.maxsize,
            "stopping": int(self._stopping),
        }

    async def stop(self) -> None:
        async with self._state_lock:
            self._stopping = True
            while True:
                try:
                    job = self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                try:
                    if not job.future.done():
                        job.future.set_result(
                            DeliveryOutcome(
                                False, "notification_dispatcher_stopping", "", 0
                            )
                        )
                finally:
                    self._queue.task_done()

        workers = list(self._workers)
        self._workers.clear()
        for task in workers:
            task.cancel()
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)
        log.info("Telegram notification dispatcher stopped")


_DISPATCHER: TelegramNotificationDispatcher | None = None


def notification_dispatcher_stats() -> dict[str, int]:
    dispatcher = _DISPATCHER
    if dispatcher is None:
        return {"workers": 0, "queued": 0, "queue_max": 0, "stopping": 0}
    return dispatcher.stats()


def get_notification_dispatcher() -> TelegramNotificationDispatcher:
    global _DISPATCHER
    if _DISPATCHER is None:
        _DISPATCHER = TelegramNotificationDispatcher()
    return _DISPATCHER


async def send_queued_private_message(
    bot,
    user_id: int,
    text: str,
    *,
    parse_mode: str | None = "HTML",
    attempts: int = 3,
    log_context: str = "notification",
    reply_markup=None,
) -> DeliveryOutcome:
    dispatcher = get_notification_dispatcher()
    return await dispatcher.submit(
        lambda: send_private_message(
            bot,
            user_id,
            text,
            parse_mode=parse_mode,
            attempts=attempts,
            log_context=log_context,
            reply_markup=reply_markup,
        ),
        user_id=user_id,
        label=log_context,
    )


async def probe_queued_private_chat(bot, user_id: int) -> DeliveryOutcome:
    dispatcher = get_notification_dispatcher()
    return await dispatcher.submit(
        lambda: probe_private_chat(bot, user_id),
        user_id=user_id,
        label="private chat probe",
    )


async def start_notification_dispatcher() -> None:
    await get_notification_dispatcher().start()


async def stop_notification_dispatcher() -> None:
    global _DISPATCHER
    dispatcher = _DISPATCHER
    _DISPATCHER = None
    if dispatcher is not None:
        await dispatcher.stop()
