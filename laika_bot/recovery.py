from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Awaitable, Callable, Protocol

from .models import utcnow
from .services.telegram_accounts import ACCOUNT_AUTH_FAILURES

logger = logging.getLogger(__name__)


class RecoveryDatabase(Protocol):
    async def recover_stuck_jobs(
        self,
        *,
        stale_before: datetime,
        retry_due_at: datetime,
        max_attempts: int,
        max_rows_per_kind: int,
        exclude_join_job_ids: set[int] | None = None,
        exclude_reaction_job_ids: set[int] | None = None,
        exclude_view_job_ids: set[int] | None = None,
    ) -> dict[str, int]: ...

    async def recent_recovery_failure_count(self, *, since: datetime) -> int: ...

    async def list_accounts(self, *, active_only: bool = False) -> list[object]: ...

    async def list_problem_accounts(self) -> list[object]: ...

    async def list_channels(
        self, *, active_only: bool = False, kind: str | None = None
    ) -> list[object]: ...

    async def mark_account_healthy(self, account_id: int) -> bool: ...

    async def restore_problem_account(self, account_id: int) -> bool: ...

    async def set_account_error(
        self, account_id: int, error: str, *, flood_until: datetime | None = None
    ) -> None: ...


class RecoveryJobService(Protocol):
    def active_job_ids(self) -> dict[str, set[int]]: ...

    def worker_health_snapshot(
        self, *, now: datetime | None = None
    ) -> list[dict[str, object | None]]: ...

    def mark_worker_restarting(self, name: str, detail: str) -> None: ...

    def mark_worker_restarted(self, name: str) -> None: ...

    async def schedule_missing_channel_joins(
        self, channel: object, *, account_ids: list[int] | None = None
    ) -> dict[str, int]: ...


class RecoveryPool(Protocol):
    def lock_for(self, account_id: int) -> asyncio.Lock: ...

    def has_connected_client(self, account_id: int) -> bool: ...

    async def ensure_authorized(self, account: object) -> object: ...

    async def remove_unauthorized_account(
        self, account_id: int, *, context: str
    ) -> bool: ...

    async def remove_unauthorized_account_while_locked(
        self, account_id: int, *, context: str
    ) -> bool: ...

    async def disconnect_client(self, account_id: int) -> None: ...


class RecoveryAlerts(Protocol):
    async def notify_external_incident(
        self,
        key: str,
        *,
        title: str,
        detail: str,
        severity: str = "warning",
    ) -> bool: ...

    async def resolve_external_incident(
        self, key: str, *, title: str | None = None
    ) -> bool: ...


WorkerFactory = Callable[[], Awaitable[None]]
Clock = Callable[[], datetime]


@dataclass(slots=True)
class RecoverySnapshot:
    enabled: bool
    last_run_at: datetime | None = None
    last_success_at: datetime | None = None
    last_error_at: datetime | None = None
    last_error: str | None = None
    runs: int = 0
    stuck_requeued: int = 0
    stuck_failed: int = 0
    accounts_cleared: int = 0
    accounts_restored: int = 0
    accounts_quarantined: int = 0
    account_transient_errors: int = 0
    joins_rescheduled: int = 0
    worker_restarts: int = 0
    worker_restart_failures: int = 0

    def as_dict(self) -> dict[str, object | None]:
        return {
            field_name: getattr(self, field_name)
            for field_name in self.__dataclass_fields__
        }


@dataclass(slots=True)
class WorkerRuntime:
    public_name: str
    health_name: str
    factory: WorkerFactory
    crashes: deque[datetime] = field(default_factory=deque)
    attempt_task: asyncio.Task[None] | None = None
    restart_reason: str | None = None
    last_restart_requested_at: datetime | None = None


class AutomaticRecoveryService:
    """Conservative self-healing for queues, sessions and core workers.

    The service never requests login codes, never bypasses FloodWait, never restores
    a session unless Telethon proves it is authorized and belongs to the stored
    Telegram user id, and never retries a queue row after the configured attempt
    budget is exhausted.
    """

    def __init__(
        self,
        settings: object,
        db: RecoveryDatabase,
        jobs: RecoveryJobService,
        pool: RecoveryPool,
        alerts: RecoveryAlerts | None = None,
        *,
        clock: Clock = utcnow,
    ) -> None:
        self.settings = settings
        self.db = db
        self.jobs = jobs
        self.pool = pool
        self.alerts = alerts
        self.clock = clock
        self._snapshot = RecoverySnapshot(enabled=self.enabled)
        self._lock = asyncio.Lock()
        self._ready_account_next_check: dict[int, datetime] = {}
        self._problem_account_next_check: dict[int, datetime] = {}
        self._workers: dict[str, WorkerRuntime] = {}

    @property
    def enabled(self) -> bool:
        return bool(getattr(self.settings, "auto_recovery_enabled", True))

    @property
    def check_interval_seconds(self) -> int:
        return max(
            15, int(getattr(self.settings, "recovery_check_interval_seconds", 60))
        )

    @property
    def stuck_after_seconds(self) -> int:
        configured = max(
            60, int(getattr(self.settings, "recovery_stuck_after_seconds", 600))
        )
        attempt_timeout = max(
            int(getattr(self.settings, "reaction_attempt_timeout_seconds", 180)),
            int(getattr(self.settings, "membership_attempt_timeout_seconds", 180)),
            int(getattr(self.settings, "view_attempt_timeout_seconds", 180)),
        )
        return max(configured, attempt_timeout + 60)

    def snapshot(self) -> dict[str, object | None]:
        return self._snapshot.as_dict()

    async def _notify_incident(
        self, key: str, *, title: str, detail: str, severity: str = "warning"
    ) -> None:
        if self.alerts is None:
            return
        try:
            await self.alerts.notify_external_incident(
                key, title=title, detail=detail, severity=severity
            )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception("Automatic recovery alert failed key=%s", key)

    async def _resolve_incident(self, key: str, *, title: str | None = None) -> None:
        if self.alerts is None:
            return
        try:
            await self.alerts.resolve_external_incident(key, title=title)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception("Automatic recovery resolution failed key=%s", key)

    async def _recover_stuck_jobs(self, now: datetime) -> dict[str, int]:
        active = self.jobs.active_job_ids()
        result = await self.db.recover_stuck_jobs(
            stale_before=now - timedelta(seconds=self.stuck_after_seconds),
            retry_due_at=now + timedelta(seconds=5),
            max_attempts=max(
                1, int(getattr(self.settings, "max_reaction_attempts", 5))
            ),
            max_rows_per_kind=max(
                1, int(getattr(self.settings, "recovery_max_jobs_per_cycle", 100))
            ),
            exclude_join_job_ids=active.get("join", set()),
            exclude_reaction_job_ids=active.get("reaction", set()),
            exclude_view_job_ids=active.get("view", set()),
        )
        requeued = sum(
            int(result.get(f"{kind}_requeued", 0))
            for kind in ("join", "reaction", "view")
        )
        failed = sum(
            int(result.get(f"{kind}_failed", 0))
            for kind in ("join", "reaction", "view")
        )
        self._snapshot.stuck_requeued += requeued
        self._snapshot.stuck_failed += failed
        failure_window_minutes = max(
            1, int(getattr(self.settings, "alert_failure_window_minutes", 15))
        )
        recent_failed = await self.db.recent_recovery_failure_count(
            since=now - timedelta(minutes=failure_window_minutes)
        )
        if recent_failed:
            await self._notify_incident(
                "recovery:queue_attempts_exhausted",
                title="Автовосстановление не смогло вернуть часть заданий",
                detail=(
                    f"За последние {failure_window_minutes} мин. исчерпан лимит "
                    f"попыток у заданий: {recent_failed} · в текущем цикле: {failed}"
                ),
                severity="critical",
            )
        else:
            await self._resolve_incident(
                "recovery:queue_attempts_exhausted",
                title="Автовосстановление очередей",
            )
        if requeued or failed:
            logger.warning(
                "Automatic queue recovery requeued=%s failed=%s details=%s",
                requeued,
                failed,
                result,
            )
        return result

    async def _reschedule_restored_account(self, account_id: int) -> tuple[int, int]:
        scheduled = 0
        failures = 0
        channels = await self.db.list_channels(active_only=True)
        for channel in channels:
            try:
                result = await self.jobs.schedule_missing_channel_joins(
                    channel, account_ids=[account_id]
                )
                scheduled += int(result.get("scheduled", 0))
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                failures += 1
                logger.exception(
                    "Failed to reschedule restored account=%s target=%s",
                    account_id,
                    getattr(channel, "id", "unknown"),
                )
        return scheduled, failures

    async def _check_account(
        self, account: object, *, was_problem: bool, now: datetime
    ) -> tuple[str, int]:
        account_id = int(getattr(account, "id"))
        timeout_seconds = max(
            5,
            int(
                getattr(
                    self.settings, "recovery_account_check_timeout_seconds", 30
                )
            ),
        )
        restored = False
        cleared = False
        quarantined = False
        try:
            async with asyncio.timeout(timeout_seconds):
                async with self.pool.lock_for(account_id):
                    # The candidate list was built before this lock was acquired.
                    # A manual bulk cleanup may have deleted the row meanwhile,
                    # or a manual re-authorization may have restored it. Never
                    # reconnect a stale ORM object after either transition.
                    get_account = getattr(self.db, "get_account", None)
                    current_account = (
                        await get_account(account_id)
                        if callable(get_account)
                        else account
                    )
                    if current_account is None:
                        return "unchanged", 0
                    current_status = str(getattr(current_account, "status", ""))
                    if was_problem and current_status != "unauthorized":
                        return "unchanged", 0
                    if not was_problem and (
                        current_status == "unauthorized"
                        or not bool(getattr(current_account, "is_active", True))
                    ):
                        return "unchanged", 0
                    account = current_account
                    # Determine ownership only after the account lock is held. A
                    # worker could have connected the client while this recovery
                    # check was waiting for the same lock.
                    was_connected = self.pool.has_connected_client(account_id)
                    try:
                        await self.pool.ensure_authorized(account)
                        if was_problem:
                            restored = await self.db.restore_problem_account(account_id)
                        else:
                            cleared = await self.db.mark_account_healthy(account_id)
                    except ACCOUNT_AUTH_FAILURES:
                        # Quarantine/drop while still owning the account lock. This
                        # prevents another Telegram action from acquiring the same
                        # cached client between the failed identity check and drop.
                        quarantined = await self.pool.remove_unauthorized_account_while_locked(
                            account_id,
                            context=(
                                "automatic-problem-recheck"
                                if was_problem
                                else "automatic-transient-recheck"
                            ),
                        )
                    finally:
                        # A client opened only for this check must be disconnected
                        # before releasing the lock. Disconnecting outside the lock
                        # could tear down a session already acquired by another job.
                        if (
                            not was_connected
                            and self.pool.has_connected_client(account_id)
                        ):
                            await self.pool.disconnect_client(account_id)

            if was_problem and restored:
                scheduled, schedule_failures = await self._reschedule_restored_account(
                    account_id
                )
                await self._resolve_incident(
                    f"recovery:account_not_restored:{account_id}",
                    title=f"Аккаунт восстановлен: ID {account_id}",
                )
                if schedule_failures:
                    await self._notify_incident(
                        f"recovery:restored_account_join_schedule:{account_id}",
                        title="Аккаунт восстановлен, но подписки восстановлены не полностью",
                        detail=(
                            f"Аккаунт ID {account_id} · ошибок каналов/групп: "
                            f"{schedule_failures}"
                        ),
                        severity="warning",
                    )
                else:
                    await self._resolve_incident(
                        f"recovery:restored_account_join_schedule:{account_id}",
                        title="Очереди восстановленного аккаунта",
                    )
                return "restored", scheduled

            if was_problem:
                if quarantined:
                    # The row was already quarantined in the normal case, but a
                    # concurrent status repair may have temporarily changed it.
                    logger.warning(
                        "Automatic problem-account recheck re-quarantined account=%s",
                        account_id,
                    )
                await self._notify_incident(
                    f"recovery:account_not_restored:{account_id}",
                    title="Аккаунт не удалось восстановить автоматически",
                    detail=(
                        f"Аккаунт ID {account_id} по-прежнему не авторизован. "
                        "Требуется ручная повторная авторизация."
                    ),
                    severity="warning",
                )
                return "still_problem", 0

            if quarantined:
                return "quarantined", 0
            return ("cleared" if cleared else "unchanged"), 0
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            if not was_problem:
                await self.db.set_account_error(
                    account_id, f"Автопроверка: {type(exc).__name__}: {exc}"
                )
            logger.warning(
                "Automatic account recovery deferred account=%s problem=%s error=%s: %s",
                account_id,
                was_problem,
                type(exc).__name__,
                exc,
            )
            return "transient_errors", 0

    async def _recover_accounts(self, now: datetime) -> dict[str, int]:
        result = {
            "cleared": 0,
            "restored": 0,
            "quarantined": 0,
            "still_problem": 0,
            "unchanged": 0,
            "transient_errors": 0,
            "joins_rescheduled": 0,
        }
        limit = max(
            1, int(getattr(self.settings, "recovery_max_accounts_per_cycle", 5))
        )
        ready_interval = max(
            60, int(getattr(self.settings, "recovery_account_recheck_seconds", 1800))
        )
        problem_interval = max(
            300,
            int(
                getattr(
                    self.settings, "recovery_quarantine_recheck_seconds", 21600
                )
            ),
        )

        ready_accounts = await self.db.list_accounts(active_only=True)
        problem_accounts = await self.db.list_problem_accounts()
        ready_ids = {int(getattr(account, "id")) for account in ready_accounts}
        problem_ids = {int(getattr(account, "id")) for account in problem_accounts}
        # Prevent unbounded memory growth when accounts are deleted or restored.
        self._ready_account_next_check = {
            account_id: due
            for account_id, due in self._ready_account_next_check.items()
            if account_id in ready_ids
        }
        self._problem_account_next_check = {
            account_id: due
            for account_id, due in self._problem_account_next_check.items()
            if account_id in problem_ids
        }

        ready_candidates: list[tuple[object, bool]] = []
        for account in ready_accounts:
            account_id = int(getattr(account, "id"))
            flood_until = getattr(account, "flood_until", None)
            if flood_until is not None and flood_until > now:
                continue
            if not getattr(account, "last_error", None) and flood_until is None:
                continue
            if self._ready_account_next_check.get(account_id, now) > now:
                continue
            ready_candidates.append((account, False))

        problem_candidates: list[tuple[object, bool]] = []
        for account in problem_accounts:
            account_id = int(getattr(account, "id"))
            detected_at = getattr(account, "problem_detected_at", None)
            if isinstance(detected_at, datetime) and (
                now - detected_at
            ).total_seconds() < problem_interval:
                continue
            if self._problem_account_next_check.get(account_id, now) > now:
                continue
            problem_candidates.append((account, True))

        # Interleave both classes so a large transient-error population cannot
        # permanently starve quarantined accounts (and vice versa).
        candidates: list[tuple[object, bool]] = []
        ready_index = 0
        problem_index = 0
        while len(candidates) < limit and (
            ready_index < len(ready_candidates)
            or problem_index < len(problem_candidates)
        ):
            if problem_index < len(problem_candidates):
                candidates.append(problem_candidates[problem_index])
                problem_index += 1
                if len(candidates) >= limit:
                    break
            if ready_index < len(ready_candidates):
                candidates.append(ready_candidates[ready_index])
                ready_index += 1

        for account, was_problem in candidates:
            account_id = int(getattr(account, "id"))
            if was_problem:
                self._problem_account_next_check[account_id] = now + timedelta(
                    seconds=problem_interval
                )
            else:
                self._ready_account_next_check[account_id] = now + timedelta(
                    seconds=ready_interval
                )
            status, scheduled = await self._check_account(
                account, was_problem=was_problem, now=now
            )
            result[status if status in result else "unchanged"] += 1
            result["joins_rescheduled"] += scheduled

        self._snapshot.accounts_cleared += result["cleared"]
        self._snapshot.accounts_restored += result["restored"]
        self._snapshot.accounts_quarantined += result["quarantined"]
        self._snapshot.account_transient_errors += result["transient_errors"]
        self._snapshot.joins_rescheduled += result["joins_rescheduled"]

        if result["transient_errors"]:
            await self._notify_incident(
                "recovery:account_transient_errors",
                title="Автовосстановление аккаунтов выполнено не полностью",
                detail=(
                    f"Временных ошибок: {result['transient_errors']} · "
                    f"проверено за цикл: {len(candidates)}"
                ),
                severity="warning",
            )
        else:
            await self._resolve_incident(
                "recovery:account_transient_errors",
                title="Автовосстановление аккаунтов",
            )
        return result

    async def _restart_stale_workers(self, now: datetime) -> int:
        restarted = 0
        cooldown = max(
            60,
            int(getattr(self.settings, "recovery_worker_restart_cooldown_seconds", 300)),
        )
        for row in self.jobs.worker_health_snapshot(now=now):
            status = str(row.get("status", "starting"))
            if status not in {"stale", "blocked"}:
                continue
            health_name = str(row.get("name", ""))
            runtime = self._workers.get(health_name)
            if runtime is None or runtime.attempt_task is None:
                continue
            last = runtime.last_restart_requested_at
            if last is not None and (now - last).total_seconds() < cooldown:
                continue
            runtime.last_restart_requested_at = now
            runtime.restart_reason = (
                f"Автоматический перезапуск: status={status}, "
                f"heartbeat_age={row.get('age_seconds')}"
            )
            runtime.attempt_task.cancel()
            restarted += 1
        return restarted

    async def evaluate_once(self) -> dict[str, int]:
        if not self.enabled:
            return {"disabled": 1}
        async with self._lock:
            now = self.clock()
            self._snapshot.last_run_at = now
            self._snapshot.runs += 1
            try:
                restarted = await self._restart_stale_workers(now)
                if restarted:
                    # Let cancellation reach the stale worker before deciding which
                    # running DB rows are still owned by a live Telegram task.
                    await asyncio.sleep(0)
                queue = await self._recover_stuck_jobs(now)
                accounts = await self._recover_accounts(now)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._snapshot.last_error_at = now
                self._snapshot.last_error = f"{type(exc).__name__}: {exc}"[:500]
                await self._notify_incident(
                    "recovery:monitor_failed",
                    title="Цикл автоматического восстановления завершился ошибкой",
                    detail=self._snapshot.last_error,
                    severity="critical",
                )
                logger.exception("Automatic recovery cycle failed")
                raise
            self._snapshot.last_success_at = now
            self._snapshot.last_error = None
            await self._resolve_incident(
                "recovery:monitor_failed",
                title="Автоматическое восстановление",
            )
            return {
                **queue,
                **{f"account_{key}": value for key, value in accounts.items()},
                "workers_restarted": restarted,
            }

    async def monitor_loop(self) -> None:
        while True:
            try:
                await self.evaluate_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                # The recovery task remains alive after transient DB/Telegram errors.
                # Repeated failures are surfaced by critical alerts.
                pass
            await asyncio.sleep(self.check_interval_seconds)

    def _trim_crash_window(self, runtime: WorkerRuntime, now: datetime) -> None:
        window = max(
            60,
            int(
                getattr(
                    self.settings, "recovery_worker_restart_window_seconds", 900
                )
            ),
        )
        cutoff = now - timedelta(seconds=window)
        while runtime.crashes and runtime.crashes[0] < cutoff:
            runtime.crashes.popleft()

    async def run_restartable_worker(
        self, public_name: str, health_name: str, factory: WorkerFactory
    ) -> None:
        """Run one worker with bounded local restarts before Railway escalation."""

        runtime = WorkerRuntime(
            public_name=public_name,
            health_name=health_name,
            factory=factory,
        )
        self._workers[health_name] = runtime
        incident_key = f"recovery:worker_restart:{health_name}"
        try:
            while True:
                attempt_started = self.clock()
                runtime.attempt_task = asyncio.create_task(
                    factory(), name=f"{public_name}-attempt"
                )
                try:
                    while not runtime.attempt_task.done():
                        await asyncio.wait({runtime.attempt_task}, timeout=1)
                        row = next(
                            (
                                item
                                for item in self.jobs.worker_health_snapshot(
                                    now=self.clock()
                                )
                                if str(item.get("name")) == health_name
                            ),
                            None,
                        )
                        if row:
                            last_tick = row.get("last_tick_at")
                            fresh_heartbeat = bool(
                                isinstance(last_tick, datetime)
                                and last_tick >= attempt_started
                            )
                            if fresh_heartbeat:
                                # Clear only the supervisor's own restart marker.
                                # Genuine worker errors remain governed by the
                                # worker-specific success rules.
                                self.jobs.mark_worker_restarted(health_name)
                                refreshed = next(
                                    (
                                        item
                                        for item in self.jobs.worker_health_snapshot(
                                            now=self.clock()
                                        )
                                        if str(item.get("name")) == health_name
                                    ),
                                    None,
                                )
                                if refreshed and str(refreshed.get("status")) == "ok":
                                    await self._resolve_incident(
                                        incident_key,
                                        title=f"Worker восстановлен: {public_name}",
                                    )
                    if runtime.attempt_task.cancelled():
                        if runtime.restart_reason:
                            error = RuntimeError(runtime.restart_reason)
                            runtime.restart_reason = None
                        else:
                            raise asyncio.CancelledError
                    else:
                        task_error = runtime.attempt_task.exception()
                        error = task_error or RuntimeError(
                            "Worker завершился без ожидаемого исключения"
                        )
                        runtime.restart_reason = None
                except asyncio.CancelledError:
                    if runtime.attempt_task and not runtime.attempt_task.done():
                        runtime.attempt_task.cancel()
                        await asyncio.gather(
                            runtime.attempt_task, return_exceptions=True
                        )
                    raise

                now = self.clock()
                runtime.crashes.append(now)
                self._trim_crash_window(runtime, now)
                self._snapshot.worker_restarts += 1
                self.jobs.mark_worker_restarting(
                    health_name, f"{type(error).__name__}: {error}"
                )
                max_restarts = max(
                    0,
                    int(
                        getattr(
                            self.settings, "recovery_worker_max_restarts", 3
                        )
                    ),
                )
                if len(runtime.crashes) > max_restarts:
                    self._snapshot.worker_restart_failures += 1
                    await self._notify_incident(
                        incident_key,
                        title=f"Worker не удалось восстановить: {public_name}",
                        detail=(
                            f"Сбоев в окне: {len(runtime.crashes)} · "
                            f"последняя ошибка: {type(error).__name__}: {error}"
                        ),
                        severity="critical",
                    )
                    raise RuntimeError(
                        f"Worker restart budget exhausted: {public_name}"
                    ) from error

                await self._notify_incident(
                    incident_key,
                    title=f"Автоматический перезапуск worker: {public_name}",
                    detail=(
                        f"Попытка {len(runtime.crashes)}/{max_restarts} · "
                        f"причина: {type(error).__name__}: {error}"
                    ),
                    severity="warning",
                )
                backoff = max(
                    1,
                    int(
                        getattr(
                            self.settings, "recovery_worker_restart_backoff_seconds", 5
                        )
                    ),
                )
                await asyncio.sleep(min(60, backoff * (2 ** (len(runtime.crashes) - 1))))
        finally:
            runtime.attempt_task = None
            self._workers.pop(health_name, None)
