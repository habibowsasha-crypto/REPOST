from __future__ import annotations

import asyncio
import hashlib
import json
import random
import logging
import time
import math
import os
import sys
import uuid
from collections import OrderedDict as _OrderedDict
from contextlib import asynccontextmanager
from contextvars import ContextVar
from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Dict, List, Optional

import aiosqlite

from app import __version__
from app.config import get_settings
from app.database.statistics_schema import (
    STATISTICS_V2_PG_SCHEMA,
    STATISTICS_V2_SQLITE_SCHEMA,
    pg_migrate_statistics_v2,
    pg_preflight_statistics_v2_columns,
    sqlite_migrate_statistics_v2,
)
from app.services.models import TpMode, UserMode, UserSettings
from app.services.exchange_identity import clean_exchange_id
from app.services.execution_exposure import (
    execution_be_protection_confirmed,
    execution_cleanup_unresolved,
    execution_payload_dict,
    execution_zero_exposure_confirmed,
    finite_number,
    manual_required_zero_exposure_release_state,
    critical_zero_exposure_proof_state,
)
from app.services.financial_reconciliation_models import (
    FINANCIAL_ACTIVE_STATUSES,
    FINANCIAL_STATUS_AMBIGUOUS,
    FINANCIAL_STATUS_CONFIRMED,
    FINANCIAL_STATUS_PARTIAL,
    FINANCIAL_STATUS_PENDING,
    FINANCIAL_STATUS_PROCESSING,
    FINANCIAL_STATUS_UNAVAILABLE,
    FINANCIAL_TERMINAL_STATUSES,
    ORDER_STATUS_AMBIGUOUS,
    ORDER_STATUS_CONFIRMED,
    ORDER_STATUS_EXPECTED,
    FinancialFillRecord,
    FinancialOrderExpectation,
    aggregate_fill_records,
    decimal_text,
    fill_matches_expectation,
    normalize_close_type,
    normalize_datetime_text,
    normalize_fill_records,
    normalize_order_expectations,
    normalize_side,
    normalize_status,
    optional_text,
)

log = logging.getLogger(__name__)

try:  # optional at runtime; required for Railway PostgreSQL
    import asyncpg
except Exception:  # pragma: no cover
    asyncpg = None

# Reuse PostgreSQL connections instead of opening a new TCP/TLS connection for
# every DB helper. The home dashboard calls several independent helpers; on
# Railway, connection handshakes were the main reason a simple ``Меню`` update
# could take 10-15 seconds.
_PG_POOL: Any = None
_PG_POOL_LOCK = asyncio.Lock()

# Monitor workers can create a short startup burst: public prices, event
# verifier, critical reconciliation and the full fallback all become runnable at
# once.  Without an admission cap they can occupy every pool connection and
# make a simple Telegram ``Меню`` request wait behind monitor work.  The context
# flag is inherited by child tasks, while Telegram handlers and trade dispatcher
# tasks remain outside this cap.
_MONITOR_DB_CONTEXT: ContextVar[bool] = ContextVar(
    "antilud_monitor_db_context", default=False
)
_MONITOR_DB_SEMAPHORE: asyncio.Semaphore | None = None
_MONITOR_DB_SEMAPHORE_LIMIT = 0
# v1.0.7g7h2f5g5b2: reserve one ordinary monitor DB slot for the
# five-second critical reconciliation lane.  Low-priority public/full/
# diagnostics work must not be able to consume every ordinary monitor permit
# and make the safety lane wait through admission + pool-acquire timeouts.
_MONITOR_CRITICAL_DB_SEMAPHORE: asyncio.Semaphore | None = None
_MONITOR_CRITICAL_DB_SEMAPHORE_LIMIT = 0
_MONITOR_ADVISORY_SEMAPHORE: asyncio.Semaphore | None = None
_MONITOR_ADVISORY_SEMAPHORE_LIMIT = 0
# Reserve part of the existing advisory-holder budget for safety-critical
# reconciliation and fresh market-event verification.  The split never increases
# total monitor pool pressure.
_MONITOR_CRITICAL_ADVISORY_SEMAPHORE: asyncio.Semaphore | None = None
_MONITOR_CRITICAL_ADVISORY_SEMAPHORE_LIMIT = 0
_SAFETY_CRITICAL_MONITOR_STAGES = frozenset({"critical", "event-critical"})
_MONITOR_DB_ACQUIRE_TIMEOUT_SEC = 5.0
_GENERAL_DB_ACQUIRE_TIMEOUT_SEC = 20.0
_POOL_TIMEOUT_LAST_LOG: Dict[str, float] = {}
_POOL_RELEASE_LAST_LOG: Dict[str, float] = {}
_PENDING_POOL_RELEASE_TASKS: set[asyncio.Task[Any]] = set()
_EXPECTED_TERMINATED_RELEASE_TASKS: set[asyncio.Task[Any]] = set()
_ADVISORY_BUSY_LAST_LOG: Dict[str, float] = {}
_MONITOR_DB_STAGE: ContextVar[str] = ContextVar(
    "antilud_monitor_db_stage", default="foreground"
)
_CURRENT_ADVISORY_CONN: ContextVar[Any] = ContextVar(
    "antilud_current_advisory_conn", default=None
)
_MONITOR_DEFERRED_REASONS: ContextVar[list[dict[str, Any]] | None] = ContextVar(
    "antilud_monitor_deferred_reasons", default=None
)
_MONITOR_FULL_ADVISORY_SEMAPHORE: asyncio.Semaphore | None = None
_MONITOR_FULL_ADVISORY_SEMAPHORE_LIMIT = 0


def _current_advisory_connection() -> Any | None:
    """Return this task's already-held PostgreSQL advisory connection."""

    owner = _CURRENT_ADVISORY_CONN.get()
    task = asyncio.current_task()
    if (
        isinstance(owner, tuple)
        and len(owner) == 2
        and owner[0] is task
        and owner[1] is not None
    ):
        return owner[1]
    return None


def _record_critical_counter(key: str, amount: int = 1) -> None:
    if monitor_workload_stage() not in _SAFETY_CRITICAL_MONITOR_STAGES:
        return
    try:
        from app.services.monitor_diagnostics import record_counter

        record_counter(key, amount)
    except Exception:
        return


class MonitorAdvisoryBusy(TimeoutError):
    """Expected monitor contention that must be retried, not treated as a crash."""

    def __init__(self, *, phase: str, key: str, stage: str, timeout_sec: float):
        self.phase = str(phase)
        self.key = str(key)
        self.stage = str(stage or "monitor")
        self.timeout_sec = float(timeout_sec)
        super().__init__(
            f"monitor advisory busy phase={self.phase} stage={self.stage} "
            f"timeout={self.timeout_sec:.1f}s key={self.key!r}"
        )


def _log_pool_timeout(phase: str, *, workload: str, timeout_sec: float) -> None:
    key = f"{phase}:{workload}"
    now = time.monotonic()
    if now - _POOL_TIMEOUT_LAST_LOG.get(key, 0.0) < 10.0:
        return
    _POOL_TIMEOUT_LAST_LOG[key] = now
    log.error(
        "POSTGRES_POOL_TIMEOUT phase=%s workload=%s timeout_sec=%.1f",
        phase,
        workload,
        timeout_sec,
    )


def _consume_release_task_result(task: asyncio.Task[Any]) -> None:
    """Consume a detached pool-release result and drop its strong ownership.

    G65 distinguishes the expected asyncpg error produced after we deliberately
    terminate a connection during caller cancellation from a genuine pool
    cleanup failure. This keeps diagnostics useful without hiding unrelated
    release exceptions.
    """

    _PENDING_POOL_RELEASE_TASKS.discard(task)
    expected_terminated = task in _EXPECTED_TERMINATED_RELEASE_TASKS
    _EXPECTED_TERMINATED_RELEASE_TASKS.discard(task)
    try:
        task.result()
    except asyncio.CancelledError:
        return
    except Exception as exc:
        error_type = type(exc).__name__
        if expected_terminated and error_type == "ConnectionDoesNotExistError":
            log.debug(
                "POSTGRES_POOL_RELEASE_EXPECTED_TERMINATED error=%s",
                error_type,
            )
            return
        key = f"detached:{error_type}"
        now = time.monotonic()
        if now - _POOL_RELEASE_LAST_LOG.get(key, 0.0) >= 10.0:
            _POOL_RELEASE_LAST_LOG[key] = now
            # Cleanup exceptions can include driver/DSN context. Keep this
            # marker deliberately type-only so diagnostics never expose a
            # credential-bearing connection string.
            log.warning(
                "POSTGRES_POOL_RELEASE_BACKGROUND_FAILED error=%s",
                error_type,
            )


def _own_detached_release_task(
    task: asyncio.Task[Any], *, expected_terminated: bool = False
) -> None:
    """Keep a detached release alive until its result is consumed.

    asyncio keeps only weak references to background tasks.  Without this
    process-owned set a cancelled caller could return while Pool.release() is
    still resetting the connection, allowing the cleanup task to be garbage
    collected as ``Task was destroyed but it is pending``.
    """

    _PENDING_POOL_RELEASE_TASKS.add(task)
    if expected_terminated:
        _EXPECTED_TERMINATED_RELEASE_TASKS.add(task)
    task.add_done_callback(_consume_release_task_result)


def _terminate_pool_connection(conn: Any) -> bool:
    """Best-effort hard close of one asyncpg pooled connection proxy.

    ``PoolConnectionHolder.release`` already terminates on reset failures, but an
    outer task cancellation can otherwise leave its shielded release coroutine
    detached.  Termination is synchronous in asyncpg and causes the holder to
    return to the pool without reusing a connection whose protocol state is
    uncertain.
    """

    candidates = [getattr(conn, "_con", None), conn]
    for candidate in candidates:
        terminate = getattr(candidate, "terminate", None)
        if not callable(terminate):
            continue
        try:
            terminate()
            return True
        except Exception:
            continue
    return False


async def _release_pg_connection_safely(
    pool: Any,
    conn: Any,
    *,
    timeout_sec: float,
    phase: str,
    workload: str,
    caller_cancelled: bool = False,
) -> bool:
    """Return a connection without leaking a shielded asyncpg release task.

    A release/reset failure happens *after* the caller's SQL result or original
    exception is already known.  Raising the cleanup failure would make writes
    look unsuccessful and could trigger unsafe duplicate retries.  We therefore
    terminate the uncertain connection, log the cleanup problem, and preserve
    only task cancellation.
    """

    try:
        release_awaitable = pool.release(conn, timeout=max(0.1, float(timeout_sec)))
    except Exception as exc:
        terminated = _terminate_pool_connection(conn)
        key = f"{phase}:{workload}:{type(exc).__name__}"
        now = time.monotonic()
        if now - _POOL_RELEASE_LAST_LOG.get(key, 0.0) >= 10.0:
            _POOL_RELEASE_LAST_LOG[key] = now
            log.error(
                "POSTGRES_POOL_RELEASE_FAILED phase=%s workload=%s error=%s terminated=%s",
                phase,
                workload,
                type(exc).__name__,
                int(terminated),
            )
        return False

    release_task = asyncio.create_task(
        release_awaitable,
        name=f"postgres-release:{phase}:{workload}",
    )
    if caller_cancelled:
        # The SQL coroutine is already unwinding because its caller timed out or
        # cancelled.  Do not add the release/reset timeout to that critical
        # path: terminate synchronously, keep ownership of Pool.release and let
        # its callback consume the eventual result.  The original cancellation
        # remains active in the caller and will propagate after cleanup returns.
        _terminate_pool_connection(conn)
        _own_detached_release_task(release_task, expected_terminated=True)
        return False
    try:
        await asyncio.shield(release_task)
        return True
    except asyncio.CancelledError:
        # Keep the asyncpg Pool.release coroutine alive so its own internal
        # shield is not orphaned.  Hard-terminate now to free the holder quickly;
        # the callback consumes the eventual release result.
        _terminate_pool_connection(conn)
        _own_detached_release_task(release_task, expected_terminated=True)
        raise
    except Exception as exc:
        terminated = _terminate_pool_connection(conn)
        key = f"{phase}:{workload}:{type(exc).__name__}"
        now = time.monotonic()
        if now - _POOL_RELEASE_LAST_LOG.get(key, 0.0) >= 10.0:
            _POOL_RELEASE_LAST_LOG[key] = now
            log.error(
                "POSTGRES_POOL_RELEASE_FAILED phase=%s workload=%s error=%s terminated=%s",
                phase,
                workload,
                type(exc).__name__,
                int(terminated),
            )
        return False


def _monitor_ordinary_budget(pool_max: int) -> int:
    """Total ordinary monitor connections allowed across all monitor lanes."""

    pool_max = max(2, int(pool_max))
    return max(1, min(4, pool_max - 2))


def _monitor_critical_ordinary_budget(total_budget: int) -> int:
    """Critical share of the unchanged ordinary monitor DB budget.

    G64 production evidence showed that a one-slot critical ordinary reserve
    could be occupied by the five-second lifecycle reconcile long enough for
    TP->BE preflight / fresh MARKET EVENT verification to hit the five-second
    admission timeout.  Rebalance the *existing* budget instead of increasing
    monitor pool pressure: the default four ordinary monitor slots become
    2 critical + 2 general.  Smaller pools keep the previous one-slot critical
    reserve, and a one-slot deployment continues to share that slot.
    """

    total = max(1, int(total_budget))
    if total >= 4:
        return 2
    if total >= 2:
        return 1
    return 0


def _monitor_db_semaphore() -> asyncio.Semaphore:
    """Admission for non-critical ordinary monitor DB scopes."""

    global _MONITOR_DB_SEMAPHORE, _MONITOR_DB_SEMAPHORE_LIMIT
    settings = get_settings()
    pool_max = max(2, int(getattr(settings, "POSTGRES_POOL_MAX_SIZE", 12) or 12))
    total_budget = _monitor_ordinary_budget(pool_max)
    # G64: reserve a second critical slot on the default four-slot ordinary
    # monitor budget without increasing the total number of monitor DB users.
    critical_budget = _monitor_critical_ordinary_budget(total_budget)
    limit = total_budget - critical_budget if critical_budget else total_budget
    if _MONITOR_DB_SEMAPHORE is None or _MONITOR_DB_SEMAPHORE_LIMIT != limit:
        _MONITOR_DB_SEMAPHORE = asyncio.Semaphore(limit)
        _MONITOR_DB_SEMAPHORE_LIMIT = limit
    return _MONITOR_DB_SEMAPHORE


def _monitor_critical_db_semaphore() -> asyncio.Semaphore:
    """Dedicated admission for safety-critical ordinary DB paths.

    The critical reserve is carved out of the same total ordinary monitor
    budget.  It therefore cannot reduce the foreground PostgreSQL reserve.
    """

    global _MONITOR_CRITICAL_DB_SEMAPHORE
    global _MONITOR_CRITICAL_DB_SEMAPHORE_LIMIT
    settings = get_settings()
    pool_max = max(2, int(getattr(settings, "POSTGRES_POOL_MAX_SIZE", 12) or 12))
    total_budget = _monitor_ordinary_budget(pool_max)
    limit = _monitor_critical_ordinary_budget(total_budget)
    if limit <= 0:
        _MONITOR_CRITICAL_DB_SEMAPHORE = None
        _MONITOR_CRITICAL_DB_SEMAPHORE_LIMIT = 0
        return _monitor_db_semaphore()
    if (
        _MONITOR_CRITICAL_DB_SEMAPHORE is None
        or _MONITOR_CRITICAL_DB_SEMAPHORE_LIMIT != limit
    ):
        _MONITOR_CRITICAL_DB_SEMAPHORE = asyncio.Semaphore(limit)
        _MONITOR_CRITICAL_DB_SEMAPHORE_LIMIT = limit
    return _MONITOR_CRITICAL_DB_SEMAPHORE


def _monitor_advisory_budget(pool_max: int) -> int:
    """Total outer monitor advisory holders allowed across all monitor lanes."""

    normalized = max(2, int(pool_max))
    ordinary_limit = _monitor_ordinary_budget(normalized)
    return max(1, min(4, max(1, normalized - ordinary_limit - 4)))


def _monitor_critical_advisory_budget(total_budget: int) -> int:
    """Critical share of the unchanged advisory budget.

    The default total budget is four, split 2 critical + 2 general.  A budget of
    two or three reserves one critical holder.  A one-slot deployment cannot
    physically isolate two lanes without overcommitting its PostgreSQL pool and
    therefore falls back to the shared semaphore.
    """

    total = max(1, int(total_budget))
    if total >= 4:
        return 2
    if total >= 2:
        return 1
    return 0


def _monitor_advisory_semaphore() -> asyncio.Semaphore:
    """Admission for non-critical outer monitor advisory holders.

    Nested advisory locks reuse the same PostgreSQL connection in this task and
    therefore consume neither another pool slot nor another admission permit.
    """

    global _MONITOR_ADVISORY_SEMAPHORE, _MONITOR_ADVISORY_SEMAPHORE_LIMIT
    settings = get_settings()
    pool_max = max(2, int(getattr(settings, "POSTGRES_POOL_MAX_SIZE", 12) or 12))
    total_budget = _monitor_advisory_budget(pool_max)
    critical_budget = _monitor_critical_advisory_budget(total_budget)
    limit = total_budget - critical_budget if critical_budget else total_budget
    if (
        _MONITOR_ADVISORY_SEMAPHORE is None
        or _MONITOR_ADVISORY_SEMAPHORE_LIMIT != limit
    ):
        _MONITOR_ADVISORY_SEMAPHORE = asyncio.Semaphore(limit)
        _MONITOR_ADVISORY_SEMAPHORE_LIMIT = limit
    return _MONITOR_ADVISORY_SEMAPHORE


def _monitor_critical_advisory_semaphore() -> asyncio.Semaphore:
    """Dedicated admission for fresh event and critical reconcile advisories."""

    global _MONITOR_CRITICAL_ADVISORY_SEMAPHORE
    global _MONITOR_CRITICAL_ADVISORY_SEMAPHORE_LIMIT
    settings = get_settings()
    pool_max = max(2, int(getattr(settings, "POSTGRES_POOL_MAX_SIZE", 12) or 12))
    total_budget = _monitor_advisory_budget(pool_max)
    limit = _monitor_critical_advisory_budget(total_budget)
    if limit <= 0:
        _MONITOR_CRITICAL_ADVISORY_SEMAPHORE = None
        _MONITOR_CRITICAL_ADVISORY_SEMAPHORE_LIMIT = 0
        return _monitor_advisory_semaphore()
    if (
        _MONITOR_CRITICAL_ADVISORY_SEMAPHORE is None
        or _MONITOR_CRITICAL_ADVISORY_SEMAPHORE_LIMIT != limit
    ):
        _MONITOR_CRITICAL_ADVISORY_SEMAPHORE = asyncio.Semaphore(limit)
        _MONITOR_CRITICAL_ADVISORY_SEMAPHORE_LIMIT = limit
    return _MONITOR_CRITICAL_ADVISORY_SEMAPHORE


def _monitor_full_advisory_semaphore() -> asyncio.Semaphore:
    """Allow at most one low-priority full-reconcile advisory holder locally."""

    global _MONITOR_FULL_ADVISORY_SEMAPHORE
    global _MONITOR_FULL_ADVISORY_SEMAPHORE_LIMIT
    limit = 1
    if (
        _MONITOR_FULL_ADVISORY_SEMAPHORE is None
        or _MONITOR_FULL_ADVISORY_SEMAPHORE_LIMIT != limit
    ):
        _MONITOR_FULL_ADVISORY_SEMAPHORE = asyncio.Semaphore(limit)
        _MONITOR_FULL_ADVISORY_SEMAPHORE_LIMIT = limit
    return _MONITOR_FULL_ADVISORY_SEMAPHORE


def monitor_db_admission_snapshot() -> Dict[str, int]:
    settings = get_settings()
    pool_max = max(2, int(getattr(settings, "POSTGRES_POOL_MAX_SIZE", 12) or 12))
    total_ordinary = _monitor_ordinary_budget(pool_max)
    _monitor_db_semaphore()
    _monitor_critical_db_semaphore()
    total_advisory = _monitor_advisory_budget(pool_max)
    _monitor_advisory_semaphore()
    _monitor_critical_advisory_semaphore()
    _monitor_full_advisory_semaphore()
    critical_limit = (
        int(_MONITOR_CRITICAL_DB_SEMAPHORE_LIMIT)
        if total_ordinary >= 2
        else int(_MONITOR_DB_SEMAPHORE_LIMIT)
    )
    return {
        "pool_max": pool_max,
        "ordinary_monitor_limit": int(_MONITOR_DB_SEMAPHORE_LIMIT),
        "critical_monitor_limit": critical_limit,
        "ordinary_monitor_total_limit": int(total_ordinary),
        # Compatibility key remains the total advisory-holder budget.  The two
        # following keys expose the actual stage-specific split.
        "advisory_monitor_limit": int(total_advisory),
        "general_advisory_monitor_limit": int(_MONITOR_ADVISORY_SEMAPHORE_LIMIT),
        "critical_advisory_monitor_limit": int(
            _MONITOR_CRITICAL_ADVISORY_SEMAPHORE_LIMIT
            or _MONITOR_ADVISORY_SEMAPHORE_LIMIT
        ),
        "critical_advisory_isolated": int(
            _MONITOR_CRITICAL_ADVISORY_SEMAPHORE_LIMIT > 0
        ),
        "full_advisory_monitor_limit": int(_MONITOR_FULL_ADVISORY_SEMAPHORE_LIMIT),
    }


def monitor_workload_stage() -> str:
    return str(_MONITOR_DB_STAGE.get() or "foreground")


def _record_monitor_deferred(*, phase: str, key: str, stage: str) -> None:
    reasons = _MONITOR_DEFERRED_REASONS.get()
    if reasons is not None:
        reasons.append({"phase": str(phase), "key": str(key), "stage": str(stage)})


@asynccontextmanager
async def monitor_deferred_capture() -> AsyncIterator[list[dict[str, Any]]]:
    """Collect expected lock deferrals produced by one verifier user task."""

    reasons: list[dict[str, Any]] = []
    token = _MONITOR_DEFERRED_REASONS.set(reasons)
    try:
        yield reasons
    finally:
        _MONITOR_DEFERRED_REASONS.reset(token)


@asynccontextmanager
async def monitor_db_workload(stage: str = "monitor") -> AsyncIterator[None]:
    """Mark the current task tree as background monitor DB workload."""

    token = _MONITOR_DB_CONTEXT.set(True)
    stage_token = _MONITOR_DB_STAGE.set(str(stage or "monitor"))
    try:
        yield
    finally:
        _MONITOR_DB_STAGE.reset(stage_token)
        _MONITOR_DB_CONTEXT.reset(token)


async def _ensure_pg_pool() -> Any:
    global _PG_POOL
    if not is_postgres():
        return None
    if asyncpg is None:
        raise RuntimeError("DATABASE_URL задан, но asyncpg не установлен")
    if _PG_POOL is not None and not getattr(_PG_POOL, "_closed", False):
        return _PG_POOL
    async with _PG_POOL_LOCK:
        if _PG_POOL is None or getattr(_PG_POOL, "_closed", False):
            settings = get_settings()
            min_size = max(1, int(getattr(settings, "POSTGRES_POOL_MIN_SIZE", 1) or 1))
            max_size = max(
                min_size, int(getattr(settings, "POSTGRES_POOL_MAX_SIZE", 10) or 10)
            )
            _PG_POOL = await asyncpg.create_pool(
                dsn=settings.DATABASE_URL,
                min_size=min_size,
                max_size=max_size,
                timeout=10,
                command_timeout=30,
            )
    return _PG_POOL


async def close_db() -> None:
    """Close the shared PostgreSQL pool during graceful shutdown."""
    global _PG_POOL
    pool = _PG_POOL
    _PG_POOL = None
    if pool is not None and not getattr(pool, "_closed", False):
        await pool.close()


_MAX_LOCK_CACHE = 2000


def _get_or_create_lock(d: _OrderedDict, key, max_size: int) -> asyncio.Lock:
    """Return a per-key lock without evicting a lock that is still in use.

    Evicting a locked object can create a second lock for the same key and break
    mutual exclusion. We therefore prune only unlocked oldest entries. A small
    temporary overflow is safer than concurrent order cancellation.
    """
    if key in d:
        d.move_to_end(key)
        return d[key]
    lock = asyncio.Lock()
    d[key] = lock
    while len(d) > max_size:
        removed = False
        for old_key, old_lock in list(d.items()):
            if old_key == key or old_lock.locked():
                continue
            d.pop(old_key, None)
            removed = True
            break
        if not removed:
            break
    return lock


_EXECUTION_LOCKS: _OrderedDict[int, asyncio.Lock] = _OrderedDict()
_USER_ENSURE_LOCKS: _OrderedDict[int, asyncio.Lock] = _OrderedDict()
_ENSURED_USERS: set[int] = set()

# Per-(user_id, symbol) lock to prevent two monitor workers from simultaneously
# performing symbol-wide DELETE algoOpenOrders for the same user+symbol.
_SYMBOL_ACTION_LOCKS: _OrderedDict[tuple[int, str], asyncio.Lock] = _OrderedDict()
_SYMBOL_ACTION_LOCKS_META = asyncio.Lock()


@asynccontextmanager
async def symbol_action_lock(user_id: int, symbol: str):
    """Serialize order mutations for one user+symbol in this bot process.

    Signal entry, BE replacement and lifecycle cleanup all use this lock, so a
    local monitor cannot cancel orders while a new position is being opened.
    Cross-process TP writes are protected separately by PostgreSQL advisory
    locks inside the BingX adapter; the service must still run with one replica.
    """
    key = (int(user_id), str(symbol).upper())
    async with _SYMBOL_ACTION_LOCKS_META:
        lock = _get_or_create_lock(_SYMBOL_ACTION_LOCKS, key, _MAX_LOCK_CACHE)
    wait_started = time.monotonic()
    async with lock:
        try:
            from app.services.monitor_diagnostics import record_wait

            record_wait("lock_wait_ms", (time.monotonic() - wait_started) * 1000)
        except Exception:
            pass
        yield


@asynccontextmanager
async def execution_lock(
    execution_id: int, *, defer_monitor_busy: bool | None = None
) -> AsyncIterator[bool]:
    """Serialize one execution locally and across overlapping Railway processes.

    The local lock protects concurrent monitor tasks in one process. PostgreSQL
    advisory locking additionally prevents an old and a new deploy from acting
    on the same execution at the same time. SQLite keeps the local-only fallback
    used by tests and single-process development.
    """
    key = int(execution_id or 0)
    lock = _get_or_create_lock(_EXECUTION_LOCKS, key, _MAX_LOCK_CACHE)
    wait_started = time.monotonic()
    async with lock:
        try:
            from app.services.monitor_diagnostics import record_wait

            record_wait("lock_wait_ms", (time.monotonic() - wait_started) * 1000)
        except Exception:
            pass
        should_defer = (
            bool(_MONITOR_DB_CONTEXT.get())
            if defer_monitor_busy is None
            else bool(defer_monitor_busy)
        )
        reused_full_connection = bool(
            monitor_workload_stage() == "full"
            and _current_advisory_connection() is not None
        )
        advisory_cm = distributed_advisory_lock(f"execution:{key}")
        try:
            await advisory_cm.__aenter__()
            if reused_full_connection:
                try:
                    from app.services.monitor_diagnostics import record_counter

                    # Count only successfully acquired exact execution locks.
                    # g5b3 incremented before __aenter__, so a busy/deferred lock
                    # could be reported as reused even though it never entered.
                    record_counter("full_execution_lock_connection_reused")
                except Exception:
                    pass
        except MonitorAdvisoryBusy as exc:
            if not should_defer:
                raise
            _record_monitor_deferred(
                phase=exc.phase, key=f"execution:{key}", stage=exc.stage
            )
            yield False
            return

        try:
            try:
                yield True
            except MonitorAdvisoryBusy as exc:
                if not should_defer:
                    raise
                _record_monitor_deferred(
                    phase=exc.phase, key=f"execution:{key}", stage=exc.stage
                )
                log.warning(
                    "EXECUTION_LOCK_BODY_DEFERRED execution_id=%s phase=%s "
                    "stage=%s key=%s",
                    key,
                    exc.phase,
                    exc.stage,
                    exc.key,
                )
                return
        finally:
            await advisory_cm.__aexit__(None, None, None)


@asynccontextmanager
async def distributed_advisory_lock(key: str, *, timeout_sec: float = 20.0):
    """Cross-process PostgreSQL advisory lock with monitor-safe deferral.

    The outermost lock in a task owns one pool connection. Nested BingX
    STOP/TP locks reuse that same connection, preventing the old pattern where
    an execution lock held one connection and an adapter lock waited for a
    second connection. Expected monitor contention raises MonitorAdvisoryBusy so
    the caller can durably defer instead of logging a traceback.
    """
    if not is_postgres():
        yield
        return

    digest = hashlib.sha256(str(key or "").encode("utf-8")).digest()
    lock_id = int.from_bytes(digest[:8], "big", signed=True)
    timeout = max(0.1, float(timeout_sec or 0.1))
    deadline = asyncio.get_running_loop().time() + timeout
    wait_started = time.monotonic()
    monitor = bool(_MONITOR_DB_CONTEXT.get())
    stage = monitor_workload_stage() if monitor else "foreground"

    pool = None
    current_task = asyncio.current_task()
    advisory_owner = _CURRENT_ADVISORY_CONN.get()
    conn = (
        advisory_owner[1]
        if isinstance(advisory_owner, tuple)
        and len(advisory_owner) == 2
        and advisory_owner[0] is current_task
        else None
    )
    owns_conn = conn is None
    advisory_sem: asyncio.Semaphore | None = None
    full_sem: asyncio.Semaphore | None = None
    advisory_slot = False
    full_slot = False
    conn_token = None
    acquired = False

    def monitor_busy(phase: str, wait_timeout: float) -> MonitorAdvisoryBusy:
        log_key = f"{phase}:{stage}:{key}"
        now = time.monotonic()
        if now - _ADVISORY_BUSY_LAST_LOG.get(log_key, 0.0) >= 10.0:
            _ADVISORY_BUSY_LAST_LOG[log_key] = now
            log.warning(
                "ADVISORY_BUSY_DEFERRED phase=%s stage=%s key=%s timeout_sec=%.1f",
                phase,
                stage,
                key,
                wait_timeout,
            )
        return MonitorAdvisoryBusy(
            phase=phase, key=str(key), stage=stage, timeout_sec=wait_timeout
        )

    try:
        if owns_conn:
            pool = await _ensure_pg_pool()
            if monitor:
                if stage == "full":
                    full_sem = _monitor_full_advisory_semaphore()
                    try:
                        await asyncio.wait_for(
                            full_sem.acquire(),
                            timeout=_MONITOR_DB_ACQUIRE_TIMEOUT_SEC,
                        )
                    except asyncio.TimeoutError as exc:
                        raise monitor_busy(
                            "full_advisory_admission",
                            _MONITOR_DB_ACQUIRE_TIMEOUT_SEC,
                        ) from exc
                    full_slot = True
                advisory_sem = (
                    _monitor_critical_advisory_semaphore()
                    if stage in _SAFETY_CRITICAL_MONITOR_STAGES
                    else _monitor_advisory_semaphore()
                )
                try:
                    await asyncio.wait_for(
                        advisory_sem.acquire(),
                        timeout=_MONITOR_DB_ACQUIRE_TIMEOUT_SEC,
                    )
                except asyncio.TimeoutError as exc:
                    raise monitor_busy(
                        "advisory_admission", _MONITOR_DB_ACQUIRE_TIMEOUT_SEC
                    ) from exc
                advisory_slot = True

            acquire_timeout = (
                _MONITOR_DB_ACQUIRE_TIMEOUT_SEC
                if monitor
                else _GENERAL_DB_ACQUIRE_TIMEOUT_SEC
            )
            try:
                conn = await pool.acquire(timeout=acquire_timeout)
            except asyncio.TimeoutError as exc:
                if monitor:
                    raise monitor_busy(
                        "advisory_pool_acquire", acquire_timeout
                    ) from exc
                _log_pool_timeout(
                    "advisory_pool_acquire",
                    workload="foreground",
                    timeout_sec=acquire_timeout,
                )
                raise
            conn_token = _CURRENT_ADVISORY_CONN.set((current_task, conn))

        while True:
            acquired = bool(
                await conn.fetchval("SELECT pg_try_advisory_lock($1::bigint)", lock_id)
            )
            if acquired:
                try:
                    from app.services.monitor_diagnostics import record_wait

                    record_wait(
                        "lock_wait_ms",
                        (time.monotonic() - wait_started) * 1000,
                    )
                except Exception:
                    pass
                break
            if asyncio.get_running_loop().time() >= deadline:
                try:
                    from app.services.monitor_diagnostics import record_wait

                    record_wait(
                        "lock_wait_ms",
                        (time.monotonic() - wait_started) * 1000,
                    )
                except Exception:
                    pass
                if monitor:
                    raise monitor_busy("advisory_lock", timeout)
                raise TimeoutError(
                    f"PostgreSQL advisory lock timeout after {timeout:.1f}s for key={key!r}"
                )
            await asyncio.sleep(0.10)
        yield
    finally:
        active_body_exception = sys.exc_info()[1]
        body_exception_active = active_body_exception is not None
        body_cancelled = isinstance(active_body_exception, asyncio.CancelledError)
        cleanup_cancel: asyncio.CancelledError | None = None
        nested_unlock_error: Exception | None = None
        if conn is not None and acquired:
            try:
                await conn.execute("SELECT pg_advisory_unlock($1::bigint)", lock_id)
            except asyncio.CancelledError as exc:
                # A cancelled unlock must not skip connection/semaphore cleanup.
                # Terminating the session releases all PostgreSQL advisory locks.
                cleanup_cancel = exc
                _terminate_pool_connection(conn)
            except Exception as exc:
                log.exception("Failed to release PostgreSQL advisory lock %s", lock_id)
                _terminate_pool_connection(conn)
                # A nested lock shares the outer session.  Terminating that
                # session also releases the outer lock, so normal execution must
                # not continue under a lock that no longer exists.
                if not owns_conn:
                    nested_unlock_error = exc
        if owns_conn and conn_token is not None:
            _CURRENT_ADVISORY_CONN.reset(conn_token)
        if owns_conn and conn is not None and pool is not None:
            try:
                await _release_pg_connection_safely(
                    pool,
                    conn,
                    timeout_sec=_MONITOR_DB_ACQUIRE_TIMEOUT_SEC,
                    phase="advisory",
                    workload=("monitor" if monitor else "foreground"),
                    caller_cancelled=(body_cancelled or cleanup_cancel is not None),
                )
            except asyncio.CancelledError as exc:
                cleanup_cancel = cleanup_cancel or exc
        if advisory_slot and advisory_sem is not None:
            advisory_sem.release()
        if full_slot and full_sem is not None:
            full_sem.release()
        if cleanup_cancel is not None and not body_exception_active:
            raise cleanup_cancel
        if nested_unlock_error is not None and not body_exception_active:
            raise RuntimeError("PostgreSQL nested advisory unlock failed") from nested_unlock_error


SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
  telegram_id INTEGER PRIMARY KEY,
  username TEXT,
  is_admin INTEGER DEFAULT 0,
  is_active INTEGER DEFAULT 1,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS user_api_keys (
  user_id INTEGER NOT NULL,
  exchange TEXT NOT NULL DEFAULT 'bingx',
  api_key_encrypted TEXT NOT NULL,
  api_secret_encrypted TEXT NOT NULL,
  passphrase_encrypted TEXT,
  testnet INTEGER DEFAULT 0,
  enabled INTEGER DEFAULT 1,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(user_id, exchange)
);
CREATE TABLE IF NOT EXISTS api_key_quarantines (
  user_id INTEGER NOT NULL,
  exchange TEXT NOT NULL DEFAULT 'bingx',
  active INTEGER NOT NULL DEFAULT 1,
  error_code TEXT,
  error_message TEXT,
  endpoint TEXT,
  credential_fingerprint TEXT,
  incident_token TEXT,
  user_notify_claimed_at TEXT,
  admin_notify_claimed_at TEXT,
  hit_count INTEGER NOT NULL DEFAULT 1,
  first_detected_at TEXT DEFAULT CURRENT_TIMESTAMP,
  last_detected_at TEXT DEFAULT CURRENT_TIMESTAMP,
  user_notified_at TEXT,
  admin_notified_at TEXT,
  cleared_at TEXT,
  clear_reason TEXT,
  PRIMARY KEY(user_id, exchange)
);
CREATE TABLE IF NOT EXISTS user_settings (
  user_id INTEGER PRIMARY KEY,
  exchange TEXT DEFAULT 'bingx',
  mode TEXT DEFAULT 'preview',
  risk_per_trade_percent REAL DEFAULT 1.0,
  daily_risk_limit_percent REAL DEFAULT 10.0,
  max_open_trades INTEGER DEFAULT 10,
  max_portfolio_risk_percent REAL DEFAULT 10.0,
  exclude_be_trades_from_risk INTEGER DEFAULT 1,
  tp_limit TEXT DEFAULT 'all',
  tp_mode TEXT DEFAULT 'bell',
  be_after_tp1_enabled INTEGER DEFAULT 1,
  be_trigger_tp_index INTEGER DEFAULT 1,
  use_signal_tp_percents INTEGER DEFAULT 0,
  skip_trade_notifications_enabled INTEGER NOT NULL DEFAULT 0,
  manual_tp_percents TEXT DEFAULT '[]',
  whitelisted INTEGER DEFAULT 0,
  whitelisted_exchanges TEXT DEFAULT NULL,
  limit_ttl_hours INTEGER DEFAULT 24,
  limit_tp_invalidation_mode TEXT DEFAULT 'half',
  limit_policy_preset TEXT DEFAULT 'balanced'
);
CREATE TABLE IF NOT EXISTS signal_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_chat_id INTEGER,
  source_message_id INTEGER,
  source_title TEXT,
  signal_hash TEXT,
  signal_id TEXT,
  symbol TEXT,
  side TEXT,
  entry REAL,
  stop REAL,
  targets_json TEXT,
  raw_text TEXT,
  status TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS trade_executions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  trade_group_id INTEGER,
  signal_hash TEXT,
  user_id INTEGER,
  symbol TEXT,
  side TEXT,
  entry REAL,
  stop REAL,
  targets_json TEXT,
  tp_distribution_json TEXT,
  tp_distribution_source TEXT NOT NULL DEFAULT 'configured_pre_fill',
  tp_distribution_locked INTEGER NOT NULL DEFAULT 0,
  tp_distribution_version INTEGER NOT NULL DEFAULT 1,
  risk_percent REAL,
  equity_snapshot_usd REAL,
  planned_risk_usd REAL,
  initial_price_risk_usd REAL,
  initial_risk_percent_of_equity REAL,
  estimated_fee_risk_usd REAL,
  expected_loss_at_stop_usd REAL,
  planned_entry_qty REAL,
  stop_distance REAL,
  risk_snapshot_at TEXT,
  risk_snapshot_source TEXT,
  risk_snapshot_status TEXT NOT NULL DEFAULT 'missing',
  risk_snapshot_reason TEXT,
  qty REAL,
  leverage INTEGER,
  status TEXT,
  reason TEXT,
  exchange_order_ids_json TEXT DEFAULT '{}',
  outcome TEXT,
  realized_pnl REAL,
  close_type TEXT,
  closed_at TEXT,
  critical_next_check_at TEXT,
  critical_unchanged_count INTEGER NOT NULL DEFAULT 0,
  critical_reason_hash TEXT,
  critical_last_change_at TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS trade_groups (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  signal_hash TEXT NOT NULL,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  entry_type TEXT NOT NULL,
  planned_entry REAL DEFAULT 0,
  stop_price REAL NOT NULL,
  targets_json TEXT NOT NULL,
  source_chat_id INTEGER,
  source_message_id INTEGER,
  status TEXT DEFAULT 'active',
  last_price REAL,
  last_price_at TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS market_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  trade_group_id INTEGER NOT NULL,
  event_key TEXT NOT NULL,
  event_type TEXT NOT NULL,
  event_priority INTEGER NOT NULL DEFAULT 20,
  level_index INTEGER DEFAULT 0,
  trigger_price REAL NOT NULL,
  observed_price REAL,
  status TEXT DEFAULT 'pending',
  armed INTEGER NOT NULL DEFAULT 0,
  rearm_count INTEGER NOT NULL DEFAULT 0,
  last_rearmed_at TEXT,
  retrigger_requested INTEGER NOT NULL DEFAULT 0,
  retrigger_observed_price REAL,
  attempts INTEGER DEFAULT 0,
  next_attempt_at TEXT DEFAULT CURRENT_TIMESTAMP,
  last_error TEXT,
  lease_token TEXT,
  lease_generation INTEGER NOT NULL DEFAULT 0,
  lease_expires_at TEXT,
  outcome_kind TEXT,
  watch_lane TEXT NOT NULL DEFAULT 'critical',
  escalated_at TEXT,
  stuck_started_at TEXT,
  last_stuck_alert_at TEXT,
  last_stuck_reminder_at TEXT,
  stuck_reason TEXT,
  coalesced_event_keys TEXT,
  phase TEXT NOT NULL DEFAULT 'LEGACY',
  fast_attempts INTEGER NOT NULL DEFAULT 0,
  deep_attempts INTEGER NOT NULL DEFAULT 0,
  final_attempts INTEGER NOT NULL DEFAULT 0,
  unchanged_evidence_count INTEGER NOT NULL DEFAULT 0,
  evidence_fingerprint TEXT,
  evidence_snapshot_json TEXT,
  shadow_decision TEXT,
  shadow_reason TEXT,
  shadow_evaluated_at TEXT,
  shadow_version INTEGER NOT NULL DEFAULT 1,
  terminal_outcome TEXT,
  terminal_reason TEXT,
  terminal_at TEXT,
  manual_review_at TEXT,
  automation_enabled INTEGER NOT NULL DEFAULT 1,
  last_exchange_change_at TEXT,
  migration_state TEXT NOT NULL DEFAULT 'none',
  migration_version INTEGER NOT NULL DEFAULT 0,
  migration_started_at TEXT,
  migration_completed_at TEXT,
  migration_reason TEXT,
  manual_resolution TEXT,
  manual_resolution_admin_id INTEGER,
  manual_resolution_at TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(trade_group_id, event_key)
);
CREATE TABLE IF NOT EXISTS market_event_execution_states (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id INTEGER NOT NULL,
  execution_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  entry_state TEXT NOT NULL DEFAULT 'UNKNOWN',
  entry_order_id TEXT,
  entry_requested_qty REAL NOT NULL DEFAULT 0,
  entry_filled_qty REAL NOT NULL DEFAULT 0,
  entry_remaining_qty REAL NOT NULL DEFAULT 0,
  entry_exchange_status TEXT,
  tp_level_index INTEGER NOT NULL DEFAULT 1,
  tp_state TEXT NOT NULL DEFAULT 'UNKNOWN',
  tp_order_id TEXT,
  tp_expected_qty REAL NOT NULL DEFAULT 0,
  tp_filled_qty REAL NOT NULL DEFAULT 0,
  tp_remaining_qty REAL NOT NULL DEFAULT 0,
  tp_exchange_status TEXT,
  zero_exposure INTEGER NOT NULL DEFAULT 0,
  source_row_updated_at TEXT,
  evidence_fingerprint TEXT NOT NULL,
  evidence_json TEXT NOT NULL,
  observed_at TEXT DEFAULT CURRENT_TIMESTAMP,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(event_id, execution_id)
);
CREATE TABLE IF NOT EXISTS market_event_evidence_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id INTEGER NOT NULL,
  attempt_type TEXT NOT NULL DEFAULT 'shadow',
  attempt_number INTEGER NOT NULL DEFAULT 0,
  evidence_fingerprint TEXT NOT NULL,
  evidence_json TEXT NOT NULL,
  decision TEXT NOT NULL,
  reason TEXT,
  worker_id TEXT,
  lease_generation INTEGER NOT NULL DEFAULT 0,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS market_event_manual_actions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id INTEGER NOT NULL,
  trade_group_id INTEGER NOT NULL,
  admin_user_id INTEGER NOT NULL,
  action TEXT NOT NULL,
  comment TEXT,
  before_state_json TEXT NOT NULL,
  after_state_json TEXT NOT NULL,
  evidence_fingerprint TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS durable_notifications (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  dedup_key TEXT NOT NULL UNIQUE,
  user_id INTEGER NOT NULL,
  message_text TEXT NOT NULL,
  reply_markup_json TEXT,
  source TEXT DEFAULT 'monitor',
  status TEXT DEFAULT 'pending',
  attempts INTEGER DEFAULT 0,
  next_attempt_at TEXT DEFAULT CURRENT_TIMESTAMP,
  last_error TEXT,
  delivered_at TEXT,
  claim_token TEXT,
  claim_generation INTEGER NOT NULL DEFAULT 0,
  claim_expires_at TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS dedup (
  signal_hash TEXT,
  source_chat_id INTEGER,
  signal_id TEXT,
  user_id INTEGER,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(signal_hash, user_id)
);

CREATE TABLE IF NOT EXISTS user_agreements (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  username TEXT,
  agreement_version TEXT NOT NULL,
  agreement_hash TEXT NOT NULL,
  accepted_text TEXT,
  accepted_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""

PG_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
  telegram_id BIGINT PRIMARY KEY,
  username TEXT,
  is_admin INTEGER DEFAULT 0,
  is_active INTEGER DEFAULT 1,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS user_api_keys (
  user_id BIGINT NOT NULL,
  exchange TEXT NOT NULL DEFAULT 'bingx',
  api_key_encrypted TEXT NOT NULL,
  api_secret_encrypted TEXT NOT NULL,
  passphrase_encrypted TEXT,
  testnet INTEGER DEFAULT 0,
  enabled INTEGER DEFAULT 1,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY(user_id, exchange)
);
CREATE TABLE IF NOT EXISTS api_key_quarantines (
  user_id BIGINT NOT NULL,
  exchange TEXT NOT NULL DEFAULT 'bingx',
  active INTEGER NOT NULL DEFAULT 1,
  error_code TEXT,
  error_message TEXT,
  endpoint TEXT,
  credential_fingerprint TEXT,
  incident_token TEXT,
  user_notify_claimed_at TIMESTAMPTZ,
  admin_notify_claimed_at TIMESTAMPTZ,
  hit_count INTEGER NOT NULL DEFAULT 1,
  first_detected_at TIMESTAMPTZ DEFAULT NOW(),
  last_detected_at TIMESTAMPTZ DEFAULT NOW(),
  user_notified_at TIMESTAMPTZ,
  admin_notified_at TIMESTAMPTZ,
  cleared_at TIMESTAMPTZ,
  clear_reason TEXT,
  PRIMARY KEY(user_id, exchange)
);
CREATE TABLE IF NOT EXISTS user_settings (
  user_id BIGINT PRIMARY KEY,
  exchange TEXT DEFAULT 'bingx',
  mode TEXT DEFAULT 'preview',
  risk_per_trade_percent DOUBLE PRECISION DEFAULT 1.0,
  daily_risk_limit_percent DOUBLE PRECISION DEFAULT 10.0,
  max_open_trades INTEGER DEFAULT 10,
  max_portfolio_risk_percent DOUBLE PRECISION DEFAULT 10.0,
  exclude_be_trades_from_risk INTEGER DEFAULT 1,
  tp_limit TEXT DEFAULT 'all',
  tp_mode TEXT DEFAULT 'bell',
  be_after_tp1_enabled INTEGER DEFAULT 1,
  be_trigger_tp_index INTEGER DEFAULT 1,
  use_signal_tp_percents INTEGER DEFAULT 0,
  skip_trade_notifications_enabled INTEGER NOT NULL DEFAULT 0,
  manual_tp_percents TEXT DEFAULT '[]',
  whitelisted INTEGER DEFAULT 0,
  whitelisted_exchanges TEXT DEFAULT NULL,
  limit_ttl_hours INTEGER DEFAULT 24,
  limit_tp_invalidation_mode TEXT DEFAULT 'half',
  limit_policy_preset TEXT DEFAULT 'balanced'
);
CREATE TABLE IF NOT EXISTS signal_events (
  id BIGSERIAL PRIMARY KEY,
  source_chat_id BIGINT,
  source_message_id BIGINT,
  source_title TEXT,
  signal_hash TEXT,
  signal_id TEXT,
  symbol TEXT,
  side TEXT,
  entry DOUBLE PRECISION,
  stop DOUBLE PRECISION,
  targets_json TEXT,
  raw_text TEXT,
  status TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS trade_executions (
  id BIGSERIAL PRIMARY KEY,
  trade_group_id BIGINT,
  signal_hash TEXT,
  user_id BIGINT,
  symbol TEXT,
  side TEXT,
  entry DOUBLE PRECISION,
  stop DOUBLE PRECISION,
  targets_json TEXT,
  tp_distribution_json TEXT,
  tp_distribution_source TEXT NOT NULL DEFAULT 'configured_pre_fill',
  tp_distribution_locked INTEGER NOT NULL DEFAULT 0,
  tp_distribution_version INTEGER NOT NULL DEFAULT 1,
  risk_percent DOUBLE PRECISION,
  equity_snapshot_usd DOUBLE PRECISION,
  planned_risk_usd DOUBLE PRECISION,
  initial_price_risk_usd DOUBLE PRECISION,
  initial_risk_percent_of_equity DOUBLE PRECISION,
  estimated_fee_risk_usd DOUBLE PRECISION,
  expected_loss_at_stop_usd DOUBLE PRECISION,
  planned_entry_qty DOUBLE PRECISION,
  stop_distance DOUBLE PRECISION,
  risk_snapshot_at TIMESTAMPTZ,
  risk_snapshot_source TEXT,
  risk_snapshot_status TEXT NOT NULL DEFAULT 'missing',
  risk_snapshot_reason TEXT,
  qty DOUBLE PRECISION,
  leverage INTEGER,
  status TEXT,
  reason TEXT,
  exchange_order_ids_json TEXT DEFAULT '{}',
  outcome TEXT,
  realized_pnl DOUBLE PRECISION,
  close_type TEXT,
  closed_at TIMESTAMPTZ,
  critical_next_check_at TIMESTAMPTZ,
  critical_unchanged_count INTEGER NOT NULL DEFAULT 0,
  critical_reason_hash TEXT,
  critical_last_change_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS trade_groups (
  id BIGSERIAL PRIMARY KEY,
  signal_hash TEXT NOT NULL,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  entry_type TEXT NOT NULL,
  planned_entry DOUBLE PRECISION DEFAULT 0,
  stop_price DOUBLE PRECISION NOT NULL,
  targets_json TEXT NOT NULL,
  source_chat_id BIGINT,
  source_message_id BIGINT,
  status TEXT DEFAULT 'active',
  last_price DOUBLE PRECISION,
  last_price_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS market_events (
  id BIGSERIAL PRIMARY KEY,
  trade_group_id BIGINT NOT NULL,
  event_key TEXT NOT NULL,
  event_type TEXT NOT NULL,
  event_priority INTEGER NOT NULL DEFAULT 20,
  level_index INTEGER DEFAULT 0,
  trigger_price DOUBLE PRECISION NOT NULL,
  observed_price DOUBLE PRECISION,
  status TEXT DEFAULT 'pending',
  armed INTEGER NOT NULL DEFAULT 0,
  rearm_count INTEGER NOT NULL DEFAULT 0,
  last_rearmed_at TIMESTAMPTZ,
  retrigger_requested INTEGER NOT NULL DEFAULT 0,
  retrigger_observed_price DOUBLE PRECISION,
  attempts INTEGER DEFAULT 0,
  next_attempt_at TIMESTAMPTZ DEFAULT NOW(),
  last_error TEXT,
  lease_token TEXT,
  lease_generation INTEGER NOT NULL DEFAULT 0,
  lease_expires_at TIMESTAMPTZ,
  outcome_kind TEXT,
  watch_lane TEXT NOT NULL DEFAULT 'critical',
  escalated_at TIMESTAMPTZ,
  stuck_started_at TIMESTAMPTZ,
  last_stuck_alert_at TIMESTAMPTZ,
  last_stuck_reminder_at TIMESTAMPTZ,
  stuck_reason TEXT,
  coalesced_event_keys TEXT,
  phase TEXT NOT NULL DEFAULT 'LEGACY',
  fast_attempts INTEGER NOT NULL DEFAULT 0,
  deep_attempts INTEGER NOT NULL DEFAULT 0,
  final_attempts INTEGER NOT NULL DEFAULT 0,
  unchanged_evidence_count INTEGER NOT NULL DEFAULT 0,
  evidence_fingerprint TEXT,
  evidence_snapshot_json TEXT,
  shadow_decision TEXT,
  shadow_reason TEXT,
  shadow_evaluated_at TIMESTAMPTZ,
  shadow_version INTEGER NOT NULL DEFAULT 1,
  terminal_outcome TEXT,
  terminal_reason TEXT,
  terminal_at TIMESTAMPTZ,
  manual_review_at TIMESTAMPTZ,
  automation_enabled INTEGER NOT NULL DEFAULT 1,
  last_exchange_change_at TIMESTAMPTZ,
  migration_state TEXT NOT NULL DEFAULT 'none',
  migration_version INTEGER NOT NULL DEFAULT 0,
  migration_started_at TIMESTAMPTZ,
  migration_completed_at TIMESTAMPTZ,
  migration_reason TEXT,
  manual_resolution TEXT,
  manual_resolution_admin_id BIGINT,
  manual_resolution_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(trade_group_id, event_key)
);
CREATE TABLE IF NOT EXISTS market_event_execution_states (
  id BIGSERIAL PRIMARY KEY,
  event_id BIGINT NOT NULL,
  execution_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  entry_state TEXT NOT NULL DEFAULT 'UNKNOWN',
  entry_order_id TEXT,
  entry_requested_qty DOUBLE PRECISION NOT NULL DEFAULT 0,
  entry_filled_qty DOUBLE PRECISION NOT NULL DEFAULT 0,
  entry_remaining_qty DOUBLE PRECISION NOT NULL DEFAULT 0,
  entry_exchange_status TEXT,
  tp_level_index INTEGER NOT NULL DEFAULT 1,
  tp_state TEXT NOT NULL DEFAULT 'UNKNOWN',
  tp_order_id TEXT,
  tp_expected_qty DOUBLE PRECISION NOT NULL DEFAULT 0,
  tp_filled_qty DOUBLE PRECISION NOT NULL DEFAULT 0,
  tp_remaining_qty DOUBLE PRECISION NOT NULL DEFAULT 0,
  tp_exchange_status TEXT,
  zero_exposure INTEGER NOT NULL DEFAULT 0,
  source_row_updated_at TIMESTAMPTZ,
  evidence_fingerprint TEXT NOT NULL,
  evidence_json TEXT NOT NULL,
  observed_at TIMESTAMPTZ DEFAULT NOW(),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(event_id, execution_id)
);
CREATE TABLE IF NOT EXISTS market_event_evidence_history (
  id BIGSERIAL PRIMARY KEY,
  event_id BIGINT NOT NULL,
  attempt_type TEXT NOT NULL DEFAULT 'shadow',
  attempt_number INTEGER NOT NULL DEFAULT 0,
  evidence_fingerprint TEXT NOT NULL,
  evidence_json TEXT NOT NULL,
  decision TEXT NOT NULL,
  reason TEXT,
  worker_id TEXT,
  lease_generation INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS market_event_manual_actions (
  id BIGSERIAL PRIMARY KEY,
  event_id BIGINT NOT NULL,
  trade_group_id BIGINT NOT NULL,
  admin_user_id BIGINT NOT NULL,
  action TEXT NOT NULL,
  comment TEXT,
  before_state_json TEXT NOT NULL,
  after_state_json TEXT NOT NULL,
  evidence_fingerprint TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS durable_notifications (
  id BIGSERIAL PRIMARY KEY,
  dedup_key TEXT NOT NULL UNIQUE,
  user_id BIGINT NOT NULL,
  message_text TEXT NOT NULL,
  reply_markup_json TEXT,
  source TEXT DEFAULT 'monitor',
  status TEXT DEFAULT 'pending',
  attempts INTEGER DEFAULT 0,
  next_attempt_at TIMESTAMPTZ DEFAULT NOW(),
  last_error TEXT,
  delivered_at TIMESTAMPTZ,
  claim_token TEXT,
  claim_generation INTEGER NOT NULL DEFAULT 0,
  claim_expires_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS dedup (
  signal_hash TEXT,
  source_chat_id BIGINT,
  signal_id TEXT,
  user_id BIGINT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY(signal_hash, user_id)
);

CREATE TABLE IF NOT EXISTS user_agreements (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  username TEXT,
  agreement_version TEXT NOT NULL,
  agreement_hash TEXT NOT NULL,
  accepted_text TEXT,
  accepted_at TIMESTAMPTZ DEFAULT NOW()
);
"""


SIGNAL_ANALYTICS_SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS signal_analytics_signals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ingest_event_id TEXT NOT NULL,
  dedup_key TEXT NOT NULL UNIQUE,
  content_fingerprint TEXT NOT NULL,
  source_chat_id INTEGER NOT NULL,
  first_source_message_id INTEGER,
  last_source_message_id INTEGER,
  source_title TEXT,
  sender_chat_id INTEGER,
  sender_chat_title TEXT,
  published_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  signal_id_text TEXT,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  order_type TEXT NOT NULL,
  source_format TEXT NOT NULL,
  timeframe TEXT,
  strategy TEXT,
  source_leverage INTEGER,
  entry_low TEXT NOT NULL,
  entry_high TEXT NOT NULL,
  entry_reference TEXT NOT NULL,
  stop_price TEXT NOT NULL,
  targets_json TEXT NOT NULL,
  target_percents_json TEXT NOT NULL DEFAULT '[]',
  target_percents_source TEXT NOT NULL DEFAULT 'source_or_empty',
  recovery_attempts INTEGER NOT NULL DEFAULT 0,
  recovery_next_attempt_at TEXT,
  recovery_processing_started_at TEXT,
  recovery_lease_token TEXT,
  recovery_last_error TEXT,
  recovery_cursor_at TEXT,
  raw_text TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'shadow_received',
  duplicate_count INTEGER NOT NULL DEFAULT 1,
  trade_group_id INTEGER,
  needs_recovery INTEGER NOT NULL DEFAULT 0,
  tracking_started_at TEXT,
  expiry_at TEXT,
  zone_touched_at TEXT,
  activated_at TEXT,
  activated_price TEXT,
  max_tp_index INTEGER NOT NULL DEFAULT 0,
  be_trigger_tp_index INTEGER NOT NULL DEFAULT 0,
  be_armed_at TEXT,
  completed_at TEXT,
  terminal_reason TEXT,
  ambiguous_reason TEXT,
  last_observed_at TEXT,
  last_observed_price TEXT,
  state_version INTEGER NOT NULL DEFAULT 0,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS signal_analytics_source_status_published_idx
  ON signal_analytics_signals(source_chat_id,status,published_at);
CREATE INDEX IF NOT EXISTS signal_analytics_symbol_status_published_idx
  ON signal_analytics_signals(symbol,status,published_at);
CREATE INDEX IF NOT EXISTS signal_analytics_fingerprint_idx
  ON signal_analytics_signals(source_chat_id,content_fingerprint);
CREATE INDEX IF NOT EXISTS signal_analytics_fingerprint_published_v2_idx
  ON signal_analytics_signals(source_chat_id,content_fingerprint,published_at);
CREATE TABLE IF NOT EXISTS signal_analytics_observations (
  ingest_event_id TEXT PRIMARY KEY,
  dedup_key TEXT NOT NULL,
  source_message_id INTEGER,
  observed_at TEXT NOT NULL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS signal_analytics_observations_dedup_idx
  ON signal_analytics_observations(dedup_key,observed_at);

CREATE TABLE IF NOT EXISTS signal_analytics_level_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  signal_id INTEGER NOT NULL,
  event_key TEXT NOT NULL,
  event_type TEXT NOT NULL,
  level_index INTEGER NOT NULL DEFAULT 0,
  observed_at TEXT NOT NULL,
  observed_price TEXT NOT NULL,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(signal_id,event_key)
);
CREATE INDEX IF NOT EXISTS signal_analytics_level_events_signal_idx
  ON signal_analytics_level_events(signal_id,observed_at);
"""

SIGNAL_ANALYTICS_PG_SCHEMA = """
CREATE TABLE IF NOT EXISTS signal_analytics_signals (
  id BIGSERIAL PRIMARY KEY,
  ingest_event_id TEXT NOT NULL,
  dedup_key TEXT NOT NULL UNIQUE,
  content_fingerprint TEXT NOT NULL,
  source_chat_id BIGINT NOT NULL,
  first_source_message_id BIGINT,
  last_source_message_id BIGINT,
  source_title TEXT,
  sender_chat_id BIGINT,
  sender_chat_title TEXT,
  published_at TIMESTAMPTZ NOT NULL,
  last_seen_at TIMESTAMPTZ NOT NULL,
  signal_id_text TEXT,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  order_type TEXT NOT NULL,
  source_format TEXT NOT NULL,
  timeframe TEXT,
  strategy TEXT,
  source_leverage INTEGER,
  entry_low NUMERIC NOT NULL,
  entry_high NUMERIC NOT NULL,
  entry_reference NUMERIC NOT NULL,
  stop_price NUMERIC NOT NULL,
  targets_json TEXT NOT NULL,
  target_percents_json TEXT NOT NULL DEFAULT '[]',
  target_percents_source TEXT NOT NULL DEFAULT 'source_or_empty',
  recovery_attempts INTEGER NOT NULL DEFAULT 0,
  recovery_next_attempt_at TIMESTAMPTZ,
  recovery_processing_started_at TIMESTAMPTZ,
  recovery_lease_token TEXT,
  recovery_last_error TEXT,
  recovery_cursor_at TIMESTAMPTZ,
  raw_text TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'shadow_received',
  duplicate_count INTEGER NOT NULL DEFAULT 1,
  trade_group_id BIGINT,
  needs_recovery INTEGER NOT NULL DEFAULT 0,
  tracking_started_at TIMESTAMPTZ,
  expiry_at TIMESTAMPTZ,
  zone_touched_at TIMESTAMPTZ,
  activated_at TIMESTAMPTZ,
  activated_price NUMERIC,
  max_tp_index INTEGER NOT NULL DEFAULT 0,
  be_trigger_tp_index INTEGER NOT NULL DEFAULT 0,
  be_armed_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  terminal_reason TEXT,
  ambiguous_reason TEXT,
  last_observed_at TIMESTAMPTZ,
  last_observed_price NUMERIC,
  state_version INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS signal_analytics_source_status_published_pg_idx
  ON signal_analytics_signals(source_chat_id,status,published_at);
CREATE INDEX IF NOT EXISTS signal_analytics_symbol_status_published_pg_idx
  ON signal_analytics_signals(symbol,status,published_at);
CREATE INDEX IF NOT EXISTS signal_analytics_fingerprint_pg_idx
  ON signal_analytics_signals(source_chat_id,content_fingerprint);
CREATE INDEX IF NOT EXISTS signal_analytics_fingerprint_published_v2_pg_idx
  ON signal_analytics_signals(source_chat_id,content_fingerprint,published_at);
CREATE TABLE IF NOT EXISTS signal_analytics_observations (
  ingest_event_id TEXT PRIMARY KEY,
  dedup_key TEXT NOT NULL,
  source_message_id BIGINT,
  observed_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS signal_analytics_observations_dedup_pg_idx
  ON signal_analytics_observations(dedup_key,observed_at);

CREATE TABLE IF NOT EXISTS signal_analytics_level_events (
  id BIGSERIAL PRIMARY KEY,
  signal_id BIGINT NOT NULL,
  event_key TEXT NOT NULL,
  event_type TEXT NOT NULL,
  level_index INTEGER NOT NULL DEFAULT 0,
  observed_at TIMESTAMPTZ NOT NULL,
  observed_price NUMERIC NOT NULL,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(signal_id,event_key)
);
CREATE INDEX IF NOT EXISTS signal_analytics_level_events_signal_pg_idx
  ON signal_analytics_level_events(signal_id,observed_at);
"""

FINANCIAL_RECONCILIATION_SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS financial_reconciliation_jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  execution_id INTEGER NOT NULL UNIQUE,
  user_id INTEGER NOT NULL,
  exchange TEXT NOT NULL DEFAULT 'bingx',
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  close_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  strategy_gross_pnl TEXT NOT NULL DEFAULT '0',
  exchange_gross_pnl TEXT,
  total_trading_fee TEXT,
  net_pnl_after_trading_fee TEXT,
  fee_asset TEXT,
  expected_order_count INTEGER NOT NULL DEFAULT 0,
  confirmed_order_count INTEGER NOT NULL DEFAULT 0,
  fill_count INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT DEFAULT CURRENT_TIMESTAMP,
  deadline_at TEXT,
  processing_started_at TEXT,
  lease_token TEXT,
  last_error TEXT,
  terminal_at TEXT,
  confirmed_at TEXT,
  resolved_at TEXT,
  notification_dedup_key TEXT NOT NULL UNIQUE,
  notification_status TEXT NOT NULL DEFAULT 'pending',
  notified_at TEXT,
  result_version INTEGER NOT NULL DEFAULT 1,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS financial_reconciliation_jobs_status_due_idx
  ON financial_reconciliation_jobs(status,next_attempt_at);
CREATE INDEX IF NOT EXISTS financial_reconciliation_jobs_user_created_idx
  ON financial_reconciliation_jobs(user_id,created_at);
CREATE INDEX IF NOT EXISTS financial_reconciliation_jobs_symbol_created_idx
  ON financial_reconciliation_jobs(symbol,created_at);

CREATE TABLE IF NOT EXISTS financial_reconciliation_orders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id INTEGER NOT NULL,
  execution_id INTEGER NOT NULL,
  order_key TEXT NOT NULL,
  exchange_order_id TEXT,
  client_order_id TEXT,
  role TEXT NOT NULL,
  tp_index INTEGER NOT NULL DEFAULT 0,
  required INTEGER NOT NULL DEFAULT 1,
  expected_qty TEXT,
  status TEXT NOT NULL DEFAULT 'expected',
  confirmed_fill_count INTEGER NOT NULL DEFAULT 0,
  confirmed_qty TEXT,
  confirmed_fee TEXT,
  last_checked_at TEXT,
  last_error TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(job_id,order_key)
);
CREATE INDEX IF NOT EXISTS financial_reconciliation_orders_job_status_idx
  ON financial_reconciliation_orders(job_id,status);
CREATE INDEX IF NOT EXISTS financial_reconciliation_orders_exchange_id_idx
  ON financial_reconciliation_orders(exchange_order_id);
CREATE UNIQUE INDEX IF NOT EXISTS financial_reconciliation_orders_job_exchange_unique_idx
  ON financial_reconciliation_orders(job_id,exchange_order_id);
CREATE UNIQUE INDEX IF NOT EXISTS financial_reconciliation_orders_job_client_unique_idx
  ON financial_reconciliation_orders(job_id,client_order_id);

CREATE TABLE IF NOT EXISTS financial_reconciliation_fills (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id INTEGER NOT NULL,
  execution_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  exchange TEXT NOT NULL DEFAULT 'bingx',
  trade_id TEXT NOT NULL,
  order_id TEXT NOT NULL,
  order_key TEXT NOT NULL,
  role TEXT NOT NULL,
  tp_index INTEGER NOT NULL DEFAULT 0,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  price TEXT NOT NULL,
  qty TEXT NOT NULL,
  realized_pnl TEXT NOT NULL DEFAULT '0',
  fee TEXT NOT NULL DEFAULT '0',
  fee_asset TEXT,
  fill_time TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(exchange,user_id,trade_id)
);
CREATE INDEX IF NOT EXISTS financial_reconciliation_fills_job_time_idx
  ON financial_reconciliation_fills(job_id,fill_time);
CREATE INDEX IF NOT EXISTS financial_reconciliation_fills_order_idx
  ON financial_reconciliation_fills(order_id,fill_time);
"""

FINANCIAL_RECONCILIATION_PG_SCHEMA = """
CREATE TABLE IF NOT EXISTS financial_reconciliation_jobs (
  id BIGSERIAL PRIMARY KEY,
  execution_id BIGINT NOT NULL UNIQUE,
  user_id BIGINT NOT NULL,
  exchange TEXT NOT NULL DEFAULT 'bingx',
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  close_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  strategy_gross_pnl NUMERIC NOT NULL DEFAULT 0,
  exchange_gross_pnl NUMERIC,
  total_trading_fee NUMERIC,
  net_pnl_after_trading_fee NUMERIC,
  fee_asset TEXT,
  expected_order_count INTEGER NOT NULL DEFAULT 0,
  confirmed_order_count INTEGER NOT NULL DEFAULT 0,
  fill_count INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ DEFAULT NOW(),
  deadline_at TIMESTAMPTZ,
  processing_started_at TIMESTAMPTZ,
  lease_token TEXT,
  last_error TEXT,
  terminal_at TIMESTAMPTZ,
  confirmed_at TIMESTAMPTZ,
  resolved_at TIMESTAMPTZ,
  notification_dedup_key TEXT NOT NULL UNIQUE,
  notification_status TEXT NOT NULL DEFAULT 'pending',
  notified_at TIMESTAMPTZ,
  result_version INTEGER NOT NULL DEFAULT 1,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS financial_reconciliation_jobs_status_due_pg_idx
  ON financial_reconciliation_jobs(status,next_attempt_at);
CREATE INDEX IF NOT EXISTS financial_reconciliation_jobs_user_created_pg_idx
  ON financial_reconciliation_jobs(user_id,created_at);
CREATE INDEX IF NOT EXISTS financial_reconciliation_jobs_symbol_created_pg_idx
  ON financial_reconciliation_jobs(symbol,created_at);

CREATE TABLE IF NOT EXISTS financial_reconciliation_orders (
  id BIGSERIAL PRIMARY KEY,
  job_id BIGINT NOT NULL,
  execution_id BIGINT NOT NULL,
  order_key TEXT NOT NULL,
  exchange_order_id TEXT,
  client_order_id TEXT,
  role TEXT NOT NULL,
  tp_index INTEGER NOT NULL DEFAULT 0,
  required INTEGER NOT NULL DEFAULT 1,
  expected_qty NUMERIC,
  status TEXT NOT NULL DEFAULT 'expected',
  confirmed_fill_count INTEGER NOT NULL DEFAULT 0,
  confirmed_qty NUMERIC,
  confirmed_fee NUMERIC,
  last_checked_at TIMESTAMPTZ,
  last_error TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(job_id,order_key)
);
CREATE INDEX IF NOT EXISTS financial_reconciliation_orders_job_status_pg_idx
  ON financial_reconciliation_orders(job_id,status);
CREATE INDEX IF NOT EXISTS financial_reconciliation_orders_exchange_id_pg_idx
  ON financial_reconciliation_orders(exchange_order_id);
CREATE UNIQUE INDEX IF NOT EXISTS financial_reconciliation_orders_job_exchange_unique_pg_idx
  ON financial_reconciliation_orders(job_id,exchange_order_id);
CREATE UNIQUE INDEX IF NOT EXISTS financial_reconciliation_orders_job_client_unique_pg_idx
  ON financial_reconciliation_orders(job_id,client_order_id);

CREATE TABLE IF NOT EXISTS financial_reconciliation_fills (
  id BIGSERIAL PRIMARY KEY,
  job_id BIGINT NOT NULL,
  execution_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  exchange TEXT NOT NULL DEFAULT 'bingx',
  trade_id TEXT NOT NULL,
  order_id TEXT NOT NULL,
  order_key TEXT NOT NULL,
  role TEXT NOT NULL,
  tp_index INTEGER NOT NULL DEFAULT 0,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  price NUMERIC NOT NULL,
  qty NUMERIC NOT NULL,
  realized_pnl NUMERIC NOT NULL DEFAULT 0,
  fee NUMERIC NOT NULL DEFAULT 0,
  fee_asset TEXT,
  fill_time TIMESTAMPTZ NOT NULL,
  fingerprint TEXT NOT NULL,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(exchange,user_id,trade_id)
);
CREATE INDEX IF NOT EXISTS financial_reconciliation_fills_job_time_pg_idx
  ON financial_reconciliation_fills(job_id,fill_time);
CREATE INDEX IF NOT EXISTS financial_reconciliation_fills_order_pg_idx
  ON financial_reconciliation_fills(order_id,fill_time);
"""



def is_postgres() -> bool:
    return bool(
        (get_settings().DATABASE_URL or "").startswith(("postgres://", "postgresql://"))
    )


async def _sqlite_migrate_signal_analytics(conn: aiosqlite.Connection) -> None:
    """Idempotently upgrade an existing Stage-1 analytics table."""

    cursor = await conn.execute("PRAGMA table_info(signal_analytics_signals)")
    columns = {str(row[1]) for row in await cursor.fetchall()}
    missing = {
        "tracking_started_at": "TEXT",
        "expiry_at": "TEXT",
        "zone_touched_at": "TEXT",
        "activated_at": "TEXT",
        "activated_price": "TEXT",
        "max_tp_index": "INTEGER NOT NULL DEFAULT 0",
        "be_trigger_tp_index": "INTEGER NOT NULL DEFAULT 0",
        "be_armed_at": "TEXT",
        "completed_at": "TEXT",
        "terminal_reason": "TEXT",
        "ambiguous_reason": "TEXT",
        "last_observed_at": "TEXT",
        "last_observed_price": "TEXT",
        "state_version": "INTEGER NOT NULL DEFAULT 0",
    }
    for column, ddl in missing.items():
        if column not in columns:
            await conn.execute(
                f"ALTER TABLE signal_analytics_signals ADD COLUMN {column} {ddl}"
            )
    await conn.execute(
        "UPDATE signal_analytics_signals SET max_tp_index=0 "
        "WHERE max_tp_index IS NULL OR max_tp_index < 0"
    )
    await conn.execute(
        "UPDATE signal_analytics_signals SET be_trigger_tp_index=0 "
        "WHERE be_trigger_tp_index IS NULL OR be_trigger_tp_index < 0"
    )
    await conn.execute(
        "UPDATE signal_analytics_signals SET state_version=0 "
        "WHERE state_version IS NULL OR state_version < 0"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS signal_analytics_status_expiry_idx "
        "ON signal_analytics_signals(status,expiry_at)"
    )
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS signal_analytics_level_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          signal_id INTEGER NOT NULL,
          event_key TEXT NOT NULL,
          event_type TEXT NOT NULL,
          level_index INTEGER NOT NULL DEFAULT 0,
          observed_at TEXT NOT NULL,
          observed_price TEXT NOT NULL,
          metadata_json TEXT NOT NULL DEFAULT '{}',
          created_at TEXT DEFAULT CURRENT_TIMESTAMP,
          UNIQUE(signal_id,event_key)
        )
    """)
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS signal_analytics_level_events_signal_idx "
        "ON signal_analytics_level_events(signal_id,observed_at)"
    )


async def _pg_migrate_signal_analytics(conn: Any) -> None:
    """Idempotently upgrade an existing PostgreSQL Stage-1 analytics table."""

    statements = (
        "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS tracking_started_at TIMESTAMPTZ",
        "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS expiry_at TIMESTAMPTZ",
        "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS zone_touched_at TIMESTAMPTZ",
        "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS activated_at TIMESTAMPTZ",
        "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS activated_price NUMERIC",
        "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS max_tp_index INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS be_trigger_tp_index INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS be_armed_at TIMESTAMPTZ",
        "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ",
        "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS terminal_reason TEXT",
        "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS ambiguous_reason TEXT",
        "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS last_observed_at TIMESTAMPTZ",
        "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS last_observed_price NUMERIC",
        "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS state_version INTEGER NOT NULL DEFAULT 0",
    )
    for statement in statements:
        await conn.execute(statement)
    await conn.execute(
        "UPDATE signal_analytics_signals SET max_tp_index=0 "
        "WHERE max_tp_index IS NULL OR max_tp_index < 0"
    )
    await conn.execute(
        "UPDATE signal_analytics_signals SET be_trigger_tp_index=0 "
        "WHERE be_trigger_tp_index IS NULL OR be_trigger_tp_index < 0"
    )
    await conn.execute(
        "UPDATE signal_analytics_signals SET state_version=0 "
        "WHERE state_version IS NULL OR state_version < 0"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS signal_analytics_status_expiry_pg_idx "
        "ON signal_analytics_signals(status,expiry_at)"
    )
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS signal_analytics_level_events (
          id BIGSERIAL PRIMARY KEY,
          signal_id BIGINT NOT NULL,
          event_key TEXT NOT NULL,
          event_type TEXT NOT NULL,
          level_index INTEGER NOT NULL DEFAULT 0,
          observed_at TIMESTAMPTZ NOT NULL,
          observed_price NUMERIC NOT NULL,
          metadata_json TEXT NOT NULL DEFAULT '{}',
          created_at TIMESTAMPTZ DEFAULT NOW(),
          UNIQUE(signal_id,event_key)
        )
    """)
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS signal_analytics_level_events_signal_pg_idx "
        "ON signal_analytics_level_events(signal_id,observed_at)"
    )


async def init_db() -> None:
    settings = get_settings()
    # init_db may run more than once in tests or controlled restarts. Do not let
    # the process-local fast path assume users exist in a freshly selected DB.
    _ENSURED_USERS.clear()
    logging.info("DB backend: %s | %s", storage_backend(), persistence_hint())
    if is_postgres():
        pool = await _ensure_pg_pool()
        async with pool.acquire(timeout=_GENERAL_DB_ACQUIRE_TIMEOUT_SEC) as conn:
            await conn.execute(PG_SCHEMA)
            await _pg_migrate(conn)
            try:
                await conn.execute(SIGNAL_ANALYTICS_PG_SCHEMA)
                await _pg_migrate_signal_analytics(conn)
            except Exception as exc:
                # Analytics is an optional observer. A schema mismatch or
                # permission issue must not prevent the trading core from
                # starting; the bounded writer will remain fail-open and expose
                # its own DB error counters if the feature is enabled.
                logging.exception(
                    "SIGNAL_ANALYTICS_SCHEMA_INIT_FAILED_FAIL_OPEN backend=postgres "
                    "error=%s",
                    f"{type(exc).__name__}: {exc}",
                )
            try:
                await conn.execute(FINANCIAL_RECONCILIATION_PG_SCHEMA)
            except Exception as exc:
                # Financial reconciliation is deferred accounting only. Schema
                # failure must never block ENTRY/STOP/TP/BE or lifecycle startup.
                logging.exception(
                    "FINANCIAL_RECONCILIATION_SCHEMA_INIT_FAILED_FAIL_OPEN "
                    "backend=postgres error=%s",
                    f"{type(exc).__name__}: {exc}",
                )
            try:
                # Existing PostgreSQL tables must receive additive columns
                # before the schema creates indexes that reference them.
                await pg_preflight_statistics_v2_columns(conn)
                await conn.execute(STATISTICS_V2_PG_SCHEMA)
                await pg_migrate_statistics_v2(
                    conn,
                    source_version=__version__,
                    seed_legacy_period=True,
                )
            except Exception as exc:
                # Step-3 statistics storage is an additive observer projection.
                # It is deliberately fail-open and must never block trading startup.
                logging.exception(
                    "STATISTICS_V2_SCHEMA_INIT_FAILED_FAIL_OPEN "
                    "backend=postgres error=%s",
                    f"{type(exc).__name__}: {exc}",
                )
        return
    path = settings.DATABASE_PATH
    if path != ":memory":
        dirname = os.path.dirname(path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
    async with aiosqlite.connect(path) as sqldb:
        await sqldb.executescript(SQLITE_SCHEMA)
        await _sqlite_migrate(sqldb)
        await sqldb.commit()
        try:
            await sqldb.executescript(
                "BEGIN;\n" + SIGNAL_ANALYTICS_SQLITE_SCHEMA + "\nCOMMIT;"
            )
            await _sqlite_migrate_signal_analytics(sqldb)
            await sqldb.commit()
        except Exception as exc:
            try:
                await sqldb.rollback()
            except Exception:
                pass
            logging.exception(
                "SIGNAL_ANALYTICS_SCHEMA_INIT_FAILED_FAIL_OPEN backend=sqlite "
                "error=%s",
                f"{type(exc).__name__}: {exc}",
            )
        try:
            await sqldb.executescript(
                "BEGIN;\n" + FINANCIAL_RECONCILIATION_SQLITE_SCHEMA + "\nCOMMIT;"
            )
            await sqldb.commit()
        except Exception as exc:
            try:
                await sqldb.rollback()
            except Exception:
                pass
            logging.exception(
                "FINANCIAL_RECONCILIATION_SCHEMA_INIT_FAILED_FAIL_OPEN "
                "backend=sqlite error=%s",
                f"{type(exc).__name__}: {exc}",
            )
        try:
            await sqldb.executescript(
                "BEGIN;\n" + STATISTICS_V2_SQLITE_SCHEMA + "\nCOMMIT;"
            )
            await sqlite_migrate_statistics_v2(
                sqldb,
                source_version=__version__,
                seed_legacy_period=True,
            )
            await sqldb.commit()
        except Exception as exc:
            try:
                await sqldb.rollback()
            except Exception:
                pass
            logging.exception(
                "STATISTICS_V2_SCHEMA_INIT_FAILED_FAIL_OPEN "
                "backend=sqlite error=%s",
                f"{type(exc).__name__}: {exc}",
            )


async def _sqlite_migrate(conn: aiosqlite.Connection) -> None:
    cur = await conn.execute("PRAGMA table_info(user_settings)")
    cols = {row[1] for row in await cur.fetchall()}
    missing_columns = {
        "exchange": "TEXT DEFAULT 'bingx'",
        "mode": "TEXT DEFAULT 'preview'",
        "risk_per_trade_percent": "REAL DEFAULT 1.0",
        "daily_risk_limit_percent": "REAL DEFAULT 10.0",
        "max_open_trades": "INTEGER DEFAULT 10",
        "max_portfolio_risk_percent": "REAL DEFAULT 10.0",
        "exclude_be_trades_from_risk": "INTEGER DEFAULT 1",
        "tp_limit": "TEXT DEFAULT 'all'",
        "tp_mode": "TEXT DEFAULT 'bell'",
        "be_after_tp1_enabled": "INTEGER DEFAULT 1",
        "be_trigger_tp_index": "INTEGER DEFAULT 1",
        "use_signal_tp_percents": "INTEGER DEFAULT 0",
        "skip_trade_notifications_enabled": "INTEGER NOT NULL DEFAULT 0",
        "manual_tp_percents": "TEXT DEFAULT '[]'",
        "whitelisted": "INTEGER DEFAULT 0",
        # BingX-only whitelist marker. NULL means no access; "bingx"/"all" means access.
        "whitelisted_exchanges": "TEXT DEFAULT NULL",
        "limit_ttl_hours": "INTEGER DEFAULT 24",
        "limit_tp_invalidation_mode": "TEXT DEFAULT 'half'",
        "limit_policy_preset": "TEXT DEFAULT 'balanced'",
    }
    added_be_trigger = False
    for col, ddl in missing_columns.items():
        if col not in cols:
            await conn.execute(f"ALTER TABLE user_settings ADD COLUMN {col} {ddl}")
            if col == "be_trigger_tp_index":
                added_be_trigger = True
    # Backward compatibility: old off switch remains respected after adding smart BE.
    if added_be_trigger or "be_after_tp1_enabled" in cols:
        await conn.execute(
            "UPDATE user_settings SET be_trigger_tp_index=0 WHERE COALESCE(be_after_tp1_enabled,1)=0"
        )
    # v1.5.7: smart BE is intentionally limited to TP1-TP3.
    # Existing TP4/TP5 choices are safely normalized to TP3 on startup.
    await conn.execute(
        "UPDATE user_settings SET be_trigger_tp_index=3 "
        "WHERE COALESCE(be_after_tp1_enabled,1)=1 AND COALESCE(be_trigger_tp_index,1)>3"
    )
    await conn.execute(
        "UPDATE user_settings SET exchange='bingx' "
        "WHERE exchange IS NULL OR exchange <> 'bingx'"
    )
    await conn.execute(
        "UPDATE user_settings SET limit_ttl_hours=24 "
        "WHERE limit_ttl_hours IS NULL OR limit_ttl_hours < 0 OR limit_ttl_hours > 168"
    )
    await conn.execute(
        "UPDATE user_settings SET limit_tp_invalidation_mode='half' "
        "WHERE limit_tp_invalidation_mode IS NULL OR limit_tp_invalidation_mode NOT IN ('none','tp1','tp2','half','last')"
    )
    await conn.execute(
        "UPDATE user_settings SET limit_policy_preset='balanced' "
        "WHERE limit_policy_preset IS NULL OR limit_policy_preset=''"
    )
    await conn.execute(
        "UPDATE user_settings SET skip_trade_notifications_enabled=0 "
        "WHERE skip_trade_notifications_enabled IS NULL "
        "OR skip_trade_notifications_enabled NOT IN (0,1)"
    )

    # v1.0.3 had PRIMARY KEY(user_id) for API keys and some old SQLite DBs
    # do not have an `exchange` column at all. Inspect/migrate before any
    # query touches user_api_keys.exchange; otherwise startup can fail with
    # "no such column: exchange" on older local /data/*.db files.
    cur = await conn.execute("PRAGMA table_info(user_api_keys)")
    api_cols = await cur.fetchall()
    api_col_names = {row[1] for row in api_cols}
    pk_cols = [row[1] for row in api_cols if row[5]]
    # Critical order: add `exchange` before rebuilding legacy PK(user_id) tables.
    # v1.0.3 SQLite DBs had PRIMARY KEY(user_id) and no exchange column; without
    # this ALTER first, the INSERT...SELECT during rebuild crashes with
    # "no such column: exchange".
    if "exchange" not in api_col_names:
        await conn.execute(
            "ALTER TABLE user_api_keys ADD COLUMN exchange TEXT DEFAULT 'legacy'"
        )
        await conn.execute(
            "UPDATE user_api_keys SET exchange='legacy' WHERE exchange IS NULL OR exchange=''"
        )
        api_col_names.add("exchange")

    if pk_cols == ["user_id"]:
        await conn.execute("ALTER TABLE user_api_keys RENAME TO user_api_keys_old")
        await conn.execute("""
        CREATE TABLE user_api_keys (
          user_id INTEGER NOT NULL,
          exchange TEXT NOT NULL DEFAULT 'bingx',
          api_key_encrypted TEXT NOT NULL,
          api_secret_encrypted TEXT NOT NULL,
          passphrase_encrypted TEXT,
          testnet INTEGER DEFAULT 0,
          enabled INTEGER DEFAULT 1,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP,
          updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY(user_id, exchange)
        )
        """)
        await conn.execute("""
        INSERT OR IGNORE INTO user_api_keys(user_id, exchange, api_key_encrypted, api_secret_encrypted, passphrase_encrypted, testnet, enabled, created_at, updated_at)
        SELECT user_id, 'bingx', api_key_encrypted, api_secret_encrypted, passphrase_encrypted, COALESCE(testnet,0), COALESCE(enabled,1), created_at, updated_at
        FROM user_api_keys_old
        WHERE LOWER(COALESCE(exchange, '')) = 'bingx'
        """)
        await conn.execute("DROP TABLE user_api_keys_old")

    # Remove credentials for exchanges that are no longer part of this build.
    await conn.execute(
        "DELETE FROM user_api_keys WHERE COALESCE(exchange,'') <> 'bingx'"
    )

    # v1.0.7g7h2f3: concurrent notification claims and incident identity.
    cur = await conn.execute("PRAGMA table_info(api_key_quarantines)")
    quarantine_cols = {row[1] for row in await cur.fetchall()}
    quarantine_missing = {
        "incident_token": "TEXT",
        "user_notify_claimed_at": "TEXT",
        "admin_notify_claimed_at": "TEXT",
    }
    for col, ddl in quarantine_missing.items():
        if col not in quarantine_cols:
            await conn.execute(
                f"ALTER TABLE api_key_quarantines ADD COLUMN {col} {ddl}"
            )
    await conn.execute(
        "UPDATE api_key_quarantines SET incident_token=lower(hex(randomblob(16))) "
        "WHERE incident_token IS NULL OR incident_token=''"
    )

    await _sqlite_normalize_bingx_whitelist(conn)
    archived = await _sqlite_archive_legacy_executions(conn)
    if archived:
        logging.warning(
            "BingX-only migration archived %s legacy active executions", archived
        )
    legacy_tp_rows = await _sqlite_mark_legacy_synthetic_tp_rows(conn)
    if legacy_tp_rows:
        logging.warning(
            "TP integrity migration moved %s legacy synthetic-TP executions to partial_error",
            legacy_tp_rows,
        )
    # v1.5.9: durable trade outcome fields for the 30-day dashboard/winrate.
    cur = await conn.execute("PRAGMA table_info(trade_executions)")
    execution_cols = {row[1] for row in await cur.fetchall()}
    execution_missing = {
        "trade_group_id": "INTEGER",
        "outcome": "TEXT",
        "realized_pnl": "REAL",
        "close_type": "TEXT",
        "closed_at": "TEXT",
        "critical_next_check_at": "TEXT",
        "critical_unchanged_count": "INTEGER NOT NULL DEFAULT 0",
        "critical_reason_hash": "TEXT",
        "critical_last_change_at": "TEXT",
        "equity_snapshot_usd": "REAL",
        "planned_risk_usd": "REAL",
        "initial_price_risk_usd": "REAL",
        "initial_risk_percent_of_equity": "REAL",
        "estimated_fee_risk_usd": "REAL",
        "expected_loss_at_stop_usd": "REAL",
        "planned_entry_qty": "REAL",
        "stop_distance": "REAL",
        "risk_snapshot_at": "TEXT",
        "risk_snapshot_source": "TEXT",
        "risk_snapshot_status": "TEXT NOT NULL DEFAULT 'missing'",
        "risk_snapshot_reason": "TEXT",
        "tp_distribution_source": "TEXT NOT NULL DEFAULT 'configured_pre_fill'",
        "tp_distribution_locked": "INTEGER NOT NULL DEFAULT 0",
        "tp_distribution_version": "INTEGER NOT NULL DEFAULT 1",
    }
    for col, ddl in execution_missing.items():
        if col not in execution_cols:
            await conn.execute(f"ALTER TABLE trade_executions ADD COLUMN {col} {ddl}")
    await conn.execute(
        "UPDATE trade_executions SET risk_snapshot_status='missing' "
        "WHERE risk_snapshot_status IS NULL OR risk_snapshot_status=''"
    )
    await conn.execute(
        "UPDATE trade_executions SET tp_distribution_source='configured_pre_fill' "
        "WHERE tp_distribution_source IS NULL OR tp_distribution_source=''"
    )
    await conn.execute(
        "UPDATE trade_executions SET tp_distribution_locked=0 "
        "WHERE tp_distribution_locked IS NULL OR tp_distribution_locked NOT IN (0,1)"
    )
    await conn.execute(
        "UPDATE trade_executions SET tp_distribution_version=1 "
        "WHERE tp_distribution_version IS NULL OR tp_distribution_version < 1"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS trade_executions_user_closed_idx "
        "ON trade_executions(user_id, closed_at)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS trade_executions_user_status_created_idx "
        "ON trade_executions(user_id, status, created_at)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS trade_executions_user_symbol_status_idx "
        "ON trade_executions(user_id, symbol, status)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS trade_executions_status_updated_idx "
        "ON trade_executions(status, updated_at, created_at)"
    )
    await conn.execute(
        "UPDATE trade_executions SET critical_unchanged_count=0 "
        "WHERE critical_unchanged_count IS NULL OR critical_unchanged_count < 0"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS trade_executions_critical_due_idx "
        "ON trade_executions(status, critical_next_check_at)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS dedup_created_idx ON dedup(created_at)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS trade_executions_group_status_idx "
        "ON trade_executions(trade_group_id, status)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS trade_groups_symbol_status_idx "
        "ON trade_groups(symbol, status)"
    )
    # v1.0.7g7h: durable market-event gate.  Existing rows are re-armed once
    # at process startup so a deploy cannot strand a price level that is
    # already crossed.  During normal runtime, enqueue_market_event() consumes
    # the arm and rearm_market_event() restores it only after hysteresis retreat.
    cur = await conn.execute("PRAGMA table_info(market_events)")
    market_event_cols = {row[1] for row in await cur.fetchall()}
    market_event_missing = {
        "armed": "INTEGER NOT NULL DEFAULT 0",
        "rearm_count": "INTEGER NOT NULL DEFAULT 0",
        "last_rearmed_at": "TEXT",
        "retrigger_requested": "INTEGER NOT NULL DEFAULT 0",
        "retrigger_observed_price": "REAL",
        "event_priority": "INTEGER NOT NULL DEFAULT 20",
        "lease_token": "TEXT",
        "lease_generation": "INTEGER NOT NULL DEFAULT 0",
        "lease_expires_at": "TEXT",
        "outcome_kind": "TEXT",
        "watch_lane": "TEXT NOT NULL DEFAULT 'critical'",
        "escalated_at": "TEXT",
        "stuck_started_at": "TEXT",
        "last_stuck_alert_at": "TEXT",
        "last_stuck_reminder_at": "TEXT",
        "stuck_reason": "TEXT",
        "coalesced_event_keys": "TEXT",
        "phase": "TEXT NOT NULL DEFAULT 'LEGACY'",
        "fast_attempts": "INTEGER NOT NULL DEFAULT 0",
        "deep_attempts": "INTEGER NOT NULL DEFAULT 0",
        "final_attempts": "INTEGER NOT NULL DEFAULT 0",
        "unchanged_evidence_count": "INTEGER NOT NULL DEFAULT 0",
        "evidence_fingerprint": "TEXT",
        "evidence_snapshot_json": "TEXT",
        "shadow_decision": "TEXT",
        "shadow_reason": "TEXT",
        "shadow_evaluated_at": "TEXT",
        "shadow_version": "INTEGER NOT NULL DEFAULT 1",
        "terminal_outcome": "TEXT",
        "terminal_reason": "TEXT",
        "terminal_at": "TEXT",
        "manual_review_at": "TEXT",
        "automation_enabled": "INTEGER NOT NULL DEFAULT 1",
        "last_exchange_change_at": "TEXT",
        "migration_state": "TEXT NOT NULL DEFAULT 'none'",
        "migration_version": "INTEGER NOT NULL DEFAULT 0",
        "migration_started_at": "TEXT",
        "migration_completed_at": "TEXT",
        "migration_reason": "TEXT",
        "manual_resolution": "TEXT",
        "manual_resolution_admin_id": "INTEGER",
        "manual_resolution_at": "TEXT",
    }
    for col, ddl in market_event_missing.items():
        if col not in market_event_cols:
            await conn.execute(f"ALTER TABLE market_events ADD COLUMN {col} {ddl}")
    await conn.execute(
        "UPDATE market_events SET rearm_count=0 "
        "WHERE rearm_count IS NULL OR rearm_count < 0"
    )
    await conn.execute(
        "UPDATE market_events SET retrigger_requested=0 "
        "WHERE retrigger_requested IS NULL OR retrigger_requested NOT IN (0,1)"
    )
    await conn.execute(
        "UPDATE market_events SET armed=1 "
        "WHERE (armed IS NULL OR armed NOT IN (0,1) OR armed=0) "
        "AND COALESCE(automation_enabled,1)=1"
    )
    await conn.execute(
        "UPDATE market_events SET event_priority=CASE UPPER(COALESCE(event_type,'')) "
        "WHEN 'STOP' THEN 0 WHEN 'TP' THEN 10 ELSE 20 END "
        "WHERE COALESCE(event_priority,-1) <> CASE UPPER(COALESCE(event_type,'')) "
        "WHEN 'STOP' THEN 0 WHEN 'TP' THEN 10 ELSE 20 END"
    )
    await conn.execute(
        "UPDATE market_events SET lease_generation=0 "
        "WHERE lease_generation IS NULL OR lease_generation < 0"
    )
    await conn.execute(
        "UPDATE market_events SET lease_token=NULL,lease_expires_at=NULL "
        "WHERE status<>'processing'"
    )
    await conn.execute(
        "UPDATE market_events SET watch_lane='critical' "
        "WHERE watch_lane IS NULL OR watch_lane NOT IN ('critical','admin')"
    )
    # g30 could leave a deferred re-cross bit on TP siblings that had already
    # been made not-applicable for a pending LIMIT group.  Repair those rows on
    # upgrade so a later real entry/crossing can re-arm normally.
    await conn.execute(
        """UPDATE market_events
           SET armed=0,retrigger_requested=0,retrigger_observed_price=NULL,
               watch_lane='critical',escalated_at=NULL,stuck_started_at=NULL,
               last_stuck_alert_at=NULL,last_stuck_reminder_at=NULL,
               coalesced_event_keys=NULL,lease_token=NULL,lease_expires_at=NULL,
               updated_at=CURRENT_TIMESTAMP
           WHERE status='done'
             AND outcome_kind='not_applicable_pending_entry'"""
    )
    terminal_repair = await conn.execute(
        """UPDATE market_events
           SET armed=0,automation_enabled=0,next_attempt_at=NULL,
               retrigger_requested=0,retrigger_observed_price=NULL,
               lease_token=NULL,lease_expires_at=NULL,updated_at=CURRENT_TIMESTAMP
           WHERE status='done'
             AND phase='COMPLETED'
             AND terminal_outcome IS NOT NULL
             AND UPPER(terminal_outcome)<>'UNKNOWN'
             AND (COALESCE(automation_enabled,1)<>0
                  OR COALESCE(armed,0)<>0
                  OR next_attempt_at IS NOT NULL
                  OR COALESCE(retrigger_requested,0)<>0)"""
    )
    repaired_terminal_rows = max(0, int(terminal_repair.rowcount or 0))
    if repaired_terminal_rows:
        log.warning(
            "MARKET_EVENT_TERMINAL_AUTOMATION_REPAIR backend=sqlite rows=%s",
            repaired_terminal_rows,
        )
    # g27 could mark a manual TP watcher DONE after its bounded 160-attempt
    # window even while the linked execution remained active.  Reopen only
    # legacy rows that predate outcome_kind, exceeded that exact boundary and
    # still belong to an actively managed execution.  The migration marker
    # prevents repeated reopening on later g28 restarts.
    active_statuses = tuple(_GROUP_ACTIVE_EXECUTION_STATUSES)
    active_placeholders = ",".join(["?"] * len(active_statuses))
    await conn.execute(
        f"""
        UPDATE market_events AS me
        SET status='pending',armed=0,next_attempt_at=CURRENT_TIMESTAMP,
            last_error='g28 reopened legacy g27 exhausted TP watcher',
            lease_token=NULL,lease_expires_at=NULL,
            outcome_kind='migration_reopened_g27_exhausted_tp_watch',
            updated_at=CURRENT_TIMESTAMP
        WHERE UPPER(COALESCE(me.event_type,''))='TP'
          AND me.status='done'
          AND COALESCE(me.attempts,0)>=160
          AND me.outcome_kind IS NULL
          AND EXISTS (
              SELECT 1 FROM trade_executions e
              WHERE e.trade_group_id=me.trade_group_id
                AND e.status IN ({active_placeholders})
          )
        """,
        active_statuses,
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS market_events_status_due_idx "
        "ON market_events(status, next_attempt_at)"
    )
    # Keep the legacy index name for old databases and add a new priority-aware
    # index under a distinct name.  SQLite/PostgreSQL IF NOT EXISTS never
    # rewrites an existing index definition in place.
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS market_events_priority_due_idx "
        "ON market_events(status, event_priority, next_attempt_at, trade_group_id)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS market_events_lane_due_idx "
        "ON market_events(status, watch_lane, event_priority, next_attempt_at, trade_group_id)"
    )
    await conn.execute(
        "UPDATE market_events SET phase='LEGACY' "
        "WHERE phase IS NULL OR TRIM(phase)=''"
    )
    await conn.execute(
        "UPDATE market_events SET fast_attempts=0,deep_attempts=0,final_attempts=0, "
        "unchanged_evidence_count=0 "
        "WHERE fast_attempts IS NULL OR fast_attempts<0 "
        "OR deep_attempts IS NULL OR deep_attempts<0 "
        "OR final_attempts IS NULL OR final_attempts<0 "
        "OR unchanged_evidence_count IS NULL OR unchanged_evidence_count<0"
    )
    await conn.execute(
        "UPDATE market_events SET automation_enabled=1 "
        "WHERE automation_enabled IS NULL OR automation_enabled NOT IN (0,1)"
    )
    await conn.execute(
        "UPDATE market_events SET migration_state='none' "
        "WHERE migration_state IS NULL OR TRIM(migration_state)=''"
    )
    await conn.execute(
        "UPDATE market_events SET migration_version=0 "
        "WHERE migration_version IS NULL OR migration_version<0"
    )
    await conn.execute(
        "UPDATE market_events SET shadow_version=1 "
        "WHERE shadow_version IS NULL OR shadow_version<1"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS market_event_execution_states_event_idx "
        "ON market_event_execution_states(event_id, execution_id)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS market_event_evidence_history_event_idx "
        "ON market_event_evidence_history(event_id, created_at)"
    )
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS market_event_manual_actions ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,event_id INTEGER NOT NULL,"
        "trade_group_id INTEGER NOT NULL,admin_user_id INTEGER NOT NULL,"
        "action TEXT NOT NULL,comment TEXT,before_state_json TEXT NOT NULL,"
        "after_state_json TEXT NOT NULL,evidence_fingerprint TEXT,"
        "created_at TEXT DEFAULT CURRENT_TIMESTAMP)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS market_event_manual_actions_event_idx "
        "ON market_event_manual_actions(event_id, created_at)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS market_events_migration_idx "
        "ON market_events(migration_state, status, watch_lane, attempts, trade_group_id)"
    )
    # Keep the legacy due index compatible with pre-g31 databases: the new
    # claim_expires_at column may not exist yet at this point in migration.
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS durable_notifications_status_due_idx "
        "ON durable_notifications(status, next_attempt_at)"
    )
    cur = await conn.execute("PRAGMA table_info(durable_notifications)")
    durable_cols = {row[1] for row in await cur.fetchall()}
    if "reply_markup_json" not in durable_cols:
        await conn.execute(
            "ALTER TABLE durable_notifications ADD COLUMN reply_markup_json TEXT"
        )
    if "claim_token" not in durable_cols:
        await conn.execute(
            "ALTER TABLE durable_notifications ADD COLUMN claim_token TEXT"
        )
    if "claim_generation" not in durable_cols:
        await conn.execute(
            "ALTER TABLE durable_notifications ADD COLUMN claim_generation INTEGER NOT NULL DEFAULT 0"
        )
    if "claim_expires_at" not in durable_cols:
        await conn.execute(
            "ALTER TABLE durable_notifications ADD COLUMN claim_expires_at TEXT"
        )
    await conn.execute(
        "UPDATE durable_notifications SET claim_generation=0 "
        "WHERE claim_generation IS NULL OR claim_generation < 0"
    )
    await conn.execute(
        "UPDATE durable_notifications SET status='pending',claim_token=NULL,claim_expires_at=NULL "
        "WHERE status='processing' AND (claim_expires_at IS NULL OR datetime(claim_expires_at)<datetime('now'))"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS durable_notifications_claim_due_idx "
        "ON durable_notifications(status, next_attempt_at, claim_expires_at)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS user_agreements_user_version_idx ON user_agreements(user_id, agreement_version)"
    )


async def _pg_migrate(conn: Any) -> None:
    await conn.execute(
        "ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS exchange TEXT DEFAULT 'bingx'"
    )
    await conn.execute(
        "ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS mode TEXT DEFAULT 'preview'"
    )
    await conn.execute(
        "ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS risk_per_trade_percent DOUBLE PRECISION DEFAULT 1.0"
    )
    await conn.execute(
        "ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS daily_risk_limit_percent DOUBLE PRECISION DEFAULT 10.0"
    )
    await conn.execute(
        "ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS max_open_trades INTEGER DEFAULT 10"
    )
    await conn.execute(
        "ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS max_portfolio_risk_percent DOUBLE PRECISION DEFAULT 10.0"
    )
    await conn.execute(
        "ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS exclude_be_trades_from_risk INTEGER DEFAULT 1"
    )
    await conn.execute(
        "ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS tp_limit TEXT DEFAULT 'all'"
    )
    await conn.execute(
        "ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS tp_mode TEXT DEFAULT 'bell'"
    )
    await conn.execute(
        "ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS be_after_tp1_enabled INTEGER DEFAULT 1"
    )
    await conn.execute(
        "ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS be_trigger_tp_index INTEGER DEFAULT 1"
    )
    await conn.execute(
        "ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS use_signal_tp_percents INTEGER DEFAULT 0"
    )
    await conn.execute(
        "ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS skip_trade_notifications_enabled INTEGER NOT NULL DEFAULT 0"
    )
    await conn.execute(
        "ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS manual_tp_percents TEXT DEFAULT '[]'"
    )
    await conn.execute(
        "ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS whitelisted INTEGER DEFAULT 0"
    )
    await conn.execute(
        "ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS whitelisted_exchanges TEXT DEFAULT NULL"
    )
    await conn.execute(
        "ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS limit_ttl_hours INTEGER DEFAULT 24"
    )
    await conn.execute(
        "ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS limit_tp_invalidation_mode TEXT DEFAULT 'half'"
    )
    await conn.execute(
        "ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS limit_policy_preset TEXT DEFAULT 'balanced'"
    )
    # New accounts use the simplified Standard profile even on an existing DB.
    await conn.execute(
        "ALTER TABLE user_settings ALTER COLUMN limit_ttl_hours SET DEFAULT 24"
    )
    await conn.execute(
        "ALTER TABLE user_settings ALTER COLUMN limit_tp_invalidation_mode SET DEFAULT 'half'"
    )
    await conn.execute(
        "ALTER TABLE user_settings ALTER COLUMN limit_policy_preset SET DEFAULT 'balanced'"
    )
    await conn.execute(
        "ALTER TABLE user_settings ALTER COLUMN skip_trade_notifications_enabled SET DEFAULT 0"
    )
    await conn.execute(
        "UPDATE user_settings SET skip_trade_notifications_enabled=0 "
        "WHERE skip_trade_notifications_enabled IS NULL "
        "OR skip_trade_notifications_enabled NOT IN (0,1)"
    )
    await conn.execute(
        "ALTER TABLE user_settings ALTER COLUMN skip_trade_notifications_enabled SET NOT NULL"
    )
    await conn.execute(
        "UPDATE user_settings SET be_trigger_tp_index=0 WHERE COALESCE(be_after_tp1_enabled,1)=0"
    )
    # v1.5.7: smart BE is intentionally limited to TP1-TP3.
    # Existing TP4/TP5 choices are safely normalized to TP3 on startup.
    await conn.execute(
        "UPDATE user_settings SET be_trigger_tp_index=3 "
        "WHERE COALESCE(be_after_tp1_enabled,1)=1 AND COALESCE(be_trigger_tp_index,1)>3"
    )
    # BingX-only runtime: normalize every user setting to BingX.
    await conn.execute(
        "UPDATE user_settings SET exchange='bingx' "
        "WHERE exchange IS NULL OR exchange <> 'bingx'"
    )
    await conn.execute(
        "UPDATE user_settings SET limit_ttl_hours=24 "
        "WHERE limit_ttl_hours IS NULL OR limit_ttl_hours < 0 OR limit_ttl_hours > 168"
    )
    await conn.execute(
        "UPDATE user_settings SET limit_tp_invalidation_mode='half' "
        "WHERE limit_tp_invalidation_mode IS NULL OR limit_tp_invalidation_mode NOT IN ('none','tp1','tp2','half','last')"
    )
    await conn.execute(
        "UPDATE user_settings SET limit_policy_preset='balanced' "
        "WHERE limit_policy_preset IS NULL OR limit_policy_preset=''"
    )
    await conn.execute(
        "ALTER TABLE user_api_keys ADD COLUMN IF NOT EXISTS exchange TEXT DEFAULT 'legacy'"
    )
    await conn.execute(
        "UPDATE user_api_keys SET exchange='legacy' WHERE exchange IS NULL OR exchange=''"
    )
    # Remove credentials for exchanges that are no longer part of this build.
    await conn.execute(
        "DELETE FROM user_api_keys WHERE COALESCE(exchange,'') <> 'bingx'"
    )
    await conn.execute(
        "ALTER TABLE user_api_keys ALTER COLUMN exchange SET DEFAULT 'bingx'"
    )
    await conn.execute("ALTER TABLE user_api_keys ALTER COLUMN exchange SET NOT NULL")
    await conn.execute(
        "ALTER TABLE api_key_quarantines ADD COLUMN IF NOT EXISTS incident_token TEXT"
    )
    await conn.execute(
        "ALTER TABLE api_key_quarantines ADD COLUMN IF NOT EXISTS user_notify_claimed_at TIMESTAMPTZ"
    )
    await conn.execute(
        "ALTER TABLE api_key_quarantines ADD COLUMN IF NOT EXISTS admin_notify_claimed_at TIMESTAMPTZ"
    )
    await conn.execute(
        "UPDATE api_key_quarantines SET incident_token="
        "md5(random()::text || clock_timestamp()::text || user_id::text || exchange) "
        "WHERE incident_token IS NULL OR incident_token=''"
    )
    # Older builds had PRIMARY KEY(user_id). Keep one encrypted BingX credential row per user.
    await conn.execute("""
    DO $$
    DECLARE pkname text;
    BEGIN
      SELECT conname INTO pkname
      FROM pg_constraint
      WHERE conrelid = 'user_api_keys'::regclass AND contype = 'p'
      LIMIT 1;
      IF pkname IS NOT NULL THEN
        EXECUTE format('ALTER TABLE user_api_keys DROP CONSTRAINT %I', pkname);
      END IF;
    END $$;
    """)
    await conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS user_api_keys_user_exchange_uidx ON user_api_keys(user_id, exchange)"
    )
    await _pg_normalize_bingx_whitelist(conn)
    archived = await _pg_archive_legacy_executions(conn)
    if archived:
        logging.warning(
            "BingX-only migration archived %s legacy active executions", archived
        )
    legacy_tp_rows = await _pg_mark_legacy_synthetic_tp_rows(conn)
    if legacy_tp_rows:
        logging.warning(
            "TP integrity migration moved %s legacy synthetic-TP executions to partial_error",
            legacy_tp_rows,
        )
    # v1.5.9: durable trade outcome fields for the 30-day dashboard/winrate.
    await conn.execute(
        "ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS trade_group_id BIGINT"
    )
    await conn.execute(
        "ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS outcome TEXT"
    )
    await conn.execute(
        "ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS realized_pnl DOUBLE PRECISION"
    )
    await conn.execute(
        "ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS close_type TEXT"
    )
    await conn.execute(
        "ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS closed_at TIMESTAMPTZ"
    )
    await conn.execute(
        "ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS critical_next_check_at TIMESTAMPTZ"
    )
    await conn.execute(
        "ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS critical_unchanged_count INTEGER DEFAULT 0"
    )
    await conn.execute(
        "ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS critical_reason_hash TEXT"
    )
    await conn.execute(
        "ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS critical_last_change_at TIMESTAMPTZ"
    )
    for statement in (
        "ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS equity_snapshot_usd DOUBLE PRECISION",
        "ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS planned_risk_usd DOUBLE PRECISION",
        "ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS initial_price_risk_usd DOUBLE PRECISION",
        "ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS initial_risk_percent_of_equity DOUBLE PRECISION",
        "ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS estimated_fee_risk_usd DOUBLE PRECISION",
        "ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS expected_loss_at_stop_usd DOUBLE PRECISION",
        "ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS planned_entry_qty DOUBLE PRECISION",
        "ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS stop_distance DOUBLE PRECISION",
        "ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS risk_snapshot_at TIMESTAMPTZ",
        "ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS risk_snapshot_source TEXT",
        "ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS risk_snapshot_status TEXT NOT NULL DEFAULT 'missing'",
        "ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS risk_snapshot_reason TEXT",
        "ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS tp_distribution_source TEXT NOT NULL DEFAULT 'configured_pre_fill'",
        "ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS tp_distribution_locked INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS tp_distribution_version INTEGER NOT NULL DEFAULT 1",
    ):
        await conn.execute(statement)
    await conn.execute(
        "UPDATE trade_executions SET risk_snapshot_status='missing' "
        "WHERE risk_snapshot_status IS NULL OR risk_snapshot_status=''"
    )
    await conn.execute(
        "UPDATE trade_executions SET tp_distribution_source='configured_pre_fill' "
        "WHERE tp_distribution_source IS NULL OR tp_distribution_source=''"
    )
    await conn.execute(
        "UPDATE trade_executions SET tp_distribution_locked=0 "
        "WHERE tp_distribution_locked IS NULL OR tp_distribution_locked NOT IN (0,1)"
    )
    await conn.execute(
        "UPDATE trade_executions SET tp_distribution_version=1 "
        "WHERE tp_distribution_version IS NULL OR tp_distribution_version < 1"
    )
    await conn.execute(
        "UPDATE trade_executions SET critical_unchanged_count=0 "
        "WHERE critical_unchanged_count IS NULL OR critical_unchanged_count < 0"
    )
    await conn.execute(
        "ALTER TABLE trade_executions ALTER COLUMN critical_unchanged_count SET DEFAULT 0"
    )
    await conn.execute(
        "ALTER TABLE trade_executions ALTER COLUMN critical_unchanged_count SET NOT NULL"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS trade_executions_user_closed_pg_idx "
        "ON trade_executions(user_id, closed_at)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS trade_executions_user_status_created_pg_idx "
        "ON trade_executions(user_id, status, created_at DESC)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS trade_executions_user_symbol_status_pg_idx "
        "ON trade_executions(user_id, UPPER(symbol), status)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS trade_executions_status_updated_pg_idx "
        "ON trade_executions(status, updated_at, created_at)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS trade_executions_critical_due_pg_idx "
        "ON trade_executions(status, critical_next_check_at)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS dedup_created_pg_idx ON dedup(created_at)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS trade_executions_group_status_pg_idx "
        "ON trade_executions(trade_group_id, status)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS trade_groups_symbol_status_pg_idx "
        "ON trade_groups(UPPER(symbol), status)"
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS durable_notifications (
          id BIGSERIAL PRIMARY KEY,
          dedup_key TEXT NOT NULL UNIQUE,
          user_id BIGINT NOT NULL,
          message_text TEXT NOT NULL,
          reply_markup_json TEXT,
          source TEXT DEFAULT 'monitor',
          status TEXT DEFAULT 'pending',
          attempts INTEGER DEFAULT 0,
          next_attempt_at TIMESTAMPTZ DEFAULT NOW(),
          last_error TEXT,
          delivered_at TIMESTAMPTZ,
          claim_token TEXT,
          claim_generation INTEGER NOT NULL DEFAULT 0,
          claim_expires_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          updated_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )
    await conn.execute(
        "ALTER TABLE durable_notifications ADD COLUMN IF NOT EXISTS reply_markup_json TEXT"
    )
    await conn.execute(
        "ALTER TABLE durable_notifications ADD COLUMN IF NOT EXISTS claim_token TEXT"
    )
    await conn.execute(
        "ALTER TABLE durable_notifications ADD COLUMN IF NOT EXISTS claim_generation INTEGER DEFAULT 0"
    )
    await conn.execute(
        "ALTER TABLE durable_notifications ADD COLUMN IF NOT EXISTS claim_expires_at TIMESTAMPTZ"
    )
    await conn.execute(
        "UPDATE durable_notifications SET claim_generation=0 "
        "WHERE claim_generation IS NULL OR claim_generation < 0"
    )
    await conn.execute(
        "UPDATE durable_notifications SET status='pending',claim_token=NULL,claim_expires_at=NULL "
        "WHERE status='processing' AND (claim_expires_at IS NULL OR claim_expires_at<NOW())"
    )
    await conn.execute(
        "ALTER TABLE durable_notifications ALTER COLUMN claim_generation SET DEFAULT 0"
    )
    await conn.execute(
        "ALTER TABLE durable_notifications ALTER COLUMN claim_generation SET NOT NULL"
    )
    # v1.0.7g7h durable ARMED/PENDING/COOLDOWN gate.  Re-arm persisted
    # rows once on startup for safe redeploy recovery; normal re-arming is
    # driven only by a hysteresis retreat in the public price loop.
    await conn.execute(
        "ALTER TABLE market_events ADD COLUMN IF NOT EXISTS armed INTEGER DEFAULT 0"
    )
    await conn.execute(
        "ALTER TABLE market_events ADD COLUMN IF NOT EXISTS rearm_count INTEGER DEFAULT 0"
    )
    await conn.execute(
        "ALTER TABLE market_events ADD COLUMN IF NOT EXISTS last_rearmed_at TIMESTAMPTZ"
    )
    await conn.execute(
        "ALTER TABLE market_events ADD COLUMN IF NOT EXISTS retrigger_requested INTEGER DEFAULT 0"
    )
    await conn.execute(
        "ALTER TABLE market_events ADD COLUMN IF NOT EXISTS retrigger_observed_price DOUBLE PRECISION"
    )
    await conn.execute(
        "ALTER TABLE market_events ADD COLUMN IF NOT EXISTS event_priority INTEGER DEFAULT 20"
    )
    await conn.execute(
        "ALTER TABLE market_events ADD COLUMN IF NOT EXISTS lease_token TEXT"
    )
    await conn.execute(
        "ALTER TABLE market_events ADD COLUMN IF NOT EXISTS lease_generation INTEGER DEFAULT 0"
    )
    await conn.execute(
        "ALTER TABLE market_events ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ"
    )
    await conn.execute(
        "ALTER TABLE market_events ADD COLUMN IF NOT EXISTS outcome_kind TEXT"
    )
    await conn.execute(
        "ALTER TABLE market_events ADD COLUMN IF NOT EXISTS watch_lane TEXT DEFAULT 'critical'"
    )
    await conn.execute(
        "ALTER TABLE market_events ADD COLUMN IF NOT EXISTS escalated_at TIMESTAMPTZ"
    )
    await conn.execute(
        "ALTER TABLE market_events ADD COLUMN IF NOT EXISTS stuck_started_at TIMESTAMPTZ"
    )
    await conn.execute(
        "ALTER TABLE market_events ADD COLUMN IF NOT EXISTS last_stuck_alert_at TIMESTAMPTZ"
    )
    await conn.execute(
        "ALTER TABLE market_events ADD COLUMN IF NOT EXISTS last_stuck_reminder_at TIMESTAMPTZ"
    )
    await conn.execute(
        "ALTER TABLE market_events ADD COLUMN IF NOT EXISTS stuck_reason TEXT"
    )
    await conn.execute(
        "ALTER TABLE market_events ADD COLUMN IF NOT EXISTS coalesced_event_keys TEXT"
    )
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS phase TEXT DEFAULT 'LEGACY'")
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS fast_attempts INTEGER DEFAULT 0")
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS deep_attempts INTEGER DEFAULT 0")
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS final_attempts INTEGER DEFAULT 0")
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS unchanged_evidence_count INTEGER DEFAULT 0")
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS evidence_fingerprint TEXT")
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS evidence_snapshot_json TEXT")
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS shadow_decision TEXT")
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS shadow_reason TEXT")
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS shadow_evaluated_at TIMESTAMPTZ")
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS shadow_version INTEGER DEFAULT 1")
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS terminal_outcome TEXT")
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS terminal_reason TEXT")
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS terminal_at TIMESTAMPTZ")
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS manual_review_at TIMESTAMPTZ")
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS automation_enabled INTEGER DEFAULT 1")
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS last_exchange_change_at TIMESTAMPTZ")
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS migration_state TEXT DEFAULT 'none'")
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS migration_version INTEGER DEFAULT 0")
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS migration_started_at TIMESTAMPTZ")
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS migration_completed_at TIMESTAMPTZ")
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS migration_reason TEXT")
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS manual_resolution TEXT")
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS manual_resolution_admin_id BIGINT")
    await conn.execute("ALTER TABLE market_events ADD COLUMN IF NOT EXISTS manual_resolution_at TIMESTAMPTZ")
    await conn.execute(
        "UPDATE market_events SET rearm_count=0 "
        "WHERE rearm_count IS NULL OR rearm_count < 0"
    )
    await conn.execute(
        "UPDATE market_events SET retrigger_requested=0 "
        "WHERE retrigger_requested IS NULL OR retrigger_requested NOT IN (0,1)"
    )
    await conn.execute(
        "UPDATE market_events SET armed=1 "
        "WHERE armed IS DISTINCT FROM 1 "
        "AND COALESCE(automation_enabled,1)=1"
    )
    await conn.execute(
        "UPDATE market_events SET event_priority=CASE UPPER(COALESCE(event_type,'')) "
        "WHEN 'STOP' THEN 0 WHEN 'TP' THEN 10 ELSE 20 END "
        "WHERE COALESCE(event_priority,-1) <> CASE UPPER(COALESCE(event_type,'')) "
        "WHEN 'STOP' THEN 0 WHEN 'TP' THEN 10 ELSE 20 END"
    )
    await conn.execute(
        "UPDATE market_events SET lease_generation=0 "
        "WHERE lease_generation IS NULL OR lease_generation < 0"
    )
    await conn.execute(
        "UPDATE market_events SET lease_token=NULL,lease_expires_at=NULL "
        "WHERE status<>'processing'"
    )
    await conn.execute(
        "UPDATE market_events SET watch_lane='critical' "
        "WHERE watch_lane IS NULL OR watch_lane NOT IN ('critical','admin')"
    )
    await conn.execute(
        """UPDATE market_events
           SET armed=0,retrigger_requested=0,retrigger_observed_price=NULL,
               watch_lane='critical',escalated_at=NULL,stuck_started_at=NULL,
               last_stuck_alert_at=NULL,last_stuck_reminder_at=NULL,
               coalesced_event_keys=NULL,lease_token=NULL,lease_expires_at=NULL,
               updated_at=NOW()
           WHERE status='done'
             AND outcome_kind='not_applicable_pending_entry'"""
    )
    terminal_repair_tag = await conn.execute(
        """UPDATE market_events
           SET armed=0,automation_enabled=0,next_attempt_at=NULL,
               retrigger_requested=0,retrigger_observed_price=NULL,
               lease_token=NULL,lease_expires_at=NULL,updated_at=NOW()
           WHERE status='done'
             AND phase='COMPLETED'
             AND terminal_outcome IS NOT NULL
             AND UPPER(terminal_outcome)<>'UNKNOWN'
             AND (COALESCE(automation_enabled,1)<>0
                  OR COALESCE(armed,0)<>0
                  OR next_attempt_at IS NOT NULL
                  OR COALESCE(retrigger_requested,0)<>0)"""
    )
    try:
        repaired_terminal_rows = int(str(terminal_repair_tag).rsplit(" ", 1)[-1])
    except (TypeError, ValueError):
        repaired_terminal_rows = 0
    if repaired_terminal_rows:
        log.warning(
            "MARKET_EVENT_TERMINAL_AUTOMATION_REPAIR backend=postgres rows=%s",
            repaired_terminal_rows,
        )
    # Recover the exact legacy g27 terminal-watch hole without reopening
    # ordinary completed TP rows.  outcome_kind is NULL only on pre-g28 rows;
    # after this one-time migration every subsequent finish is self-identifying.
    await conn.execute(
        """
        UPDATE market_events me
        SET status='pending',armed=0,next_attempt_at=NOW(),
            last_error='g28 reopened legacy g27 exhausted TP watcher',
            lease_token=NULL,lease_expires_at=NULL,
            outcome_kind='migration_reopened_g27_exhausted_tp_watch',
            updated_at=NOW()
        WHERE UPPER(COALESCE(me.event_type,''))='TP'
          AND me.status='done'
          AND COALESCE(me.attempts,0)>=160
          AND me.outcome_kind IS NULL
          AND EXISTS (
              SELECT 1 FROM trade_executions e
              WHERE e.trade_group_id=me.trade_group_id
                AND e.status = ANY($1::text[])
          )
        """,
        list(_GROUP_ACTIVE_EXECUTION_STATUSES),
    )
    await conn.execute("ALTER TABLE market_events ALTER COLUMN armed SET DEFAULT 0")
    await conn.execute("ALTER TABLE market_events ALTER COLUMN armed SET NOT NULL")
    await conn.execute(
        "ALTER TABLE market_events ALTER COLUMN rearm_count SET DEFAULT 0"
    )
    await conn.execute(
        "ALTER TABLE market_events ALTER COLUMN rearm_count SET NOT NULL"
    )
    await conn.execute(
        "ALTER TABLE market_events ALTER COLUMN retrigger_requested SET DEFAULT 0"
    )
    await conn.execute(
        "ALTER TABLE market_events ALTER COLUMN retrigger_requested SET NOT NULL"
    )
    await conn.execute(
        "ALTER TABLE market_events ALTER COLUMN event_priority SET DEFAULT 20"
    )
    await conn.execute(
        "ALTER TABLE market_events ALTER COLUMN event_priority SET NOT NULL"
    )
    await conn.execute(
        "ALTER TABLE market_events ALTER COLUMN lease_generation SET DEFAULT 0"
    )
    await conn.execute(
        "ALTER TABLE market_events ALTER COLUMN lease_generation SET NOT NULL"
    )
    await conn.execute(
        "ALTER TABLE market_events ALTER COLUMN watch_lane SET DEFAULT 'critical'"
    )
    await conn.execute(
        "ALTER TABLE market_events ALTER COLUMN watch_lane SET NOT NULL"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS market_events_status_due_pg_idx "
        "ON market_events(status, next_attempt_at)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS market_events_priority_due_pg_idx "
        "ON market_events(status, event_priority, next_attempt_at, trade_group_id)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS market_events_lane_due_pg_idx "
        "ON market_events(status, watch_lane, event_priority, next_attempt_at, trade_group_id)"
    )
    await conn.execute("UPDATE market_events SET phase='LEGACY' WHERE phase IS NULL OR BTRIM(phase)=''")
    await conn.execute(
        "UPDATE market_events SET fast_attempts=0,deep_attempts=0,final_attempts=0,unchanged_evidence_count=0 "
        "WHERE fast_attempts IS NULL OR fast_attempts<0 OR deep_attempts IS NULL OR deep_attempts<0 "
        "OR final_attempts IS NULL OR final_attempts<0 OR unchanged_evidence_count IS NULL OR unchanged_evidence_count<0"
    )
    await conn.execute("UPDATE market_events SET automation_enabled=1 WHERE automation_enabled IS NULL OR automation_enabled NOT IN (0,1)")
    await conn.execute("UPDATE market_events SET migration_state='none' WHERE migration_state IS NULL OR BTRIM(migration_state)=''")
    await conn.execute("UPDATE market_events SET migration_version=0 WHERE migration_version IS NULL OR migration_version<0")
    await conn.execute("UPDATE market_events SET shadow_version=1 WHERE shadow_version IS NULL OR shadow_version<1")
    await conn.execute("ALTER TABLE market_events ALTER COLUMN phase SET DEFAULT 'LEGACY'")
    await conn.execute("ALTER TABLE market_events ALTER COLUMN phase SET NOT NULL")
    await conn.execute("ALTER TABLE market_events ALTER COLUMN fast_attempts SET DEFAULT 0")
    await conn.execute("ALTER TABLE market_events ALTER COLUMN fast_attempts SET NOT NULL")
    await conn.execute("ALTER TABLE market_events ALTER COLUMN deep_attempts SET DEFAULT 0")
    await conn.execute("ALTER TABLE market_events ALTER COLUMN deep_attempts SET NOT NULL")
    await conn.execute("ALTER TABLE market_events ALTER COLUMN final_attempts SET DEFAULT 0")
    await conn.execute("ALTER TABLE market_events ALTER COLUMN final_attempts SET NOT NULL")
    await conn.execute("ALTER TABLE market_events ALTER COLUMN unchanged_evidence_count SET DEFAULT 0")
    await conn.execute("ALTER TABLE market_events ALTER COLUMN unchanged_evidence_count SET NOT NULL")
    await conn.execute("ALTER TABLE market_events ALTER COLUMN automation_enabled SET DEFAULT 1")
    await conn.execute("ALTER TABLE market_events ALTER COLUMN automation_enabled SET NOT NULL")
    await conn.execute("ALTER TABLE market_events ALTER COLUMN shadow_version SET DEFAULT 1")
    await conn.execute("ALTER TABLE market_events ALTER COLUMN shadow_version SET NOT NULL")
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS market_event_execution_states_event_pg_idx "
        "ON market_event_execution_states(event_id, execution_id)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS market_event_evidence_history_event_pg_idx "
        "ON market_event_evidence_history(event_id, created_at)"
    )
    await conn.execute(
        """CREATE TABLE IF NOT EXISTS market_event_manual_actions (
               id BIGSERIAL PRIMARY KEY,event_id BIGINT NOT NULL,
               trade_group_id BIGINT NOT NULL,admin_user_id BIGINT NOT NULL,
               action TEXT NOT NULL,comment TEXT,before_state_json TEXT NOT NULL,
               after_state_json TEXT NOT NULL,evidence_fingerprint TEXT,
               created_at TIMESTAMPTZ DEFAULT NOW())"""
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS market_event_manual_actions_event_pg_idx "
        "ON market_event_manual_actions(event_id, created_at)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS market_events_migration_pg_idx "
        "ON market_events(migration_state, status, watch_lane, attempts, trade_group_id)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS durable_notifications_status_due_pg_idx "
        "ON durable_notifications(status, next_attempt_at)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS durable_notifications_claim_due_pg_idx "
        "ON durable_notifications(status, next_attempt_at, claim_expires_at)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS user_agreements_user_version_pg_idx ON user_agreements(user_id, agreement_version)"
    )


@asynccontextmanager
async def connect() -> AsyncIterator[Any]:
    settings = get_settings()
    started = time.monotonic()
    failed = False
    monitor_sem: asyncio.Semaphore | None = None
    monitor_slot = False
    try:
        if is_postgres():
            # v1.0.7g7h2f5g5b3a: the g5b3 full-stage wrapper already owns one
            # PostgreSQL advisory session for the entire lifecycle/BE lane.
            # Reusing it here is essential: otherwise ordinary helpers such as
            # the page SELECT try to checkout a second pool connection while
            # the stage session is still held.  On small pools this self-blocks
            # until POSTGRES_POOL_TIMEOUT; on the default pool it also preserves
            # an avoidable extra checkout/reset and makes the claimed per-lane
            # session reuse only partial.
            advisory_conn = (
                _current_advisory_connection()
                if monitor_workload_stage() == "full"
                else None
            )
            if advisory_conn is not None:
                try:
                    from app.services.monitor_diagnostics import record_counter

                    record_counter("full_stage_db_connection_reused")
                except Exception:
                    pass
                yield advisory_conn
                return

            pool = await _ensure_pg_pool()
            if _MONITOR_DB_CONTEXT.get():
                stage = monitor_workload_stage()
                monitor_sem = (
                    _monitor_critical_db_semaphore()
                    if stage in _SAFETY_CRITICAL_MONITOR_STAGES
                    else _monitor_db_semaphore()
                )
                try:
                    await asyncio.wait_for(
                        monitor_sem.acquire(),
                        timeout=_MONITOR_DB_ACQUIRE_TIMEOUT_SEC,
                    )
                except asyncio.TimeoutError:
                    _log_pool_timeout(
                        "ordinary_admission",
                        workload="monitor",
                        timeout_sec=_MONITOR_DB_ACQUIRE_TIMEOUT_SEC,
                    )
                    raise
                monitor_slot = True
            # asyncpg's pool-acquire wait previously had no explicit upper bound.
            # A stuck/background burst could therefore make both monitor tasks and
            # Telegram menu reads wait indefinitely.
            acquire_timeout = (
                _MONITOR_DB_ACQUIRE_TIMEOUT_SEC
                if _MONITOR_DB_CONTEXT.get()
                else _GENERAL_DB_ACQUIRE_TIMEOUT_SEC
            )
            try:
                conn = await pool.acquire(timeout=acquire_timeout)
            except asyncio.TimeoutError:
                _log_pool_timeout(
                    "ordinary_pool_acquire",
                    workload=("monitor" if _MONITOR_DB_CONTEXT.get() else "foreground"),
                    timeout_sec=acquire_timeout,
                )
                raise
            body_cancelled = False
            try:
                yield conn
            except asyncio.CancelledError:
                body_cancelled = True
                raise
            finally:
                release_conn = conn
                conn = None
                await _release_pg_connection_safely(
                    pool,
                    release_conn,
                    timeout_sec=_MONITOR_DB_ACQUIRE_TIMEOUT_SEC,
                    phase="ordinary",
                    workload=(
                        "monitor" if _MONITOR_DB_CONTEXT.get() else "foreground"
                    ),
                    caller_cancelled=body_cancelled,
                )
        else:
            sqldb = await aiosqlite.connect(settings.DATABASE_PATH)
            sqldb.row_factory = aiosqlite.Row
            try:
                yield sqldb
            finally:
                await sqldb.close()
    except BaseException:
        failed = True
        raise
    finally:
        if monitor_slot and monitor_sem is not None:
            monitor_sem.release()
        # v1.0.7a1 diagnostics only: every DB helper already passes through this
        # context, so its connection scope gives a safe aggregate DB-time signal
        # without proxying or changing any SQL/transaction behavior.
        try:
            from app.services.monitor_diagnostics import record_db_scope

            record_db_scope(
                (time.monotonic() - started) * 1000,
                error=failed,
            )
        except Exception:
            pass


def _dict(row: Any) -> Dict[str, Any]:
    return dict(row) if row else {}


_BingX_EXCHANGE = "bingx"
_LEGACY_ACTIVE_EXECUTION_STATUSES = {
    "opening_intent",
    "opened",
    "pending_limit",
    "protected",
    "partial_error",
    "manual_required",
    "partial_unrecoverable",
    "closed_pending_history",
    "closed_on_exchange",
    "closed_stop_catchup",
}
_LEGACY_ARCHIVE_STATUS = "archived_legacy_exchange"
_LEGACY_ARCHIVE_REASON = (
    "v1.5.6 BingX-only: исполнение относится к удалённой или неопределённой бирже; "
    "автоматическое сопровождение отключено"
)


def _execution_exchange_from_payload(value: Any) -> str:
    """Read the exchange explicitly recorded in an execution payload.

    Empty or malformed payloads are deliberately treated as unknown. A BingX-only
    runtime must never silently adopt a legacy row because a monitor could then
    touch orders created on a different venue.
    """
    try:
        payload = value if isinstance(value, dict) else json.loads(value or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        return ""
    if not isinstance(payload, dict):
        return ""
    return str(payload.get("exchange") or "").strip().lower()


def _is_bingx_execution_row(row: Dict[str, Any]) -> bool:
    return _execution_exchange_from_payload(row.get("exchange_order_ids_json")) in {
        _BingX_EXCHANGE,
        "mexc",
    }


def _ensure_bingx_execution_json(value: Any) -> str:
    """Return a valid execution JSON payload explicitly marked as BingX."""
    try:
        payload = value if isinstance(value, dict) else json.loads(value or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    payload["exchange"] = _BingX_EXCHANGE
    return json.dumps(payload, ensure_ascii=False, default=str)


def _bingx_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Fail closed: monitors and risk accounting see explicit BingX rows only."""
    return [row for row in rows if _is_bingx_execution_row(row)]


def _append_archive_reason(old_reason: Any) -> str:
    old = str(old_reason or "").strip()
    if _LEGACY_ARCHIVE_REASON in old:
        return old
    return f"{old} | {_LEGACY_ARCHIVE_REASON}" if old else _LEGACY_ARCHIVE_REASON


async def _sqlite_archive_legacy_executions(conn: aiosqlite.Connection) -> int:
    placeholders = ",".join(["?"] * len(_LEGACY_ACTIVE_EXECUTION_STATUSES))
    cur = await conn.execute(
        f"SELECT id, reason, exchange_order_ids_json FROM trade_executions "
        f"WHERE status IN ({placeholders})",
        tuple(_LEGACY_ACTIVE_EXECUTION_STATUSES),
    )
    rows = await cur.fetchall()
    archived = 0
    for row in rows:
        if _execution_exchange_from_payload(row[2]) in {_BingX_EXCHANGE, "mexc"}:
            continue
        await conn.execute(
            "UPDATE trade_executions SET status=?, reason=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (_LEGACY_ARCHIVE_STATUS, _append_archive_reason(row[1]), int(row[0])),
        )
        archived += 1
    return archived


async def _pg_archive_legacy_executions(conn: Any) -> int:
    rows = await conn.fetch(
        "SELECT id, reason, exchange_order_ids_json FROM trade_executions "
        "WHERE status = ANY($1::text[])",
        list(_LEGACY_ACTIVE_EXECUTION_STATUSES),
    )
    archived = 0
    for row in rows:
        if _execution_exchange_from_payload(row["exchange_order_ids_json"]) in {
            _BingX_EXCHANGE,
            "mexc",
        }:
            continue
        await conn.execute(
            "UPDATE trade_executions SET status=$1, reason=$2, updated_at=NOW() WHERE id=$3",
            _LEGACY_ARCHIVE_STATUS,
            _append_archive_reason(row["reason"]),
            int(row["id"]),
        )
        archived += 1
    return archived


_LEGACY_SYNTHETIC_TP_MARKERS = (
    "TAKE_PROFIT_SKIPPED_COVERED",
    "_idempotent_coverage_full",
)
_LEGACY_SYNTHETIC_TP_REASON = (
    "v1.6.2 TP integrity migration: synthetic TP success marker requires live recovery"
)


def _append_legacy_tp_reason(old_reason: Any) -> str:
    old = str(old_reason or "").strip()
    if _LEGACY_SYNTHETIC_TP_REASON in old:
        return old
    return (
        f"{old} | {_LEGACY_SYNTHETIC_TP_REASON}" if old else _LEGACY_SYNTHETIC_TP_REASON
    )


async def _sqlite_mark_legacy_synthetic_tp_rows(conn: aiosqlite.Connection) -> int:
    cur = await conn.execute(
        "SELECT id, reason FROM trade_executions "
        "WHERE status IN ('opened','protected','partial_error') "
        "AND (exchange_order_ids_json LIKE ? OR exchange_order_ids_json LIKE ?)",
        tuple(f"%{marker}%" for marker in _LEGACY_SYNTHETIC_TP_MARKERS),
    )
    rows = await cur.fetchall()
    for execution_id, reason in rows:
        await conn.execute(
            "UPDATE trade_executions SET status='partial_error', reason=?, "
            "updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (_append_legacy_tp_reason(reason), int(execution_id)),
        )
    return len(rows)


async def _pg_mark_legacy_synthetic_tp_rows(conn: Any) -> int:
    rows = await conn.fetch(
        "SELECT id, reason FROM trade_executions "
        "WHERE status = ANY($1::text[]) "
        "AND (exchange_order_ids_json LIKE $2 OR exchange_order_ids_json LIKE $3)",
        ["opened", "protected", "partial_error"],
        f"%{_LEGACY_SYNTHETIC_TP_MARKERS[0]}%",
        f"%{_LEGACY_SYNTHETIC_TP_MARKERS[1]}%",
    )
    for row in rows:
        await conn.execute(
            "UPDATE trade_executions SET status='partial_error', reason=$1, "
            "updated_at=NOW() WHERE id=$2",
            _append_legacy_tp_reason(row["reason"]),
            int(row["id"]),
        )
    return len(rows)


async def _sqlite_normalize_bingx_whitelist(conn: aiosqlite.Connection) -> None:
    cur = await conn.execute(
        "SELECT user_id, COALESCE(whitelisted,0), whitelisted_exchanges FROM user_settings"
    )
    for user_id, legacy_flag, raw_grants in await cur.fetchall():
        grants = _parse_wl_exchanges(raw_grants)
        allowed = (
            int(legacy_flag or 0) == 1 or "all" in grants or _BingX_EXCHANGE in grants
        )
        await conn.execute(
            "UPDATE user_settings SET whitelisted=?, whitelisted_exchanges=? WHERE user_id=?",
            (1 if allowed else 0, _BingX_EXCHANGE if allowed else None, int(user_id)),
        )


async def _pg_normalize_bingx_whitelist(conn: Any) -> None:
    rows = await conn.fetch(
        "SELECT user_id, COALESCE(whitelisted,0) AS whitelisted, "
        "whitelisted_exchanges FROM user_settings"
    )
    for row in rows:
        grants = _parse_wl_exchanges(row["whitelisted_exchanges"])
        allowed = (
            int(row["whitelisted"] or 0) == 1
            or "all" in grants
            or _BingX_EXCHANGE in grants
        )
        await conn.execute(
            "UPDATE user_settings SET whitelisted=$1, whitelisted_exchanges=$2 WHERE user_id=$3",
            1 if allowed else 0,
            _BingX_EXCHANGE if allowed else None,
            int(row["user_id"]),
        )


async def ensure_user(
    telegram_id: int, username: str | None = None, is_admin: bool = False
) -> None:
    telegram_id = int(telegram_id)
    if telegram_id in _ENSURED_USERS:
        return
    lock = _get_or_create_lock(_USER_ENSURE_LOCKS, telegram_id, _MAX_LOCK_CACHE)
    async with lock:
        if telegram_id in _ENSURED_USERS:
            return
        settings = get_settings()
        is_admin = is_admin or telegram_id in settings.admin_ids
        # Admins are auto-whitelisted (they bypass the manual whitelist requirement).
        initial_whitelisted = 1 if is_admin else 0
        if is_postgres():
            async with connect() as c:
                await c.execute(
                    "INSERT INTO users(telegram_id, username, is_admin) VALUES($1,$2,$3) "
                    "ON CONFLICT(telegram_id) DO NOTHING",
                    telegram_id,
                    username,
                    int(is_admin),
                )
                await c.execute(
                    """INSERT INTO user_settings(user_id, exchange, mode, risk_per_trade_percent, daily_risk_limit_percent, max_open_trades, max_portfolio_risk_percent, exclude_be_trades_from_risk, tp_limit, tp_mode, be_after_tp1_enabled, be_trigger_tp_index, whitelisted, whitelisted_exchanges, limit_ttl_hours, limit_tp_invalidation_mode, limit_policy_preset, skip_trade_notifications_enabled)
                    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18) ON CONFLICT(user_id) DO NOTHING""",
                    telegram_id,
                    settings.safe_default_exchange,
                    "preview",
                    settings.DEFAULT_RISK_PERCENT,
                    settings.DEFAULT_DAILY_RISK_LIMIT_PERCENT,
                    settings.DEFAULT_MAX_OPEN_TRADES,
                    settings.DEFAULT_MAX_PORTFOLIO_RISK,
                    int(settings.DEFAULT_EXCLUDE_BE_TRADES_FROM_RISK),
                    settings.DEFAULT_TP_LIMIT,
                    settings.DEFAULT_TP_MODE,
                    int(settings.DEFAULT_BE_AFTER_TP1),
                    int(
                        settings.DEFAULT_BE_TRIGGER_TP_INDEX
                        if settings.DEFAULT_BE_AFTER_TP1
                        else 0
                    ),
                    initial_whitelisted,
                    _BingX_EXCHANGE if is_admin else None,
                    int(settings.LIMIT_ORDER_TTL_HOURS),
                    "half",
                    "balanced",
                    0,
                )
                if is_admin:
                    await c.execute(
                        "UPDATE user_settings SET whitelisted=1, whitelisted_exchanges='bingx' WHERE user_id=$1",
                        telegram_id,
                    )
            _ENSURED_USERS.add(telegram_id)
            return
        async with connect() as c:
            await c.execute(
                "INSERT OR IGNORE INTO users(telegram_id, username, is_admin) VALUES (?, ?, ?)",
                (telegram_id, username, int(is_admin)),
            )
            await c.execute(
                """INSERT OR IGNORE INTO user_settings(user_id, exchange, mode, risk_per_trade_percent, daily_risk_limit_percent, max_open_trades, max_portfolio_risk_percent, exclude_be_trades_from_risk, tp_limit, tp_mode, be_after_tp1_enabled, be_trigger_tp_index, whitelisted, whitelisted_exchanges, limit_ttl_hours, limit_tp_invalidation_mode, limit_policy_preset, skip_trade_notifications_enabled)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    telegram_id,
                    settings.safe_default_exchange,
                    "preview",
                    settings.DEFAULT_RISK_PERCENT,
                    settings.DEFAULT_DAILY_RISK_LIMIT_PERCENT,
                    settings.DEFAULT_MAX_OPEN_TRADES,
                    settings.DEFAULT_MAX_PORTFOLIO_RISK,
                    int(settings.DEFAULT_EXCLUDE_BE_TRADES_FROM_RISK),
                    settings.DEFAULT_TP_LIMIT,
                    settings.DEFAULT_TP_MODE,
                    int(settings.DEFAULT_BE_AFTER_TP1),
                    int(
                        settings.DEFAULT_BE_TRIGGER_TP_INDEX
                        if settings.DEFAULT_BE_AFTER_TP1
                        else 0
                    ),
                    initial_whitelisted,
                    _BingX_EXCHANGE if is_admin else None,
                    int(settings.LIMIT_ORDER_TTL_HOURS),
                    "half",
                    "balanced",
                    0,
                ),
            )
            if is_admin:
                await c.execute(
                    "UPDATE user_settings SET whitelisted=1, whitelisted_exchanges='bingx' WHERE user_id=?",
                    (telegram_id,),
                )
            await c.commit()
        _ENSURED_USERS.add(telegram_id)


def _strict_bool_setting(value: Any, *, field: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        parsed = float(value)
        if math.isfinite(parsed) and parsed in {0.0, 1.0}:
            return bool(int(parsed))
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    raise ValueError(f"{field}: требуется логическое значение")


def _strict_integer_setting(
    value: Any,
    *,
    field: str,
    allowed: set[int] | None = None,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    """Parse an integer setting without silently truncating fractions.

    ``int(1.9)`` and ``int(True)`` are both legal Python, but accepting either
    for a trading control can move BE too early or shorten a LIMIT lifetime.
    Integer-looking strings/floats (``"2"``, ``2.0``) remain compatible.
    """
    if isinstance(value, bool) or value is None:
        raise ValueError(f"{field}: требуется целое число")
    try:
        raw = str(value).strip()
        if not raw:
            raise ValueError
        parsed = Decimal(raw)
    except (InvalidOperation, TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field}: требуется целое число") from exc
    if not parsed.is_finite() or parsed != parsed.to_integral_value():
        raise ValueError(f"{field}: требуется целое число")
    result = int(parsed)
    if allowed is not None and result not in allowed:
        allowed_text = ", ".join(str(item) for item in sorted(allowed))
        raise ValueError(f"{field}: допустимы значения {allowed_text}")
    if minimum is not None and result < minimum:
        raise ValueError(f"{field}: значение должно быть не меньше {minimum}")
    if maximum is not None and result > maximum:
        raise ValueError(f"{field}: значение должно быть не больше {maximum}")
    return result


def _validated_manual_tp_percents(value: Any) -> list[float]:
    if value in (None, "", []):
        return []
    if not isinstance(value, (list, tuple)):
        raise ValueError("manual_tp_percents должен быть списком")
    if len(value) > 20:
        raise ValueError("manual_tp_percents: максимум 20 значений")
    parsed: list[float] = []
    for item in value:
        if isinstance(item, bool):
            raise ValueError("manual_tp_percents: требуется число")
        number = float(item)
        if not math.isfinite(number) or number <= 0:
            raise ValueError("manual_tp_percents: значения должны быть конечными и > 0")
        parsed.append(number)
    if parsed and abs(sum(parsed) - 100.0) > 0.001:
        raise ValueError("manual_tp_percents должен давать 100%")
    return parsed


async def get_user_settings(user_id: int, *, ensure: bool = True) -> UserSettings:
    if ensure:
        await ensure_user(user_id)
    if is_postgres():
        async with connect() as c:
            row = await c.fetchrow(
                "SELECT * FROM user_settings WHERE user_id=$1", user_id
            )
    else:
        async with connect() as c:
            cur = await c.execute(
                "SELECT * FROM user_settings WHERE user_id=?", (user_id,)
            )
            row = await cur.fetchone()
    r = _dict(row)
    if not r:
        raise RuntimeError("settings row missing")

    # Repair legacy/corrupted risk settings written before strict validation
    # existed. Reads must never expose NaN, infinities, negative limits or
    # impossible trade counts to the executor. Values are reset to the current
    # validated defaults and persisted once so subsequent reads are clean.
    from app.services.risk_engine import (
        validate_daily_risk_limit_percent,
        validate_max_open_trades,
        validate_max_portfolio_risk_percent,
        validate_risk_percent,
    )

    cfg = get_settings()
    risk_specs = {
        "risk_per_trade_percent": (validate_risk_percent, cfg.DEFAULT_RISK_PERCENT),
        "daily_risk_limit_percent": (
            validate_daily_risk_limit_percent,
            cfg.DEFAULT_DAILY_RISK_LIMIT_PERCENT,
        ),
        "max_open_trades": (validate_max_open_trades, cfg.DEFAULT_MAX_OPEN_TRADES),
        "max_portfolio_risk_percent": (
            validate_max_portfolio_risk_percent,
            cfg.DEFAULT_MAX_PORTFOLIO_RISK,
        ),
    }
    repaired: dict[str, float | int] = {}
    for field, (validator, default_value) in risk_specs.items():
        try:
            r[field] = validator(r.get(field))
        except (TypeError, ValueError, OverflowError):
            safe_value = validator(default_value)
            r[field] = safe_value
            repaired[field] = safe_value
    if repaired:
        log.warning(
            "Repaired invalid legacy risk settings user_id=%s fields=%s",
            int(user_id),
            sorted(repaired),
        )
        for field, safe_value in repaired.items():
            await set_user_setting(int(user_id), field, safe_value)

    general_repairs: dict[str, Any] = {}

    try:
        mode = UserMode(str(r.get("mode") or "").strip().lower())
    except ValueError:
        mode = UserMode.PREVIEW
        general_repairs["mode"] = mode.value

    tp_limit = str(r.get("tp_limit") or "all").strip().lower()
    if tp_limit not in {"3", "all"}:
        tp_limit = "all"
        general_repairs["tp_limit"] = tp_limit

    try:
        tp_mode = TpMode(str(r.get("tp_mode") or "").strip().lower())
    except ValueError:
        tp_mode = TpMode(str(cfg.DEFAULT_TP_MODE).strip().lower())
        general_repairs["tp_mode"] = tp_mode.value

    bool_defaults = {
        "exclude_be_trades_from_risk": bool(cfg.DEFAULT_EXCLUDE_BE_TRADES_FROM_RISK),
        "be_after_tp1_enabled": bool(cfg.DEFAULT_BE_AFTER_TP1),
        "use_signal_tp_percents": False,
        "skip_trade_notifications_enabled": False,
    }
    normalized_bools: dict[str, bool] = {}
    for field, default_value in bool_defaults.items():
        try:
            normalized_bools[field] = _strict_bool_setting(r.get(field), field=field)
        except ValueError:
            normalized_bools[field] = default_value
            general_repairs[field] = int(default_value)

    try:
        be_trigger = _strict_integer_setting(
            r.get("be_trigger_tp_index"),
            field="be_trigger_tp_index",
            allowed={0, 1, 2, 3},
        )
    except ValueError:
        if normalized_bools["be_after_tp1_enabled"]:
            try:
                be_trigger = _strict_integer_setting(
                    cfg.DEFAULT_BE_TRIGGER_TP_INDEX,
                    field="DEFAULT_BE_TRIGGER_TP_INDEX",
                    allowed={0, 1, 2, 3},
                )
            except ValueError:
                be_trigger = 1
        else:
            be_trigger = 0
        general_repairs["be_trigger_tp_index"] = be_trigger
    if not normalized_bools["be_after_tp1_enabled"] and be_trigger != 0:
        be_trigger = 0
        general_repairs["be_trigger_tp_index"] = 0

    try:
        manual_raw = json.loads(r.get("manual_tp_percents") or "[]")
        manual = _validated_manual_tp_percents(manual_raw)
    except (TypeError, ValueError, OverflowError, json.JSONDecodeError):
        manual = []
        general_repairs["manual_tp_percents"] = manual

    # MANUAL without a valid non-empty 100% distribution would crash the next
    # signal execution. Repair the mode together with the corrupted payload.
    if tp_mode == TpMode.MANUAL and not manual:
        try:
            safe_tp_mode = TpMode(str(cfg.DEFAULT_TP_MODE).strip().lower())
        except ValueError:
            safe_tp_mode = TpMode.BELL
        if safe_tp_mode == TpMode.MANUAL:
            safe_tp_mode = TpMode.BELL
        tp_mode = safe_tp_mode
        general_repairs["tp_mode"] = tp_mode.value

    try:
        limit_ttl = _strict_integer_setting(
            r.get("limit_ttl_hours"),
            field="limit_ttl_hours",
            minimum=0,
            maximum=168,
        )
    except ValueError:
        # Preserve the historical safe repair target for already-corrupted
        # rows. Runtime LIMIT_ORDER_TTL_HOURS controls only newly created users;
        # it must not silently rewrite existing saved policies.
        limit_ttl = 24
        general_repairs["limit_ttl_hours"] = limit_ttl

    limit_tp_mode = str(r.get("limit_tp_invalidation_mode") or "half").strip().lower()
    if limit_tp_mode not in {"none", "tp1", "tp2", "half", "last"}:
        limit_tp_mode = "half"
        general_repairs["limit_tp_invalidation_mode"] = limit_tp_mode

    if general_repairs:
        log.warning(
            "Repaired invalid legacy user settings user_id=%s fields=%s",
            int(user_id),
            sorted(general_repairs),
        )
        for field, safe_value in general_repairs.items():
            await set_user_setting(int(user_id), field, safe_value)

    return UserSettings(
        telegram_id=user_id,
        exchange=(
            str(r.get("exchange") or cfg.safe_default_exchange).lower()
            if str(r.get("exchange") or "").lower() in {"bingx"}
            else cfg.safe_default_exchange
        ),
        mode=mode,
        risk_per_trade_percent=float(r["risk_per_trade_percent"]),
        daily_risk_limit_percent=float(r["daily_risk_limit_percent"]),
        max_open_trades=int(r["max_open_trades"]),
        max_portfolio_risk_percent=float(r["max_portfolio_risk_percent"]),
        exclude_be_trades_from_risk=normalized_bools["exclude_be_trades_from_risk"],
        tp_limit=tp_limit,
        tp_mode=tp_mode,
        be_after_tp1_enabled=normalized_bools["be_after_tp1_enabled"],
        be_trigger_tp_index=be_trigger,
        use_signal_tp_percents=normalized_bools["use_signal_tp_percents"],
        skip_trade_notifications_enabled=normalized_bools[
            "skip_trade_notifications_enabled"
        ],
        manual_tp_percents=manual,
        limit_ttl_hours=limit_ttl,
        limit_tp_invalidation_mode=limit_tp_mode,
        limit_policy_preset=str(r.get("limit_policy_preset") or "balanced").lower(),
    )


async def set_user_setting(user_id: int, key: str, value: Any) -> None:
    allowed = {
        "exchange",
        "mode",
        "risk_per_trade_percent",
        "daily_risk_limit_percent",
        "max_open_trades",
        "max_portfolio_risk_percent",
        "exclude_be_trades_from_risk",
        "tp_limit",
        "tp_mode",
        "be_after_tp1_enabled",
        "be_trigger_tp_index",
        "use_signal_tp_percents",
        "skip_trade_notifications_enabled",
        "manual_tp_percents",
        "limit_ttl_hours",
        "limit_tp_invalidation_mode",
        "limit_policy_preset",
    }
    if key not in allowed:
        raise ValueError("unsupported setting")
    if key == "exchange":
        requested = str(value or "").strip().lower()
        if requested != _BingX_EXCHANGE:
            raise ValueError("BingX is the only supported exchange")
        value = _BingX_EXCHANGE
    if key == "mode":
        value = UserMode(str(value or "").strip().lower()).value
    if key == "tp_limit":
        value = str(value or "all").strip().lower()
        if value not in {"3", "all"}:
            raise ValueError("tp_limit должен быть 3 или all")
    if key == "tp_mode":
        value = TpMode(str(value or "").strip().lower()).value
    if key in {
        "exclude_be_trades_from_risk",
        "be_after_tp1_enabled",
        "use_signal_tp_percents",
        "skip_trade_notifications_enabled",
    }:
        value = int(_strict_bool_setting(value, field=key))
    if key == "be_trigger_tp_index":
        value = _strict_integer_setting(
            value,
            field="be_trigger_tp_index",
            allowed={0, 1, 2, 3},
        )
    if key == "risk_per_trade_percent":
        from app.services.risk_engine import validate_risk_percent

        value = validate_risk_percent(value)
    if key == "daily_risk_limit_percent":
        from app.services.risk_engine import validate_daily_risk_limit_percent

        value = validate_daily_risk_limit_percent(value)
    if key == "max_portfolio_risk_percent":
        from app.services.risk_engine import validate_max_portfolio_risk_percent

        value = validate_max_portfolio_risk_percent(value)
    if key == "max_open_trades":
        from app.services.risk_engine import validate_max_open_trades

        value = validate_max_open_trades(value)
    if key == "limit_ttl_hours":
        value = _strict_integer_setting(
            value,
            field="limit_ttl_hours",
            minimum=0,
            maximum=168,
        )
    if key == "limit_tp_invalidation_mode":
        value = str(value or "last").strip().lower()
        if value not in {"none", "tp1", "tp2", "half", "last"}:
            raise ValueError("unsupported LIMIT TP invalidation mode")
    if key == "limit_policy_preset":
        value = str(value or "custom").strip().lower()[:32]
    if key == "manual_tp_percents":
        value = json.dumps(_validated_manual_tp_percents(value))
    await ensure_user(user_id)
    if is_postgres():
        async with connect() as c:
            await c.execute(
                f"UPDATE user_settings SET {key}=$1 WHERE user_id=$2", value, user_id
            )
    else:
        async with connect() as c:
            await c.execute(
                f"UPDATE user_settings SET {key}=? WHERE user_id=?", (value, user_id)
            )
            await c.commit()
    # Invalidate in-memory user_settings cache for this user.
    try:
        from app.services.ttl_cache import invalidate_user

        invalidate_user(int(user_id))
    except Exception:
        # Best-effort cache invalidation — if the TTL cache module isn't
        # importable yet (startup race) or already invalidated, that's fine:
        # stale entries expire on their own within seconds via TTL.
        pass


def _normalize_api_exchange(exchange: str | None = None) -> str:
    normalized = str(exchange or _BingX_EXCHANGE).lower().strip()
    if normalized not in {"bingx", "mexc"}:
        raise ValueError("BingX is the only supported exchange")
    return _BingX_EXCHANGE


_API_KEY_FIELDS = (
    "user_id",
    "exchange",
    "api_key_encrypted",
    "api_secret_encrypted",
    "passphrase_encrypted",
    "testnet",
    "enabled",
    "created_at",
    "updated_at",
)
_QUARANTINE_FIELDS = (
    "active",
    "error_code",
    "error_message",
    "endpoint",
    "credential_fingerprint",
    "incident_token",
    "hit_count",
    "first_detected_at",
    "last_detected_at",
    "user_notified_at",
    "admin_notified_at",
    "user_notify_claimed_at",
    "admin_notify_claimed_at",
    "cleared_at",
    "clear_reason",
)


def api_credential_fingerprint(api_row: Dict[str, Any] | None) -> str:
    """Return a non-secret identity for one stored encrypted credential pair."""

    row = dict(api_row or {})
    key = str(row.get("api_key_encrypted") or "")
    secret = str(row.get("api_secret_encrypted") or "")
    if not key or not secret:
        return ""
    return hashlib.sha256(f"{key}\0{secret}".encode("utf-8")).hexdigest()


def _api_key_from_join(row: Any) -> Dict[str, Any]:
    data = _dict(row)
    return {field: data.get(field) for field in _API_KEY_FIELDS if field in data}


def _quarantine_from_join(row: Any) -> Dict[str, Any] | None:
    data = _dict(row)
    if int(data.get("quarantine_active") or 0) != 1:
        return None
    marker: Dict[str, Any] = {
        "api_quarantined": True,
        "user_id": data.get("user_id"),
        "exchange": data.get("exchange"),
    }
    for field in _QUARANTINE_FIELDS:
        marker[field] = data.get(f"quarantine_{field}")
    return marker


def _notification_columns(audience: str) -> tuple[str, str]:
    normalized = str(audience or "").lower().strip()
    if normalized == "user":
        return "user_notified_at", "user_notify_claimed_at"
    if normalized == "admin":
        return "admin_notified_at", "admin_notify_claimed_at"
    raise ValueError("audience must be user or admin")


async def get_active_api_key_quarantine(
    user_id: int, exchange: str | None = None
) -> Optional[Dict[str, Any]]:
    """Return a valid active marker bound to the current enabled credential."""

    normalized = _normalize_api_exchange(exchange)
    row = await get_api_key(int(user_id), normalized, include_quarantine=True)
    if row and row.get("api_quarantined") is True:
        return row
    return None


async def quarantine_api_key_permission(
    user_id: int,
    *,
    exchange: str | None = None,
    error_code: str = "100004",
    error_message: str = "Permission denied",
    endpoint: str = "",
    credential_fingerprint: str = "",
) -> Dict[str, Any]:
    """Durably quarantine new ENTRY after exact BingX permission code 100004.

    Credential comparison and quarantine upsert are serialized with API reconnects.
    This prevents a late response from an old key from quarantining a newly saved key.
    """

    raw_exchange = str(exchange or _BingX_EXCHANGE).lower().strip()
    code = str(error_code or "")[:64].strip()
    endpoint_text = str(endpoint or "")[:300]
    if raw_exchange != _BingX_EXCHANGE:
        return {
            "ignored_exchange": True,
            "newly_quarantined": False,
            "error_code": code,
            "endpoint": endpoint_text,
        }
    normalized = _normalize_api_exchange(raw_exchange)
    uid = int(user_id)
    if code != "100004":
        return {
            "ignored_error_code": True,
            "newly_quarantined": False,
            "error_code": code,
            "endpoint": endpoint_text,
        }

    message = str(error_message or "Permission denied")[:1000]
    expected_fingerprint = str(credential_fingerprint or "")[:128]
    if not expected_fingerprint:
        return {
            "stale_credential": True,
            "credential_identity_missing": True,
            "newly_quarantined": False,
            "error_code": code,
            "endpoint": endpoint_text,
        }
    new_incident_token = uuid.uuid4().hex
    was_active = False
    result_row: Dict[str, Any] = {}

    if is_postgres():
        async with connect() as c:
            async with c.transaction():
                current_key = await c.fetchrow(
                    "SELECT api_key_encrypted, api_secret_encrypted "
                    "FROM user_api_keys "
                    "WHERE user_id=$1 AND exchange=$2 AND enabled=1 FOR UPDATE",
                    uid,
                    normalized,
                )
                current_fingerprint = api_credential_fingerprint(
                    _dict(current_key) if current_key else None
                )
                if not current_fingerprint:
                    return {
                        "stale_credential": True,
                        "credential_missing": True,
                        "newly_quarantined": False,
                        "error_code": code,
                        "endpoint": endpoint_text,
                    }
                if expected_fingerprint and current_fingerprint != expected_fingerprint:
                    return {
                        "stale_credential": True,
                        "newly_quarantined": False,
                        "error_code": code,
                        "endpoint": endpoint_text,
                    }
                previous = await c.fetchrow(
                    "SELECT active, credential_fingerprint FROM api_key_quarantines "
                    "WHERE user_id=$1 AND exchange=$2 FOR UPDATE",
                    uid,
                    normalized,
                )
                was_active = bool(
                    previous
                    and int(previous.get("active") or 0) == 1
                    and str(previous.get("credential_fingerprint") or "")
                    == current_fingerprint
                )
                row = await c.fetchrow(
                    """INSERT INTO api_key_quarantines(
                           user_id, exchange, active, error_code, error_message,
                           endpoint, credential_fingerprint, incident_token, hit_count,
                           first_detected_at, last_detected_at, user_notified_at,
                           admin_notified_at, user_notify_claimed_at,
                           admin_notify_claimed_at, cleared_at, clear_reason
                       ) VALUES($1,$2,1,$3,$4,$5,$6,$7,1,NOW(),NOW(),NULL,NULL,NULL,NULL,NULL,NULL)
                       ON CONFLICT(user_id, exchange) DO UPDATE SET
                           active=1,
                           error_code=EXCLUDED.error_code,
                           error_message=EXCLUDED.error_message,
                           endpoint=EXCLUDED.endpoint,
                           credential_fingerprint=EXCLUDED.credential_fingerprint,
                           incident_token=CASE
                               WHEN api_key_quarantines.active=1
                                AND api_key_quarantines.credential_fingerprint=EXCLUDED.credential_fingerprint
                               THEN COALESCE(NULLIF(api_key_quarantines.incident_token,''), EXCLUDED.incident_token)
                               ELSE EXCLUDED.incident_token END,
                           hit_count=CASE
                               WHEN api_key_quarantines.active=1
                                AND api_key_quarantines.credential_fingerprint=EXCLUDED.credential_fingerprint
                               THEN api_key_quarantines.hit_count + 1 ELSE 1 END,
                           first_detected_at=CASE
                               WHEN api_key_quarantines.active=1
                                AND api_key_quarantines.credential_fingerprint=EXCLUDED.credential_fingerprint
                               THEN api_key_quarantines.first_detected_at ELSE NOW() END,
                           last_detected_at=NOW(),
                           user_notified_at=CASE
                               WHEN api_key_quarantines.active=1
                                AND api_key_quarantines.credential_fingerprint=EXCLUDED.credential_fingerprint
                               THEN api_key_quarantines.user_notified_at ELSE NULL END,
                           admin_notified_at=CASE
                               WHEN api_key_quarantines.active=1
                                AND api_key_quarantines.credential_fingerprint=EXCLUDED.credential_fingerprint
                               THEN api_key_quarantines.admin_notified_at ELSE NULL END,
                           user_notify_claimed_at=CASE
                               WHEN api_key_quarantines.active=1
                                AND api_key_quarantines.credential_fingerprint=EXCLUDED.credential_fingerprint
                               THEN api_key_quarantines.user_notify_claimed_at ELSE NULL END,
                           admin_notify_claimed_at=CASE
                               WHEN api_key_quarantines.active=1
                                AND api_key_quarantines.credential_fingerprint=EXCLUDED.credential_fingerprint
                               THEN api_key_quarantines.admin_notify_claimed_at ELSE NULL END,
                           cleared_at=NULL,
                           clear_reason=NULL
                       RETURNING *""",
                    uid,
                    normalized,
                    code,
                    message,
                    endpoint_text,
                    expected_fingerprint or current_fingerprint,
                    new_incident_token,
                )
                result_row = _dict(row)
    else:
        async with connect() as c:
            await c.execute("BEGIN IMMEDIATE")
            try:
                cur = await c.execute(
                    "SELECT api_key_encrypted, api_secret_encrypted "
                    "FROM user_api_keys WHERE user_id=? AND exchange=? AND enabled=1",
                    (uid, normalized),
                )
                current_key = await cur.fetchone()
                current_fingerprint = api_credential_fingerprint(
                    _dict(current_key) if current_key else None
                )
                if not current_fingerprint:
                    await c.rollback()
                    return {
                        "stale_credential": True,
                        "credential_missing": True,
                        "newly_quarantined": False,
                        "error_code": code,
                        "endpoint": endpoint_text,
                    }
                if expected_fingerprint and current_fingerprint != expected_fingerprint:
                    await c.rollback()
                    return {
                        "stale_credential": True,
                        "newly_quarantined": False,
                        "error_code": code,
                        "endpoint": endpoint_text,
                    }
                cur = await c.execute(
                    "SELECT active, credential_fingerprint FROM api_key_quarantines "
                    "WHERE user_id=? AND exchange=?",
                    (uid, normalized),
                )
                previous = await cur.fetchone()
                was_active = bool(
                    previous
                    and int(previous[0] or 0) == 1
                    and str(previous[1] or "") == current_fingerprint
                )
                await c.execute(
                    """INSERT INTO api_key_quarantines(
                           user_id, exchange, active, error_code, error_message,
                           endpoint, credential_fingerprint, incident_token, hit_count,
                           first_detected_at, last_detected_at, user_notified_at,
                           admin_notified_at, user_notify_claimed_at,
                           admin_notify_claimed_at, cleared_at, clear_reason
                       ) VALUES(?,?,1,?,?,?,?,?,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,NULL,NULL,NULL,NULL,NULL,NULL)
                       ON CONFLICT(user_id, exchange) DO UPDATE SET
                           active=1,
                           error_code=excluded.error_code,
                           error_message=excluded.error_message,
                           endpoint=excluded.endpoint,
                           credential_fingerprint=excluded.credential_fingerprint,
                           incident_token=CASE
                               WHEN api_key_quarantines.active=1
                                AND api_key_quarantines.credential_fingerprint=excluded.credential_fingerprint
                               THEN COALESCE(NULLIF(api_key_quarantines.incident_token,''), excluded.incident_token)
                               ELSE excluded.incident_token END,
                           hit_count=CASE
                               WHEN api_key_quarantines.active=1
                                AND api_key_quarantines.credential_fingerprint=excluded.credential_fingerprint
                               THEN api_key_quarantines.hit_count + 1 ELSE 1 END,
                           first_detected_at=CASE
                               WHEN api_key_quarantines.active=1
                                AND api_key_quarantines.credential_fingerprint=excluded.credential_fingerprint
                               THEN api_key_quarantines.first_detected_at ELSE CURRENT_TIMESTAMP END,
                           last_detected_at=CURRENT_TIMESTAMP,
                           user_notified_at=CASE
                               WHEN api_key_quarantines.active=1
                                AND api_key_quarantines.credential_fingerprint=excluded.credential_fingerprint
                               THEN api_key_quarantines.user_notified_at ELSE NULL END,
                           admin_notified_at=CASE
                               WHEN api_key_quarantines.active=1
                                AND api_key_quarantines.credential_fingerprint=excluded.credential_fingerprint
                               THEN api_key_quarantines.admin_notified_at ELSE NULL END,
                           user_notify_claimed_at=CASE
                               WHEN api_key_quarantines.active=1
                                AND api_key_quarantines.credential_fingerprint=excluded.credential_fingerprint
                               THEN api_key_quarantines.user_notify_claimed_at ELSE NULL END,
                           admin_notify_claimed_at=CASE
                               WHEN api_key_quarantines.active=1
                                AND api_key_quarantines.credential_fingerprint=excluded.credential_fingerprint
                               THEN api_key_quarantines.admin_notify_claimed_at ELSE NULL END,
                           cleared_at=NULL,
                           clear_reason=NULL""",
                    (
                        uid,
                        normalized,
                        code,
                        message,
                        endpoint_text,
                        expected_fingerprint or current_fingerprint,
                        new_incident_token,
                    ),
                )
                cur = await c.execute(
                    "SELECT * FROM api_key_quarantines "
                    "WHERE user_id=? AND exchange=? AND active=1",
                    (uid, normalized),
                )
                result_row = _dict(await cur.fetchone())
                await c.commit()
            except BaseException:
                await c.rollback()
                raise

    try:
        from app.services.ttl_cache import invalidate_user

        invalidate_user(uid)
    except Exception:
        pass
    result_row["newly_quarantined"] = not was_active
    return result_row


async def claim_api_key_quarantine_notification(
    user_id: int,
    audience: str,
    incident_token: str,
    *,
    exchange: str | None = None,
    lease_seconds: int = 300,
) -> bool:
    """Atomically claim one user/admin notification for an active incident."""

    normalized = _normalize_api_exchange(exchange)
    notified_column, claim_column = _notification_columns(audience)
    token = str(incident_token or "")[:128]
    if not token:
        return False
    lease = max(30, min(int(lease_seconds or 300), 3600))
    if is_postgres():
        async with connect() as c:
            row = await c.fetchrow(
                f"UPDATE api_key_quarantines SET {claim_column}=NOW() "
                "WHERE user_id=$1 AND exchange=$2 AND active=1 "
                "AND incident_token=$3 "
                f"AND {notified_column} IS NULL "
                f"AND ({claim_column} IS NULL OR {claim_column} < NOW() - ($4::double precision * INTERVAL '1 second')) "
                "RETURNING incident_token",
                int(user_id),
                normalized,
                token,
                lease,
            )
            return bool(row)
    async with connect() as c:
        cur = await c.execute(
            f"UPDATE api_key_quarantines SET {claim_column}=CURRENT_TIMESTAMP "
            "WHERE user_id=? AND exchange=? AND active=1 "
            "AND incident_token=? "
            f"AND {notified_column} IS NULL "
            f"AND ({claim_column} IS NULL OR {claim_column} < datetime('now', ?))",
            (int(user_id), normalized, token, f"-{lease} seconds"),
        )
        await c.commit()
        return bool(cur.rowcount)


async def release_api_key_quarantine_notification_claim(
    user_id: int,
    audience: str,
    incident_token: str,
    *,
    exchange: str | None = None,
) -> bool:
    normalized = _normalize_api_exchange(exchange)
    notified_column, claim_column = _notification_columns(audience)
    token = str(incident_token or "")[:128]
    if not token:
        return False
    if is_postgres():
        async with connect() as c:
            result = await c.execute(
                f"UPDATE api_key_quarantines SET {claim_column}=NULL "
                "WHERE user_id=$1 AND exchange=$2 AND active=1 "
                "AND incident_token=$3 "
                f"AND {notified_column} IS NULL",
                int(user_id),
                normalized,
                token,
            )
            return str(result).endswith("1")
    async with connect() as c:
        cur = await c.execute(
            f"UPDATE api_key_quarantines SET {claim_column}=NULL "
            "WHERE user_id=? AND exchange=? AND active=1 "
            "AND incident_token=? "
            f"AND {notified_column} IS NULL",
            (int(user_id), normalized, token),
        )
        await c.commit()
        return bool(cur.rowcount)


async def mark_api_key_quarantine_notified(
    user_id: int,
    audience: str,
    *,
    exchange: str | None = None,
    incident_token: str = "",
) -> bool:
    """Mark delivery only for the exact active quarantine incident."""

    normalized = _normalize_api_exchange(exchange)
    notified_column, claim_column = _notification_columns(audience)
    token = str(incident_token or "")[:128]
    if not token:
        return False
    if is_postgres():
        async with connect() as c:
            result = await c.execute(
                f"UPDATE api_key_quarantines SET {notified_column}=COALESCE({notified_column}, NOW()), "
                f"{claim_column}=NULL WHERE user_id=$1 AND exchange=$2 "
                "AND active=1 AND incident_token=$3",
                int(user_id),
                normalized,
                token,
            )
            return str(result).endswith("1")
    async with connect() as c:
        cur = await c.execute(
            f"UPDATE api_key_quarantines SET {notified_column}=COALESCE({notified_column}, CURRENT_TIMESTAMP), "
            f"{claim_column}=NULL WHERE user_id=? AND exchange=? "
            "AND active=1 AND incident_token=?",
            (int(user_id), normalized, token),
        )
        await c.commit()
        return bool(cur.rowcount)


async def clear_api_key_quarantine(
    user_id: int,
    *,
    exchange: str | None = None,
    reason: str = "api_reconnected",
) -> None:
    normalized = _normalize_api_exchange(exchange)
    reason_text = str(reason or "api_reconnected")[:300]
    if is_postgres():
        async with connect() as c:
            await c.execute(
                "UPDATE api_key_quarantines SET active=0, cleared_at=NOW(), "
                "clear_reason=$3, user_notify_claimed_at=NULL, "
                "admin_notify_claimed_at=NULL "
                "WHERE user_id=$1 AND exchange=$2 AND active=1",
                int(user_id),
                normalized,
                reason_text,
            )
    else:
        async with connect() as c:
            await c.execute(
                "UPDATE api_key_quarantines SET active=0, cleared_at=CURRENT_TIMESTAMP, "
                "clear_reason=?, user_notify_claimed_at=NULL, "
                "admin_notify_claimed_at=NULL "
                "WHERE user_id=? AND exchange=? AND active=1",
                (reason_text, int(user_id), normalized),
            )
            await c.commit()
    try:
        from app.services.ttl_cache import invalidate_user

        invalidate_user(int(user_id))
    except Exception:
        pass


async def save_api_key(
    user_id: int,
    api_key_enc: str,
    api_secret_enc: str,
    passphrase_enc: str | None = None,
    exchange: str = "bingx",
    testnet: bool = False,
) -> None:
    exchange = (exchange or "bingx").lower().strip()
    if exchange not in {"bingx", "mexc"}:
        raise ValueError("BingX is the only supported exchange")
    exchange = _BingX_EXCHANGE
    uid = int(user_id)
    await ensure_user(uid)
    if is_postgres():
        async with connect() as c:
            async with c.transaction():
                await c.execute(
                    """INSERT INTO user_api_keys(user_id, exchange, api_key_encrypted, api_secret_encrypted, passphrase_encrypted, testnet, enabled)
                    VALUES($1,$2,$3,$4,$5,$6,1)
                    ON CONFLICT(user_id, exchange) DO UPDATE SET api_key_encrypted=EXCLUDED.api_key_encrypted, api_secret_encrypted=EXCLUDED.api_secret_encrypted, passphrase_encrypted=EXCLUDED.passphrase_encrypted, testnet=EXCLUDED.testnet, enabled=1, updated_at=NOW()""",
                    uid,
                    exchange,
                    api_key_enc,
                    api_secret_enc,
                    passphrase_enc,
                    int(testnet),
                )
                await c.execute(
                    "UPDATE api_key_quarantines SET active=0, cleared_at=NOW(), "
                    "clear_reason='verified_api_reconnect', "
                    "user_notify_claimed_at=NULL, admin_notify_claimed_at=NULL "
                    "WHERE user_id=$1 AND exchange=$2 AND active=1",
                    uid,
                    exchange,
                )
    else:
        async with connect() as c:
            await c.execute("BEGIN IMMEDIATE")
            try:
                await c.execute(
                    """INSERT INTO user_api_keys(user_id, exchange, api_key_encrypted, api_secret_encrypted, passphrase_encrypted, testnet, enabled)
                    VALUES (?, ?, ?, ?, ?, ?, 1)
                    ON CONFLICT(user_id, exchange) DO UPDATE SET api_key_encrypted=excluded.api_key_encrypted, api_secret_encrypted=excluded.api_secret_encrypted, passphrase_encrypted=excluded.passphrase_encrypted, testnet=excluded.testnet, enabled=1, updated_at=CURRENT_TIMESTAMP""",
                    (
                        uid,
                        exchange,
                        api_key_enc,
                        api_secret_enc,
                        passphrase_enc,
                        int(testnet),
                    ),
                )
                await c.execute(
                    "UPDATE api_key_quarantines SET active=0, "
                    "cleared_at=CURRENT_TIMESTAMP, "
                    "clear_reason='verified_api_reconnect', "
                    "user_notify_claimed_at=NULL, admin_notify_claimed_at=NULL "
                    "WHERE user_id=? AND exchange=? AND active=1",
                    (uid, exchange),
                )
                await c.commit()
            except BaseException:
                await c.rollback()
                raise

    try:
        from app.services.ttl_cache import invalidate_user

        invalidate_user(uid)
    except Exception:
        pass


async def disable_api_key(user_id: int, exchange: str | None = None) -> None:
    requested = (exchange or "").lower().strip()
    if requested and requested != _BingX_EXCHANGE:
        raise ValueError("BingX is the only supported exchange")
    normalized = _BingX_EXCHANGE
    uid = int(user_id)
    if is_postgres():
        async with connect() as c:
            async with c.transaction():
                await c.execute(
                    "UPDATE user_api_keys SET enabled=0 "
                    "WHERE user_id=$1 AND exchange=$2",
                    uid,
                    normalized,
                )
                await c.execute(
                    "UPDATE api_key_quarantines SET active=0, cleared_at=NOW(), "
                    "clear_reason='manual_api_disable', "
                    "user_notify_claimed_at=NULL, admin_notify_claimed_at=NULL "
                    "WHERE user_id=$1 AND exchange=$2 AND active=1",
                    uid,
                    normalized,
                )
    else:
        async with connect() as c:
            await c.execute("BEGIN IMMEDIATE")
            try:
                await c.execute(
                    "UPDATE user_api_keys SET enabled=0 WHERE user_id=? AND exchange=?",
                    (uid, normalized),
                )
                await c.execute(
                    "UPDATE api_key_quarantines SET active=0, "
                    "cleared_at=CURRENT_TIMESTAMP, "
                    "clear_reason='manual_api_disable', "
                    "user_notify_claimed_at=NULL, admin_notify_claimed_at=NULL "
                    "WHERE user_id=? AND exchange=? AND active=1",
                    (uid, normalized),
                )
                await c.commit()
            except BaseException:
                await c.rollback()
                raise
    try:
        from app.services.ttl_cache import invalidate_user

        invalidate_user(uid)
    except Exception:
        pass


async def get_api_key(
    user_id: int,
    exchange: str | None = None,
    *,
    include_quarantine: bool = False,
) -> Optional[Dict[str, Any]]:
    exchange = (exchange or "").lower().strip()
    if not exchange:
        active = await get_user_settings(user_id)
        exchange = (active.exchange or get_settings().safe_default_exchange).lower()
    if exchange not in {"bingx", "mexc"}:
        return None
    exchange = _BingX_EXCHANGE
    uid = int(user_id)

    if not include_quarantine:
        if is_postgres():
            async with connect() as c:
                row = await c.fetchrow(
                    "SELECT * FROM user_api_keys "
                    "WHERE user_id=$1 AND exchange=$2 AND enabled=1",
                    uid,
                    exchange,
                )
                return _dict(row) if row else None
        async with connect() as c:
            cur = await c.execute(
                "SELECT * FROM user_api_keys "
                "WHERE user_id=? AND exchange=? AND enabled=1",
                (uid, exchange),
            )
            row = await cur.fetchone()
            return _dict(row) if row else None

    quarantine_select = ", ".join(
        f"q.{field} AS quarantine_{field}" for field in _QUARANTINE_FIELDS
    )
    if is_postgres():
        async with connect() as c:
            row = await c.fetchrow(
                f"SELECT k.*, {quarantine_select} FROM user_api_keys k "
                "LEFT JOIN api_key_quarantines q ON q.user_id=k.user_id "
                "AND q.exchange=k.exchange AND q.active=1 "
                "WHERE k.user_id=$1 AND k.exchange=$2 AND k.enabled=1",
                uid,
                exchange,
            )
    else:
        async with connect() as c:
            cur = await c.execute(
                f"SELECT k.*, {quarantine_select} FROM user_api_keys k "
                "LEFT JOIN api_key_quarantines q ON q.user_id=k.user_id "
                "AND q.exchange=k.exchange AND q.active=1 "
                "WHERE k.user_id=? AND k.exchange=? AND k.enabled=1",
                (uid, exchange),
            )
            row = await cur.fetchone()
    if not row:
        return None

    api_row = _api_key_from_join(row)
    marker = _quarantine_from_join(row)
    if marker is None:
        return api_row
    current_fingerprint = api_credential_fingerprint(api_row)
    marker_fingerprint = str(marker.get("credential_fingerprint") or "")
    if not current_fingerprint or marker_fingerprint != current_fingerprint:
        log.warning(
            "API_PERMISSION_STALE_QUARANTINE_IGNORED user_id=%s exchange=%s incident=%s",
            uid,
            exchange,
            str(marker.get("incident_token") or "-")[:32],
        )
        return api_row
    return marker


def storage_backend() -> str:
    """Human-readable active storage backend."""
    return "postgres" if is_postgres() else "sqlite"


def persistence_hint() -> str:
    settings = get_settings()
    if is_postgres():
        return "✅ Постоянная память: PostgreSQL через DATABASE_URL"
    path = settings.DATABASE_PATH
    if path.startswith("/data/"):
        return "⚠️ SQLite в /data. Память сохранится после редеплоя только если в Railway подключён Volume к /data."
    if path == ":memory:" or path.startswith("/tmp/"):
        return "❌ Временная SQLite-память. После редеплоя/рестарта API и настройки пропадут."
    return f"⚠️ SQLite-файл: {path}. Убедись, что путь находится на постоянном диске/volume."


async def list_user_api_exchanges(user_id: int) -> List[str]:
    if is_postgres():
        async with connect() as c:
            rows = await c.fetch(
                "SELECT exchange FROM user_api_keys "
                "WHERE user_id=$1 AND enabled=1 "
                "AND exchange='bingx' ORDER BY exchange",
                user_id,
            )
            return [str(r["exchange"]).upper() for r in rows]
    async with connect() as c:
        cur = await c.execute(
            "SELECT exchange FROM user_api_keys "
            "WHERE user_id=? AND enabled=1 "
            "AND exchange='bingx' ORDER BY exchange",
            (user_id,),
        )
        rows = await cur.fetchall()
        return [str(r[0]).upper() for r in rows]


async def auto_recipients() -> List[int]:
    """Return user_ids of users who should execute the signal.

    A user qualifies when:
      * ``mode = 'auto'``
      * AND they are whitelisted for THEIR ACTIVE exchange (the one set via
        ``vip биржа …``).  This combines the legacy global ``whitelisted=1``
        flag with the new per-exchange ``whitelisted_exchanges`` CSV.
    """
    rows: List[tuple[int, str, int, Any]] = []
    if is_postgres():
        async with connect() as c:
            db_rows = await c.fetch(
                "SELECT user_id, COALESCE(exchange,'bingx') AS ex, "
                "COALESCE(whitelisted,0) AS w, whitelisted_exchanges AS wex "
                "FROM user_settings WHERE mode='auto'"
            )
            rows = [
                (int(r["user_id"]), str(r["ex"]), int(r["w"]), r["wex"])
                for r in db_rows
            ]
    else:
        async with connect() as c:
            cur = await c.execute(
                "SELECT user_id, COALESCE(exchange,'bingx'), COALESCE(whitelisted,0), "
                "whitelisted_exchanges FROM user_settings WHERE mode='auto'"
            )
            for r in await cur.fetchall():
                rows.append((int(r[0]), str(r[1]), int(r[2]), r[3]))
    return [uid for uid, ex, w, wex in rows if _is_whitelisted_for_exchange(w, wex, ex)]


async def preview_recipients() -> List[int]:
    """Return user_ids who should see preview-only (no trade execution).

    Includes:
      * ``mode='preview'`` regardless of whitelist
      * ``mode='auto'`` but NOT whitelisted for their active exchange
        (silently demoted to preview to avoid losing the signal).
    """
    rows: List[tuple[int, str, str, int, Any]] = []
    if is_postgres():
        async with connect() as c:
            db_rows = await c.fetch(
                "SELECT user_id, COALESCE(exchange,'bingx') AS ex, "
                "COALESCE(mode,'preview') AS mode, "
                "COALESCE(whitelisted,0) AS w, whitelisted_exchanges AS wex "
                "FROM user_settings"
            )
            rows = [
                (int(r["user_id"]), str(r["ex"]), str(r["mode"]), int(r["w"]), r["wex"])
                for r in db_rows
            ]
    else:
        async with connect() as c:
            cur = await c.execute(
                "SELECT user_id, COALESCE(exchange,'bingx'), COALESCE(mode,'preview'), "
                "COALESCE(whitelisted,0), whitelisted_exchanges FROM user_settings"
            )
            for r in await cur.fetchall():
                rows.append((int(r[0]), str(r[1]), str(r[2]), int(r[3]), r[4]))
    out: List[int] = []
    seen: set[int] = set()
    for uid, ex, mode, w, wex in rows:
        # Telegram's service account 777000 and non-positive ids can never be
        # reached as private bot recipients.  Excluding them here prevents a
        # guaranteed warning on every signal without mutating user settings.
        if uid <= 0 or uid == 777000 or uid in seen:
            continue
        if mode == "preview":
            out.append(uid)
            seen.add(uid)
        elif mode == "auto" and not _is_whitelisted_for_exchange(w, wex, ex):
            out.append(uid)
            seen.add(uid)
    return out


def _parse_wl_exchanges(value: Any) -> set[str]:
    """Parse the CSV stored in user_settings.whitelisted_exchanges.

    Returns a set of normalised exchange names.  The special token "all" is
    preserved as a sentinel so callers can shortcut their checks.
    """
    if value is None:
        return set()
    s = str(value).strip().lower()
    if not s:
        return set()
    parts = {p.strip() for p in s.replace(";", ",").split(",")}
    return {p for p in parts if p}


def _wl_exchanges_csv(exchanges: set[str]) -> Optional[str]:
    """Inverse of _parse_wl_exchanges. None means "no per-exchange grant"."""
    if not exchanges:
        return None
    if "all" in exchanges:
        return "all"
    return ",".join(sorted(exchanges))


def _is_whitelisted_for_exchange(
    legacy_flag: int,
    wl_exchanges_value: Any,
    exchange: str,
) -> bool:
    """Decision rule, applied per (user, exchange):

    1. If whitelisted_exchanges is set → it is authoritative.
       - "all" grants every enabled exchange.
       - Otherwise the exchange must appear in the CSV.
    2. If whitelisted_exchanges is empty (legacy users) → fall back to the
       old global flag whitelisted=1 meaning "all exchanges".
    """
    grants = _parse_wl_exchanges(wl_exchanges_value)
    target = (exchange or "").lower().strip()
    if grants:
        return "all" in grants or target in grants
    return int(legacy_flag or 0) == 1


async def set_user_whitelisted(user_id: int, value: bool) -> None:
    """Grant or revoke the only supported trading permission: BingX."""
    await ensure_user(user_id)
    val = 1 if value else 0
    new_csv = _BingX_EXCHANGE if value else None
    if is_postgres():
        async with connect() as c:
            await c.execute(
                "UPDATE user_settings SET whitelisted=$1, whitelisted_exchanges=$2 "
                "WHERE user_id=$3",
                val,
                new_csv,
                int(user_id),
            )
    else:
        async with connect() as c:
            await c.execute(
                "UPDATE user_settings SET whitelisted=?, whitelisted_exchanges=? "
                "WHERE user_id=?",
                (val, new_csv, int(user_id)),
            )
            await c.commit()
    try:
        from app.services.ttl_cache import invalidate_user

        invalidate_user(int(user_id))
    except Exception:
        # Best-effort cache invalidation — if the TTL cache module isn't
        # importable yet (startup race) or already invalidated, that's fine:
        # stale entries expire on their own within seconds via TTL.
        pass


async def add_user_whitelist_exchange(user_id: int, exchange: str) -> set[str]:
    """Grant BingX permission while keeping the legacy function signature."""
    await ensure_user(user_id)
    ex = (exchange or "").lower().strip()
    if ex not in {_BingX_EXCHANGE, "mexc", "all"}:
        raise ValueError("BingX is the only supported exchange")
    new_set = {_BingX_EXCHANGE}
    if is_postgres():
        async with connect() as c:
            await c.execute(
                "UPDATE user_settings SET whitelisted=1, whitelisted_exchanges=$1 "
                "WHERE user_id=$2",
                _BingX_EXCHANGE,
                int(user_id),
            )
    else:
        async with connect() as c:
            await c.execute(
                "UPDATE user_settings SET whitelisted=1, whitelisted_exchanges=? "
                "WHERE user_id=?",
                (_BingX_EXCHANGE, int(user_id)),
            )
            await c.commit()
    try:
        from app.services.ttl_cache import invalidate_user

        invalidate_user(int(user_id))
    except Exception:
        pass
    return new_set


async def remove_user_whitelist_exchange(
    user_id: int,
    exchange: str,
) -> set[str]:
    """Revoke BingX permission while keeping the legacy function signature."""
    await ensure_user(user_id)
    ex = (exchange or "").lower().strip()
    if ex not in {_BingX_EXCHANGE, "mexc", "all"}:
        raise ValueError("BingX is the only supported exchange")
    if is_postgres():
        async with connect() as c:
            await c.execute(
                "UPDATE user_settings SET whitelisted=$1, whitelisted_exchanges=$2 "
                "WHERE user_id=$3",
                0,
                None,
                int(user_id),
            )
    else:
        async with connect() as c:
            await c.execute(
                "UPDATE user_settings SET whitelisted=?, whitelisted_exchanges=? "
                "WHERE user_id=?",
                (0, None, int(user_id)),
            )
            await c.commit()
    try:
        from app.services.ttl_cache import invalidate_user

        invalidate_user(int(user_id))
    except Exception:
        pass
    return set()


async def get_user_whitelist_exchanges(user_id: int) -> set[str]:
    """Return the set of exchanges this user can trade on (may contain 'all')."""
    if is_postgres():
        async with connect() as c:
            row = await c.fetchrow(
                "SELECT COALESCE(whitelisted, 0) AS w, whitelisted_exchanges AS ex "
                "FROM user_settings WHERE user_id=$1",
                int(user_id),
            )
            if not row:
                return set()
            grants = _parse_wl_exchanges(row["ex"])
            if grants:
                return (
                    {_BingX_EXCHANGE}
                    if ("all" in grants or _BingX_EXCHANGE in grants)
                    else set()
                )
            return {_BingX_EXCHANGE} if int(row["w"] or 0) == 1 else set()
    async with connect() as c:
        cur = await c.execute(
            "SELECT COALESCE(whitelisted, 0), whitelisted_exchanges "
            "FROM user_settings WHERE user_id=?",
            (int(user_id),),
        )
        row = await cur.fetchone()
        if not row:
            return set()
        grants = _parse_wl_exchanges(row[1])
        if grants:
            return (
                {_BingX_EXCHANGE}
                if ("all" in grants or _BingX_EXCHANGE in grants)
                else set()
            )
        return {_BingX_EXCHANGE} if int(row[0] or 0) == 1 else set()


async def is_user_whitelisted(user_id: int, exchange: str | None = None) -> bool:
    """Check if a user is in the whitelist.

    Backward-compatible signature: when called without ``exchange`` the
    function returns True if the user is whitelisted ANYWHERE (legacy global
    semantics).  Pass ``exchange='bingx'`` for the per-exchange check.
    """
    grants = await get_user_whitelist_exchanges(int(user_id))
    if not grants:
        return False
    if exchange is None:
        return True  # whitelisted anywhere
    return "all" in grants or (exchange or "").lower().strip() in grants


async def list_whitelisted_users() -> List[int]:
    """Return all user_ids that have ANY whitelist grant."""
    if is_postgres():
        async with connect() as c:
            rows = await c.fetch(
                "SELECT user_id FROM user_settings "
                "WHERE COALESCE(whitelisted, 0) = 1 OR "
                "(whitelisted_exchanges IS NOT NULL AND whitelisted_exchanges <> '') "
                "ORDER BY user_id"
            )
            return [int(r["user_id"]) for r in rows]
    async with connect() as c:
        cur = await c.execute(
            "SELECT user_id FROM user_settings "
            "WHERE COALESCE(whitelisted, 0) = 1 "
            "OR (whitelisted_exchanges IS NOT NULL AND whitelisted_exchanges <> '') "
            "ORDER BY user_id"
        )
        return [int(r[0]) for r in await cur.fetchall()]


async def list_users_with_exchanges() -> List[dict]:
    """Return all users with their connected BingX API and fixed BingX exchange.

    For each user returns:
      - telegram_id: int
      - username: str | None
      - is_admin: bool
      - whitelisted: bool
      - whitelist_exchanges: set[str] — BingX whitelist grant
        (may contain the sentinel 'all'); empty set means no whitelist
      - mode: str(auto / preview / off)
      - active_exchange: str — fixed exchange used for trades
      - connected_exchanges: list[str] — saved BingX API marker
      - created_at: datetime | None
    """
    if is_postgres():
        async with connect() as c:
            rows = await c.fetch(
                """
                SELECT
                    u.telegram_id,
                    u.username,
                    COALESCE(u.is_admin, 0) AS is_admin,
                    u.created_at,
                    COALESCE(s.exchange, 'bingx') AS active_exchange,
                    COALESCE(s.mode, 'preview') AS mode,
                    COALESCE(s.whitelisted, 0) AS whitelisted,
                    s.whitelisted_exchanges AS wl_ex,
                    COALESCE(
                        ARRAY(
                            SELECT k.exchange FROM user_api_keys k
                            WHERE k.user_id = u.telegram_id AND COALESCE(k.enabled, 1) = 1 AND k.exchange IN ('bingx','mexc')
                            ORDER BY k.exchange
                        ),
                        ARRAY[]::text[]
                    ) AS connected_exchanges
                FROM users u
                LEFT JOIN user_settings s ON s.user_id = u.telegram_id
                ORDER BY u.created_at DESC NULLS LAST, u.telegram_id
                """
            )
            out: List[dict] = []
            for r in rows:
                grants = _parse_wl_exchanges(r["wl_ex"])
                if not grants and bool(r["whitelisted"]):
                    grants = {"all"}
                out.append(
                    {
                        "telegram_id": int(r["telegram_id"]),
                        "username": r["username"],
                        "is_admin": bool(r["is_admin"]),
                        "created_at": r["created_at"],
                        "active_exchange": str(r["active_exchange"] or "bingx"),
                        "mode": str(r["mode"] or "preview"),
                        "whitelisted": bool(r["whitelisted"]) or bool(grants),
                        "whitelist_exchanges": grants,
                        "connected_exchanges": list(r["connected_exchanges"] or []),
                    }
                )
            return out
    async with connect() as c:
        cur = await c.execute(
            """
            SELECT
                u.telegram_id,
                u.username,
                COALESCE(u.is_admin, 0) AS is_admin,
                u.created_at,
                COALESCE(s.exchange, 'bingx') AS active_exchange,
                COALESCE(s.mode, 'preview') AS mode,
                COALESCE(s.whitelisted, 0) AS whitelisted,
                s.whitelisted_exchanges AS wl_ex
            FROM users u
            LEFT JOIN user_settings s ON s.user_id = u.telegram_id
            ORDER BY u.created_at DESC, u.telegram_id
            """
        )
        rows = await cur.fetchall()
        out: List[dict] = []
        for r in rows:
            uid = int(r[0])
            cur2 = await c.execute(
                "SELECT exchange FROM user_api_keys WHERE user_id=? AND COALESCE(enabled,1)=1 AND exchange IN ('bingx','mexc') ORDER BY exchange",
                (uid,),
            )
            keys = [str(kr[0]) for kr in await cur2.fetchall()]
            grants = _parse_wl_exchanges(r[7])
            if not grants and bool(r[6]):
                grants = {"all"}
            out.append(
                {
                    "telegram_id": uid,
                    "username": r[1],
                    "is_admin": bool(r[2]),
                    "created_at": r[3],
                    "active_exchange": str(r[4] or "bingx"),
                    "mode": str(r[5] or "preview"),
                    "whitelisted": bool(r[6]) or bool(grants),
                    "whitelist_exchanges": grants,
                    "connected_exchanges": keys,
                }
            )
        return out


async def is_duplicate(sig_hash: str, user_id: int) -> bool:
    """Return True while a signal hash is inside the configured dedup window.

    ``VIP_SIGNAL_DEDUP_TTL_SECONDS <= 0`` preserves the legacy permanent
    behavior. Positive values allow the same setup again only after the TTL.
    """
    ttl = int(get_settings().VIP_SIGNAL_DEDUP_TTL_SECONDS or 0)
    if is_postgres():
        async with connect() as c:
            if ttl > 0:
                row = await c.fetchrow(
                    """SELECT 1 FROM dedup
                    WHERE signal_hash=$1 AND user_id=$2
                      AND created_at >= NOW() - ($3 * INTERVAL '1 second')""",
                    sig_hash,
                    user_id,
                    ttl,
                )
            else:
                row = await c.fetchrow(
                    "SELECT 1 FROM dedup WHERE signal_hash=$1 AND user_id=$2",
                    sig_hash,
                    user_id,
                )
            return row is not None
    async with connect() as c:
        if ttl > 0:
            cur = await c.execute(
                """SELECT 1 FROM dedup
                WHERE signal_hash=? AND user_id=?
                  AND datetime(created_at) >= datetime('now', ?)""",
                (sig_hash, user_id, f"-{ttl} seconds"),
            )
        else:
            cur = await c.execute(
                "SELECT 1 FROM dedup WHERE signal_hash=? AND user_id=?",
                (sig_hash, user_id),
            )
        return await cur.fetchone() is not None


async def mark_duplicate(
    sig_hash: str, source_chat_id: int | None, signal_id: str | None, user_id: int
) -> None:
    """Insert or refresh a dedup marker atomically."""
    if is_postgres():
        async with connect() as c:
            await c.execute(
                """INSERT INTO dedup(signal_hash, source_chat_id, signal_id, user_id)
                VALUES($1,$2,$3,$4)
                ON CONFLICT(signal_hash, user_id) DO UPDATE SET
                  source_chat_id=EXCLUDED.source_chat_id,
                  signal_id=EXCLUDED.signal_id,
                  created_at=NOW()""",
                sig_hash,
                source_chat_id,
                signal_id,
                user_id,
            )
        return
    async with connect() as c:
        await c.execute(
            """INSERT INTO dedup(signal_hash, source_chat_id, signal_id, user_id)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(signal_hash, user_id) DO UPDATE SET
              source_chat_id=excluded.source_chat_id,
              signal_id=excluded.signal_id,
              created_at=CURRENT_TIMESTAMP""",
            (sig_hash, source_chat_id, signal_id, user_id),
        )
        await c.commit()


async def claim_duplicate(
    sig_hash: str,
    source_chat_id: int | None,
    signal_id: str | None,
    user_id: int,
) -> bool:
    """Atomically claim a signal hash for one recipient.

    Returns ``True`` only when this caller inserted the marker or refreshed an
    expired TTL marker. This closes the check-then-insert race for signal-wide
    warnings across concurrent Telegram updates or Railway processes.
    """
    ttl = int(get_settings().VIP_SIGNAL_DEDUP_TTL_SECONDS or 0)
    if is_postgres():
        async with connect() as c:
            if ttl > 0:
                row = await c.fetchrow(
                    """INSERT INTO dedup(signal_hash, source_chat_id, signal_id, user_id)
                    VALUES($1,$2,$3,$4)
                    ON CONFLICT(signal_hash, user_id) DO UPDATE SET
                      source_chat_id=EXCLUDED.source_chat_id,
                      signal_id=EXCLUDED.signal_id,
                      created_at=NOW()
                    WHERE dedup.created_at < NOW() - ($5 * INTERVAL '1 second')
                    RETURNING 1""",
                    sig_hash,
                    source_chat_id,
                    signal_id,
                    user_id,
                    ttl,
                )
            else:
                row = await c.fetchrow(
                    """INSERT INTO dedup(signal_hash, source_chat_id, signal_id, user_id)
                    VALUES($1,$2,$3,$4)
                    ON CONFLICT(signal_hash, user_id) DO NOTHING
                    RETURNING 1""",
                    sig_hash,
                    source_chat_id,
                    signal_id,
                    user_id,
                )
            return row is not None

    async with connect() as c:
        if ttl > 0:
            await c.execute(
                """INSERT INTO dedup(signal_hash, source_chat_id, signal_id, user_id)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(signal_hash, user_id) DO UPDATE SET
                  source_chat_id=excluded.source_chat_id,
                  signal_id=excluded.signal_id,
                  created_at=CURRENT_TIMESTAMP
                WHERE datetime(dedup.created_at) < datetime('now', ?)""",
                (sig_hash, source_chat_id, signal_id, user_id, f"-{ttl} seconds"),
            )
        else:
            await c.execute(
                """INSERT INTO dedup(signal_hash, source_chat_id, signal_id, user_id)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(signal_hash, user_id) DO NOTHING""",
                (sig_hash, source_chat_id, signal_id, user_id),
            )
        changes_cur = await c.execute("SELECT changes()")
        row = await changes_cur.fetchone()
        await c.commit()
        return bool(row and int(row[0] or 0) > 0)


async def release_duplicate(
    sig_hash: str,
    user_id: int,
    *,
    expected_signal_id: str | None = None,
) -> None:
    """Release only the caller's failed pre-notification claim.

    ``expected_signal_id`` acts as a claim token. Without the exact predicate a
    slow failed sender could delete a newer claim refreshed by another process
    after a very short dedup TTL.
    """
    if is_postgres():
        async with connect() as c:
            if expected_signal_id is None:
                await c.execute(
                    "DELETE FROM dedup WHERE signal_hash=$1 AND user_id=$2",
                    sig_hash,
                    user_id,
                )
            else:
                await c.execute(
                    """DELETE FROM dedup
                    WHERE signal_hash=$1 AND user_id=$2 AND signal_id=$3""",
                    sig_hash,
                    user_id,
                    expected_signal_id,
                )
        return
    async with connect() as c:
        if expected_signal_id is None:
            await c.execute(
                "DELETE FROM dedup WHERE signal_hash=? AND user_id=?",
                (sig_hash, user_id),
            )
        else:
            await c.execute(
                """DELETE FROM dedup
                WHERE signal_hash=? AND user_id=? AND signal_id=?""",
                (sig_hash, user_id, expected_signal_id),
            )
        await c.commit()


async def clear_dedup() -> None:
    if is_postgres():
        async with connect() as c:
            await c.execute("DELETE FROM dedup")
        return
    async with connect() as c:
        await c.execute("DELETE FROM dedup")
        await c.commit()


async def clear_user_dedup(user_id: int) -> int:
    """Clear dedup markers only for one user and return affected rows."""
    if is_postgres():
        async with connect() as c:
            result = await c.execute("DELETE FROM dedup WHERE user_id=$1", int(user_id))
            try:
                return int(str(result).split()[-1])
            except Exception:
                return 0
    async with connect() as c:
        cur = await c.execute("DELETE FROM dedup WHERE user_id=?", (int(user_id),))
        await c.commit()
        return int(cur.rowcount or 0) if (cur.rowcount or 0) > 0 else 0


async def log_execution(**kwargs: Any) -> int:
    """Insert one execution row and return its id.

    Existing callers may ignore the return value.  v1.6.68 uses it to persist a
    durable pre-entry `opening_intent` before the private BingX write, then
    updates the same row after read-back instead of creating a duplicate row.
    """
    keys = [
        "trade_group_id",
        "signal_hash",
        "user_id",
        "symbol",
        "side",
        "entry",
        "stop",
        "targets_json",
        "tp_distribution_json",
        "tp_distribution_source",
        "tp_distribution_locked",
        "tp_distribution_version",
        "risk_percent",
        "equity_snapshot_usd",
        "planned_risk_usd",
        "initial_price_risk_usd",
        "initial_risk_percent_of_equity",
        "estimated_fee_risk_usd",
        "expected_loss_at_stop_usd",
        "planned_entry_qty",
        "stop_distance",
        "risk_snapshot_at",
        "risk_snapshot_source",
        "risk_snapshot_status",
        "risk_snapshot_reason",
        "qty",
        "leverage",
        "status",
        "reason",
        "exchange_order_ids_json",
    ]
    kwargs["exchange_order_ids_json"] = _ensure_bingx_execution_json(
        kwargs.get("exchange_order_ids_json")
    )
    kwargs.setdefault("tp_distribution_source", "configured_pre_fill")
    kwargs.setdefault("tp_distribution_locked", 0)
    kwargs.setdefault("tp_distribution_version", 1)
    kwargs.setdefault("risk_snapshot_status", "missing")
    vals = [kwargs.get(k) for k in keys]
    if is_postgres():
        ph = ",".join(f"${i}" for i in range(1, len(keys) + 1))
        async with connect() as c:
            row = await c.fetchrow(
                f"INSERT INTO trade_executions({','.join(keys)}) VALUES ({ph}) RETURNING id",
                *vals,
            )
            return int(row["id"]) if row and row["id"] is not None else 0
    async with connect() as c:
        cur = await c.execute(
            f"INSERT INTO trade_executions({','.join(keys)}) VALUES ({','.join(['?'] * len(keys))})",
            vals,
        )
        await c.commit()
        return int(getattr(cur, "lastrowid", 0) or 0)


def _finite_optional_float(value: Any) -> float | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def _utc_datetime_or_none(value: datetime | str | None) -> datetime | None:
    parsed: datetime | None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


async def update_execution_statistics_snapshot(
    execution_id: int,
    *,
    equity_snapshot_usd: float | None = None,
    planned_risk_usd: float | None = None,
    initial_price_risk_usd: float | None = None,
    initial_risk_percent_of_equity: float | None = None,
    estimated_fee_risk_usd: float | None = None,
    expected_loss_at_stop_usd: float | None = None,
    planned_entry_qty: float | None = None,
    stop_distance: float | None = None,
    risk_snapshot_at: datetime | str | None = None,
    risk_snapshot_source: str = "unknown",
    risk_snapshot_status: str = "complete",
    risk_snapshot_reason: str | None = None,
    qty: float | None = None,
) -> bool:
    """Persist statistics-only risk evidence without changing trading state."""

    source = str(risk_snapshot_source or "unknown").strip()[:80] or "unknown"
    status = str(risk_snapshot_status or "missing").strip().lower()[:32]
    if status not in {"complete", "partial", "missing", "invalid"}:
        status = "invalid"
    reason = str(risk_snapshot_reason or "").strip()[:240] or None
    values = (
        _finite_optional_float(equity_snapshot_usd),
        _finite_optional_float(planned_risk_usd),
        _finite_optional_float(initial_price_risk_usd),
        _finite_optional_float(initial_risk_percent_of_equity),
        _finite_optional_float(estimated_fee_risk_usd),
        _finite_optional_float(expected_loss_at_stop_usd),
        _finite_optional_float(planned_entry_qty),
        _finite_optional_float(stop_distance),
        _utc_datetime_or_none(risk_snapshot_at),
        source,
        status,
        reason,
        _finite_optional_float(qty),
        int(execution_id),
    )
    if is_postgres():
        async with connect() as c:
            result = await c.execute(
                """
                UPDATE trade_executions
                SET equity_snapshot_usd=COALESCE($1,equity_snapshot_usd),
                    planned_risk_usd=COALESCE($2,planned_risk_usd),
                    initial_price_risk_usd=COALESCE($3,initial_price_risk_usd),
                    initial_risk_percent_of_equity=COALESCE($4,initial_risk_percent_of_equity),
                    estimated_fee_risk_usd=COALESCE($5,estimated_fee_risk_usd),
                    expected_loss_at_stop_usd=COALESCE($6,expected_loss_at_stop_usd),
                    planned_entry_qty=COALESCE($7,planned_entry_qty),
                    stop_distance=COALESCE($8,stop_distance),
                    risk_snapshot_at=COALESCE($9,risk_snapshot_at),
                    risk_snapshot_source=$10,
                    risk_snapshot_status=$11,
                    risk_snapshot_reason=$12,
                    qty=COALESCE($13,qty),
                    updated_at=NOW()
                WHERE id=$14
                """,
                *values,
            )
            return str(result).endswith(" 1")
    sqlite_values = list(values)
    if isinstance(sqlite_values[8], datetime):
        sqlite_values[8] = sqlite_values[8].isoformat()
    async with connect() as c:
        cursor = await c.execute(
            """
            UPDATE trade_executions
            SET equity_snapshot_usd=COALESCE(?,equity_snapshot_usd),
                planned_risk_usd=COALESCE(?,planned_risk_usd),
                initial_price_risk_usd=COALESCE(?,initial_price_risk_usd),
                initial_risk_percent_of_equity=COALESCE(?,initial_risk_percent_of_equity),
                estimated_fee_risk_usd=COALESCE(?,estimated_fee_risk_usd),
                expected_loss_at_stop_usd=COALESCE(?,expected_loss_at_stop_usd),
                planned_entry_qty=COALESCE(?,planned_entry_qty),
                stop_distance=COALESCE(?,stop_distance),
                risk_snapshot_at=COALESCE(?,risk_snapshot_at),
                risk_snapshot_source=?,
                risk_snapshot_status=?,
                risk_snapshot_reason=?,
                qty=COALESCE(?,qty),
                updated_at=CURRENT_TIMESTAMP
            WHERE id=?
            """,
            tuple(sqlite_values),
        )
        await c.commit()
        return int(cursor.rowcount or 0) == 1


def _canonical_positive_number_list(raw: str, *, max_items: int = 20) -> list[float] | None:
    try:
        values = json.loads(str(raw or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(values, list) or not values or len(values) > max_items:
        return None
    result: list[float] = []
    for value in values:
        parsed = _finite_optional_float(value)
        if parsed is None or parsed <= 0:
            return None
        result.append(parsed)
    return result


async def finalize_execution_tp_distribution(
    execution_id: int,
    *,
    targets_json: str,
    tp_distribution_json: str,
    source: str,
    version: int = 1,
) -> bool:
    """CAS-lock the exact rounded TP plan used by the execution.

    Identical repeats are idempotent. A conflicting later worker cannot rewrite
    historical allocation evidence after the plan has been locked.
    """

    targets = _canonical_positive_number_list(targets_json)
    pcts = _canonical_positive_number_list(tp_distribution_json)
    if targets is None or pcts is None or len(targets) != len(pcts):
        return False
    total = sum(pcts)
    if abs(total - 100.0) > 0.02:
        return False
    normalized_targets = [int(v) if v.is_integer() else round(v, 12) for v in targets]
    normalized_pcts = [int(v) if v.is_integer() else round(v, 8) for v in pcts]
    target_text = json.dumps(normalized_targets, ensure_ascii=False, separators=(",", ":"))
    pct_text = json.dumps(normalized_pcts, ensure_ascii=False, separators=(",", ":"))
    source_text = str(source or "execution_exact").strip()[:80] or "execution_exact"
    version_value = max(1, int(version))
    if is_postgres():
        async with connect() as c:
            result = await c.execute(
                """
                UPDATE trade_executions
                SET targets_json=$1,tp_distribution_json=$2,
                    tp_distribution_source=CASE
                      WHEN COALESCE(tp_distribution_locked,0)=1
                        THEN tp_distribution_source ELSE $3 END,
                    tp_distribution_locked=1,
                    tp_distribution_version=CASE
                      WHEN COALESCE(tp_distribution_locked,0)=1
                        THEN tp_distribution_version ELSE $4 END,
                    updated_at=NOW()
                WHERE id=$5 AND (
                  COALESCE(tp_distribution_locked,0)=0 OR (
                    targets_json=$1 AND tp_distribution_json=$2
                  )
                )
                """,
                target_text, pct_text, source_text, version_value, int(execution_id),
            )
            return str(result).endswith(" 1")
    async with connect() as c:
        cursor = await c.execute(
            """
            UPDATE trade_executions
            SET targets_json=?,tp_distribution_json=?,
                tp_distribution_source=CASE
                  WHEN COALESCE(tp_distribution_locked,0)=1
                    THEN tp_distribution_source ELSE ? END,
                tp_distribution_locked=1,
                tp_distribution_version=CASE
                  WHEN COALESCE(tp_distribution_locked,0)=1
                    THEN tp_distribution_version ELSE ? END,
                updated_at=CURRENT_TIMESTAMP
            WHERE id=? AND (
              COALESCE(tp_distribution_locked,0)=0 OR (
                targets_json=? AND tp_distribution_json=?
              )
            )
            """,
            (
                target_text, pct_text, source_text, version_value, int(execution_id),
                target_text, pct_text,
            ),
        )
        await c.commit()
        return int(cursor.rowcount or 0) == 1


async def record_execution_outcome(
    execution_id: int,
    *,
    outcome: str,
    realized_pnl: float,
    close_type: str,
) -> None:
    """Persist the final classified result used by dashboard statistics.

    ``outcome`` is one of ``win``, ``breakeven``, ``loss`` or ``unknown``.
    A BE close is stored separately so the UI can show both the user's primary
    winrate (BE counts as victory) and the strict profitable-only winrate.
    """
    normalized = str(outcome or "unknown").strip().lower()
    if normalized not in {"win", "breakeven", "loss", "unknown"}:
        normalized = "unknown"
    close_type = str(close_type or "unknown").strip().lower()[:80]
    pnl = float(realized_pnl or 0.0)
    if is_postgres():
        async with connect() as c:
            await c.execute(
                """
                UPDATE trade_executions
                SET outcome=$1, realized_pnl=$2, close_type=$3,
                    closed_at=COALESCE(closed_at, NOW()), updated_at=NOW()
                WHERE id=$4
                """,
                normalized,
                pnl,
                close_type,
                int(execution_id),
            )
        return
    async with connect() as c:
        await c.execute(
            """
            UPDATE trade_executions
            SET outcome=?, realized_pnl=?, close_type=?,
                closed_at=COALESCE(closed_at, CURRENT_TIMESTAMP),
                updated_at=CURRENT_TIMESTAMP
            WHERE id=?
            """,
            (normalized, pnl, close_type, int(execution_id)),
        )
        await c.commit()


async def user_dashboard_executions(
    user_id: int, days: int = 30
) -> List[Dict[str, Any]]:
    """Rows required for the home dashboard and 30-day winrate.

    Active rows are returned regardless of age. Closed rows are limited to the
    requested period. Legacy executions without an explicit BingX marker remain
    excluded by the existing fail-closed BingX filter.
    """
    days = max(1, min(int(days or 30), 3650))
    active_statuses = [
        "opening_intent",
        "opened",
        "pending_limit",
        "protected",
        "partial_error",
        "manual_required",
        "partial_unrecoverable",
    ]
    closed_statuses = [
        "closed_on_exchange",
        "closed_stop_catchup",
        "closed_on_exchange_cleanup",
    ]
    if is_postgres():
        async with connect() as c:
            rows = await c.fetch(
                """
                SELECT id, user_id, symbol, side, entry, stop, targets_json,
                       tp_distribution_json, risk_percent, qty, leverage, status,
                       reason, exchange_order_ids_json, outcome, realized_pnl,
                       close_type, closed_at, created_at, updated_at,
                       CASE
                         WHEN (outcome IS NOT NULL OR status = ANY($3::text[]))
                          AND COALESCE(closed_at, updated_at, created_at)
                              >= NOW() - ($4::int * INTERVAL '1 day')
                         THEN TRUE ELSE FALSE
                       END AS dashboard_stats_eligible
                FROM trade_executions
                WHERE user_id=$1
                  AND (
                    status = ANY($2::text[])
                    OR (
                      (outcome IS NOT NULL OR status = ANY($3::text[]))
                      AND COALESCE(closed_at, updated_at, created_at)
                          >= NOW() - ($4::int * INTERVAL '1 day')
                    )
                  )
                ORDER BY COALESCE(closed_at, updated_at, created_at) DESC
                """,
                int(user_id),
                active_statuses,
                closed_statuses,
                days,
            )
            return _bingx_rows([_dict(r) for r in rows])

    active_ph = ",".join(["?"] * len(active_statuses))
    closed_ph = ",".join(["?"] * len(closed_statuses))
    modifier = f"-{days} days"
    async with connect() as c:
        cur = await c.execute(
            f"""
            SELECT id, user_id, symbol, side, entry, stop, targets_json,
                   tp_distribution_json, risk_percent, qty, leverage, status,
                   reason, exchange_order_ids_json, outcome, realized_pnl,
                   close_type, closed_at, created_at, updated_at,
                   CASE
                     WHEN (outcome IS NOT NULL OR status IN ({closed_ph}))
                      AND datetime(COALESCE(closed_at, updated_at, created_at))
                          >= datetime('now', ?)
                     THEN 1 ELSE 0
                   END AS dashboard_stats_eligible
            FROM trade_executions
            WHERE user_id=?
              AND (
                status IN ({active_ph})
                OR (
                  (outcome IS NOT NULL OR status IN ({closed_ph}))
                  AND datetime(COALESCE(closed_at, updated_at, created_at))
                      >= datetime('now', ?)
                )
              )
            ORDER BY datetime(COALESCE(closed_at, updated_at, created_at)) DESC
            """,
            (
                *closed_statuses,
                modifier,
                int(user_id),
                *active_statuses,
                *closed_statuses,
                modifier,
            ),
        )
        return _bingx_rows([_dict(r) for r in await cur.fetchall()])


async def _paged_bingx_execution_rows(
    statuses: list[str] | tuple[str, ...],
    *,
    limit: int,
    after_id: int,
) -> List[Dict[str, Any]]:
    """Fill a rotating page only after explicit-BingX filtering.

    The old fixed ``limit * 4`` SQL window could consist entirely of legacy or
    malformed non-BingX rows. The worker then received an empty page, reset its
    cursor to zero and permanently starved valid BingX executions with larger
    ids. Raw rows are now scanned in stable pages until the requested BingX page
    is full or the table is exhausted.
    """
    requested = int(limit or 0)
    if requested <= 0:
        return []
    wanted = min(requested, 5000)
    cursor = max(0, int(after_id or 0))
    page_size = max(200, min(2000, wanted * 4))
    result: list[Dict[str, Any]] = []
    status_values = [str(value) for value in statuses if str(value)]
    if not status_values:
        return result

    if is_postgres():
        async with connect() as c:
            while len(result) < wanted:
                rows = await c.fetch(
                    """SELECT * FROM trade_executions
                    WHERE status = ANY($1::text[]) AND id>$2
                    ORDER BY id ASC LIMIT $3""",
                    status_values,
                    cursor,
                    page_size,
                )
                raw = [_dict(row) for row in rows]
                if not raw:
                    break
                next_cursor = int(raw[-1].get("id") or 0)
                if next_cursor <= cursor:
                    break
                cursor = next_cursor
                result.extend(_bingx_rows(raw))
                if len(raw) < page_size:
                    break
        return result[:wanted]

    placeholders = ",".join(["?"] * len(status_values))
    async with connect() as c:
        while len(result) < wanted:
            cur = await c.execute(
                f"""SELECT * FROM trade_executions
                WHERE status IN ({placeholders}) AND id>?
                ORDER BY id ASC LIMIT ?""",
                (*status_values, cursor, page_size),
            )
            raw = [_dict(row) for row in await cur.fetchall()]
            if not raw:
                break
            next_cursor = int(raw[-1].get("id") or 0)
            if next_cursor <= cursor:
                break
            cursor = next_cursor
            result.extend(_bingx_rows(raw))
            if len(raw) < page_size:
                break
    return result[:wanted]


_CRITICAL_MANUAL_BACKOFF_DELAYS_SEC = (30, 60, 120, 300)
_CRITICAL_CLEANUP_RETRY_DELAYS_SEC = (60, 300, 900, 3600)
_CRITICAL_ZERO_PROOF_KEY = "critical_zero_exposure_v1"
_CRITICAL_CLEANUP_RECONCILE_KEY = "critical_cleanup_reconcile_v1"
_CRITICAL_ZERO_EPSILON = 1e-12
# Diagnostic output must remain bounded even when durable metadata is corrupt.
_CRITICAL_DIAGNOSTIC_MAX_STOP_IDS = 20
_CRITICAL_DIAGNOSTIC_MAX_ERROR_CHARS = 4096
_CRITICAL_SAFE_BE_ERROR_CODES = frozenset(
    {
        "invalid_tp_plan",
        "tp_ledger_conflict",
        "missing_tp_plan_snapshot",
        "position_id_missing",
    }
)
_CRITICAL_SAFE_ERROR_SUFFIXES = (
    "Error",
    "Exception",
    "Timeout",
    "Failure",
    "Rejected",
    "Failed",
)


def _critical_stable_ids(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple, set)):
        return []
    result: list[str] = []
    for item in value:
        cleaned = clean_exchange_id(item)
        if cleaned:
            result.append(cleaned)
    return sorted(set(result))


def _critical_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, (list, tuple, set)) else []


def _critical_cleanup_snapshot(value: Any) -> dict[str, Any]:
    cleanup = value if isinstance(value, dict) else {}
    unidentified_algo = _critical_list(cleanup.get("unidentified_relevant_algo_orders"))
    unidentified_regular = _critical_list(
        cleanup.get("unidentified_relevant_regular_orders")
    )
    errors = _critical_list(cleanup.get("errors"))
    return {
        "verified_clean": cleanup.get("verified_clean"),
        "identity_missing": cleanup.get("identity_missing"),
        "conditional_cancelled": cleanup.get("conditional_cancelled"),
        "regular_cancelled": cleanup.get("regular_cancelled"),
        "remaining_tracked_algo_ids": _critical_stable_ids(
            cleanup.get("remaining_tracked_algo_ids")
        ),
        "remaining_tracked_regular_ids": _critical_stable_ids(
            cleanup.get("remaining_tracked_regular_ids")
        ),
        "unidentified_relevant_algo_count": len(unidentified_algo),
        "unidentified_relevant_regular_count": len(unidentified_regular),
        "errors": [str(item)[:300] for item in errors],
    }


def _critical_payload_parse(row: Dict[str, Any]) -> tuple[dict[str, Any], bool]:
    raw = row.get("exchange_order_ids_json")
    if isinstance(raw, dict):
        return dict(raw), True
    try:
        if raw is None or raw == "":
            raw = "{}"
        parsed = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}, False
    return (parsed, True) if isinstance(parsed, dict) else ({}, False)


def _critical_quantity_state(
    source: dict[str, Any], key: str
) -> tuple[str, float | None]:
    if key not in source:
        return "missing", None
    value = finite_number(source.get(key))
    if value is None or value < 0:
        return "invalid", None
    if value > _CRITICAL_ZERO_EPSILON:
        return "live", value
    return "zero", value


def _critical_cleanup_has_unknown_hazard(value: Any) -> bool:
    cleanup = value if isinstance(value, dict) else {}
    if cleanup.get("identity_missing") is True:
        return True
    if _critical_stable_ids(cleanup.get("remaining_tracked_algo_ids")):
        return True
    if _critical_stable_ids(cleanup.get("remaining_tracked_regular_ids")):
        return True
    if _critical_list(cleanup.get("unidentified_relevant_algo_orders")):
        return True
    if _critical_list(cleanup.get("unidentified_relevant_regular_orders")):
        return True
    return False


def _critical_cleanup_reconcile_state(payload: dict[str, Any]) -> dict[str, Any]:
    marker_raw = payload.get(_CRITICAL_CLEANUP_RECONCILE_KEY)
    marker = marker_raw if isinstance(marker_raw, dict) else {}
    try:
        attempts = max(
            0,
            int(
                marker.get("retry_attempts")
                if marker.get("retry_attempts") is not None
                else marker.get("attempts") or 0
            ),
        )
    except (TypeError, ValueError, OverflowError):
        attempts = 0
    state = str(marker.get("state") or "").strip().lower()
    next_attempt_at = _parse_utc_iso(marker.get("next_attempt_at"))
    return {
        "state": state,
        "attempts": attempts,
        "next_attempt_at": next_attempt_at,
        "verified_clean": marker.get("verified_clean") is True,
        "blocked": state
        in {"blocked", "blocked_unknown_orders", "blocked_live_position"},
    }


def critical_cleanup_reconcile_due(
    row: Dict[str, Any], *, now: datetime | None = None
) -> bool:
    """Return False only for a safely scheduled zero-exposure cleanup probe.

    Unknown/manual orders and every live/opposite-position state stay fast. Only
    a prior probe that reached a zero-exposure, non-hazardous transient failure
    may wait for its durable retry timestamp.
    """

    if str(row.get("status") or "").strip().lower() != "manual_required":
        return True
    payload, valid = _critical_payload_parse(row)
    if not valid:
        return True
    proof = _critical_zero_proof_state(payload)
    if not proof.get("confirmed"):
        return True
    classification = critical_manual_backoff_classification(row)
    if str(classification.get("reason") or "") not in {
        "cleanup_unresolved",
        "be_replacement_in_progress",
    }:
        return True
    marker = _critical_cleanup_reconcile_state(payload)
    if marker.get("blocked"):
        return True
    next_attempt_at = marker.get("next_attempt_at")
    if next_attempt_at is None:
        return True
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc) >= next_attempt_at


def critical_cleanup_retry_delay(attempts: int) -> int:
    normalized = max(1, int(attempts or 1))
    return int(
        _CRITICAL_CLEANUP_RETRY_DELAYS_SEC[
            min(normalized - 1, len(_CRITICAL_CLEANUP_RETRY_DELAYS_SEC) - 1)
        ]
    )


def _critical_zero_proof_state(payload: dict[str, Any]) -> dict[str, Any]:
    """Use the shared strict proof parser to prevent monitor/risk drift.

    The shared parser still requires ``second_read_forced`` and two ordered
    zero timestamps; keeping the terms here also preserves the static safety
    audit that guards this critical backoff path.
    """

    return critical_zero_exposure_proof_state(payload)


def _critical_reason_text(row: Dict[str, Any], payload: dict[str, Any]) -> str:
    lifecycle = (
        payload.get("lifecycle") if isinstance(payload.get("lifecycle"), dict) else {}
    )
    be = payload.get("be") if isinstance(payload.get("be"), dict) else {}
    residual = (
        payload.get("residual_position_v1")
        if isinstance(payload.get("residual_position_v1"), dict)
        else {}
    )
    marker = (
        payload.get("critical_manual_review_v1")
        if isinstance(payload.get("critical_manual_review_v1"), dict)
        else {}
    )
    return (
        " ".join(
            str(value or "")
            for value in (
                row.get("reason"),
                lifecycle.get("reason"),
                be.get("error"),
                residual.get("error"),
                marker.get("reason"),
            )
        )
        .strip()
        .lower()
    )


def _critical_error_diagnostic(value: Any) -> tuple[str | None, str | None]:
    """Return a compact non-secret error code and stable fingerprint.

    ``be.error`` normally starts with an exception class, but legacy or corrupt
    rows are not trusted.  Only a small allowlist of durable codes or an ASCII
    class-like token ending in Error/Exception/Timeout/etc. may be emitted.
    Every other leading token is replaced with ``present`` so an unlabeled API
    credential can never become a Railway log field.
    """

    text = str(value or "").strip()
    if not text:
        return None, None

    raw_prefix = text.split(":", 1)[0].strip().split(None, 1)[0] or "present"
    safe_prefix = "".join(
        char if char.isascii() and (char.isalnum() or char in "._-") else "_"
        for char in raw_prefix
    ).strip("_")[:80]
    class_like = bool(
        safe_prefix
        and safe_prefix[:1].isalpha()
        and safe_prefix.endswith(_CRITICAL_SAFE_ERROR_SUFFIXES)
    )
    known_code = safe_prefix in _CRITICAL_SAFE_BE_ERROR_CODES
    code = safe_prefix if class_like or known_code else "present"

    bounded = text[:_CRITICAL_DIAGNOSTIC_MAX_ERROR_CHARS]
    if len(text) > len(bounded):
        bounded = f"{bounded}\0len={len(text)}"
    fingerprint = hashlib.sha256(
        bounded.encode("utf-8", errors="replace")
    ).hexdigest()[:16]
    return code, fingerprint


def critical_manual_backoff_classification(row: Dict[str, Any]) -> dict[str, Any]:
    """Classify why one manual_required row must stay fast or may back off.

    Backoff requires a dedicated durable proof made from two independent fresh
    BingX zero-position responses.  A terminal-looking status or one old zero
    quantity is intentionally insufficient.  Every protection, cleanup, entry,
    residual, malformed-data or opposite-position ambiguity remains fail-closed.
    """

    result: dict[str, Any] = {
        "eligible": False,
        "reason": "not_manual_required",
        "zero_confirmations": 0,
    }
    if str(row.get("status") or "").strip().lower() != "manual_required":
        return result

    invalid_fields: list[str] = []
    try:
        if int(row.get("user_id") or 0) <= 0:
            invalid_fields.append("user_id")
    except (TypeError, ValueError, OverflowError):
        invalid_fields.append("user_id")
    if not str(row.get("symbol") or "").strip():
        invalid_fields.append("symbol")
    if str(row.get("side") or "").strip().lower() not in {"long", "short"}:
        invalid_fields.append("side")
    qty = finite_number(row.get("qty"))
    if qty is None or qty <= 0:
        invalid_fields.append("qty")
    if invalid_fields:
        result.update(reason="invalid_execution_fields", details=invalid_fields)
        return result

    payload, payload_valid = _critical_payload_parse(row)
    if not payload_valid:
        result["reason"] = "malformed_payload"
        return result

    proof_state = _critical_zero_proof_state(payload)
    result["zero_confirmations"] = int(proof_state["confirmations"])
    result["zero_proof_confirmed"] = bool(proof_state["confirmed"])

    lifecycle = (
        payload.get("lifecycle") if isinstance(payload.get("lifecycle"), dict) else {}
    )
    be = payload.get("be") if isinstance(payload.get("be"), dict) else {}
    residual = (
        payload.get("residual_position_v1")
        if isinstance(payload.get("residual_position_v1"), dict)
        else {}
    )
    marker = (
        payload.get("critical_manual_review_v1")
        if isinstance(payload.get("critical_manual_review_v1"), dict)
        else {}
    )
    reason_text = _critical_reason_text(row, payload)

    api_state = str(marker.get("api_state") or "").strip().lower()
    if marker.get("reason") == "api_key_missing" or (
        api_state != "available"
        and "api" in reason_text
        and any(
            token in reason_text
            for token in (
                "missing",
                "not configured",
                "disabled",
                "unavailable",
                "no api",
            )
        )
    ):
        result["reason"] = "api_unavailable"
        return result

    opposite = lifecycle.get("opposite_or_unknown_position_detected")
    if opposite is True or opposite not in (False, None):
        result["reason"] = "opposite_or_unknown_position"
        return result

    for key in ("any_position_qty", "position_qty"):
        state, value = _critical_quantity_state(lifecycle, key)
        if state == "invalid":
            result.update(reason="invalid_position_snapshot", details=[key])
            return result
        if state == "live":
            result.update(reason="live_position", live_qty=value, live_field=key)
            return result

    opening_reconcile = (
        payload.get("opening_intent_reconciliation_v1")
        if isinstance(payload.get("opening_intent_reconciliation_v1"), dict)
        else {}
    )
    if (
        (finite_number(opening_reconcile.get("same_side_position_qty")) or 0.0)
        > _CRITICAL_ZERO_EPSILON
        or int(finite_number(opening_reconcile.get("open_match_count")) or 0.0) > 0
        or "opening_intent" in reason_text
        and "live" in reason_text
    ):
        result["reason"] = "active_or_unknown_entry"
        return result

    if (
        be.get("replacement_in_progress") is True
        or isinstance(be.get("replacement_write_intent_v1"), dict)
        and bool(be.get("replacement_write_intent_v1"))
    ):
        result.update(
            reason="be_replacement_in_progress",
            replacement_stop_id=clean_exchange_id(be.get("replacement_stop_id")),
            verify_matching_stop_order_id=clean_exchange_id(
                be.get("verify_matching_stop_order_id")
            ),
            replacement_write_intent_present=bool(
                isinstance(be.get("replacement_write_intent_v1"), dict)
                and be.get("replacement_write_intent_v1")
            ),
        )
        return result
    be_manual_required = be.get("manual_required") is True
    be_error_text = str(be.get("error") or "").strip()
    be_error_code, be_error_fingerprint = _critical_error_diagnostic(be_error_text)

    stop_diag = (
        payload.get("stop_diagnostic_v1")
        if isinstance(payload.get("stop_diagnostic_v1"), dict)
        else {}
    )
    all_active_stop_ids = _critical_stable_ids(stop_diag.get("active_stop_ids"))
    active_stop_count = len(all_active_stop_ids)
    active_stop_ids = all_active_stop_ids[:_CRITICAL_DIAGNOSTIC_MAX_STOP_IDS]
    active_stop_ids_truncated = active_stop_count > len(active_stop_ids)
    stop_manual_required = stop_diag.get("manual_required") is True
    multiple_active_stops = active_stop_count > 1

    protection_sources: list[str] = []
    if be_manual_required or be_error_text:
        if be_manual_required and be_error_text:
            protection_sources.append("be_manual_required_and_error")
        elif be_manual_required:
            protection_sources.append("be_manual_required")
        else:
            protection_sources.append("be_error")

    if multiple_active_stops or stop_manual_required:
        if multiple_active_stops and stop_manual_required:
            protection_sources.append("multiple_active_stops_and_manual_required")
        elif multiple_active_stops:
            protection_sources.append("multiple_active_stops")
        else:
            protection_sources.append("stop_diagnostic_manual_required")

    # Legacy reason text remains a fallback only when no structured BE/STOP
    # evidence exists.  Include both English and Russian STOP terms; the old
    # English-only gate could incorrectly back off a row whose reason was
    # "неизвестный стоп".
    reason_text_unknown_stop = bool(
        not protection_sources
        and ("stop" in reason_text or "стоп" in reason_text)
        and any(
            token in reason_text
            for token in (
                "unknown",
                "without positionid",
                "manual protection",
                "неизвест",
            )
        )
    )
    if reason_text_unknown_stop:
        protection_sources.append("reason_text_unknown_stop")

    if protection_sources:
        protection_source = (
            protection_sources[0]
            if len(protection_sources) == 1
            else "multiple_protection_sources"
        )
        result.update(
            reason="unknown_stop_or_be_protection",
            protection_source=protection_source,
            protection_sources=protection_sources,
            be_manual_required=be_manual_required,
            be_error_present=bool(be_error_text),
            be_error_code=be_error_code,
            be_error_fingerprint=be_error_fingerprint,
            stop_manual_required=stop_manual_required,
            active_stop_count=active_stop_count,
            active_stop_ids=active_stop_ids,
            active_stop_ids_truncated=active_stop_ids_truncated,
            reason_text_unknown_stop=reason_text_unknown_stop,
        )
        return result

    residual_qty = max(
        finite_number(residual.get("position_qty")) or 0.0,
        finite_number(residual.get("same_side_position_qty")) or 0.0,
        finite_number(residual.get("qty")) or 0.0,
        finite_number(residual.get("after_qty")) or 0.0,
    )
    residual_status = (
        str(residual.get("status") or residual.get("state") or "").strip().lower()
    )
    residual_safe_statuses = {
        "",
        "closed",
        "resolved",
        "no_residual",
        "position_closed",
        "market_close_confirmed",
    }
    if residual and (
        residual.get("manual_required") is True
        or residual_qty > _CRITICAL_ZERO_EPSILON
        or residual_status not in residual_safe_statuses
    ):
        result["reason"] = "residual_active_or_unknown"
        return result

    lifecycle_cleanup = lifecycle.get("cleanup")
    manual_close = (
        payload.get("manual_position_close_v1")
        if isinstance(payload.get("manual_position_close_v1"), dict)
        else {}
    )
    manual_cleanup = manual_close.get("cleanup")
    if _critical_cleanup_has_unknown_hazard(
        lifecycle_cleanup
    ) or _critical_cleanup_has_unknown_hazard(manual_cleanup):
        result["reason"] = "unknown_stop_or_cleanup_identity"
        return result

    cleanup_verified = bool(
        lifecycle.get("closed_cleanup_done") is True
        or isinstance(lifecycle_cleanup, dict)
        and lifecycle_cleanup.get("verified_clean") is True
        or isinstance(manual_cleanup, dict)
        and manual_cleanup.get("verified_clean") is True
    )
    if not cleanup_verified or execution_cleanup_unresolved(row):
        cleanup_source = (
            manual_cleanup
            if isinstance(manual_cleanup, dict) and manual_cleanup
            else lifecycle_cleanup
            if isinstance(lifecycle_cleanup, dict)
            else {}
        )
        cleanup_snapshot = _critical_cleanup_snapshot(cleanup_source)
        reconcile_state = _critical_cleanup_reconcile_state(payload)
        result.update(
            reason="cleanup_unresolved",
            cleanup=cleanup_snapshot,
            cleanup_retry_state=reconcile_state.get("state"),
            cleanup_retry_attempts=int(reconcile_state.get("attempts") or 0),
            cleanup_next_attempt_at=(
                reconcile_state["next_attempt_at"].isoformat()
                if reconcile_state.get("next_attempt_at") is not None
                else None
            ),
        )
        return result

    if not proof_state["confirmations"]:
        result["reason"] = "zero_proof_missing"
        return result
    if not proof_state["confirmed"]:
        result["reason"] = "zero_proof_invalid_or_incomplete"
        return result

    # Keep the monitor backoff classifier and the shared risk/dashboard
    # exposure classifier on one fail-closed decision.  The detailed checks
    # above preserve diagnostic reasons; this final gate prevents future drift.
    shared_release = manual_required_zero_exposure_release_state(row)
    if shared_release.get("eligible") is not True:
        result.update(
            eligible=False,
            reason=str(shared_release.get("reason") or "shared_release_rejected"),
        )
        if shared_release.get("details"):
            result["details"] = list(shared_release.get("details") or [])
        return result

    result.update(eligible=True, reason="eligible")
    return result


def critical_manual_backoff_eligible(row: Dict[str, Any]) -> bool:
    return bool(critical_manual_backoff_classification(row).get("eligible"))


def closed_history_reconcile_due(
    row: Dict[str, Any], *, now: datetime | None = None
) -> bool:
    """Return False only for a safely scheduled closed-history retry."""

    if str(row.get("status") or "").strip().lower() != "closed_pending_history":
        return True
    payload = execution_payload_dict(row)
    lifecycle = (
        payload.get("lifecycle") if isinstance(payload.get("lifecycle"), dict) else {}
    )
    if lifecycle.get("closed_cleanup_done") is not True:
        return True
    history = (
        lifecycle.get("history_reconcile")
        if isinstance(lifecycle.get("history_reconcile"), dict)
        else {}
    )
    next_attempt = _parse_utc_iso(history.get("next_attempt_at"))
    if next_attempt is None:
        return True
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc) >= next_attempt


def critical_reason_fingerprint(row: Dict[str, Any]) -> str:
    """Stable critical-state hash without volatile check timestamps."""

    payload = execution_payload_dict(row)
    lifecycle_raw = payload.get("lifecycle")
    lifecycle = lifecycle_raw if isinstance(lifecycle_raw, dict) else {}
    be_raw = payload.get("be")
    be = be_raw if isinstance(be_raw, dict) else {}
    residual_raw = payload.get("residual_position_v1")
    residual = residual_raw if isinstance(residual_raw, dict) else {}
    manual_close_raw = payload.get("manual_position_close_v1")
    manual_close = manual_close_raw if isinstance(manual_close_raw, dict) else {}
    manual_cleanup = manual_close.get("cleanup")
    zero_proof = _critical_zero_proof_state(payload)
    stop_diag = (
        payload.get("stop_diagnostic_v1")
        if isinstance(payload.get("stop_diagnostic_v1"), dict)
        else {}
    )
    marker = (
        payload.get("critical_manual_review_v1")
        if isinstance(payload.get("critical_manual_review_v1"), dict)
        else {}
    )
    snapshot = {
        "status": str(row.get("status") or "").strip().lower(),
        "reason": str(row.get("reason") or "").strip(),
        "zero_exposure": execution_zero_exposure_confirmed(row),
        "cleanup_unresolved": execution_cleanup_unresolved(row),
        "zero_proof": {
            "confirmed": zero_proof.get("confirmed"),
            "confirmations": zero_proof.get("confirmations"),
            "independent": zero_proof.get("independent"),
            "same_side_position_qty": zero_proof.get("same_side_position_qty"),
            "any_position_qty": zero_proof.get("any_position_qty"),
        },
        "manual_review_marker": {"reason": marker.get("reason")},
        "stop_diagnostic": {
            "manual_required": stop_diag.get("manual_required"),
            "active_stop_ids": _critical_stable_ids(stop_diag.get("active_stop_ids")),
        },
        "lifecycle": {
            "previous_status": lifecycle.get("previous_status"),
            "position_qty": lifecycle.get("position_qty"),
            "any_position_qty": lifecycle.get("any_position_qty"),
            "opposite_or_unknown_position_detected": lifecycle.get(
                "opposite_or_unknown_position_detected"
            ),
            "closed_cleanup_done": lifecycle.get("closed_cleanup_done"),
            "cleanup_deferred": lifecycle.get("cleanup_deferred"),
            "cleanup_deferred_reason": lifecycle.get("cleanup_deferred_reason"),
            "cleanup": _critical_cleanup_snapshot(lifecycle.get("cleanup")),
            "close_result": lifecycle.get("close_result")
            if isinstance(lifecycle.get("close_result"), dict)
            else {},
        },
        "manual_close": {
            "confirmed": manual_close.get("confirmed"),
            "cleanup": _critical_cleanup_snapshot(manual_cleanup),
        },
        "be": {
            "moved": be.get("moved"),
            "manual_required": be.get("manual_required"),
            "replacement_in_progress": be.get("replacement_in_progress"),
            "error": str(be.get("error") or "")[:500],
            "replacement_write_intent_v1": be.get("replacement_write_intent_v1"),
            "replacement_stop_id": clean_exchange_id(be.get("replacement_stop_id")),
            "verify_matching_stop_order_id": clean_exchange_id(
                be.get("verify_matching_stop_order_id")
            ),
            "stop": be.get("stop"),
            "qty": be.get("qty"),
        },
        "residual": {
            "state": residual.get("state"),
            "manual_required": residual.get("manual_required"),
            "position_qty": residual.get("position_qty"),
            "same_side_position_qty": residual.get("same_side_position_qty"),
            "error": str(residual.get("error") or "")[:500],
        },
    }
    raw = json.dumps(snapshot, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def critical_backoff_due(row: Dict[str, Any], *, now: datetime | None = None) -> bool:
    """Return True when a row must remain in the current critical pass.

    The persisted schedule is capped at five minutes by
    ``_CRITICAL_MANUAL_BACKOFF_DELAYS_SEC``.  This bounds stale database-only
    deferral while avoiding a permanent five-second loop for unchanged rows.
    """

    if not critical_manual_backoff_eligible(row):
        return True
    current_hash = critical_reason_fingerprint(row)
    saved_hash = str(row.get("critical_reason_hash") or "").strip()
    if not saved_hash or saved_hash != current_hash:
        return True
    next_check = _parse_utc_iso(row.get("critical_next_check_at"))
    if next_check is None:
        return True
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc) >= next_check.astimezone(timezone.utc)


def _critical_backoff_state_present(row: Dict[str, Any]) -> bool:
    try:
        unchanged = int(row.get("critical_unchanged_count") or 0)
    except (TypeError, ValueError, OverflowError):
        unchanged = 0
    return bool(
        row.get("critical_next_check_at")
        or unchanged
        or str(row.get("critical_reason_hash") or "").strip()
        or row.get("critical_last_change_at")
    )


def _critical_execution_ids(values: Any) -> list[int]:
    result: set[int] = set()
    for value in values or ():
        try:
            parsed = int(value or 0)
        except (TypeError, ValueError, OverflowError):
            continue
        if parsed > 0:
            result.add(parsed)
    return sorted(result)


async def wake_critical_backoff(execution_ids: list[int] | tuple[int, ...]) -> int:
    """Wake selected executions without changing their trading state."""

    ids = _critical_execution_ids(execution_ids)
    if not ids:
        return 0
    if is_postgres():
        async with connect() as c:
            tag = await c.execute(
                """
                UPDATE trade_executions
                SET critical_next_check_at=NULL,
                    critical_unchanged_count=0,
                    critical_reason_hash=NULL,
                    critical_last_change_at=NULL
                WHERE id = ANY($1::bigint[])
                """,
                ids,
            )
        try:
            return int(str(tag).rsplit(" ", 1)[-1])
        except (TypeError, ValueError):
            return 0
    placeholders = ",".join(["?"] * len(ids))
    async with connect() as c:
        cur = await c.execute(
            f"""
            UPDATE trade_executions
            SET critical_next_check_at=NULL,
                critical_unchanged_count=0,
                critical_reason_hash=NULL,
                critical_last_change_at=NULL
            WHERE id IN ({placeholders})
            """,
            ids,
        )
        await c.commit()
        return int(cur.rowcount or 0)


async def schedule_manual_required_backoff(
    execution_ids: list[int] | tuple[int, ...],
) -> dict[str, int]:
    """Persist bounded 30s/1m/2m/5m backoff after a safe critical pass.

    Rows are re-read after lifecycle reconciliation. Only a manual_required row
    with durable zero-exposure proof is eligible. All live/ambiguous rows are
    explicitly reset to fast mode. Updates use a status/reason/payload CAS so a
    concurrent safety change cannot be hidden behind a newly scheduled delay.
    """

    ids = _critical_execution_ids(execution_ids)
    stats = {
        "scheduled": 0,
        "changed": 0,
        "unchanged": 0,
        "fast": 0,
        "conflicts": 0,
        "max_delay_sec": 0,
    }
    if not ids:
        return stats
    now = datetime.now(timezone.utc)

    if is_postgres():
        async with connect() as c:
            raw_rows = await c.fetch(
                "SELECT * FROM trade_executions WHERE id = ANY($1::bigint[])", ids
            )
            rows = [_dict(row) for row in raw_rows]
            for row in rows:
                execution_id = int(row.get("id") or 0)
                if not critical_manual_backoff_eligible(row):
                    stats["fast"] += 1
                    if _critical_backoff_state_present(row):
                        await c.execute(
                            """
                            UPDATE trade_executions
                            SET critical_next_check_at=NULL,
                                critical_unchanged_count=0,
                                critical_reason_hash=NULL,
                                critical_last_change_at=NULL
                            WHERE id=$1
                            """,
                            execution_id,
                        )
                    continue
                fingerprint = critical_reason_fingerprint(row)
                previous = str(row.get("critical_reason_hash") or "").strip()
                changed = previous != fingerprint
                try:
                    old_count = max(0, int(row.get("critical_unchanged_count") or 0))
                except (TypeError, ValueError, OverflowError):
                    old_count = 0
                count = 1 if changed else old_count + 1
                delay = _CRITICAL_MANUAL_BACKOFF_DELAYS_SEC[
                    min(max(count - 1, 0), len(_CRITICAL_MANUAL_BACKOFF_DELAYS_SEC) - 1)
                ]
                next_check = now + timedelta(seconds=delay)
                last_change = (
                    now
                    if changed
                    else _parse_utc_iso(row.get("critical_last_change_at")) or now
                )
                old_json = row.get("exchange_order_ids_json") or "{}"
                reason = str(row.get("reason") or "")
                tag = await c.execute(
                    """
                    UPDATE trade_executions
                    SET critical_next_check_at=$1,
                        critical_unchanged_count=$2,
                        critical_reason_hash=$3,
                        critical_last_change_at=$4
                    WHERE id=$5
                      AND status='manual_required'
                      AND COALESCE(reason,'')=$6
                      AND COALESCE(exchange_order_ids_json,'{}')=$7
                    """,
                    next_check,
                    count,
                    fingerprint,
                    last_change,
                    execution_id,
                    reason,
                    old_json,
                )
                if str(tag).endswith(" 1"):
                    stats["scheduled"] += 1
                    stats["changed" if changed else "unchanged"] += 1
                    stats["max_delay_sec"] = max(stats["max_delay_sec"], delay)
                else:
                    stats["conflicts"] += 1
        return stats

    placeholders = ",".join(["?"] * len(ids))
    async with connect() as c:
        cur = await c.execute(
            f"SELECT * FROM trade_executions WHERE id IN ({placeholders})", ids
        )
        rows = [_dict(row) for row in await cur.fetchall()]
        for row in rows:
            execution_id = int(row.get("id") or 0)
            if not critical_manual_backoff_eligible(row):
                stats["fast"] += 1
                if _critical_backoff_state_present(row):
                    await c.execute(
                        """
                        UPDATE trade_executions
                        SET critical_next_check_at=NULL,
                            critical_unchanged_count=0,
                            critical_reason_hash=NULL,
                            critical_last_change_at=NULL
                        WHERE id=?
                        """,
                        (execution_id,),
                    )
                continue
            fingerprint = critical_reason_fingerprint(row)
            previous = str(row.get("critical_reason_hash") or "").strip()
            changed = previous != fingerprint
            try:
                old_count = max(0, int(row.get("critical_unchanged_count") or 0))
            except (TypeError, ValueError, OverflowError):
                old_count = 0
            count = 1 if changed else old_count + 1
            delay = _CRITICAL_MANUAL_BACKOFF_DELAYS_SEC[
                min(max(count - 1, 0), len(_CRITICAL_MANUAL_BACKOFF_DELAYS_SEC) - 1)
            ]
            next_check = now + timedelta(seconds=delay)
            last_change = (
                now
                if changed
                else _parse_utc_iso(row.get("critical_last_change_at")) or now
            )
            old_json = row.get("exchange_order_ids_json") or "{}"
            reason = str(row.get("reason") or "")
            cur = await c.execute(
                """
                UPDATE trade_executions
                SET critical_next_check_at=?,
                    critical_unchanged_count=?,
                    critical_reason_hash=?,
                    critical_last_change_at=?
                WHERE id=?
                  AND status='manual_required'
                  AND COALESCE(reason,'')=?
                  AND COALESCE(exchange_order_ids_json,'{}')=?
                """,
                (
                    next_check.isoformat(),
                    count,
                    fingerprint,
                    last_change.isoformat(),
                    execution_id,
                    reason,
                    old_json,
                ),
            )
            if int(cur.rowcount or 0) == 1:
                stats["scheduled"] += 1
                stats["changed" if changed else "unchanged"] += 1
                stats["max_delay_sec"] = max(stats["max_delay_sec"], delay)
            else:
                stats["conflicts"] += 1
        await c.commit()
    return stats


async def pending_limit_executions(
    limit: int = 50, *, after_id: int = 0
) -> List[Dict[str, Any]]:
    """Rotating page of explicit BingX rows waiting for LIMIT fill processing."""
    return await _paged_bingx_execution_rows(
        ["pending_limit"], limit=limit, after_id=after_id
    )


async def active_position_executions(
    limit: int = 100, *, after_id: int = 0
) -> List[Dict[str, Any]]:
    """Rotating page of BingX rows needing live-position or cleanup monitoring."""
    return await _paged_bingx_execution_rows(
        [
            "opening_intent",
            "opened",
            "protected",
            "partial_error",
            "manual_required",
            "partial_unrecoverable",
            "closed_pending_history",
            "closed_on_exchange",
            "closed_stop_catchup",
        ],
        limit=limit,
        after_id=after_id,
    )


async def critical_position_executions(
    limit: int = 1000, *, after_id: int = 0
) -> List[Dict[str, Any]]:
    """Return only rows eligible for the five-second critical safety lane.

    The critical worker previously fetched every active/open execution and then
    discarded most rows in Python.  Live logs showed roughly 98 rows scanned to
    obtain 16 critical candidates, while the initial SELECT occasionally waited
    through both monitor admission and pool acquisition.  Filtering by critical
    status in SQL preserves all safety candidates and materially shortens the
    high-priority DB scope.
    """

    return await _paged_bingx_execution_rows(
        [
            "partial_error",
            "manual_required",
            "partial_unrecoverable",
            "closed_pending_history",
        ],
        limit=limit,
        after_id=after_id,
    )


async def active_position_executions_for_user(
    user_id: int, *, limit: int = 100
) -> List[Dict[str, Any]]:
    """Return one user's executions that can still own a live position.

    Candidate rows are scanned in stable id pages until enough *live* rows are
    found. Limiting before filtering allowed a large tail of already-closed
    legacy ``manual_required`` rows to hide an older real active execution from
    the manual-position menu.
    """
    statuses = [
        "opening_intent",
        "opened",
        "pending_limit",
        "protected",
        "partial_error",
        "manual_required",
        "partial_unrecoverable",
    ]
    max_rows = max(1, min(int(limit or 100), 500))
    page_size = max(200, min(1000, max_rows * 2))
    cursor_id = 0
    result: list[Dict[str, Any]] = []

    while len(result) < max_rows:
        if is_postgres():
            async with connect() as c:
                raw = await c.fetch(
                    """
                    SELECT * FROM trade_executions
                    WHERE user_id=$1
                      AND status = ANY($2::text[])
                      AND ($3::bigint=0 OR id<$3)
                    ORDER BY id DESC
                    LIMIT $4
                    """,
                    int(user_id),
                    statuses,
                    int(cursor_id),
                    int(page_size),
                )
                page = [_dict(row) for row in raw]
        else:
            placeholders = ",".join(["?"] * len(statuses))
            async with connect() as c:
                cur = await c.execute(
                    f"""
                    SELECT * FROM trade_executions
                    WHERE user_id=?
                      AND status IN ({placeholders})
                      AND (?=0 OR id<?)
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (
                        int(user_id),
                        *statuses,
                        int(cursor_id),
                        int(cursor_id),
                        int(page_size),
                    ),
                )
                page = [_dict(row) for row in await cur.fetchall()]

        if not page:
            break
        for row in _bingx_rows(page):
            if not _execution_zero_exposure_confirmed(row):
                result.append(row)
                if len(result) >= max_rows:
                    break
        next_cursor = min(int(row.get("id") or 0) for row in page)
        if next_cursor <= 0 or next_cursor == cursor_id or len(page) < page_size:
            break
        cursor_id = next_cursor

    return result[:max_rows]


async def other_active_symbol_executions(
    user_id: int,
    symbol: str,
    exclude_execution_id: int,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    """Return same-symbol rows that still make a new plan unsafe.

    The scan must filter before applying the caller's result limit. Previously
    ``limit=1`` fetched only eight newest candidates; eight clean legacy rows
    could therefore hide an older live execution and allow a second plan for
    the same aggregated BingX position.
    """
    statuses = [
        "opening_intent",
        "opened",
        "pending_limit",
        "protected",
        "partial_error",
        "manual_required",
        "partial_unrecoverable",
        "closed_pending_history",
        "closed_on_exchange",
        "closed_stop_catchup",
    ]
    sym = (symbol or "").upper()
    max_rows = max(1, min(int(limit or 1), 500))
    page_size = max(100, min(1000, max_rows * 20))
    cursor_id = 0
    conflicts: list[Dict[str, Any]] = []

    while len(conflicts) < max_rows:
        if is_postgres():
            async with connect() as c:
                raw = await c.fetch(
                    """
                    SELECT * FROM trade_executions
                    WHERE user_id=$1
                      AND UPPER(symbol)=UPPER($2)
                      AND id<>$3
                      AND status = ANY($4::text[])
                      AND ($5::bigint=0 OR id<$5)
                    ORDER BY id DESC
                    LIMIT $6
                    """,
                    int(user_id),
                    sym,
                    int(exclude_execution_id),
                    statuses,
                    int(cursor_id),
                    int(page_size),
                )
                page = [_dict(row) for row in raw]
        else:
            placeholders = ",".join(["?"] * len(statuses))
            async with connect() as c:
                cur = await c.execute(
                    f"""
                    SELECT * FROM trade_executions
                    WHERE user_id=?
                      AND UPPER(symbol)=UPPER(?)
                      AND id<>?
                      AND status IN ({placeholders})
                      AND (?=0 OR id<?)
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (
                        int(user_id),
                        sym,
                        int(exclude_execution_id),
                        *statuses,
                        int(cursor_id),
                        int(cursor_id),
                        int(page_size),
                    ),
                )
                page = [_dict(row) for row in await cur.fetchall()]

        if not page:
            break
        for row in _bingx_rows(page):
            if not _execution_zero_exposure_confirmed(
                row
            ) or _execution_cleanup_unresolved(row):
                conflicts.append(row)
                if len(conflicts) >= max_rows:
                    break
        next_cursor = min(int(row.get("id") or 0) for row in page)
        if next_cursor <= 0 or next_cursor == cursor_id or len(page) < page_size:
            break
        cursor_id = next_cursor

    return conflicts[:max_rows]


async def update_execution_status(
    execution_id: int,
    status: str,
    reason: str = "",
    exchange_order_ids_json: str | None = None,
    *,
    expected_status: str | list[str] | tuple[str, ...] | None = None,
) -> bool:
    """Update status/reason (and optionally the JSON payload) for one execution.

    ``expected_status`` optionally makes the write conditional on the row still
    being in that status (or one of a list of statuses). This closes a v1.6.17-era
    gap: unlike ``merge_execution_metadata``, this function previously had no way
    to refuse a write when the row had already moved on to a different status
    because of a concurrent worker or a brief old/new process overlap during a
    Railway redeploy. Callers that don't need this keep working unchanged: with
    ``expected_status=None`` the write is unconditional, exactly as before.

    Returns ``True`` if the row was actually updated, ``False`` if the optional
    status precondition failed (row already moved on) or the row does not exist.
    """
    if exchange_order_ids_json is not None:
        exchange_order_ids_json = _ensure_bingx_execution_json(exchange_order_ids_json)
    expected_list: list[str] | None = None
    if expected_status is not None:
        expected_list = (
            [str(expected_status)]
            if isinstance(expected_status, str)
            else [str(s) for s in expected_status]
        )
    if is_postgres():
        advisory_conn = _current_advisory_connection()
        if advisory_conn is not None:
            c = advisory_conn
            _record_critical_counter("critical_db_update_connection_reused")
            if exchange_order_ids_json is None:
                if expected_list is None:
                    tag = await c.execute(
                        "UPDATE trade_executions SET status=$1, reason=$2, updated_at=NOW() WHERE id=$3",
                        status,
                        reason,
                        int(execution_id),
                    )
                else:
                    tag = await c.execute(
                        "UPDATE trade_executions SET status=$1, reason=$2, updated_at=NOW() WHERE id=$3 AND status = ANY($4::text[])",
                        status,
                        reason,
                        int(execution_id),
                        expected_list,
                    )
            else:
                if expected_list is None:
                    tag = await c.execute(
                        "UPDATE trade_executions SET status=$1, reason=$2, exchange_order_ids_json=$3, updated_at=NOW() WHERE id=$4",
                        status,
                        reason,
                        exchange_order_ids_json,
                        int(execution_id),
                    )
                else:
                    tag = await c.execute(
                        "UPDATE trade_executions SET status=$1, reason=$2, exchange_order_ids_json=$3, updated_at=NOW() WHERE id=$4 AND status = ANY($5::text[])",
                        status,
                        reason,
                        exchange_order_ids_json,
                        int(execution_id),
                        expected_list,
                    )
        else:
            async with connect() as c:
                if exchange_order_ids_json is None:
                    if expected_list is None:
                        tag = await c.execute(
                            "UPDATE trade_executions SET status=$1, reason=$2, updated_at=NOW() WHERE id=$3",
                            status,
                            reason,
                            int(execution_id),
                        )
                    else:
                        tag = await c.execute(
                            "UPDATE trade_executions SET status=$1, reason=$2, updated_at=NOW() WHERE id=$3 AND status = ANY($4::text[])",
                            status,
                            reason,
                            int(execution_id),
                            expected_list,
                        )
                else:
                    if expected_list is None:
                        tag = await c.execute(
                            "UPDATE trade_executions SET status=$1, reason=$2, exchange_order_ids_json=$3, updated_at=NOW() WHERE id=$4",
                            status,
                            reason,
                            exchange_order_ids_json,
                            int(execution_id),
                        )
                    else:
                        tag = await c.execute(
                            "UPDATE trade_executions SET status=$1, reason=$2, exchange_order_ids_json=$3, updated_at=NOW() WHERE id=$4 AND status = ANY($5::text[])",
                            status,
                            reason,
                            exchange_order_ids_json,
                            int(execution_id),
                            expected_list,
                        )
        changed = str(tag).endswith(" 1")
        if changed:
            _record_critical_counter("critical_db_rows_changed")
        return changed
    async with connect() as c:
        if exchange_order_ids_json is None:
            if expected_list is None:
                cur = await c.execute(
                    "UPDATE trade_executions SET status=?, reason=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                    (status, reason, int(execution_id)),
                )
            else:
                placeholders = ",".join(["?"] * len(expected_list))
                cur = await c.execute(
                    f"UPDATE trade_executions SET status=?, reason=?, updated_at=CURRENT_TIMESTAMP WHERE id=? AND status IN ({placeholders})",
                    (status, reason, int(execution_id), *expected_list),
                )
        else:
            if expected_list is None:
                cur = await c.execute(
                    "UPDATE trade_executions SET status=?, reason=?, exchange_order_ids_json=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                    (status, reason, exchange_order_ids_json, int(execution_id)),
                )
            else:
                placeholders = ",".join(["?"] * len(expected_list))
                cur = await c.execute(
                    f"UPDATE trade_executions SET status=?, reason=?, exchange_order_ids_json=?, updated_at=CURRENT_TIMESTAMP WHERE id=? AND status IN ({placeholders})",
                    (
                        status,
                        reason,
                        exchange_order_ids_json,
                        int(execution_id),
                        *expected_list,
                    ),
                )
        await c.commit()
        return int(cur.rowcount or 0) > 0


async def get_execution_by_id(execution_id: int) -> Dict[str, Any] | None:
    if is_postgres():
        advisory_conn = _current_advisory_connection()
        if advisory_conn is not None:
            row = await advisory_conn.fetchrow(
                "SELECT * FROM trade_executions WHERE id=$1", int(execution_id)
            )
            _record_critical_counter("critical_db_locked_reads_reused")
            return _dict(row) if row else None
        async with connect() as c:
            row = await c.fetchrow(
                "SELECT * FROM trade_executions WHERE id=$1", int(execution_id)
            )
            return _dict(row) if row else None
    async with connect() as c:
        cur = await c.execute(
            "SELECT * FROM trade_executions WHERE id=?", (int(execution_id),)
        )
        row = await cur.fetchone()
        return _dict(row) if row else None


def _merge_lists_safely(base: list[Any], patch: list[Any]) -> list[Any]:
    """Merge list patches without dropping already persisted TP/action entries.

    Dict items with tp_index+type or tp_index are updated by identity;
    all other items are appended if they are not already present.
    This keeps TP1/TP2 logs when another monitor adds its own section later.
    """

    def identity(item: Any) -> tuple[Any, ...] | None:
        if not isinstance(item, dict):
            return None
        if "tp_index" in item and "type" in item:
            return (item.get("type"), item.get("tp_index"))
        if "tp_index" in item:
            return ("tp", item.get("tp_index"))
        if "client_id" in item:
            return ("client_id", item.get("client_id"))
        return None

    # Normalize duplicate identities already present in durable JSON. Previous
    # versions indexed only the last duplicate and left earlier copies behind,
    # so a canonical TP repair could not actually remove duplicate tp_index rows
    # from PostgreSQL/SQLite. Merge duplicates in-place before applying the patch.
    out: list[Any] = []
    index: dict[tuple[Any, ...], int] = {}
    for item in base or []:
        ident = identity(item)
        if ident is not None and ident in index:
            old = out[index[ident]]
            if isinstance(old, dict) and isinstance(item, dict):
                out[index[ident]] = _deep_merge_dict(old, item)
            else:
                out[index[ident]] = item
            continue
        out.append(item)
        if ident is not None:
            index[ident] = len(out) - 1

    for item in patch or []:
        ident = identity(item)
        if ident is not None and ident in index:
            old = out[index[ident]]
            if isinstance(old, dict) and isinstance(item, dict):
                out[index[ident]] = _deep_merge_dict(old, item)
            else:
                out[index[ident]] = item
        elif item not in out:
            out.append(item)
    return out


def _deep_merge_dict(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    out = dict(base or {})
    for k, v in (patch or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge_dict(out[k], v)
        elif isinstance(v, list) and isinstance(out.get(k), list):
            out[k] = _merge_lists_safely(out[k], v)
        else:
            out[k] = v
    return out


def _maybe_attach_write_flow_audit(
    payload: dict[str, Any],
    *,
    status: str = "",
    stage: str | None = None,
) -> dict[str, Any]:
    """Attach non-secret write-flow audit after a full durable JSON merge.

    BE/recovery/limit-catchup workers usually pass only a small patch. Building
    the audit after the merge keeps MEXC-style diagnostics consistent with the
    exact row state that is being persisted. The helper is intentionally fail-open
    for diagnostics only: safety writes must not be blocked by audit formatting.
    """
    if not stage:
        return payload
    out = dict(payload or {})
    try:
        from app.services.write_flow_audit import build_write_flow_audit

        out["write_flow_audit_v1"] = build_write_flow_audit(
            out,
            status=str(status or ""),
            stage=str(stage or ""),
        )
    except Exception as exc:  # pragma: no cover - diagnostics must not block writes
        out["write_flow_audit_v1"] = {
            "version": 1,
            "status": str(status or ""),
            "stage": str(stage or ""),
            "audit_error": f"{type(exc).__name__}: {str(exc)[:300]}",
        }
    return out


async def update_execution_status_merge(
    execution_id: int,
    status: str,
    reason: str = "",
    exchange_order_ids_patch: dict[str, Any] | None = None,
    *,
    expected_status: str | list[str] | tuple[str, ...] | None = None,
    write_flow_audit_stage: str | None = None,
    write_flow_audit_status: str | None = None,
) -> bool:
    """Update execution status while preserving order-id sections.

    v1.0.15: optimistic DB-level retry is used in addition to the in-process
    execution_lock. This reduces the chance of losing JSON patches if two bot
    processes overlap during deploy. It is still strongly recommended to run
    only one worker process for this trading bot.

    v1.6.18: ``expected_status`` optionally makes the whole write (status +
    JSON) conditional on the row still being in that status (or one of a list
    of statuses). The JSON-equality CAS alone did not stop a stale, delayed
    conclusion from one worker pass from overwriting a newer, more current
    status written by a later pass -- both writes could pass their own
    JSON-equality check in sequence even though they disagreed about the
    trade's actual state. Callers should pass the status they read at the top
    of their own processing loop; without ``expected_status`` behavior is
    unchanged from before. Returns ``True`` only if the row was actually
    written; ``False`` means the row had already moved to a different status
    (or does not exist) and this call's conclusion is stale.
    """
    expected_list: list[str] | None = None
    if expected_status is not None:
        expected_list = (
            [str(expected_status)]
            if isinstance(expected_status, str)
            else [str(s) for s in expected_status]
        )

    if exchange_order_ids_patch is None:
        return await update_execution_status(
            execution_id, status, reason, None, expected_status=expected_list
        )

    for _attempt in range(5):
        current = await get_execution_by_id(execution_id)
        if not current:
            return False
        if (
            expected_list is not None
            and str(current.get("status") or "") not in expected_list
        ):
            return False
        old_json = (current or {}).get("exchange_order_ids_json") or "{}"
        current_payload: dict[str, Any] = {}
        try:
            current_payload = json.loads(old_json or "{}")
        except Exception:
            current_payload = {}

        merged = _deep_merge_dict(current_payload, exchange_order_ids_patch)
        merged = _maybe_attach_write_flow_audit(
            merged,
            status=write_flow_audit_status or status,
            stage=write_flow_audit_stage,
        )
        new_json = _ensure_bingx_execution_json(merged)

        if is_postgres():
            advisory_conn = _current_advisory_connection()
            if advisory_conn is not None:
                c = advisory_conn
                _record_critical_counter("critical_db_update_connection_reused")
                if expected_list is None:
                    tag = await c.execute(
                        """
                        UPDATE trade_executions
                        SET status=$1, reason=$2, exchange_order_ids_json=$3, updated_at=NOW()
                        WHERE id=$4 AND COALESCE(exchange_order_ids_json, '{}')=$5
                        """,
                        status,
                        reason,
                        new_json,
                        int(execution_id),
                        old_json,
                    )
                else:
                    tag = await c.execute(
                        """
                        UPDATE trade_executions
                        SET status=$1, reason=$2, exchange_order_ids_json=$3, updated_at=NOW()
                        WHERE id=$4 AND COALESCE(exchange_order_ids_json, '{}')=$5 AND status = ANY($6::text[])
                        """,
                        status,
                        reason,
                        new_json,
                        int(execution_id),
                        old_json,
                        expected_list,
                    )
            else:
                async with connect() as c:
                    if expected_list is None:
                        tag = await c.execute(
                            """
                            UPDATE trade_executions
                            SET status=$1, reason=$2, exchange_order_ids_json=$3, updated_at=NOW()
                            WHERE id=$4 AND COALESCE(exchange_order_ids_json, '{}')=$5
                            """,
                            status,
                            reason,
                            new_json,
                            int(execution_id),
                            old_json,
                        )
                    else:
                        tag = await c.execute(
                            """
                            UPDATE trade_executions
                            SET status=$1, reason=$2, exchange_order_ids_json=$3, updated_at=NOW()
                            WHERE id=$4 AND COALESCE(exchange_order_ids_json, '{}')=$5 AND status = ANY($6::text[])
                            """,
                            status,
                            reason,
                            new_json,
                            int(execution_id),
                            old_json,
                            expected_list,
                        )
            if str(tag).endswith(" 1"):
                _record_critical_counter("critical_db_rows_changed")
                return True
        else:
            async with connect() as c:
                if expected_list is None:
                    cur = await c.execute(
                        """
                        UPDATE trade_executions
                        SET status=?, reason=?, exchange_order_ids_json=?, updated_at=CURRENT_TIMESTAMP
                        WHERE id=? AND COALESCE(exchange_order_ids_json, '{}')=?
                        """,
                        (status, reason, new_json, int(execution_id), old_json),
                    )
                else:
                    placeholders = ",".join(["?"] * len(expected_list))
                    cur = await c.execute(
                        f"""
                        UPDATE trade_executions
                        SET status=?, reason=?, exchange_order_ids_json=?, updated_at=CURRENT_TIMESTAMP
                        WHERE id=? AND COALESCE(exchange_order_ids_json, '{{}}')=? AND status IN ({placeholders})
                        """,
                        (
                            status,
                            reason,
                            new_json,
                            int(execution_id),
                            old_json,
                            *expected_list,
                        ),
                    )
                await c.commit()
                if cur.rowcount == 1:
                    return True
        await asyncio.sleep(0.05 + random.random() * 0.05)

    # Last-resort merge after repeated conflicts. Prefer preserving latest row,
    # but still honor an explicit expected_status precondition -- silently
    # forcing a write here despite a status mismatch is exactly what v1.6.18
    # closes.
    current = await get_execution_by_id(execution_id)
    if not current:
        return False
    if (
        expected_list is not None
        and str(current.get("status") or "") not in expected_list
    ):
        return False
    try:
        current_payload = json.loads(
            (current or {}).get("exchange_order_ids_json") or "{}"
        )
    except Exception:
        current_payload = {}
    merged = _deep_merge_dict(current_payload, exchange_order_ids_patch)
    merged = _maybe_attach_write_flow_audit(
        merged,
        status=write_flow_audit_status or status,
        stage=write_flow_audit_stage,
    )
    return await update_execution_status(
        execution_id,
        status,
        reason,
        json.dumps(merged, ensure_ascii=False, default=str),
        expected_status=expected_list,
    )


def _parse_utc_iso(value: Any) -> datetime | None:
    try:
        text = str(value or "").strip().replace("Z", "+00:00")
        if not text:
            return None
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return None


def _market_event_datetime(value: Any, *, field: str) -> datetime | None:
    """Normalize a market-event timestamp for an asyncpg TIMESTAMPTZ bind.

    asyncpg intentionally rejects strings for timestamp parameters.  Accept
    legacy ISO text from older callers/rows, but convert it to one aware UTC
    datetime before a PostgreSQL statement is executed.
    """

    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    parsed = _parse_utc_iso(value)
    if parsed is None:
        raise ValueError(f"invalid {field} timestamp")
    return parsed


async def reserve_limit_cancel_write(
    execution_id: int,
    *,
    policy_reason: str,
    expected_status: str = "pending_limit",
    max_writes: int = 3,
    backoff_seconds: tuple[int, ...] = (30, 120, 600),
) -> dict[str, Any]:
    """Atomically reserve one exact LIMIT cancel write before network I/O.

    A reservation is itself counted as a consumed write attempt. This is
    intentional: if the process dies after the HTTP request leaves the worker
    but before its result is persisted, a restart must not be able to dispatch
    an uncounted fourth cancel. The next pass first performs read-only
    reconciliation until the durable backoff expires.
    """

    limit = max(1, int(max_writes or 1))
    delays = tuple(max(0, int(item)) for item in backoff_seconds) or (30,)
    token = uuid.uuid4().hex

    for _attempt in range(8):
        current = await get_execution_by_id(int(execution_id))
        if not current:
            return {"reserved": False, "reason": "execution_missing", "record": {}}
        if str(current.get("status") or "") != str(expected_status or ""):
            return {
                "reserved": False,
                "reason": "status_changed",
                "record": {},
            }

        old_json = current.get("exchange_order_ids_json") or "{}"
        try:
            payload = json.loads(old_json or "{}")
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}

        raw = payload.get("limit_cancel_pending")
        record = dict(raw) if isinstance(raw, dict) else {}
        try:
            writes = max(0, int(record.get("write_attempts") or 0))
        except (TypeError, ValueError, OverflowError):
            writes = 0

        now = datetime.now(timezone.utc)
        exhausted = bool(record.get("exhausted")) or writes >= limit
        if exhausted:
            return {
                "reserved": False,
                "reason": "exhausted",
                "record": record,
            }

        next_retry_at = _parse_utc_iso(record.get("next_retry_at"))
        if next_retry_at is not None and now < next_retry_at:
            return {
                "reserved": False,
                "reason": "deferred",
                "record": record,
                "next_retry_at": next_retry_at.isoformat(),
            }

        attempt_no = writes + 1
        delay = delays[min(attempt_no - 1, len(delays) - 1)]
        next_retry = now + timedelta(seconds=delay)
        reservation = {
            "version": 1,
            "token": token,
            "attempt_no": attempt_no,
            "reserved_at": now.isoformat(),
            "state": "reserved",
        }
        record.update(
            {
                "reason": str(policy_reason or "").strip().lower(),
                "error": "cancel write reserved before external request",
                "disposition": "write_reserved",
                "write_attempts": attempt_no,
                "max_write_attempts": limit,
                "write_attempted_last_pass": False,
                "first_detected_at": str(record.get("first_detected_at") or "").strip()
                or now.isoformat(),
                "last_checked_at": now.isoformat(),
                "last_write_reserved_at": now.isoformat(),
                "next_retry_at": next_retry.isoformat(),
                "exhausted": attempt_no >= limit,
                "read_only_last_pass": False,
                "manual_action_required": attempt_no >= limit,
                "reservation_v1": reservation,
            }
        )
        payload["limit_cancel_pending"] = record
        new_json = _ensure_bingx_execution_json(payload)

        if is_postgres():
            async with connect() as c:
                tag = await c.execute(
                    """
                    UPDATE trade_executions
                    SET exchange_order_ids_json=$1, updated_at=NOW()
                    WHERE id=$2
                      AND COALESCE(exchange_order_ids_json, '{}')=$3
                      AND status=$4
                    """,
                    new_json,
                    int(execution_id),
                    old_json,
                    str(expected_status),
                )
            written = str(tag).endswith(" 1")
        else:
            async with connect() as c:
                cur = await c.execute(
                    """
                    UPDATE trade_executions
                    SET exchange_order_ids_json=?, updated_at=CURRENT_TIMESTAMP
                    WHERE id=?
                      AND COALESCE(exchange_order_ids_json, '{}')=?
                      AND status=?
                    """,
                    (
                        new_json,
                        int(execution_id),
                        old_json,
                        str(expected_status),
                    ),
                )
                await c.commit()
                written = cur.rowcount == 1

        if written:
            return {
                "reserved": True,
                "reason": "reserved",
                "record": record,
                "reservation": reservation,
            }
        await asyncio.sleep(0.02 + random.random() * 0.03)

    return {"reserved": False, "reason": "cas_conflict", "record": {}}


async def merge_execution_metadata(
    execution_id: int,
    patch: dict[str, Any],
    *,
    expected_status: str | None = None,
    write_flow_audit_stage: str | None = None,
    write_flow_audit_status: str | None = None,
) -> bool:
    """Deep-merge execution JSON without rewriting status or reason.

    ``expected_status`` optionally makes the JSON update conditional on the row
    still being in the state inspected by the caller. This closes the small
    read/write window where an entry can fill while a pending-LIMIT runtime or
    policy patch is waiting on PostgreSQL.
    """

    if not patch:
        return False
    expected = str(expected_status or "").strip()
    for _attempt in range(5):
        current = await get_execution_by_id(int(execution_id))
        if not current:
            return False
        if expected and str(current.get("status") or "") != expected:
            return False
        old_json = current.get("exchange_order_ids_json") or "{}"
        try:
            current_payload = json.loads(old_json or "{}")
        except Exception:
            current_payload = {}
        merged = _deep_merge_dict(current_payload, patch)
        normalized_merged_json = _ensure_bingx_execution_json(merged)
        try:
            normalized_merged_payload = json.loads(normalized_merged_json)
        except Exception:
            normalized_merged_payload = merged
        if (
            monitor_workload_stage() == "critical"
            and not write_flow_audit_stage
            and normalized_merged_payload == current_payload
        ):
            _record_critical_counter("critical_db_writes_skipped")
            return True
        merged = _maybe_attach_write_flow_audit(
            merged,
            status=write_flow_audit_status or str(current.get("status") or ""),
            stage=write_flow_audit_stage,
        )
        new_json = _ensure_bingx_execution_json(merged)

        if is_postgres():
            advisory_conn = _current_advisory_connection()
            if advisory_conn is not None:
                c = advisory_conn
                _record_critical_counter("critical_db_update_connection_reused")
                if expected:
                    tag = await c.execute(
                        """
                        UPDATE trade_executions
                        SET exchange_order_ids_json=$1, updated_at=NOW()
                        WHERE id=$2
                          AND COALESCE(exchange_order_ids_json, '{}')=$3
                          AND status=$4
                        """,
                        new_json,
                        int(execution_id),
                        old_json,
                        expected,
                    )
                else:
                    tag = await c.execute(
                        """
                        UPDATE trade_executions
                        SET exchange_order_ids_json=$1, updated_at=NOW()
                        WHERE id=$2 AND COALESCE(exchange_order_ids_json, '{}')=$3
                        """,
                        new_json,
                        int(execution_id),
                        old_json,
                    )
            else:
                async with connect() as c:
                    if expected:
                        tag = await c.execute(
                            """
                            UPDATE trade_executions
                            SET exchange_order_ids_json=$1, updated_at=NOW()
                            WHERE id=$2
                              AND COALESCE(exchange_order_ids_json, '{}')=$3
                              AND status=$4
                            """,
                            new_json,
                            int(execution_id),
                            old_json,
                            expected,
                        )
                    else:
                        tag = await c.execute(
                            """
                            UPDATE trade_executions
                            SET exchange_order_ids_json=$1, updated_at=NOW()
                            WHERE id=$2 AND COALESCE(exchange_order_ids_json, '{}')=$3
                            """,
                            new_json,
                            int(execution_id),
                            old_json,
                        )
            if str(tag).endswith(" 1"):
                _record_critical_counter("critical_db_rows_changed")
                return True
        else:
            async with connect() as c:
                if expected:
                    cur = await c.execute(
                        """
                        UPDATE trade_executions
                        SET exchange_order_ids_json=?, updated_at=CURRENT_TIMESTAMP
                        WHERE id=?
                          AND COALESCE(exchange_order_ids_json, '{}')=?
                          AND status=?
                        """,
                        (new_json, int(execution_id), old_json, expected),
                    )
                else:
                    cur = await c.execute(
                        """
                        UPDATE trade_executions
                        SET exchange_order_ids_json=?, updated_at=CURRENT_TIMESTAMP
                        WHERE id=? AND COALESCE(exchange_order_ids_json, '{}')=?
                        """,
                        (new_json, int(execution_id), old_json),
                    )
                await c.commit()
                if cur.rowcount == 1:
                    return True
        await asyncio.sleep(0.05 + random.random() * 0.05)

    # Last-resort JSON-only optimistic retry. Never remove the state condition.
    current = await get_execution_by_id(int(execution_id))
    if not current:
        return False
    if expected and str(current.get("status") or "") != expected:
        return False
    old_json = current.get("exchange_order_ids_json") or "{}"
    try:
        current_payload = json.loads(old_json or "{}")
    except Exception:
        current_payload = {}
    merged = _deep_merge_dict(current_payload, patch)
    merged = _maybe_attach_write_flow_audit(
        merged,
        status=write_flow_audit_status or str(current.get("status") or ""),
        stage=write_flow_audit_stage,
    )
    new_json = _ensure_bingx_execution_json(merged)
    if is_postgres():
        async with connect() as c:
            if expected:
                tag = await c.execute(
                    """
                    UPDATE trade_executions
                    SET exchange_order_ids_json=$1, updated_at=NOW()
                    WHERE id=$2
                      AND COALESCE(exchange_order_ids_json, '{}')=$3
                      AND status=$4
                    """,
                    new_json,
                    int(execution_id),
                    old_json,
                    expected,
                )
            else:
                tag = await c.execute(
                    """
                    UPDATE trade_executions
                    SET exchange_order_ids_json=$1, updated_at=NOW()
                    WHERE id=$2 AND COALESCE(exchange_order_ids_json, '{}')=$3
                    """,
                    new_json,
                    int(execution_id),
                    old_json,
                )
        return str(tag).endswith(" 1")
    async with connect() as c:
        if expected:
            cur = await c.execute(
                """
                UPDATE trade_executions
                SET exchange_order_ids_json=?, updated_at=CURRENT_TIMESTAMP
                WHERE id=?
                  AND COALESCE(exchange_order_ids_json, '{}')=?
                  AND status=?
                """,
                (new_json, int(execution_id), old_json, expected),
            )
        else:
            cur = await c.execute(
                """
                UPDATE trade_executions
                SET exchange_order_ids_json=?, updated_at=CURRENT_TIMESTAMP
                WHERE id=? AND COALESCE(exchange_order_ids_json, '{}')=?
                """,
                (new_json, int(execution_id), old_json),
            )
        await c.commit()
        return cur.rowcount == 1


async def merge_latest_execution_metadata(
    signal_hash: str,
    user_id: int,
    patch: dict[str, Any],
) -> bool:
    """Merge diagnostic metadata into the newest execution for signal+user.

    The dispatcher learns final latency only after the main execution row has
    been inserted. This helper preserves every existing TP/SL/order section and
    adds the timing patch optimistically. Preflight skips have no execution row
    and simply return False.
    """
    if not signal_hash or not patch:
        return False
    if is_postgres():
        async with connect() as c:
            row = await c.fetchrow(
                """
                SELECT id
                FROM trade_executions
                WHERE signal_hash=$1 AND user_id=$2
                ORDER BY id DESC
                LIMIT 1
                """,
                str(signal_hash),
                int(user_id),
            )
    else:
        async with connect() as c:
            cur = await c.execute(
                """
                SELECT id
                FROM trade_executions
                WHERE signal_hash=? AND user_id=?
                ORDER BY id DESC
                LIMIT 1
                """,
                (str(signal_hash), int(user_id)),
            )
            row = await cur.fetchone()
    if not row:
        return False
    data = _dict(row)
    return await merge_execution_metadata(int(data["id"]), patch)


_FAIL_CLOSED_EXECUTION_RISK_PERCENT = 5.0


def _positive_finite_percent(value: Any) -> float | None:
    """Parse a positive finite percentage from persisted legacy data."""
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed) or parsed <= 0:
        return None
    return min(100.0, parsed)


def _execution_payload_dict(row: Dict[str, Any]) -> dict[str, Any]:
    """Backward-compatible wrapper around the shared exposure classifier."""
    return execution_payload_dict(row)


def _finite_number(value: Any) -> float | None:
    return finite_number(value)


def _execution_zero_exposure_confirmed(row: Dict[str, Any]) -> bool:
    return execution_zero_exposure_confirmed(row)


def _execution_cleanup_unresolved(row: Dict[str, Any]) -> bool:
    return execution_cleanup_unresolved(row)


def _effective_execution_risk_percent(row: Dict[str, Any]) -> tuple[float, bool, bool]:
    """Return conservative execution risk and integrity markers.

    The durable DB column is the pre-trade risk snapshot.  MARKET orders also
    persist the post-fill realised risk in ``market_fill_risk_v1``.  Admission
    must use the larger value so adverse slippage cannot reduce portfolio risk.
    Corrupt/zero/NaN legacy values fail closed to the maximum supported
    per-trade risk rather than silently becoming 0%.

    Returns ``(risk, used_integrity_fallback, market_adjusted)``.
    """
    saved = _positive_finite_percent(row.get("risk_percent"))
    realized: float | None = None
    try:
        payload = json.loads(row.get("exchange_order_ids_json") or "{}")
        if isinstance(payload, dict):
            snapshot = payload.get("market_fill_risk_v1")
            if isinstance(snapshot, dict):
                realized = _positive_finite_percent(
                    snapshot.get("realized_risk_percent")
                )
    except (TypeError, ValueError, json.JSONDecodeError):
        realized = None

    candidates = [value for value in (saved, realized) if value is not None]
    if not candidates:
        return _FAIL_CLOSED_EXECUTION_RISK_PERCENT, True, False
    effective = max(candidates)
    return (
        effective,
        False,
        bool(realized is not None and (saved is None or realized > saved)),
    )


async def user_risk_state(
    user_id: int, *, settings_row: UserSettings | None = None
) -> Dict[str, Any]:
    """Aggregate current live execution risk before opening a VIP trade.

    Confirmed BE can release an otherwise active execution when the user enabled
    that policy. Confirmed position closure always releases slots, portfolio
    risk and today's live-risk usage immediately, including legacy rows that
    remained ``manual_required`` only because cleanup/history needed review.
    Ambiguous/open/reverse-position rows remain fail-closed.
    """
    active_statuses = {
        "opening_intent",
        "opened",
        "pending_limit",
        "protected",
        "partial_error",
        "manual_required",
        "partial_unrecoverable",
    }
    # Daily usage follows the user's live-risk policy: once a position/LIMIT is
    # confirmed closed or cancelled, its percentage is released immediately.
    # ``closed_stop_catchup`` was previously kept until 00:00 UTC and caused a
    # confirmed STOP close to keep inflating the day limit.
    daily_statuses = set(active_statuses)

    if settings_row is None:
        settings_row = await get_user_settings(user_id)
    exclude_be = bool(getattr(settings_row, "exclude_be_trades_from_risk", False))

    def be_moved(row: Dict[str, Any]) -> bool:
        return execution_be_protection_confirmed(row)

    queried_statuses = active_statuses | daily_statuses
    if is_postgres():
        async with connect() as c:
            rows = await c.fetch(
                """
                SELECT id, user_id, symbol, side, qty, status, risk_percent, exchange_order_ids_json, created_at,
                       (created_at AT TIME ZONE 'UTC' >= date_trunc('day', NOW() AT TIME ZONE 'UTC')) AS is_today
                FROM trade_executions
                WHERE user_id=$1
                  AND status = ANY($2::text[])
                """,
                user_id,
                list(queried_statuses),
            )
            all_rows = [_dict(r) for r in rows]
    else:
        placeholders = ",".join(["?"] * len(queried_statuses))
        async with connect() as c:
            cur = await c.execute(
                f"""
                SELECT id, user_id, symbol, side, qty, status, risk_percent, exchange_order_ids_json, created_at,
                       CASE WHEN datetime(created_at) >= datetime('now','start of day')
                            THEN 1 ELSE 0 END AS is_today
                FROM trade_executions
                WHERE user_id=? AND status IN ({placeholders})
                """,
                (user_id, *queried_statuses),
            )
            all_rows = [_dict(r) for r in await cur.fetchall()]

    all_rows = _bingx_rows(all_rows)
    active_status_rows = [
        row for row in all_rows if str(row.get("status") or "") in active_statuses
    ]
    closed_released_rows = [
        row for row in active_status_rows if _execution_zero_exposure_confirmed(row)
    ]
    exposure_rows = [
        row for row in active_status_rows if not _execution_zero_exposure_confirmed(row)
    ]
    today_exposure_rows = [
        row
        for row in exposure_rows
        if bool(row.get("is_today")) and str(row.get("status") or "") in daily_statuses
    ]

    active_total_count = len(exposure_rows)
    be_released_count = sum(1 for row in exposure_rows if be_moved(row))

    if exclude_be:
        active_risk_rows = [row for row in exposure_rows if not be_moved(row)]
        daily_risk_rows = [row for row in today_exposure_rows if not be_moved(row)]
    else:
        active_risk_rows = exposure_rows
        daily_risk_rows = today_exposure_rows

    active_values = [_effective_execution_risk_percent(row) for row in active_risk_rows]
    daily_values = [_effective_execution_risk_percent(row) for row in daily_risk_rows]
    evaluated_by_id: dict[int, tuple[float, bool, bool]] = {}
    for row in (*active_risk_rows, *daily_risk_rows):
        evaluated_by_id[int(row.get("id") or 0)] = _effective_execution_risk_percent(
            row
        )
    fallback_ids = {
        execution_id for execution_id, result in evaluated_by_id.items() if result[1]
    }
    market_adjusted_ids = {
        execution_id for execution_id, result in evaluated_by_id.items() if result[2]
    }

    cleanup_pending_rows = [
        row for row in closed_released_rows if _execution_cleanup_unresolved(row)
    ]
    active_ids = [int(row.get("id") or 0) for row in active_risk_rows]
    released_ids = [int(row.get("id") or 0) for row in closed_released_rows]
    cleanup_pending_ids = [int(row.get("id") or 0) for row in cleanup_pending_rows]
    if closed_released_rows:
        log.info(
            "risk reconciliation user_id=%s active_ids=%s released_closed_ids=%s cleanup_pending_ids=%s",
            int(user_id),
            active_ids[:50],
            released_ids[:50],
            cleanup_pending_ids[:50],
        )

    return {
        "active_count": len(active_risk_rows),
        "active_risk_percent": float(sum(value[0] for value in active_values)),
        "daily_risk_percent": float(sum(value[0] for value in daily_values)),
        "active_total_count": active_total_count,
        "be_released_count": be_released_count,
        "closed_risk_released_count": len(closed_released_rows),
        "closed_cleanup_pending_count": len(cleanup_pending_rows),
        "active_execution_ids": active_ids,
        "closed_risk_released_execution_ids": released_ids,
        "closed_cleanup_pending_execution_ids": cleanup_pending_ids,
        "exclude_be_trades_from_risk": exclude_be,
        "risk_integrity_fallback_count": len(fallback_ids),
        "market_risk_adjusted_count": len(market_adjusted_ids),
    }


async def _manual_required_be_recovery_executions(
    limit: int = 100, *, after_id: int = 0
) -> List[Dict[str, Any]]:
    """Return only manual_required rows carrying a durable BE checkpoint.

    SQL prefiltering avoids locking/re-reading every unrelated manual-review
    execution. Service code still performs the authoritative typed JSON check.
    """

    requested = max(0, min(int(limit or 0), 5000))
    if requested <= 0:
        return []
    cursor = max(0, int(after_id or 0))
    page_size = max(200, min(2000, requested * 4))
    result: list[Dict[str, Any]] = []
    key_patterns = (
        '%"replacement_write_intent_v1"%',
        '%"cleanup_cancel_intent_v1"%',
        '%"replacement_in_progress"%',
    )
    if is_postgres():
        async with connect() as c:
            while len(result) < requested:
                rows = await c.fetch(
                    """SELECT * FROM trade_executions
                    WHERE status='manual_required' AND id>$1
                      AND (exchange_order_ids_json LIKE $2
                           OR exchange_order_ids_json LIKE $3
                           OR exchange_order_ids_json LIKE $4)
                    ORDER BY id ASC LIMIT $5""",
                    cursor,
                    key_patterns[0],
                    key_patterns[1],
                    key_patterns[2],
                    page_size,
                )
                raw = [_dict(row) for row in rows]
                if not raw:
                    break
                next_cursor = int(raw[-1].get("id") or 0)
                if next_cursor <= cursor:
                    break
                cursor = next_cursor
                for row in _bingx_rows(raw):
                    payload = _execution_payload_dict(row)
                    be_state = (
                        payload.get("be") if isinstance(payload.get("be"), dict) else {}
                    )
                    if (
                        isinstance(be_state.get("replacement_write_intent_v1"), dict)
                        or isinstance(be_state.get("cleanup_cancel_intent_v1"), dict)
                        or (
                            be_state.get("replacement_in_progress")
                            and clean_exchange_id(
                                be_state.get("replacement_stop_id")
                                or be_state.get("verify_matching_stop_order_id")
                            )
                        )
                    ):
                        result.append(row)
                        if len(result) >= requested:
                            break
                if len(raw) < page_size:
                    break
        return result[:requested]

    async with connect() as c:
        while len(result) < requested:
            cur = await c.execute(
                """SELECT * FROM trade_executions
                WHERE status='manual_required' AND id>?
                  AND (exchange_order_ids_json LIKE ?
                       OR exchange_order_ids_json LIKE ?
                       OR exchange_order_ids_json LIKE ?)
                ORDER BY id ASC LIMIT ?""",
                (cursor, *key_patterns, page_size),
            )
            raw = [_dict(row) for row in await cur.fetchall()]
            if not raw:
                break
            next_cursor = int(raw[-1].get("id") or 0)
            if next_cursor <= cursor:
                break
            cursor = next_cursor
            for row in _bingx_rows(raw):
                payload = _execution_payload_dict(row)
                be_state = (
                    payload.get("be") if isinstance(payload.get("be"), dict) else {}
                )
                if (
                    isinstance(be_state.get("replacement_write_intent_v1"), dict)
                    or isinstance(be_state.get("cleanup_cancel_intent_v1"), dict)
                    or (
                        be_state.get("replacement_in_progress")
                        and clean_exchange_id(
                            be_state.get("replacement_stop_id")
                            or be_state.get("verify_matching_stop_order_id")
                        )
                    )
                ):
                    result.append(row)
                    if len(result) >= requested:
                        break
            if len(raw) < page_size:
                break
    return result[:requested]


async def be_monitor_executions(
    limit: int = 100, *, after_id: int = 0
) -> List[Dict[str, Any]]:
    """Rotating page of active BE rows plus checkpointed recovery only."""

    requested = max(0, min(int(limit or 0), 5000))
    if requested <= 0:
        return []
    # Keep the two reads sequential.  BE monitoring is background work and
    # must not consume two PostgreSQL connections at once merely to build one
    # page, especially during startup/backlog recovery.
    normal_rows = await _paged_bingx_execution_rows(
        ["opened", "protected", "partial_error"],
        limit=requested,
        after_id=after_id,
    )
    recovery_rows = await _manual_required_be_recovery_executions(
        limit=requested,
        after_id=after_id,
    )
    by_id: dict[int, Dict[str, Any]] = {}
    for row in [*normal_rows, *recovery_rows]:
        row_id = int(row.get("id") or 0)
        if row_id > int(after_id or 0):
            by_id[row_id] = row
    return [by_id[row_id] for row_id in sorted(by_id)[:requested]]


async def protected_background_tp_executions(
    limit: int = 50, *, after_id: int = 0
) -> List[Dict[str, Any]]:
    """Protected BingX rows that may carry a queued background TP durable job.

    The JSON marker is filtered in service code for SQLite/PostgreSQL parity;
    this helper intentionally returns only explicit BingX protected rows.
    """
    return await _paged_bingx_execution_rows(
        ["protected"], limit=limit, after_id=after_id
    )


async def partial_error_executions(
    limit: int = 50, *, after_id: int = 0
) -> List[Dict[str, Any]]:
    """Rotating page of incomplete or legacy-unrecoverable TP protection rows."""
    return await _paged_bingx_execution_rows(
        ["partial_error", "partial_unrecoverable"],
        limit=limit,
        after_id=after_id,
    )


async def get_durable_notification(dedup_key: str) -> Dict[str, Any] | None:
    key = str(dedup_key or "").strip()
    if not key:
        return None
    if is_postgres():
        async with connect() as c:
            row = await c.fetchrow(
                "SELECT * FROM durable_notifications WHERE dedup_key=$1", key
            )
            return _dict(row) if row else None
    async with connect() as c:
        cur = await c.execute(
            "SELECT * FROM durable_notifications WHERE dedup_key=?", (key,)
        )
        row = await cur.fetchone()
        return _dict(row) if row else None


async def upsert_durable_notification(
    *,
    dedup_key: str,
    user_id: int,
    message_text: str,
    source: str,
    attempts: int,
    next_attempt_at: str | None,
    last_error: str = "",
    delivered: bool = False,
    reply_markup_json: str | None = None,
) -> None:
    key = str(dedup_key or "").strip()
    if not key:
        raise ValueError("durable notification dedup_key is required")
    status = "delivered" if delivered else "pending"
    if is_postgres():
        async with connect() as c:
            await c.execute(
                """
                INSERT INTO durable_notifications(
                    dedup_key,user_id,message_text,reply_markup_json,source,status,attempts,
                    next_attempt_at,last_error,delivered_at,updated_at
                ) VALUES($1,$2,$3,$4,$5,$6,$7,
                    CASE WHEN $8::text IS NULL THEN NOW() ELSE $8::timestamptz END,
                    $9,CASE WHEN $10 THEN NOW() ELSE NULL END,NOW())
                ON CONFLICT(dedup_key) DO UPDATE SET
                    user_id=CASE
                        WHEN durable_notifications.status='processing'
                         AND COALESCE(durable_notifications.claim_expires_at,
                                      durable_notifications.updated_at)>=NOW()
                            THEN durable_notifications.user_id
                        ELSE EXCLUDED.user_id
                    END,
                    message_text=CASE
                        WHEN durable_notifications.status='processing'
                         AND COALESCE(durable_notifications.claim_expires_at,
                                      durable_notifications.updated_at)>=NOW()
                            THEN durable_notifications.message_text
                        ELSE EXCLUDED.message_text
                    END,
                    reply_markup_json=CASE
                        WHEN durable_notifications.status='processing'
                         AND COALESCE(durable_notifications.claim_expires_at,
                                      durable_notifications.updated_at)>=NOW()
                            THEN durable_notifications.reply_markup_json
                        ELSE EXCLUDED.reply_markup_json
                    END,
                    source=CASE
                        WHEN durable_notifications.status='processing'
                         AND COALESCE(durable_notifications.claim_expires_at,
                                      durable_notifications.updated_at)>=NOW()
                            THEN durable_notifications.source
                        ELSE EXCLUDED.source
                    END,
                    status=CASE
                        WHEN durable_notifications.status='processing'
                         AND COALESCE(durable_notifications.claim_expires_at,
                                      durable_notifications.updated_at)>=NOW()
                            THEN 'processing'
                        ELSE EXCLUDED.status
                    END,
                    attempts=CASE
                        WHEN durable_notifications.status='processing'
                         AND COALESCE(durable_notifications.claim_expires_at,
                                      durable_notifications.updated_at)>=NOW()
                            THEN durable_notifications.attempts
                        ELSE EXCLUDED.attempts
                    END,
                    next_attempt_at=CASE
                        WHEN durable_notifications.status='processing'
                         AND COALESCE(durable_notifications.claim_expires_at,
                                      durable_notifications.updated_at)>=NOW()
                            THEN durable_notifications.next_attempt_at
                        ELSE EXCLUDED.next_attempt_at
                    END,
                    last_error=CASE
                        WHEN durable_notifications.status='processing'
                         AND COALESCE(durable_notifications.claim_expires_at,
                                      durable_notifications.updated_at)>=NOW()
                            THEN durable_notifications.last_error
                        ELSE EXCLUDED.last_error
                    END,
                    delivered_at=CASE
                        WHEN durable_notifications.status='processing'
                         AND COALESCE(durable_notifications.claim_expires_at,
                                      durable_notifications.updated_at)>=NOW()
                            THEN durable_notifications.delivered_at
                        WHEN $10 THEN NOW() ELSE NULL
                    END,
                    claim_token=CASE
                        WHEN durable_notifications.status='processing'
                         AND COALESCE(durable_notifications.claim_expires_at,
                                      durable_notifications.updated_at)>=NOW()
                            THEN durable_notifications.claim_token
                        ELSE NULL
                    END,
                    claim_expires_at=CASE
                        WHEN durable_notifications.status='processing'
                         AND COALESCE(durable_notifications.claim_expires_at,
                                      durable_notifications.updated_at)>=NOW()
                            THEN durable_notifications.claim_expires_at
                        ELSE NULL
                    END,
                    updated_at=NOW()
                """,
                key,
                int(user_id),
                str(message_text),
                reply_markup_json,
                str(source or "monitor"),
                status,
                int(attempts),
                next_attempt_at,
                str(last_error or ""),
                bool(delivered),
            )
        return
    async with connect() as c:
        await c.execute(
            """
            INSERT INTO durable_notifications(
                dedup_key,user_id,message_text,reply_markup_json,source,status,attempts,
                next_attempt_at,last_error,delivered_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,COALESCE(?,CURRENT_TIMESTAMP),?,
                CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE NULL END,CURRENT_TIMESTAMP)
            ON CONFLICT(dedup_key) DO UPDATE SET
                user_id=CASE
                    WHEN durable_notifications.status='processing'
                     AND datetime(COALESCE(durable_notifications.claim_expires_at,
                                           durable_notifications.updated_at))>=datetime('now')
                        THEN durable_notifications.user_id
                    ELSE excluded.user_id
                END,
                message_text=CASE
                    WHEN durable_notifications.status='processing'
                     AND datetime(COALESCE(durable_notifications.claim_expires_at,
                                           durable_notifications.updated_at))>=datetime('now')
                        THEN durable_notifications.message_text
                    ELSE excluded.message_text
                END,
                reply_markup_json=CASE
                    WHEN durable_notifications.status='processing'
                     AND datetime(COALESCE(durable_notifications.claim_expires_at,
                                           durable_notifications.updated_at))>=datetime('now')
                        THEN durable_notifications.reply_markup_json
                    ELSE excluded.reply_markup_json
                END,
                source=CASE
                    WHEN durable_notifications.status='processing'
                     AND datetime(COALESCE(durable_notifications.claim_expires_at,
                                           durable_notifications.updated_at))>=datetime('now')
                        THEN durable_notifications.source
                    ELSE excluded.source
                END,
                status=CASE
                    WHEN durable_notifications.status='processing'
                     AND datetime(COALESCE(durable_notifications.claim_expires_at,
                                           durable_notifications.updated_at))>=datetime('now')
                        THEN 'processing'
                    ELSE excluded.status
                END,
                attempts=CASE
                    WHEN durable_notifications.status='processing'
                     AND datetime(COALESCE(durable_notifications.claim_expires_at,
                                           durable_notifications.updated_at))>=datetime('now')
                        THEN durable_notifications.attempts
                    ELSE excluded.attempts
                END,
                next_attempt_at=CASE
                    WHEN durable_notifications.status='processing'
                     AND datetime(COALESCE(durable_notifications.claim_expires_at,
                                           durable_notifications.updated_at))>=datetime('now')
                        THEN durable_notifications.next_attempt_at
                    ELSE excluded.next_attempt_at
                END,
                last_error=CASE
                    WHEN durable_notifications.status='processing'
                     AND datetime(COALESCE(durable_notifications.claim_expires_at,
                                           durable_notifications.updated_at))>=datetime('now')
                        THEN durable_notifications.last_error
                    ELSE excluded.last_error
                END,
                delivered_at=CASE
                    WHEN durable_notifications.status='processing'
                     AND datetime(COALESCE(durable_notifications.claim_expires_at,
                                           durable_notifications.updated_at))>=datetime('now')
                        THEN durable_notifications.delivered_at
                    WHEN ? THEN CURRENT_TIMESTAMP ELSE NULL
                END,
                claim_token=CASE
                    WHEN durable_notifications.status='processing'
                     AND datetime(COALESCE(durable_notifications.claim_expires_at,
                                           durable_notifications.updated_at))>=datetime('now')
                        THEN durable_notifications.claim_token
                    ELSE NULL
                END,
                claim_expires_at=CASE
                    WHEN durable_notifications.status='processing'
                     AND datetime(COALESCE(durable_notifications.claim_expires_at,
                                           durable_notifications.updated_at))>=datetime('now')
                        THEN durable_notifications.claim_expires_at
                    ELSE NULL
                END,
                updated_at=CURRENT_TIMESTAMP
            """,
            (
                key,
                int(user_id),
                str(message_text),
                reply_markup_json,
                str(source or "monitor"),
                status,
                int(attempts),
                next_attempt_at,
                str(last_error or ""),
                1 if delivered else 0,
                1 if delivered else 0,
            ),
        )
        await c.commit()


async def _claim_durable_notifications(
    *,
    limit: int,
    dedup_key: str | None = None,
    lease_seconds: float = 60.0,
) -> List[Dict[str, Any]]:
    """Atomically claim due outbox rows across processes and deploy overlap."""

    lim = max(1, min(int(limit or 100), 1000))
    token = uuid.uuid4().hex
    lease_sec = max(15.0, min(float(lease_seconds or 60.0), 900.0))
    key = str(dedup_key or "").strip()

    if is_postgres():
        async with connect() as c:
            rows = await c.fetch(
                """WITH picked AS (
                       SELECT id
                       FROM durable_notifications
                       WHERE ($4::text='' OR dedup_key=$4)
                         AND (
                           (status='pending' AND next_attempt_at<=NOW())
                           OR (
                             status='processing'
                             AND COALESCE(claim_expires_at,updated_at) < NOW()
                           )
                         )
                       ORDER BY next_attempt_at,id
                       FOR UPDATE SKIP LOCKED
                       LIMIT $1
                   )
                   UPDATE durable_notifications d
                   SET status='processing',claim_token=$2,
                       claim_generation=COALESCE(claim_generation,0)+1,
                       claim_expires_at=NOW()+($3::double precision*INTERVAL '1 second'),
                       updated_at=NOW()
                   FROM picked
                   WHERE d.id=picked.id
                   RETURNING d.*""",
                lim,
                token,
                lease_sec,
                key,
            )
            return [_dict(row) for row in rows]

    async with connect() as c:
        await c.execute("BEGIN IMMEDIATE")
        where_key = " AND dedup_key=?" if key else ""
        params: list[Any] = []
        if key:
            params.append(key)
        params.append(lim)
        cur = await c.execute(
            f"""SELECT id
                FROM durable_notifications
                WHERE (
                    (status='pending' AND datetime(next_attempt_at)<=datetime('now'))
                    OR (
                        status='processing'
                        AND datetime(COALESCE(claim_expires_at,updated_at))<datetime('now')
                    )
                ){where_key}
                ORDER BY datetime(next_attempt_at),id
                LIMIT ?""",
            tuple(params),
        )
        ids = [int(row[0]) for row in await cur.fetchall()]
        if not ids:
            await c.commit()
            return []
        ph = ",".join(["?"] * len(ids))
        expires = (datetime.now(timezone.utc) + timedelta(seconds=lease_sec)).isoformat()
        await c.execute(
            f"""UPDATE durable_notifications
                SET status='processing',claim_token=?,
                    claim_generation=COALESCE(claim_generation,0)+1,
                    claim_expires_at=?,updated_at=CURRENT_TIMESTAMP
                WHERE id IN ({ph})""",
            (token, expires, *ids),
        )
        cur = await c.execute(
            f"SELECT * FROM durable_notifications WHERE id IN ({ph}) ORDER BY id",
            ids,
        )
        rows = [_dict(row) for row in await cur.fetchall()]
        await c.commit()
        return rows


async def claim_due_durable_notifications(
    limit: int = 100, *, lease_seconds: float = 60.0
) -> List[Dict[str, Any]]:
    return await _claim_durable_notifications(
        limit=limit, lease_seconds=lease_seconds
    )


async def claim_durable_notification_by_key(
    dedup_key: str, *, lease_seconds: float = 60.0
) -> Dict[str, Any] | None:
    rows = await _claim_durable_notifications(
        limit=1, dedup_key=dedup_key, lease_seconds=lease_seconds
    )
    return rows[0] if rows else None


async def due_durable_notifications(limit: int = 100) -> List[Dict[str, Any]]:
    """Backward-compatible alias that now returns atomically claimed rows."""
    return await claim_due_durable_notifications(limit=limit)


async def complete_durable_notification_claim(
    notification_id: int,
    *,
    claim_token: str,
    claim_generation: int,
    delivered: bool,
    attempts: int,
    next_attempt_at: str | None,
    last_error: str,
) -> bool:
    """CAS-complete one claimed notification without cross-replica overwrite."""

    token = str(claim_token or "")
    generation = int(claim_generation or 0)
    if int(notification_id or 0) <= 0 or not token or generation <= 0:
        return False
    status = "delivered" if delivered else "pending"
    safe_error = str(last_error or "")[:1000]
    if is_postgres():
        async with connect() as c:
            tag = await c.execute(
                """UPDATE durable_notifications
                   SET status=$1,attempts=$2,
                       next_attempt_at=CASE
                           WHEN $3 THEN NOW()
                           WHEN $4::text IS NULL THEN NOW()
                           ELSE $4::timestamptz
                       END,
                       last_error=$5,
                       delivered_at=CASE WHEN $3 THEN NOW() ELSE NULL END,
                       claim_token=NULL,claim_expires_at=NULL,updated_at=NOW()
                   WHERE id=$6 AND status='processing'
                     AND claim_token=$7 AND claim_generation=$8""",
                status,
                max(0, int(attempts or 0)),
                bool(delivered),
                next_attempt_at,
                safe_error,
                int(notification_id),
                token,
                generation,
            )
        return str(tag).endswith(" 1")

    async with connect() as c:
        cur = await c.execute(
            """UPDATE durable_notifications
               SET status=?,attempts=?,next_attempt_at=COALESCE(?,CURRENT_TIMESTAMP),
                   last_error=?,
                   delivered_at=CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE NULL END,
                   claim_token=NULL,claim_expires_at=NULL,updated_at=CURRENT_TIMESTAMP
               WHERE id=? AND status='processing'
                 AND claim_token=? AND claim_generation=?""",
            (
                status,
                max(0, int(attempts or 0)),
                next_attempt_at,
                safe_error,
                1 if delivered else 0,
                int(notification_id),
                token,
                generation,
            ),
        )
        await c.commit()
        return int(cur.rowcount or 0) == 1


async def prune_durable_notifications(
    *, delivered_retention_days: int = 30, limit: int = 1000
) -> int:
    """Delete only old delivered outbox rows in bounded batches."""

    days = max(7, min(int(delivered_retention_days or 30), 3650))
    lim = max(1, min(int(limit or 1000), 10000))
    if is_postgres():
        async with connect() as c:
            tag = await c.execute(
                """DELETE FROM durable_notifications
                   WHERE id IN (
                       SELECT id FROM durable_notifications
                       WHERE status='delivered'
                         AND delivered_at < NOW()-($1::integer*INTERVAL '1 day')
                       ORDER BY delivered_at,id
                       LIMIT $2
                   )""",
                days,
                lim,
            )
        try:
            return int(str(tag).split()[-1])
        except (TypeError, ValueError, IndexError):
            return 0

    async with connect() as c:
        cur = await c.execute(
            """SELECT id FROM durable_notifications
               WHERE status='delivered'
                 AND datetime(delivered_at)<datetime('now',?)
               ORDER BY datetime(delivered_at),id LIMIT ?""",
            (f"-{days} days", lim),
        )
        ids = [int(row[0]) for row in await cur.fetchall()]
        if not ids:
            return 0
        ph = ",".join(["?"] * len(ids))
        cur = await c.execute(
            f"DELETE FROM durable_notifications WHERE id IN ({ph})", ids
        )
        await c.commit()
        return max(0, int(cur.rowcount or 0))


async def has_accepted_terms(
    user_id: int, agreement_version: str, agreement_hash: str
) -> bool:
    """Return True only if the user accepted the exact current terms version/hash."""
    await ensure_user(user_id)
    if is_postgres():
        async with connect() as c:
            row = await c.fetchrow(
                """SELECT id FROM user_agreements
                WHERE user_id=$1 AND agreement_version=$2 AND agreement_hash=$3
                ORDER BY accepted_at DESC LIMIT 1""",
                user_id,
                agreement_version,
                agreement_hash,
            )
            return bool(row)
    async with connect() as c:
        cur = await c.execute(
            """SELECT id FROM user_agreements
            WHERE user_id=? AND agreement_version=? AND agreement_hash=?
            ORDER BY accepted_at DESC LIMIT 1""",
            (user_id, agreement_version, agreement_hash),
        )
        row = await cur.fetchone()
        return bool(row)


async def accept_terms(
    user_id: int,
    username: str | None,
    agreement_version: str,
    agreement_hash: str,
    accepted_text: str,
) -> None:
    """Persist a Telegram click-acceptance for the current terms."""
    await ensure_user(user_id, username)
    if is_postgres():
        async with connect() as c:
            await c.execute(
                """INSERT INTO user_agreements(user_id, username, agreement_version, agreement_hash, accepted_text)
                VALUES($1,$2,$3,$4,$5)""",
                user_id,
                username,
                agreement_version,
                agreement_hash,
                accepted_text,
            )
        return
    async with connect() as c:
        await c.execute(
            """INSERT INTO user_agreements(user_id, username, agreement_version, agreement_hash, accepted_text)
            VALUES(?,?,?,?,?)""",
            (user_id, username, agreement_version, agreement_hash, accepted_text),
        )
        await c.commit()


# ---------------------------------------------------------------------------
# v1.6.0 event-driven group monitoring
# ---------------------------------------------------------------------------

MARKET_EVENT_PRIORITY_STOP = 0
MARKET_EVENT_PRIORITY_TP = 10
MARKET_EVENT_PRIORITY_ENTRY = 20
MARKET_EVENT_LEASE_SECONDS = 120.0
# Orphan cleanup is administrative maintenance, never part of the 250ms
# critical STOP/TP claim path. Each process reserves at most one cleanup pass
# per interval; the SQL itself is idempotent across replicas.
MARKET_EVENT_ORPHAN_CLEANUP_INTERVAL_SEC = 600.0
_MARKET_EVENT_ORPHAN_CLEANUP_LAST_MONO = 0.0


def _reserve_market_event_orphan_cleanup(watch_lane: str) -> bool:
    global _MARKET_EVENT_ORPHAN_CLEANUP_LAST_MONO
    if str(watch_lane or "").strip().lower() != "admin":
        return False
    now = time.monotonic()
    if (
        _MARKET_EVENT_ORPHAN_CLEANUP_LAST_MONO > 0.0
        and now - _MARKET_EVENT_ORPHAN_CLEANUP_LAST_MONO
        < MARKET_EVENT_ORPHAN_CLEANUP_INTERVAL_SEC
    ):
        return False
    _MARKET_EVENT_ORPHAN_CLEANUP_LAST_MONO = now
    return True


def market_event_priority(event_type: Any) -> int:
    """Return the durable safety order used before any HTTP work starts."""

    kind = str(event_type or "").strip().upper()
    if kind == "STOP":
        return MARKET_EVENT_PRIORITY_STOP
    if kind == "TP":
        return MARKET_EVENT_PRIORITY_TP
    return MARKET_EVENT_PRIORITY_ENTRY

_GROUP_ACTIVE_EXECUTION_STATUSES = (
    "opened",
    "pending_limit",
    "protected",
    "partial_error",
    "manual_required",
    "partial_unrecoverable",
    "closed_pending_history",
    "closed_on_exchange",
    "closed_stop_catchup",
)

# G69: these entry outcomes prove that the execution can no longer become a
# live position.  Once the *whole* trade group has no active executions left,
# any durable ENTRY/TP/STOP market-event for that group is stale work.
_MARKET_EVENT_EAGER_TERMINAL_EXECUTION_STATUSES = (
    "canceled_expired",
    "canceled_tp_progress",
    "canceled_stop_invalidated",
    "canceled_external",
)


async def finish_stale_market_events_if_group_inactive(execution_id: int) -> dict[str, int]:
    """Eagerly terminalize stale durable market-events after a proven entry cancel.

    This is deliberately DB-only cleanup: it performs no BingX reads/writes and
    does not infer fills.  The mutation is guarded by two facts evaluated in the
    database: the referenced execution is in one of the proven terminal entry
    states above, and its trade group has *zero* executions in the active-status
    set.  Processing leases are fenced by clearing their token and incrementing
    ``lease_generation`` so an already-running stale verifier cannot overwrite
    the terminal result later.
    """

    eid = int(execution_id or 0)
    if eid <= 0:
        return {"trade_group_id": 0, "finished": 0}

    active_statuses = list(_GROUP_ACTIVE_EXECUTION_STATUSES)
    terminal_statuses = list(_MARKET_EVENT_EAGER_TERMINAL_EXECUTION_STATUSES)

    if is_postgres():
        async with connect() as c:
            row = await c.fetchrow(
                """SELECT id,trade_group_id,status FROM trade_executions WHERE id=$1""",
                eid,
            )
            current = _dict(row) if row else None
            group_id = int((current or {}).get("trade_group_id") or 0)
            if (
                group_id <= 0
                or str((current or {}).get("status") or "") not in terminal_statuses
            ):
                return {"trade_group_id": group_id, "finished": 0}

            tag = await c.execute(
                """
                UPDATE market_events me
                SET status='done',armed=0,last_error='',
                    outcome_kind='terminal_no_active_execution_eager',
                    lease_token=NULL,lease_expires_at=NULL,
                    lease_generation=COALESCE(lease_generation,0)+1,
                    retrigger_requested=0,retrigger_observed_price=NULL,
                    escalated_at=NULL,stuck_started_at=NULL,
                    last_stuck_alert_at=NULL,last_stuck_reminder_at=NULL,
                    stuck_reason=NULL,coalesced_event_keys=NULL,updated_at=NOW()
                WHERE me.trade_group_id=$1
                  AND me.status IN ('pending','processing')
                  AND NOT EXISTS (
                      SELECT 1 FROM trade_executions te
                      WHERE te.trade_group_id=$1
                        AND te.status = ANY($2::text[])
                  )
                """,
                group_id,
                active_statuses,
            )
            try:
                finished = int(str(tag).rsplit(" ", 1)[-1])
            except (TypeError, ValueError):
                finished = 0
            return {"trade_group_id": group_id, "finished": finished}

    async with connect() as c:
        cur = await c.execute(
            "SELECT id,trade_group_id,status FROM trade_executions WHERE id=?",
            (eid,),
        )
        raw = await cur.fetchone()
        current = _dict(raw) if raw else None
        group_id = int((current or {}).get("trade_group_id") or 0)
        if (
            group_id <= 0
            or str((current or {}).get("status") or "") not in terminal_statuses
        ):
            return {"trade_group_id": group_id, "finished": 0}

        placeholders = ",".join(["?"] * len(active_statuses))
        cur = await c.execute(
            f"""
            UPDATE market_events
            SET status='done',armed=0,last_error='',
                outcome_kind='terminal_no_active_execution_eager',
                lease_token=NULL,lease_expires_at=NULL,
                lease_generation=COALESCE(lease_generation,0)+1,
                retrigger_requested=0,retrigger_observed_price=NULL,
                escalated_at=NULL,stuck_started_at=NULL,
                last_stuck_alert_at=NULL,last_stuck_reminder_at=NULL,
                stuck_reason=NULL,coalesced_event_keys=NULL,
                updated_at=CURRENT_TIMESTAMP
            WHERE trade_group_id=?
              AND status IN ('pending','processing')
              AND NOT EXISTS (
                  SELECT 1 FROM trade_executions te
                  WHERE te.trade_group_id=?
                    AND te.status IN ({placeholders})
              )
            """,
            (group_id, group_id, *active_statuses),
        )
        finished = max(0, int(getattr(cur, "rowcount", 0) or 0))
        await c.commit()
        return {"trade_group_id": group_id, "finished": finished}


async def create_trade_group(
    *,
    signal_hash: str,
    symbol: str,
    side: str,
    entry_type: str,
    planned_entry: float,
    stop_price: float,
    targets_json: str,
    source_chat_id: int | None = None,
    source_message_id: int | None = None,
) -> int:
    """Create one durable common plan for all user executions of a signal."""
    if is_postgres():
        async with connect() as c:
            value = await c.fetchval(
                """
                INSERT INTO trade_groups(
                    signal_hash, symbol, side, entry_type, planned_entry,
                    stop_price, targets_json, source_chat_id, source_message_id, status
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'building')
                RETURNING id
                """,
                str(signal_hash),
                str(symbol).upper(),
                str(side).lower(),
                str(entry_type).upper(),
                float(planned_entry or 0.0),
                float(stop_price),
                str(targets_json),
                source_chat_id,
                source_message_id,
            )
            return int(value)
    async with connect() as c:
        cur = await c.execute(
            """
            INSERT INTO trade_groups(
                signal_hash, symbol, side, entry_type, planned_entry,
                stop_price, targets_json, source_chat_id, source_message_id, status
            ) VALUES (?,?,?,?,?,?,?,?,?,'building')
            """,
            (
                str(signal_hash),
                str(symbol).upper(),
                str(side).lower(),
                str(entry_type).upper(),
                float(planned_entry or 0.0),
                float(stop_price),
                str(targets_json),
                source_chat_id,
                source_message_id,
            ),
        )
        await c.commit()
        return int(cur.lastrowid)


async def finalize_trade_group(trade_group_id: int) -> str:
    """Publish a fully-dispatched group or close it when nobody is trackable.

    A group is created as ``building`` before parallel account execution starts.
    Keeping it invisible to the public-price event loop prevents TP/STOP events
    from being consumed before slower user executions are linked to the plan.
    """
    gid = int(trade_group_id)
    statuses = list(_GROUP_ACTIVE_EXECUTION_STATUSES)
    if is_postgres():
        async with connect() as c:
            row = await c.fetchrow(
                """
                UPDATE trade_groups g
                SET status = CASE WHEN EXISTS (
                        SELECT 1 FROM trade_executions e
                        WHERE e.trade_group_id=g.id AND e.status = ANY($1::text[])
                    ) THEN 'active' ELSE 'closed' END,
                    updated_at=NOW()
                WHERE g.id=$2
                RETURNING status
                """,
                statuses,
                gid,
            )
            return str(row["status"] if row else "")
    placeholders = ",".join(["?"] * len(statuses))
    async with connect() as c:
        cur = await c.execute(
            f"""
            UPDATE trade_groups
            SET status = CASE WHEN EXISTS (
                    SELECT 1 FROM trade_executions e
                    WHERE e.trade_group_id=trade_groups.id
                      AND e.status IN ({placeholders})
                ) THEN 'active' ELSE 'closed' END,
                updated_at=CURRENT_TIMESTAMP
            WHERE id=?
            """,
            (*statuses, gid),
        )
        await c.commit()
        if int(cur.rowcount or 0) <= 0:
            return ""
        cur = await c.execute("SELECT status FROM trade_groups WHERE id=?", (gid,))
        row = await cur.fetchone()
        return str(row[0] if row else "")


async def recover_stale_building_trade_groups(stale_after_sec: int = 120) -> int:
    """Recover groups left in ``building`` by a crash during fan-out.

    Active linked executions are published; empty groups are closed.  The slow
    age gate ensures a normal large fan-out is never published prematurely.
    """
    age = max(30, int(stale_after_sec or 120))
    statuses = list(_GROUP_ACTIVE_EXECUTION_STATUSES)
    if is_postgres():
        async with connect() as c:
            result = await c.execute(
                """
                UPDATE trade_groups g
                SET status = CASE WHEN EXISTS (
                        SELECT 1 FROM trade_executions e
                        WHERE e.trade_group_id=g.id AND e.status = ANY($1::text[])
                    ) THEN 'active' ELSE 'closed' END,
                    updated_at=NOW()
                WHERE g.status='building'
                  AND g.updated_at < NOW() - ($2::int * INTERVAL '1 second')
                """,
                statuses,
                age,
            )
            try:
                return int(str(result).split()[-1])
            except Exception:
                return 0
    placeholders = ",".join(["?"] * len(statuses))
    async with connect() as c:
        modifier = f"-{age} seconds"
        cur = await c.execute(
            f"""
            UPDATE trade_groups
            SET status = CASE WHEN EXISTS (
                    SELECT 1 FROM trade_executions e
                    WHERE e.trade_group_id=trade_groups.id
                      AND e.status IN ({placeholders})
                ) THEN 'active' ELSE 'closed' END,
                updated_at=CURRENT_TIMESTAMP
            WHERE status='building'
              AND datetime(updated_at) < datetime('now', ?)
            """,
            (*statuses, modifier),
        )
        await c.commit()
        return max(0, int(cur.rowcount or 0))


async def active_trade_groups(limit: int = 100) -> List[Dict[str, Any]]:
    """Return active plans that still have at least one trackable execution."""
    lim = max(1, min(int(limit or 100), 1000))
    statuses = list(_GROUP_ACTIVE_EXECUTION_STATUSES)
    if is_postgres():
        async with connect() as c:
            rows = await c.fetch(
                """
                SELECT g.*, COUNT(e.id)::int AS active_execution_count
                FROM trade_groups g
                JOIN trade_executions e ON e.trade_group_id=g.id
                WHERE g.status='active' AND e.status = ANY($1::text[])
                GROUP BY g.id
                ORDER BY g.created_at ASC
                LIMIT $2
                """,
                statuses,
                lim,
            )
            return [_dict(r) for r in rows]
    placeholders = ",".join(["?"] * len(statuses))
    async with connect() as c:
        cur = await c.execute(
            f"""
            SELECT g.*, COUNT(e.id) AS active_execution_count
            FROM trade_groups g
            JOIN trade_executions e ON e.trade_group_id=g.id
            WHERE g.status='active' AND e.status IN ({placeholders})
            GROUP BY g.id
            ORDER BY g.created_at ASC
            LIMIT ?
            """,
            (*statuses, lim),
        )
        return [_dict(r) for r in await cur.fetchall()]


async def trade_group_executions(
    trade_group_id: int,
    *,
    active_only: bool = True,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    """Return BingX executions linked to one common plan.

    Apply the caller's limit after explicit-exchange filtering so malformed or
    legacy rows cannot hide valid group executions.
    """
    wanted = max(1, min(int(limit or 500), 5000))
    page_size = max(200, min(2000, wanted * 2))
    statuses = list(_GROUP_ACTIVE_EXECUTION_STATUSES)
    result: list[Dict[str, Any]] = []
    offset = 0

    if is_postgres():
        async with connect() as c:
            while len(result) < wanted:
                if active_only:
                    rows = await c.fetch(
                        """SELECT * FROM trade_executions
                        WHERE trade_group_id=$1 AND status = ANY($2::text[])
                        ORDER BY user_id,id LIMIT $3 OFFSET $4""",
                        int(trade_group_id),
                        statuses,
                        page_size,
                        offset,
                    )
                else:
                    rows = await c.fetch(
                        """SELECT * FROM trade_executions
                        WHERE trade_group_id=$1
                        ORDER BY user_id,id LIMIT $2 OFFSET $3""",
                        int(trade_group_id),
                        page_size,
                        offset,
                    )
                raw = [_dict(row) for row in rows]
                if not raw:
                    break
                result.extend(_bingx_rows(raw))
                offset += len(raw)
                if len(raw) < page_size:
                    break
        return result[:wanted]

    async with connect() as c:
        while len(result) < wanted:
            if active_only:
                placeholders = ",".join(["?"] * len(statuses))
                cur = await c.execute(
                    f"""SELECT * FROM trade_executions
                    WHERE trade_group_id=? AND status IN ({placeholders})
                    ORDER BY user_id,id LIMIT ? OFFSET ?""",
                    (int(trade_group_id), *statuses, page_size, offset),
                )
            else:
                cur = await c.execute(
                    """SELECT * FROM trade_executions
                    WHERE trade_group_id=?
                    ORDER BY user_id,id LIMIT ? OFFSET ?""",
                    (int(trade_group_id), page_size, offset),
                )
            raw = [_dict(row) for row in await cur.fetchall()]
            if not raw:
                break
            result.extend(_bingx_rows(raw))
            offset += len(raw)
            if len(raw) < page_size:
                break
    return result[:wanted]


def _normalize_trade_group_price_updates(
    updates: List[tuple[int, float]],
) -> List[tuple[int, float]]:
    """Return finite positive prices, de-duplicated by group id.

    The public-price loop owns one current value per active trade group.  A
    defensive last-value-wins normalization keeps the PostgreSQL UNNEST arrays
    unique and prevents malformed values from poisoning the whole batch.
    """
    normalized: Dict[int, float] = {}
    for raw_group_id, raw_price in updates:
        try:
            group_id = int(raw_group_id)
            price = float(raw_price)
        except (TypeError, ValueError, OverflowError):
            continue
        if group_id <= 0 or not math.isfinite(price) or price <= 0:
            continue
        normalized[group_id] = price
    return list(normalized.items())


async def update_trade_group_prices_batch(
    updates: List[tuple[int, float]],
) -> int:
    """Persist many public prices in one DB scope/transaction.

    PostgreSQL uses one UPDATE ... FROM UNNEST statement, so the previous
    per-group pool acquire and round-trip are replaced by a single statement.
    SQLite keeps test/local compatibility with one executemany transaction.
    No exchange state or trading decision depends on the returned count.
    """
    rows = _normalize_trade_group_price_updates(updates)
    if not rows:
        return 0

    if is_postgres():
        group_ids = [group_id for group_id, _price in rows]
        prices = [price for _group_id, price in rows]
        async with connect() as c:
            await c.execute(
                """
                UPDATE trade_groups AS g
                SET last_price = batch.price,
                    last_price_at = NOW(),
                    updated_at = NOW()
                FROM UNNEST($1::bigint[], $2::double precision[])
                     AS batch(id, price)
                WHERE g.id = batch.id
                """,
                group_ids,
                prices,
            )
        return len(rows)

    async with connect() as c:
        await c.executemany(
            """
            UPDATE trade_groups
            SET last_price=?,
                last_price_at=CURRENT_TIMESTAMP,
                updated_at=CURRENT_TIMESTAMP
            WHERE id=?
            """,
            [(price, group_id) for group_id, price in rows],
        )
        await c.commit()
    return len(rows)


async def update_trade_group_price(trade_group_id: int, price: float) -> None:
    """Backward-compatible one-row wrapper around the batch primitive."""
    await update_trade_group_prices_batch([(trade_group_id, price)])


def _normalize_market_event_batch(
    events: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Validate and de-duplicate one public-tick MARKET EVENT batch.

    Last value wins per durable ``(trade_group_id,event_key)`` identity. The
    final list is sorted by durable safety priority, preserving STOP before TP
    and ENTRY inside the single PostgreSQL statement.
    """

    normalized: Dict[tuple[int, str], Dict[str, Any]] = {}
    for raw in events or []:
        if not isinstance(raw, dict):
            continue
        try:
            group_id = int(raw.get("trade_group_id") or 0)
            event_key = str(raw.get("event_key") or "").strip()
            event_type = str(raw.get("event_type") or "").strip().upper()
            level_index = int(raw.get("level_index") or 0)
            trigger_price = float(raw.get("trigger_price") or 0.0)
            observed_price = float(raw.get("observed_price") or 0.0)
        except (TypeError, ValueError, OverflowError):
            continue
        if (
            group_id <= 0
            or not event_key
            or event_type not in {"STOP", "TP", "ENTRY"}
            or not math.isfinite(trigger_price)
            or not math.isfinite(observed_price)
            or trigger_price <= 0
            or observed_price <= 0
        ):
            continue
        normalized[(group_id, event_key)] = {
            "trade_group_id": group_id,
            "event_key": event_key,
            "event_type": event_type,
            "event_priority": market_event_priority(event_type),
            "level_index": max(0, level_index),
            "trigger_price": trigger_price,
            "observed_price": observed_price,
        }
    return sorted(
        normalized.values(),
        key=lambda row: (
            int(row["event_priority"]),
            int(row["trade_group_id"]),
            str(row["event_key"]),
        ),
    )


async def enqueue_market_events_batch(
    events: List[Dict[str, Any]],
) -> Dict[tuple[int, str], bool]:
    """Consume many durable event arms in one DB scope.

    The SQL is the exact set-wise equivalent of :func:`enqueue_market_event`.
    A per-key boolean reports whether that row was inserted/rearmed by this
    call. Existing or blocked durable rows return ``False`` but remain safe for
    process-local gate suppression.
    """

    rows = _normalize_market_event_batch(events)
    if not rows:
        return {}
    if is_postgres():
        group_ids = [int(row["trade_group_id"]) for row in rows]
        event_keys = [str(row["event_key"]) for row in rows]
        event_types = [str(row["event_type"]) for row in rows]
        priorities = [int(row["event_priority"]) for row in rows]
        level_indexes = [int(row["level_index"]) for row in rows]
        trigger_prices = [float(row["trigger_price"]) for row in rows]
        observed_prices = [float(row["observed_price"]) for row in rows]
        async with connect() as c:
            result_rows = await c.fetch(
                """
                WITH input AS (
                    SELECT *
                    FROM UNNEST(
                        $1::bigint[], $2::text[], $3::text[], $4::integer[],
                        $5::integer[], $6::double precision[],
                        $7::double precision[]
                    ) WITH ORDINALITY AS i(
                        trade_group_id,event_key,event_type,event_priority,
                        level_index,trigger_price,observed_price,ordinality
                    )
                ), upserted AS (
                    INSERT INTO market_events(
                        trade_group_id,event_key,event_type,event_priority,
                        level_index,trigger_price,observed_price,status,armed,
                        retrigger_requested,retrigger_observed_price,next_attempt_at
                    )
                    SELECT trade_group_id,event_key,event_type,event_priority,
                           level_index,trigger_price,observed_price,
                           'pending',0,0,NULL,NOW()
                    FROM input
                    ORDER BY event_priority, ordinality
                    ON CONFLICT(trade_group_id,event_key) DO UPDATE
                    SET event_type=EXCLUDED.event_type,
                        event_priority=EXCLUDED.event_priority,
                        level_index=EXCLUDED.level_index,
                        trigger_price=EXCLUDED.trigger_price,
                        observed_price=EXCLUDED.observed_price,
                        status=CASE
                            WHEN market_events.status='done' THEN 'pending'
                            ELSE market_events.status
                        END,
                        armed=0,
                        retrigger_requested=CASE
                            WHEN market_events.status IN ('pending','processing') THEN 1
                            ELSE 0
                        END,
                        retrigger_observed_price=CASE
                            WHEN market_events.status IN ('pending','processing')
                                THEN EXCLUDED.observed_price
                            ELSE NULL
                        END,
                        attempts=CASE
                            WHEN market_events.status='done' THEN 0
                            ELSE market_events.attempts
                        END,
                        next_attempt_at=CASE
                            WHEN market_events.status='done' THEN NOW()
                            ELSE market_events.next_attempt_at
                        END,
                        last_error=CASE
                            WHEN market_events.status='done' THEN NULL
                            ELSE market_events.last_error
                        END,
                        watch_lane='critical',
                        escalated_at=NULL,stuck_started_at=NULL,
                        last_stuck_alert_at=NULL,last_stuck_reminder_at=NULL,
                        stuck_reason=NULL,coalesced_event_keys=NULL,
                        updated_at=NOW()
                    WHERE COALESCE(market_events.automation_enabled,1)=1
                      AND COALESCE(market_events.armed,1)=1
                      AND COALESCE(market_events.retrigger_requested,0)=0
                      AND market_events.status IN ('done','pending','processing')
                    RETURNING trade_group_id,event_key
                )
                SELECT i.trade_group_id,i.event_key,
                       EXISTS(
                           SELECT 1 FROM upserted u
                           WHERE u.trade_group_id=i.trade_group_id
                             AND u.event_key=i.event_key
                       ) AS inserted
                FROM input i
                ORDER BY i.event_priority,i.ordinality
                """,
                group_ids,
                event_keys,
                event_types,
                priorities,
                level_indexes,
                trigger_prices,
                observed_prices,
            )
        return {
            (int(row["trade_group_id"]), str(row["event_key"])): bool(
                row["inserted"]
            )
            for row in result_rows
        }

    result: Dict[tuple[int, str], bool] = {}
    async with connect() as c:
        await c.execute("BEGIN IMMEDIATE")
        for row in rows:
            cur = await c.execute(
                """
                INSERT INTO market_events(
                    trade_group_id,event_key,event_type,event_priority,level_index,
                    trigger_price,observed_price,status,armed,
                    retrigger_requested,retrigger_observed_price,next_attempt_at
                ) VALUES (?,?,?,?,?,?,?,'pending',0,0,NULL,CURRENT_TIMESTAMP)
                ON CONFLICT(trade_group_id,event_key) DO UPDATE SET
                    event_type=excluded.event_type,
                    event_priority=excluded.event_priority,
                    level_index=excluded.level_index,
                    trigger_price=excluded.trigger_price,
                    observed_price=excluded.observed_price,
                    status=CASE
                        WHEN market_events.status='done' THEN 'pending'
                        ELSE market_events.status
                    END,
                    armed=0,
                    retrigger_requested=CASE
                        WHEN market_events.status IN ('pending','processing') THEN 1
                        ELSE 0
                    END,
                    retrigger_observed_price=CASE
                        WHEN market_events.status IN ('pending','processing')
                            THEN excluded.observed_price
                        ELSE NULL
                    END,
                    attempts=CASE
                        WHEN market_events.status='done' THEN 0
                        ELSE market_events.attempts
                    END,
                    next_attempt_at=CASE
                        WHEN market_events.status='done' THEN CURRENT_TIMESTAMP
                        ELSE market_events.next_attempt_at
                    END,
                    last_error=CASE
                        WHEN market_events.status='done' THEN NULL
                        ELSE market_events.last_error
                    END,
                    watch_lane='critical',
                    escalated_at=NULL,stuck_started_at=NULL,
                    last_stuck_alert_at=NULL,last_stuck_reminder_at=NULL,
                    stuck_reason=NULL,coalesced_event_keys=NULL,
                    updated_at=CURRENT_TIMESTAMP
                WHERE COALESCE(market_events.automation_enabled,1)=1
                  AND COALESCE(market_events.armed,1)=1
                  AND COALESCE(market_events.retrigger_requested,0)=0
                  AND market_events.status IN ('done','pending','processing')
                """,
                (
                    int(row["trade_group_id"]),
                    str(row["event_key"]),
                    str(row["event_type"]),
                    int(row["event_priority"]),
                    int(row["level_index"]),
                    float(row["trigger_price"]),
                    float(row["observed_price"]),
                ),
            )
            result[(int(row["trade_group_id"]), str(row["event_key"]))] = (
                int(cur.rowcount or 0) > 0
            )
        await c.commit()
    return result


async def has_due_or_live_stop_market_event() -> bool:
    """Return whether protective STOP verification currently owns priority.

    G61 calls this only after an overdue TP preflight candidate was found.  It
    is a second, read-only guard immediately before BE work takes verifier
    capacity, narrowing the race between the candidate peek and STOP arrival.
    """

    if is_postgres():
        async with connect() as c:
            row = await c.fetchrow(
                """
                SELECT 1 AS present
                FROM market_events s
                WHERE COALESCE(s.automation_enabled,1)=1
                  AND COALESCE(s.watch_lane,'critical')='critical'
                  AND UPPER(COALESCE(s.event_type,''))='STOP'
                  AND (
                    (s.status='pending' AND s.next_attempt_at <= NOW())
                    OR (
                        s.status='processing'
                        AND COALESCE(
                            s.lease_expires_at,
                            s.updated_at + INTERVAL '2 minutes'
                        ) >= NOW()
                    )
                  )
                LIMIT 1
                """
            )
            return row is not None

    async with connect() as c:
        cur = await c.execute(
            """
            SELECT 1 AS present
            FROM market_events s
            WHERE COALESCE(s.automation_enabled,1)=1
              AND COALESCE(s.watch_lane,'critical')='critical'
              AND UPPER(COALESCE(s.event_type,''))='STOP'
              AND (
                (s.status='pending' AND datetime(s.next_attempt_at) <= datetime('now'))
                OR (
                    s.status='processing'
                    AND datetime(
                        COALESCE(
                            s.lease_expires_at,
                            datetime(s.updated_at,'+2 minutes')
                        )
                    ) >= datetime('now')
                )
              )
            LIMIT 1
            """
        )
        return (await cur.fetchone()) is not None


async def peek_due_tp_market_events_for_be_preflight(
    limit: int = 2, *, min_due_lag_sec: float = 15.0
) -> List[Dict[str, Any]]:
    """Return overdue critical TP events for the protective BE preflight lane.

    The read is intentionally non-owning: the ordinary MARKET EVENT verifier
    remains the sole owner of leases, attempts and terminal outcomes.  No TP is
    returned while a due or live STOP event exists, so the preflight cannot
    consume safety capacity ahead of STOP handling.  At most one TP per trade
    group is returned, preferring the highest reached TP because it can satisfy
    any configured BE trigger at or below that level.
    """

    lim = max(1, min(int(limit or 2), 8))
    lag = max(0.5, min(float(min_due_lag_sec or 15.0), 60.0))
    if is_postgres():
        async with connect() as c:
            rows = await c.fetch(
                """
                WITH stop_guard AS (
                    SELECT 1
                    FROM market_events s
                    WHERE COALESCE(s.automation_enabled,1)=1
                      AND COALESCE(s.watch_lane,'critical')='critical'
                      AND UPPER(COALESCE(s.event_type,''))='STOP'
                      AND (
                        (s.status='pending' AND s.next_attempt_at <= NOW())
                        OR (
                            s.status='processing'
                            AND COALESCE(
                                s.lease_expires_at,
                                s.updated_at + INTERVAL '2 minutes'
                            ) >= NOW()
                        )
                      )
                    LIMIT 1
                ), per_group AS (
                    SELECT DISTINCT ON (m.trade_group_id) m.*
                    FROM market_events m
                    WHERE NOT EXISTS (SELECT 1 FROM stop_guard)
                      AND COALESCE(m.automation_enabled,1)=1
                      AND COALESCE(m.watch_lane,'critical')='critical'
                      AND UPPER(COALESCE(m.event_type,''))='TP'
                      AND m.status='pending'
                      AND m.next_attempt_at <=
                          NOW()-($2::double precision*INTERVAL '1 second')
                      AND NOT EXISTS (
                          SELECT 1
                          FROM market_events busy
                          WHERE busy.trade_group_id=m.trade_group_id
                            AND busy.status='processing'
                            AND COALESCE(
                                busy.lease_expires_at,
                                busy.updated_at + INTERVAL '2 minutes'
                            ) >= NOW()
                      )
                    ORDER BY m.trade_group_id,
                             COALESCE(m.level_index,0) DESC,
                             m.next_attempt_at,m.id
                )
                SELECT per_group.*,
                       EXTRACT(EPOCH FROM (NOW()-per_group.next_attempt_at))
                           AS due_lag_sec
                FROM per_group
                ORDER BY per_group.next_attempt_at,per_group.id
                LIMIT $1
                """,
                lim,
                lag,
            )
            return [_dict(row) for row in rows]

    async with connect() as c:
        cur = await c.execute(
            """
            WITH stop_guard AS (
                SELECT 1
                FROM market_events s
                WHERE COALESCE(s.automation_enabled,1)=1
                  AND COALESCE(s.watch_lane,'critical')='critical'
                  AND UPPER(COALESCE(s.event_type,''))='STOP'
                  AND (
                    (s.status='pending' AND datetime(s.next_attempt_at) <= datetime('now'))
                    OR (
                        s.status='processing'
                        AND datetime(
                            COALESCE(
                                s.lease_expires_at,
                                datetime(s.updated_at,'+2 minutes')
                            )
                        ) >= datetime('now')
                    )
                  )
                LIMIT 1
            ), ranked AS (
                SELECT m.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY m.trade_group_id
                           ORDER BY COALESCE(m.level_index,0) DESC,
                                    datetime(m.next_attempt_at),m.id
                       ) AS rn
                FROM market_events m
                WHERE NOT EXISTS (SELECT 1 FROM stop_guard)
                  AND COALESCE(m.automation_enabled,1)=1
                  AND COALESCE(m.watch_lane,'critical')='critical'
                  AND UPPER(COALESCE(m.event_type,''))='TP'
                  AND m.status='pending'
                  AND datetime(m.next_attempt_at) <= datetime('now', ?)
                  AND NOT EXISTS (
                      SELECT 1 FROM market_events busy
                      WHERE busy.trade_group_id=m.trade_group_id
                        AND busy.status='processing'
                        AND datetime(
                            COALESCE(
                                busy.lease_expires_at,
                                datetime(busy.updated_at,'+2 minutes')
                            )
                        ) >= datetime('now')
                  )
            )
            SELECT ranked.*,
                   (julianday('now')-julianday(ranked.next_attempt_at))*86400.0
                       AS due_lag_sec
            FROM ranked
            WHERE rn=1
            ORDER BY datetime(next_attempt_at),id
            LIMIT ?
            """,
            (f"-{lag:.3f} seconds", lim),
        )
        return [_dict(row) for row in await cur.fetchall()]


async def enqueue_market_event(
    *,
    trade_group_id: int,
    event_key: str,
    event_type: str,
    level_index: int,
    trigger_price: float,
    observed_price: float,
) -> bool:
    """Consume one durable event arm and enqueue exactly one verification.

    State mapping:
      * armed=1,status=done -> ARMED
      * armed=0,status in pending/processing -> PENDING
      * armed=0,status=done -> COOLDOWN

    A completed event cannot be queued again until rearm_market_event() has
    restored its arm.  This is the durable half of the price hysteresis gate.
    """
    if is_postgres():
        async with connect() as c:
            row = await c.fetchrow(
                """
                INSERT INTO market_events(
                    trade_group_id,event_key,event_type,event_priority,level_index,
                    trigger_price,observed_price,status,armed,
                    retrigger_requested,retrigger_observed_price,next_attempt_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,'pending',0,0,NULL,NOW())
                ON CONFLICT(trade_group_id,event_key) DO UPDATE
                SET event_type=EXCLUDED.event_type,
                    event_priority=EXCLUDED.event_priority,
                    level_index=EXCLUDED.level_index,
                    trigger_price=EXCLUDED.trigger_price,
                    observed_price=EXCLUDED.observed_price,
                    status=CASE
                        WHEN market_events.status='done' THEN 'pending'
                        ELSE market_events.status
                    END,
                    armed=0,
                    retrigger_requested=CASE
                        WHEN market_events.status IN ('pending','processing') THEN 1
                        ELSE 0
                    END,
                    retrigger_observed_price=CASE
                        WHEN market_events.status IN ('pending','processing')
                            THEN EXCLUDED.observed_price
                        ELSE NULL
                    END,
                    attempts=CASE
                        WHEN market_events.status='done' THEN 0
                        ELSE market_events.attempts
                    END,
                    next_attempt_at=CASE
                        WHEN market_events.status='done' THEN NOW()
                        ELSE market_events.next_attempt_at
                    END,
                    last_error=CASE
                        WHEN market_events.status='done' THEN NULL
                        ELSE market_events.last_error
                    END,
                    watch_lane='critical',
                    escalated_at=NULL,stuck_started_at=NULL,
                    last_stuck_alert_at=NULL,last_stuck_reminder_at=NULL,
                    stuck_reason=NULL,coalesced_event_keys=NULL,
                    updated_at=NOW()
                WHERE COALESCE(market_events.automation_enabled,1)=1
                  AND COALESCE(market_events.armed,1)=1
                  AND COALESCE(market_events.retrigger_requested,0)=0
                  AND market_events.status IN ('done','pending','processing')
                RETURNING id
                """,
                int(trade_group_id),
                str(event_key),
                str(event_type),
                market_event_priority(event_type),
                int(level_index or 0),
                float(trigger_price),
                float(observed_price),
            )
            return bool(row)
    async with connect() as c:
        cur = await c.execute(
            """
            INSERT INTO market_events(
                trade_group_id,event_key,event_type,event_priority,level_index,
                trigger_price,observed_price,status,armed,
                retrigger_requested,retrigger_observed_price,next_attempt_at
            ) VALUES (?,?,?,?,?,?,?,'pending',0,0,NULL,CURRENT_TIMESTAMP)
            ON CONFLICT(trade_group_id,event_key) DO UPDATE SET
                event_type=excluded.event_type,
                event_priority=excluded.event_priority,
                level_index=excluded.level_index,
                trigger_price=excluded.trigger_price,
                observed_price=excluded.observed_price,
                status=CASE
                    WHEN market_events.status='done' THEN 'pending'
                    ELSE market_events.status
                END,
                armed=0,
                retrigger_requested=CASE
                    WHEN market_events.status IN ('pending','processing') THEN 1
                    ELSE 0
                END,
                retrigger_observed_price=CASE
                    WHEN market_events.status IN ('pending','processing')
                        THEN excluded.observed_price
                    ELSE NULL
                END,
                attempts=CASE
                    WHEN market_events.status='done' THEN 0
                    ELSE market_events.attempts
                END,
                next_attempt_at=CASE
                    WHEN market_events.status='done' THEN CURRENT_TIMESTAMP
                    ELSE market_events.next_attempt_at
                END,
                last_error=CASE
                    WHEN market_events.status='done' THEN NULL
                    ELSE market_events.last_error
                END,
                watch_lane='critical',
                escalated_at=NULL,stuck_started_at=NULL,
                last_stuck_alert_at=NULL,last_stuck_reminder_at=NULL,
                stuck_reason=NULL,coalesced_event_keys=NULL,
                updated_at=CURRENT_TIMESTAMP
            WHERE COALESCE(market_events.automation_enabled,1)=1
              AND COALESCE(market_events.armed,1)=1
              AND COALESCE(market_events.retrigger_requested,0)=0
              AND market_events.status IN ('done','pending','processing')
            """,
            (
                int(trade_group_id),
                str(event_key),
                str(event_type),
                market_event_priority(event_type),
                int(level_index or 0),
                float(trigger_price),
                float(observed_price),
            ),
        )
        await c.commit()
        return int(cur.rowcount or 0) > 0


def _normalize_market_event_rearm_batch(
    items: List[tuple[int, str]],
) -> List[tuple[int, str]]:
    normalized: Dict[tuple[int, str], None] = {}
    for raw_group_id, raw_event_key in items or []:
        try:
            group_id = int(raw_group_id)
        except (TypeError, ValueError, OverflowError):
            continue
        event_key = str(raw_event_key or "").strip()
        if group_id > 0 and event_key:
            normalized[(group_id, event_key)] = None
    return list(normalized)


async def rearm_market_event_states_batch(
    items: List[tuple[int, str]],
) -> Dict[tuple[int, str], str]:
    """Restore many durable event gates in one DB scope with exact states."""

    rows = _normalize_market_event_rearm_batch(items)
    if not rows:
        return {}
    if is_postgres():
        group_ids = [row[0] for row in rows]
        event_keys = [row[1] for row in rows]
        async with connect() as c:
            result_rows = await c.fetch(
                """
                WITH input AS (
                    SELECT *
                    FROM UNNEST($1::bigint[], $2::text[])
                         WITH ORDINALITY AS i(
                             trade_group_id,event_key,ordinality
                         )
                ), updated AS (
                    UPDATE market_events AS m
                    SET armed=1,
                        rearm_count=COALESCE(m.rearm_count,0)+1,
                        last_rearmed_at=NOW(),
                        updated_at=NOW()
                    FROM input i
                    WHERE m.trade_group_id=i.trade_group_id
                      AND m.event_key=i.event_key
                      AND COALESCE(m.automation_enabled,1)=1
                      AND COALESCE(m.armed,0)<>1
                      AND COALESCE(m.retrigger_requested,0)=0
                    RETURNING m.trade_group_id,m.event_key
                )
                SELECT i.trade_group_id,i.event_key,
                       CASE
                           WHEN EXISTS(
                               SELECT 1 FROM updated u
                               WHERE u.trade_group_id=i.trade_group_id
                                 AND u.event_key=i.event_key
                           ) THEN 'rearmed'
                           WHEN NOT EXISTS(
                               SELECT 1 FROM market_events m
                               WHERE m.trade_group_id=i.trade_group_id
                                 AND m.event_key=i.event_key
                           ) THEN 'missing'
                           WHEN EXISTS(
                               SELECT 1 FROM market_events m
                               WHERE m.trade_group_id=i.trade_group_id
                                 AND m.event_key=i.event_key
                                 AND COALESCE(m.retrigger_requested,0)=1
                           ) THEN 'deferred_exists'
                           WHEN EXISTS(
                               SELECT 1 FROM market_events m
                               WHERE m.trade_group_id=i.trade_group_id
                                 AND m.event_key=i.event_key
                                 AND COALESCE(m.automation_enabled,1)=0
                           ) THEN 'automation_disabled'
                           WHEN EXISTS(
                               SELECT 1 FROM market_events m
                               WHERE m.trade_group_id=i.trade_group_id
                                 AND m.event_key=i.event_key
                                 AND COALESCE(m.armed,0)=1
                           ) THEN 'already_armed'
                           ELSE 'blocked'
                       END AS state
                FROM input i
                ORDER BY i.ordinality
                """,
                group_ids,
                event_keys,
            )
        return {
            (int(row["trade_group_id"]), str(row["event_key"])): str(
                row["state"] or "blocked"
            )
            for row in result_rows
        }

    result: Dict[tuple[int, str], str] = {}
    async with connect() as c:
        await c.execute("BEGIN IMMEDIATE")
        for group_id, event_key in rows:
            cur = await c.execute(
                """
                SELECT armed,retrigger_requested,automation_enabled
                FROM market_events
                WHERE trade_group_id=? AND event_key=?
                """,
                (group_id, event_key),
            )
            row = await cur.fetchone()
            if row is None:
                result[(group_id, event_key)] = "missing"
                continue
            armed = int(row[0] or 0)
            retrigger_requested = int(row[1] or 0)
            automation_enabled = int(row[2] if row[2] is not None else 1)
            if automation_enabled == 0:
                state = "automation_disabled"
            elif retrigger_requested == 1:
                state = "deferred_exists"
            elif armed == 1:
                state = "already_armed"
            else:
                update_cur = await c.execute(
                    """
                    UPDATE market_events
                    SET armed=1,
                        rearm_count=COALESCE(rearm_count,0)+1,
                        last_rearmed_at=CURRENT_TIMESTAMP,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE trade_group_id=? AND event_key=?
                      AND COALESCE(automation_enabled,1)=1
                      AND COALESCE(armed,0)<>1
                      AND COALESCE(retrigger_requested,0)=0
                    """,
                    (group_id, event_key),
                )
                state = "rearmed" if int(update_cur.rowcount or 0) > 0 else "blocked"
            result[(group_id, event_key)] = state
        await c.commit()
    return result


async def rearm_market_event_state(*, trade_group_id: int, event_key: str) -> str:
    """Atomically restore one event arm and explain why it was not restored.

    Returned states:
      * ``rearmed`` - the durable arm changed from 0 to 1;
      * ``already_armed`` - a previous/ambiguous call already restored it;
      * ``deferred_exists`` - one follow-up crossing is already recorded;
      * ``automation_disabled`` - terminal manual review blocks rearming;
      * ``missing`` - no durable row exists for this key;
      * ``blocked`` - an unexpected state was left unchanged.

    ``deferred_exists`` is the important v1.0.7g7h1 guard: while one verifier
    is pending/processing, at most one later genuine re-cross may be retained.
    """
    group_id = int(trade_group_id)
    key = str(event_key)
    if is_postgres():
        async with connect() as c:
            row = await c.fetchrow(
                """
                WITH updated AS (
                    UPDATE market_events
                    SET armed=1,
                        rearm_count=COALESCE(rearm_count,0)+1,
                        last_rearmed_at=NOW(),
                        updated_at=NOW()
                    WHERE trade_group_id=$1 AND event_key=$2
                      AND COALESCE(automation_enabled,1)=1
                      AND COALESCE(armed,0)<>1
                      AND COALESCE(retrigger_requested,0)=0
                    RETURNING id
                )
                SELECT CASE
                    WHEN EXISTS(SELECT 1 FROM updated) THEN 'rearmed'
                    WHEN NOT EXISTS(
                        SELECT 1 FROM market_events
                        WHERE trade_group_id=$1 AND event_key=$2
                    ) THEN 'missing'
                    WHEN EXISTS(
                        SELECT 1 FROM market_events
                        WHERE trade_group_id=$1 AND event_key=$2
                          AND COALESCE(retrigger_requested,0)=1
                    ) THEN 'deferred_exists'
                    WHEN EXISTS(
                        SELECT 1 FROM market_events
                        WHERE trade_group_id=$1 AND event_key=$2
                          AND COALESCE(automation_enabled,1)=0
                    ) THEN 'automation_disabled'
                    WHEN EXISTS(
                        SELECT 1 FROM market_events
                        WHERE trade_group_id=$1 AND event_key=$2
                          AND COALESCE(armed,0)=1
                    ) THEN 'already_armed'
                    ELSE 'blocked'
                END AS state
                """,
                group_id,
                key,
            )
            return str((row or {}).get("state") or "blocked")

    async with connect() as c:
        await c.execute("BEGIN IMMEDIATE")
        cur = await c.execute(
            """
            SELECT armed,retrigger_requested,automation_enabled
            FROM market_events
            WHERE trade_group_id=? AND event_key=?
            """,
            (group_id, key),
        )
        row = await cur.fetchone()
        if row is None:
            await c.commit()
            return "missing"
        armed = int(row[0] or 0)
        retrigger_requested = int(row[1] or 0)
        automation_enabled = int(row[2] if row[2] is not None else 1)
        if automation_enabled == 0:
            await c.commit()
            return "automation_disabled"
        if retrigger_requested == 1:
            await c.commit()
            return "deferred_exists"
        if armed == 1:
            await c.commit()
            return "already_armed"
        cur = await c.execute(
            """
            UPDATE market_events
            SET armed=1,
                rearm_count=COALESCE(rearm_count,0)+1,
                last_rearmed_at=CURRENT_TIMESTAMP,
                updated_at=CURRENT_TIMESTAMP
            WHERE trade_group_id=? AND event_key=?
              AND COALESCE(automation_enabled,1)=1
              AND COALESCE(armed,0)<>1
              AND COALESCE(retrigger_requested,0)=0
            """,
            (group_id, key),
        )
        await c.commit()
        return "rearmed" if int(cur.rowcount or 0) > 0 else "blocked"


async def rearm_market_event(*, trade_group_id: int, event_key: str) -> bool:
    """Backward-compatible boolean wrapper for direct callers/tests."""
    return (
        await rearm_market_event_state(
            trade_group_id=trade_group_id,
            event_key=event_key,
        )
        == "rearmed"
    )


async def claim_due_market_events(
    limit: int = 20, *, watch_lane: str = "critical"
) -> List[Dict[str, Any]]:
    """Atomically claim one lane of due events with fenced leases.

    The latency-sensitive lane contains fresh STOP/TP/ENTRY work.  Long-lived
    administrative watches are claimed by a separate low-capacity worker and
    therefore cannot consume the verifier capacity reserved for fresh safety
    events.  Only one event per trade group is claimed in a batch.
    """

    lim = max(1, min(int(limit or 20), 200))
    lane = str(watch_lane or "critical").strip().lower()
    if lane not in {"critical", "admin"}:
        raise ValueError("watch_lane must be 'critical' or 'admin'")
    claim_token = uuid.uuid4().hex
    lease_sec = float(MARKET_EVENT_LEASE_SECONDS)
    cleanup_orphans = _reserve_market_event_orphan_cleanup(lane)
    if is_postgres():
        async with connect() as c:
            async with c.transaction():
                if cleanup_orphans:
                    orphan_tag = await c.execute(
                        """UPDATE market_events me
                           SET status='done',armed=0,retrigger_requested=0,
                               retrigger_observed_price=NULL,lease_token=NULL,
                               lease_expires_at=NULL,
                               lease_generation=COALESCE(lease_generation,0)+1,
                               outcome_kind='orphan_trade_group',
                               last_error='trade_group_missing',
                               stuck_reason='trade_group_missing',updated_at=NOW()
                           WHERE NOT EXISTS (
                               SELECT 1 FROM trade_groups tg
                               WHERE tg.id=me.trade_group_id
                           )
                             AND me.created_at < NOW()-INTERVAL '10 minutes'
                             AND NOT EXISTS (
                               SELECT 1 FROM market_events active
                               WHERE active.trade_group_id=me.trade_group_id
                                 AND active.status='processing'
                                 AND COALESCE(
                                     active.lease_expires_at,
                                     active.updated_at + INTERVAL '2 minutes'
                                 ) >= NOW()
                           )
                             AND (
                               me.status='pending'
                               OR (
                                   me.status='processing'
                                   AND COALESCE(
                                       me.lease_expires_at,
                                       me.updated_at + INTERVAL '2 minutes'
                                   ) < NOW()
                               )
                             )"""
                    )
                    if not str(orphan_tag).endswith(" 0"):
                        log.error(
                            "MARKET_EVENT_ORPHANS_ARCHIVED backend=postgres result=%s",
                            orphan_tag,
                        )
                rows = await c.fetch(
                    """
                WITH per_group AS (
                    SELECT DISTINCT ON (trade_group_id)
                           id,trade_group_id,event_priority,event_type,level_index,
                           next_attempt_at,escalated_at,outcome_kind
                    FROM market_events
                    WHERE COALESCE(automation_enabled,1)=1
                      AND COALESCE(watch_lane,'critical')=$4
                      AND (
                        status='pending'
                        OR (
                            status='processing'
                            AND COALESCE(
                                lease_expires_at,
                                updated_at + INTERVAL '2 minutes'
                            ) < NOW()
                        )
                      )
                      AND next_attempt_at <= NOW()
                      AND NOT EXISTS (
                          SELECT 1
                          FROM market_events busy
                          WHERE busy.trade_group_id=market_events.trade_group_id
                            AND busy.status='processing'
                            AND COALESCE(
                                busy.lease_expires_at,
                                busy.updated_at + INTERVAL '2 minutes'
                            ) >= NOW()
                            AND (
                                $4='admin'
                                OR COALESCE(busy.watch_lane,'critical')='critical'
                            )
                      )
                      AND (
                          $4 <> 'admin'
                          OR NOT EXISTS (
                              SELECT 1
                              FROM market_events urgent
                              WHERE urgent.trade_group_id=market_events.trade_group_id
                                AND COALESCE(urgent.automation_enabled,1)=1
                                AND COALESCE(urgent.watch_lane,'critical')='critical'
                                AND (
                                    urgent.status='pending'
                                    OR (
                                        urgent.status='processing'
                                        AND COALESCE(
                                            urgent.lease_expires_at,
                                            urgent.updated_at + INTERVAL '2 minutes'
                                        ) < NOW()
                                    )
                                )
                                AND urgent.next_attempt_at <= NOW()
                          )
                      )
                    ORDER BY trade_group_id,
                             COALESCE(event_priority,20),
                             CASE WHEN escalated_at IS NULL AND LOWER(COALESCE(outcome_kind,'')) NOT LIKE '%watch%' THEN 0 ELSE 1 END,
                             CASE WHEN UPPER(event_type)='TP' THEN level_index ELSE 0 END DESC,
                             next_attempt_at,id
                ), picked AS (
                    SELECT p.id
                    FROM per_group p
                    JOIN trade_groups tg ON tg.id=p.trade_group_id
                    ORDER BY COALESCE(p.event_priority,20),
                             CASE WHEN p.escalated_at IS NULL AND LOWER(COALESCE(p.outcome_kind,'')) NOT LIKE '%watch%' THEN 0 ELSE 1 END,
                             CASE WHEN UPPER(p.event_type)='TP' THEN p.level_index ELSE 0 END DESC,
                             p.next_attempt_at,p.id
                    -- The trade-group row is the stable cross-lane mutex.  Locking
                    -- only market_events allowed critical/admin transactions to
                    -- select different rows of the same group concurrently.
                    FOR UPDATE OF tg SKIP LOCKED
                    LIMIT $1
                )
                UPDATE market_events m
                SET status='processing',
                    lease_token=$2,
                    lease_generation=COALESCE(lease_generation,0)+1,
                    lease_expires_at=NOW()+($3::double precision*INTERVAL '1 second'),
                    updated_at=NOW()
                FROM picked
                WHERE m.id=picked.id
                RETURNING m.*
                """,
                    lim,
                    claim_token,
                    lease_sec,
                    lane,
                )
                if lane == "critical" and rows:
                    group_ids = sorted(
                        {int(row.get("trade_group_id") or 0) for row in rows}
                        - {0}
                    )
                    if group_ids and hasattr(c, "execute"):
                        await c.execute(
                            """UPDATE market_events
                            SET status='pending',
                                lease_token=NULL,
                                lease_expires_at=NULL,
                                lease_generation=COALESCE(lease_generation,0)+1,
                                next_attempt_at=GREATEST(
                                    next_attempt_at, NOW()+INTERVAL '5 seconds'
                                ),
                                outcome_kind='admin_watch_preempted_by_critical',
                                updated_at=NOW()
                            WHERE trade_group_id=ANY($1::bigint[])
                              AND COALESCE(watch_lane,'critical')='admin'
                              AND status='processing'
                              AND COALESCE(
                                    lease_expires_at,
                                    updated_at + INTERVAL '2 minutes'
                                  ) >= NOW()
                            """,
                            group_ids,
                        )
            return sorted(
                (_dict(r) for r in rows),
                key=lambda row: (
                    int(row.get("event_priority"))
                    if row.get("event_priority") is not None
                    else MARKET_EVENT_PRIORITY_ENTRY,
                    0
                    if (
                        not row.get("escalated_at")
                        and "watch" not in str(row.get("outcome_kind") or "").lower()
                    )
                    else 1,
                    -int(row.get("level_index") or 0)
                    if str(row.get("event_type") or "").upper() == "TP"
                    else 0,
                    str(row.get("next_attempt_at") or ""),
                    int(row.get("id") or 0),
                ),
            )

    async with connect() as c:
        await c.execute("BEGIN IMMEDIATE")
        if cleanup_orphans:
            orphan_cur = await c.execute(
                """UPDATE market_events
                   SET status='done',armed=0,retrigger_requested=0,
                       retrigger_observed_price=NULL,lease_token=NULL,
                       lease_expires_at=NULL,
                       lease_generation=COALESCE(lease_generation,0)+1,
                       outcome_kind='orphan_trade_group',
                       last_error='trade_group_missing',
                       stuck_reason='trade_group_missing',
                       updated_at=CURRENT_TIMESTAMP
                   WHERE NOT EXISTS (
                       SELECT 1 FROM trade_groups tg
                       WHERE tg.id=market_events.trade_group_id
                   )
                     AND datetime(created_at)<datetime('now','-10 minutes')
                     AND NOT EXISTS (
                       SELECT 1 FROM market_events active
                       WHERE active.trade_group_id=market_events.trade_group_id
                         AND active.status='processing'
                         AND datetime(
                               COALESCE(active.lease_expires_at,active.updated_at),
                               CASE WHEN active.lease_expires_at IS NULL
                                   THEN '+2 minutes' ELSE '+0 seconds' END
                             ) >= datetime('now')
                     )
                     AND (
                       status='pending'
                       OR (
                           status='processing'
                           AND datetime(
                               COALESCE(lease_expires_at,updated_at),
                               CASE WHEN lease_expires_at IS NULL
                                   THEN '+2 minutes' ELSE '+0 seconds' END
                           ) < datetime('now')
                       )
                     )"""
            )
            if int(orphan_cur.rowcount or 0) > 0:
                log.error(
                    "MARKET_EVENT_ORPHANS_ARCHIVED backend=sqlite count=%s",
                    int(orphan_cur.rowcount or 0),
                )
        cur = await c.execute(
            """
            SELECT m.id
            FROM market_events m
            WHERE COALESCE(m.automation_enabled,1)=1
              AND COALESCE(m.watch_lane,'critical')=?
              AND (
                m.status='pending'
                OR (
                    m.status='processing'
                    AND datetime(COALESCE(m.lease_expires_at,m.updated_at),
                                 CASE WHEN m.lease_expires_at IS NULL THEN '+2 minutes' ELSE '+0 seconds' END)
                        < datetime('now')
                )
              )
              AND datetime(m.next_attempt_at) <= datetime('now')
              AND NOT EXISTS (
                    SELECT 1
                    FROM market_events busy
                    WHERE busy.trade_group_id=m.trade_group_id
                      AND busy.status='processing'
                      AND datetime(
                            COALESCE(busy.lease_expires_at,busy.updated_at),
                            CASE WHEN busy.lease_expires_at IS NULL
                                THEN '+2 minutes' ELSE '+0 seconds' END
                          ) >= datetime('now')
                      AND (
                            ?='admin'
                            OR COALESCE(busy.watch_lane,'critical')='critical'
                          )
              )
              AND (
                    ? <> 'admin'
                    OR NOT EXISTS (
                        SELECT 1
                        FROM market_events urgent
                        WHERE urgent.trade_group_id=m.trade_group_id
                          AND COALESCE(urgent.automation_enabled,1)=1
                          AND COALESCE(urgent.watch_lane,'critical')='critical'
                          AND (
                              urgent.status='pending'
                              OR (
                                  urgent.status='processing'
                                  AND datetime(
                                        COALESCE(urgent.lease_expires_at,urgent.updated_at),
                                        CASE WHEN urgent.lease_expires_at IS NULL
                                            THEN '+2 minutes' ELSE '+0 seconds' END
                                      ) < datetime('now')
                              )
                          )
                          AND datetime(urgent.next_attempt_at) <= datetime('now')
                    )
              )
              AND NOT EXISTS (
                    SELECT 1
                    FROM market_events x
                    WHERE x.trade_group_id=m.trade_group_id
                      AND COALESCE(x.automation_enabled,1)=1
                      AND COALESCE(x.watch_lane,'critical')=?
                      AND (
                        x.status='pending'
                        OR (
                            x.status='processing'
                            AND datetime(COALESCE(x.lease_expires_at,x.updated_at),
                                         CASE WHEN x.lease_expires_at IS NULL THEN '+2 minutes' ELSE '+0 seconds' END)
                                < datetime('now')
                        )
                      )
                      AND datetime(x.next_attempt_at) <= datetime('now')
                      AND (
                        COALESCE(x.event_priority,20) < COALESCE(m.event_priority,20)
                        OR (
                            COALESCE(x.event_priority,20)=COALESCE(m.event_priority,20)
                            AND CASE WHEN x.escalated_at IS NULL AND LOWER(COALESCE(x.outcome_kind,'')) NOT LIKE '%watch%' THEN 0 ELSE 1 END
                                < CASE WHEN m.escalated_at IS NULL AND LOWER(COALESCE(m.outcome_kind,'')) NOT LIKE '%watch%' THEN 0 ELSE 1 END
                        )
                        OR (
                            COALESCE(x.event_priority,20)=COALESCE(m.event_priority,20)
                            AND CASE WHEN x.escalated_at IS NULL AND LOWER(COALESCE(x.outcome_kind,'')) NOT LIKE '%watch%' THEN 0 ELSE 1 END
                                = CASE WHEN m.escalated_at IS NULL AND LOWER(COALESCE(m.outcome_kind,'')) NOT LIKE '%watch%' THEN 0 ELSE 1 END
                            AND CASE WHEN UPPER(x.event_type)='TP' THEN COALESCE(x.level_index,0) ELSE 0 END
                                > CASE WHEN UPPER(m.event_type)='TP' THEN COALESCE(m.level_index,0) ELSE 0 END
                        )
                        OR (
                            COALESCE(x.event_priority,20)=COALESCE(m.event_priority,20)
                            AND CASE WHEN x.escalated_at IS NULL AND LOWER(COALESCE(x.outcome_kind,'')) NOT LIKE '%watch%' THEN 0 ELSE 1 END
                                = CASE WHEN m.escalated_at IS NULL AND LOWER(COALESCE(m.outcome_kind,'')) NOT LIKE '%watch%' THEN 0 ELSE 1 END
                            AND CASE WHEN UPPER(x.event_type)='TP' THEN COALESCE(x.level_index,0) ELSE 0 END
                                = CASE WHEN UPPER(m.event_type)='TP' THEN COALESCE(m.level_index,0) ELSE 0 END
                            AND datetime(x.next_attempt_at) < datetime(m.next_attempt_at)
                        )
                        OR (
                            COALESCE(x.event_priority,20)=COALESCE(m.event_priority,20)
                            AND CASE WHEN x.escalated_at IS NULL AND LOWER(COALESCE(x.outcome_kind,'')) NOT LIKE '%watch%' THEN 0 ELSE 1 END
                                = CASE WHEN m.escalated_at IS NULL AND LOWER(COALESCE(m.outcome_kind,'')) NOT LIKE '%watch%' THEN 0 ELSE 1 END
                            AND CASE WHEN UPPER(x.event_type)='TP' THEN COALESCE(x.level_index,0) ELSE 0 END
                                = CASE WHEN UPPER(m.event_type)='TP' THEN COALESCE(m.level_index,0) ELSE 0 END
                            AND datetime(x.next_attempt_at)=datetime(m.next_attempt_at)
                            AND x.id < m.id
                        )
                      )
              )
            ORDER BY COALESCE(m.event_priority,20),
                     CASE WHEN m.escalated_at IS NULL AND LOWER(COALESCE(m.outcome_kind,'')) NOT LIKE '%watch%' THEN 0 ELSE 1 END,
                     CASE WHEN UPPER(m.event_type)='TP' THEN COALESCE(m.level_index,0) ELSE 0 END DESC,
                     datetime(m.next_attempt_at),m.id
            LIMIT ?
            """,
            (lane, lane, lane, lane, lim),
        )
        ids = [int(r[0]) for r in await cur.fetchall()]
        if not ids:
            await c.commit()
            return []
        ph = ",".join(["?"] * len(ids))
        if lane == "critical":
            group_ph = ",".join(["?"] * len(ids))
            await c.execute(
                f"""UPDATE market_events
                    SET status='pending',lease_token=NULL,lease_expires_at=NULL,
                        lease_generation=COALESCE(lease_generation,0)+1,
                        next_attempt_at=CASE
                            WHEN datetime(next_attempt_at) < datetime('now','+5 seconds')
                                THEN datetime('now','+5 seconds')
                            ELSE next_attempt_at
                        END,
                        outcome_kind='admin_watch_preempted_by_critical',
                        updated_at=CURRENT_TIMESTAMP
                    WHERE COALESCE(watch_lane,'critical')='admin'
                      AND status='processing'
                      AND trade_group_id IN (
                          SELECT DISTINCT trade_group_id
                          FROM market_events WHERE id IN ({group_ph})
                      )
                      AND datetime(
                            COALESCE(lease_expires_at,updated_at),
                            CASE WHEN lease_expires_at IS NULL
                                THEN '+2 minutes' ELSE '+0 seconds' END
                          ) >= datetime('now')""",
                ids,
            )
        lease_expires = (
            datetime.now(timezone.utc) + timedelta(seconds=lease_sec)
        ).isoformat()
        await c.execute(
            f"""UPDATE market_events
                SET status='processing',lease_token=?,
                    lease_generation=COALESCE(lease_generation,0)+1,
                    lease_expires_at=?,updated_at=CURRENT_TIMESTAMP
                WHERE id IN ({ph})""",
            (claim_token, lease_expires, *ids),
        )
        cur = await c.execute(
            f"""SELECT * FROM market_events WHERE id IN ({ph})
                ORDER BY COALESCE(event_priority,20),
                         CASE WHEN escalated_at IS NULL AND LOWER(COALESCE(outcome_kind,'')) NOT LIKE '%watch%' THEN 0 ELSE 1 END,
                         CASE WHEN UPPER(event_type)='TP' THEN COALESCE(level_index,0) ELSE 0 END DESC,
                         datetime(next_attempt_at),id""",
            ids,
        )
        rows = [_dict(r) for r in await cur.fetchall()]
        await c.commit()
        return rows


async def claim_due_admin_market_events(limit: int = 2) -> List[Dict[str, Any]]:
    """Claim the quarantined administrative lane at deliberately low capacity."""

    return await claim_due_market_events(limit=limit, watch_lane="admin")


async def market_event_diagnostic(event_id: int) -> Dict[str, Any] | None:
    """Return one read-only market-event snapshot for an administrator."""

    normalized_id = int(event_id or 0)
    if normalized_id <= 0:
        return None
    if is_postgres():
        async with connect() as c:
            row = await c.fetchrow(
                """SELECT me.*,tg.symbol AS group_symbol,tg.side AS group_side,
                          tg.status AS group_status
                   FROM market_events me
                   LEFT JOIN trade_groups tg ON tg.id=me.trade_group_id
                   WHERE me.id=$1""",
                normalized_id,
            )
            result = _dict(row) if row else None
    else:
        async with connect() as c:
            cur = await c.execute(
                """SELECT me.*,tg.symbol AS group_symbol,tg.side AS group_side,
                          tg.status AS group_status
                   FROM market_events me
                   LEFT JOIN trade_groups tg ON tg.id=me.trade_group_id
                   WHERE me.id=?""",
                (normalized_id,),
            )
            row = await cur.fetchone()
            result = _dict(row) if row else None
    if not result:
        return None
    executions = await trade_group_executions(
        int(result.get("trade_group_id") or 0), active_only=False, limit=5000
    )
    statuses = sorted({str(item.get("status") or "unknown") for item in executions})
    result["execution_count"] = len(executions)
    result["user_count"] = len(
        {int(item.get("user_id") or 0) for item in executions if int(item.get("user_id") or 0) > 0}
    )
    result["execution_statuses"] = statuses
    return result


async def coalesce_pending_limit_tp_events(
    *,
    trade_group_id: int,
    current_event_id: int,
    current_lease_token: str,
    current_lease_generation: int,
) -> Dict[str, Any]:
    """Collapse pending TP rows for one not-yet-entered group safely.

    The caller must prove ownership of the currently processing event.  All TP
    rows are locked before any mutation, so a stale worker cannot convert a
    newer owner's group into an administrative watch.  Deferred retrigger bits
    are consumed by the coalesced watch and cleared on every completed sibling;
    a future real crossing can therefore re-arm normally.
    """

    group_id = int(trade_group_id)
    event_id = int(current_event_id)
    token = str(current_lease_token or "")
    generation = int(current_lease_generation or 0)
    empty = {
        "canonical_event_id": 0,
        "event_keys": [],
        "coalesced": 0,
        "lease_conflict": False,
    }
    if group_id <= 0 or event_id <= 0 or not token or generation <= 0:
        return {**empty, "lease_conflict": True}

    if is_postgres():
        async with connect() as c:
            async with c.transaction():
                all_rows = await c.fetch(
                    """SELECT id,event_key,level_index,status,lease_token,
                              lease_generation
                       FROM market_events
                       WHERE trade_group_id=$1 AND UPPER(event_type)='TP'
                       ORDER BY COALESCE(level_index,0),id
                       FOR UPDATE""",
                    group_id,
                )
                rows = [_dict(row) for row in all_rows]
                owned = next(
                    (row for row in rows if int(row.get("id") or 0) == event_id),
                    None,
                )
                if (
                    not owned
                    or str(owned.get("status") or "") != "processing"
                    or str(owned.get("lease_token") or "") != token
                    or int(owned.get("lease_generation") or 0) != generation
                ):
                    return {**empty, "lease_conflict": True}

                candidates = [
                    row
                    for row in rows
                    if str(row.get("status") or "") == "pending"
                    or int(row.get("id") or 0) == event_id
                ]
                if not candidates:
                    return empty
                canonical = min(
                    candidates,
                    key=lambda row: (
                        int(row.get("level_index") or 0),
                        int(row.get("id") or 0),
                    ),
                )
                canonical_id = int(canonical.get("id") or 0)
                keys = [str(row.get("event_key") or "TP") for row in rows]
                encoded = json.dumps(keys, ensure_ascii=False, separators=(",", ":"))
                await c.execute(
                    """UPDATE market_events
                       SET coalesced_event_keys=$1,watch_lane='admin',
                           outcome_kind='pending_limit_group_watch',
                           retrigger_requested=0,retrigger_observed_price=NULL,
                           updated_at=NOW(),
                           next_attempt_at=CASE
                               WHEN status='pending' THEN NOW()
                               ELSE next_attempt_at
                           END
                       WHERE id=$2""",
                    encoded,
                    canonical_id,
                )
                tag = await c.execute(
                    """UPDATE market_events
                       SET status='done',armed=0,
                           outcome_kind='not_applicable_pending_entry',
                           watch_lane='critical',escalated_at=NULL,stuck_started_at=NULL,
                           last_stuck_alert_at=NULL,last_stuck_reminder_at=NULL,
                           stuck_reason='pending_limit_no_transition',
                           coalesced_event_keys=NULL,
                           retrigger_requested=0,retrigger_observed_price=NULL,
                           lease_token=NULL,lease_expires_at=NULL,updated_at=NOW()
                       WHERE trade_group_id=$1 AND UPPER(event_type)='TP'
                         AND id<>$2 AND id<>$3
                         AND (status='pending' OR (status='done' AND outcome_kind='not_applicable_pending_entry'))""",
                    group_id,
                    canonical_id,
                    event_id,
                )
                try:
                    coalesced = int(str(tag).split()[-1])
                except (TypeError, ValueError, IndexError):
                    coalesced = 0
                return {
                    "canonical_event_id": canonical_id,
                    "event_keys": keys,
                    "coalesced": max(0, coalesced),
                    "lease_conflict": False,
                }

    async with connect() as c:
        await c.execute("BEGIN IMMEDIATE")
        cur = await c.execute(
            """SELECT id,event_key,level_index,status,lease_token,lease_generation
               FROM market_events
               WHERE trade_group_id=? AND UPPER(event_type)='TP'
               ORDER BY COALESCE(level_index,0),id""",
            (group_id,),
        )
        rows = [_dict(row) for row in await cur.fetchall()]
        owned = next(
            (row for row in rows if int(row.get("id") or 0) == event_id),
            None,
        )
        if (
            not owned
            or str(owned.get("status") or "") != "processing"
            or str(owned.get("lease_token") or "") != token
            or int(owned.get("lease_generation") or 0) != generation
        ):
            await c.commit()
            return {**empty, "lease_conflict": True}

        candidates = [
            row
            for row in rows
            if str(row.get("status") or "") == "pending"
            or int(row.get("id") or 0) == event_id
        ]
        if not candidates:
            await c.commit()
            return empty
        canonical = min(
            candidates,
            key=lambda row: (
                int(row.get("level_index") or 0),
                int(row.get("id") or 0),
            ),
        )
        canonical_id = int(canonical.get("id") or 0)
        keys = [str(row.get("event_key") or "TP") for row in rows]
        encoded = json.dumps(keys, ensure_ascii=False, separators=(",", ":"))
        await c.execute(
            """UPDATE market_events
               SET coalesced_event_keys=?,watch_lane='admin',
                   outcome_kind='pending_limit_group_watch',
                   retrigger_requested=0,retrigger_observed_price=NULL,
                   updated_at=CURRENT_TIMESTAMP,
                   next_attempt_at=CASE
                       WHEN status='pending' THEN CURRENT_TIMESTAMP
                       ELSE next_attempt_at
                   END
               WHERE id=?""",
            (encoded, canonical_id),
        )
        cur = await c.execute(
            """UPDATE market_events
               SET status='done',armed=0,
                   outcome_kind='not_applicable_pending_entry',
                   watch_lane='critical',escalated_at=NULL,stuck_started_at=NULL,
                   last_stuck_alert_at=NULL,last_stuck_reminder_at=NULL,
                   stuck_reason='pending_limit_no_transition',
                   coalesced_event_keys=NULL,
                   retrigger_requested=0,retrigger_observed_price=NULL,
                   lease_token=NULL,lease_expires_at=NULL,updated_at=CURRENT_TIMESTAMP
               WHERE trade_group_id=? AND UPPER(event_type)='TP'
                 AND id<>? AND id<>?
                 AND (status='pending' OR (status='done' AND outcome_kind='not_applicable_pending_entry'))""",
            (group_id, canonical_id, event_id),
        )
        coalesced = max(0, int(cur.rowcount or 0))
        await c.commit()
        return {
            "canonical_event_id": canonical_id,
            "event_keys": keys,
            "coalesced": coalesced,
            "lease_conflict": False,
        }


async def commit_market_event_watch_result(
    event_id: int,
    *,
    retry_after_sec: float,
    error: str,
    increment_attempt: bool,
    lease_token: str,
    lease_generation: int,
    outcome_kind: str,
    watch_lane: str,
    escalated_at: datetime | str | None,
    stuck_started_at: datetime | str | None,
    last_stuck_alert_at: datetime | str | None,
    last_stuck_reminder_at: datetime | str | None,
    stuck_reason: str,
    coalesced_event_keys: str | None = None,
    reset_attempts: bool = False,
    notifications: List[Dict[str, Any]] | None = None,
) -> bool:
    """CAS-commit a nonterminal watch transition and its notification outbox.

    The market-event state and durable notification rows are written in one DB
    transaction.  Delivery can then fail safely: the existing notification
    worker retries the outbox after restart without duplicating the transition.
    """

    token = str(lease_token or "")
    generation = int(lease_generation or 0)
    if not token or generation <= 0:
        raise ValueError("fenced watch commit requires lease token and generation")
    lane = str(watch_lane or "critical").strip().lower()
    if lane not in {"critical", "admin"}:
        raise ValueError("invalid market-event watch lane")
    delay = max(0.0, float(retry_after_sec or 0.0))
    attempt_delta = 1 if increment_attempt else 0
    safe_error = str(error or "")[:1000]
    safe_outcome = str(outcome_kind or "")[:120]
    safe_reason = str(stuck_reason or "")[:500]
    specs = list(notifications or [])

    def _validated_specs() -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        for raw in specs:
            key = str(raw.get("dedup_key") or "").strip()
            user_id = int(raw.get("user_id") or 0)
            text = str(raw.get("message_text") or "").strip()
            if not key or user_id <= 0 or not text:
                continue
            result.append(
                {
                    "dedup_key": key[:256],
                    "user_id": user_id,
                    "message_text": text,
                    "source": str(raw.get("source") or "market_event_watch")[:120],
                }
            )
        return result

    valid_specs = _validated_specs()
    normalized_times = {
        "escalated_at": _market_event_datetime(
            escalated_at, field="escalated_at"
        ),
        "stuck_started_at": _market_event_datetime(
            stuck_started_at, field="stuck_started_at"
        ),
        "last_stuck_alert_at": _market_event_datetime(
            last_stuck_alert_at, field="last_stuck_alert_at"
        ),
        "last_stuck_reminder_at": _market_event_datetime(
            last_stuck_reminder_at, field="last_stuck_reminder_at"
        ),
    }
    if is_postgres():
        async with connect() as c:
            async with c.transaction():
                current = await c.fetchrow(
                    """SELECT retrigger_requested
                    FROM market_events
                    WHERE id=$1 AND status='processing'
                      AND lease_token=$2 AND lease_generation=$3
                    FOR UPDATE""",
                    int(event_id),
                    token,
                    generation,
                )
                if not current:
                    return False
                retrigger = int(current.get("retrigger_requested") or 0) == 1
                if retrigger:
                    tag = await c.execute(
                        """UPDATE market_events
                        SET status='pending',attempts=0,
                            observed_price=COALESCE(retrigger_observed_price,observed_price),
                            next_attempt_at=NOW(),last_error=NULL,
                            outcome_kind='retrigger_pending',watch_lane='critical',
                            escalated_at=NULL,stuck_started_at=NULL,
                            last_stuck_alert_at=NULL,last_stuck_reminder_at=NULL,
                            stuck_reason=NULL,coalesced_event_keys=NULL,
                            lease_token=NULL,lease_expires_at=NULL,
                            retrigger_requested=0,retrigger_observed_price=NULL,updated_at=NOW()
                        WHERE id=$1 AND status='processing'
                          AND lease_token=$2 AND lease_generation=$3""",
                        int(event_id),
                        token,
                        generation,
                    )
                    return str(tag).endswith(" 1")
                tag = await c.execute(
                    """UPDATE market_events
                    SET status='pending',
                        attempts=CASE WHEN $15 THEN 0 ELSE attempts+$1 END,
                        last_error=$2,
                        next_attempt_at=NOW()+($3::double precision*INTERVAL '1 second'),
                        outcome_kind=$4,watch_lane=$5,
                        escalated_at=$6::timestamptz,stuck_started_at=$7::timestamptz,
                        last_stuck_alert_at=$8::timestamptz,
                        last_stuck_reminder_at=$9::timestamptz,
                        stuck_reason=$10,coalesced_event_keys=COALESCE($11,coalesced_event_keys),
                        lease_token=NULL,lease_expires_at=NULL,updated_at=NOW()
                    WHERE id=$12 AND status='processing'
                      AND lease_token=$13 AND lease_generation=$14""",
                    attempt_delta,
                    safe_error,
                    delay,
                    safe_outcome,
                    lane,
                    normalized_times["escalated_at"],
                    normalized_times["stuck_started_at"],
                    normalized_times["last_stuck_alert_at"],
                    normalized_times["last_stuck_reminder_at"],
                    safe_reason,
                    coalesced_event_keys,
                    int(event_id),
                    token,
                    generation,
                    bool(reset_attempts),
                )
                if not str(tag).endswith(" 1"):
                    return False
                for spec in valid_specs:
                    await c.execute(
                        """INSERT INTO durable_notifications(
                            dedup_key,user_id,message_text,source,status,attempts,
                            next_attempt_at,last_error,delivered_at,updated_at
                        ) VALUES($1,$2,$3,$4,'pending',0,NOW(),'',NULL,NOW())
                        ON CONFLICT(dedup_key) DO NOTHING""",
                        spec["dedup_key"],
                        spec["user_id"],
                        spec["message_text"],
                        spec["source"],
                    )
                return True

    sqlite_times = {
        key: (value.isoformat() if value is not None else None)
        for key, value in normalized_times.items()
    }
    async with connect() as c:
        await c.execute("BEGIN IMMEDIATE")
        cur = await c.execute(
            """SELECT retrigger_requested FROM market_events
            WHERE id=? AND status='processing'
              AND lease_token=? AND lease_generation=?""",
            (int(event_id), token, generation),
        )
        current = await cur.fetchone()
        if current is None:
            await c.commit()
            return False
        retrigger = int(current[0] or 0) == 1
        if retrigger:
            cur = await c.execute(
                """UPDATE market_events
                SET status='pending',attempts=0,
                    observed_price=COALESCE(retrigger_observed_price,observed_price),
                    next_attempt_at=CURRENT_TIMESTAMP,last_error=NULL,
                    outcome_kind='retrigger_pending',watch_lane='critical',
                    escalated_at=NULL,stuck_started_at=NULL,
                    last_stuck_alert_at=NULL,last_stuck_reminder_at=NULL,
                    stuck_reason=NULL,coalesced_event_keys=NULL,
                    lease_token=NULL,lease_expires_at=NULL,
                    retrigger_requested=0,retrigger_observed_price=NULL,
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=? AND status='processing'
                  AND lease_token=? AND lease_generation=?""",
                (int(event_id), token, generation),
            )
            await c.commit()
            return int(cur.rowcount or 0) == 1
        modifier = f"+{delay:.3f} seconds"
        cur = await c.execute(
            """UPDATE market_events
            SET status='pending',
                attempts=CASE WHEN ? THEN 0 ELSE attempts+? END,last_error=?,
                next_attempt_at=datetime('now',?),outcome_kind=?,watch_lane=?,
                escalated_at=?,stuck_started_at=?,last_stuck_alert_at=?,
                last_stuck_reminder_at=?,stuck_reason=?,
                coalesced_event_keys=COALESCE(?,coalesced_event_keys),
                lease_token=NULL,lease_expires_at=NULL,updated_at=CURRENT_TIMESTAMP
            WHERE id=? AND status='processing'
              AND lease_token=? AND lease_generation=?""",
            (
                1 if reset_attempts else 0,
                attempt_delta,
                safe_error,
                modifier,
                safe_outcome,
                lane,
                sqlite_times["escalated_at"],
                sqlite_times["stuck_started_at"],
                sqlite_times["last_stuck_alert_at"],
                sqlite_times["last_stuck_reminder_at"],
                safe_reason,
                coalesced_event_keys,
                int(event_id),
                token,
                generation,
            ),
        )
        if int(cur.rowcount or 0) != 1:
            await c.commit()
            return False
        for spec in valid_specs:
            await c.execute(
                """INSERT INTO durable_notifications(
                    dedup_key,user_id,message_text,source,status,attempts,
                    next_attempt_at,last_error,delivered_at,updated_at
                ) VALUES(?,?,?,?,'pending',0,CURRENT_TIMESTAMP,'',NULL,CURRENT_TIMESTAMP)
                ON CONFLICT(dedup_key) DO NOTHING""",
                (
                    spec["dedup_key"],
                    spec["user_id"],
                    spec["message_text"],
                    spec["source"],
                ),
            )
        await c.commit()
        return True


async def extend_market_event_lease(
    event_id: int,
    *,
    lease_token: str,
    lease_generation: int,
    lease_seconds: float = MARKET_EVENT_LEASE_SECONDS,
) -> bool:
    """Extend only the currently owned processing lease."""

    token = str(lease_token or "")
    generation = int(lease_generation or 0)
    if not token or generation <= 0:
        return False
    seconds = max(15.0, float(lease_seconds or MARKET_EVENT_LEASE_SECONDS))
    if is_postgres():
        async with connect() as c:
            tag = await c.execute(
                """
                UPDATE market_events
                SET lease_expires_at=NOW()+($1::double precision*INTERVAL '1 second'),
                    updated_at=NOW()
                WHERE id=$2 AND status='processing'
                  AND lease_token=$3 AND lease_generation=$4
                """,
                seconds,
                int(event_id),
                token,
                generation,
            )
        return str(tag).endswith(" 1")
    expires = (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()
    async with connect() as c:
        cur = await c.execute(
            """
            UPDATE market_events
            SET lease_expires_at=?,updated_at=CURRENT_TIMESTAMP
            WHERE id=? AND status='processing'
              AND lease_token=? AND lease_generation=?
            """,
            (expires, int(event_id), token, generation),
        )
        await c.commit()
        return int(cur.rowcount or 0) == 1


async def release_market_event_lease(
    event_id: int,
    *,
    lease_token: str,
    lease_generation: int,
    retry_after_sec: float = 15.0,
    error: str = "lease released after verifier failure",
) -> bool:
    """Best-effort CAS release used when the final outcome write itself fails."""

    token = str(lease_token or "")
    generation = int(lease_generation or 0)
    if not token or generation <= 0:
        return False
    safe_error = str(error or "")[:1000]
    delay = max(5.0, float(retry_after_sec or 0.0))
    if is_postgres():
        async with connect() as c:
            tag = await c.execute(
                """
                UPDATE market_events
                SET status='pending',last_error=$1,
                    next_attempt_at=NOW()+($2::double precision*INTERVAL '1 second'),
                    lease_token=NULL,lease_expires_at=NULL,updated_at=NOW()
                WHERE id=$3 AND status='processing'
                  AND lease_token=$4 AND lease_generation=$5
                """,
                safe_error,
                delay,
                int(event_id),
                token,
                generation,
            )
        return str(tag).endswith(" 1")
    modifier = f"+{delay:.3f} seconds"
    async with connect() as c:
        cur = await c.execute(
            """
            UPDATE market_events
            SET status='pending',last_error=?,next_attempt_at=datetime('now',?),
                lease_token=NULL,lease_expires_at=NULL,updated_at=CURRENT_TIMESTAMP
            WHERE id=? AND status='processing'
              AND lease_token=? AND lease_generation=?
            """,
            (safe_error, modifier, int(event_id), token, generation),
        )
        await c.commit()
        return int(cur.rowcount or 0) == 1




async def market_event_migration_candidates(
    *,
    min_attempts: int = 6,
    limit: int = 10,
    target_group_id: int | None = None,
    include_shadow: bool = True,
) -> List[Dict[str, Any]]:
    """Return bounded legacy stuck TP rows eligible for one final g42 check.

    This is read-only.  Active leased rows are never selected; a processing row
    must first return to pending through the normal lease/recovery path.
    """

    minimum = max(1, int(min_attempts or 1))
    bounded = max(1, min(1000, int(limit or 10)))
    target = int(target_group_id or 0)
    if is_postgres():
        async with connect() as c:
            rows = await c.fetch(
                """
                SELECT * FROM market_events
                WHERE UPPER(COALESCE(event_type,''))='TP'
                  AND status='pending'
                  AND COALESCE(automation_enabled,1)=1
                  AND COALESCE(attempts,0)>=$1
                  AND (COALESCE(migration_state,'none')='none'
                       OR ($4::boolean AND migration_state='shadow'))
                  AND UPPER(COALESCE(phase,'LEGACY')) NOT IN ('MANUAL_REVIEW','COMPLETED')
                  AND (
                       COALESCE(stuck_reason,'')='pending_limit_no_transition'
                    OR COALESCE(last_error,'') LIKE '%pending_limit_no_transition%'
                    OR COALESCE(shadow_reason,'')='pending_limit_no_transition'
                  )
                  AND ($2::bigint=0 OR trade_group_id=$2)
                ORDER BY CASE WHEN trade_group_id=$3::bigint THEN 0 ELSE 1 END,
                         COALESCE(attempts,0) DESC,id
                LIMIT $5
                """,
                minimum,
                target,
                target,
                bool(include_shadow),
                bounded,
            )
        return [dict(row) for row in rows]
    async with connect() as c:
        cur = await c.execute(
            """
            SELECT * FROM market_events
            WHERE UPPER(COALESCE(event_type,''))='TP'
              AND status='pending'
              AND COALESCE(automation_enabled,1)=1
              AND COALESCE(attempts,0)>=?
              AND (COALESCE(migration_state,'none')='none'
                   OR (?=1 AND migration_state='shadow'))
              AND UPPER(COALESCE(phase,'LEGACY')) NOT IN ('MANUAL_REVIEW','COMPLETED')
              AND (
                   COALESCE(stuck_reason,'')='pending_limit_no_transition'
                OR COALESCE(last_error,'') LIKE '%pending_limit_no_transition%'
                OR COALESCE(shadow_reason,'')='pending_limit_no_transition'
              )
              AND (?=0 OR trade_group_id=?)
            ORDER BY CASE WHEN trade_group_id=? THEN 0 ELSE 1 END,
                     COALESCE(attempts,0) DESC,id
            LIMIT ?
            """,
            (minimum, 1 if include_shadow else 0, target, target, target, bounded),
        )
        rows = await cur.fetchall()
    return [_dict(row) for row in rows]


async def mark_market_event_migration_shadow(
    event_id: int,
    *,
    migration_version: int = 1,
    reason: str = "g42_shadow_candidate",
) -> bool:
    """Record rollout visibility without changing status, retries or ownership."""

    safe_reason = str(reason or "g42_shadow_candidate")[:1000]
    version = max(1, int(migration_version or 1))
    if is_postgres():
        async with connect() as c:
            tag = await c.execute(
                """UPDATE market_events
                   SET migration_state='shadow',migration_version=$1,
                       migration_reason=$2,updated_at=NOW()
                   WHERE id=$3 AND COALESCE(migration_state,'none')='none'
                     AND status='pending'""",
                version,
                safe_reason,
                int(event_id),
            )
        return str(tag).endswith(" 1")
    async with connect() as c:
        cur = await c.execute(
            """UPDATE market_events
               SET migration_state='shadow',migration_version=?,
                   migration_reason=?,updated_at=CURRENT_TIMESTAMP
               WHERE id=? AND COALESCE(migration_state,'none')='none'
                 AND status='pending'""",
            (version, safe_reason, int(event_id)),
        )
        await c.commit()
        return int(cur.rowcount or 0) == 1


async def prepare_market_event_final_migration(
    event_id: int,
    *,
    max_fast_attempts: int,
    max_deep_attempts: int,
    migration_version: int = 1,
    reason: str = "g42_legacy_stuck_final_check",
) -> bool:
    """Atomically schedule exactly one final state-machine observation.

    The row must be pending and unleased.  Existing legacy attempt history is
    preserved, while finite counters are positioned immediately before the one
    permitted FINAL check.  No BingX order is created, cancelled or modified.
    """

    fast = max(1, int(max_fast_attempts or 1))
    deep = max(1, int(max_deep_attempts or 1))
    version = max(1, int(migration_version or 1))
    safe_reason = str(reason or "g42_legacy_stuck_final_check")[:1000]
    if is_postgres():
        async with connect() as c:
            tag = await c.execute(
                """UPDATE market_events SET
                       phase='FINAL_CHECK_PENDING',fast_attempts=$1,
                       deep_attempts=$2,final_attempts=0,
                       migration_state='prepared',migration_version=$3,
                       migration_started_at=NOW(),migration_completed_at=NULL,
                       migration_reason=$4,watch_lane='admin',
                       next_attempt_at=NOW(),last_error=$4,
                       lease_token=NULL,lease_expires_at=NULL,updated_at=NOW()
                   WHERE id=$5 AND status='pending'
                     AND COALESCE(automation_enabled,1)=1
                     AND COALESCE(migration_state,'none') IN ('none','shadow')
                     AND UPPER(COALESCE(event_type,''))='TP'""",
                fast,
                deep,
                version,
                safe_reason,
                int(event_id),
            )
        return str(tag).endswith(" 1")
    async with connect() as c:
        await c.execute("BEGIN IMMEDIATE")
        cur = await c.execute(
            """UPDATE market_events SET
                   phase='FINAL_CHECK_PENDING',fast_attempts=?,deep_attempts=?,
                   final_attempts=0,migration_state='prepared',migration_version=?,
                   migration_started_at=CURRENT_TIMESTAMP,migration_completed_at=NULL,
                   migration_reason=?,watch_lane='admin',
                   next_attempt_at=CURRENT_TIMESTAMP,last_error=?,
                   lease_token=NULL,lease_expires_at=NULL,updated_at=CURRENT_TIMESTAMP
               WHERE id=? AND status='pending'
                 AND COALESCE(automation_enabled,1)=1
                 AND COALESCE(migration_state,'none') IN ('none','shadow')
                 AND UPPER(COALESCE(event_type,''))='TP'""",
            (fast, deep, version, safe_reason, safe_reason, int(event_id)),
        )
        await c.commit()
        return int(cur.rowcount or 0) == 1


async def market_event_rollout_snapshot(
    target_group_id: int | None = 1541,
) -> Dict[str, Any]:
    """Return secret-free global and target-group rollout counters."""

    target = int(target_group_id or 1541)
    if is_postgres():
        async with connect() as c:
            row = await c.fetchrow(
                """SELECT
                    COUNT(*) FILTER (WHERE COALESCE(migration_state,'none')='none') AS none_count,
                    COUNT(*) FILTER (WHERE migration_state='shadow') AS shadow_count,
                    COUNT(*) FILTER (WHERE migration_state='prepared') AS prepared_count,
                    COUNT(*) FILTER (WHERE migration_state='completed') AS completed_count,
                    COUNT(*) FILTER (WHERE phase='MANUAL_REVIEW') AS manual_review_count,
                    COUNT(*) FILTER (WHERE trade_group_id=$1) AS target_group_rows,
                    COUNT(*) FILTER (WHERE trade_group_id=$1 AND migration_state='shadow') AS target_shadow_count,
                    COUNT(*) FILTER (WHERE trade_group_id=$1 AND migration_state='prepared') AS target_prepared_count,
                    COUNT(*) FILTER (WHERE trade_group_id=$1 AND migration_state='completed') AS target_completed_count,
                    COUNT(*) FILTER (WHERE trade_group_id=$1 AND phase='MANUAL_REVIEW') AS target_manual_review_count
                   FROM market_events""",
                target,
            )
        result = dict(row or {})
    else:
        async with connect() as c:
            cur = await c.execute(
                """SELECT
                    SUM(CASE WHEN COALESCE(migration_state,'none')='none' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN migration_state='shadow' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN migration_state='prepared' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN migration_state='completed' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN phase='MANUAL_REVIEW' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN trade_group_id=? THEN 1 ELSE 0 END),
                    SUM(CASE WHEN trade_group_id=? AND migration_state='shadow' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN trade_group_id=? AND migration_state='prepared' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN trade_group_id=? AND migration_state='completed' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN trade_group_id=? AND phase='MANUAL_REVIEW' THEN 1 ELSE 0 END)
                   FROM market_events""",
                (target, target, target, target, target),
            )
            row = await cur.fetchone()
        values = list(row or [0] * 10)
        result = {
            "none_count": int(values[0] or 0),
            "shadow_count": int(values[1] or 0),
            "prepared_count": int(values[2] or 0),
            "completed_count": int(values[3] or 0),
            "manual_review_count": int(values[4] or 0),
            "target_group_rows": int(values[5] or 0),
            "target_shadow_count": int(values[6] or 0),
            "target_prepared_count": int(values[7] or 0),
            "target_completed_count": int(values[8] or 0),
            "target_manual_review_count": int(values[9] or 0),
        }

    result["target_group_id"] = target
    # Backward-compatible key retained for older diagnostics/tests.
    result["group_1541_rows"] = int(result.get("target_group_rows") or 0)
    return result


def _market_event_audit_state(row: Dict[str, Any]) -> Dict[str, Any]:
    keys = (
        "id", "trade_group_id", "event_key", "event_type", "status", "phase",
        "attempts", "fast_attempts", "deep_attempts", "final_attempts",
        "automation_enabled", "outcome_kind", "terminal_outcome",
        "terminal_reason", "migration_state", "migration_version",
        "manual_resolution", "evidence_fingerprint",
    )
    return {key: row.get(key) for key in keys}


async def retry_market_event_manual_review(
    event_id: int,
    *,
    admin_user_id: int,
    max_fast_attempts: int,
    max_deep_attempts: int,
    comment: str = "",
) -> bool:
    """Admin-authorized one-shot FINAL recheck; never changes exchange orders."""

    eid = int(event_id)
    admin = int(admin_user_id)
    safe_comment = str(comment or "")[:1000]
    fast = max(1, int(max_fast_attempts or 1))
    deep = max(1, int(max_deep_attempts or 1))
    if is_postgres():
        async with connect() as c:
            async with c.transaction():
                record = await c.fetchrow("SELECT * FROM market_events WHERE id=$1 FOR UPDATE", eid)
                if not record:
                    return False
                before = dict(record)
                if str(before.get("phase") or "").upper() != "MANUAL_REVIEW":
                    return False
                tag = await c.execute(
                    """UPDATE market_events SET status='pending',phase='FINAL_CHECK_PENDING',
                           fast_attempts=$1,deep_attempts=$2,final_attempts=0,
                           automation_enabled=1,next_attempt_at=NOW(),watch_lane='admin',
                           manual_review_at=NULL,manual_resolution=NULL,
                           manual_resolution_admin_id=NULL,manual_resolution_at=NULL,
                           terminal_outcome=NULL,terminal_reason=$3,terminal_at=NULL,
                           outcome_kind='manual_recheck_requested',last_error=$3,
                           migration_state='prepared',migration_started_at=NOW(),
                           migration_completed_at=NULL,migration_reason='admin_final_recheck',
                           lease_token=NULL,lease_expires_at=NULL,updated_at=NOW()
                       WHERE id=$4 AND phase='MANUAL_REVIEW' AND COALESCE(automation_enabled,0)=0""",
                    fast, deep, safe_comment or "admin_final_recheck", eid,
                )
                if not str(tag).endswith(" 1"):
                    return False
                after_record = await c.fetchrow("SELECT * FROM market_events WHERE id=$1", eid)
                after = dict(after_record or {})
                await c.execute(
                    """INSERT INTO market_event_manual_actions(
                           event_id,trade_group_id,admin_user_id,action,comment,
                           before_state_json,after_state_json,evidence_fingerprint)
                       VALUES($1,$2,$3,'retry_final',$4,$5,$6,$7)""",
                    eid, int(before.get("trade_group_id") or 0), admin, safe_comment,
                    json.dumps(_market_event_audit_state(before), sort_keys=True, ensure_ascii=False),
                    json.dumps(_market_event_audit_state(after), sort_keys=True, ensure_ascii=False),
                    str(before.get("evidence_fingerprint") or "") or None,
                )
        return True
    async with connect() as c:
        await c.execute("BEGIN IMMEDIATE")
        cur = await c.execute("SELECT * FROM market_events WHERE id=?", (eid,))
        record = await cur.fetchone()
        if not record:
            await c.rollback()
            return False
        before = _dict(record)
        if str(before.get("phase") or "").upper() != "MANUAL_REVIEW":
            await c.rollback()
            return False
        cur = await c.execute(
            """UPDATE market_events SET status='pending',phase='FINAL_CHECK_PENDING',
                   fast_attempts=?,deep_attempts=?,final_attempts=0,
                   automation_enabled=1,next_attempt_at=CURRENT_TIMESTAMP,watch_lane='admin',
                   manual_review_at=NULL,manual_resolution=NULL,
                   manual_resolution_admin_id=NULL,manual_resolution_at=NULL,
                   terminal_outcome=NULL,terminal_reason=?,terminal_at=NULL,
                   outcome_kind='manual_recheck_requested',last_error=?,
                   migration_state='prepared',migration_started_at=CURRENT_TIMESTAMP,
                   migration_completed_at=NULL,migration_reason='admin_final_recheck',
                   lease_token=NULL,lease_expires_at=NULL,updated_at=CURRENT_TIMESTAMP
               WHERE id=? AND phase='MANUAL_REVIEW' AND COALESCE(automation_enabled,0)=0""",
            (fast, deep, safe_comment or "admin_final_recheck",
             safe_comment or "admin_final_recheck", eid),
        )
        if int(cur.rowcount or 0) != 1:
            await c.rollback()
            return False
        cur = await c.execute("SELECT * FROM market_events WHERE id=?", (eid,))
        after = _dict(await cur.fetchone())
        await c.execute(
            """INSERT INTO market_event_manual_actions(
                   event_id,trade_group_id,admin_user_id,action,comment,
                   before_state_json,after_state_json,evidence_fingerprint)
               VALUES(?,?,?,?,?,?,?,?)""",
            (eid, int(before.get("trade_group_id") or 0), admin, "retry_final",
             safe_comment,
             json.dumps(_market_event_audit_state(before), sort_keys=True, ensure_ascii=False),
             json.dumps(_market_event_audit_state(after), sort_keys=True, ensure_ascii=False),
             str(before.get("evidence_fingerprint") or "") or None),
        )
        await c.commit()
    return True


async def resolve_market_event_manual_review(
    event_id: int,
    *,
    admin_user_id: int,
    action: str,
    comment: str = "",
) -> bool:
    """Record an operational manual conclusion while preserving stats quarantine."""

    allowed = {
        "tp_filled_manual",
        "tp_not_filled_manual",
        "entry_never_filled_manual",
        "unknown_manual",
    }
    normalized = str(action or "").strip().lower()
    if normalized not in allowed:
        raise ValueError("unsupported market-event manual resolution")
    eid = int(event_id)
    admin = int(admin_user_id)
    safe_comment = str(comment or "")[:1000]
    terminal = normalized.upper()
    if is_postgres():
        async with connect() as c:
            async with c.transaction():
                record = await c.fetchrow("SELECT * FROM market_events WHERE id=$1 FOR UPDATE", eid)
                if not record:
                    return False
                before = dict(record)
                if str(before.get("phase") or "").upper() != "MANUAL_REVIEW":
                    return False
                tag = await c.execute(
                    """UPDATE market_events SET status='done',phase='MANUAL_REVIEW',
                           automation_enabled=0,next_attempt_at=NULL,armed=0,
                           outcome_kind=$1,terminal_outcome=$2,
                           terminal_reason=$3,terminal_at=COALESCE(terminal_at,NOW()),
                           manual_review_at=COALESCE(manual_review_at,NOW()),
                           manual_resolution=$1,manual_resolution_admin_id=$4,
                           manual_resolution_at=NOW(),migration_state='completed',
                           migration_completed_at=COALESCE(migration_completed_at,NOW()),
                           lease_token=NULL,lease_expires_at=NULL,updated_at=NOW()
                       WHERE id=$5 AND phase='MANUAL_REVIEW'""",
                    normalized, terminal, safe_comment or normalized, admin, eid,
                )
                if not str(tag).endswith(" 1"):
                    return False
                after_record = await c.fetchrow("SELECT * FROM market_events WHERE id=$1", eid)
                after = dict(after_record or {})
                await c.execute(
                    """INSERT INTO market_event_manual_actions(
                           event_id,trade_group_id,admin_user_id,action,comment,
                           before_state_json,after_state_json,evidence_fingerprint)
                       VALUES($1,$2,$3,$4,$5,$6,$7,$8)""",
                    eid, int(before.get("trade_group_id") or 0), admin, normalized,
                    safe_comment,
                    json.dumps(_market_event_audit_state(before), sort_keys=True, ensure_ascii=False),
                    json.dumps(_market_event_audit_state(after), sort_keys=True, ensure_ascii=False),
                    str(before.get("evidence_fingerprint") or "") or None,
                )
        return True
    async with connect() as c:
        await c.execute("BEGIN IMMEDIATE")
        cur = await c.execute("SELECT * FROM market_events WHERE id=?", (eid,))
        record = await cur.fetchone()
        if not record:
            await c.rollback()
            return False
        before = _dict(record)
        if str(before.get("phase") or "").upper() != "MANUAL_REVIEW":
            await c.rollback()
            return False
        cur = await c.execute(
            """UPDATE market_events SET status='done',phase='MANUAL_REVIEW',
                   automation_enabled=0,next_attempt_at=NULL,armed=0,
                   outcome_kind=?,terminal_outcome=?,terminal_reason=?,
                   terminal_at=COALESCE(terminal_at,CURRENT_TIMESTAMP),
                   manual_review_at=COALESCE(manual_review_at,CURRENT_TIMESTAMP),
                   manual_resolution=?,manual_resolution_admin_id=?,
                   manual_resolution_at=CURRENT_TIMESTAMP,migration_state='completed',
                   migration_completed_at=COALESCE(migration_completed_at,CURRENT_TIMESTAMP),
                   lease_token=NULL,lease_expires_at=NULL,updated_at=CURRENT_TIMESTAMP
               WHERE id=? AND phase='MANUAL_REVIEW'""",
            (normalized, terminal, safe_comment or normalized, normalized, admin, eid),
        )
        if int(cur.rowcount or 0) != 1:
            await c.rollback()
            return False
        cur = await c.execute("SELECT * FROM market_events WHERE id=?", (eid,))
        after = _dict(await cur.fetchone())
        await c.execute(
            """INSERT INTO market_event_manual_actions(
                   event_id,trade_group_id,admin_user_id,action,comment,
                   before_state_json,after_state_json,evidence_fingerprint)
               VALUES(?,?,?,?,?,?,?,?)""",
            (eid, int(before.get("trade_group_id") or 0), admin, normalized,
             safe_comment,
             json.dumps(_market_event_audit_state(before), sort_keys=True, ensure_ascii=False),
             json.dumps(_market_event_audit_state(after), sort_keys=True, ensure_ascii=False),
             str(before.get("evidence_fingerprint") or "") or None),
        )
        await c.commit()
    return True


async def market_event_manual_action_history(
    event_id: int, *, limit: int = 20
) -> List[Dict[str, Any]]:
    bounded = max(1, min(200, int(limit or 20)))
    if is_postgres():
        async with connect() as c:
            rows = await c.fetch(
                "SELECT * FROM market_event_manual_actions WHERE event_id=$1 ORDER BY id DESC LIMIT $2",
                int(event_id), bounded,
            )
        return [dict(row) for row in rows]
    async with connect() as c:
        cur = await c.execute(
            "SELECT * FROM market_event_manual_actions WHERE event_id=? ORDER BY id DESC LIMIT ?",
            (int(event_id), bounded),
        )
        rows = await cur.fetchall()
    return [_dict(row) for row in rows]


async def commit_market_event_state_machine(
    event_id: int,
    *,
    phase: str,
    terminal: bool,
    manual_review: bool,
    outcome: str,
    reason: str,
    retry_after_sec: float,
    fast_attempts: int,
    deep_attempts: int,
    final_attempts: int,
    lease_token: str,
    lease_generation: int,
) -> bool:
    """CAS-commit one finite g40 ENTRY/TP state-machine transition.

    Manual review and exact exchange-backed terminal outcomes are terminal for
    automation: both clear durable retries, disable rearming/enqueueing, discard
    stale retrigger bits, and release the fenced lease. A terminal evidence row
    is idempotent for one trade-group/event identity.
    """

    token = str(lease_token or "")
    generation = int(lease_generation or 0)
    if not token or generation <= 0:
        return False
    safe_phase = str(phase or "FAST_CHECK_PENDING")[:80]
    safe_outcome = str(outcome or "EVIDENCE_UNRESOLVED")[:120]
    safe_reason = str(reason or "")[:1000]
    retry = max(0.0, float(retry_after_sec or 0.0))
    fast = max(0, int(fast_attempts or 0))
    deep = max(0, int(deep_attempts or 0))
    final = max(0, int(final_attempts or 0))

    if is_postgres():
        async with connect() as c:
            if manual_review:
                tag = await c.execute(
                    """UPDATE market_events SET
                           status='done',armed=0,attempts=attempts+1,
                           phase='MANUAL_REVIEW',fast_attempts=$1,
                           deep_attempts=$2,final_attempts=$3,
                           terminal_outcome='UNKNOWN',terminal_reason=$4,
                           terminal_at=NOW(),manual_review_at=NOW(),
                           automation_enabled=0,next_attempt_at=NULL,
                           outcome_kind='manual_review',last_error=$4,
                           migration_state=CASE WHEN migration_state='prepared' THEN 'completed' ELSE migration_state END,
                           migration_completed_at=CASE WHEN migration_state='prepared' THEN NOW() ELSE migration_completed_at END,
                           watch_lane='admin',retrigger_requested=0,
                           retrigger_observed_price=NULL,lease_token=NULL,
                           lease_expires_at=NULL,updated_at=NOW()
                       WHERE id=$5 AND status='processing'
                         AND lease_token=$6 AND lease_generation=$7""",
                    fast, deep, final, safe_reason, int(event_id), token, generation,
                )
            elif terminal:
                tag = await c.execute(
                    """UPDATE market_events SET
                           status='done',armed=0,attempts=attempts+1,
                           phase='COMPLETED',fast_attempts=$1,deep_attempts=$2,
                           final_attempts=$3,terminal_outcome=$4,terminal_reason=$5,
                           terminal_at=NOW(),manual_review_at=NULL,
                           automation_enabled=0,next_attempt_at=NULL,
                           outcome_kind=$4,last_error=$5,
                           migration_state=CASE WHEN migration_state='prepared' THEN 'completed' ELSE migration_state END,
                           migration_completed_at=CASE WHEN migration_state='prepared' THEN NOW() ELSE migration_completed_at END,
                           watch_lane='critical',escalated_at=NULL,stuck_started_at=NULL,
                           last_stuck_alert_at=NULL,last_stuck_reminder_at=NULL,
                           stuck_reason=NULL,coalesced_event_keys=NULL,
                           retrigger_requested=0,retrigger_observed_price=NULL,
                           lease_token=NULL,lease_expires_at=NULL,updated_at=NOW()
                       WHERE id=$6 AND status='processing'
                         AND lease_token=$7 AND lease_generation=$8""",
                    fast, deep, final, safe_outcome, safe_reason,
                    int(event_id), token, generation,
                )
            else:
                tag = await c.execute(
                    """UPDATE market_events SET
                           status='pending',attempts=attempts+1,phase=$1,
                           fast_attempts=$2,deep_attempts=$3,final_attempts=$4,
                           next_attempt_at=NOW()+($5::double precision*INTERVAL '1 second'),
                           outcome_kind='state_machine_retry',last_error=$6,
                           watch_lane='critical',escalated_at=NULL,stuck_started_at=NULL,
                           last_stuck_alert_at=NULL,last_stuck_reminder_at=NULL,
                           stuck_reason=NULL,lease_token=NULL,lease_expires_at=NULL,
                           updated_at=NOW()
                       WHERE id=$7 AND status='processing'
                         AND lease_token=$8 AND lease_generation=$9""",
                    safe_phase, fast, deep, final, retry, safe_reason,
                    int(event_id), token, generation,
                )
        return str(tag).endswith(" 1")

    modifier = f"+{retry:.3f} seconds"
    async with connect() as c:
        await c.execute("BEGIN IMMEDIATE")
        if manual_review:
            cur = await c.execute(
                """UPDATE market_events SET
                       status='done',armed=0,attempts=attempts+1,
                       phase='MANUAL_REVIEW',fast_attempts=?,deep_attempts=?,final_attempts=?,
                       terminal_outcome='UNKNOWN',terminal_reason=?,
                       terminal_at=CURRENT_TIMESTAMP,manual_review_at=CURRENT_TIMESTAMP,
                       automation_enabled=0,next_attempt_at=NULL,
                       outcome_kind='manual_review',last_error=?,
                       migration_state=CASE WHEN migration_state='prepared' THEN 'completed' ELSE migration_state END,
                       migration_completed_at=CASE WHEN migration_state='prepared' THEN CURRENT_TIMESTAMP ELSE migration_completed_at END,
                       watch_lane='admin',retrigger_requested=0,retrigger_observed_price=NULL,
                       lease_token=NULL,lease_expires_at=NULL,updated_at=CURRENT_TIMESTAMP
                   WHERE id=? AND status='processing'
                     AND lease_token=? AND lease_generation=?""",
                (fast, deep, final, safe_reason, safe_reason,
                 int(event_id), token, generation),
            )
        elif terminal:
            cur = await c.execute(
                """UPDATE market_events SET
                       status='done',armed=0,attempts=attempts+1,
                       phase='COMPLETED',fast_attempts=?,deep_attempts=?,final_attempts=?,
                       terminal_outcome=?,terminal_reason=?,terminal_at=CURRENT_TIMESTAMP,
                       manual_review_at=NULL,automation_enabled=0,next_attempt_at=NULL,
                       outcome_kind=?,last_error=?,
                       migration_state=CASE WHEN migration_state='prepared' THEN 'completed' ELSE migration_state END,
                       migration_completed_at=CASE WHEN migration_state='prepared' THEN CURRENT_TIMESTAMP ELSE migration_completed_at END,
                       watch_lane='critical',escalated_at=NULL,stuck_started_at=NULL,
                       last_stuck_alert_at=NULL,last_stuck_reminder_at=NULL,
                       stuck_reason=NULL,coalesced_event_keys=NULL,
                       retrigger_requested=0,retrigger_observed_price=NULL,
                       lease_token=NULL,lease_expires_at=NULL,updated_at=CURRENT_TIMESTAMP
                   WHERE id=? AND status='processing'
                     AND lease_token=? AND lease_generation=?""",
                (fast, deep, final, safe_outcome, safe_reason,
                 safe_outcome, safe_reason, int(event_id), token, generation),
            )
        else:
            cur = await c.execute(
                """UPDATE market_events SET
                       status='pending',attempts=attempts+1,phase=?,
                       fast_attempts=?,deep_attempts=?,final_attempts=?,
                       next_attempt_at=datetime('now',?),
                       outcome_kind='state_machine_retry',last_error=?,
                       watch_lane='critical',escalated_at=NULL,stuck_started_at=NULL,
                       last_stuck_alert_at=NULL,last_stuck_reminder_at=NULL,
                       stuck_reason=NULL,lease_token=NULL,lease_expires_at=NULL,
                       updated_at=CURRENT_TIMESTAMP
                   WHERE id=? AND status='processing'
                     AND lease_token=? AND lease_generation=?""",
                (safe_phase, fast, deep, final, modifier, safe_reason,
                 int(event_id), token, generation),
            )
        await c.commit()
        return int(cur.rowcount or 0) == 1


async def mark_market_event_statistics_manual_review(
    trade_group_id: int, *, reason: str = "market_event_manual_review"
) -> int:
    """Fail-open quarantine for statistics linked to an ambiguous event."""

    group_id = int(trade_group_id or 0)
    if group_id <= 0:
        return 0
    safe_reason = str(reason or "market_event_manual_review")[:240]
    try:
        if is_postgres():
            async with connect() as c:
                async with c.transaction():
                    tag = await c.execute(
                        """UPDATE analytics_execution_results SET
                               market_event_review_status='manual_review',
                               market_event_exclusion_reason=$1,
                               market_event_reviewed_at=NOW(),
                               final_eligible=0,simulation_eligible=0,risk_analysis_eligible=0,
                               quality_gate_version=0,quality_evaluated_at=NULL,updated_at=NOW()
                           WHERE trade_group_id=$2""",
                        safe_reason, group_id,
                    )
                    await c.execute(
                        """UPDATE signal_analytics_signals SET
                               final_eligible=0,simulation_eligible=0,risk_analysis_eligible=0,
                               quality_gate_version=0,quality_evaluated_at=NULL,updated_at=NOW()
                           WHERE id IN (
                               SELECT DISTINCT analytics_signal_id
                               FROM analytics_execution_results
                               WHERE trade_group_id=$1 AND analytics_signal_id IS NOT NULL
                           )""",
                        group_id,
                    )
            try:
                return int(str(tag).split()[-1])
            except (ValueError, IndexError):
                return 0
        async with connect() as c:
            await c.execute("BEGIN IMMEDIATE")
            cur = await c.execute(
                """UPDATE analytics_execution_results SET
                       market_event_review_status='manual_review',
                       market_event_exclusion_reason=?,
                       market_event_reviewed_at=CURRENT_TIMESTAMP,
                       final_eligible=0,simulation_eligible=0,risk_analysis_eligible=0,
                       quality_gate_version=0,quality_evaluated_at=NULL,
                       updated_at=CURRENT_TIMESTAMP
                   WHERE trade_group_id=?""",
                (safe_reason, group_id),
            )
            await c.execute(
                """UPDATE signal_analytics_signals SET
                       final_eligible=0,simulation_eligible=0,risk_analysis_eligible=0,
                       quality_gate_version=0,quality_evaluated_at=NULL,
                       updated_at=CURRENT_TIMESTAMP
                   WHERE id IN (
                       SELECT DISTINCT analytics_signal_id
                       FROM analytics_execution_results
                       WHERE trade_group_id=? AND analytics_signal_id IS NOT NULL
                   )""",
                (group_id,),
            )
            await c.commit()
            return max(0, int(cur.rowcount or 0))
    except Exception as exc:
        # Statistics must never block trading protection or event completion.
        log.warning(
            "MARKET_EVENT_STATISTICS_MANUAL_REVIEW_FAILED group_id=%s error_type=%s error=%s",
            group_id, type(exc).__name__, str(exc)[:300],
        )
        return 0



async def clear_market_event_statistics_manual_review(
    trade_group_id: int, *, reason: str = "exact_exchange_evidence_after_manual_recheck"
) -> int:
    """Clear only the manual-review marker after new exact exchange evidence.

    Eligibility is not granted here.  Quality versions are reset so the normal
    Quality Gate must recompute every linked execution and signal.
    """

    group_id = int(trade_group_id or 0)
    if group_id <= 0:
        return 0
    safe_reason = str(reason or "exact_exchange_evidence_after_manual_recheck")[:240]
    try:
        if is_postgres():
            async with connect() as c:
                async with c.transaction():
                    tag = await c.execute(
                        """UPDATE analytics_execution_results SET
                               market_event_review_status='clear',
                               market_event_exclusion_reason=NULL,
                               market_event_reviewed_at=NOW(),
                               final_eligible=0,simulation_eligible=0,risk_analysis_eligible=0,
                               quality_gate_version=0,quality_evaluated_at=NULL,updated_at=NOW()
                           WHERE trade_group_id=$1
                             AND market_event_review_status='manual_review'""",
                        group_id,
                    )
                    await c.execute(
                        """UPDATE signal_analytics_signals SET
                               final_eligible=0,simulation_eligible=0,risk_analysis_eligible=0,
                               quality_gate_version=0,quality_evaluated_at=NULL,updated_at=NOW()
                           WHERE id IN (
                               SELECT DISTINCT analytics_signal_id
                               FROM analytics_execution_results
                               WHERE trade_group_id=$1 AND analytics_signal_id IS NOT NULL
                           )""",
                        group_id,
                    )
            try:
                changed = int(str(tag).split()[-1])
            except (ValueError, IndexError):
                changed = 0
        else:
            async with connect() as c:
                await c.execute("BEGIN IMMEDIATE")
                cur = await c.execute(
                    """UPDATE analytics_execution_results SET
                           market_event_review_status='clear',
                           market_event_exclusion_reason=NULL,
                           market_event_reviewed_at=CURRENT_TIMESTAMP,
                           final_eligible=0,simulation_eligible=0,risk_analysis_eligible=0,
                           quality_gate_version=0,quality_evaluated_at=NULL,
                           updated_at=CURRENT_TIMESTAMP
                       WHERE trade_group_id=?
                         AND market_event_review_status='manual_review'""",
                    (group_id,),
                )
                await c.execute(
                    """UPDATE signal_analytics_signals SET
                           final_eligible=0,simulation_eligible=0,risk_analysis_eligible=0,
                           quality_gate_version=0,quality_evaluated_at=NULL,
                           updated_at=CURRENT_TIMESTAMP
                       WHERE id IN (
                           SELECT DISTINCT analytics_signal_id
                           FROM analytics_execution_results
                           WHERE trade_group_id=? AND analytics_signal_id IS NOT NULL
                       )""",
                    (group_id,),
                )
                await c.commit()
                changed = max(0, int(cur.rowcount or 0))
        log.info(
            "MARKET_EVENT_STATISTICS_REVIEW_CLEARED group_id=%s rows=%s reason=%s",
            group_id, changed, safe_reason,
        )
        return changed
    except Exception as exc:
        log.warning(
            "MARKET_EVENT_STATISTICS_REVIEW_CLEAR_FAILED group_id=%s error_type=%s error=%s",
            group_id, type(exc).__name__, str(exc)[:300],
        )
        return 0


async def finish_market_event(
    event_id: int,
    *,
    done: bool,
    retry_after_sec: float = 0.0,
    error: str = "",
    increment_attempt: bool = True,
    force_terminal: bool = False,
    lease_token: str = "",
    lease_generation: int = 0,
    outcome_kind: str = "",
) -> bool:
    """CAS-finish an event or schedule its next durable verification attempt.

    Claimed workers must supply the lease token and generation returned by
    :func:`claim_due_market_events`.  A stale worker then receives ``False`` and
    cannot overwrite a newer owner's result.  Empty lease parameters retain the
    legacy direct-call behavior used by maintenance tools and older tests.
    """

    if force_terminal and not done:
        raise ValueError("force_terminal requires done=True")

    attempt_delta = 1 if increment_attempt else 0
    safe_error = str(error or "")[:1000]
    safe_outcome = str(outcome_kind or "")[:120]
    token = str(lease_token or "")
    generation = int(lease_generation or 0)
    fenced = bool(token and generation > 0)

    if is_postgres():
        where = "id=$3"
        if fenced:
            where += " AND status='processing' AND lease_token=$4 AND lease_generation=$5"
        async with connect() as c:
            if done and force_terminal:
                if fenced:
                    tag = await c.execute(
                        f"""
                        UPDATE market_events
                        SET status='done',attempts=attempts+$1,last_error=$2,
                            outcome_kind=$6,lease_token=NULL,lease_expires_at=NULL,
                            retrigger_requested=0,retrigger_observed_price=NULL,
                            updated_at=NOW()
                        WHERE {where}
                        """,
                        attempt_delta,
                        safe_error,
                        int(event_id),
                        token,
                        generation,
                        safe_outcome,
                    )
                elif safe_outcome:
                    tag = await c.execute(
                        """
                        UPDATE market_events
                        SET status='done',attempts=attempts+$1,last_error=$2,
                            outcome_kind=$4,lease_token=NULL,lease_expires_at=NULL,
                            retrigger_requested=0,retrigger_observed_price=NULL,
                            updated_at=NOW()
                        WHERE id=$3
                        """,
                        attempt_delta,
                        safe_error,
                        int(event_id),
                        safe_outcome,
                    )
                else:
                    # Preserve the historical direct-call contract for older
                    # maintenance tools/tests that do not use leases/outcomes.
                    tag = await c.execute(
                        """
                        UPDATE market_events
                        SET status='done',attempts=attempts+$1,last_error=$2,
                            lease_token=NULL,lease_expires_at=NULL,
                            retrigger_requested=0,retrigger_observed_price=NULL,
                            updated_at=NOW()
                        WHERE id=$3
                        """,
                        attempt_delta,
                        safe_error,
                        int(event_id),
                    )
            elif done:
                if fenced:
                    tag = await c.execute(
                        f"""
                        UPDATE market_events
                        SET status=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN 'pending' ELSE 'done' END,
                            attempts=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN 0 ELSE attempts+$1 END,
                            observed_price=CASE WHEN COALESCE(retrigger_requested,0)=1
                                THEN COALESCE(retrigger_observed_price,observed_price) ELSE observed_price END,
                            next_attempt_at=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NOW() ELSE next_attempt_at END,
                            last_error=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE $2 END,
                            outcome_kind=CASE
                                WHEN COALESCE(retrigger_requested,0)=1 THEN 'retrigger_pending'
                                ELSE $6
                            END,
                            watch_lane=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN 'critical' ELSE watch_lane END,
                            escalated_at=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE escalated_at END,
                            stuck_started_at=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE stuck_started_at END,
                            last_stuck_alert_at=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE last_stuck_alert_at END,
                            last_stuck_reminder_at=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE last_stuck_reminder_at END,
                            stuck_reason=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE stuck_reason END,
                            coalesced_event_keys=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE coalesced_event_keys END,
                            lease_token=NULL,lease_expires_at=NULL,
                            retrigger_requested=0,retrigger_observed_price=NULL,updated_at=NOW()
                        WHERE {where}
                        """,
                        attempt_delta,
                        safe_error,
                        int(event_id),
                        token,
                        generation,
                        safe_outcome,
                    )
                elif safe_outcome:
                    tag = await c.execute(
                        """
                        UPDATE market_events
                        SET status=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN 'pending' ELSE 'done' END,
                            attempts=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN 0 ELSE attempts+$1 END,
                            observed_price=CASE WHEN COALESCE(retrigger_requested,0)=1
                                THEN COALESCE(retrigger_observed_price,observed_price) ELSE observed_price END,
                            next_attempt_at=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NOW() ELSE next_attempt_at END,
                            last_error=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE $2 END,
                            outcome_kind=CASE
                                WHEN COALESCE(retrigger_requested,0)=1 THEN 'retrigger_pending'
                                ELSE $4
                            END,
                            watch_lane=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN 'critical' ELSE watch_lane END,
                            escalated_at=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE escalated_at END,
                            stuck_started_at=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE stuck_started_at END,
                            last_stuck_alert_at=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE last_stuck_alert_at END,
                            last_stuck_reminder_at=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE last_stuck_reminder_at END,
                            stuck_reason=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE stuck_reason END,
                            coalesced_event_keys=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE coalesced_event_keys END,
                            lease_token=NULL,lease_expires_at=NULL,
                            retrigger_requested=0,retrigger_observed_price=NULL,updated_at=NOW()
                        WHERE id=$3
                        """,
                        attempt_delta,
                        safe_error,
                        int(event_id),
                        safe_outcome,
                    )
                else:
                    tag = await c.execute(
                        """
                        UPDATE market_events
                        SET status=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN 'pending' ELSE 'done' END,
                            attempts=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN 0 ELSE attempts+$1 END,
                            observed_price=CASE WHEN COALESCE(retrigger_requested,0)=1
                                THEN COALESCE(retrigger_observed_price,observed_price) ELSE observed_price END,
                            next_attempt_at=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NOW() ELSE next_attempt_at END,
                            last_error=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE $2 END,
                            outcome_kind=CASE
                                WHEN COALESCE(retrigger_requested,0)=1 THEN 'retrigger_pending'
                                ELSE outcome_kind
                            END,
                            watch_lane=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN 'critical' ELSE watch_lane END,
                            escalated_at=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE escalated_at END,
                            stuck_started_at=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE stuck_started_at END,
                            last_stuck_alert_at=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE last_stuck_alert_at END,
                            last_stuck_reminder_at=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE last_stuck_reminder_at END,
                            stuck_reason=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE stuck_reason END,
                            coalesced_event_keys=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE coalesced_event_keys END,
                            lease_token=NULL,lease_expires_at=NULL,
                            retrigger_requested=0,retrigger_observed_price=NULL,updated_at=NOW()
                        WHERE id=$3
                        """,
                        attempt_delta,
                        safe_error,
                        int(event_id),
                    )
            else:
                if fenced:
                    tag = await c.execute(
                        """
                        UPDATE market_events
                        SET status='pending',attempts=attempts+$1,last_error=$2,
                            next_attempt_at=NOW()+($3::double precision*INTERVAL '1 second'),
                            outcome_kind=$7,lease_token=NULL,lease_expires_at=NULL,updated_at=NOW()
                        WHERE id=$4 AND status='processing'
                          AND lease_token=$5 AND lease_generation=$6
                        """,
                        attempt_delta,
                        safe_error,
                        float(max(0.0, retry_after_sec)),
                        int(event_id),
                        token,
                        generation,
                        safe_outcome,
                    )
                elif safe_outcome:
                    tag = await c.execute(
                        """
                        UPDATE market_events
                        SET status='pending',attempts=attempts+$1,last_error=$2,
                            next_attempt_at=NOW()+($3::double precision*INTERVAL '1 second'),
                            outcome_kind=$5,lease_token=NULL,lease_expires_at=NULL,updated_at=NOW()
                        WHERE id=$4
                        """,
                        attempt_delta,
                        safe_error,
                        float(max(0.0, retry_after_sec)),
                        int(event_id),
                        safe_outcome,
                    )
                else:
                    tag = await c.execute(
                        """
                        UPDATE market_events
                        SET status='pending',attempts=attempts+$1,last_error=$2,
                            next_attempt_at=NOW()+($3::double precision*INTERVAL '1 second'),
                            lease_token=NULL,lease_expires_at=NULL,updated_at=NOW()
                        WHERE id=$4
                        """,
                        attempt_delta,
                        safe_error,
                        float(max(0.0, retry_after_sec)),
                        int(event_id),
                    )
        return str(tag).endswith(" 1")

    lease_clause = ""
    lease_args: tuple[Any, ...] = ()
    if fenced:
        lease_clause = " AND status='processing' AND lease_token=? AND lease_generation=?"
        lease_args = (token, generation)
    async with connect() as c:
        if done and force_terminal:
            cur = await c.execute(
                f"""
                UPDATE market_events
                SET status='done',attempts=attempts+?,last_error=?,outcome_kind=?,
                    lease_token=NULL,lease_expires_at=NULL,
                    retrigger_requested=0,retrigger_observed_price=NULL,
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=?{lease_clause}
                """,
                (attempt_delta, safe_error, safe_outcome, int(event_id), *lease_args),
            )
        elif done:
            cur = await c.execute(
                f"""
                UPDATE market_events
                SET status=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN 'pending' ELSE 'done' END,
                    attempts=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN 0 ELSE attempts+? END,
                    observed_price=CASE WHEN COALESCE(retrigger_requested,0)=1
                        THEN COALESCE(retrigger_observed_price,observed_price) ELSE observed_price END,
                    next_attempt_at=CASE WHEN COALESCE(retrigger_requested,0)=1
                        THEN CURRENT_TIMESTAMP ELSE next_attempt_at END,
                    last_error=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE ? END,
                    outcome_kind=CASE
                        WHEN COALESCE(retrigger_requested,0)=1 THEN 'retrigger_pending'
                        ELSE ?
                    END,
                    watch_lane=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN 'critical' ELSE watch_lane END,
                    escalated_at=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE escalated_at END,
                    stuck_started_at=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE stuck_started_at END,
                    last_stuck_alert_at=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE last_stuck_alert_at END,
                    last_stuck_reminder_at=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE last_stuck_reminder_at END,
                    stuck_reason=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE stuck_reason END,
                    coalesced_event_keys=CASE WHEN COALESCE(retrigger_requested,0)=1 THEN NULL ELSE coalesced_event_keys END,
                    lease_token=NULL,lease_expires_at=NULL,
                    retrigger_requested=0,retrigger_observed_price=NULL,
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=?{lease_clause}
                """,
                (attempt_delta, safe_error, safe_outcome, int(event_id), *lease_args),
            )
        else:
            modifier = f"+{float(max(0.0, retry_after_sec)):.3f} seconds"
            cur = await c.execute(
                f"""
                UPDATE market_events
                SET status='pending',attempts=attempts+?,last_error=?,
                    next_attempt_at=datetime('now',?),outcome_kind=?,
                    lease_token=NULL,lease_expires_at=NULL,updated_at=CURRENT_TIMESTAMP
                WHERE id=?{lease_clause}
                """,
                (
                    attempt_delta,
                    safe_error,
                    modifier,
                    safe_outcome,
                    int(event_id),
                    *lease_args,
                ),
            )
        await c.commit()
        return int(cur.rowcount or 0) == 1


async def record_market_event_shadow_evidence(
    event_id: int,
    *,
    snapshot: Dict[str, Any],
    execution_states: List[Dict[str, Any]],
    worker_id: str = "",
    lease_generation: int = 0,
) -> Dict[str, Any]:
    """Persist one read-only market-event evidence snapshot.

    This Step-1 store is observational only: it never changes status, attempts,
    next_attempt_at, watch_lane, outcome_kind, lease fields or any
    trading/exchange field. Identical fingerprints only advance the dedicated
    shadow evaluation timestamp/counter; they do not rewrite the large JSON,
    per-execution state rows, generic ``updated_at`` or history.
    """

    eid = int(event_id or 0)
    if eid <= 0:
        return {"written": False, "changed": False, "reason": "invalid_event_id"}
    fingerprint = str(snapshot.get("evidence_fingerprint") or "").strip()
    decision = str(snapshot.get("shadow_decision") or "").strip()
    reason = str(snapshot.get("shadow_reason") or "").strip()
    if not fingerprint or not decision:
        return {"written": False, "changed": False, "reason": "invalid_snapshot"}
    snapshot_json = json.dumps(
        snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    )
    # ``market_events`` is a latency-sensitive hot table and many legacy
    # queries still select the complete row. Keep only a compact event summary
    # there; the full immutable snapshot lives in the append-only history and
    # normalized per-execution table.
    event_snapshot = dict(snapshot)
    event_snapshot["execution_count"] = len(snapshot.get("executions") or [])
    event_snapshot.pop("executions", None)
    event_snapshot_json = json.dumps(
        event_snapshot,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    worker = str(worker_id or "")[:200]
    generation = max(0, int(lease_generation or 0))
    attempt_number = max(0, int(snapshot.get("legacy_attempts") or 0)) + 1
    observed_raw = str(snapshot.get("observed_at") or "").strip()
    try:
        observed_at = datetime.fromisoformat(observed_raw.replace("Z", "+00:00"))
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=timezone.utc)
        observed_at = observed_at.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return {"written": False, "changed": False, "reason": "invalid_observed_at"}

    if is_postgres():
        async with connect() as c:
            async with c.transaction():
                current = await c.fetchrow(
                    "SELECT evidence_fingerprint,unchanged_evidence_count,shadow_evaluated_at "
                    "FROM market_events WHERE id=$1 FOR UPDATE",
                    eid,
                )
                if not current:
                    return {"written": False, "changed": False, "reason": "event_not_found"}
                current_evaluated = current["shadow_evaluated_at"]
                if current_evaluated is not None:
                    if isinstance(current_evaluated, str):
                        try:
                            current_evaluated = datetime.fromisoformat(
                                current_evaluated.replace("Z", "+00:00")
                            )
                        except (TypeError, ValueError, OverflowError):
                            current_evaluated = None
                    if isinstance(current_evaluated, datetime):
                        if current_evaluated.tzinfo is None:
                            current_evaluated = current_evaluated.replace(tzinfo=timezone.utc)
                        if observed_at < current_evaluated.astimezone(timezone.utc):
                            return {
                                "written": False,
                                "changed": False,
                                "reason": "stale_snapshot",
                            }
                changed = str(current["evidence_fingerprint"] or "") != fingerprint
                unchanged_count = (
                    0 if changed else int(current["unchanged_evidence_count"] or 0) + 1
                )
                if changed:
                    tag = await c.execute(
                        """
                        UPDATE market_events
                        SET phase=CASE WHEN phase='LEGACY' THEN 'SHADOW_OBSERVATION' ELSE phase END,
                            evidence_fingerprint=$2,evidence_snapshot_json=$3,
                            shadow_decision=$4,shadow_reason=$5,shadow_evaluated_at=$6,
                            shadow_version=1,unchanged_evidence_count=0,
                            last_exchange_change_at=$6
                        WHERE id=$1
                        """,
                        eid,
                        fingerprint,
                        event_snapshot_json,
                        decision,
                        reason,
                        observed_at,
                    )
                else:
                    tag = await c.execute(
                        """
                        UPDATE market_events
                        SET phase=CASE WHEN phase='LEGACY' THEN 'SHADOW_OBSERVATION' ELSE phase END,
                            shadow_evaluated_at=$3,unchanged_evidence_count=$2
                        WHERE id=$1
                        """,
                        eid,
                        unchanged_count,
                        observed_at,
                    )
                if not str(tag).endswith(" 1"):
                    return {"written": False, "changed": False, "reason": "event_update_failed"}

                if changed:
                    current_execution_ids = sorted(
                        {
                            int(state.get("execution_id") or 0)
                            for state in execution_states
                            if int(state.get("execution_id") or 0) > 0
                        }
                    )
                    # Keep the normalized state table an exact projection of
                    # the current snapshot. A removed/unlinked execution must
                    # not survive as stale shadow evidence.
                    await c.execute(
                        "DELETE FROM market_event_execution_states "
                        "WHERE event_id=$1 AND NOT (execution_id = ANY($2::bigint[]))",
                        eid,
                        current_execution_ids,
                    )
                    for state in execution_states:
                        execution_id = int(state.get("execution_id") or 0)
                        if execution_id <= 0:
                            continue
                        source_updated = state.get("source_row_updated_at") or None
                        if isinstance(source_updated, str):
                            try:
                                source_updated = datetime.fromisoformat(
                                    source_updated.replace("Z", "+00:00")
                                )
                                if source_updated.tzinfo is None:
                                    source_updated = source_updated.replace(tzinfo=timezone.utc)
                            except (TypeError, ValueError, OverflowError):
                                source_updated = None
                        await c.execute(
                            """
                            INSERT INTO market_event_execution_states(
                                event_id,execution_id,user_id,entry_state,entry_order_id,
                                entry_requested_qty,entry_filled_qty,entry_remaining_qty,entry_exchange_status,
                                tp_level_index,tp_state,tp_order_id,tp_expected_qty,tp_filled_qty,
                                tp_remaining_qty,tp_exchange_status,zero_exposure,source_row_updated_at,
                                evidence_fingerprint,evidence_json,observed_at,updated_at
                            ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,NOW(),NOW())
                            ON CONFLICT(event_id,execution_id) DO UPDATE SET
                                user_id=EXCLUDED.user_id,entry_state=EXCLUDED.entry_state,
                                entry_order_id=EXCLUDED.entry_order_id,entry_requested_qty=EXCLUDED.entry_requested_qty,
                                entry_filled_qty=EXCLUDED.entry_filled_qty,entry_remaining_qty=EXCLUDED.entry_remaining_qty,
                                entry_exchange_status=EXCLUDED.entry_exchange_status,tp_level_index=EXCLUDED.tp_level_index,
                                tp_state=EXCLUDED.tp_state,tp_order_id=EXCLUDED.tp_order_id,
                                tp_expected_qty=EXCLUDED.tp_expected_qty,tp_filled_qty=EXCLUDED.tp_filled_qty,
                                tp_remaining_qty=EXCLUDED.tp_remaining_qty,tp_exchange_status=EXCLUDED.tp_exchange_status,
                                zero_exposure=EXCLUDED.zero_exposure,source_row_updated_at=EXCLUDED.source_row_updated_at,
                                evidence_fingerprint=EXCLUDED.evidence_fingerprint,evidence_json=EXCLUDED.evidence_json,
                                observed_at=NOW(),updated_at=NOW()
                            """,
                            eid,
                            execution_id,
                            int(state.get("user_id") or 0),
                            str(state.get("entry_state") or "UNKNOWN"),
                            str(state.get("entry_order_id") or "") or None,
                            float(state.get("entry_requested_qty") or 0.0),
                            float(state.get("entry_filled_qty") or 0.0),
                            float(state.get("entry_remaining_qty") or 0.0),
                            str(state.get("entry_exchange_status") or "") or None,
                            int(state.get("tp_level_index") or 1),
                            str(state.get("tp_state") or "UNKNOWN"),
                            str(state.get("tp_order_id") or "") or None,
                            float(state.get("tp_expected_qty") or 0.0),
                            float(state.get("tp_filled_qty") or 0.0),
                            float(state.get("tp_remaining_qty") or 0.0),
                            str(state.get("tp_exchange_status") or "") or None,
                            int(state.get("zero_exposure") or 0),
                            source_updated,
                            fingerprint,
                            str(state.get("evidence_json") or "{}"),
                        )
                    await c.execute(
                        """
                        INSERT INTO market_event_evidence_history(
                            event_id,attempt_type,attempt_number,evidence_fingerprint,
                            evidence_json,decision,reason,worker_id,lease_generation
                        ) VALUES($1,'shadow',$2,$3,$4,$5,$6,$7,$8)
                        """,
                        eid,
                        attempt_number,
                        fingerprint,
                        snapshot_json,
                        decision,
                        reason or None,
                        worker or None,
                        generation,
                    )
                return {
                    "written": True,
                    "changed": changed,
                    "unchanged_evidence_count": unchanged_count,
                }

    async with connect() as c:
        await c.execute("BEGIN IMMEDIATE")
        try:
            cur = await c.execute(
                "SELECT evidence_fingerprint,unchanged_evidence_count,shadow_evaluated_at "
                "FROM market_events WHERE id=?",
                (eid,),
            )
            current = await cur.fetchone()
            if not current:
                await c.rollback()
                return {"written": False, "changed": False, "reason": "event_not_found"}
            current_evaluated = current[2]
            if current_evaluated:
                try:
                    current_evaluated_dt = datetime.fromisoformat(
                        str(current_evaluated).replace("Z", "+00:00")
                    )
                    if current_evaluated_dt.tzinfo is None:
                        current_evaluated_dt = current_evaluated_dt.replace(tzinfo=timezone.utc)
                    if observed_at < current_evaluated_dt.astimezone(timezone.utc):
                        await c.rollback()
                        return {
                            "written": False,
                            "changed": False,
                            "reason": "stale_snapshot",
                        }
                except (TypeError, ValueError, OverflowError):
                    pass
            changed = str(current[0] or "") != fingerprint
            unchanged_count = 0 if changed else int(current[1] or 0) + 1
            if changed:
                cur = await c.execute(
                    """
                    UPDATE market_events
                    SET phase=CASE WHEN phase='LEGACY' THEN 'SHADOW_OBSERVATION' ELSE phase END,
                        evidence_fingerprint=?,evidence_snapshot_json=?,shadow_decision=?,shadow_reason=?,
                        shadow_evaluated_at=?,shadow_version=1,
                        unchanged_evidence_count=0,last_exchange_change_at=?
                    WHERE id=?
                    """,
                    (
                        fingerprint,
                        event_snapshot_json,
                        decision,
                        reason,
                        observed_at.isoformat(),
                        observed_at.isoformat(),
                        eid,
                    ),
                )
            else:
                cur = await c.execute(
                    """
                    UPDATE market_events
                    SET phase=CASE WHEN phase='LEGACY' THEN 'SHADOW_OBSERVATION' ELSE phase END,
                        shadow_evaluated_at=?,unchanged_evidence_count=?
                    WHERE id=?
                    """,
                    (observed_at.isoformat(), unchanged_count, eid),
                )
            if int(cur.rowcount or 0) != 1:
                await c.rollback()
                return {"written": False, "changed": False, "reason": "event_update_failed"}

            if changed:
                current_execution_ids = sorted(
                    {
                        int(state.get("execution_id") or 0)
                        for state in execution_states
                        if int(state.get("execution_id") or 0) > 0
                    }
                )
                # Keep the normalized state table an exact projection of the
                # current snapshot instead of retaining removed executions.
                if current_execution_ids:
                    placeholders = ",".join("?" for _ in current_execution_ids)
                    await c.execute(
                        f"DELETE FROM market_event_execution_states "
                        f"WHERE event_id=? AND execution_id NOT IN ({placeholders})",
                        (eid, *current_execution_ids),
                    )
                else:
                    await c.execute(
                        "DELETE FROM market_event_execution_states WHERE event_id=?",
                        (eid,),
                    )
                for state in execution_states:
                    execution_id = int(state.get("execution_id") or 0)
                    if execution_id <= 0:
                        continue
                    await c.execute(
                        """
                        INSERT INTO market_event_execution_states(
                            event_id,execution_id,user_id,entry_state,entry_order_id,
                            entry_requested_qty,entry_filled_qty,entry_remaining_qty,entry_exchange_status,
                            tp_level_index,tp_state,tp_order_id,tp_expected_qty,tp_filled_qty,
                            tp_remaining_qty,tp_exchange_status,zero_exposure,source_row_updated_at,
                            evidence_fingerprint,evidence_json,observed_at,updated_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
                        ON CONFLICT(event_id,execution_id) DO UPDATE SET
                            user_id=excluded.user_id,entry_state=excluded.entry_state,
                            entry_order_id=excluded.entry_order_id,entry_requested_qty=excluded.entry_requested_qty,
                            entry_filled_qty=excluded.entry_filled_qty,entry_remaining_qty=excluded.entry_remaining_qty,
                            entry_exchange_status=excluded.entry_exchange_status,tp_level_index=excluded.tp_level_index,
                            tp_state=excluded.tp_state,tp_order_id=excluded.tp_order_id,
                            tp_expected_qty=excluded.tp_expected_qty,tp_filled_qty=excluded.tp_filled_qty,
                            tp_remaining_qty=excluded.tp_remaining_qty,tp_exchange_status=excluded.tp_exchange_status,
                            zero_exposure=excluded.zero_exposure,source_row_updated_at=excluded.source_row_updated_at,
                            evidence_fingerprint=excluded.evidence_fingerprint,evidence_json=excluded.evidence_json,
                            observed_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
                        """,
                        (
                            eid,
                            execution_id,
                            int(state.get("user_id") or 0),
                            str(state.get("entry_state") or "UNKNOWN"),
                            str(state.get("entry_order_id") or "") or None,
                            float(state.get("entry_requested_qty") or 0.0),
                            float(state.get("entry_filled_qty") or 0.0),
                            float(state.get("entry_remaining_qty") or 0.0),
                            str(state.get("entry_exchange_status") or "") or None,
                            int(state.get("tp_level_index") or 1),
                            str(state.get("tp_state") or "UNKNOWN"),
                            str(state.get("tp_order_id") or "") or None,
                            float(state.get("tp_expected_qty") or 0.0),
                            float(state.get("tp_filled_qty") or 0.0),
                            float(state.get("tp_remaining_qty") or 0.0),
                            str(state.get("tp_exchange_status") or "") or None,
                            int(state.get("zero_exposure") or 0),
                            str(state.get("source_row_updated_at") or "") or None,
                            fingerprint,
                            str(state.get("evidence_json") or "{}"),
                        ),
                    )
                await c.execute(
                    """
                    INSERT INTO market_event_evidence_history(
                        event_id,attempt_type,attempt_number,evidence_fingerprint,
                        evidence_json,decision,reason,worker_id,lease_generation
                    ) VALUES(?,'shadow',?,?,?,?,?,?,?)
                    """,
                    (
                        eid,
                        attempt_number,
                        fingerprint,
                        snapshot_json,
                        decision,
                        reason or None,
                        worker or None,
                        generation,
                    ),
                )
            await c.commit()
            return {
                "written": True,
                "changed": changed,
                "unchanged_evidence_count": unchanged_count,
            }
        except Exception:
            await c.rollback()
            raise


async def market_event_shadow_evidence(event_id: int) -> Dict[str, Any]:
    """Return current Step-1 shadow state plus per-execution evidence."""

    eid = int(event_id or 0)
    if eid <= 0:
        return {}
    if is_postgres():
        async with connect() as c:
            event = await c.fetchrow("SELECT * FROM market_events WHERE id=$1", eid)
            states = await c.fetch(
                "SELECT * FROM market_event_execution_states WHERE event_id=$1 ORDER BY user_id,execution_id",
                eid,
            )
            history = await c.fetch(
                "SELECT * FROM market_event_evidence_history WHERE event_id=$1 ORDER BY id",
                eid,
            )
            return {
                "event": _dict(event),
                "execution_states": [_dict(row) for row in states],
                "history": [_dict(row) for row in history],
            }
    async with connect() as c:
        cur = await c.execute("SELECT * FROM market_events WHERE id=?", (eid,))
        event = _dict(await cur.fetchone())
        cur = await c.execute(
            "SELECT * FROM market_event_execution_states WHERE event_id=? ORDER BY user_id,execution_id",
            (eid,),
        )
        states = [_dict(row) for row in await cur.fetchall()]
        cur = await c.execute(
            "SELECT * FROM market_event_evidence_history WHERE event_id=? ORDER BY id",
            (eid,),
        )
        history = [_dict(row) for row in await cur.fetchall()]
        return {"event": event, "execution_states": states, "history": history}


async def close_inactive_trade_groups() -> int:
    """Close plans once no trackable user execution remains."""
    statuses = list(_GROUP_ACTIVE_EXECUTION_STATUSES)
    if is_postgres():
        async with connect() as c:
            result = await c.execute(
                """
                UPDATE trade_groups g
                SET status='closed',updated_at=NOW()
                WHERE g.status='active'
                  AND NOT EXISTS (
                    SELECT 1 FROM trade_executions e
                    WHERE e.trade_group_id=g.id AND e.status = ANY($1::text[])
                  )
                """,
                statuses,
            )
            try:
                return int(str(result).split()[-1])
            except Exception:
                return 0
    placeholders = ",".join(["?"] * len(statuses))
    async with connect() as c:
        cur = await c.execute(
            f"""
            UPDATE trade_groups
            SET status='closed',updated_at=CURRENT_TIMESTAMP
            WHERE status='active'
              AND NOT EXISTS (
                SELECT 1 FROM trade_executions e
                WHERE e.trade_group_id=trade_groups.id
                  AND e.status IN ({placeholders})
              )
            """,
            statuses,
        )
        await c.commit()
        return max(0, int(cur.rowcount or 0))


async def pending_limit_executions_for_user(
    user_id: int, *, limit: int = 100
) -> List[Dict[str, Any]]:
    """Return this user's explicit-BingX active LIMIT executions, newest first."""
    wanted = max(1, min(int(limit or 100), 500))
    page_size = max(200, min(1000, wanted * 2))
    cursor_id = 0
    result: list[Dict[str, Any]] = []

    while len(result) < wanted:
        if is_postgres():
            async with connect() as c:
                raw_rows = await c.fetch(
                    """SELECT * FROM trade_executions
                    WHERE user_id=$1 AND status='pending_limit'
                      AND ($2::bigint=0 OR id<$2)
                    ORDER BY id DESC LIMIT $3""",
                    int(user_id),
                    cursor_id,
                    page_size,
                )
                raw = [_dict(row) for row in raw_rows]
        else:
            async with connect() as c:
                cur = await c.execute(
                    """SELECT * FROM trade_executions
                    WHERE user_id=? AND status='pending_limit'
                      AND (?=0 OR id<?)
                    ORDER BY id DESC LIMIT ?""",
                    (int(user_id), cursor_id, cursor_id, page_size),
                )
                raw = [_dict(row) for row in await cur.fetchall()]

        if not raw:
            break
        result.extend(_bingx_rows(raw))
        next_cursor = min(int(row.get("id") or 0) for row in raw)
        if next_cursor <= 0 or next_cursor == cursor_id or len(raw) < page_size:
            break
        cursor_id = next_cursor

    return result[:wanted]


async def apply_limit_policy_to_pending(
    user_id: int,
    *,
    ttl_hours: int,
    tp_mode: str,
    preset: str = "custom",
    execution_ids: list[int] | None = None,
) -> int:
    """Replace policy snapshots for current pending entries after confirmation.

    The TP threshold is rebuilt for each row because users may have LIMITs with
    different target counts. Exchange actions are left to the normal monitor,
    which re-checks order state/dealVol and confirms every cancellation.
    """
    from app.services.limit_policy import POLICY_KEY, build_policy

    rows = await pending_limit_executions_for_user(int(user_id), limit=500)
    allowed_ids = (
        {int(x) for x in execution_ids if int(x) > 0}
        if execution_ids is not None
        else None
    )
    changed = 0
    for row in rows:
        execution_id = int(row.get("id") or 0)
        if not execution_id:
            continue
        if allowed_ids is not None and execution_id not in allowed_ids:
            continue
        try:
            targets = json.loads(row.get("targets_json") or "[]")
        except (TypeError, ValueError):
            targets = []
        policy = build_policy(
            ttl_hours=ttl_hours,
            tp_mode=tp_mode,
            targets=targets,
            preset=preset,
        )
        async with execution_lock(execution_id):
            latest = await get_execution_by_id(execution_id)
            if not latest or str(latest.get("status") or "") != "pending_limit":
                continue
            # Applying a menu policy is a metadata-only operation. Never
            # rewrite a possibly newer execution status/reason from a stale
            # pending row during Railway process overlap.
            saved = await merge_execution_metadata(
                execution_id,
                {POLICY_KEY: policy, "limit_cancel_pending": None},
                expected_status="pending_limit",
            )
            if saved:
                changed += 1
    return changed


async def set_user_limit_policy(
    user_id: int, *, ttl_hours: int, tp_mode: str, preset: str = "custom"
) -> None:
    """Atomically save all three per-user stale-LIMIT settings."""
    from app.services.limit_policy import normalize_ttl_hours, normalize_tp_mode

    ttl = normalize_ttl_hours(ttl_hours)
    mode = normalize_tp_mode(tp_mode)
    preset_value = str(preset or "custom").strip().lower()[:32] or "custom"
    await ensure_user(int(user_id))
    if is_postgres():
        async with connect() as c:
            await c.execute(
                """
                UPDATE user_settings
                SET limit_ttl_hours=$1,
                    limit_tp_invalidation_mode=$2,
                    limit_policy_preset=$3
                WHERE user_id=$4
                """,
                ttl,
                mode,
                preset_value,
                int(user_id),
            )
    else:
        async with connect() as c:
            await c.execute(
                """
                UPDATE user_settings
                SET limit_ttl_hours=?,
                    limit_tp_invalidation_mode=?,
                    limit_policy_preset=?
                WHERE user_id=?
                """,
                (ttl, mode, preset_value, int(user_id)),
            )
            await c.commit()
    try:
        from app.services.ttl_cache import invalidate_user

        invalidate_user(int(user_id))
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Deferred financial reconciliation storage (g5b3g2 / step 2)
# ---------------------------------------------------------------------------


def _financial_identifier(value: Any, *, field: str, max_length: int = 160) -> str:
    text = str(value or "").strip().replace("\x00", "")
    if not text:
        raise ValueError(f"{field} is required")
    if len(text) > max_length:
        raise ValueError(f"{field} is too long")
    if any(char in text for char in ("\r", "\n")):
        raise ValueError(f"{field} contains unsupported control characters")
    return text


def _financial_error_text(value: Any) -> str:
    return str(value or "").replace("\x00", "")[:1000]


def _financial_pg_decimal(value: str | None) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def _financial_pg_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _financial_default_deadline(*, seconds: float) -> str:
    bounded = max(30.0, min(float(seconds or 900.0), 86_400.0))
    return (datetime.now(timezone.utc) + timedelta(seconds=bounded)).isoformat()


def _financial_job_dedup_key(execution_id: int) -> str:
    return f"execution:{int(execution_id)}:financial_final:v1"


def _financial_validate_order_expectation_row(
    item: FinancialOrderExpectation, row: Dict[str, Any]
) -> None:
    if (
        str(row.get("role") or "") != item.role
        or int(row.get("tp_index") or 0) != item.tp_index
    ):
        raise ValueError(
            f"conflicting durable financial order identity: {item.order_key}"
        )
    stored_exchange = optional_text(row.get("exchange_order_id"), max_length=128)
    stored_client = optional_text(row.get("client_order_id"), max_length=128)
    if item.exchange_order_id and stored_exchange and item.exchange_order_id != stored_exchange:
        raise ValueError("financial order identity has conflicting exchange_order_id")
    if item.client_order_id and stored_client and item.client_order_id != stored_client:
        raise ValueError("financial order identity has conflicting client_order_id")
    stored_qty = row.get("expected_qty")
    if item.expected_qty is not None and stored_qty not in (None, ""):
        if Decimal(item.expected_qty) != Decimal(str(stored_qty)):
            raise ValueError("financial order identity has conflicting expected_qty")


async def enqueue_financial_reconciliation_job(
    *,
    execution_id: int,
    user_id: int,
    exchange: str,
    symbol: str,
    side: str,
    close_type: str,
    strategy_gross_pnl: Any,
    order_expectations: List[Dict[str, Any]] | List[FinancialOrderExpectation],
    terminal_at: Any = None,
    deadline_after_sec: float = 900.0,
) -> Dict[str, Any]:
    """Create one durable, idempotent reconciliation job per execution.

    Repeated lifecycle passes may safely call this function. A terminal financial
    result is never resurrected to ``pending``. New exact order identities may be
    appended while preserving existing confirmed rows.
    """

    execution_id = int(execution_id)
    user_id = int(user_id)
    if execution_id <= 0 or user_id <= 0:
        raise ValueError("execution_id and user_id must be positive")
    exchange_value = _financial_identifier(
        exchange or "bingx", field="exchange", max_length=32
    ).lower()
    symbol_value = _financial_identifier(symbol, field="symbol", max_length=64).upper()
    side_value = normalize_side(side)
    close_type_value = normalize_close_type(close_type)
    gross_text = str(decimal_text(strategy_gross_pnl, field="strategy_gross_pnl"))
    terminal_text = normalize_datetime_text(
        terminal_at or datetime.now(timezone.utc),
        field="terminal_at",
        allow_none=False,
    )
    deadline_text = _financial_default_deadline(seconds=deadline_after_sec)
    expectations = normalize_order_expectations(order_expectations or [])
    notification_key = _financial_job_dedup_key(execution_id)

    if is_postgres():
        async with connect() as c:
            async with c.transaction():
                row = await c.fetchrow(
                    """
                    INSERT INTO financial_reconciliation_jobs(
                        execution_id,user_id,exchange,symbol,side,close_type,status,
                        strategy_gross_pnl,next_attempt_at,deadline_at,terminal_at,
                        notification_dedup_key,notification_status,updated_at
                    ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,NOW(),$9,$10,$11,'pending',NOW())
                    ON CONFLICT(execution_id) DO UPDATE SET
                        strategy_gross_pnl=CASE
                            WHEN financial_reconciliation_jobs.status = ANY($12::text[])
                                THEN EXCLUDED.strategy_gross_pnl
                            ELSE financial_reconciliation_jobs.strategy_gross_pnl
                        END,
                        deadline_at=COALESCE(
                            financial_reconciliation_jobs.deadline_at,
                            EXCLUDED.deadline_at
                        ),
                        terminal_at=COALESCE(
                            financial_reconciliation_jobs.terminal_at,
                            EXCLUDED.terminal_at
                        ),
                        updated_at=NOW()
                    RETURNING *
                    """,
                    execution_id,
                    user_id,
                    exchange_value,
                    symbol_value,
                    side_value,
                    close_type_value,
                    FINANCIAL_STATUS_PENDING,
                    Decimal(gross_text),
                    _financial_pg_datetime(deadline_text),
                    _financial_pg_datetime(terminal_text),
                    notification_key,
                    list(FINANCIAL_ACTIVE_STATUSES),
                )
                if not row:
                    raise RuntimeError("failed to create financial reconciliation job")
                if (
                    int(row["user_id"]) != user_id
                    or str(row["exchange"]).lower() != exchange_value
                    or str(row["symbol"]).upper() != symbol_value
                    or str(row["side"]).lower() != side_value
                    or str(row["close_type"]).lower() != close_type_value
                ):
                    raise ValueError("execution_id already owns a different financial job")
                job_id = int(row["id"])
                if str(row["status"]) in FINANCIAL_TERMINAL_STATUSES:
                    return _dict(row)

                for item in expectations:
                    stored = await c.fetchrow(
                        """
                        SELECT * FROM financial_reconciliation_orders
                        WHERE job_id=$1 AND (
                            order_key=$2
                            OR ($3::text IS NOT NULL AND client_order_id=$3)
                            OR ($4::text IS NOT NULL AND exchange_order_id=$4)
                        )
                        ORDER BY CASE WHEN order_key=$2 THEN 0 ELSE 1 END,id
                        LIMIT 1 FOR UPDATE
                        """,
                        job_id,
                        item.order_key,
                        item.client_order_id,
                        item.exchange_order_id,
                    )
                    if stored:
                        stored_dict = _dict(stored)
                        _financial_validate_order_expectation_row(item, stored_dict)
                        desired_key = str(stored["order_key"])
                        if item.exchange_order_id and not stored["exchange_order_id"]:
                            desired_key = f"order:{item.exchange_order_id}"
                            collision = await c.fetchrow(
                                """
                                SELECT id FROM financial_reconciliation_orders
                                WHERE job_id=$1 AND order_key=$2 AND id<>$3
                                """,
                                job_id,
                                desired_key,
                                int(stored["id"]),
                            )
                            if collision:
                                raise ValueError(
                                    "exchange order identity already belongs to another expectation"
                                )
                        await c.execute(
                            """
                            UPDATE financial_reconciliation_orders
                            SET order_key=$1,
                                exchange_order_id=COALESCE(exchange_order_id,$2),
                                client_order_id=COALESCE(client_order_id,$3),
                                required=GREATEST(required,$4),
                                expected_qty=COALESCE(expected_qty,$5),
                                metadata_json=CASE
                                    WHEN metadata_json='{}' THEN $6
                                    ELSE metadata_json
                                END,
                                updated_at=NOW()
                            WHERE id=$7
                            """,
                            desired_key,
                            item.exchange_order_id,
                            item.client_order_id,
                            1 if item.required else 0,
                            _financial_pg_decimal(item.expected_qty),
                            item.metadata_json,
                            int(stored["id"]),
                        )
                    else:
                        await c.execute(
                            """
                            INSERT INTO financial_reconciliation_orders(
                                job_id,execution_id,order_key,exchange_order_id,
                                client_order_id,role,tp_index,required,expected_qty,
                                status,metadata_json,updated_at
                            ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'expected',$10,NOW())
                            """,
                            job_id,
                            execution_id,
                            item.order_key,
                            item.exchange_order_id,
                            item.client_order_id,
                            item.role,
                            item.tp_index,
                            1 if item.required else 0,
                            _financial_pg_decimal(item.expected_qty),
                            item.metadata_json,
                        )
                await c.execute(
                    """
                    UPDATE financial_reconciliation_jobs j
                    SET expected_order_count=(
                            SELECT COUNT(*) FROM financial_reconciliation_orders o
                            WHERE o.job_id=j.id AND o.required=1
                        ),
                        updated_at=NOW()
                    WHERE j.id=$1
                    """,
                    job_id,
                )
                final_row = await c.fetchrow(
                    "SELECT * FROM financial_reconciliation_jobs WHERE id=$1",
                    job_id,
                )
                return _dict(final_row)

    async with connect() as c:
        await c.execute("BEGIN IMMEDIATE")
        try:
            await c.execute(
                """
                INSERT INTO financial_reconciliation_jobs(
                    execution_id,user_id,exchange,symbol,side,close_type,status,
                    strategy_gross_pnl,next_attempt_at,deadline_at,terminal_at,
                    notification_dedup_key,notification_status,updated_at
                ) VALUES(?,?,?,?,?,?,?, ?,CURRENT_TIMESTAMP,?,?,?,'pending',CURRENT_TIMESTAMP)
                ON CONFLICT(execution_id) DO UPDATE SET
                    strategy_gross_pnl=CASE
                        WHEN financial_reconciliation_jobs.status IN ('pending','processing')
                            THEN excluded.strategy_gross_pnl
                        ELSE financial_reconciliation_jobs.strategy_gross_pnl
                    END,
                    deadline_at=COALESCE(
                        financial_reconciliation_jobs.deadline_at,
                        excluded.deadline_at
                    ),
                    terminal_at=COALESCE(
                        financial_reconciliation_jobs.terminal_at,
                        excluded.terminal_at
                    ),
                    updated_at=CURRENT_TIMESTAMP
                """,
                (
                    execution_id,
                    user_id,
                    exchange_value,
                    symbol_value,
                    side_value,
                    close_type_value,
                    FINANCIAL_STATUS_PENDING,
                    gross_text,
                    deadline_text,
                    terminal_text,
                    notification_key,
                ),
            )
            cur = await c.execute(
                "SELECT * FROM financial_reconciliation_jobs WHERE execution_id=?",
                (execution_id,),
            )
            row = await cur.fetchone()
            if not row:
                raise RuntimeError("failed to create financial reconciliation job")
            if (
                int(row["user_id"]) != user_id
                or str(row["exchange"]).lower() != exchange_value
                or str(row["symbol"]).upper() != symbol_value
                or str(row["side"]).lower() != side_value
                or str(row["close_type"]).lower() != close_type_value
            ):
                raise ValueError("execution_id already owns a different financial job")
            job_id = int(row["id"])
            if str(row["status"]) in FINANCIAL_TERMINAL_STATUSES:
                await c.commit()
                return _dict(row)

            for item in expectations:
                cur = await c.execute(
                    """
                    SELECT * FROM financial_reconciliation_orders
                    WHERE job_id=? AND (
                        order_key=?
                        OR (? IS NOT NULL AND client_order_id=?)
                        OR (? IS NOT NULL AND exchange_order_id=?)
                    )
                    ORDER BY CASE WHEN order_key=? THEN 0 ELSE 1 END,id
                    LIMIT 1
                    """,
                    (
                        job_id,
                        item.order_key,
                        item.client_order_id,
                        item.client_order_id,
                        item.exchange_order_id,
                        item.exchange_order_id,
                        item.order_key,
                    ),
                )
                stored = await cur.fetchone()
                if stored:
                    stored_dict = _dict(stored)
                    _financial_validate_order_expectation_row(item, stored_dict)
                    desired_key = str(stored["order_key"])
                    if item.exchange_order_id and not stored["exchange_order_id"]:
                        desired_key = f"order:{item.exchange_order_id}"
                        cur = await c.execute(
                            """
                            SELECT id FROM financial_reconciliation_orders
                            WHERE job_id=? AND order_key=? AND id<>?
                            """,
                            (job_id, desired_key, int(stored["id"])),
                        )
                        if await cur.fetchone():
                            raise ValueError(
                                "exchange order identity already belongs to another expectation"
                            )
                    await c.execute(
                        """
                        UPDATE financial_reconciliation_orders
                        SET order_key=?,
                            exchange_order_id=COALESCE(exchange_order_id,?),
                            client_order_id=COALESCE(client_order_id,?),
                            required=MAX(required,?),
                            expected_qty=COALESCE(expected_qty,?),
                            metadata_json=CASE
                                WHEN metadata_json='{}' THEN ?
                                ELSE metadata_json
                            END,
                            updated_at=CURRENT_TIMESTAMP
                        WHERE id=?
                        """,
                        (
                            desired_key,
                            item.exchange_order_id,
                            item.client_order_id,
                            1 if item.required else 0,
                            item.expected_qty,
                            item.metadata_json,
                            int(stored["id"]),
                        ),
                    )
                else:
                    await c.execute(
                        """
                        INSERT INTO financial_reconciliation_orders(
                            job_id,execution_id,order_key,exchange_order_id,
                            client_order_id,role,tp_index,required,expected_qty,
                            status,metadata_json,updated_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,'expected',?,CURRENT_TIMESTAMP)
                        """,
                        (
                            job_id,
                            execution_id,
                            item.order_key,
                            item.exchange_order_id,
                            item.client_order_id,
                            item.role,
                            item.tp_index,
                            1 if item.required else 0,
                            item.expected_qty,
                            item.metadata_json,
                        ),
                    )
            await c.execute(
                """
                UPDATE financial_reconciliation_jobs
                SET expected_order_count=(
                        SELECT COUNT(*) FROM financial_reconciliation_orders
                        WHERE job_id=? AND required=1
                    ),
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (job_id, job_id),
            )
            cur = await c.execute(
                "SELECT * FROM financial_reconciliation_jobs WHERE id=?",
                (job_id,),
            )
            final_row = await cur.fetchone()
            await c.commit()
            return _dict(final_row)
        except BaseException:
            await c.rollback()
            raise


async def pending_financial_reconciliation_enqueue_rows(
    *, limit: int = 20
) -> List[Dict[str, Any]]:
    """Return terminal executions whose durable enqueue marker is still due.

    The marker is written into ``exchange_order_ids_json`` before the separate
    financial job insert.  This bounded scan recovers the narrow crash window
    without touching historical executions that do not carry the versioned
    marker.  Exact due/state validation is repeated by the enqueue service.
    """

    bounded = max(1, min(int(limit or 20), 100))
    batch_size = max(100, bounded * 4)
    marker_like = '%"financial_reconciliation_enqueue_v1"%'

    def _is_due_runnable(row: dict[str, Any], *, now: datetime) -> bool:
        raw = row.get("exchange_order_ids_json")
        try:
            payload = json.loads(raw or "{}") if isinstance(raw, str) else dict(raw or {})
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
        marker = payload.get("financial_reconciliation_enqueue_v1")
        if not isinstance(marker, dict) or str(marker.get("state") or "") not in {
            "ready",
            "retry",
        }:
            return False
        due_raw = marker.get("next_attempt_at")
        if due_raw in (None, ""):
            return True
        try:
            due = datetime.fromisoformat(str(due_raw).replace("Z", "+00:00"))
        except (TypeError, ValueError, OverflowError):
            # The enqueue service owns final validation.  Returning malformed
            # runnable markers lets it durably convert them into a retry marker
            # instead of hiding them forever in this recovery scan.
            return True
        if due.tzinfo is None:
            due = due.replace(tzinfo=timezone.utc)
        return due.astimezone(timezone.utc) <= now

    # Do not apply the caller's limit before parsing marker state.  A first page
    # full of permanent ``blocked`` markers (or future retries) would otherwise
    # starve every newer ready marker forever.  Page by execution id and retain
    # only runnable markers while keeping memory bounded to one small page.
    now = datetime.now(timezone.utc)
    selected: list[dict[str, Any]] = []
    cursor = 0
    if is_postgres():
        async with connect() as c:
            while len(selected) < bounded:
                rows = await c.fetch(
                    """
                    SELECT e.*
                    FROM trade_executions e
                    LEFT JOIN financial_reconciliation_jobs j
                      ON j.execution_id=e.id
                    WHERE e.status='closed_on_exchange_cleanup'
                      AND j.id IS NULL
                      AND e.id>$1
                      AND COALESCE(e.exchange_order_ids_json,'{}') LIKE $2
                    ORDER BY e.id ASC
                    LIMIT $3
                    """,
                    cursor,
                    marker_like,
                    batch_size,
                )
                if not rows:
                    break
                page = [_dict(row) for row in rows]
                cursor = int(page[-1]["id"])
                selected.extend(
                    row for row in page if _is_due_runnable(row, now=now)
                )
                if len(page) < batch_size:
                    break
            return selected[:bounded]
    async with connect() as c:
        while len(selected) < bounded:
            cur = await c.execute(
                """
                SELECT e.*
                FROM trade_executions e
                LEFT JOIN financial_reconciliation_jobs j
                  ON j.execution_id=e.id
                WHERE e.status='closed_on_exchange_cleanup'
                  AND j.id IS NULL
                  AND e.id>?
                  AND COALESCE(e.exchange_order_ids_json,'{}') LIKE ?
                ORDER BY e.id ASC
                LIMIT ?
                """,
                (cursor, marker_like, batch_size),
            )
            page = [_dict(row) for row in await cur.fetchall()]
            if not page:
                break
            cursor = int(page[-1]["id"])
            selected.extend(row for row in page if _is_due_runnable(row, now=now))
            if len(page) < batch_size:
                break
        return selected[:bounded]


async def get_financial_reconciliation_job(
    *, execution_id: int | None = None, job_id: int | None = None
) -> Dict[str, Any] | None:
    if execution_id is None and job_id is None:
        raise ValueError("execution_id or job_id is required")
    if is_postgres():
        async with connect() as c:
            if job_id is not None:
                row = await c.fetchrow(
                    "SELECT * FROM financial_reconciliation_jobs WHERE id=$1",
                    int(job_id),
                )
            else:
                row = await c.fetchrow(
                    "SELECT * FROM financial_reconciliation_jobs WHERE execution_id=$1",
                    int(execution_id or 0),
                )
            return _dict(row) if row else None
    async with connect() as c:
        if job_id is not None:
            cur = await c.execute(
                "SELECT * FROM financial_reconciliation_jobs WHERE id=?",
                (int(job_id),),
            )
        else:
            cur = await c.execute(
                "SELECT * FROM financial_reconciliation_jobs WHERE execution_id=?",
                (int(execution_id or 0),),
            )
        row = await cur.fetchone()
        return _dict(row) if row else None


async def list_financial_reconciliation_orders(job_id: int) -> List[Dict[str, Any]]:
    if is_postgres():
        async with connect() as c:
            rows = await c.fetch(
                """
                SELECT * FROM financial_reconciliation_orders
                WHERE job_id=$1 ORDER BY role,tp_index,id
                """,
                int(job_id),
            )
            return [_dict(row) for row in rows]
    async with connect() as c:
        cur = await c.execute(
            """
            SELECT * FROM financial_reconciliation_orders
            WHERE job_id=? ORDER BY role,tp_index,id
            """,
            (int(job_id),),
        )
        return [_dict(row) for row in await cur.fetchall()]


async def list_financial_reconciliation_fills(job_id: int) -> List[Dict[str, Any]]:
    if is_postgres():
        async with connect() as c:
            rows = await c.fetch(
                """
                SELECT * FROM financial_reconciliation_fills
                WHERE job_id=$1 ORDER BY fill_time,id
                """,
                int(job_id),
            )
            return [_dict(row) for row in rows]
    async with connect() as c:
        cur = await c.execute(
            """
            SELECT * FROM financial_reconciliation_fills
            WHERE job_id=? ORDER BY datetime(fill_time),id
            """,
            (int(job_id),),
        )
        return [_dict(row) for row in await cur.fetchall()]


async def claim_due_financial_reconciliation_jobs(
    *, limit: int = 1, stale_after_sec: float = 120.0
) -> List[Dict[str, Any]]:
    """Atomically lease due jobs; abandoned processing jobs are recoverable."""

    bounded_limit = max(1, min(int(limit or 1), 50))
    stale_seconds = max(30.0, min(float(stale_after_sec or 120.0), 3600.0))
    claimed: list[Dict[str, Any]] = []
    if is_postgres():
        async with connect() as c:
            async with c.transaction():
                rows = await c.fetch(
                    """
                    SELECT * FROM financial_reconciliation_jobs
                    WHERE (
                        (status='pending' AND next_attempt_at <= NOW())
                        OR (
                            status='processing'
                            AND processing_started_at < NOW() - ($2::double precision * INTERVAL '1 second')
                        )
                    )
                    ORDER BY next_attempt_at,id
                    FOR UPDATE SKIP LOCKED
                    LIMIT $1
                    """,
                    bounded_limit,
                    stale_seconds,
                )
                for row in rows:
                    lease_token = uuid.uuid4().hex
                    updated = await c.fetchrow(
                        """
                        UPDATE financial_reconciliation_jobs
                        SET status='processing',lease_token=$1,
                            processing_started_at=NOW(),attempts=attempts+1,
                            updated_at=NOW()
                        WHERE id=$2
                        RETURNING *
                        """,
                        lease_token,
                        int(row["id"]),
                    )
                    if updated:
                        claimed.append(_dict(updated))
        return claimed

    async with connect() as c:
        await c.execute("BEGIN IMMEDIATE")
        try:
            cur = await c.execute(
                """
                SELECT * FROM financial_reconciliation_jobs
                WHERE (
                    (status='pending' AND datetime(next_attempt_at) <= datetime('now'))
                    OR (
                        status='processing'
                        AND datetime(processing_started_at) < datetime('now', ?)
                    )
                )
                ORDER BY datetime(next_attempt_at),id
                LIMIT ?
                """,
                (f"-{int(stale_seconds)} seconds", bounded_limit),
            )
            rows = await cur.fetchall()
            for row in rows:
                lease_token = uuid.uuid4().hex
                await c.execute(
                    """
                    UPDATE financial_reconciliation_jobs
                    SET status='processing',lease_token=?,
                        processing_started_at=CURRENT_TIMESTAMP,
                        attempts=attempts+1,updated_at=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (lease_token, int(row["id"])),
                )
                cur = await c.execute(
                    "SELECT * FROM financial_reconciliation_jobs WHERE id=?",
                    (int(row["id"]),),
                )
                updated = await cur.fetchone()
                if updated:
                    claimed.append(_dict(updated))
            await c.commit()
            return claimed
        except BaseException:
            await c.rollback()
            raise


async def reschedule_financial_reconciliation_job(
    *,
    job_id: int,
    lease_token: str,
    retry_after_sec: float,
    error: str = "",
) -> bool:
    delay = max(1.0, min(float(retry_after_sec or 1.0), 86_400.0))
    token = _financial_identifier(lease_token, field="lease_token", max_length=128)
    safe_error = _financial_error_text(error)
    if is_postgres():
        async with connect() as c:
            row = await c.fetchrow(
                """
                UPDATE financial_reconciliation_jobs
                SET status='pending',
                    next_attempt_at=NOW()+($1::double precision * INTERVAL '1 second'),
                    processing_started_at=NULL,lease_token=NULL,last_error=$2,
                    updated_at=NOW()
                WHERE id=$3 AND status='processing' AND lease_token=$4
                RETURNING id
                """,
                delay,
                safe_error,
                int(job_id),
                token,
            )
            return bool(row)
    async with connect() as c:
        cur = await c.execute(
            """
            UPDATE financial_reconciliation_jobs
            SET status='pending',
                next_attempt_at=datetime('now', ?),
                processing_started_at=NULL,lease_token=NULL,last_error=?,
                updated_at=CURRENT_TIMESTAMP
            WHERE id=? AND status='processing' AND lease_token=?
            """,
            (f"+{int(math.ceil(delay))} seconds", safe_error, int(job_id), token),
        )
        await c.commit()
        return int(cur.rowcount or 0) == 1


def _financial_fill_from_db_row(row: Dict[str, Any]) -> FinancialFillRecord:
    return FinancialFillRecord.from_mapping(
        {
            "trade_id": row.get("trade_id"),
            "order_id": row.get("order_id"),
            "role": row.get("role"),
            "tp_index": row.get("tp_index"),
            "symbol": row.get("symbol"),
            "side": row.get("side"),
            "price": row.get("price"),
            "qty": row.get("qty"),
            "realized_pnl": row.get("realized_pnl"),
            "fee": row.get("fee"),
            "fee_asset": row.get("fee_asset"),
            "fill_time": row.get("fill_time"),
            "metadata_json": row.get("metadata_json"),
        }
    )


def _financial_order_fill_evaluation(
    order: Dict[str, Any],
    matched: List[FinancialFillRecord],
) -> tuple[str, str | None, Decimal, Decimal, list[str]]:
    """Evaluate whether all fills for one expected order are complete and owned.

    Exact order identity alone is not enough: the role/TP index must match and,
    when an expected quantity is known, all partial fills must add up exactly.
    """

    order_fee = sum((Decimal(fill.fee) for fill in matched), Decimal("0"))
    order_qty = sum((Decimal(fill.qty) for fill in matched), Decimal("0"))
    expectation = FinancialOrderExpectation.from_mapping(
        {
            "exchange_order_id": order.get("exchange_order_id"),
            "client_order_id": order.get("client_order_id"),
            "role": order.get("role"),
            "tp_index": order.get("tp_index"),
            "required": bool(order.get("required", True)),
            "expected_qty": order.get("expected_qty"),
            "metadata_json": order.get("metadata_json"),
        }
    )
    conflicts: list[str] = []
    for fill in matched:
        if not fill_matches_expectation(fill, expectation):
            conflicts.append(f"trade_id={fill.trade_id}:role_or_tp_mismatch")
    if conflicts:
        return ORDER_STATUS_AMBIGUOUS, "role_or_tp_mismatch", order_qty, order_fee, conflicts

    expected_qty_raw = order.get("expected_qty")
    if expected_qty_raw in (None, ""):
        # Exact orderId history may still be delayed. Without the durable order
        # quantity we cannot prove that every partial fill (and therefore every
        # fee row) has arrived, so a worker must never confirm this order.
        return (
            ORDER_STATUS_EXPECTED,
            "expected_qty_missing",
            order_qty,
            order_fee,
            conflicts,
        )
    if expected_qty_raw not in (None, ""):
        expected_qty = Decimal(str(expected_qty_raw))
        if order_qty > expected_qty:
            conflicts.append(
                f"order_key={expectation.order_key}:fill_qty_exceeds_expected"
            )
            return (
                ORDER_STATUS_AMBIGUOUS,
                "fill_qty_exceeds_expected",
                order_qty,
                order_fee,
                conflicts,
            )
        if order_qty < expected_qty:
            return (
                ORDER_STATUS_EXPECTED,
                "partial_fill_qty",
                order_qty,
                order_fee,
                conflicts,
            )
    return ORDER_STATUS_CONFIRMED, None, order_qty, order_fee, conflicts


async def upsert_financial_reconciliation_fills(
    *,
    job_id: int,
    lease_token: str,
    fills: List[Dict[str, Any]] | List[FinancialFillRecord],
) -> Dict[str, Any]:
    """Idempotently store fills and refresh aggregate accounting fields.

    A ``trade_id`` already owned by another execution is a durable ambiguity,
    never a fill to silently reuse. The job becomes terminal ``ambiguous`` and
    the conflicting fill remains owned by its original execution.
    """

    token = _financial_identifier(lease_token, field="lease_token", max_length=128)
    records = normalize_fill_records(fills or [])
    conflicts: list[str] = []

    if is_postgres():
        async with connect() as c:
            async with c.transaction():
                job = await c.fetchrow(
                    """
                    SELECT * FROM financial_reconciliation_jobs
                    WHERE id=$1 AND status='processing' AND lease_token=$2
                    FOR UPDATE
                    """,
                    int(job_id),
                    token,
                )
                if not job:
                    raise RuntimeError("financial reconciliation lease is stale")
                expected_rows = await c.fetch(
                    "SELECT * FROM financial_reconciliation_orders WHERE job_id=$1",
                    int(job_id),
                )
                expected_keys = {str(row["order_key"]) for row in expected_rows}
                for item in records:
                    if item.order_key not in expected_keys:
                        conflicts.append(f"trade_id={item.trade_id}:unexpected_order")
                        continue
                    if (
                        item.symbol != str(job["symbol"]).upper()
                        or item.side != str(job["side"]).lower()
                    ):
                        conflicts.append(f"trade_id={item.trade_id}:symbol_or_side_mismatch")
                        continue
                    await c.execute(
                        """
                        INSERT INTO financial_reconciliation_fills(
                            job_id,execution_id,user_id,exchange,trade_id,order_id,
                            order_key,role,tp_index,symbol,side,price,qty,
                            realized_pnl,fee,fee_asset,fill_time,fingerprint,
                            metadata_json
                        ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,
                                 $14,$15,$16,$17,$18,$19)
                        ON CONFLICT(exchange,user_id,trade_id) DO NOTHING
                        """,
                        int(job_id),
                        int(job["execution_id"]),
                        int(job["user_id"]),
                        str(job["exchange"]),
                        item.trade_id,
                        item.order_id,
                        item.order_key,
                        item.role,
                        item.tp_index,
                        item.symbol,
                        item.side,
                        Decimal(item.price),
                        Decimal(item.qty),
                        Decimal(item.realized_pnl),
                        Decimal(item.fee),
                        item.fee_asset,
                        _financial_pg_datetime(item.fill_time),
                        item.fingerprint,
                        item.metadata_json,
                    )
                    existing = await c.fetchrow(
                        """
                        SELECT * FROM financial_reconciliation_fills
                        WHERE exchange=$1 AND user_id=$2 AND trade_id=$3
                        """,
                        str(job["exchange"]),
                        int(job["user_id"]),
                        item.trade_id,
                    )
                    if not existing or (
                        int(existing["execution_id"]) != int(job["execution_id"])
                        or str(existing["fingerprint"]) != item.fingerprint
                    ):
                        conflicts.append(f"trade_id={item.trade_id}:ownership_collision")

                rows = await c.fetch(
                    """
                    SELECT * FROM financial_reconciliation_fills
                    WHERE job_id=$1 ORDER BY fill_time,id
                    """,
                    int(job_id),
                )
                normalized_rows = [_financial_fill_from_db_row(_dict(row)) for row in rows]
                aggregate = aggregate_fill_records(normalized_rows)
                if aggregate["mixed_fee_assets"]:
                    conflicts.append("mixed_fee_assets")
                orders = await c.fetch(
                    "SELECT * FROM financial_reconciliation_orders WHERE job_id=$1",
                    int(job_id),
                )
                fills_by_key: dict[str, list[FinancialFillRecord]] = {}
                for item in normalized_rows:
                    fills_by_key.setdefault(item.order_key, []).append(item)
                for order in orders:
                    matched = fills_by_key.get(str(order["order_key"]), [])
                    if not matched:
                        continue
                    order_status, order_error, order_qty, order_fee, order_conflicts = (
                        _financial_order_fill_evaluation(_dict(order), matched)
                    )
                    conflicts.extend(order_conflicts)
                    await c.execute(
                        """
                        UPDATE financial_reconciliation_orders
                        SET status=$1,confirmed_fill_count=$2,
                            confirmed_qty=$3,confirmed_fee=$4,
                            last_checked_at=NOW(),last_error=$5,updated_at=NOW()
                        WHERE id=$6
                        """,
                        order_status,
                        len(matched),
                        order_qty,
                        order_fee,
                        order_error,
                        int(order["id"]),
                    )
                confirmed_required = int(
                    await c.fetchval(
                        """
                        SELECT COUNT(*) FROM financial_reconciliation_orders
                        WHERE job_id=$1 AND required=1 AND status='confirmed'
                        """,
                        int(job_id),
                    )
                    or 0
                )
                if conflicts:
                    safe_error = _financial_error_text(";".join(conflicts))
                    updated = await c.fetchrow(
                        """
                        UPDATE financial_reconciliation_jobs
                        SET status='ambiguous',exchange_gross_pnl=$1,
                            total_trading_fee=$2,net_pnl_after_trading_fee=$3,
                            fee_asset=$4,confirmed_order_count=$5,fill_count=$6,
                            last_error=$7,processing_started_at=NULL,
                            lease_token=NULL,resolved_at=NOW(),updated_at=NOW()
                        WHERE id=$8 AND status='processing' AND lease_token=$9
                        RETURNING *
                        """,
                        _financial_pg_decimal(aggregate["exchange_gross_pnl"]),
                        _financial_pg_decimal(aggregate["total_trading_fee"]),
                        _financial_pg_decimal(aggregate["net_pnl_after_trading_fee"]),
                        aggregate["fee_asset"],
                        confirmed_required,
                        int(aggregate["fill_count"]),
                        safe_error,
                        int(job_id),
                        token,
                    )
                else:
                    updated = await c.fetchrow(
                        """
                        UPDATE financial_reconciliation_jobs
                        SET exchange_gross_pnl=$1,total_trading_fee=$2,
                            net_pnl_after_trading_fee=$3,fee_asset=$4,
                            confirmed_order_count=$5,fill_count=$6,
                            last_error=NULL,updated_at=NOW()
                        WHERE id=$7 AND status='processing' AND lease_token=$8
                        RETURNING *
                        """,
                        _financial_pg_decimal(aggregate["exchange_gross_pnl"]),
                        _financial_pg_decimal(aggregate["total_trading_fee"]),
                        _financial_pg_decimal(aggregate["net_pnl_after_trading_fee"]),
                        aggregate["fee_asset"],
                        confirmed_required,
                        int(aggregate["fill_count"]),
                        int(job_id),
                        token,
                    )
                if not updated:
                    raise RuntimeError("financial reconciliation lease changed during fill write")
                result = _dict(updated)
                result["conflicts"] = list(conflicts)
                result["mixed_fee_assets"] = bool(aggregate["mixed_fee_assets"])
                return result

    async with connect() as c:
        await c.execute("BEGIN IMMEDIATE")
        try:
            cur = await c.execute(
                """
                SELECT * FROM financial_reconciliation_jobs
                WHERE id=? AND status='processing' AND lease_token=?
                """,
                (int(job_id), token),
            )
            job = await cur.fetchone()
            if not job:
                raise RuntimeError("financial reconciliation lease is stale")
            cur = await c.execute(
                "SELECT * FROM financial_reconciliation_orders WHERE job_id=?",
                (int(job_id),),
            )
            expected_keys = {str(row["order_key"]) for row in await cur.fetchall()}
            for item in records:
                if item.order_key not in expected_keys:
                    conflicts.append(f"trade_id={item.trade_id}:unexpected_order")
                    continue
                if (
                    item.symbol != str(job["symbol"]).upper()
                    or item.side != str(job["side"]).lower()
                ):
                    conflicts.append(f"trade_id={item.trade_id}:symbol_or_side_mismatch")
                    continue
                await c.execute(
                    """
                    INSERT OR IGNORE INTO financial_reconciliation_fills(
                        job_id,execution_id,user_id,exchange,trade_id,order_id,
                        order_key,role,tp_index,symbol,side,price,qty,
                        realized_pnl,fee,fee_asset,fill_time,fingerprint,
                        metadata_json
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        int(job_id),
                        int(job["execution_id"]),
                        int(job["user_id"]),
                        str(job["exchange"]),
                        item.trade_id,
                        item.order_id,
                        item.order_key,
                        item.role,
                        item.tp_index,
                        item.symbol,
                        item.side,
                        item.price,
                        item.qty,
                        item.realized_pnl,
                        item.fee,
                        item.fee_asset,
                        item.fill_time,
                        item.fingerprint,
                        item.metadata_json,
                    ),
                )
                cur = await c.execute(
                    """
                    SELECT * FROM financial_reconciliation_fills
                    WHERE exchange=? AND user_id=? AND trade_id=?
                    """,
                    (str(job["exchange"]), int(job["user_id"]), item.trade_id),
                )
                existing = await cur.fetchone()
                if not existing or (
                    int(existing["execution_id"]) != int(job["execution_id"])
                    or str(existing["fingerprint"]) != item.fingerprint
                ):
                    conflicts.append(f"trade_id={item.trade_id}:ownership_collision")

            cur = await c.execute(
                """
                SELECT * FROM financial_reconciliation_fills
                WHERE job_id=? ORDER BY datetime(fill_time),id
                """,
                (int(job_id),),
            )
            normalized_rows = [_financial_fill_from_db_row(_dict(row)) for row in await cur.fetchall()]
            aggregate = aggregate_fill_records(normalized_rows)
            if aggregate["mixed_fee_assets"]:
                conflicts.append("mixed_fee_assets")
            cur = await c.execute(
                "SELECT * FROM financial_reconciliation_orders WHERE job_id=?",
                (int(job_id),),
            )
            orders = await cur.fetchall()
            fills_by_key: dict[str, list[FinancialFillRecord]] = {}
            for item in normalized_rows:
                fills_by_key.setdefault(item.order_key, []).append(item)
            for order in orders:
                matched = fills_by_key.get(str(order["order_key"]), [])
                if not matched:
                    continue
                order_status, order_error, order_qty, order_fee, order_conflicts = (
                    _financial_order_fill_evaluation(_dict(order), matched)
                )
                conflicts.extend(order_conflicts)
                await c.execute(
                    """
                    UPDATE financial_reconciliation_orders
                    SET status=?,confirmed_fill_count=?,
                        confirmed_qty=?,confirmed_fee=?,
                        last_checked_at=CURRENT_TIMESTAMP,last_error=?,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (
                        order_status,
                        len(matched),
                        str(decimal_text(order_qty, field="confirmed_qty", nonnegative=True)),
                        str(decimal_text(order_fee, field="confirmed_fee")),
                        order_error,
                        int(order["id"]),
                    ),
                )
            cur = await c.execute(
                """
                SELECT COUNT(*) FROM financial_reconciliation_orders
                WHERE job_id=? AND required=1 AND status='confirmed'
                """,
                (int(job_id),),
            )
            confirmed_required = int((await cur.fetchone())[0] or 0)
            if conflicts:
                await c.execute(
                    """
                    UPDATE financial_reconciliation_jobs
                    SET status='ambiguous',exchange_gross_pnl=?,
                        total_trading_fee=?,net_pnl_after_trading_fee=?,
                        fee_asset=?,confirmed_order_count=?,fill_count=?,
                        last_error=?,processing_started_at=NULL,lease_token=NULL,
                        resolved_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
                    WHERE id=? AND status='processing' AND lease_token=?
                    """,
                    (
                        aggregate["exchange_gross_pnl"],
                        aggregate["total_trading_fee"],
                        aggregate["net_pnl_after_trading_fee"],
                        aggregate["fee_asset"],
                        confirmed_required,
                        int(aggregate["fill_count"]),
                        _financial_error_text(";".join(conflicts)),
                        int(job_id),
                        token,
                    ),
                )
            else:
                await c.execute(
                    """
                    UPDATE financial_reconciliation_jobs
                    SET exchange_gross_pnl=?,total_trading_fee=?,
                        net_pnl_after_trading_fee=?,fee_asset=?,
                        confirmed_order_count=?,fill_count=?,last_error=NULL,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE id=? AND status='processing' AND lease_token=?
                    """,
                    (
                        aggregate["exchange_gross_pnl"],
                        aggregate["total_trading_fee"],
                        aggregate["net_pnl_after_trading_fee"],
                        aggregate["fee_asset"],
                        confirmed_required,
                        int(aggregate["fill_count"]),
                        int(job_id),
                        token,
                    ),
                )
            cur = await c.execute(
                "SELECT * FROM financial_reconciliation_jobs WHERE id=?",
                (int(job_id),),
            )
            updated = await cur.fetchone()
            if not updated:
                raise RuntimeError("financial reconciliation job disappeared")
            await c.commit()
            result = _dict(updated)
            result["conflicts"] = list(conflicts)
            result["mixed_fee_assets"] = bool(aggregate["mixed_fee_assets"])
            return result
        except BaseException:
            await c.rollback()
            raise


async def finalize_financial_reconciliation_job(
    *,
    job_id: int,
    lease_token: str,
    status: str,
    error: str = "",
) -> Dict[str, Any]:
    """Finalize one leased job without changing strategy outcome semantics."""

    terminal_status = normalize_status(status, terminal_only=True)
    token = _financial_identifier(lease_token, field="lease_token", max_length=128)
    safe_error = _financial_error_text(error)
    if is_postgres():
        async with connect() as c:
            async with c.transaction():
                job = await c.fetchrow(
                    """
                    SELECT * FROM financial_reconciliation_jobs
                    WHERE id=$1 AND status='processing' AND lease_token=$2
                    FOR UPDATE
                    """,
                    int(job_id),
                    token,
                )
                if not job:
                    raise RuntimeError("financial reconciliation lease is stale")
                expected = int(job["expected_order_count"] or 0)
                confirmed = int(job["confirmed_order_count"] or 0)
                fill_count = int(job["fill_count"] or 0)
                if terminal_status == FINANCIAL_STATUS_CONFIRMED and (
                    expected <= 0 or confirmed != expected
                ):
                    raise ValueError("confirmed financial result requires every required order")
                if terminal_status == FINANCIAL_STATUS_PARTIAL and fill_count <= 0:
                    raise ValueError("partial financial result requires at least one fill")
                replacement_order_status = {
                    FINANCIAL_STATUS_PARTIAL: "missing",
                    FINANCIAL_STATUS_AMBIGUOUS: "ambiguous",
                    FINANCIAL_STATUS_UNAVAILABLE: "unavailable",
                }.get(terminal_status)
                if replacement_order_status:
                    await c.execute(
                        """
                        UPDATE financial_reconciliation_orders
                        SET status=$1,last_error=$2,last_checked_at=NOW(),updated_at=NOW()
                        WHERE job_id=$3 AND required=1 AND status<>'confirmed'
                        """,
                        replacement_order_status,
                        safe_error,
                        int(job_id),
                    )
                updated = await c.fetchrow(
                    """
                    UPDATE financial_reconciliation_jobs
                    SET status=$1,last_error=$2,processing_started_at=NULL,
                        lease_token=NULL,resolved_at=NOW(),
                        confirmed_at=CASE WHEN $1='confirmed' THEN NOW() ELSE NULL END,
                        updated_at=NOW()
                    WHERE id=$3 AND status='processing' AND lease_token=$4
                    RETURNING *
                    """,
                    terminal_status,
                    safe_error,
                    int(job_id),
                    token,
                )
                if not updated:
                    raise RuntimeError("financial reconciliation lease changed during finalize")
                return _dict(updated)

    async with connect() as c:
        await c.execute("BEGIN IMMEDIATE")
        try:
            cur = await c.execute(
                """
                SELECT * FROM financial_reconciliation_jobs
                WHERE id=? AND status='processing' AND lease_token=?
                """,
                (int(job_id), token),
            )
            job = await cur.fetchone()
            if not job:
                raise RuntimeError("financial reconciliation lease is stale")
            expected = int(job["expected_order_count"] or 0)
            confirmed = int(job["confirmed_order_count"] or 0)
            fill_count = int(job["fill_count"] or 0)
            if terminal_status == FINANCIAL_STATUS_CONFIRMED and (
                expected <= 0 or confirmed != expected
            ):
                raise ValueError("confirmed financial result requires every required order")
            if terminal_status == FINANCIAL_STATUS_PARTIAL and fill_count <= 0:
                raise ValueError("partial financial result requires at least one fill")
            replacement_order_status = {
                FINANCIAL_STATUS_PARTIAL: "missing",
                FINANCIAL_STATUS_AMBIGUOUS: "ambiguous",
                FINANCIAL_STATUS_UNAVAILABLE: "unavailable",
            }.get(terminal_status)
            if replacement_order_status:
                await c.execute(
                    """
                    UPDATE financial_reconciliation_orders
                    SET status=?,last_error=?,last_checked_at=CURRENT_TIMESTAMP,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE job_id=? AND required=1 AND status<>'confirmed'
                    """,
                    (replacement_order_status, safe_error, int(job_id)),
                )
            cur = await c.execute(
                """
                UPDATE financial_reconciliation_jobs
                SET status=?,last_error=?,processing_started_at=NULL,
                    lease_token=NULL,resolved_at=CURRENT_TIMESTAMP,
                    confirmed_at=CASE WHEN ?='confirmed' THEN CURRENT_TIMESTAMP ELSE NULL END,
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=? AND status='processing' AND lease_token=?
                """,
                (terminal_status, safe_error, terminal_status, int(job_id), token),
            )
            if int(cur.rowcount or 0) != 1:
                raise RuntimeError("financial reconciliation lease changed during finalize")
            cur = await c.execute(
                "SELECT * FROM financial_reconciliation_jobs WHERE id=?",
                (int(job_id),),
            )
            updated = await cur.fetchone()
            await c.commit()
            return _dict(updated)
        except BaseException:
            await c.rollback()
            raise


async def set_financial_reconciliation_notification_status(
    *, job_id: int, status: str
) -> bool:
    normalized = str(status or "").strip().lower()
    if normalized not in {"pending", "queued", "delivered", "skipped"}:
        raise ValueError("unsupported financial notification status")
    if is_postgres():
        async with connect() as c:
            row = await c.fetchrow(
                """
                UPDATE financial_reconciliation_jobs
                SET notification_status=$1,
                    notified_at=CASE WHEN $1='delivered' THEN NOW() ELSE notified_at END,
                    updated_at=NOW()
                WHERE id=$2 RETURNING id
                """,
                normalized,
                int(job_id),
            )
            return bool(row)
    async with connect() as c:
        cur = await c.execute(
            """
            UPDATE financial_reconciliation_jobs
            SET notification_status=?,
                notified_at=CASE WHEN ?='delivered' THEN CURRENT_TIMESTAMP ELSE notified_at END,
                updated_at=CURRENT_TIMESTAMP
            WHERE id=?
            """,
            (normalized, normalized, int(job_id)),
        )
        await c.commit()
        return int(cur.rowcount or 0) == 1


async def claim_due_financial_reconciliation_notifications(
    *, limit: int = 20, stale_after_sec: float = 120.0
) -> List[Dict[str, Any]]:
    """Atomically claim terminal financial summaries for Telegram delivery.

    A stale ``queued`` claim is recoverable after a process crash.  ``delivered``
    and ``skipped`` rows are permanent barriers and are never reclaimed.
    """

    bounded = max(1, min(int(limit or 20), 100))
    stale = max(30.0, min(float(stale_after_sec or 120.0), 3600.0))
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=stale)
    terminal = sorted(FINANCIAL_TERMINAL_STATUSES)
    if is_postgres():
        async with connect() as c:
            async with c.transaction():
                rows = await c.fetch(
                    """
                    WITH candidates AS (
                        SELECT id
                        FROM financial_reconciliation_jobs
                        WHERE status = ANY($1::text[])
                          AND (
                            notification_status='pending'
                            OR (notification_status='queued' AND updated_at <= $2)
                          )
                        ORDER BY resolved_at NULLS LAST,id
                        FOR UPDATE SKIP LOCKED
                        LIMIT $3
                    )
                    UPDATE financial_reconciliation_jobs j
                    SET notification_status='queued',updated_at=NOW()
                    FROM candidates c
                    WHERE j.id=c.id
                    RETURNING j.*
                    """,
                    terminal,
                    cutoff,
                    bounded,
                )
                return [_dict(row) for row in rows]

    async with connect() as c:
        await c.execute("BEGIN IMMEDIATE")
        try:
            placeholders = ",".join("?" for _ in terminal)
            cur = await c.execute(
                f"""
                SELECT * FROM financial_reconciliation_jobs
                WHERE status IN ({placeholders})
                  AND (
                    notification_status='pending'
                    OR (notification_status='queued' AND datetime(updated_at) <= datetime(?))
                  )
                ORDER BY CASE WHEN resolved_at IS NULL THEN 1 ELSE 0 END,
                         datetime(resolved_at),id
                LIMIT ?
                """,
                (*terminal, cutoff.isoformat(), bounded),
            )
            rows = [_dict(row) for row in await cur.fetchall()]
            claimed: list[Dict[str, Any]] = []
            for row in rows:
                cur = await c.execute(
                    """
                    UPDATE financial_reconciliation_jobs
                    SET notification_status='queued',updated_at=CURRENT_TIMESTAMP
                    WHERE id=? AND (
                        notification_status='pending'
                        OR (notification_status='queued' AND datetime(updated_at) <= datetime(?))
                    )
                    """,
                    (int(row["id"]), cutoff.isoformat()),
                )
                if int(cur.rowcount or 0) == 1:
                    row["notification_status"] = "queued"
                    claimed.append(row)
            await c.commit()
            return claimed
        except BaseException:
            await c.rollback()
            raise


async def financial_reconciliation_queue_snapshot() -> Dict[str, int]:
    """Small sanitized queue snapshot for future diagnostics and tests."""

    statuses = [
        FINANCIAL_STATUS_PENDING,
        FINANCIAL_STATUS_PROCESSING,
        FINANCIAL_STATUS_CONFIRMED,
        FINANCIAL_STATUS_PARTIAL,
        FINANCIAL_STATUS_AMBIGUOUS,
        FINANCIAL_STATUS_UNAVAILABLE,
    ]
    result = {status: 0 for status in statuses}
    if is_postgres():
        async with connect() as c:
            rows = await c.fetch(
                """
                SELECT status,COUNT(*) AS count
                FROM financial_reconciliation_jobs
                GROUP BY status
                """
            )
            for row in rows:
                status = str(row["status"])
                if status in result:
                    result[status] = int(row["count"] or 0)
            return result
    async with connect() as c:
        cur = await c.execute(
            """
            SELECT status,COUNT(*) AS count
            FROM financial_reconciliation_jobs
            GROUP BY status
            """
        )
        for row in await cur.fetchall():
            status = str(row["status"])
            if status in result:
                result[status] = int(row["count"] or 0)
        return result


async def bind_financial_order_exchange_identity(
    *,
    job_id: int,
    lease_token: str,
    client_order_id: str,
    exchange_order_id: str,
) -> Dict[str, Any]:
    """Bind one client-only expectation to an exact BingX order ID.

    The update is lease-protected and refuses to merge two durable expectation
    rows. Step 3 may use this after resolving a crash-safe clientOrderID through
    BingX order history, before exact fill rows are accepted.
    """

    token = _financial_identifier(lease_token, field="lease_token", max_length=128)
    client_id = _financial_identifier(
        client_order_id, field="client_order_id", max_length=128
    )
    exchange_id = _financial_identifier(
        exchange_order_id, field="exchange_order_id", max_length=128
    )
    new_key = f"order:{exchange_id}"
    if is_postgres():
        async with connect() as c:
            async with c.transaction():
                job = await c.fetchrow(
                    """
                    SELECT id FROM financial_reconciliation_jobs
                    WHERE id=$1 AND status='processing' AND lease_token=$2
                    FOR UPDATE
                    """,
                    int(job_id),
                    token,
                )
                if not job:
                    raise RuntimeError("financial reconciliation lease is stale")
                row = await c.fetchrow(
                    """
                    SELECT * FROM financial_reconciliation_orders
                    WHERE job_id=$1 AND client_order_id=$2
                    FOR UPDATE
                    """,
                    int(job_id),
                    client_id,
                )
                if not row:
                    raise KeyError("financial client order expectation not found")
                existing = await c.fetchrow(
                    """
                    SELECT id FROM financial_reconciliation_orders
                    WHERE job_id=$1 AND order_key=$2 AND id<>$3
                    """,
                    int(job_id),
                    new_key,
                    int(row["id"]),
                )
                if existing:
                    raise ValueError("exchange order identity already belongs to another expectation")
                updated = await c.fetchrow(
                    """
                    UPDATE financial_reconciliation_orders
                    SET order_key=$1,exchange_order_id=$2,updated_at=NOW()
                    WHERE id=$3 RETURNING *
                    """,
                    new_key,
                    exchange_id,
                    int(row["id"]),
                )
                return _dict(updated)

    async with connect() as c:
        await c.execute("BEGIN IMMEDIATE")
        try:
            cur = await c.execute(
                """
                SELECT id FROM financial_reconciliation_jobs
                WHERE id=? AND status='processing' AND lease_token=?
                """,
                (int(job_id), token),
            )
            if not await cur.fetchone():
                raise RuntimeError("financial reconciliation lease is stale")
            cur = await c.execute(
                """
                SELECT * FROM financial_reconciliation_orders
                WHERE job_id=? AND client_order_id=?
                """,
                (int(job_id), client_id),
            )
            row = await cur.fetchone()
            if not row:
                raise KeyError("financial client order expectation not found")
            cur = await c.execute(
                """
                SELECT id FROM financial_reconciliation_orders
                WHERE job_id=? AND order_key=? AND id<>?
                """,
                (int(job_id), new_key, int(row["id"])),
            )
            if await cur.fetchone():
                raise ValueError(
                    "exchange order identity already belongs to another expectation"
                )
            await c.execute(
                """
                UPDATE financial_reconciliation_orders
                SET order_key=?,exchange_order_id=?,updated_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (new_key, exchange_id, int(row["id"])),
            )
            cur = await c.execute(
                "SELECT * FROM financial_reconciliation_orders WHERE id=?",
                (int(row["id"]),),
            )
            updated = await cur.fetchone()
            await c.commit()
            return _dict(updated)
        except BaseException:
            await c.rollback()
            raise
