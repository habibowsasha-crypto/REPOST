from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Awaitable, Callable

from app.config import get_settings
from app.database import db
from app.services.limit_tp_catchup import process_pending_limit_tp_catchup_once
from app.services.be_monitor import process_be_monitor_once
from app.services.partial_tp_recovery import process_partial_tp_recovery_once
from app.services.signal_executor import process_background_tp_recovery_once
from app.services.position_lifecycle_guard import process_position_lifecycle_guard_once
from app.services.full_reconcile_locality import account_local_full_pass_rows
from app.services.durable_notifications import durable_notification_worker_loop
from app.services.market_event_rollout import market_event_rollout_loop
from app.services.event_driven_monitor import (
    market_price_event_loop,
    market_event_verifier_loop,
    market_event_be_preflight_loop,
    market_event_admin_watch_loop,
    critical_account_reconcile_loop,
    trade_group_housekeeping_loop,
    PRICE_STREAM_DEGRADED,
    get_fresh_public_last_price_snapshot,
)
from app.services.monitor_diagnostics import (
    diagnostic_span,
    event_loop_heartbeat_loop,
    mark_cycle_completed,
    monitor_diagnostics_summary_loop,
    record_counter,
)

log = logging.getLogger(__name__)

# G53: keep full fallback stages bounded so they yield DB/advisory resources
# to the five-second critical and MARKET EVENT lanes. The existing cursor in
# each service preserves full rotating coverage across subsequent cycles.
_FULL_LIFECYCLE_CHUNK_ROWS = 24
_FULL_BE_CHUNK_ROWS = 24
# G63: one bounded early BE pass closes the restart blind spot before the slow
# rotating full fallback reaches every page. It reuses the existing BE engine;
# exact TP history recovery inside that engine remains fail-closed.
_STARTUP_BE_CATCHUP_INITIAL_DELAY_SEC = 3.0
_STARTUP_BE_CATCHUP_PAGE_ROWS = 24
_STARTUP_BE_CATCHUP_MAX_PAGES = 2
_STARTUP_BE_CATCHUP_NEW_PAGE_BUDGET_SEC = 30.0
# G64: the heavy full fallback must not compete with the one-shot G63 restart
# TP->BE catch-up.  The gate is bounded so a broken catch-up cannot starve the
# fallback forever, and the short grace lets released DB/advisory resources
# settle before the first heavy pass.
_G64_FULL_STARTUP_GATE_TIMEOUT_SEC = 60.0
_G64_FULL_STARTUP_GRACE_SEC = 5.0
NotifyFn = Callable[[int, str], Awaitable[object] | object]


async def _run_monitor_task(coro, *, stage: str = "monitor"):
    """Run one background monitor task under bounded DB admission.

    The context propagates to child verifier tasks. Telegram handlers and the
    direct trade dispatcher are intentionally outside this scope, so monitor
    bursts cannot consume every local PostgreSQL pool slot.
    """
    async with db.monitor_db_workload(stage=stage):
        return await coro


async def _monitor_startup_watchdog_loop() -> None:
    """Emit a clear warning when startup cycles have not completed in time."""
    from app.services.monitor_diagnostics import heartbeat_snapshot

    await asyncio.sleep(30.0)
    while True:
        snapshot = heartbeat_snapshot()
        ages = dict(snapshot.get("cycle_last_age_sec") or {})
        thresholds = {
            "public_price": 60.0,
            "critical_reconcile": 120.0,
            "event_verifier": 60.0,
            "event_admin_watch": 180.0,
        }
        missing = [name for name in thresholds if name not in ages]
        stale = {
            name: float(ages.get(name) or 0.0)
            for name, limit in thresholds.items()
            if name in ages and float(ages.get(name) or 0.0) > limit
        }
        if missing or stale:
            log.error(
                "MONITOR_STARTUP_STALL missing_cycles=%s stale_cycles=%s; "
                "background DB calls are bounded and Telegram/UI slots are reserved",
                ",".join(missing) or "none",
                ",".join(f"{name}:{age:.1f}s" for name, age in sorted(stale.items())) or "none",
            )
        await asyncio.sleep(30.0)


@dataclass(frozen=True)
class _SafeCallOutcome:
    processed: int
    completed: bool
    deferred: bool = False


async def _safe_call_outcome(
    name: str,
    coro,
    *,
    diagnostic_label: str | None = None,
    diagnostic_metadata: dict[str, object] | None = None,
) -> _SafeCallOutcome:
    """Run one monitor lane and preserve whether it actually completed.

    A zero processed count is a successful no-op.  Exceptions and advisory-lock
    deferrals are different: the lane did not finish and the alternating full
    scheduler must not silently treat it as covered.
    """

    with diagnostic_span(
        diagnostic_label or name,
        emit=True,
        metadata=diagnostic_metadata,
    ) as span:
        try:
            result = int(await coro)
            span.set("result", result)
            span.set("completed", 1)
            return _SafeCallOutcome(result, True, False)
        except asyncio.CancelledError:
            raise
        except db.MonitorAdvisoryBusy as exc:
            span.set("result", 0)
            span.set("completed", 0)
            span.set("deferred", 1)
            log.warning(
                "%s monitor iteration deferred phase=%s stage=%s key=%s",
                name,
                exc.phase,
                exc.stage,
                exc.key,
            )
            return _SafeCallOutcome(0, False, True)
        except Exception:
            span.errors += 1
            span.set("result", 0)
            span.set("completed", 0)
            log.exception("%s monitor iteration failed", name)
            return _SafeCallOutcome(0, False, False)


async def _safe_call(
    name: str,
    coro,
    *,
    diagnostic_label: str | None = None,
    diagnostic_metadata: dict[str, object] | None = None,
) -> int:
    outcome = await _safe_call_outcome(
        name,
        coro,
        diagnostic_label=diagnostic_label,
        diagnostic_metadata=diagnostic_metadata,
    )
    return outcome.processed


def _opposite_heavy_stage(stage: str) -> str:
    return "be" if stage == "lifecycle" else "lifecycle"


async def _run_full_stage_with_advisory_session(
    name: str,
    coro_factory: Callable[[], Awaitable[int]],
    *,
    session_key: str,
    diagnostic_label: str,
    diagnostic_metadata: dict[str, object] | None = None,
) -> _SafeCallOutcome:
    """Run one heavy full-reconcile stage on one PostgreSQL advisory session.

    Full lifecycle and background BE inspect many executions sequentially.  The
    per-execution lock must remain exact, but repeatedly acquiring/releasing a
    pool connection for every row makes asyncpg perform a session reset for each
    execution.  A narrow stage-level advisory scope keeps one low-priority full
    connection for the duration of the lane; nested execution/STOP/TP advisory
    locks still use their original exact keys and are released row-by-row.

    ``coro_factory`` is intentionally lazy.  If the outer stage lock is busy, no
    un-awaited lifecycle/BE coroutine is created while ``_safe_call_outcome``
    records the lane as deferred for the bounded scheduler retry.
    """

    async def _session_bound_call() -> int:
        if not db.is_postgres():
            return int(await coro_factory())
        async with db.distributed_advisory_lock(
            f"full-reconcile-stage:{session_key}"
        ):
            record_counter("full_stage_advisory_session_opened")
            log.info(
                "FULL_STAGE_ADVISORY_SESSION stage=%s connection_scope=shared",
                session_key,
            )
            return int(await coro_factory())

    return await _safe_call_outcome(
        name,
        _session_bound_call(),
        diagnostic_label=diagnostic_label,
        diagnostic_metadata=diagnostic_metadata,
    )


async def _run_full_be_fallback_stage(
    notify: NotifyFn | None,
    *,
    settings,
    cycle_span,
) -> _SafeCallOutcome:
    """Run one full BE fallback with a fresh public-price snapshot.

    Public prices are reused only for the pre-read.  Every private position,
    STOP/open-order/history read-back and every post-write verification remains
    unchanged inside ``process_be_monitor_once``.  A broken optional public
    snapshot must not terminate the full worker: BE safely falls back to its
    original direct exchange reads.
    """

    try:
        raw_prices, raw_snapshot = get_fresh_public_last_price_snapshot(
            max_age_sec=float(settings.MARKET_PRICE_STALE_SEC),
            require_healthy=True,
        )
        if not isinstance(raw_prices, dict) or not isinstance(raw_snapshot, dict):
            raise TypeError("public-price snapshot returned an invalid shape")
        be_public_prices = dict(raw_prices)
        be_snapshot = {
            key: int(raw_snapshot[key])
            for key in (
                "fresh_rows",
                "stale_rows",
                "total_rows",
                "oldest_age_ms",
                "newest_age_ms",
                "stream_degraded",
            )
        }
        if any(
            be_snapshot[key] < 0
            for key in (
                "fresh_rows",
                "stale_rows",
                "total_rows",
                "oldest_age_ms",
                "newest_age_ms",
            )
        ):
            raise ValueError("public-price snapshot contains negative counters")
        snapshot_error = 0
    except Exception:
        log.exception(
            "full BE public-price snapshot failed; falling back to direct exchange reads"
        )
        be_public_prices = {}
        be_snapshot = {
            "fresh_rows": 0,
            "stale_rows": 0,
            "total_rows": 0,
            "oldest_age_ms": 0,
            "newest_age_ms": 0,
            "stream_degraded": 1,
        }
        snapshot_error = 1

    cycle_span.set("be_public_price_snapshot_rows", be_snapshot["fresh_rows"])
    cycle_span.set("be_public_price_snapshot_stale", be_snapshot["stale_rows"])
    cycle_span.set(
        "be_public_price_snapshot_oldest_age_ms",
        be_snapshot["oldest_age_ms"],
    )
    cycle_span.set(
        "be_public_price_snapshot_degraded",
        be_snapshot["stream_degraded"],
    )
    cycle_span.set("be_public_price_snapshot_error", snapshot_error)
    return await _run_full_stage_with_advisory_session(
        "be_monitor",
        lambda: process_be_monitor_once(
            notify=notify,
            market_prices=be_public_prices or None,
            scan_limit=_FULL_BE_CHUNK_ROWS,
        ),
        session_key="be",
        diagnostic_label="full.be_monitor",
        diagnostic_metadata={
            "public_price_snapshot_rows": be_snapshot["fresh_rows"],
            "public_price_snapshot_stale": be_snapshot["stale_rows"],
            "public_price_snapshot_oldest_age_ms": be_snapshot[
                "oldest_age_ms"
            ],
            "public_price_snapshot_degraded": be_snapshot[
                "stream_degraded"
            ],
            "public_price_snapshot_error": snapshot_error,
        },
    )


async def restart_tp_be_startup_catchup_once(
    notify: NotifyFn | None = None,
) -> int:
    """Run a bounded early BE catch-up immediately after Railway restart.

    G61 protects TP events that are already queued. G63 additionally scans the
    first two rotating BE pages before the slow full fallback can take multiple
    cooldown cycles to reach them. The existing BE engine still requires exact
    TP evidence/position reduction and owns the normal safe STOP replacement.
    """

    await asyncio.sleep(_STARTUP_BE_CATCHUP_INITIAL_DELAY_SEC)
    started = time.monotonic()
    after_id = 0
    pages = 0
    scanned = 0
    actions = 0
    snapshot_rows = 0
    try:
        prices, snapshot = get_fresh_public_last_price_snapshot(
            max_age_sec=10.0, require_healthy=True
        )
        if not isinstance(prices, dict):
            prices = {}
        if not prices:
            # The first public-price pass usually completes a few seconds after
            # monitor startup. Give it one short chance so this recovery reuses
            # public data instead of multiplying private ticker reads.
            await asyncio.sleep(2.0)
            prices, snapshot = get_fresh_public_last_price_snapshot(
                max_age_sec=10.0, require_healthy=True
            )
            if not isinstance(prices, dict):
                prices = {}
        if isinstance(snapshot, dict):
            snapshot_rows = int(snapshot.get("fresh_rows") or 0)
    except Exception:
        prices = {}
        log.info(
            "G63_RESTART_TP_BE_CATCHUP phase=price_snapshot_unavailable "
            "fallback=be_direct_price_read"
        )

    log.warning(
        "G63_RESTART_TP_BE_CATCHUP phase=start max_pages=%s page_rows=%s "
        "price_snapshot_rows=%s",
        _STARTUP_BE_CATCHUP_MAX_PAGES,
        _STARTUP_BE_CATCHUP_PAGE_ROWS,
        snapshot_rows,
    )
    for _ in range(_STARTUP_BE_CATCHUP_MAX_PAGES):
        if time.monotonic() - started >= _STARTUP_BE_CATCHUP_NEW_PAGE_BUDGET_SEC:
            break
        rows = await db.be_monitor_executions(
            limit=_STARTUP_BE_CATCHUP_PAGE_ROWS, after_id=after_id
        )
        if not rows:
            break
        pages += 1
        scanned += len(rows)
        next_after_id = max(int(row.get("id") or 0) for row in rows)
        if next_after_id <= after_id:
            break
        after_id = next_after_id
        ordered_rows = account_local_full_pass_rows(rows)
        try:
            actions += int(
                await process_be_monitor_once(
                    notify=notify,
                    rows_override=ordered_rows,
                    market_prices=prices or None,
                )
            )
        except asyncio.CancelledError:
            raise
        except db.MonitorAdvisoryBusy as exc:
            log.info(
                "G63_RESTART_TP_BE_CATCHUP phase=page_deferred page=%s "
                "after_id=%s reason=advisory_busy stage=%s",
                pages,
                after_id,
                exc.stage,
            )
            break
        except Exception:
            log.exception(
                "G63_RESTART_TP_BE_CATCHUP phase=page_failed page=%s after_id=%s",
                pages,
                after_id,
            )
            break
        await asyncio.sleep(0)

    log.info(
        "G63_RESTART_TP_BE_CATCHUP phase=complete pages=%s scanned=%s "
        "actions=%s duration_ms=%s",
        pages,
        scanned,
        actions,
        int((time.monotonic() - started) * 1000),
    )
    return actions


async def _restart_tp_be_startup_catchup_with_signal(
    notify: NotifyFn | None,
    done_event: asyncio.Event,
) -> int:
    """Run G63 catch-up and always release the G64 full-fallback gate."""

    try:
        return int(await restart_tp_be_startup_catchup_once(notify=notify))
    finally:
        done_event.set()
        log.info("G64_FULL_STARTUP_GATE phase=catchup_released")


async def _wait_for_restart_tp_be_catchup_before_full(
    done_event: asyncio.Event,
) -> None:
    """Bounded startup ordering for the heavy full fallback.

    Critical reconcile, MARKET EVENT verification, public price and the G61
    TP->BE preflight remain live immediately.  Only the slow full fallback is
    held behind the one-shot restart BE scan.
    """

    started = time.monotonic()
    timed_out = False
    try:
        await asyncio.wait_for(
            done_event.wait(), timeout=_G64_FULL_STARTUP_GATE_TIMEOUT_SEC
        )
    except asyncio.TimeoutError:
        timed_out = True
        log.warning(
            "G64_FULL_STARTUP_GATE phase=timeout timeout_sec=%s action=continue_fail_open",
            _G64_FULL_STARTUP_GATE_TIMEOUT_SEC,
        )
    if _G64_FULL_STARTUP_GRACE_SEC > 0:
        await asyncio.sleep(_G64_FULL_STARTUP_GRACE_SEC)
    log.info(
        "G64_FULL_STARTUP_GATE phase=complete timed_out=%s waited_ms=%s grace_sec=%s",
        int(timed_out),
        int((time.monotonic() - started) * 1000),
        _G64_FULL_STARTUP_GRACE_SEC,
    )


async def _g64_gated_full_reconcile_worker_loop(
    notify: NotifyFn | None,
    done_event: asyncio.Event,
) -> None:
    await _wait_for_restart_tp_be_catchup_before_full(done_event)
    await full_reconcile_worker_loop(notify=notify, initial_delay_sec=0.0)


async def full_reconcile_worker_loop(
    notify: NotifyFn | None = None, *, initial_delay_sec: float = 0.0
) -> None:
    """Slow rotating private-account audit used as a safety fallback.

    The first pass keeps historical bootstrap coverage and attempts both heavy
    stages.  Steady state alternates lifecycle and BE.  A failed or advisory-
    deferred heavy lane receives one immediate next-cycle retry; if that retry
    also fails, the scheduler yields to the other lane so neither safety fallback
    can be starved indefinitely.
    """

    settings = get_settings()
    interval = float(settings.MONITOR_FULL_RECONCILE_INTERVAL_SEC)
    if initial_delay_sec > 0:
        await asyncio.sleep(float(initial_delay_sec))

    bootstrap = True
    next_heavy_stage = "lifecycle"
    retry_stage: str | None = None
    while True:
        started = time.monotonic()
        price_stream = "degraded" if PRICE_STREAM_DEGRADED.is_set() else "healthy"
        heavy_stage = (
            "bootstrap_both"
            if bootstrap
            else (retry_stage or next_heavy_stage)
        )
        retry_attempt = bool(not bootstrap and retry_stage == heavy_stage)
        lifecycle_outcome: _SafeCallOutcome | None = None
        be_outcome: _SafeCallOutcome | None = None

        with diagnostic_span(
            "cycle.full_reconcile",
            emit=True,
            metadata={
                "price_stream": price_stream,
                "heavy_stage": heavy_stage,
                "heavy_stage_retry_attempt": int(retry_attempt),
            },
        ) as cycle_span:
            processed = 0
            # Small durable recovery lanes stay on every fallback cycle. Their
            # crash-recovery obligations are independent from lane rotation.
            processed += await _safe_call(
                "limit_tp_catchup",
                process_pending_limit_tp_catchup_once(notify=notify),
                diagnostic_label="full.limit_tp_catchup",
            )
            processed += await _safe_call(
                "background_tp_recovery",
                process_background_tp_recovery_once(notify=notify),
                diagnostic_label="full.background_tp_recovery",
            )
            processed += await _safe_call(
                "partial_tp_recovery",
                process_partial_tp_recovery_once(notify=notify),
                diagnostic_label="full.partial_tp_recovery",
            )

            if heavy_stage in {"bootstrap_both", "lifecycle"}:
                lifecycle_outcome = await _run_full_stage_with_advisory_session(
                    "position_lifecycle_guard",
                    lambda: process_position_lifecycle_guard_once(
                        notify=notify,
                        scan_limit=_FULL_LIFECYCLE_CHUNK_ROWS,
                    ),
                    session_key="lifecycle",
                    diagnostic_label="full.position_lifecycle_guard",
                )
                processed += lifecycle_outcome.processed

            if heavy_stage in {"bootstrap_both", "be"}:
                be_outcome = await _run_full_be_fallback_stage(
                    notify,
                    settings=settings,
                    cycle_span=cycle_span,
                )
                processed += be_outcome.processed

            attempted_outcomes = [
                outcome
                for outcome in (lifecycle_outcome, be_outcome)
                if outcome is not None
            ]
            heavy_stage_completed = bool(attempted_outcomes) and all(
                outcome.completed for outcome in attempted_outcomes
            )
            heavy_stage_deferred = any(
                outcome.deferred for outcome in attempted_outcomes
            )
            cycle_span.set("result", processed)
            cycle_span.set("heavy_stage", heavy_stage)
            cycle_span.set("heavy_stage_completed", int(heavy_stage_completed))
            cycle_span.set("heavy_stage_deferred", int(heavy_stage_deferred))
            mark_cycle_completed("full_reconcile")

        if bootstrap:
            bootstrap = False
            lifecycle_completed = bool(
                lifecycle_outcome is not None and lifecycle_outcome.completed
            )
            be_completed = bool(be_outcome is not None and be_outcome.completed)
            if lifecycle_completed and be_completed:
                retry_stage = None
                next_heavy_stage = "lifecycle"
            elif not lifecycle_completed:
                retry_stage = "lifecycle"
                next_heavy_stage = "be"
            else:
                retry_stage = "be"
                next_heavy_stage = "lifecycle"
        elif heavy_stage_completed:
            retry_stage = None
            next_heavy_stage = _opposite_heavy_stage(heavy_stage)
        elif retry_attempt:
            # One bounded retry has already failed/deferred. Yield to the other
            # fallback lane, then normal alternation will revisit this one.
            retry_stage = None
            next_heavy_stage = _opposite_heavy_stage(heavy_stage)
        else:
            # Do not silently rotate away after the first incomplete lane.
            retry_stage = heavy_stage
            next_heavy_stage = _opposite_heavy_stage(heavy_stage)

        scheduled_stage = retry_stage or next_heavy_stage
        duration = time.monotonic() - started
        # MONITOR_FULL_RECONCILE_INTERVAL_SEC is a real post-cycle cooldown, not
        # a target period. Alternating heavy fallback stages gives DB/API a rest
        # window while critical reconcile remains continuously active.
        cooldown = max(30.0, interval)
        level = log.warning if duration >= interval else log.info
        level(
            "full reconcile cycle processed=%s duration_ms=%s next_target_sec=%s price_stream=%s schedule=post_cycle_cooldown heavy_stage=%s heavy_stage_completed=%s heavy_stage_deferred=%s heavy_stage_retry_attempt=%s next_heavy_stage=%s retry_stage=%s",
            processed,
            int(duration * 1000),
            int(cooldown),
            "degraded" if PRICE_STREAM_DEGRADED.is_set() else "healthy",
            heavy_stage,
            int(heavy_stage_completed),
            int(heavy_stage_deferred),
            int(retry_attempt),
            scheduled_stage,
            retry_stage or "none",
        )
        await asyncio.sleep(cooldown)


# Legacy loops remain available when EVENT_DRIVEN_MONITOR_ENABLED=false.
async def trade_protection_worker_loop(notify: NotifyFn | None = None) -> None:
    settings = get_settings()
    interval = max(5, int(settings.MONITOR_ACTIVE_INTERVAL_SEC))
    while True:
        started = time.monotonic()
        processed = 0
        processed += await _safe_call(
            "limit_tp_catchup", process_pending_limit_tp_catchup_once(notify=notify)
        )
        processed += await _safe_call(
            "background_tp_recovery", process_background_tp_recovery_once(notify=notify)
        )
        processed += await _safe_call(
            "partial_tp_recovery", process_partial_tp_recovery_once(notify=notify)
        )
        processed += await _safe_call(
            "position_lifecycle_guard",
            process_position_lifecycle_guard_once(notify=notify),
        )
        elapsed = time.monotonic() - started
        log.debug(
            "legacy trade protection processed=%s duration_ms=%s",
            processed,
            int(elapsed * 1000),
        )
        await asyncio.sleep(max(0.1, interval - elapsed))


async def be_worker_loop(notify: NotifyFn | None = None) -> None:
    settings = get_settings()
    interval = max(5, int(settings.MONITOR_ACTIVE_INTERVAL_SEC))
    while True:
        started = time.monotonic()
        processed = await _safe_call(
            "be_monitor", process_be_monitor_once(notify=notify)
        )
        elapsed = time.monotonic() - started
        log.debug(
            "legacy BE processed=%s duration_ms=%s", processed, int(elapsed * 1000)
        )
        await asyncio.sleep(max(0.1, interval - elapsed))


def start_monitor_workers(notify: NotifyFn | None = None) -> list[asyncio.Task[None]]:
    """Start monitoring tasks inside exactly one Railway bot process."""
    settings = get_settings()
    if db.is_postgres():
        admission = db.monitor_db_admission_snapshot()
        log.info(
            "POSTGRES_MONITOR_ADMISSION pool_max=%s ordinary_limit=%s "
            "critical_limit=%s ordinary_total_limit=%s advisory_total=%s "
            "advisory_general=%s advisory_critical=%s advisory_isolated=%s "
            "full_advisory_limit=%s foreground_reserved_at_least=%s",
            admission["pool_max"],
            admission["ordinary_monitor_limit"],
            admission["critical_monitor_limit"],
            admission["ordinary_monitor_total_limit"],
            admission["advisory_monitor_limit"],
            admission["general_advisory_monitor_limit"],
            admission["critical_advisory_monitor_limit"],
            admission["critical_advisory_isolated"],
            admission["full_advisory_monitor_limit"],
            max(
                0,
                admission["pool_max"]
                - admission["ordinary_monitor_total_limit"]
                - admission["advisory_monitor_limit"],
            ),
        )
    tasks: list[asyncio.Task[None]] = [
        asyncio.create_task(
            _run_monitor_task(
                durable_notification_worker_loop(notify),
                stage="notifications",
            ),
            name="durable-monitor-notifications",
        )
    ]
    if bool(getattr(settings, "MONITOR_DIAGNOSTICS_ENABLED", True)):
        tasks.extend(
            [
                asyncio.create_task(
                    event_loop_heartbeat_loop(),
                    name="monitor-event-loop-heartbeat",
                ),
                asyncio.create_task(
                    _run_monitor_task(monitor_diagnostics_summary_loop(), stage="diagnostics"),
                    name="monitor-diagnostics-summary",
                ),
                asyncio.create_task(
                    _monitor_startup_watchdog_loop(),
                    name="monitor-startup-watchdog",
                ),
            ]
        )

    if bool(settings.EVENT_DRIVEN_MONITOR_ENABLED):
        startup_be_done = asyncio.Event()
        tasks.extend(
            [
                asyncio.create_task(
                    _run_monitor_task(market_price_event_loop(), stage="public"),
                    name="mexc-public-price-events",
                ),
                asyncio.create_task(
                    _run_monitor_task(
                        market_event_verifier_loop(notify=notify),
                        stage="event-critical",
                    ),
                    name="mexc-account-event-verifier",
                ),
                asyncio.create_task(
                    _run_monitor_task(
                        market_event_be_preflight_loop(notify=notify),
                        stage="event-critical",
                    ),
                    name="bingx-tp-be-backlog-preflight",
                ),
                asyncio.create_task(
                    _run_monitor_task(
                        market_event_admin_watch_loop(notify=notify),
                        stage="event-admin",
                    ),
                    name="mexc-market-event-admin-watch",
                ),
                asyncio.create_task(
                    _run_monitor_task(
                        market_event_rollout_loop(notify=notify),
                        stage="event-rollout",
                    ),
                    name="bingx-market-event-rollout",
                ),
                asyncio.create_task(
                    _run_monitor_task(critical_account_reconcile_loop(notify=notify), stage="critical"),
                    name="mexc-critical-account-reconcile",
                ),
                asyncio.create_task(
                    _run_monitor_task(
                        trade_group_housekeeping_loop(),
                        stage="housekeeping",
                    ),
                    name="mexc-trade-group-housekeeping",
                ),
                asyncio.create_task(
                    _run_monitor_task(
                        _restart_tp_be_startup_catchup_with_signal(
                            notify, startup_be_done
                        ),
                        stage="startup-be",
                    ),
                    name="bingx-restart-tp-be-catchup",
                ),
                asyncio.create_task(
                    _run_monitor_task(
                        _g64_gated_full_reconcile_worker_loop(
                            notify, startup_be_done
                        ),
                        stage="full",
                    ),
                    name="mexc-full-reconcile-fallback",
                ),
            ]
        )
        requested_verifiers = max(1, int(settings.EVENT_VERIFY_WORKERS))
        if db.is_postgres():
            admission = db.monitor_db_admission_snapshot()
            effective_verifiers = min(
                requested_verifiers,
                max(
                    1,
                    int(
                        admission.get("critical_advisory_monitor_limit")
                        or admission["advisory_monitor_limit"]
                    ),
                ),
            )
        else:
            effective_verifiers = requested_verifiers
        log.info(
            "Started event-driven monitoring: public price=%ss, verifier workers=%s "
            "effective=%s, admin-watch=1, critical=%ss, full fallback cooldown=%ss, housekeeping=60s",
            settings.MARKET_PRICE_POLL_INTERVAL_SEC,
            requested_verifiers,
            effective_verifiers,
            settings.MONITOR_CRITICAL_INTERVAL_SEC,
            settings.MONITOR_FULL_RECONCILE_INTERVAL_SEC,
        )
        return tasks

    workers = max(1, int(settings.MONITOR_WORKERS or 2))
    tasks.append(
        asyncio.create_task(
            _run_monitor_task(trade_protection_worker_loop(notify=notify), stage="legacy"),
            name="legacy-trade-protection",
        )
    )
    if workers >= 2:
        tasks.append(
            asyncio.create_task(
                _run_monitor_task(be_worker_loop(notify=notify), stage="legacy_be"),
                name="legacy-be-worker",
            )
        )
    log.warning(
        "Event-driven monitoring disabled; started %s legacy monitor task(s)",
        len(tasks),
    )
    return tasks
