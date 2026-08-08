from __future__ import annotations

import math
import asyncio
import contextvars
import hashlib
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from collections import Counter, defaultdict, OrderedDict
from typing import Any, Awaitable, Callable

from app.config import get_settings
from app.database import db
from app.exchanges.bingx.adapter import BingxAdapter as BingxAdapter
from app.services.be_monitor import process_be_monitor_once
from app.services.limit_tp_catchup import process_pending_limit_tp_catchup_once
from app.services.partial_tp_recovery import process_partial_tp_recovery_once
from app.services.position_lifecycle_guard import process_position_lifecycle_guard_once
from app.services.execution_exposure import execution_zero_exposure_confirmed
from app.services.market_event_evidence import (
    build_market_event_evidence_snapshot,
    decide_market_event_state_machine,
    execution_state_rows as market_event_execution_state_rows,
)
from app.services.market_event_exchange_context import MarketEventExchangeContext
from app.services.market_event_rollout import (
    g67_prepared_target_event_allowed,
    market_event_stage_allows_group,
)
from app.services.signal_analytics_ingress import (
    get_signal_analytics_tracking_symbols,
    submit_signal_analytics_price_snapshot,
)
from app.services.monitor_diagnostics import (
    diagnostic_span,
    mark_cycle_completed,
    record_monitor_error,
    record_wait,
    timed_db_call,
)
from app.services.durable_notifications import send_or_enqueue
from app.services.notification_style import card, esc, ensure_visual_card

log = logging.getLogger(__name__)
_CRITICAL_SCAN_CURSOR = 0
NotifyFn = Callable[[int, str], Awaitable[object] | object]

# Local de-duplication avoids issuing INSERT ... ON CONFLICT on every 1-second
# price tick. PostgreSQL remains the durable source of truth after redeploy.
_LOCAL_EVENT_SEEN: set[tuple[int, str]] = set()
_LOCAL_EVENT_SAFETY_RECHECK_AT: dict[tuple[int, str], float] = {}
_LOCAL_EVENT_SAFETY_RECHECKS_USED: dict[tuple[int, str], int] = {}
# When a durable row already contains one deferred re-cross, avoid polling the
# same row on every public-price tick while the current verifier finishes.
_LOCAL_EVENT_REARM_RETRY_AT: dict[tuple[int, str], float] = {}
# Safety rechecks are intentionally single-probe jobs.  A process restart may
# lose this optimization, but the durable event remains safe and simply falls
# back to the ordinary bounded retry sequence.
_LOCAL_EVENT_SINGLE_PROBE: set[tuple[int, str]] = set()
_GROUP_LOCKS: OrderedDict[int, asyncio.Lock] = OrderedDict()
_MAX_GROUP_LOCKS = 2000
_LAST_PRICE_PERSIST_MONO: dict[int, float] = {}
# Process-local last-price snapshot published by the healthy public market loop.
# The slow full BE fallback may reuse only fresh entries from this map.  This
# never replaces fair-price validation for manual BE and never survives a
# process restart.
_PUBLIC_LAST_PRICE_SNAPSHOT: dict[str, tuple[float, float]] = {}
_EVENT_VERIFY_SEMAPHORE: asyncio.Semaphore | None = None
_EVENT_VERIFY_SEMAPHORE_LIMIT = 0
_EVENT_ADMIN_VERIFY_SEMAPHORE: asyncio.Semaphore | None = None
_EVENT_ADMIN_VERIFY_SEMAPHORE_LIMIT = 0
PRICE_STREAM_DEGRADED = asyncio.Event()
_PUBLIC_DB_TIMEOUT_SEC = 5.0
_PRICE_PERSIST_TIMEOUT_SEC = 3.0
_EVENT_ENQUEUE_TIMEOUT_SEC = 5.0
_EVENT_DB_TIMEOUT_SEC = 10.0
# g39 Step 1 shadow persistence must never delay legacy event completion.
# Keep a small process-local cap and drop observational samples under pressure.
_MARKET_EVENT_EVIDENCE_SHADOW_MAX_TASKS = 32
_MARKET_EVENT_EVIDENCE_SHADOW_TASKS: set[asyncio.Task[None]] = set()
_MARKET_EVENT_EVIDENCE_SHADOW_EVENT_IDS: set[int] = set()
_MARKET_EVENT_EVIDENCE_SHADOW_DROP_LOG_MONO = 0.0
_CRITICAL_DB_TIMEOUT_SEC = 10.0
# v1.0.7g7h2f5g5b: trade-group recovery/closure is housekeeping, not part of
# the latency-sensitive public-price cycle.  One low-priority worker performs
# the writes on a post-run interval with bounded backoff.
_GROUP_HOUSEKEEPING_DB_TIMEOUT_SEC = 10.0
_GROUP_HOUSEKEEPING_INTERVAL_SEC = 60.0
_GROUP_HOUSEKEEPING_MAX_BACKOFF_SEC = 300.0
_GROUP_HOUSEKEEPING_INITIAL_DELAY_SEC = 30.0
_GROUP_CACHE_REFRESH_RETRY_SEC = 5.0
_GROUP_CACHE_VALIDATION_RETRY_SEC = 30.0
_GROUP_CACHE_STALE_DEGRADED_SEC = 30.0
_GROUP_CACHE_STALE_LOG_INTERVAL_SEC = 60.0
_LAST_PRICE_PERSIST_WARNING_MONO = 0.0
# The percentage band remains the fallback, but a genuine retreat must also
# clear several real BingX price ticks.  This prevents one-tick quote noise on
# low-priced contracts from re-arming the same ENTRY/TP/STOP over and over.
_EVENT_REARM_HYSTERESIS_RATIO = 0.0005
_EVENT_REARM_MIN_TICKS = 3
_EVENT_DEFERRED_REARM_RETRY_SEC = 5.0

# G61: when Railway restart catch-up creates a large MARKET EVENT backlog, a
# dedicated protective preflight may run the existing BE engine before the full
# TP lifecycle verifier reaches that event. It is idle under healthy latency,
# never owns the durable event, and never runs ahead of due/live STOP events.
_BE_PREFLIGHT_MIN_DUE_LAG_SEC = 15.0
_BE_PREFLIGHT_POLL_SEC = 1.00
_BE_PREFLIGHT_RETRY_EVENT_SEC = 15.0
_BE_PREFLIGHT_RECENT_TTL_SEC = 60.0
_BE_PREFLIGHT_RECENT: dict[int, float] = {}


def _terminal_review_allowed_for_group(settings: Any, group_id: int) -> bool:
    """Fail closed unless the active rollout stage explicitly owns the group."""

    return bool(
        getattr(settings, "MARKET_EVENT_TERMINAL_REVIEW_ENABLED", False)
    ) and market_event_stage_allows_group(
        str(getattr(settings, "MARKET_EVENT_ROLLOUT_STAGE", "off") or "off"),
        int(group_id or 0),
        int(
            getattr(settings, "MARKET_EVENT_MIGRATION_TARGET_GROUP_ID", 1541)
            or 1541
        ),
    )


# After the normal fast retries, keep two bounded late-propagation probes.
# This replaces unbounded re-creation of ordinary actions=0 events while
# preserving a safe window for delayed BingX fill/position propagation.
_EVENT_LATE_RECOVERY_DELAYS_SEC = (10.0, 30.0)
# A TP price touch can precede the actual owned TP fill.  Legacy/manual_required
# executions are intentionally excluded from the ordinary background BE scan,
# so completing their durable TP event after only the generic 10/30-second
# probes can leave the position under the old STOP until the hourly critical
# backoff wakes.  Keep one bounded protective watch alive for those rows:
# fast for the first minute, moderate for the next nine minutes, then once per
# minute for roughly two hours.  The watch still uses the existing strict
# ownership/read-back logic; it never treats price alone as a TP fill.
_MANUAL_TP_WATCH_FAST_ATTEMPTS = 12
_MANUAL_TP_WATCH_MEDIUM_ATTEMPTS = 48
_MANUAL_TP_WATCH_MAX_ATTEMPTS = 160
# After the fast/medium/slow window the event remains durable. Active STOP/TP
# work stays on the critical lane and is retried within 60 seconds; only proven
# no-entry administrative watches use the 300/900-second cadence below.
_EVENT_ESCALATED_WATCH_RETRY_SEC = 300.0
_EVENT_STUCK_WATCH_AFTER_SEC = 3600.0
_EVENT_STUCK_WATCH_RETRY_SEC = 900.0
_EVENT_STUCK_REMINDER_SEC = 21600.0
_EVENT_LEASE_HEARTBEAT_SEC = 30.0
# Administrative watches are explicitly preemptible by fresh STOP/TP/ENTRY work.
# They must discover the fenced lease-generation change quickly enough to release
# the shared per-group lock before the safety SLA expires.
_EVENT_ADMIN_LEASE_HEARTBEAT_SEC = 2.0
_EVENT_LEASE_EXTEND_SEC = 120.0
_EVENT_LEASE_ABORT_AFTER_SEC = 90.0
_EVENT_SLA_WARN_SEC = {"STOP": 30.0, "TP": 60.0, "ENTRY": 120.0}
# A consumed gate is allowed one low-frequency safety recheck even if price
# never leaves the neutral band.  This prevents an ENTRY/TP fill that arrives
# minutes later from waiting for the much slower full reconcile.
_EVENT_SAFETY_RECHECK_DELAYS_SEC = {
    "ENTRY": (60.0, 180.0),
    "TP": (60.0, 180.0),
    "STOP": (30.0, 90.0),
}
_MANUAL_FAST_LOG_STATE: OrderedDict[int, tuple[str, float]] = OrderedDict()
_MANUAL_FAST_LOG_STATE_MAX = 2048
_MANUAL_FAST_LOG_INTERVAL_SEC = 300.0
_MANUAL_FAST_SNAPSHOT_INTERVAL_SEC = 300.0
_MANUAL_FAST_SNAPSHOT_LAST_MONO = 0.0
_MANUAL_FAST_REASON_COUNTERS = {
    "invalid_execution_fields": "manual_fast_invalid_data",
    "malformed_payload": "manual_fast_invalid_data",
    "invalid_position_snapshot": "manual_fast_invalid_data",
    "api_unavailable": "manual_fast_no_api",
    "live_position": "manual_fast_live_position",
    "opposite_or_unknown_position": "manual_fast_opposite_position",
    "active_or_unknown_entry": "manual_fast_active_entry",
    "be_replacement_in_progress": "manual_fast_be_replacement",
    "unknown_stop_or_be_protection": "manual_fast_unknown_stop",
    "unknown_stop_or_cleanup_identity": "manual_fast_unknown_stop",
    "residual_active_or_unknown": "manual_fast_residual",
    "cleanup_unresolved": "manual_fast_cleanup_unresolved",
    "zero_proof_missing": "manual_fast_zero_proof_missing",
    "zero_proof_invalid_or_incomplete": "manual_fast_zero_proof_invalid",
}




class MarketEventLeaseLost(RuntimeError):
    """The durable event lease no longer belongs to this verifier."""

class MonitorDBOperationTimeout(asyncio.TimeoutError):
    """One explicitly bounded monitor DB operation exceeded its deadline."""

    def __init__(self, operation: str, timeout_sec: float):
        super().__init__(f"monitor DB operation timed out: {operation}")
        self.operation = str(operation)
        self.timeout_sec = float(timeout_sec)


async def _bounded_db_call(name: str, awaitable, *, timeout_sec: float):
    """Bound one monitor DB operation so a pool/row lock cannot stall a loop."""

    timeout = max(0.1, float(timeout_sec))
    try:
        return await asyncio.wait_for(
            timed_db_call(name, awaitable),
            timeout=timeout,
        )
    except asyncio.TimeoutError as exc:
        # Keep DB deadlines distinguishable from BingX/network timeouts raised
        # by the work performed after the DB snapshot has already loaded.
        raise MonitorDBOperationTimeout(name, timeout) from exc


async def _persist_market_event_evidence_shadow(
    *,
    event_id: int,
    group_id: int,
    event_label: str,
    snapshot: dict[str, Any],
    worker_id: str,
    lease_generation: int,
) -> None:
    """Persist one observational snapshot outside the live verifier path."""

    try:
        result = await _bounded_db_call(
            "market_event_shadow_evidence",
            db.record_market_event_shadow_evidence(
                event_id,
                snapshot=snapshot,
                execution_states=market_event_execution_state_rows(snapshot),
                worker_id=worker_id,
                lease_generation=lease_generation,
            ),
            timeout_sec=_EVENT_DB_TIMEOUT_SEC,
        )
        if bool(result.get("changed")):
            log.info(
                "MARKET_EVENT_EVIDENCE_SHADOW event_id=%s group_id=%s "
                "event=%s decision=%s reason=%s executions=%s",
                event_id,
                group_id,
                event_label,
                snapshot.get("shadow_decision"),
                snapshot.get("shadow_reason"),
                len(snapshot.get("executions") or []),
            )
    except asyncio.CancelledError:
        raise
    except Exception as shadow_exc:
        record_monitor_error("event_verify.evidence_shadow", shadow_exc)
        log.warning(
            "MARKET_EVENT_EVIDENCE_SHADOW_FAILED event_id=%s "
            "group_id=%s event=%s error_type=%s error=%s",
            event_id,
            group_id,
            event_label,
            type(shadow_exc).__name__,
            str(shadow_exc)[:300],
        )


def _schedule_market_event_evidence_shadow(
    *,
    event_id: int,
    group_id: int,
    event_label: str,
    snapshot: dict[str, Any],
    worker_id: str,
    lease_generation: int,
) -> bool:
    """Schedule bounded shadow persistence without delaying live safety work."""

    global _MARKET_EVENT_EVIDENCE_SHADOW_DROP_LOG_MONO
    safe_event_id = int(event_id or 0)
    if safe_event_id <= 0 or safe_event_id in _MARKET_EVENT_EVIDENCE_SHADOW_EVENT_IDS:
        return False
    if len(_MARKET_EVENT_EVIDENCE_SHADOW_TASKS) >= _MARKET_EVENT_EVIDENCE_SHADOW_MAX_TASKS:
        now = time.monotonic()
        if now - _MARKET_EVENT_EVIDENCE_SHADOW_DROP_LOG_MONO >= 60.0:
            _MARKET_EVENT_EVIDENCE_SHADOW_DROP_LOG_MONO = now
            log.warning(
                "MARKET_EVENT_EVIDENCE_SHADOW_DROPPED reason=task_cap active=%s cap=%s",
                len(_MARKET_EVENT_EVIDENCE_SHADOW_TASKS),
                _MARKET_EVENT_EVIDENCE_SHADOW_MAX_TASKS,
            )
        return False
    # Use a fresh context: the verifier task may carry a full-reconcile
    # advisory connection/contextvar that must never leak into an independent
    # background DB writer.
    task = asyncio.create_task(
        _persist_market_event_evidence_shadow(
            event_id=safe_event_id,
            group_id=group_id,
            event_label=event_label,
            snapshot=snapshot,
            worker_id=worker_id,
            lease_generation=lease_generation,
        ),
        name=f"market-event-evidence-shadow:{safe_event_id}",
        context=contextvars.Context(),
    )
    _MARKET_EVENT_EVIDENCE_SHADOW_TASKS.add(task)
    _MARKET_EVENT_EVIDENCE_SHADOW_EVENT_IDS.add(safe_event_id)

    def _done(done_task: asyncio.Task[None]) -> None:
        _MARKET_EVENT_EVIDENCE_SHADOW_TASKS.discard(done_task)
        _MARKET_EVENT_EVIDENCE_SHADOW_EVENT_IDS.discard(safe_event_id)

    task.add_done_callback(_done)
    return True


def _group_lock(group_id: int) -> asyncio.Lock:
    gid = int(group_id)
    lock = _GROUP_LOCKS.get(gid)
    if lock is not None:
        _GROUP_LOCKS.move_to_end(gid)
        return lock
    lock = asyncio.Lock()
    _GROUP_LOCKS[gid] = lock
    while len(_GROUP_LOCKS) > _MAX_GROUP_LOCKS:
        removed = False
        for old_gid, old_lock in list(_GROUP_LOCKS.items()):
            if old_gid == gid or old_lock.locked():
                continue
            _GROUP_LOCKS.pop(old_gid, None)
            removed = True
            break
        if not removed:
            break
    return lock


def _clear_local_event_state(group_id: int, event_key: str) -> None:
    """Drop process-local gate state for one terminal durable event."""

    local_key = (int(group_id), str(event_key))
    _LOCAL_EVENT_SEEN.discard(local_key)
    _LOCAL_EVENT_SAFETY_RECHECK_AT.pop(local_key, None)
    _LOCAL_EVENT_SAFETY_RECHECKS_USED.pop(local_key, None)
    _LOCAL_EVENT_REARM_RETRY_AT.pop(local_key, None)
    _LOCAL_EVENT_SINGLE_PROBE.discard(local_key)


def _prune_closed_group_state(active_group_ids: set[int]) -> None:
    """Drop process-local timestamps for trade groups no longer active.

    Durable event rows remain in the database. These maps are only loop-local
    acceleration state, so retaining closed group ids forever creates a slow
    memory leak in long-running Railway deployments without adding safety.
    """
    active_ids = {int(group_id) for group_id in active_group_ids if int(group_id) > 0}
    for group_id in tuple(_LAST_PRICE_PERSIST_MONO):
        if group_id not in active_ids:
            _LAST_PRICE_PERSIST_MONO.pop(group_id, None)
    for local_key in tuple(_LOCAL_EVENT_SAFETY_RECHECK_AT):
        if local_key[0] not in active_ids:
            _LOCAL_EVENT_SAFETY_RECHECK_AT.pop(local_key, None)
    for local_key in tuple(_LOCAL_EVENT_SAFETY_RECHECKS_USED):
        if local_key[0] not in active_ids:
            _LOCAL_EVENT_SAFETY_RECHECKS_USED.pop(local_key, None)
    for local_key in tuple(_LOCAL_EVENT_REARM_RETRY_AT):
        if local_key[0] not in active_ids:
            _LOCAL_EVENT_REARM_RETRY_AT.pop(local_key, None)
    _LOCAL_EVENT_SINGLE_PROBE.intersection_update(
        {key for key in _LOCAL_EVENT_SINGLE_PROBE if key[0] in active_ids}
    )

    # The durable database uniqueness constraint remains authoritative. Keep
    # the existing high-water guard for the larger event-key set, but prune it
    # against the same active group snapshot when that guard is reached.
    if len(_LOCAL_EVENT_SEEN) > 10000:
        _LOCAL_EVENT_SEEN.intersection_update(
            {(gid, key) for gid, key in _LOCAL_EVENT_SEEN if gid in active_ids}
        )


async def _persist_due_trade_group_prices(
    group_prices: list[tuple[int, float]],
    *,
    persist_now: float,
) -> int:
    """Persist all due group prices with one DB call.

    Process-local cadence markers are advanced only after the database batch
    succeeds.  A failed batch is therefore retried on the next public cycle
    instead of silently suppressing persistence for five seconds.
    """
    due = [
        (int(group_id), float(price))
        for group_id, price in group_prices
        if int(group_id) > 0
        and float(price) > 0
        and persist_now - _LAST_PRICE_PERSIST_MONO.get(int(group_id), 0.0) >= 5.0
    ]
    if not due:
        return 0

    global _LAST_PRICE_PERSIST_WARNING_MONO
    try:
        persisted = await _bounded_db_call(
            "update_trade_group_prices_batch",
            db.update_trade_group_prices_batch(due),
            timeout_sec=_PRICE_PERSIST_TIMEOUT_SEC,
        )
    except asyncio.TimeoutError:
        # last_price persistence is diagnostic/state convenience only. Event
        # detection must continue even when PostgreSQL is waiting on a lock.
        now = time.monotonic()
        if now - _LAST_PRICE_PERSIST_WARNING_MONO >= 10.0:
            log.error(
                "PUBLIC_PRICE_DB_TIMEOUT operation=last_price_batch rows=%s timeout_sec=%.1f; event evaluation continues",
                len(due),
                _PRICE_PERSIST_TIMEOUT_SEC,
            )
            _LAST_PRICE_PERSIST_WARNING_MONO = now
        return 0
    for group_id, _price in due:
        _LAST_PRICE_PERSIST_MONO[group_id] = persist_now
    return int(persisted or 0)


def _event_verify_concurrency_limit(requested: int) -> int:
    """Cap heavy event verification to PostgreSQL monitor admission capacity."""

    normalized = max(1, int(requested or 1))
    if not db.is_postgres():
        return normalized
    try:
        admission = db.monitor_db_admission_snapshot()
        advisory_limit = max(
            1,
            int(
                admission.get("critical_advisory_monitor_limit")
                or admission.get("advisory_monitor_limit")
                or 1
            ),
        )
        return min(normalized, advisory_limit)
    except Exception:
        return min(normalized, 2)


def _event_verify_semaphore(
    limit: int, *, admin_lane: bool = False
) -> asyncio.Semaphore:
    """Return an isolated verifier-capacity semaphore for one event lane.

    Administrative pending-limit watches are deliberately serialized and never
    consume permits reserved for fresh STOP/TP/ENTRY verification.  This is a
    process-local capacity guard; database leases remain the cross-process
    ownership authority.
    """
    global _EVENT_VERIFY_SEMAPHORE, _EVENT_VERIFY_SEMAPHORE_LIMIT
    global _EVENT_ADMIN_VERIFY_SEMAPHORE, _EVENT_ADMIN_VERIFY_SEMAPHORE_LIMIT

    normalized = 1 if admin_lane else max(1, int(limit or 1))
    if admin_lane:
        if (
            _EVENT_ADMIN_VERIFY_SEMAPHORE is None
            or _EVENT_ADMIN_VERIFY_SEMAPHORE_LIMIT != normalized
        ):
            _EVENT_ADMIN_VERIFY_SEMAPHORE = asyncio.Semaphore(normalized)
            _EVENT_ADMIN_VERIFY_SEMAPHORE_LIMIT = normalized
        return _EVENT_ADMIN_VERIFY_SEMAPHORE

    if _EVENT_VERIFY_SEMAPHORE is None or _EVENT_VERIFY_SEMAPHORE_LIMIT != normalized:
        _EVENT_VERIFY_SEMAPHORE = asyncio.Semaphore(normalized)
        _EVENT_VERIFY_SEMAPHORE_LIMIT = normalized
    return _EVENT_VERIFY_SEMAPHORE


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return int(default)


def _safe_individual_ticker_count(interval_sec: float) -> int:
    """Conservative burst cap for BingX's 10 public ticker calls / 2 sec."""
    interval = max(0.25, float(interval_sec or 0.25))
    # Requests for active symbols are issued together, so never burst more
    # than eight even when a long polling interval would allow that on average.
    return min(8, max(1, int(4.0 * interval)))


def _f(value: Any, default: float = 0.0) -> float:
    """Parse a finite non-negative exchange scalar without repairing corruption."""
    try:
        if value in (None, "") or isinstance(value, bool):
            return default
        parsed = float(value)
        return parsed if math.isfinite(parsed) and parsed >= 0 else default
    except (TypeError, ValueError, OverflowError):
        return default


def _publish_public_last_prices(
    prices: dict[str, dict[str, float]],
    active_symbols: list[str] | set[str] | tuple[str, ...],
    *,
    observed_at: float | None = None,
    authoritative_scope: bool = False,
) -> int:
    """Publish successful public *last* prices for bounded in-process reuse.

    Failed/omitted active symbols keep their previous last-good value until the
    freshness reader expires it. Symbols no longer active are removed
    immediately. Only the actual ticker ``last`` field is published so the full
    BE fallback preserves its existing ``fetch_last_price`` semantics.
    """

    now = time.monotonic() if observed_at is None else float(observed_at)
    requested_scope = {
        str(symbol or "").upper() for symbol in active_symbols if symbol
    }
    # A successful all-ticker response is authoritative and can prune symbols
    # no longer present. Partial/per-symbol reads preserve older last-good rows
    # until freshness expiry instead of clearing them on one omitted response.
    active = (
        requested_scope
        if authoritative_scope
        else requested_scope | set(_PUBLIC_LAST_PRICE_SNAPSHOT)
    )
    for symbol in tuple(_PUBLIC_LAST_PRICE_SNAPSHOT):
        if symbol not in active:
            _PUBLIC_LAST_PRICE_SNAPSHOT.pop(symbol, None)

    published = 0
    for symbol in active:
        row = prices.get(symbol) or {}
        last = _f(row.get("last"), 0.0)
        if last <= 0:
            continue
        _PUBLIC_LAST_PRICE_SNAPSHOT[symbol] = (float(last), now)
        published += 1
    return published


def get_fresh_public_last_price_snapshot(
    *,
    max_age_sec: float,
    require_healthy: bool = True,
) -> tuple[dict[str, float], dict[str, int]]:
    """Return a copy of fresh public last prices plus bounded diagnostics.

    This is deliberately fail-closed: when the public stream is degraded, or a
    symbol is older than ``max_age_sec``, it is omitted and the caller performs
    the original direct BingX read. No exchange-write read-back uses this data.
    """

    now = time.monotonic()
    max_age = max(0.1, float(max_age_sec or 0.1))
    total_rows = len(_PUBLIC_LAST_PRICE_SNAPSHOT)
    if require_healthy and PRICE_STREAM_DEGRADED.is_set():
        return {}, {
            "fresh_rows": 0,
            "stale_rows": total_rows,
            "total_rows": total_rows,
            "oldest_age_ms": 0,
            "newest_age_ms": 0,
            "stream_degraded": 1,
        }

    fresh: dict[str, float] = {}
    ages: list[float] = []
    stale_rows = 0
    for symbol, (price, observed_at) in tuple(_PUBLIC_LAST_PRICE_SNAPSHOT.items()):
        age = max(0.0, now - float(observed_at))
        if _f(price, 0.0) > 0 and age <= max_age:
            fresh[symbol] = float(price)
            ages.append(age)
        else:
            stale_rows += 1

    return fresh, {
        "fresh_rows": len(fresh),
        "stale_rows": stale_rows,
        "total_rows": total_rows,
        "oldest_age_ms": int(max(ages, default=0.0) * 1000),
        "newest_age_ms": int(min(ages, default=0.0) * 1000),
        "stream_degraded": 0,
    }


def _level_reached(side: str, event_type: str, current: float, level: float) -> bool:
    if current <= 0 or level <= 0:
        return False
    side_l = str(side or "").lower()
    kind = str(event_type or "").upper()
    if kind == "ENTRY":
        # Buy LIMIT fills at/below entry; sell LIMIT fills at/above entry.
        return current <= level if side_l == "long" else current >= level
    if kind == "TP":
        return current >= level if side_l == "long" else current <= level
    if kind == "STOP":
        return current <= level if side_l == "long" else current >= level
    return False


def _level_rearm_ready(
    side: str,
    event_type: str,
    current: float,
    level: float,
    *,
    price_tick: float = 0.0,
    hysteresis_ratio: float = _EVENT_REARM_HYSTERESIS_RATIO,
    min_ticks: int = _EVENT_REARM_MIN_TICKS,
) -> bool:
    """Return True only after price has clearly left a triggered level.

    The retreat band is the larger of five basis points and several real BingX
    price ticks.  The tick-aware floor is essential for cheap contracts where
    a fixed percentage can be smaller than one displayed quote step.
    """
    if current <= 0 or level <= 0:
        return False
    ratio = max(0.0, min(float(hysteresis_ratio or 0.0), 0.05))
    try:
        tick = float(price_tick or 0.0)
    except (TypeError, ValueError, OverflowError):
        tick = 0.0
    if not math.isfinite(tick) or tick < 0:
        tick = 0.0
    ticks = max(1, min(int(min_ticks or 1), 100))
    band = max(level * ratio, tick * ticks)
    epsilon = max(abs(level) * 1e-12, tick * 1e-9, 1e-12)
    upper = level + band
    lower = max(0.0, level - band)
    side_l = str(side or "").lower()
    kind = str(event_type or "").upper()
    if side_l not in {"long", "short"}:
        return False
    if kind == "ENTRY":
        return (
            current >= upper - epsilon
            if side_l == "long"
            else current <= lower + epsilon
        )
    if kind == "TP":
        return (
            current <= lower + epsilon
            if side_l == "long"
            else current >= upper - epsilon
        )
    if kind == "STOP":
        return (
            current >= upper - epsilon
            if side_l == "long"
            else current <= lower + epsilon
        )
    return False


def _event_safety_recheck_due(
    local_key: tuple[int, str], event_type: str, *, now: float | None = None
) -> bool:
    if str(event_type or "").upper() not in _EVENT_SAFETY_RECHECK_DELAYS_SEC:
        return False
    due_at = _LOCAL_EVENT_SAFETY_RECHECK_AT.get(local_key)
    if due_at is None:
        return False
    current = time.monotonic() if now is None else float(now)
    return current >= due_at


def _schedule_event_safety_recheck(
    local_key: tuple[int, str], event_type: str, *, now: float | None = None
) -> None:
    delays = _EVENT_SAFETY_RECHECK_DELAYS_SEC.get(str(event_type or "").upper(), ())
    used = max(0, int(_LOCAL_EVENT_SAFETY_RECHECKS_USED.get(local_key, 0)))
    if used >= len(delays):
        _LOCAL_EVENT_SAFETY_RECHECK_AT.pop(local_key, None)
        return
    current = time.monotonic() if now is None else float(now)
    _LOCAL_EVENT_SAFETY_RECHECK_AT[local_key] = current + float(delays[used])


async def _enqueue_once(
    *,
    group_id: int,
    event_key: str,
    event_type: str,
    level_index: int,
    trigger_price: float,
    observed_price: float,
    safety_recheck: bool = False,
) -> bool:
    local_key = (int(group_id), str(event_key))
    if local_key in _LOCAL_EVENT_SEEN:
        return False
    _LOCAL_EVENT_REARM_RETRY_AT.pop(local_key, None)
    if safety_recheck:
        _LOCAL_EVENT_SINGLE_PROBE.add(local_key)
    else:
        _LOCAL_EVENT_SINGLE_PROBE.discard(local_key)
    try:
        inserted = await _bounded_db_call(
            "enqueue_market_event",
            db.enqueue_market_event(
                trade_group_id=group_id,
                event_key=event_key,
                event_type=event_type,
                level_index=level_index,
                trigger_price=trigger_price,
                observed_price=observed_price,
            ),
            timeout_sec=_EVENT_ENQUEUE_TIMEOUT_SEC,
        )
    except asyncio.TimeoutError:
        # Do not mark the local key. The next public tick retries the same
        # durable event instead of losing it or freezing the price worker.
        log.error(
            "PUBLIC_PRICE_DB_TIMEOUT operation=enqueue_market_event group_id=%s event=%s timeout_sec=%.1f",
            group_id,
            event_key,
            _EVENT_ENQUEUE_TIMEOUT_SEC,
        )
        _LOCAL_EVENT_SINGLE_PROBE.discard(local_key)
        return False
    except asyncio.CancelledError:
        _LOCAL_EVENT_SINGLE_PROBE.discard(local_key)
        raise
    except Exception:
        # The durable insert did not return a trustworthy outcome.  Never leave
        # process-local one-shot state behind for a row that may not exist.
        _LOCAL_EVENT_SINGLE_PROBE.discard(local_key)
        raise
    # Mark locally even when DB reports an existing durable row. This avoids a
    # conflict attempt on every subsequent tick after a process restart.
    _LOCAL_EVENT_SEEN.add(local_key)
    if safety_recheck:
        _LOCAL_EVENT_SAFETY_RECHECKS_USED[local_key] = (
            max(0, int(_LOCAL_EVENT_SAFETY_RECHECKS_USED.get(local_key, 0))) + 1
        )
        # Only a row inserted by this process is known to be the intended
        # single safety probe.  A concurrent/existing durable row keeps the
        # ordinary retry policy rather than being prematurely completed.
        if inserted:
            _LOCAL_EVENT_SINGLE_PROBE.add(local_key)
        else:
            _LOCAL_EVENT_SINGLE_PROBE.discard(local_key)
    else:
        _LOCAL_EVENT_SINGLE_PROBE.discard(local_key)
        _LOCAL_EVENT_SAFETY_RECHECKS_USED.setdefault(local_key, 0)
    _schedule_event_safety_recheck(local_key, event_type)
    if inserted:
        log.info(
            "market event queued group_id=%s event=%s trigger=%s observed=%s",
            group_id,
            event_key,
            trigger_price,
            observed_price,
        )
    return inserted


async def _rearm_once(
    *, group_id: int, event_key: str, reset_safety_budget: bool = True
) -> str:
    """Durably restore one gate and return the exact durable outcome.

    ``deferred_exists`` deliberately keeps the local cooldown closed: one
    follow-up crossing is already stored for the current pending/processing
    verifier, so further retreat/re-cross noise must not overwrite it.
    """
    local_key = (int(group_id), str(event_key))
    if local_key not in _LOCAL_EVENT_SEEN:
        return "not_seen"
    now = time.monotonic()
    retry_at = _LOCAL_EVENT_REARM_RETRY_AT.get(local_key)
    if retry_at is not None and now < retry_at:
        return "deferred_cooldown"
    try:
        state = await _bounded_db_call(
            "rearm_market_event_state",
            db.rearm_market_event_state(
                trade_group_id=group_id,
                event_key=event_key,
            ),
            timeout_sec=_EVENT_ENQUEUE_TIMEOUT_SEC,
        )
    except asyncio.TimeoutError:
        log.error(
            "PUBLIC_PRICE_DB_TIMEOUT operation=rearm_market_event group_id=%s event=%s timeout_sec=%.1f",
            group_id,
            event_key,
            _EVENT_ENQUEUE_TIMEOUT_SEC,
        )
        _LOCAL_EVENT_SAFETY_RECHECK_AT[local_key] = time.monotonic() + 5.0
        _LOCAL_EVENT_REARM_RETRY_AT[local_key] = (
            time.monotonic() + _EVENT_DEFERRED_REARM_RETRY_SEC
        )
        return "timeout"

    state = str(state or "blocked")
    if state in {"rearmed", "already_armed", "missing"}:
        # ``already_armed`` covers an ambiguous prior DB timeout safely;
        # ``missing`` lets the next genuine cross recreate the durable row.
        _LOCAL_EVENT_SEEN.discard(local_key)
        _LOCAL_EVENT_SAFETY_RECHECK_AT.pop(local_key, None)
        _LOCAL_EVENT_REARM_RETRY_AT.pop(local_key, None)
        _LOCAL_EVENT_SINGLE_PROBE.discard(local_key)
        if reset_safety_budget:
            _LOCAL_EVENT_SAFETY_RECHECKS_USED.pop(local_key, None)
    if state == "deferred_exists":
        _LOCAL_EVENT_REARM_RETRY_AT[local_key] = (
            time.monotonic() + _EVENT_DEFERRED_REARM_RETRY_SEC
        )
    elif state == "automation_disabled":
        # MANUAL_REVIEW is terminal for automatic price rearming. Keep the
        # process-local gate closed without hammering PostgreSQL on each tick.
        _LOCAL_EVENT_REARM_RETRY_AT[local_key] = time.monotonic() + 300.0
    elif state == "blocked":
        _LOCAL_EVENT_REARM_RETRY_AT[local_key] = time.monotonic() + 1.0
    if state == "rearmed":
        log.debug("market event rearmed group_id=%s event=%s", group_id, event_key)
    return state


async def _enqueue_batch_once(
    specs: list[dict[str, Any]],
) -> dict[tuple[int, str], bool]:
    """Persist one public-tick enqueue set in a single bounded DB scope."""

    normalized: dict[tuple[int, str], dict[str, Any]] = {}
    provisional_single_probe: set[tuple[int, str]] = set()
    for raw in specs or []:
        if not isinstance(raw, dict):
            continue
        local_key = (
            int(raw.get("group_id") or 0),
            str(raw.get("event_key") or ""),
        )
        if local_key[0] <= 0 or not local_key[1] or local_key in _LOCAL_EVENT_SEEN:
            continue
        spec = dict(raw)
        normalized[local_key] = spec
        _LOCAL_EVENT_REARM_RETRY_AT.pop(local_key, None)
        if bool(spec.get("safety_recheck")):
            _LOCAL_EVENT_SINGLE_PROBE.add(local_key)
            provisional_single_probe.add(local_key)
        else:
            _LOCAL_EVENT_SINGLE_PROBE.discard(local_key)
    if not normalized:
        return {}

    payload = [
        {
            "trade_group_id": key[0],
            "event_key": key[1],
            "event_type": str(spec.get("event_type") or ""),
            "level_index": int(spec.get("level_index") or 0),
            "trigger_price": float(spec.get("trigger_price") or 0.0),
            "observed_price": float(spec.get("observed_price") or 0.0),
        }
        for key, spec in normalized.items()
    ]
    try:
        inserted_by_key = await _bounded_db_call(
            "enqueue_market_events_batch",
            db.enqueue_market_events_batch(payload),
            timeout_sec=_EVENT_ENQUEUE_TIMEOUT_SEC,
        )
    except asyncio.TimeoutError:
        for local_key in provisional_single_probe:
            _LOCAL_EVENT_SINGLE_PROBE.discard(local_key)
        log.error(
            "PUBLIC_PRICE_DB_TIMEOUT operation=enqueue_market_events_batch rows=%s timeout_sec=%.1f",
            len(payload),
            _EVENT_ENQUEUE_TIMEOUT_SEC,
        )
        return {}
    except asyncio.CancelledError:
        for local_key in provisional_single_probe:
            _LOCAL_EVENT_SINGLE_PROBE.discard(local_key)
        raise
    except Exception:
        for local_key in provisional_single_probe:
            _LOCAL_EVENT_SINGLE_PROBE.discard(local_key)
        raise

    results: dict[tuple[int, str], bool] = {}
    for local_key, spec in normalized.items():
        inserted = bool((inserted_by_key or {}).get(local_key, False))
        results[local_key] = inserted
        _LOCAL_EVENT_SEEN.add(local_key)
        safety_recheck = bool(spec.get("safety_recheck"))
        if safety_recheck:
            _LOCAL_EVENT_SAFETY_RECHECKS_USED[local_key] = (
                max(0, int(_LOCAL_EVENT_SAFETY_RECHECKS_USED.get(local_key, 0))) + 1
            )
            if inserted:
                _LOCAL_EVENT_SINGLE_PROBE.add(local_key)
            else:
                _LOCAL_EVENT_SINGLE_PROBE.discard(local_key)
        else:
            _LOCAL_EVENT_SINGLE_PROBE.discard(local_key)
            _LOCAL_EVENT_SAFETY_RECHECKS_USED.setdefault(local_key, 0)
        _schedule_event_safety_recheck(
            local_key, str(spec.get("event_type") or "")
        )
        if inserted:
            log.info(
                "market event queued group_id=%s event=%s trigger=%s observed=%s",
                local_key[0],
                local_key[1],
                spec.get("trigger_price"),
                spec.get("observed_price"),
            )
    return results


async def _rearm_batch_once(
    specs: list[dict[str, Any]],
) -> dict[tuple[int, str], str]:
    """Restore many local/durable gates using one bounded DB scope."""

    results: dict[tuple[int, str], str] = {}
    eligible: dict[tuple[int, str], bool] = {}
    now = time.monotonic()
    for raw in specs or []:
        if not isinstance(raw, dict):
            continue
        local_key = (
            int(raw.get("group_id") or 0),
            str(raw.get("event_key") or ""),
        )
        if local_key not in _LOCAL_EVENT_SEEN:
            results[local_key] = "not_seen"
            continue
        retry_at = _LOCAL_EVENT_REARM_RETRY_AT.get(local_key)
        if retry_at is not None and now < retry_at:
            results[local_key] = "deferred_cooldown"
            continue
        eligible[local_key] = bool(raw.get("reset_safety_budget", True))
    if not eligible:
        return results

    try:
        states = await _bounded_db_call(
            "rearm_market_event_states_batch",
            db.rearm_market_event_states_batch(list(eligible)),
            timeout_sec=_EVENT_ENQUEUE_TIMEOUT_SEC,
        )
    except asyncio.TimeoutError:
        current = time.monotonic()
        for local_key in eligible:
            _LOCAL_EVENT_SAFETY_RECHECK_AT[local_key] = current + 5.0
            _LOCAL_EVENT_REARM_RETRY_AT[local_key] = (
                current + _EVENT_DEFERRED_REARM_RETRY_SEC
            )
            results[local_key] = "timeout"
        log.error(
            "PUBLIC_PRICE_DB_TIMEOUT operation=rearm_market_event_states_batch rows=%s timeout_sec=%.1f",
            len(eligible),
            _EVENT_ENQUEUE_TIMEOUT_SEC,
        )
        return results

    current = time.monotonic()
    for local_key, reset_safety_budget in eligible.items():
        state = str((states or {}).get(local_key) or "blocked")
        results[local_key] = state
        if state in {"rearmed", "already_armed", "missing"}:
            _LOCAL_EVENT_SEEN.discard(local_key)
            _LOCAL_EVENT_SAFETY_RECHECK_AT.pop(local_key, None)
            _LOCAL_EVENT_REARM_RETRY_AT.pop(local_key, None)
            _LOCAL_EVENT_SINGLE_PROBE.discard(local_key)
            if reset_safety_budget:
                _LOCAL_EVENT_SAFETY_RECHECKS_USED.pop(local_key, None)
        if state == "deferred_exists":
            _LOCAL_EVENT_REARM_RETRY_AT[local_key] = (
                current + _EVENT_DEFERRED_REARM_RETRY_SEC
            )
        elif state == "automation_disabled":
            _LOCAL_EVENT_REARM_RETRY_AT[local_key] = current + 300.0
        elif state == "blocked":
            _LOCAL_EVENT_REARM_RETRY_AT[local_key] = current + 1.0
        if state == "rearmed":
            log.debug(
                "market event rearmed group_id=%s event=%s",
                local_key[0],
                local_key[1],
            )
    return results


class ActiveTradeGroupSnapshotError(ValueError):
    """The DB returned a structurally unsafe active-group snapshot."""


def _group_cache_retry_delay_sec(error: Exception | None = None) -> float:
    """Return the post-failure DB refresh cooldown.

    Network/acquire failures retain the five-second recovery cadence. A
    deterministic malformed snapshot receives a longer cooldown so one corrupt
    row cannot generate a traceback and DB-read storm every public-price tick.
    The delay is intentionally independent of the public polling interval.
    """

    base = max(5.0, float(_GROUP_CACHE_REFRESH_RETRY_SEC))
    if isinstance(error, ActiveTradeGroupSnapshotError):
        return max(base, float(_GROUP_CACHE_VALIDATION_RETRY_SEC))
    return base


def _group_cache_is_stale(
    *,
    refresh_failed: bool,
    initialized: bool,
    cache_age_sec: float,
    refresh_every_sec: float,
) -> bool:
    """Return whether cached group membership is too old to call healthy."""

    stale_after = max(
        float(_GROUP_CACHE_STALE_DEGRADED_SEC),
        max(1.0, float(refresh_every_sec)) * 3.0,
    )
    if not refresh_failed or not initialized:
        return False
    try:
        age = float(cache_age_sec)
    except (TypeError, ValueError, OverflowError):
        return True
    if not math.isfinite(age):
        return True
    return age >= stale_after


def _validated_active_trade_groups_snapshot(rows: Any) -> list[dict[str, Any]]:
    """Validate one authoritative active-group snapshot before replacing cache.

    ``db.active_trade_groups`` is a strict list-of-dicts contract. Treating a
    malformed payload as empty, or accepting a partially normalized trigger row,
    could silently stop or misroute ENTRY/TP/STOP event monitoring. Reject the
    whole snapshot and preserve the last known-good cache instead.
    """

    if not isinstance(rows, list):
        raise ActiveTradeGroupSnapshotError(
            "active_trade_groups returned non-list payload: "
            f"{type(rows).__name__}"
        )

    normalized: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ActiveTradeGroupSnapshotError(
                "active_trade_groups row is not a mapping: "
                f"index={index} type={type(row).__name__}"
            )

        raw_id = row.get("id")
        if isinstance(raw_id, bool) or not isinstance(raw_id, int) or raw_id <= 0:
            raise ActiveTradeGroupSnapshotError(
                "active_trade_groups row has invalid id: "
                f"index={index} type={type(raw_id).__name__}"
            )
        group_id = raw_id
        symbol = str(row.get("symbol") or "").strip().upper()
        side = str(row.get("side") or "").strip().lower()
        entry_type = str(row.get("entry_type") or "").strip().upper()
        if (
            not symbol
            or len(symbol) > 40
            or not symbol.isascii()
            or not symbol.endswith("USDT")
            or not symbol.isalnum()
            or side not in {"long", "short"}
        ):
            raise ActiveTradeGroupSnapshotError(
                "active_trade_groups row has invalid symbol/side: "
                f"index={index} symbol={bool(symbol)} side={side!r}"
            )
        if entry_type not in {"LIMIT", "MARKET"}:
            raise ActiveTradeGroupSnapshotError(
                "active_trade_groups row has invalid entry_type: "
                f"index={index} entry_type={entry_type!r}"
            )
        if group_id in seen_ids:
            raise ActiveTradeGroupSnapshotError(
                f"active_trade_groups returned duplicate id: {group_id}"
            )

        raw_planned_entry = row.get("planned_entry")
        raw_stop_price = row.get("stop_price")
        if isinstance(raw_planned_entry, (bool, str, bytes)) or isinstance(
            raw_stop_price, (bool, str, bytes)
        ):
            raise ActiveTradeGroupSnapshotError(
                "active_trade_groups row has non-numeric trigger price: "
                f"index={index}"
            )
        try:
            planned_entry = float(raw_planned_entry or 0.0)
            stop_price = float(raw_stop_price)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ActiveTradeGroupSnapshotError(
                "active_trade_groups row has invalid trigger price: "
                f"index={index}"
            ) from exc
        if (
            not math.isfinite(planned_entry)
            or not math.isfinite(stop_price)
            or stop_price <= 0
            or (entry_type == "LIMIT" and planned_entry <= 0)
            or (entry_type == "MARKET" and planned_entry < 0)
        ):
            raise ActiveTradeGroupSnapshotError(
                "active_trade_groups row has unsafe trigger price: "
                f"index={index}"
            )

        raw_targets_json = row.get("targets_json")
        if not isinstance(raw_targets_json, str):
            raise ActiveTradeGroupSnapshotError(
                "active_trade_groups row has non-text targets_json: "
                f"index={index} type={type(raw_targets_json).__name__}"
            )
        try:
            raw_targets = json.loads(raw_targets_json)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ActiveTradeGroupSnapshotError(
                "active_trade_groups row has malformed targets_json: "
                f"index={index}"
            ) from exc
        if (
            not isinstance(raw_targets, list)
            or not raw_targets
            or len(raw_targets) > 20
        ):
            raise ActiveTradeGroupSnapshotError(
                "active_trade_groups row has invalid target count/type: "
                f"index={index}"
            )
        targets: list[float] = []
        for target_index, target in enumerate(raw_targets):
            if isinstance(target, (bool, str, bytes)):
                raise ActiveTradeGroupSnapshotError(
                    "active_trade_groups target is non-numeric: "
                    f"index={index} target_index={target_index}"
                )
            try:
                value = float(target)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ActiveTradeGroupSnapshotError(
                    "active_trade_groups target is invalid: "
                    f"index={index} target_index={target_index}"
                ) from exc
            if not math.isfinite(value) or value <= 0:
                raise ActiveTradeGroupSnapshotError(
                    "active_trade_groups target is unsafe: "
                    f"index={index} target_index={target_index}"
                )
            targets.append(value)

        market_without_entry = entry_type == "MARKET" and planned_entry <= 0
        if side == "long":
            if any(left >= right for left, right in zip(targets, targets[1:])):
                raise ActiveTradeGroupSnapshotError(
                    "active_trade_groups LONG targets are not increasing: "
                    f"index={index}"
                )
            if market_without_entry:
                unsafe_direction = any(target <= stop_price for target in targets)
            else:
                unsafe_direction = (
                    stop_price >= planned_entry
                    or any(target <= planned_entry for target in targets)
                )
        else:
            if any(left <= right for left, right in zip(targets, targets[1:])):
                raise ActiveTradeGroupSnapshotError(
                    "active_trade_groups SHORT targets are not decreasing: "
                    f"index={index}"
                )
            if market_without_entry:
                unsafe_direction = any(target >= stop_price for target in targets)
            else:
                unsafe_direction = (
                    stop_price <= planned_entry
                    or any(target >= planned_entry for target in targets)
                )
        if unsafe_direction:
            raise ActiveTradeGroupSnapshotError(
                "active_trade_groups row has inconsistent trade direction: "
                f"index={index}"
            )

        seen_ids.add(group_id)
        normalized_row = dict(row)
        # Store the canonical forms that downstream evaluation consumes. Merely
        # validating stripped/lower-cased temporaries while caching the original
        # whitespace/case would allow a valid-looking row to be skipped later.
        normalized_row.update(
            {
                "id": group_id,
                "symbol": symbol,
                "side": side,
                "entry_type": entry_type,
                "planned_entry": planned_entry,
                "stop_price": stop_price,
                "targets_json": json.dumps(targets, separators=(",", ":")),
            }
        )
        normalized.append(normalized_row)
    return normalized


async def _refresh_active_trade_groups_cache(
    current_cache: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], bool, Exception | None]:
    """Fetch active groups without discarding the last known-good snapshot.

    A temporary PostgreSQL acquire/reset timeout or malformed DB result must not
    stop public price evaluation for already-known groups.  Only a successful,
    validated read is allowed to replace the cache, including a valid empty list.
    """

    try:
        rows = await _bounded_db_call(
            "active_trade_groups",
            db.active_trade_groups(limit=1000),
            timeout_sec=_PUBLIC_DB_TIMEOUT_SEC,
        )
        normalized = _validated_active_trade_groups_snapshot(rows)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        return current_cache, False, exc
    return normalized, True, None


async def process_trade_group_housekeeping_once() -> tuple[int, int]:
    """Recover stale building groups and close inactive groups once.

    These are maintenance writes.  They are deliberately separate from the
    public price loop so a slow UPDATE or a PostgreSQL reconnect cannot delay
    price polling, event detection, STOP/TP verification or critical reconcile.
    """

    recovered = int(
        await _bounded_db_call(
            "recover_stale_building_trade_groups",
            db.recover_stale_building_trade_groups(stale_after_sec=600),
            timeout_sec=_GROUP_HOUSEKEEPING_DB_TIMEOUT_SEC,
        )
    )
    closed = int(
        await _bounded_db_call(
            "close_inactive_trade_groups",
            db.close_inactive_trade_groups(),
            timeout_sec=_GROUP_HOUSEKEEPING_DB_TIMEOUT_SEC,
        )
    )
    return recovered, closed


async def trade_group_housekeeping_loop(
    *,
    initial_delay_sec: float = _GROUP_HOUSEKEEPING_INITIAL_DELAY_SEC,
    interval_sec: float = _GROUP_HOUSEKEEPING_INTERVAL_SEC,
    max_backoff_sec: float = _GROUP_HOUSEKEEPING_MAX_BACKOFF_SEC,
) -> None:
    """Run one non-overlapping low-priority trade-group maintenance loop.

    The delay is always measured *after* an iteration completes.  A slow or
    timed-out maintenance pass therefore cannot immediately create another DB
    burst.  Expected DB timeouts use compact diagnostics and exponential
    backoff; unexpected failures remain visible with a traceback.
    """

    interval = max(5.0, float(interval_sec))
    max_backoff = max(interval, float(max_backoff_sec))
    if initial_delay_sec > 0:
        await asyncio.sleep(float(initial_delay_sec))

    consecutive_failures = 0
    while True:
        next_delay = interval
        with diagnostic_span("cycle.group_housekeeping", emit=True) as span:
            try:
                recovered, closed = await process_trade_group_housekeeping_once()
                consecutive_failures = 0
                span.set("result", recovered + closed)
                span.set("recovered", recovered)
                span.set("closed", closed)
                span.set("failures", 0)
                log.info(
                    "trade group housekeeping recovered=%s closed=%s next_run_sec=%s",
                    recovered,
                    closed,
                    int(interval),
                )
            except asyncio.CancelledError:
                raise
            except TimeoutError:
                span.errors += 1
                consecutive_failures += 1
                next_delay = min(
                    max_backoff,
                    interval * (2 ** min(consecutive_failures, 3)),
                )
                span.set("result", 0)
                span.set("failures", consecutive_failures)
                span.set("next_run_sec", int(next_delay))
                log.warning(
                    "TRADE_GROUP_HOUSEKEEPING_DB_TIMEOUT failures=%s retry_after_sec=%s",
                    consecutive_failures,
                    int(next_delay),
                )
            except Exception as exc:
                span.errors += 1
                consecutive_failures += 1
                next_delay = min(
                    max_backoff,
                    interval * (2 ** min(consecutive_failures, 3)),
                )
                span.set("result", 0)
                span.set("failures", consecutive_failures)
                span.set("next_run_sec", int(next_delay))
                log.exception(
                    "trade group housekeeping failed error_type=%s retry_after_sec=%s",
                    type(exc).__name__,
                    int(next_delay),
                )
        # Post-run cooldown: never subtract the iteration duration.
        await asyncio.sleep(next_delay)


def _be_preflight_trigger_snapshot(row: dict[str, Any]) -> int | None:
    """Return the immutable per-execution BE trigger; None means legacy."""

    raw_payload = row.get("exchange_order_ids_json")
    if isinstance(raw_payload, dict):
        payload = raw_payload
    else:
        try:
            payload = json.loads(str(raw_payload or "{}"))
        except Exception:
            return None
    if not isinstance(payload, dict):
        return None
    raw = payload.get("be_trigger_tp_index")
    if raw in (None, ""):
        return None
    if isinstance(raw, bool):
        return 0
    try:
        parsed = float(raw)
    except (TypeError, ValueError, OverflowError):
        return 0
    if not math.isfinite(parsed) or not parsed.is_integer():
        return 0
    value = int(parsed)
    return value if value in {1, 2, 3} else 0


def _be_preflight_rows_for_event(
    rows: list[dict[str, Any]], *, event_level_index: int
) -> list[dict[str, Any]]:
    """Filter before private BingX reads using the frozen BE trigger snapshot.

    Legacy rows with no frozen trigger stay eligible so the BE monitor can use
    its existing compatibility fallback. Explicitly disabled BE and triggers
    above the current TP are skipped without touching the exchange.
    """

    level = max(0, int(event_level_index or 0))
    if level <= 0:
        return []
    eligible: list[dict[str, Any]] = []
    for row in rows:
        trigger = _be_preflight_trigger_snapshot(row)
        if trigger is None or (trigger > 0 and level >= trigger):
            eligible.append(row)
    return eligible


def _be_preflight_recently_attempted(event_id: int, *, now: float) -> bool:
    expired = [
        key
        for key, attempted_at in _BE_PREFLIGHT_RECENT.items()
        if now - attempted_at >= _BE_PREFLIGHT_RECENT_TTL_SEC
    ]
    for key in expired:
        _BE_PREFLIGHT_RECENT.pop(key, None)
    attempted_at = _BE_PREFLIGHT_RECENT.get(int(event_id))
    return bool(
        attempted_at is not None
        and now - attempted_at < _BE_PREFLIGHT_RETRY_EVENT_SEC
    )


async def market_event_be_preflight_loop(notify: NotifyFn | None = None) -> None:
    """Run BE-only protection for overdue TP events during verifier backlog.

    Healthy TP events are normally claimed before the fifteen-second threshold, so
    this lane performs no work. During restart catch-up it peeks a small oldest
    due TP set after STOP work has cleared, filters linked executions by their
    immutable BE trigger snapshot, and invokes only ``process_be_monitor_once``.
    The normal verifier remains the sole owner of event attempts, lifecycle
    accounting, notifications and terminal outcome.
    """

    settings = get_settings()
    verify_limit = _event_verify_concurrency_limit(int(settings.EVENT_VERIFY_WORKERS))
    verify_sem = _event_verify_semaphore(verify_limit, admin_lane=False)

    while True:
        # With a single verifier slot, a BE preflight could delay a newly due
        # STOP. Fail closed and leave that capacity exclusively to the durable
        # verifier. Production PostgreSQL currently has two critical slots.
        if verify_limit < 2:
            await asyncio.sleep(_BE_PREFLIGHT_POLL_SEC)
            continue
        try:
            candidates = await _bounded_db_call(
                "peek_due_tp_market_events_for_be_preflight",
                db.peek_due_tp_market_events_for_be_preflight(
                    limit=4,
                    min_due_lag_sec=_BE_PREFLIGHT_MIN_DUE_LAG_SEC,
                ),
                timeout_sec=_EVENT_DB_TIMEOUT_SEC,
            )
            if not candidates:
                await asyncio.sleep(_BE_PREFLIGHT_POLL_SEC)
                continue

            processed_candidate = False
            for raw_event in candidates:
                event = dict(raw_event)
                event_id = int(event.get("id") or 0)
                group_id = int(event.get("trade_group_id") or 0)
                event_level_index = int(event.get("level_index") or 0)
                observed_price = _f(event.get("observed_price"), 0.0)
                due_lag_sec = max(0.0, _f(event.get("due_lag_sec"), 0.0))
                now = time.monotonic()
                if (
                    event_id <= 0
                    or group_id <= 0
                    or event_level_index <= 0
                    or observed_price <= 0
                    or _be_preflight_recently_attempted(event_id, now=now)
                ):
                    continue

                rows = await _bounded_db_call(
                    "trade_group_executions_be_preflight",
                    db.trade_group_executions(group_id, active_only=True, limit=500),
                    timeout_sec=_EVENT_DB_TIMEOUT_SEC,
                )
                eligible = _be_preflight_rows_for_event(
                    list(rows or []), event_level_index=event_level_index
                )
                if not eligible:
                    _BE_PREFLIGHT_RECENT[event_id] = now
                    log.debug(
                        "TP_BE_BACKLOG_PREFLIGHT phase=skip event_id=%s group_id=%s "
                        "tp=%s reason=no_be_eligible_execution",
                        event_id,
                        group_id,
                        event_level_index,
                    )
                    continue

                by_user: dict[int, list[dict[str, Any]]] = defaultdict(list)
                for row in eligible:
                    uid = int(row.get("user_id") or 0)
                    if uid > 0:
                        by_user[uid].append(row)
                if not by_user:
                    _BE_PREFLIGHT_RECENT[event_id] = now
                    continue

                stop_waiting = await _bounded_db_call(
                    "has_due_or_live_stop_market_event_be_preflight",
                    db.has_due_or_live_stop_market_event(),
                    timeout_sec=_EVENT_DB_TIMEOUT_SEC,
                )
                if stop_waiting:
                    log.info(
                        "TP_BE_BACKLOG_PREFLIGHT phase=deferred event_id=%s "
                        "group_id=%s tp=%s reason=stop_priority",
                        event_id,
                        group_id,
                        event_level_index,
                    )
                    break

                _BE_PREFLIGHT_RECENT[event_id] = now
                processed_candidate = True
                started = time.monotonic()
                actions = 0
                failed_users = 0
                log.warning(
                    "TP_BE_BACKLOG_PREFLIGHT phase=start event_id=%s group_id=%s "
                    "tp=%s due_lag_sec=%.3f users=%s",
                    event_id,
                    group_id,
                    event_level_index,
                    due_lag_sec,
                    len(by_user),
                )
                for uid, user_rows in by_user.items():
                    symbol = str(user_rows[0].get("symbol") or "").upper()
                    if not symbol:
                        continue
                    try:
                        async with verify_sem:
                            actions += int(
                                await process_be_monitor_once(
                                    notify=notify,
                                    rows_override=user_rows,
                                    market_prices={symbol: observed_price},
                                    event_level_index=event_level_index,
                                )
                            )
                    except asyncio.CancelledError:
                        raise
                    except db.MonitorAdvisoryBusy:
                        log.info(
                            "TP_BE_BACKLOG_PREFLIGHT phase=deferred event_id=%s "
                            "group_id=%s tp=%s uid=%s reason=advisory_busy",
                            event_id,
                            group_id,
                            event_level_index,
                            uid,
                        )
                    except Exception as exc:
                        failed_users += 1
                        log.warning(
                            "TP_BE_BACKLOG_PREFLIGHT phase=user_failed event_id=%s "
                            "group_id=%s tp=%s uid=%s error_type=%s error=%s",
                            event_id,
                            group_id,
                            event_level_index,
                            uid,
                            type(exc).__name__,
                            str(exc)[:240],
                            exc_info=True,
                        )
                log.info(
                    "TP_BE_BACKLOG_PREFLIGHT phase=complete event_id=%s group_id=%s "
                    "tp=%s users=%s actions=%s failed_users=%s duration_ms=%s",
                    event_id,
                    group_id,
                    event_level_index,
                    len(by_user),
                    actions,
                    failed_users,
                    int((time.monotonic() - started) * 1000),
                )
                # One BE-focused candidate at a time. Existing critical DB
                # admission still bounds total pool/advisory pressure.
                break

            await asyncio.sleep(_BE_PREFLIGHT_POLL_SEC)
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            log.warning(
                "TP_BE_BACKLOG_PREFLIGHT phase=db_timeout timeout_sec=%.1f",
                _EVENT_DB_TIMEOUT_SEC,
            )
            await asyncio.sleep(1.0)
        except Exception:
            log.exception("TP_BE_BACKLOG_PREFLIGHT phase=loop_failed")
            await asyncio.sleep(1.0)


async def market_price_event_loop() -> None:
    """Poll one public BingX price per active symbol and emit durable events.

    The loop never uses user API keys. It turns the common trading plan into an
    event source, but it does not assume an order filled. Every event is followed
    by a private, per-account verification job.

    v1.0.7g7h1 keeps a durable event arm plus a tick-aware hysteresis gate.
    Exact-level noise cannot recreate completed work, while a genuine retreat
    and re-cross remains eligible for immediate verification.
    """
    settings = get_settings()
    interval = float(settings.MARKET_PRICE_POLL_INTERVAL_SEC)
    timeout_ms = int(settings.BINGX_REQUEST_TIMEOUT_SECONDS) * 1000
    adapter = BingxAdapter(
        "", "", testnet=bool(settings.BINGX_VST), timeout_ms=timeout_ms
    )
    groups_cache: list[dict[str, Any]] = []
    # An authoritative empty list is a valid cache state.  Keep a separate
    # initialization flag so zero active groups does not trigger one DB read on
    # every public-price tick.
    group_cache_initialized = False
    last_group_refresh = 0.0
    group_refresh_retry_at = 0.0
    refresh_every = max(1.0, min(5.0, interval * 2.0))
    last_success: dict[str, float] = {}
    last_error_log: dict[str, float] = {}
    stop_only_fair: dict[str, bool] = {}
    price_ticks: dict[str, float] = {}
    trigger_catalog_refreshed_at = 0.0
    trigger_catalog_last_attempt = 0.0
    last_group_cache_stale_log = 0.0

    while True:
        started = time.monotonic()
        symbols: list[str] = []
        trade_symbols: list[str] = []
        analytics_symbols: tuple[str, ...] = ()
        analytics_checked = 0
        analytics_transitions = 0
        analytics_events = 0
        queued = 0
        gate_suppressed = 0
        gate_rearmed = 0
        gate_safety_rechecks = 0
        gate_deferred_suppressed = 0
        span_cm = diagnostic_span(
            "cycle.public_price",
            emit=True,
            metadata={
                "price_stream": "degraded" if PRICE_STREAM_DEGRADED.is_set() else "healthy"
            },
        )
        cycle_span = span_cm.__enter__()
        try:
            now = time.monotonic()
            group_refresh_failed = bool(now < group_refresh_retry_at)
            refresh_due = (
                not group_cache_initialized
                or now - last_group_refresh >= refresh_every
            )
            if refresh_due and now >= group_refresh_retry_at:
                phase_started = time.monotonic()
                fresh_groups, refresh_ok, refresh_error = (
                    await _refresh_active_trade_groups_cache(groups_cache)
                )
                cycle_span.add_ms(
                    "group_refresh_ms", (time.monotonic() - phase_started) * 1000
                )
                if refresh_ok:
                    groups_cache = fresh_groups
                    group_cache_initialized = True
                    # Measure the next refresh from completion, not from the
                    # pre-call timestamp.  A slow successful read must not make
                    # another refresh immediately due on the next loop.
                    last_group_refresh = time.monotonic()
                    group_refresh_retry_at = 0.0
                    group_refresh_failed = False
                    last_group_cache_stale_log = 0.0
                    cycle_span.set("group_cache_reused", 0)
                    cycle_span.set("group_cache_initialized", 1)
                    active_ids = {int(g.get("id") or 0) for g in groups_cache}
                    _prune_closed_group_state(active_ids)
                else:
                    group_refresh_failed = True
                    # Base the retry deadline on the *end* of the failed DB
                    # call. A five-second acquire timeout must still be
                    # followed by a real five-second cooldown instead of an
                    # immediate retry because the pre-call timestamp expired.
                    refresh_failed_at = time.monotonic()
                    # Retry cadence is independent of the public-price poll
                    # interval.  Fast custom polling (for example 0.25-1 sec)
                    # must not turn a PostgreSQL outage into an acquire storm.
                    retry_delay = _group_cache_retry_delay_sec(refresh_error)
                    group_refresh_retry_at = refresh_failed_at + retry_delay
                    cycle_span.errors += 1
                    cycle_span.set("group_cache_reused", int(bool(groups_cache)))
                    cycle_span.set("group_refresh_failed", 1)
                    cycle_span.set(
                        "group_refresh_retry_ms",
                        max(0.0, group_refresh_retry_at - refresh_failed_at) * 1000,
                    )
                    error_type = type(refresh_error).__name__ if refresh_error else "Error"
                    if isinstance(refresh_error, TimeoutError):
                        log.warning(
                            "PUBLIC_GROUP_CACHE_DB_TIMEOUT cached_groups=%s retry_after_sec=%s",
                            len(groups_cache),
                            int(max(1.0, group_refresh_retry_at - refresh_failed_at)),
                        )
                    else:
                        log.error(
                            "PUBLIC_GROUP_CACHE_REFRESH_FAILED error_type=%s cached_groups=%s retry_after_sec=%s",
                            error_type,
                            len(groups_cache),
                            int(max(1.0, group_refresh_retry_at - refresh_failed_at)),
                            exc_info=(
                                type(refresh_error),
                                refresh_error,
                                refresh_error.__traceback__,
                            ) if refresh_error is not None else None,
                        )

            analytics_symbols = get_signal_analytics_tracking_symbols()
            trade_symbols = sorted(
                {
                    str(g.get("symbol") or "").upper()
                    for g in groups_cache
                    if g.get("symbol")
                }
            )

            if not groups_cache:
                if group_refresh_failed or not group_cache_initialized:
                    # Unknown/stale DB state is not an authoritative empty group
                    # set. Preserve the trading degraded flag, but analytics-only
                    # public tracking may still proceed without touching private
                    # state or execution locks.
                    PRICE_STREAM_DEGRADED.set()
                    cycle_span.set("group_cache_unavailable", 1)
                    cycle_span.set(
                        "group_cache_initialized", int(group_cache_initialized)
                    )
                else:
                    PRICE_STREAM_DEGRADED.clear()
                    cycle_span.set("group_cache_initialized", 1)
                if not analytics_symbols:
                    _publish_public_last_prices(
                        {},
                        set(),
                        observed_at=time.monotonic(),
                        authoritative_scope=True,
                    )
                    cycle_span.set("groups", 0)
                    cycle_span.set("symbols", 0)
                    cycle_span.set("analytics_symbols", 0)
                    cycle_span.set("queued_events", 0)
                    mark_cycle_completed("public_price")
                    await asyncio.sleep(interval)
                    continue

            # One shared public snapshot covers trading and analytics. Analytics
            # never issues a separate BingX request.
            symbols = sorted(set(trade_symbols).union(analytics_symbols))
            check_now = time.monotonic()
            prices: dict[str, dict[str, float]] = {}
            bulk_error = ""
            all_ticker_snapshot = False
            price_fetch_started = time.monotonic()
            try:
                # The official ticker endpoint is limited to 10 calls / 2 sec.
                # For a small symbol set use compact per-symbol responses; when
                # that would approach the limit, switch to one all-ticker call.
                safe_individual_count = _safe_individual_ticker_count(interval)
                if len(symbols) <= safe_individual_count:
                    # Small combined trade+analytics sets keep using compact
                    # per-symbol payloads. Analytics adds no separate loop and
                    # shares the same rate-budget calculation as trading.
                    rows = await asyncio.gather(
                        *(adapter.fetch_market_prices(symbol) for symbol in symbols),
                        return_exceptions=True,
                    )
                    errors: list[str] = []
                    for symbol, row in zip(symbols, rows, strict=True):
                        if isinstance(row, Exception):
                            errors.append(f"{symbol}: {type(row).__name__}: {row}")
                        else:
                            prices[symbol] = row
                    if errors:
                        bulk_error = "; ".join(errors)[:1000]
                else:
                    # Once the combined symbol set approaches the official
                    # ticker budget, one all-ticker snapshot is safer than N
                    # concurrent pairs of ticker/premium requests.
                    prices = await adapter.fetch_market_prices_bulk(
                        symbols, include_unrequested=True
                    )
                    all_ticker_snapshot = True
            except Exception as exc:
                bulk_error = f"{type(exc).__name__}: {exc}"
            finally:
                cycle_span.add_ms(
                    "price_fetch_ms", (time.monotonic() - price_fetch_started) * 1000
                )

            published_prices = _publish_public_last_prices(
                prices,
                prices.keys() if all_ticker_snapshot else symbols,
                observed_at=time.monotonic(),
                authoritative_scope=all_ticker_snapshot,
            )
            cycle_span.set("public_snapshot_published", published_prices)

            analytics_snapshot = {
                symbol: prices[symbol]
                for symbol in analytics_symbols
                if symbol in prices
            }
            analytics_started = time.monotonic()
            analytics_result = submit_signal_analytics_price_snapshot(
                analytics_snapshot,
                observed_at=datetime.now(timezone.utc),
            )
            cycle_span.add_ms(
                "analytics_tracking_ms",
                (time.monotonic() - analytics_started) * 1000,
            )
            analytics_checked = int(analytics_result.get("signals_checked") or 0)
            analytics_transitions = int(analytics_result.get("transitions") or 0)
            analytics_events = int(analytics_result.get("events") or 0)
            cycle_span.set("analytics_symbols", len(analytics_symbols))
            cycle_span.set("analytics_checked", analytics_checked)
            cycle_span.set("analytics_transitions", analytics_transitions)
            cycle_span.set("analytics_events", analytics_events)

            catalog_started = time.monotonic()
            catalog_due = (
                not trigger_catalog_refreshed_at
                or check_now - trigger_catalog_refreshed_at >= 3600.0
                or any(symbol not in stop_only_fair for symbol in trade_symbols)
                or any(symbol not in price_ticks for symbol in trade_symbols)
            )
            if (
                trade_symbols
                and catalog_due
                and check_now - trigger_catalog_last_attempt >= 60.0
            ):
                trigger_catalog_last_attempt = check_now
                try:
                    stop_map, tick_map = await asyncio.gather(
                        adapter.fetch_stop_only_fair_map(trade_symbols),
                        adapter.fetch_price_tick_map(trade_symbols),
                    )
                    stop_only_fair.update(stop_map)
                    price_ticks.update(
                        {
                            str(symbol).upper(): float(tick)
                            for symbol, tick in tick_map.items()
                            if float(tick or 0.0) > 0
                        }
                    )
                    for symbol in trade_symbols:
                        stop_only_fair.setdefault(symbol, False)
                        # Zero means "unknown" and keeps the percentage fallback.
                        price_ticks.setdefault(symbol, 0.0)
                    trigger_catalog_refreshed_at = check_now
                except Exception as exc:
                    for symbol in trade_symbols:
                        stop_only_fair.setdefault(symbol, False)
                        price_ticks.setdefault(symbol, 0.0)
                    log.warning(
                        "BingX trigger metadata refresh failed; using safe percentage fallback: %s",
                        f"{type(exc).__name__}: {exc}",
                    )
            cycle_span.add_ms(
                "trigger_catalog_ms", (time.monotonic() - catalog_started) * 1000
            )

            for symbol in trade_symbols:
                symbol_prices = prices.get(symbol, {})
                usable = max(
                    (float(v or 0.0) for v in symbol_prices.values()), default=0.0
                )
                if usable > 0:
                    last_success[symbol] = check_now
                    continue
                stale_for = check_now - last_success.get(symbol, 0.0)
                since_log = check_now - last_error_log.get(symbol, 0.0)
                if (
                    stale_for >= float(settings.MARKET_PRICE_STALE_SEC)
                    and since_log >= 10.0
                ):
                    log.warning(
                        "public price stale symbol=%s stale_sec=%.1f error=%s",
                        symbol,
                        stale_for,
                        bulk_error or "ticker omitted symbol",
                    )
                    last_error_log[symbol] = check_now

            stale_symbols = [
                symbol
                for symbol in trade_symbols
                if check_now - last_success.get(symbol, 0.0)
                >= float(settings.MARKET_PRICE_STALE_SEC)
            ]
            group_cache_age_sec = max(0.0, check_now - last_group_refresh)
            group_cache_stale = _group_cache_is_stale(
                refresh_failed=group_refresh_failed,
                initialized=group_cache_initialized,
                cache_age_sec=group_cache_age_sec,
                refresh_every_sec=refresh_every,
            )
            cycle_span.set("group_cache_age_ms", group_cache_age_sec * 1000)
            cycle_span.set("group_cache_stale", int(group_cache_stale))
            if group_cache_stale:
                log_now = time.monotonic()
                if (
                    not last_group_cache_stale_log
                    or log_now - last_group_cache_stale_log
                    >= float(_GROUP_CACHE_STALE_LOG_INTERVAL_SEC)
                ):
                    log.warning(
                        "PUBLIC_GROUP_CACHE_STALE cached_groups=%s age_sec=%s retry_pending=%s",
                        len(groups_cache),
                        int(group_cache_age_sec),
                        int(group_refresh_retry_at > log_now),
                    )
                    last_group_cache_stale_log = log_now
            if stale_symbols or group_cache_stale:
                PRICE_STREAM_DEGRADED.set()
            else:
                PRICE_STREAM_DEGRADED.clear()

            evaluate_started = time.monotonic()
            evaluation_rows: list[tuple[
                dict[str, Any], int, str, str, float, float, float
            ]] = []
            group_prices_to_persist: list[tuple[int, float]] = []
            for group in groups_cache:
                group_id = int(group.get("id") or 0)
                symbol = str(group.get("symbol") or "").upper()
                side = str(group.get("side") or "").lower()
                if (
                    not group_id
                    or symbol not in prices
                    or side not in {"long", "short"}
                ):
                    continue
                symbol_prices = prices[symbol]
                latest_price = _f(symbol_prices.get("last"), 0.0)
                fair_price = _f(symbol_prices.get("fair"), 0.0)
                index_price = _f(symbol_prices.get("index"), 0.0)
                entry_price = latest_price or fair_price or index_price
                tp_observed_price = (
                    (fair_price or latest_price or index_price)
                    if stop_only_fair.get(symbol, False)
                    else (latest_price or fair_price or index_price)
                )
                stop_observed_price = fair_price or latest_price or index_price
                evaluation_rows.append(
                    (
                        group, group_id, symbol, side, entry_price,
                        tp_observed_price, stop_observed_price,
                    )
                )
                group_prices_to_persist.append(
                    (group_id, entry_price or stop_observed_price)
                )

            persisted_prices = await _persist_due_trade_group_prices(
                group_prices_to_persist,
                persist_now=time.monotonic(),
            )
            cycle_span.set("price_persist_candidates", len(group_prices_to_persist))
            cycle_span.set("price_persisted", persisted_prices)

            event_specs: list[dict[str, Any]] = []
            for (
                group, group_id, symbol, side, entry_price,
                tp_observed_price, stop_observed_price,
            ) in evaluation_rows:
                entry_type = str(group.get("entry_type") or "LIMIT").upper()
                planned_entry = _f(group.get("planned_entry"), 0.0)
                stop_price = _f(group.get("stop_price"), 0.0)
                try:
                    targets = [
                        float(x) for x in json.loads(group.get("targets_json") or "[]")
                    ]
                except Exception:
                    targets = []

                # Preserve end-to-end safety ordering in the in-memory plan and
                # in the set-wise DB statement: STOP, then TP, then ENTRY by
                # durable event_priority. The planner performs no exchange write.
                event_specs.append(
                    {
                        "group_id": group_id,
                        "event_key": "STOP",
                        "event_type": "STOP",
                        "level_index": 0,
                        "trigger_price": stop_price,
                        "observed_price": stop_observed_price,
                        "reached": _level_reached(
                            side, "STOP", stop_observed_price, stop_price
                        ),
                        "rearm_ready": _level_rearm_ready(
                            side,
                            "STOP",
                            stop_observed_price,
                            stop_price,
                            price_tick=price_ticks.get(symbol, 0.0),
                        ),
                    }
                )
                if entry_type == "LIMIT":
                    event_specs.append(
                        {
                            "group_id": group_id,
                            "event_key": "ENTRY",
                            "event_type": "ENTRY",
                            "level_index": 0,
                            "trigger_price": planned_entry,
                            "observed_price": entry_price,
                            "reached": _level_reached(
                                side, "ENTRY", entry_price, planned_entry
                            ),
                            "rearm_ready": _level_rearm_ready(
                                side,
                                "ENTRY",
                                entry_price,
                                planned_entry,
                                price_tick=price_ticks.get(symbol, 0.0),
                            ),
                        }
                    )
                for idx, target in enumerate(targets, start=1):
                    event_specs.append(
                        {
                            "group_id": group_id,
                            "event_key": f"TP{idx}",
                            "event_type": "TP",
                            "level_index": idx,
                            "trigger_price": target,
                            "observed_price": tp_observed_price,
                            "reached": _level_reached(
                                side, "TP", tp_observed_price, target
                            ),
                            "rearm_ready": _level_rearm_ready(
                                side,
                                "TP",
                                tp_observed_price,
                                target,
                                price_tick=price_ticks.get(symbol, 0.0),
                            ),
                        }
                    )

            rearm_specs: list[dict[str, Any]] = []
            for spec in event_specs:
                local_key = (int(spec["group_id"]), str(spec["event_key"]))
                was_seen = local_key in _LOCAL_EVENT_SEEN
                spec["was_seen"] = was_seen
                if bool(spec["reached"]) and was_seen and _event_safety_recheck_due(
                    local_key, str(spec["event_type"])
                ):
                    rearm_specs.append(
                        {
                            "group_id": local_key[0],
                            "event_key": local_key[1],
                            "reset_safety_budget": False,
                        }
                    )
                    spec["rearm_requested"] = True
                elif (
                    not bool(spec["reached"])
                    and was_seen
                    and bool(spec["rearm_ready"])
                ):
                    rearm_specs.append(
                        {
                            "group_id": local_key[0],
                            "event_key": local_key[1],
                            "reset_safety_budget": True,
                        }
                    )
                    spec["rearm_requested"] = True
                else:
                    spec["rearm_requested"] = False

            rearm_results = await _rearm_batch_once(rearm_specs)
            enqueue_specs: list[dict[str, Any]] = []
            for spec in event_specs:
                local_key = (int(spec["group_id"]), str(spec["event_key"]))
                reached = bool(spec["reached"])
                was_seen = bool(spec.get("was_seen"))
                rearm_state = (
                    rearm_results.get(local_key, "not_due")
                    if bool(spec.get("rearm_requested"))
                    else "not_due"
                )
                if reached:
                    if was_seen:
                        if rearm_state in {"rearmed", "already_armed", "missing"}:
                            enqueue_specs.append({**spec, "safety_recheck": True})
                        else:
                            gate_suppressed += 1
                            gate_deferred_suppressed += int(
                                rearm_state
                                in {"deferred_exists", "deferred_cooldown"}
                            )
                    else:
                        enqueue_specs.append({**spec, "safety_recheck": False})
                elif was_seen and bool(spec["rearm_ready"]):
                    gate_rearmed += int(rearm_state == "rearmed")
                    gate_deferred_suppressed += int(
                        rearm_state in {"deferred_exists", "deferred_cooldown"}
                    )

            enqueue_results = await _enqueue_batch_once(enqueue_specs)
            for spec in enqueue_specs:
                local_key = (int(spec["group_id"]), str(spec["event_key"]))
                inserted = int(bool(enqueue_results.get(local_key, False)))
                queued += inserted
                if bool(spec.get("safety_recheck")):
                    gate_safety_rechecks += inserted
            cycle_span.set("event_rearm_batch_rows", len(rearm_specs))
            cycle_span.set("event_enqueue_batch_rows", len(enqueue_specs))
            cycle_span.set(
                "event_db_batch_scopes",
                int(bool(rearm_specs)) + int(bool(enqueue_specs)),
            )

            cycle_span.add_ms(
                "evaluate_persist_enqueue_ms",
                (time.monotonic() - evaluate_started) * 1000,
            )

            duration = time.monotonic() - started
            cycle_span.set("groups", len(groups_cache))
            cycle_span.set("symbols", len(symbols))
            cycle_span.set("queued_events", queued)
            cycle_span.set("analytics_symbols", len(analytics_symbols))
            cycle_span.set("analytics_checked", analytics_checked)
            cycle_span.set("analytics_transitions", analytics_transitions)
            cycle_span.set("analytics_events", analytics_events)
            cycle_span.set("event_gate_suppressed", gate_suppressed)
            cycle_span.set("event_gate_rearmed", gate_rearmed)
            cycle_span.set("event_gate_safety_rechecks", gate_safety_rechecks)
            cycle_span.set(
                "event_gate_deferred_suppressed", gate_deferred_suppressed
            )
            cycle_span.set(
                "event_tick_symbols",
                sum(
                    1 for symbol in trade_symbols if price_ticks.get(symbol, 0.0) > 0
                ),
            )
            cycle_span.set(
                "price_stream",
                "degraded" if PRICE_STREAM_DEGRADED.is_set() else "healthy",
            )
            mark_cycle_completed("public_price")
            if duration >= max(2.0, interval * 2.0):
                log.warning(
                    "slow public price cycle groups=%s symbols=%s queued=%s duration_ms=%s",
                    len(groups_cache),
                    len(symbols),
                    queued,
                    int(duration * 1000),
                )
            else:
                log.debug(
                    "public price cycle groups=%s symbols=%s queued=%s duration_ms=%s",
                    len(groups_cache),
                    len(symbols),
                    queued,
                    int(duration * 1000),
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            cycle_span.errors += 1
            log.exception("market price event loop failed")
        finally:
            cycle_span.set("groups", len(groups_cache))
            cycle_span.set("symbols", len(symbols))
            cycle_span.set("queued_events", queued)
            cycle_span.set("analytics_symbols", len(analytics_symbols))
            cycle_span.set("analytics_checked", analytics_checked)
            cycle_span.set("analytics_transitions", analytics_transitions)
            cycle_span.set("analytics_events", analytics_events)
            cycle_span.set("event_gate_suppressed", gate_suppressed)
            cycle_span.set("event_gate_rearmed", gate_rearmed)
            cycle_span.set("event_gate_safety_rechecks", gate_safety_rechecks)
            cycle_span.set(
                "event_gate_deferred_suppressed", gate_deferred_suppressed
            )
            cycle_span.set(
                "event_tick_symbols",
                sum(1 for symbol in symbols if price_ticks.get(symbol, 0.0) > 0),
            )
            span_cm.__exit__(None, None, None)
        elapsed = time.monotonic() - started
        await asyncio.sleep(max(0.05, interval - elapsed))


def _manual_tp_watch_retry_seconds(attempts_after: int) -> float | None:
    """Return the bounded retry cadence for a manual-required TP watch."""

    attempt = max(1, int(attempts_after or 0))
    if attempt <= _MANUAL_TP_WATCH_FAST_ATTEMPTS:
        return 5.0
    if attempt <= _MANUAL_TP_WATCH_MEDIUM_ATTEMPTS:
        return 15.0
    if attempt <= _MANUAL_TP_WATCH_MAX_ATTEMPTS:
        return 60.0
    return None


def _event_no_action_reason(
    rows: list[dict[str, Any]],
    *,
    event_type: str,
    event_level_index: int = 0,
) -> str:
    """Classify an actions=0 verifier result without changing retry behavior.

    This helper is intentionally read-only and bounded.  It uses only the rows
    already loaded for the event; no exchange or database call is added to the
    latency-sensitive verifier path.
    """

    if not rows:
        return "no_active_rows"
    statuses = {
        str(row.get("status") or "").strip().lower()
        for row in rows
        if isinstance(row, dict)
    }
    if statuses and statuses <= {"manual_required"}:
        return "manual_required_only"

    kind = str(event_type or "").strip().upper()
    if kind == "ENTRY":
        if statuses and statuses <= {"pending_limit"}:
            return "pending_limit_no_transition"
        return "entry_state_unchanged"

    if kind == "TP":
        level = max(0, int(event_level_index or 0))
        target_rows_seen = 0
        target_rows_filled = 0
        all_be_moved = True
        for row in rows:
            try:
                payload = json.loads(str(row.get("exchange_order_ids_json") or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                payload = {}
            if not isinstance(payload, dict):
                payload = {}
            be_state = payload.get("be") if isinstance(payload.get("be"), dict) else {}
            all_be_moved = all_be_moved and be_state.get("moved") is True
            tp_rows = payload.get("tp") if isinstance(payload.get("tp"), list) else []
            for tp_row in tp_rows:
                if not isinstance(tp_row, dict):
                    continue
                try:
                    tp_index = int(tp_row.get("tp_index") or 0)
                except (TypeError, ValueError, OverflowError):
                    tp_index = 0
                if level > 0 and tp_index == level:
                    target_rows_seen += 1
                    if tp_row.get("filled") is True:
                        target_rows_filled += 1
                    break
        if target_rows_seen and target_rows_filled == target_rows_seen:
            return "tp_already_confirmed"
        if all_be_moved and rows:
            return "be_already_moved"
        if statuses and statuses <= {"pending_limit"}:
            return "pending_limit_no_transition"
        return "tp_state_unchanged"

    if kind == "STOP":
        return "stop_state_unchanged"
    return "unsupported_event" if kind else "missing_event_type"


def _event_age_seconds(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = (
            value
            if isinstance(value, datetime)
            else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        )
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds())


def _event_payload(row: dict[str, Any]) -> dict[str, Any]:
    try:
        payload = json.loads(str(row.get("exchange_order_ids_json") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _event_row_zero_exposure(row: dict[str, Any]) -> bool:
    """Use the shared strict exposure classifier for event completion.

    Status-like strings or a lone close marker are not sufficient: contradictory
    live-position metadata must keep the event fail-closed.  The shared helper
    also recognizes exact zero-fill LIMIT cancellations, including the durable
    ``canceled_external`` outcome.
    """

    return bool(execution_zero_exposure_confirmed(row))


def _event_entry_transition_confirmed(row: dict[str, Any]) -> bool:
    """Return True only when durable state proves ENTRY is no longer pending.

    ``manual_required`` is intentionally not accepted merely by status.  It may
    represent an ambiguous private write.  We accept it only with durable live
    entry/position evidence; otherwise the event remains under observation.
    """

    if _event_row_zero_exposure(row):
        return True
    status = str(row.get("status") or "").strip().lower()
    if status in {"opened", "protected", "partial_error", "partial_unrecoverable"}:
        return True
    if status != "manual_required":
        return False

    payload = _event_payload(row)
    lifecycle = payload.get("lifecycle") if isinstance(payload.get("lifecycle"), dict) else {}
    opening = (
        payload.get("opening_intent_reconciliation_v1")
        if isinstance(payload.get("opening_intent_reconciliation_v1"), dict)
        else {}
    )
    for source, keys in (
        (lifecycle, ("same_side_position_qty", "position_qty", "any_position_qty")),
        (opening, ("same_side_position_qty",)),
    ):
        for key in keys:
            if key in source and _f(source.get(key), 0.0) > 0:
                return True
    try:
        if int(opening.get("open_match_count") or 0) > 0:
            return True
    except (TypeError, ValueError, OverflowError):
        pass
    return bool(_f(payload.get("actual_entry"), 0.0) > 0 and _f(row.get("qty"), 0.0) > 0)


def _event_tp_level_confirmed(row: dict[str, Any], level: int) -> bool:
    payload = _event_payload(row)
    wanted = max(1, int(level or 1))
    tp_rows = payload.get("tp") if isinstance(payload.get("tp"), list) else []
    for fallback, item in enumerate(tp_rows, 1):
        if not isinstance(item, dict):
            continue
        try:
            index = int(item.get("tp_index") or fallback)
        except (TypeError, ValueError, OverflowError):
            continue
        if index == wanted and item.get("filled") is True:
            return True
    be_state = payload.get("be") if isinstance(payload.get("be"), dict) else {}
    if (
        be_state.get("moved") is True
        and str(be_state.get("source") or "").strip().lower() == "automatic"
        and be_state.get("trigger_fill_at") not in (None, "")
    ):
        try:
            trigger = int(
                be_state.get("trigger_tp_index")
                or be_state.get("trigger_ordinal")
                or 0
            )
        except (TypeError, ValueError, OverflowError):
            trigger = 0
        if trigger >= wanted:
            return True
    return False


def _classify_user_event_outcome(
    rows: list[dict[str, Any]],
    *,
    event_type: str,
    event_level_index: int,
) -> tuple[bool, str, str]:
    """Return (terminal, exact_outcome, no_action_reason) from durable state.

    Generic action counts are deliberately ignored.  A market event is terminal
    only when the post-verification DB state proves the exact goal for that
    event or proves that no position exposure remains.
    """

    kind = str(event_type or "").strip().upper()
    if not rows:
        return True, "terminal_no_execution", "no_rows"
    if all(_event_row_zero_exposure(row) for row in rows):
        return True, "terminal_no_position", "zero_exposure"

    statuses = {
        str(row.get("status") or "").strip().lower()
        for row in rows
        if isinstance(row, dict)
    }
    if kind == "ENTRY":
        if all(_event_entry_transition_confirmed(row) for row in rows):
            return True, "entry_transition_confirmed", ""
        if "manual_required" in statuses:
            return False, "manual_escalation", "manual_required_entry_unconfirmed"
        return False, "retry_required", "pending_limit_no_transition"

    if kind == "TP":
        # A still-live LIMIT remainder can reopen exposure after price has
        # progressed.  Do not complete the TP event until that pending entry is
        # itself filled/cancelled/closed by the strict post-state classifier.
        if "pending_limit" in statuses:
            return False, "retry_required", "pending_limit_no_transition"
        position_rows = [
            row for row in rows if not _event_row_zero_exposure(row)
        ]
        if position_rows and all(
            _event_tp_level_confirmed(row, event_level_index)
            for row in position_rows
        ):
            return True, "tp_fill_confirmed", ""
        if statuses and statuses <= {"manual_required"}:
            return False, "manual_escalation", "manual_required_only"
        return False, "retry_required", "tp_state_unconfirmed"

    if kind == "STOP":
        return False, "retry_required", "stop_close_unconfirmed"
    return False, "retry_required", "unsupported_event"


@dataclass(frozen=True)
class _WatchDecision:
    escalated_at: datetime
    stuck_started_at: datetime | None
    last_stuck_alert_at: datetime | None
    last_stuck_reminder_at: datetime | None
    send_escalation: bool
    send_stuck_alert: bool
    send_reminder: bool

    @property
    def is_stuck(self) -> bool:
        return self.stuck_started_at is not None


def _parse_watch_datetime(value: Any) -> datetime | None:
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


def _watch_state_decision(
    event: dict[str, Any], *, now: datetime | None = None
) -> _WatchDecision:
    """Derive escalation/reminder state from durable UTC timestamps only."""

    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    escalated = _parse_watch_datetime(event.get("escalated_at"))
    stuck_started = _parse_watch_datetime(event.get("stuck_started_at"))
    last_alert = _parse_watch_datetime(event.get("last_stuck_alert_at"))
    last_reminder = _parse_watch_datetime(event.get("last_stuck_reminder_at"))

    if escalated is None:
        # A pre-g30 row can carry a large inherited attempts counter.  It is
        # still a first transition for the timestamp-based lifecycle and must
        # never be mislabeled as a six-hour reminder immediately after deploy.
        return _WatchDecision(
            escalated_at=current,
            stuck_started_at=None,
            last_stuck_alert_at=None,
            last_stuck_reminder_at=None,
            send_escalation=True,
            send_stuck_alert=False,
            send_reminder=False,
        )

    if (current - escalated).total_seconds() < _EVENT_STUCK_WATCH_AFTER_SEC:
        return _WatchDecision(
            escalated_at=escalated,
            stuck_started_at=stuck_started,
            last_stuck_alert_at=last_alert,
            last_stuck_reminder_at=last_reminder,
            send_escalation=False,
            send_stuck_alert=False,
            send_reminder=False,
        )

    if stuck_started is None:
        stuck_started = current
    if last_alert is None:
        return _WatchDecision(
            escalated_at=escalated,
            stuck_started_at=stuck_started,
            last_stuck_alert_at=current,
            last_stuck_reminder_at=last_reminder,
            send_escalation=False,
            send_stuck_alert=True,
            send_reminder=False,
        )

    reminder_base = last_reminder or last_alert
    send_reminder = (
        (current - reminder_base).total_seconds() >= _EVENT_STUCK_REMINDER_SEC
    )
    return _WatchDecision(
        escalated_at=escalated,
        stuck_started_at=stuck_started,
        last_stuck_alert_at=last_alert,
        last_stuck_reminder_at=(current if send_reminder else last_reminder),
        send_escalation=False,
        send_stuck_alert=False,
        send_reminder=send_reminder,
    )


def _deterministic_retry_with_jitter(
    base_seconds: float, *, event_id: int, group_id: int
) -> float:
    """Apply stable downward jitter without exceeding the safety ceiling."""

    base = max(1.0, float(base_seconds))
    digest = hashlib.sha256(f"{int(event_id)}:{int(group_id)}".encode()).digest()
    fraction = int.from_bytes(digest[:2], "big") / 65535.0
    # 90-100% of the nominal interval.  Downward-only jitter prevents a retry
    # from exceeding the documented 60/300/900-second maximum.
    return max(1.0, base * (0.90 + 0.10 * fraction))


def _final_write_failure_retry_after(
    *, event: dict[str, Any], watch_commit: dict[str, Any] | None
) -> float:
    """Return a bounded retry delay after the final DB write itself fails.

    A one-second release loop can amplify a persistent PostgreSQL type/schema
    error into hundreds of verifier attempts and starve fresh TP/STOP work.
    Preserve the already-computed watch cadence when available; otherwise use a
    short protective delay that remains materially faster for STOP/TP than for
    ENTRY while avoiding a hot retry loop.
    """

    if watch_commit is not None:
        try:
            requested = float(watch_commit.get("retry_after_sec") or 0.0)
        except (TypeError, ValueError, OverflowError):
            requested = 0.0
        if math.isfinite(requested) and requested > 0:
            return max(15.0, min(requested, _EVENT_STUCK_WATCH_RETRY_SEC))

    event_type = str(
        event.get("event_type") or event.get("event_key") or ""
    ).upper()
    if event_type == "STOP":
        base = 15.0
    elif event_type == "TP" or event_type.startswith("TP"):
        base = 20.0
    else:
        base = 30.0
    return _deterministic_retry_with_jitter(
        base,
        event_id=int(event.get("id") or 0),
        group_id=int(event.get("trade_group_id") or 0),
    )


def _dominant_watch_reason(
    *, error: str, outcome_reasons: Counter[str], exact_outcome: str
) -> str:
    if str(error or "").strip():
        return str(error).strip()[:500]
    if outcome_reasons:
        reason, _count = sorted(
            outcome_reasons.items(), key=lambda item: (-int(item[1]), str(item[0]))
        )[0]
        return str(reason or exact_outcome or "exact_result_unconfirmed")[:500]
    return str(exact_outcome or "exact_result_unconfirmed")[:500]


def _pending_limit_admin_watch(
    *, event_type: str, error: str, outcome_reasons: Counter[str]
) -> bool:
    reasons = {str(key) for key, count in outcome_reasons.items() if int(count) > 0}
    return (
        str(event_type).upper() == "TP"
        and not str(error or "").strip()
        and bool(reasons)
        and reasons <= {"pending_limit_no_transition"}
    )


def _watch_action_text(reason: str, lane: str) -> str:
    normalized = str(reason or "")
    if "pending_limit_no_transition" in normalized:
        return "Проверьте, нужна ли ещё лимитная заявка; TP объединены в один контроль группы."
    if "manual_required" in normalized or "manual TP" in normalized:
        return "Проверьте исполнение TP и актуальный STOP; живая позиция остаётся в быстрой линии."
    if "position" in normalized or "tp_state_unconfirmed" in normalized:
        return "Проверьте позицию и защитные ордера по группе на BingX."
    if lane == "admin":
        return "Проверьте группу вручную; свежие STOP/TP этой очередью не блокируются."
    return "Проверьте точный результат события и защитные ордера по группе."


def _format_duration(seconds: float | None) -> str:
    if seconds is None or seconds < 0:
        return "неизвестно"
    total = int(seconds)
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}ч {minutes}м"
    if minutes:
        return f"{minutes}м {secs}с"
    return f"{secs}с"


def _watch_evidence_lines(
    rows: list[dict[str, Any]], *, event_level_index: int
) -> list[str]:
    """Build a bounded read-only evidence summary from persisted execution state."""

    qty_total = 0.0
    position_ids: set[str] = set()
    stop_ids: set[str] = set()
    stop_prices: set[str] = set()
    tp_states: set[str] = set()
    freshest: datetime | None = None

    def add_text(target: set[str], value: Any) -> None:
        text = str(value or "").strip()
        if text and text.lower() not in {"none", "null", "0"}:
            target.add(text[:80])

    def walk(value: Any, parent: str = "") -> None:
        if isinstance(value, dict):
            for raw_key, child in value.items():
                key = str(raw_key or "").lower()
                path = f"{parent}.{key}" if parent else key
                if key in {"positionid", "position_id", "_confirmed_position_id"}:
                    add_text(position_ids, child)
                if (
                    "stop" in path
                    and ("orderid" in key or "order_id" in key or key == "id")
                ):
                    add_text(stop_ids, child)
                if "stop" in path and key in {
                    "stopprice", "stop_price", "triggerprice", "trigger_price", "price"
                }:
                    try:
                        numeric = float(child)
                        if math.isfinite(numeric) and numeric > 0:
                            stop_prices.add(f"{numeric:g}")
                    except (TypeError, ValueError, OverflowError):
                        pass
                if key in {"tp_index", "tpindex", "level_index"}:
                    try:
                        if int(child or 0) == int(event_level_index or 0):
                            filled = value.get("filled")
                            status = value.get("status") or value.get("state")
                            tp_states.add(
                                f"TP{int(event_level_index)} "
                                f"filled={str(filled).lower() if filled is not None else 'unknown'} "
                                f"status={str(status or 'unknown')[:40]}"
                            )
                    except (TypeError, ValueError, OverflowError):
                        pass
                walk(child, path)
        elif isinstance(value, list):
            for child in value[:100]:
                walk(child, parent)

    for row in rows[:5000]:
        try:
            qty = float(row.get("qty") or 0.0)
            if math.isfinite(qty) and qty > 0:
                qty_total += qty
        except (TypeError, ValueError, OverflowError):
            pass
        stamp = _parse_watch_datetime(row.get("updated_at"))
        if stamp is not None and (freshest is None or stamp > freshest):
            freshest = stamp
        raw_payload = row.get("exchange_order_ids_json") or {}
        try:
            payload = (
                json.loads(raw_payload)
                if isinstance(raw_payload, str)
                else raw_payload
            )
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        walk(payload)

    freshness = "unknown"
    if freshest is not None:
        freshness = _format_duration(
            max(0.0, (datetime.now(timezone.utc) - freshest).total_seconds())
        )
    return [
        f"Позиция (БД): qty={qty_total:g} · positionId={','.join(sorted(position_ids)[:3]) or 'нет доказательства'}",
        f"STOP evidence: id={','.join(sorted(stop_ids)[:3]) or 'нет'} · price={','.join(sorted(stop_prices)[:3]) or 'нет'}",
        f"TP evidence: {', '.join(sorted(tp_states)[:3]) or 'точное fill-свидетельство не сохранено'} · свежесть={freshness}",
    ]


def _watch_reason_user_text(reason: str) -> str:
    normalized = str(reason or "").strip().lower()
    if "pending_limit_no_transition" in normalized:
        return "Входной LIMIT всё ещё не перешёл в подтверждённое состояние."
    if "manual_required" in normalized:
        return "Автоматическая проверка дошла до состояния, требующего внимания администратора."
    if any(token in normalized for token in ("timeout", "network", "connection")):
        return "Точная сверка временно не завершилась из-за ответа сети или биржи."
    if "advisory" in normalized and "busy" in normalized:
        return "Проверка отложена из-за занятого защитного контура; бот повторит её автоматически."
    if "zero_exposure" in normalized or "terminal_no_position" in normalized:
        return "Биржа не показывает активную позицию, но событие ещё проходит финальную сверку."
    return "Состояние сделки пока не удалось подтвердить окончательно."


def _watch_notification_text(
    *,
    transition: str,
    event: dict[str, Any],
    symbol: str,
    side: str,
    users_count: int,
    statuses: list[str],
    reason: str,
    retry_after: float,
    lane: str,
    decision: _WatchDecision,
    coalesced_keys: list[str],
    evidence_lines: list[str] | None = None,
) -> str:
    # G66: Telegram receives a compact human-readable card. Raw statuses, DB
    # evidence, lane names and exact reason codes stay in structured logs.
    if transition == "escalated":
        title = "🚨 <b>СДЕЛКА ПОД КОНТРОЛЕМ</b>"
    elif transition == "reminder":
        title = "⚠️ <b>СДЕЛКА ВСЁ ЕЩЁ ПОД КОНТРОЛЕМ</b>"
    else:
        title = "🆘 <b>ТРЕБУЕТСЯ ПРОВЕРКА СДЕЛКИ</b>"

    now = datetime.now(timezone.utc)
    age = max(0.0, (now - decision.escalated_at).total_seconds())
    levels = coalesced_keys or [
        str(event.get("event_key") or event.get("event_type") or "unknown")
    ]
    event_text = ", ".join(str(level) for level in levels[:8])
    state_text = _watch_reason_user_text(reason)
    group_id = int(event.get("trade_group_id") or 0)
    event_id = int(event.get("id") or 0)

    action_line = "🤖 Бот продолжает автоматическую точную сверку с BingX."
    if "manual_required" in str(reason or "").lower():
        action_line = "👤 Нужна проверка администратора; бот продолжит безопасный контроль."

    return card(
        title,
        symbol=symbol or "UNKNOWN",
        side=side or "unknown",
        blocks=(
            (
                f"🎯 <b>Событие:</b> {esc(event_text)}",
                f"⚠️ <b>Состояние:</b> {esc(state_text)}",
            ),
            (
                f"⏱ <b>Под контролем:</b> {esc(_format_duration(age))}",
                f"🔄 <b>Следующая проверка:</b> не позже {esc(_format_duration(retry_after))}",
            ),
            (
                action_line,
                f"🧾 <b>ID:</b> группа {group_id} · event {event_id}",
            ),
        ),
    )



def _manual_review_markup_spec(event_id: int) -> dict[str, Any]:
    eid = int(event_id)
    return {
        "inline_keyboard": [
            [
                {"text": "🔄 FINAL ещё раз", "callback_data": f"mer:retry:{eid}"},
                {"text": "📋 Доказательства", "callback_data": f"mer:view:{eid}"},
            ],
            [
                {"text": "✅ TP исполнен вручную", "callback_data": f"mer:tpfill:{eid}"},
                {"text": "❌ TP не исполнен", "callback_data": f"mer:tpno:{eid}"},
            ],
            [
                {"text": "🚫 Входа не было", "callback_data": f"mer:entryno:{eid}"},
            ],
        ]
    }


def _manual_review_notification_text(
    event: dict[str, Any],
    transition: Any,
    snapshot: dict[str, Any] | None,
) -> str:
    executions = list((snapshot or {}).get("executions") or [])
    entry_states = sorted({str((item.get("entry") or {}).get("state") or "UNKNOWN") for item in executions if isinstance(item, dict)})
    tp_states = sorted({str((item.get("tp") or {}).get("state") or "UNKNOWN") for item in executions if isinstance(item, dict)})
    filled_tp_qty = sum(
        float((item.get("tp") or {}).get("filled_qty") or 0.0)
        for item in executions if isinstance(item, dict)
    )
    position_qty = sum(
        float(item.get("position_qty") or 0.0)
        for item in executions if isinstance(item, dict)
    )
    migrated = str(event.get("migration_state") or "none") == "prepared"
    return ensure_visual_card(
        "⚠️ MARKET EVENT · АВТОПРОВЕРКИ ОСТАНОВЛЕНЫ\n"
        f"Event ID: {int(event.get('id') or 0)}\n"
        f"Группа: {int(event.get('trade_group_id') or 0)}\n"
        f"Событие: {str(event.get('event_key') or event.get('event_type') or 'unknown')}\n"
        f"ENTRY: {', '.join(entry_states) or 'UNKNOWN'}\n"
        f"TP: {', '.join(tp_states) or 'UNKNOWN'} · подтверждённый объём: {filled_tp_qty:g}\n"
        f"Текущий объём позиции по снимку: {position_qty:g}\n"
        f"Проверки: fast={int(transition.fast_attempts)} · deep={int(transition.deep_attempts)} · final={int(transition.final_attempts)}\n"
        f"Причина: {str(transition.reason or 'evidence_unresolved')[:500]}\n"
        f"Миграция старого события: {'да' if migrated else 'нет'}\n"
        "Итог: MANUAL_REVIEW. Автоматические повторы выключены, следующая попытка не назначена.\n"
        "Статистика: событие исключено из FINAL, симуляций и winrate.\n"
        "Важно: ручные кнопки фиксируют только административное решение и не создают, не отменяют и не изменяют ордера BingX."
    )


async def _notify_market_event_manual_review(
    notify: NotifyFn | None,
    *,
    event: dict[str, Any],
    transition: Any,
    snapshot: dict[str, Any] | None,
) -> None:
    settings = get_settings()
    if not bool(getattr(settings, "MARKET_EVENT_MANUAL_REVIEW_NOTIFY", True)):
        return
    event_id = int(event.get("id") or 0)
    text = _manual_review_notification_text(event, transition, snapshot)
    markup = _manual_review_markup_spec(event_id)
    for admin_id in sorted({int(value) for value in settings.admin_ids if int(value) > 0}):
        await send_or_enqueue(
            notify,
            admin_id,
            text,
            source="market_event_manual_review",
            event_key=f"market-event:{event_id}:manual-review",
            dedup_key_override=f"market-event:{event_id}:manual-review:{admin_id}",
            reply_markup_spec=markup,
        )


def _watch_notification_specs(
    *, event_id: int, transition: str, transition_at: datetime, text: str
) -> list[dict[str, Any]]:
    settings = get_settings()
    stamp = transition_at.astimezone(timezone.utc).replace(microsecond=0).isoformat()
    result: list[dict[str, Any]] = []
    for admin_id in sorted({int(value) for value in getattr(settings, "admin_ids", [])}):
        if admin_id <= 0:
            continue
        result.append(
            {
                "dedup_key": f"market-event:{int(event_id)}:{transition}:{stamp}:{admin_id}",
                "user_id": admin_id,
                "message_text": text,
                "source": "market_event_watch",
            }
        )
    return result


async def _deliver_committed_watch_notifications(
    notify: NotifyFn | None, specs: list[dict[str, Any]]
) -> None:
    for spec in specs:
        try:
            await send_or_enqueue(
                notify,
                int(spec["user_id"]),
                str(spec["message_text"]),
                source=str(spec["source"]),
                dedup_key_override=str(spec["dedup_key"]),
                event_key=str(spec["dedup_key"]),
            )
        except Exception as exc:
            # The outbox row already exists atomically with the event state.
            # A failed immediate attempt is therefore safe and will be retried.
            record_monitor_error("event_verify.watch_notify", exc)
            log.exception(
                "MARKET_EVENT_WATCH_IMMEDIATE_DELIVERY_FAILED key=%s",
                str(spec.get("dedup_key") or ""),
            )



def _prepare_watch_transition(
    *,
    event: dict[str, Any],
    event_type: str,
    event_id: int,
    group_id: int,
    symbol: str,
    side: str,
    users_count: int,
    statuses: list[str],
    error: str,
    outcome_reasons: Counter[str],
    exact_outcome: str,
    coalesced_keys: list[str],
    rows: list[dict[str, Any]],
    event_level_index: int,
    manual_live_tp: bool,
    pending_limit_watch: bool,
) -> tuple[dict[str, Any], list[dict[str, Any]], float, str, _WatchDecision]:
    reason = _dominant_watch_reason(
        error=error,
        outcome_reasons=outcome_reasons,
        exact_outcome=("manual_required_only" if manual_live_tp else exact_outcome),
    )
    decision = _watch_state_decision(event)
    lane = "admin" if pending_limit_watch and not manual_live_tp else "critical"
    if manual_live_tp or (lane == "critical" and event_type in {"STOP", "TP"}):
        nominal_retry = 60.0
    elif lane == "admin":
        nominal_retry = (
            _EVENT_STUCK_WATCH_RETRY_SEC
            if decision.is_stuck
            else _EVENT_ESCALATED_WATCH_RETRY_SEC
        )
    else:
        nominal_retry = _EVENT_ESCALATED_WATCH_RETRY_SEC
    retry_after = _deterministic_retry_with_jitter(
        nominal_retry,
        event_id=event_id,
        group_id=group_id,
    )
    if manual_live_tp:
        outcome = (
            "manual_tp_stuck_watch"
            if decision.is_stuck
            else "manual_tp_escalated_watch"
        )
    elif pending_limit_watch:
        outcome = (
            "pending_limit_group_stuck_watch"
            if decision.is_stuck
            else "pending_limit_group_watch"
        )
    else:
        outcome = "stuck_watch" if decision.is_stuck else "escalated_watch"

    transition = ""
    transition_at = decision.escalated_at
    if decision.send_escalation:
        transition = "escalated"
        transition_at = decision.escalated_at
    elif decision.send_stuck_alert:
        transition = "stuck"
        transition_at = decision.last_stuck_alert_at or datetime.now(timezone.utc)
    elif decision.send_reminder:
        transition = "reminder"
        transition_at = decision.last_stuck_reminder_at or datetime.now(timezone.utc)

    specs: list[dict[str, Any]] = []
    if transition:
        text = _watch_notification_text(
            transition=transition,
            event=event,
            symbol=symbol,
            side=side,
            users_count=users_count,
            statuses=statuses,
            reason=reason,
            retry_after=retry_after,
            lane=lane,
            decision=decision,
            coalesced_keys=coalesced_keys,
            evidence_lines=_watch_evidence_lines(
                rows, event_level_index=event_level_index
            ),
        )
        specs = _watch_notification_specs(
            event_id=event_id,
            transition=transition,
            transition_at=transition_at,
            text=text,
        )

    commit = {
        "retry_after_sec": retry_after,
        "error": error,
        "increment_attempt": True,
        "outcome_kind": outcome,
        "watch_lane": lane,
        # Keep timezone-aware datetime objects for asyncpg TIMESTAMPTZ binds.
        # The database layer serializes them only for SQLite.
        "escalated_at": decision.escalated_at,
        "stuck_started_at": decision.stuck_started_at,
        "last_stuck_alert_at": decision.last_stuck_alert_at,
        "last_stuck_reminder_at": decision.last_stuck_reminder_at,
        "stuck_reason": reason,
        "coalesced_event_keys": (
            json.dumps(coalesced_keys, ensure_ascii=False, separators=(",", ":"))
            if coalesced_keys else None
        ),
    }
    return commit, specs, retry_after, reason, decision


async def _preflight_market_event_lease(event: dict[str, Any]) -> bool:
    """Confirm a full lease budget before targeted exchange reads begin."""

    settings = get_settings()
    if not bool(getattr(settings, "MARKET_EVENT_LEASE_PREFLIGHT_ENABLED", False)):
        return True
    event_id = int(event.get("id") or 0)
    token = str(event.get("lease_token") or "")
    generation = int(event.get("lease_generation") or 0)
    if event_id <= 0 or not token or generation <= 0:
        return True
    phase = str(event.get("phase") or "").strip().upper()
    lane = str(event.get("watch_lane") or "critical").strip().lower()
    attempts = int(event.get("attempts") or 0)
    long_check = lane == "admin" or phase in {
        "DEEP_CHECK_PENDING",
        "FINAL_CHECK_PENDING",
        "MANUAL_REVIEW",
    } or attempts >= 3
    if not long_check:
        # A fresh critical claim already owns the normal 120-second lease. Do
        # not add a redundant DB round-trip before STOP/TP1 fast-path work.
        return True
    lease_seconds = max(
        30.0,
        float(getattr(settings, "MARKET_EVENT_LEASE_EXTENSION_SEC", 120.0) or 120.0),
    )
    try:
        extended = await _bounded_db_call(
            "preflight_market_event_lease",
            db.extend_market_event_lease(
                event_id,
                lease_token=token,
                lease_generation=generation,
                lease_seconds=lease_seconds,
            ),
            timeout_sec=_EVENT_DB_TIMEOUT_SEC,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        record_monitor_error("event_verify.lease_preflight", exc)
        log.warning(
            "MARKET_EVENT_LEASE_PREFLIGHT_FAILED event_id=%s generation=%s error=%s",
            event_id,
            generation,
            f"{type(exc).__name__}: {exc}",
        )
        return False
    if not extended:
        log.warning(
            "MARKET_EVENT_LEASE_SUPERSEDED event_id=%s generation=%s phase=preflight",
            event_id,
            generation,
        )
        return False
    event["lease_preflight_seconds"] = lease_seconds
    return True


async def _market_event_lease_heartbeat(
    event: dict[str, Any], ownership_lost: asyncio.Event
) -> None:
    """Extend one lease and signal the worker before stale side effects continue."""

    event_id = int(event.get("id") or 0)
    token = str(event.get("lease_token") or "")
    generation = int(event.get("lease_generation") or 0)
    if event_id <= 0 or not token or generation <= 0:
        return
    last_confirmed_owner = time.monotonic()
    heartbeat_sec = (
        _EVENT_ADMIN_LEASE_HEARTBEAT_SEC
        if str(event.get("watch_lane") or "critical").strip().lower() == "admin"
        else _EVENT_LEASE_HEARTBEAT_SEC
    )
    while True:
        await asyncio.sleep(heartbeat_sec)
        try:
            extended = await _bounded_db_call(
                "extend_market_event_lease",
                db.extend_market_event_lease(
                    event_id,
                    lease_token=token,
                    lease_generation=generation,
                    lease_seconds=_EVENT_LEASE_EXTEND_SEC,
                ),
                timeout_sec=_EVENT_DB_TIMEOUT_SEC,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            record_monitor_error("event_verify.lease_heartbeat", exc)
            age = time.monotonic() - last_confirmed_owner
            log.warning(
                "MARKET_EVENT_LEASE_HEARTBEAT_FAILED event_id=%s generation=%s "
                "owner_unconfirmed_sec=%.1f error=%s",
                event_id,
                generation,
                age,
                f"{type(exc).__name__}: {exc}",
            )
            if age >= _EVENT_LEASE_ABORT_AFTER_SEC:
                ownership_lost.set()
                log.error(
                    "MARKET_EVENT_LEASE_OWNER_UNCONFIRMED_ABORT event_id=%s "
                    "generation=%s owner_unconfirmed_sec=%.1f",
                    event_id,
                    generation,
                    age,
                )
                return
            continue
        if not extended:
            ownership_lost.set()
            log.warning(
                "MARKET_EVENT_LEASE_SUPERSEDED event_id=%s generation=%s phase=heartbeat",
                event_id,
                generation,
            )
            return
        last_confirmed_owner = time.monotonic()


async def _process_market_event(
    event: dict[str, Any], notify: NotifyFn | None
) -> None:
    """Run one fenced verifier and abort promptly if durable ownership is lost."""

    token = str(event.get("lease_token") or "")
    generation = int(event.get("lease_generation") or 0)
    if not token or generation <= 0:
        await _process_market_event_owned(event, notify)
        return
    if not await _preflight_market_event_lease(event):
        raise MarketEventLeaseLost(
            f"market event lease preflight lost id={int(event.get('id') or 0)} "
            f"generation={generation}"
        )

    ownership_lost = asyncio.Event()
    worker = asyncio.create_task(_process_market_event_owned(event, notify))
    heartbeat = asyncio.create_task(
        _market_event_lease_heartbeat(event, ownership_lost)
    )
    lost_wait = asyncio.create_task(ownership_lost.wait())
    try:
        done, _pending = await asyncio.wait(
            {worker, lost_wait}, return_when=asyncio.FIRST_COMPLETED
        )
        if lost_wait in done and ownership_lost.is_set() and not worker.done():
            log.warning(
                "MARKET_EVENT_STALE_WORKER_ABORT event_id=%s group_id=%s "
                "event=%s generation=%s action=cancel_without_retry_increment",
                int(event.get("id") or 0),
                int(event.get("trade_group_id") or 0),
                str(event.get("event_key") or event.get("event_type") or "unknown"),
                generation,
            )
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass
            raise MarketEventLeaseLost(
                f"market event lease lost id={int(event.get('id') or 0)} "
                f"generation={generation}"
            )
        await worker
    finally:
        if not worker.done():
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass
        lost_wait.cancel()
        heartbeat.cancel()
        for task in (lost_wait, heartbeat):
            try:
                await task
            except asyncio.CancelledError:
                pass


async def _verify_user_rows(
    *,
    rows: list[dict[str, Any]],
    event_type: str,
    symbol: str,
    observed_price: float,
    notify: NotifyFn | None,
    event_level_index: int = 0,
    shared_adapter_cache: dict[tuple[int, str], Any] | None = None,
    market_event_exchange_context: MarketEventExchangeContext | None = None,
) -> int:
    """Run only the monitors relevant to one event for one user."""
    prices = {symbol: float(observed_price)} if observed_price > 0 else None
    total = 0
    kind = str(event_type or "").upper()
    shared_kwargs: dict[str, Any] = {}
    if market_event_exchange_context is not None:
        shared_kwargs = {
            "shared_adapter_cache": shared_adapter_cache,
            "market_event_exchange_context": market_event_exchange_context,
        }

    if kind == "ENTRY":
        pending = [r for r in rows if str(r.get("status") or "") == "pending_limit"]
        if pending:
            total += await process_pending_limit_tp_catchup_once(
                notify=notify,
                rows_override=pending,
                market_prices=prices,
                **shared_kwargs,
            )
        # A fast fill can disappear again before catch-up sees it. Lifecycle is
        # still run so a closed/stop-out LIMIT is classified and cleaned.
        total += await process_position_lifecycle_guard_once(
            notify=notify,
            rows_override=rows,
            market_prices=prices,
            **shared_kwargs,
        )
        return total

    if kind == "TP":
        # Protective work is intentionally first.  A TP event must try the BE
        # replacement and exact lifecycle reconciliation before any LIMIT-policy
        # or partial-TP recovery work can consume exchange/DB capacity.
        pending = [r for r in rows if str(r.get("status") or "") == "pending_limit"]
        partial = [r for r in rows if str(r.get("status") or "") == "partial_error"]
        # v1.0.7g1: protective priority fast-path.
        #
        # Live ARBUSDT evidence showed that running full lifecycle/TP
        # reconciliation first could delay the new BE STOP write by more than
        # 12 seconds even though TP1 was already confirmed on BingX.  The BE
        # engine independently requires BOTH the TP price touch and a fresh
        # position reduction (or exact history), then force-refreshes positions
        # again before using STOP qty/positionId.  It is therefore safe to run
        # the protective BE path before non-protective lifecycle notifications.
        #
        # Exchange-write safety is unchanged inside process_be_monitor_once:
        # new STOP -> direct exact read-back -> old STOP exact cancel -> final
        # direct read-back.  Lifecycle still runs immediately afterwards for TP
        # ledger reconciliation, close classification and user notifications.
        priority_started = time.monotonic()
        log.info(
            "TP_BE_PRIORITY_FAST_PATH phase=start uid=%s symbol=%s tp=%s rows=%s",
            int(rows[0].get("user_id") or 0) if rows else 0,
            symbol,
            int(event_level_index or 0),
            len(rows),
        )
        be_actions = 0
        be_error: Exception | None = None
        be_traceback = None
        try:
            be_actions = await process_be_monitor_once(
                notify=notify,
                rows_override=rows,
                market_prices=prices,
                event_level_index=event_level_index,
                **shared_kwargs,
            )
        except asyncio.CancelledError:
            # Cancellation is control flow: do not start more exchange/DB work.
            raise
        except db.MonitorAdvisoryBusy:
            # Preserve the durable no-attempt-increment advisory deferral path.
            raise
        except Exception as exc:
            # v1.0.7g1: a generic BE failure must not suppress the targeted TP
            # lifecycle reconciliation.  Preserve the original traceback and
            # re-raise it after lifecycle so the durable event retry remains.
            be_error = exc
            be_traceback = exc.__traceback__
            log.warning(
                "TP_BE_PRIORITY_FAST_PATH phase=failed uid=%s symbol=%s tp=%s "
                "rows=%s error_type=%s duration_ms=%s",
                int(rows[0].get("user_id") or 0) if rows else 0,
                symbol,
                int(event_level_index or 0),
                len(rows),
                type(exc).__name__,
                int((time.monotonic() - priority_started) * 1000),
            )
        else:
            total += be_actions
            log.info(
                "TP_BE_PRIORITY_FAST_PATH phase=complete uid=%s symbol=%s tp=%s "
                "rows=%s actions=%s duration_ms=%s",
                int(rows[0].get("user_id") or 0) if rows else 0,
                symbol,
                int(event_level_index or 0),
                len(rows),
                int(be_actions),
                int((time.monotonic() - priority_started) * 1000),
            )

        reconcile_started = time.monotonic()
        try:
            lifecycle_actions = await process_position_lifecycle_guard_once(
                notify=notify,
                rows_override=rows,
                market_prices=prices,
                **shared_kwargs,
            )
        except asyncio.CancelledError:
            raise
        except db.MonitorAdvisoryBusy as lifecycle_busy:
            # Advisory contention must remain a durable deferral with no retry
            # attempt consumed.  Keep any earlier BE failure visible as context.
            if be_error is not None:
                log.error(
                    "TP_BE_PRIORITY_FAST_PATH BE failed before lifecycle advisory "
                    "deferral uid=%s symbol=%s tp=%s be_error_type=%s",
                    int(rows[0].get("user_id") or 0) if rows else 0,
                    symbol,
                    int(event_level_index or 0),
                    type(be_error).__name__,
                    exc_info=(type(be_error), be_error, be_traceback),
                )
                lifecycle_busy.add_note(
                    f"preceded by BE failure: {type(be_error).__name__}: {be_error}"
                )
            raise
        except Exception as lifecycle_error:
            if be_error is not None:
                be_error.add_note(
                    "post-BE lifecycle also failed: "
                    f"{type(lifecycle_error).__name__}: {lifecycle_error}"
                )
                raise be_error.with_traceback(be_traceback) from lifecycle_error
            raise
        else:
            total += lifecycle_actions
            log.info(
                "TP_POST_BE_RECONCILE phase=complete uid=%s symbol=%s tp=%s "
                "rows=%s actions=%s duration_ms=%s be_failed=%s",
                int(rows[0].get("user_id") or 0) if rows else 0,
                symbol,
                int(event_level_index or 0),
                len(rows),
                int(lifecycle_actions),
                int((time.monotonic() - reconcile_started) * 1000),
                int(be_error is not None),
            )

        if be_error is not None:
            raise be_error.with_traceback(be_traceback)

        # Non-protective recovery follows only after BE/lifecycle succeeded.
        if pending:
            total += await process_pending_limit_tp_catchup_once(
                notify=notify,
                rows_override=pending,
                market_prices=prices,
                **shared_kwargs,
            )
        if partial:
            total += await process_partial_tp_recovery_once(
                notify=notify,
                rows_override=partial,
                market_prices=prices,
                **shared_kwargs,
            )
        return total

    if kind == "STOP":
        # STOP/zero-exposure classification has absolute priority.  Recovery of
        # pending LIMIT or partial TP state may run only after the protective
        # lifecycle pass completes successfully.
        pending = [r for r in rows if str(r.get("status") or "") == "pending_limit"]
        partial = [r for r in rows if str(r.get("status") or "") == "partial_error"]
        total += await process_position_lifecycle_guard_once(
            notify=notify,
            rows_override=rows,
            market_prices=prices,
            **shared_kwargs,
        )
        if pending:
            total += await process_pending_limit_tp_catchup_once(
                notify=notify,
                rows_override=pending,
                market_prices=prices,
                **shared_kwargs,
            )
        if partial:
            total += await process_partial_tp_recovery_once(
                notify=notify,
                rows_override=partial,
                market_prices=prices,
                **shared_kwargs,
            )
        return total

    return total


async def _verify_user_rows_with_context(
    *,
    rows: list[dict[str, Any]],
    event_type: str,
    symbol: str,
    observed_price: float,
    notify: NotifyFn | None,
    event_level_index: int = 0,
) -> int:
    """Run one targeted event pass with an optional unified exchange snapshot."""

    settings = get_settings()
    enabled = bool(
        getattr(settings, "MARKET_EVENT_READ_COALESCING_ENABLED", False)
    )
    if not enabled:
        return await _verify_user_rows(
            rows=rows,
            event_type=event_type,
            symbol=symbol,
            observed_price=observed_price,
            notify=notify,
            event_level_index=event_level_index,
        )

    context = MarketEventExchangeContext(
        ttl_scale=float(
            getattr(settings, "MARKET_EVENT_READ_CACHE_TTL_SCALE", 1.0) or 1.0
        )
    )
    adapter_cache: dict[tuple[int, str], Any] = {}
    started = time.monotonic()
    try:
        return await _verify_user_rows(
            rows=rows,
            event_type=event_type,
            symbol=symbol,
            observed_price=observed_price,
            notify=notify,
            event_level_index=event_level_index,
            shared_adapter_cache=adapter_cache,
            market_event_exchange_context=context,
        )
    finally:
        stats = context.stats()
        log.info(
            "MARKET_EVENT_EXCHANGE_SNAPSHOT event=%s uid=%s symbol=%s "
            "duration_ms=%s %s",
            str(event_type or "unknown").upper(),
            int(rows[0].get("user_id") or 0) if rows else 0,
            str(symbol or "").upper(),
            int((time.monotonic() - started) * 1000),
            " ".join(f"{key}={value}" for key, value in stats.items()),
        )
        await context.close_adapters(adapter_cache)


async def _process_market_event_owned(event: dict[str, Any], notify: NotifyFn | None) -> None:
    settings = get_settings()
    event_id = int(event.get("id") or 0)
    group_id = int(event.get("trade_group_id") or 0)
    attempts_before = int(event.get("attempts") or 0)
    event_type = str(event.get("event_type") or "").upper()
    event_level_index = int(event.get("level_index") or 0)
    observed_price = _f(event.get("observed_price"), 0.0)
    event_key = str(event.get("event_key") or "")
    lease_token = str(event.get("lease_token") or "")
    lease_generation = int(event.get("lease_generation") or 0)
    terminal_review_allowed = (
        _terminal_review_allowed_for_group(settings, group_id)
        or g67_prepared_target_event_allowed(settings, event)
    )
    local_key = (group_id, event_key)
    single_probe = local_key in _LOCAL_EVENT_SINGLE_PROBE
    if not event_id or not group_id:
        return

    # Measure scheduler lag from the durable due time, not from row creation.
    # market_events rows are reused across genuine re-crosses, so created_at can
    # be days old even when a fresh STOP/TP was queued on time.
    queue_due_lag_sec = _event_age_seconds(
        event.get("next_attempt_at") or event.get("created_at")
    )
    queue_sla_sec = float(_EVENT_SLA_WARN_SEC.get(event_type, 120.0))
    if queue_due_lag_sec is not None and queue_due_lag_sec > queue_sla_sec:
        log.warning(
            "MARKET_EVENT_QUEUE_SLA_BREACH event_id=%s group_id=%s event=%s "
            "due_lag_sec=%.3f threshold_sec=%.1f priority=%s",
            event_id,
            group_id,
            event_key or event_type or "unknown",
            queue_due_lag_sec,
            queue_sla_sec,
            int(event.get("event_priority") or db.market_event_priority(event_type)),
        )

    group_lock = _group_lock(group_id)
    lock_wait_started = time.monotonic()
    async with group_lock:
        record_wait("lock_wait_ms", (time.monotonic() - lock_wait_started) * 1000)
        error = ""
        actions = 0
        users_count = 0
        busy_deferred = False
        results: list[tuple[int, str, bool, str]] = []
        exact_terminal = False
        exact_outcome = "retry_required"
        outcome_reasons: Counter[str] = Counter()
        manual_tp_watch_users = 0
        initial_user_ids: set[int] = set()
        symbol = ""
        side = ""
        status_summary: list[str] = []
        coalesced_keys: list[str] = []
        shadow_snapshot: dict[str, Any] | None = None
        state_machine_persisted = False
        try:
            rows = await _bounded_db_call(
                "trade_group_executions",
                db.trade_group_executions(group_id, active_only=True, limit=5000),
                timeout_sec=_EVENT_DB_TIMEOUT_SEC,
            )
            if not rows:
                # Every trackable execution finished before the verifier acquired
                # the group lock.  Finish only through the claimed lease so a
                # stale worker can never overwrite a newer owner.
                log.info(
                    "MARKET_EVENT_STALE_TERMINAL group_id=%s event=%s attempt=%s "
                    "reason=no_active_executions",
                    group_id,
                    event_key or event_type or "unknown",
                    attempts_before + 1,
                )
                stale_finish_kwargs: dict[str, Any] = {
                    "done": True,
                    "retry_after_sec": 0.0,
                    "error": "",
                    "increment_attempt": True,
                    "force_terminal": True,
                }
                if lease_token and lease_generation > 0:
                    stale_finish_kwargs.update(
                        lease_token=lease_token,
                        lease_generation=lease_generation,
                        outcome_kind="terminal_no_active_execution",
                    )
                finished = await _bounded_db_call(
                    "finish_market_event_stale_terminal",
                    db.finish_market_event(event_id, **stale_finish_kwargs),
                    timeout_sec=_EVENT_DB_TIMEOUT_SEC,
                )
                if finished is not False:
                    _clear_local_event_state(group_id, event_key)
                else:
                    log.warning(
                        "MARKET_EVENT_LEASE_CONFLICT event_id=%s group_id=%s "
                        "event=%s generation=%s phase=stale_terminal",
                        event_id,
                        group_id,
                        event_key or event_type or "unknown",
                        lease_generation,
                    )
                return

            manual_ids = [
                int(row.get("id") or 0)
                for row in rows
                if str(row.get("status") or "").strip().lower() == "manual_required"
                and int(row.get("id") or 0) > 0
            ]
            if manual_ids:
                await _bounded_db_call(
                    "wake_critical_backoff_event",
                    db.wake_critical_backoff(manual_ids),
                    timeout_sec=_EVENT_DB_TIMEOUT_SEC,
                )
            symbol = str(rows[0].get("symbol") or "").upper()
            side = str(rows[0].get("side") or "").upper()
            by_user: dict[int, list[dict[str, Any]]] = defaultdict(list)
            for row in rows:
                uid = int(row.get("user_id") or 0)
                if uid:
                    by_user[uid].append(row)
                    initial_user_ids.add(uid)
            users_count = len(by_user)
            if not by_user:
                raise RuntimeError("group has no valid user executions")

            event_admin_lane = (
                str(event.get("watch_lane") or "critical").strip().lower() == "admin"
            )
            verify_sem = _event_verify_semaphore(
                _event_verify_concurrency_limit(int(settings.EVENT_VERIFY_WORKERS)),
                admin_lane=event_admin_lane,
            )

            async def one_user(
                uid: int, user_rows: list[dict[str, Any]]
            ) -> tuple[int, str, bool, str]:
                sem_wait_started = time.monotonic()
                async with verify_sem:
                    sem_wait_ms = (time.monotonic() - sem_wait_started) * 1000
                    with diagnostic_span(
                        f"event_verify.{event_type.lower() or 'unknown'}",
                        emit=True,
                        metadata={
                            "event_type": event_type or "unknown",
                            "attempt": attempts_before + 1,
                            "rows_selected": len(user_rows),
                            "rows_source": "event_user",
                        },
                    ) as verify_span:
                        record_wait("semaphore_wait_ms", sem_wait_ms)
                        try:
                            async with db.monitor_deferred_capture() as deferred_reasons:
                                count = await _verify_user_rows_with_context(
                                    rows=user_rows,
                                    event_type=event_type,
                                    symbol=symbol,
                                    observed_price=observed_price,
                                    notify=notify,
                                    event_level_index=event_level_index,
                                )
                            verify_span.set("result", int(count))
                            no_action_reason = (
                                _event_no_action_reason(
                                    user_rows,
                                    event_type=event_type,
                                    event_level_index=event_level_index,
                                )
                                if int(count) == 0
                                else ""
                            )
                            if no_action_reason:
                                verify_span.set("no_action_reason", no_action_reason)
                            if deferred_reasons:
                                verify_span.set("deferred", len(deferred_reasons))
                                details = ",".join(
                                    f"{item.get('phase')}:{item.get('key')}"
                                    for item in deferred_reasons[:3]
                                )
                                log.warning(
                                    "EVENT_VERIFY_LOCK_DEFERRED group_id=%s event=%s uid=%s "
                                    "reasons=%s",
                                    group_id,
                                    event_type,
                                    uid,
                                    details,
                                )
                                return int(count), f"uid={uid} advisory busy: {details}", True, ""
                            return int(count), "", False, no_action_reason
                        except db.MonitorAdvisoryBusy as exc:
                            verify_span.set("result", 0)
                            verify_span.set("deferred", 1)
                            log.warning(
                                "EVENT_VERIFY_LOCK_DEFERRED group_id=%s event=%s uid=%s "
                                "phase=%s stage=%s key=%s",
                                group_id,
                                event_type,
                                uid,
                                exc.phase,
                                exc.stage,
                                exc.key,
                            )
                            return 0, f"uid={uid} advisory busy: {exc.phase}:{exc.key}", True, ""
                        except Exception as exc:
                            verify_span.errors += 1
                            verify_span.set("result", 0)
                            sample_row = user_rows[0] if user_rows else {}
                            record_monitor_error(
                                "event_verify.user",
                                exc,
                                execution_id=_safe_int(sample_row.get("id")) or None,
                                symbol=str(sample_row.get("symbol") or "") or None,
                            )
                            log.exception(
                                "event verification failed group_id=%s event=%s uid=%s",
                                group_id,
                                event_type,
                                uid,
                            )
                            return 0, f"uid={uid} {type(exc).__name__}: {exc}", False, ""

            results = await asyncio.gather(
                *(one_user(uid, user_rows) for uid, user_rows in by_user.items()),
                return_exceptions=False,
            )
            actions = sum(item[0] for item in results)
            user_errors = [item[1] for item in results if item[1]]
            busy_deferred = any(bool(item[2]) for item in results)
            no_action_reasons = Counter(
                item[3] for item in results if len(item) > 3 and item[3]
            )
            if user_errors:
                error = "; ".join(user_errors)[:1000]

            # Completion is determined from a fresh durable post-state, never
            # from a generic positive action count.  This closes the class of
            # bugs where a diagnostic/manual transition accidentally consumed
            # TP/STOP before its exact objective was achieved.
            post_rows = await _bounded_db_call(
                "trade_group_executions_post_state",
                db.trade_group_executions(group_id, active_only=False, limit=5000),
                timeout_sec=_EVENT_DB_TIMEOUT_SEC,
            )
            status_summary = sorted(
                {str(row.get("status") or "unknown") for row in post_rows}
            )
            post_by_user: dict[int, list[dict[str, Any]]] = defaultdict(list)
            for row in post_rows:
                uid = int(row.get("user_id") or 0)
                if uid in initial_user_ids:
                    post_by_user[uid].append(row)

            exact_results: list[tuple[bool, str, str]] = []
            for uid in sorted(initial_user_ids):
                result = _classify_user_event_outcome(
                    post_by_user.get(uid, []),
                    event_type=event_type,
                    event_level_index=event_level_index,
                )
                exact_results.append(result)
                if result[2]:
                    outcome_reasons[result[2]] += 1
            exact_terminal = bool(exact_results) and all(item[0] for item in exact_results)
            exact_kinds = sorted({item[1] for item in exact_results})
            if exact_terminal:
                exact_outcome = (
                    exact_kinds[0] if len(exact_kinds) == 1 else "terminal_mixed_exact"
                )
            elif exact_kinds:
                exact_outcome = (
                    exact_kinds[0] if len(exact_kinds) == 1 else "retry_mixed_exact"
                )
            manual_tp_watch_users = (
                int(outcome_reasons.get("manual_required_only", 0))
                if event_type == "TP"
                else 0
            )

            # g39 evidence remains shadow-only unless the separately guarded
            # g40 finite state machine is enabled. In g40 mode the same immutable
            # snapshot is durably persisted before any queue transition.
            evidence_enabled = (
                bool(getattr(settings, "MARKET_EVENT_EVIDENCE_SNAPSHOT_ENABLED", False))
                or bool(getattr(settings, "MARKET_EVENT_SPLIT_ENTRY_TP_STATE_ENABLED", False))
                or bool(getattr(settings, "MARKET_EVENT_TERMINAL_REVIEW_ENABLED", False))
            )
            if evidence_enabled:
                try:
                    shadow_snapshot = build_market_event_evidence_snapshot(event, post_rows)
                    if terminal_review_allowed:
                        persisted = await _bounded_db_call(
                            "market_event_state_machine_evidence",
                            db.record_market_event_shadow_evidence(
                                event_id,
                                snapshot=shadow_snapshot,
                                execution_states=market_event_execution_state_rows(shadow_snapshot),
                                worker_id=(
                                    f"event-worker:{lease_token[:12]}"
                                    if lease_token else "event-worker:unfenced"
                                ),
                                lease_generation=lease_generation,
                            ),
                            timeout_sec=_EVENT_DB_TIMEOUT_SEC,
                        )
                        state_machine_persisted = bool(persisted.get("written"))
                    else:
                        _schedule_market_event_evidence_shadow(
                            event_id=event_id,
                            group_id=group_id,
                            event_label=event_key or event_type or "unknown",
                            snapshot=shadow_snapshot,
                            worker_id=(
                                f"event-worker:{lease_token[:12]}"
                                if lease_token else "event-worker:unfenced"
                            ),
                            lease_generation=lease_generation,
                        )
                except asyncio.CancelledError:
                    raise
                except Exception as shadow_exc:
                    record_monitor_error("event_verify.evidence_shadow_build", shadow_exc)
                    shadow_snapshot = None
                    state_machine_persisted = False
                    log.warning(
                        "MARKET_EVENT_EVIDENCE_BUILD_OR_STORE_FAILED event_id=%s "
                        "group_id=%s event=%s error_type=%s error=%s",
                        event_id, group_id, event_key or event_type or "unknown",
                        type(shadow_exc).__name__, str(shadow_exc)[:300],
                    )

            no_action_reasons.update(outcome_reasons)
            no_action_reasons_json = json.dumps(
                dict(sorted(no_action_reasons.items())),
                sort_keys=True,
                separators=(",", ":"),
            )
            log_method = log.warning if user_errors else log.info
            log_method(
                "market event verified group_id=%s event=%s users=%s actions=%s "
                "attempt=%s exact_terminal=%s exact_outcome=%s "
                "failed_users=%s no_action_reasons=%s",
                group_id,
                event_key or event_type,
                users_count,
                actions,
                attempts_before + 1,
                int(exact_terminal),
                exact_outcome,
                len(user_errors),
                no_action_reasons_json,
            )
        except db.MonitorAdvisoryBusy as exc:
            busy_deferred = True
            error = f"advisory busy: {exc.phase}:{exc.key}"
            exact_outcome = "advisory_deferred"
            log.warning(
                "EVENT_VERIFY_LOCK_DEFERRED group_id=%s event=%s attempt=%s "
                "phase=%s stage=%s key=%s",
                group_id,
                event_key,
                attempts_before + 1,
                exc.phase,
                exc.stage,
                exc.key,
            )
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            exact_outcome = "verification_error"
            record_monitor_error("event_verify.event", exc)
            log.warning(
                "market event verification deferred group_id=%s event=%s attempt=%s error=%s",
                group_id,
                event_key,
                attempts_before + 1,
                error,
            )

        attempts_after = attempts_before + (0 if busy_deferred else 1)
        max_attempts = max(1, int(settings.EVENT_VERIFY_ATTEMPTS))
        base_retry = max(0.05, float(settings.EVENT_VERIFY_RETRY_BASE_SEC))
        watch_commit: dict[str, Any] | None = None
        watch_specs: list[dict[str, Any]] = []

        pending_limit_watch = _pending_limit_admin_watch(
            event_type=event_type,
            error=error,
            outcome_reasons=outcome_reasons,
        )
        if pending_limit_watch:
            coalesced = await _bounded_db_call(
                "coalesce_pending_limit_tp_events",
                db.coalesce_pending_limit_tp_events(
                    trade_group_id=group_id,
                    current_event_id=event_id,
                    current_lease_token=lease_token,
                    current_lease_generation=lease_generation,
                ),
                timeout_sec=_EVENT_DB_TIMEOUT_SEC,
            )
            if bool(coalesced.get("lease_conflict")):
                log.warning(
                    "MARKET_EVENT_LEASE_CONFLICT event_id=%s group_id=%s "
                    "event=%s generation=%s phase=coalesce_precondition",
                    event_id,
                    group_id,
                    event_key or event_type or "unknown",
                    lease_generation,
                )
                return
            coalesced_keys = [
                str(value) for value in (coalesced.get("event_keys") or []) if str(value)
            ]
            canonical_event_id = int(coalesced.get("canonical_event_id") or 0)
            if canonical_event_id and canonical_event_id != event_id:
                finished = await _bounded_db_call(
                    "finish_market_event_coalesced",
                    db.finish_market_event(
                        event_id,
                        done=True,
                        retry_after_sec=0.0,
                        error="coalesced into pending-limit group watch",
                        increment_attempt=not busy_deferred,
                        force_terminal=True,
                        lease_token=lease_token,
                        lease_generation=lease_generation,
                        outcome_kind="coalesced_pending_limit_tp",
                    ),
                    timeout_sec=_EVENT_DB_TIMEOUT_SEC,
                )
                if finished is False:
                    log.warning(
                        "MARKET_EVENT_LEASE_CONFLICT event_id=%s group_id=%s "
                        "event=%s generation=%s phase=coalesce",
                        event_id,
                        group_id,
                        event_key or event_type or "unknown",
                        lease_generation,
                    )
                return
        if not coalesced_keys:
            try:
                raw_keys = json.loads(str(event.get("coalesced_event_keys") or "[]"))
                if isinstance(raw_keys, list):
                    coalesced_keys = [str(value) for value in raw_keys if str(value)]
            except (TypeError, ValueError, json.JSONDecodeError):
                coalesced_keys = []

        # g40 Step 2: finite ENTRY/TP evidence state machine. It is deliberately
        # placed after pending-limit TP coalescing so one canonical group event
        # owns the decision. STOP and verifier errors remain on legacy safety paths.
        state_machine_enabled = terminal_review_allowed
        if (
            state_machine_enabled
            and not busy_deferred
            and not error
            and event_type in {"ENTRY", "TP"}
            and shadow_snapshot is not None
            and state_machine_persisted
        ):
            transition = decide_market_event_state_machine(
                event,
                shadow_snapshot,
                max_fast_attempts=int(getattr(settings, "MARKET_EVENT_MAX_FAST_ATTEMPTS", 3)),
                max_deep_attempts=int(getattr(settings, "MARKET_EVENT_MAX_DEEP_ATTEMPTS", 2)),
                max_final_attempts=int(getattr(settings, "MARKET_EVENT_MAX_FINAL_ATTEMPTS", 1)),
            )
            if transition.applicable:
                committed = await _bounded_db_call(
                    "commit_market_event_state_machine",
                    db.commit_market_event_state_machine(
                        event_id,
                        phase=transition.phase,
                        terminal=transition.terminal,
                        manual_review=transition.manual_review,
                        outcome=transition.outcome,
                        reason=transition.reason,
                        retry_after_sec=transition.retry_after_sec,
                        fast_attempts=transition.fast_attempts,
                        deep_attempts=transition.deep_attempts,
                        final_attempts=transition.final_attempts,
                        lease_token=lease_token,
                        lease_generation=lease_generation,
                    ),
                    timeout_sec=_EVENT_DB_TIMEOUT_SEC,
                )
                if not committed:
                    log.warning(
                        "MARKET_EVENT_LEASE_CONFLICT event_id=%s group_id=%s event=%s "
                        "generation=%s phase=state_machine_commit",
                        event_id, group_id, event_key or event_type or "unknown",
                        lease_generation,
                    )
                    return
                if transition.manual_review:
                    quarantined = await db.mark_market_event_statistics_manual_review(
                        group_id, reason="market_event_manual_review"
                    )
                    log.warning(
                        "MARKET_EVENT_MANUAL_REVIEW event_id=%s group_id=%s event=%s "
                        "attempts=%s fast=%s deep=%s final=%s reason=%s "
                        "statistics_quarantined=%s automation_enabled=0",
                        event_id, group_id, event_key or event_type or "unknown",
                        transition.total_attempts, transition.fast_attempts,
                        transition.deep_attempts, transition.final_attempts,
                        transition.reason, quarantined,
                    )
                    try:
                        await _notify_market_event_manual_review(
                            notify,
                            event=event,
                            transition=transition,
                            snapshot=shadow_snapshot,
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception as notify_exc:
                        record_monitor_error("event_verify.manual_review_notify", notify_exc)
                        log.warning(
                            "MARKET_EVENT_MANUAL_REVIEW_NOTIFY_DEFERRED event_id=%s "
                            "group_id=%s error=%s",
                            event_id, group_id,
                            f"{type(notify_exc).__name__}: {notify_exc}",
                        )
                elif transition.terminal:
                    _clear_local_event_state(group_id, event_key)
                    review_cleared = 0
                    if str(event.get("migration_reason") or "") == "admin_final_recheck":
                        review_cleared = await db.clear_market_event_statistics_manual_review(
                            group_id,
                            reason="exact_exchange_evidence_after_manual_recheck",
                        )
                    log.info(
                        "MARKET_EVENT_STATE_MACHINE_COMPLETED event_id=%s group_id=%s "
                        "event=%s outcome=%s reason=%s statistics_review_cleared=%s",
                        event_id, group_id, event_key or event_type or "unknown",
                        transition.outcome, transition.reason, review_cleared,
                    )
                else:
                    log.info(
                        "MARKET_EVENT_STATE_MACHINE_RETRY event_id=%s group_id=%s "
                        "event=%s phase=%s fast=%s deep=%s final=%s "
                        "retry_after_sec=%.1f reason=%s",
                        event_id, group_id, event_key or event_type or "unknown",
                        transition.phase, transition.fast_attempts,
                        transition.deep_attempts, transition.final_attempts,
                        transition.retry_after_sec, transition.reason,
                    )
                return

        promote_admin_watch = (
            not busy_deferred
            and not exact_terminal
            and str(event.get("watch_lane") or "critical").strip().lower() == "admin"
            and not pending_limit_watch
        )
        if promote_admin_watch:
            # A pending LIMIT group may become live between administrative
            # checks.  Move it back to the critical lane immediately instead
            # of waiting for the old administrative cadence.
            done = False
            retry_after = _deterministic_retry_with_jitter(
                2.0, event_id=event_id, group_id=group_id
            )
            watch_commit = {
                "retry_after_sec": retry_after,
                "error": error,
                "increment_attempt": False,
                "reset_attempts": True,
                "outcome_kind": "admin_watch_promoted_critical",
                "watch_lane": "critical",
                "escalated_at": None,
                "stuck_started_at": None,
                "last_stuck_alert_at": None,
                "last_stuck_reminder_at": None,
                "stuck_reason": _dominant_watch_reason(
                    error=error,
                    outcome_reasons=outcome_reasons,
                    exact_outcome=exact_outcome,
                ),
                # Promotion is a new live safety incident.  Historical pending-
                # limit TP aggregation must not leak into the critical card.
                "coalesced_event_keys": "[]",
            }
            exact_outcome = "admin_watch_promoted_critical"
            log.warning(
                "MARKET_EVENT_ADMIN_WATCH_PROMOTED group_id=%s event=%s "
                "retry_after_sec=%.1f reason=%s",
                group_id,
                event_key or event_type or "unknown",
                retry_after,
                str(watch_commit["stuck_reason"]),
            )
        elif busy_deferred:
            done = False
            retry_after = base_retry
        elif exact_terminal and not error:
            done = True
            retry_after = 0.0
            _LOCAL_EVENT_SAFETY_RECHECK_AT.pop(local_key, None)
        elif manual_tp_watch_users and not error:
            retry_after = _manual_tp_watch_retry_seconds(attempts_after)
            if retry_after is None:
                done = False
                (
                    watch_commit,
                    watch_specs,
                    retry_after,
                    watch_reason,
                    decision,
                ) = _prepare_watch_transition(
                    event=event,
                    event_type=event_type,
                    event_id=event_id,
                    group_id=group_id,
                    symbol=symbol,
                    side=side,
                    users_count=users_count,
                    statuses=status_summary,
                    error=error,
                    outcome_reasons=outcome_reasons,
                    exact_outcome=exact_outcome,
                    coalesced_keys=coalesced_keys,
                    rows=rows,
                    event_level_index=event_level_index,
                    manual_live_tp=True,
                    pending_limit_watch=False,
                )
                exact_outcome = str(watch_commit["outcome_kind"])
                log.warning(
                    "%s group_id=%s event=%s attempt=%s retry_after_sec=%.1f "
                    "manual_users=%s lane=critical reason=%s",
                    "MARKET_EVENT_STUCK_WATCH"
                    if decision.is_stuck
                    else "MARKET_EVENT_ESCALATED_WATCH",
                    group_id,
                    event_key or event_type or "unknown",
                    attempts_after,
                    retry_after,
                    manual_tp_watch_users,
                    watch_reason,
                )
            else:
                done = False
                exact_outcome = "manual_tp_watch"
                log.info(
                    "MARKET_EVENT_MANUAL_TP_WATCH group_id=%s event=%s attempt=%s "
                    "retry_after_sec=%.1f manual_users=%s actions=%s",
                    group_id,
                    event_key or event_type or "unknown",
                    attempts_after,
                    retry_after,
                    manual_tp_watch_users,
                    actions,
                )
        elif attempts_after < max_attempts:
            done = False
            retry_after = min(
                _EVENT_ESCALATED_WATCH_RETRY_SEC,
                base_retry * (2 ** min(attempts_before, 12)),
            )
        else:
            late_index = attempts_after - max_attempts
            if late_index < len(_EVENT_LATE_RECOVERY_DELAYS_SEC):
                done = False
                retry_after = _EVENT_LATE_RECOVERY_DELAYS_SEC[late_index]
                log.info(
                    "MARKET_EVENT_LATE_RECOVERY group_id=%s event=%s attempt=%s "
                    "retry_after_sec=%.1f exact_outcome=%s",
                    group_id,
                    event_key,
                    attempts_after,
                    retry_after,
                    exact_outcome,
                )
            else:
                done = False
                (
                    watch_commit,
                    watch_specs,
                    retry_after,
                    watch_reason,
                    decision,
                ) = _prepare_watch_transition(
                    event=event,
                    event_type=event_type,
                    event_id=event_id,
                    group_id=group_id,
                    symbol=symbol,
                    side=side,
                    users_count=users_count,
                    statuses=status_summary,
                    error=error,
                    outcome_reasons=outcome_reasons,
                    exact_outcome=exact_outcome,
                    coalesced_keys=coalesced_keys,
                    rows=rows,
                    event_level_index=event_level_index,
                    manual_live_tp=False,
                    pending_limit_watch=pending_limit_watch,
                )
                exact_outcome = str(watch_commit["outcome_kind"])
                log.warning(
                    "%s group_id=%s event=%s attempt=%s retry_after_sec=%.1f "
                    "lane=%s error=%s single_probe=%s reasons=%s reason=%s",
                    "MARKET_EVENT_STUCK_WATCH"
                    if decision.is_stuck
                    else "MARKET_EVENT_ESCALATED_WATCH",
                    group_id,
                    event_key or event_type or "unknown",
                    attempts_after,
                    retry_after,
                    str(watch_commit["watch_lane"]),
                    error,
                    int(single_probe),
                    json.dumps(dict(outcome_reasons), sort_keys=True),
                    watch_reason,
                )

        try:
            if watch_commit is not None and lease_token and lease_generation > 0:
                finished = await _bounded_db_call(
                    "commit_market_event_watch_result",
                    db.commit_market_event_watch_result(
                        event_id,
                        lease_token=lease_token,
                        lease_generation=lease_generation,
                        notifications=watch_specs,
                        **watch_commit,
                    ),
                    timeout_sec=_EVENT_DB_TIMEOUT_SEC,
                )
            else:
                finish_kwargs: dict[str, Any] = {
                    "done": done,
                    "retry_after_sec": retry_after,
                    "error": error,
                    "increment_attempt": not busy_deferred,
                }
                # Direct maintenance/test invocations without a lease retain
                # backward compatibility through finish_market_event.  Normal
                # production workers always claim a fenced lease and therefore
                # use the atomic event+notification outbox transaction above.
                if watch_commit is not None:
                    finish_kwargs.update(
                        done=False,
                        retry_after_sec=float(watch_commit["retry_after_sec"]),
                        error=str(watch_commit["error"]),
                        increment_attempt=bool(watch_commit["increment_attempt"]),
                    )
                if lease_token and lease_generation > 0:
                    finish_kwargs.update(
                        lease_token=lease_token,
                        lease_generation=lease_generation,
                        outcome_kind=exact_outcome,
                    )
                finished = await _bounded_db_call(
                    "finish_market_event",
                    db.finish_market_event(event_id, **finish_kwargs),
                    timeout_sec=_EVENT_DB_TIMEOUT_SEC,
                )
        except Exception as finish_exc:
            record_monitor_error("event_verify.finish", finish_exc)
            released = False
            release_retry_after = _final_write_failure_retry_after(
                event=event, watch_commit=watch_commit
            )
            try:
                released = await _bounded_db_call(
                    "release_market_event_lease",
                    db.release_market_event_lease(
                        event_id,
                        lease_token=lease_token,
                        lease_generation=lease_generation,
                        retry_after_sec=release_retry_after,
                        error=(
                            "final event write failed: "
                            f"{type(finish_exc).__name__}: {finish_exc}"
                        ),
                    ),
                    timeout_sec=_EVENT_DB_TIMEOUT_SEC,
                )
            except Exception as release_exc:
                record_monitor_error("event_verify.release", release_exc)
                log.exception(
                    "MARKET_EVENT_LEASE_RELEASE_FAILED event_id=%s group_id=%s "
                    "event=%s generation=%s",
                    event_id,
                    group_id,
                    event_key or event_type or "unknown",
                    lease_generation,
                )
            log.error(
                "MARKET_EVENT_FINISH_FAILED event_id=%s group_id=%s event=%s "
                "generation=%s released=%s retry_after_sec=%.3f error=%s",
                event_id,
                group_id,
                event_key or event_type or "unknown",
                lease_generation,
                int(bool(released)),
                release_retry_after,
                f"{type(finish_exc).__name__}: {finish_exc}",
            )
            raise

        if finished is False:
            log.warning(
                "MARKET_EVENT_LEASE_CONFLICT event_id=%s group_id=%s event=%s "
                "generation=%s done=%s outcome=%s",
                event_id,
                group_id,
                event_key or event_type or "unknown",
                lease_generation,
                int(done),
                exact_outcome,
            )
            return
        if watch_commit is not None and watch_specs:
            await _deliver_committed_watch_notifications(notify, watch_specs)
        if done:
            _LOCAL_EVENT_SINGLE_PROBE.discard(local_key)

async def _market_event_verifier_lane_loop(
    notify: NotifyFn | None, *, admin_lane: bool
) -> None:
    settings = get_settings()
    batch_size = (
        1
        if admin_lane
        else _event_verify_concurrency_limit(int(settings.EVENT_VERIFY_WORKERS))
    )
    lane_name = "admin" if admin_lane else "critical"
    diagnostic_name = (
        "cycle.event_admin_watch_batch"
        if admin_lane
        else "cycle.event_verifier_batch"
    )
    while True:
        try:
            with diagnostic_span(diagnostic_name, emit=True) as batch_span:
                claim = (
                    db.claim_due_admin_market_events(limit=batch_size)
                    if admin_lane
                    else db.claim_due_market_events(limit=batch_size)
                )
                events = await _bounded_db_call(
                    f"claim_due_{lane_name}_market_events",
                    claim,
                    timeout_sec=_EVENT_DB_TIMEOUT_SEC,
                )
                batch_span.set("rows_selected", len(events))
                batch_span.set("rows_due", len(events))
                batch_span.set("rows_source", f"market_events:{lane_name}")
                if not events:
                    batch_span.set("result", 0)
                    batch_span.set("_suppress_emit", True)
                    mark_cycle_completed(
                        "event_admin_watch" if admin_lane else "event_verifier"
                    )
                    await asyncio.sleep(2.0 if admin_lane else 0.25)
                    continue
                results = await asyncio.gather(
                    *(_process_market_event(event, notify) for event in events),
                    return_exceptions=True,
                )
                failed = 0
                for event, result in zip(events, results):
                    if isinstance(result, MarketEventLeaseLost):
                        log.warning(
                            "MARKET_EVENT_LEASE_SUPERSEDED lane=%s event_id=%s group_id=%s "
                            "event=%s action=stale_worker_discarded",
                            lane_name,
                            int(event.get("id") or 0),
                            int(event.get("trade_group_id") or 0),
                            str(event.get("event_key") or event.get("event_type") or "unknown"),
                        )
                        continue
                    if isinstance(result, BaseException):
                        failed += 1
                        record_monitor_error(f"event_verifier.{lane_name}.item", result)
                        log.error(
                            "MARKET_EVENT_ITEM_FAILED lane=%s event_id=%s group_id=%s "
                            "event=%s error=%s",
                            lane_name,
                            int(event.get("id") or 0),
                            int(event.get("trade_group_id") or 0),
                            str(event.get("event_key") or event.get("event_type") or "unknown"),
                            f"{type(result).__name__}: {result}",
                            exc_info=(type(result), result, result.__traceback__),
                        )
                batch_span.set("failed", failed)
                batch_span.set("result", len(events) - failed)
                mark_cycle_completed(
                    "event_admin_watch" if admin_lane else "event_verifier"
                )
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError as exc:
            record_monitor_error(f"event_verifier.{lane_name}.loop", exc)
            log.warning(
                "EVENT_VERIFIER_DB_TIMEOUT lane=%s timeout_sec=%.1f",
                lane_name,
                _EVENT_DB_TIMEOUT_SEC,
            )
            await asyncio.sleep(2.0 if admin_lane else 1.0)
        except Exception as exc:
            record_monitor_error(f"event_verifier.{lane_name}.loop", exc)
            log.exception("market event verifier loop failed lane=%s", lane_name)
            await asyncio.sleep(2.0 if admin_lane else 1.0)


async def market_event_verifier_loop(notify: NotifyFn | None = None) -> None:
    """Consume the latency-sensitive STOP/TP/ENTRY lane."""

    await _market_event_verifier_lane_loop(notify, admin_lane=False)


async def market_event_admin_watch_loop(notify: NotifyFn | None = None) -> None:
    """Consume quarantined watches without competing with fresh safety events."""

    await _market_event_verifier_lane_loop(notify, admin_lane=True)


def _log_manual_fast_classification(
    row: dict[str, Any], classification: dict[str, Any]
) -> None:
    execution_id = int(row.get("id") or 0)
    if execution_id <= 0:
        return
    reason = str(classification.get("reason") or "unknown")
    diagnostic_signature = reason
    if reason == "unknown_stop_or_be_protection":
        protection_sources = list(classification.get("protection_sources") or [])
        diagnostic_signature = "|".join(
            (
                reason,
                str(classification.get("protection_source") or "unknown"),
                ",".join(str(item) for item in protection_sources),
                str(classification.get("be_error_fingerprint") or "none"),
                str(bool(classification.get("stop_manual_required"))),
                str(int(classification.get("active_stop_count") or 0)),
                str(bool(classification.get("active_stop_ids_truncated"))),
                ",".join(
                    str(item)
                    for item in classification.get("active_stop_ids") or []
                ),
            )
        )
    now_mono = time.monotonic()
    previous = _MANUAL_FAST_LOG_STATE.get(execution_id)
    if (
        previous
        and previous[0] == diagnostic_signature
        and now_mono - previous[1] < _MANUAL_FAST_LOG_INTERVAL_SEC
    ):
        _MANUAL_FAST_LOG_STATE.move_to_end(execution_id)
        return
    _MANUAL_FAST_LOG_STATE[execution_id] = (diagnostic_signature, now_mono)
    _MANUAL_FAST_LOG_STATE.move_to_end(execution_id)
    while len(_MANUAL_FAST_LOG_STATE) > _MANUAL_FAST_LOG_STATE_MAX:
        _MANUAL_FAST_LOG_STATE.popitem(last=False)
    cleanup = (
        classification.get("cleanup")
        if isinstance(classification.get("cleanup"), dict)
        else {}
    )
    log.info(
        "MANUAL_FAST_CLASSIFICATION execution_id=%s user_id=%s symbol=%s side=%s reason=%s zero_confirmations=%s zero_confirmed=%s live_field=%s live_qty=%s cleanup_verified=%s cleanup_identity_missing=%s cleanup_tracked_algo=%s cleanup_tracked_regular=%s cleanup_unidentified_algo=%s cleanup_unidentified_regular=%s cleanup_errors=%s replacement_stop_id=%s verify_stop_id=%s cleanup_retry_state=%s cleanup_next_attempt_at=%s",
        execution_id,
        row.get("user_id"),
        str(row.get("symbol") or "").upper(),
        str(row.get("side") or "").upper(),
        reason,
        int(classification.get("zero_confirmations") or 0),
        bool(classification.get("zero_proof_confirmed")),
        classification.get("live_field"),
        classification.get("live_qty"),
        cleanup.get("verified_clean"),
        cleanup.get("identity_missing"),
        len(cleanup.get("remaining_tracked_algo_ids") or []),
        len(cleanup.get("remaining_tracked_regular_ids") or []),
        int(cleanup.get("unidentified_relevant_algo_count") or 0),
        int(cleanup.get("unidentified_relevant_regular_count") or 0),
        len(cleanup.get("errors") or []),
        classification.get("replacement_stop_id"),
        classification.get("verify_matching_stop_order_id"),
        classification.get("cleanup_retry_state"),
        classification.get("cleanup_next_attempt_at"),
    )
    if reason == "unknown_stop_or_be_protection":
        try:
            current_reason_hash = db.critical_reason_fingerprint(row)[:16]
        except Exception:
            current_reason_hash = None
        saved_reason_hash = (
            str(row.get("critical_reason_hash") or "").strip()[:16] or None
        )
        protection_sources = list(classification.get("protection_sources") or [])
        active_stop_ids = list(classification.get("active_stop_ids") or [])
        log.info(
            "MANUAL_PROTECTION_DIAGNOSTIC execution_id=%s user_id=%s symbol=%s side=%s source=%s sources=%s be_manual_required=%s be_error_present=%s be_error_code=%s be_error_fingerprint=%s stop_manual_required=%s active_stop_count=%s active_stop_ids=%s active_stop_ids_truncated=%s reason_text_unknown_stop=%s current_reason_hash=%s saved_reason_hash=%s",
            execution_id,
            row.get("user_id"),
            str(row.get("symbol") or "").upper(),
            str(row.get("side") or "").upper(),
            classification.get("protection_source"),
            json.dumps(protection_sources, ensure_ascii=True, separators=(",", ":")),
            bool(classification.get("be_manual_required")),
            bool(classification.get("be_error_present")),
            classification.get("be_error_code"),
            classification.get("be_error_fingerprint"),
            bool(classification.get("stop_manual_required")),
            int(classification.get("active_stop_count") or 0),
            json.dumps(active_stop_ids, ensure_ascii=True, separators=(",", ":")),
            bool(classification.get("active_stop_ids_truncated")),
            bool(classification.get("reason_text_unknown_stop")),
            current_reason_hash,
            saved_reason_hash,
        )


def _snapshot_timestamp(value: Any) -> str | None:
    """Return a JSON-safe diagnostic timestamp without changing business state."""

    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        normalized = value
        if normalized.tzinfo is None:
            normalized = normalized.replace(tzinfo=timezone.utc)
        return normalized.isoformat()
    return str(value)


def _snapshot_json_default(value: Any) -> str | None:
    """Fail-safe encoder for diagnostics-only snapshot values."""

    return _snapshot_timestamp(value)


def _log_manual_fast_snapshot(rows: list[dict[str, Any]]) -> None:
    global _MANUAL_FAST_SNAPSHOT_LAST_MONO
    now_mono = time.monotonic()
    if now_mono - _MANUAL_FAST_SNAPSHOT_LAST_MONO < _MANUAL_FAST_SNAPSHOT_INTERVAL_SEC:
        return

    # Advance the throttle before any diagnostic-only work.  A malformed value
    # must not turn into a tight logging/error loop inside critical reconcile.
    _MANUAL_FAST_SNAPSHOT_LAST_MONO = now_mono
    try:
        entries: list[dict[str, Any]] = []
        backoff_entries: list[dict[str, Any]] = []
        now_utc = datetime.now(timezone.utc)
        for row in rows:
            if str(row.get("status") or "") != "manual_required":
                continue
            classification = db.critical_manual_backoff_classification(row)
            if classification.get("eligible"):
                backoff_entries.append(
                    {
                        "execution_id": _safe_int(row.get("id")),
                        "user_id": _safe_int(row.get("user_id")),
                        "symbol": str(row.get("symbol") or "").upper(),
                        "side": str(row.get("side") or "").upper(),
                        "reason": str(classification.get("reason") or "eligible"),
                        "next_check_at": _snapshot_timestamp(
                            row.get("critical_next_check_at")
                        ),
                        "unchanged_count": _safe_int(
                            row.get("critical_unchanged_count")
                        ),
                        "due": bool(db.critical_backoff_due(row, now=now_utc)),
                    }
                )
                continue
            cleanup = (
                classification.get("cleanup")
                if isinstance(classification.get("cleanup"), dict)
                else {}
            )
            entries.append(
                {
                    "execution_id": _safe_int(row.get("id")),
                    "user_id": _safe_int(row.get("user_id")),
                    "symbol": str(row.get("symbol") or "").upper(),
                    "side": str(row.get("side") or "").upper(),
                    "reason": str(classification.get("reason") or "unknown"),
                    "zero_confirmations": _safe_int(
                        classification.get("zero_confirmations")
                    ),
                    "zero_confirmed": bool(
                        classification.get("zero_proof_confirmed")
                    ),
                    "cleanup_verified": cleanup.get("verified_clean"),
                    "cleanup_identity_missing": cleanup.get("identity_missing"),
                    "tracked_algo_remaining": len(
                        cleanup.get("remaining_tracked_algo_ids") or []
                    ),
                    "tracked_regular_remaining": len(
                        cleanup.get("remaining_tracked_regular_ids") or []
                    ),
                    "unidentified_algo": _safe_int(
                        cleanup.get("unidentified_relevant_algo_count")
                    ),
                    "unidentified_regular": _safe_int(
                        cleanup.get("unidentified_relevant_regular_count")
                    ),
                    "cleanup_errors": len(cleanup.get("errors") or []),
                    "cleanup_retry_state": classification.get("cleanup_retry_state"),
                    "cleanup_next_attempt_at": _snapshot_timestamp(
                        classification.get("cleanup_next_attempt_at")
                    ),
                    "replacement_stop_id": classification.get("replacement_stop_id"),
                    "protection_source": classification.get("protection_source"),
                    "protection_sources": list(
                        classification.get("protection_sources") or []
                    ),
                    "be_manual_required": bool(
                        classification.get("be_manual_required")
                    ),
                    "be_error_present": bool(
                        classification.get("be_error_present")
                    ),
                    "be_error_code": classification.get("be_error_code"),
                    "be_error_fingerprint": classification.get(
                        "be_error_fingerprint"
                    ),
                    "stop_manual_required": bool(
                        classification.get("stop_manual_required")
                    ),
                    "active_stop_count": _safe_int(
                        classification.get("active_stop_count")
                    ),
                    "active_stop_ids": list(
                        classification.get("active_stop_ids") or []
                    ),
                    "active_stop_ids_truncated": bool(
                        classification.get("active_stop_ids_truncated")
                    ),
                    "reason_text_unknown_stop": bool(
                        classification.get("reason_text_unknown_stop")
                    ),
                }
            )
        log.info(
            "MANUAL_FAST_SNAPSHOT rows=%s entries=%s",
            len(entries),
            json.dumps(
                entries,
                ensure_ascii=False,
                separators=(",", ":"),
                default=_snapshot_json_default,
            ),
        )
        log.info(
            "MANUAL_BACKOFF_SNAPSHOT rows=%s entries=%s",
            len(backoff_entries),
            json.dumps(
                backoff_entries,
                ensure_ascii=False,
                separators=(",", ":"),
                default=_snapshot_json_default,
            ),
        )
    except Exception as exc:
        # Snapshot logging is observability only.  It must never abort the
        # safety-critical reconcile pass or delay position protection.
        log.warning(
            "MANUAL_SNAPSHOT_DIAGNOSTICS_FAILED error_type=%s",
            type(exc).__name__,
            exc_info=True,
        )


def _select_critical_rows(
    rows: list[dict[str, Any]], *, now: datetime | None = None
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    """Partition critical candidates into due and safely deferred rows."""

    candidates = [
        row
        for row in rows
        if str(row.get("status") or "")
        in {
            "partial_error",
            "partial_unrecoverable",
            "manual_required",
            "closed_pending_history",
        }
    ]
    now_utc = now or datetime.now(timezone.utc)
    due: list[dict[str, Any]] = []
    metrics: dict[str, int] = {
        "manual_backoff_due": 0,
        "manual_backoff_deferred": 0,
        "manual_backoff_eligible": 0,
        "manual_fast": 0,
        "closed_history_due": 0,
        "closed_history_deferred": 0,
        "manual_cleanup_probe_due": 0,
        "manual_cleanup_probe_deferred": 0,
    }
    for key in set(_MANUAL_FAST_REASON_COUNTERS.values()):
        metrics[key] = 0

    for row in candidates:
        status = str(row.get("status") or "")
        if status == "closed_pending_history":
            if db.closed_history_reconcile_due(row, now=now_utc):
                metrics["closed_history_due"] += 1
                due.append(row)
            else:
                metrics["closed_history_deferred"] += 1
            continue
        if status != "manual_required":
            due.append(row)
            continue

        classification = db.critical_manual_backoff_classification(row)
        if not bool(classification.get("eligible")):
            metrics["manual_fast"] += 1
            reason = str(classification.get("reason") or "")
            counter = _MANUAL_FAST_REASON_COUNTERS.get(reason)
            if counter:
                metrics[counter] += 1
            _log_manual_fast_classification(row, classification)
            if reason in {"cleanup_unresolved", "be_replacement_in_progress"}:
                if db.critical_cleanup_reconcile_due(row, now=now_utc):
                    metrics["manual_cleanup_probe_due"] += 1
                    due.append(row)
                else:
                    metrics["manual_cleanup_probe_deferred"] += 1
                continue
            due.append(row)
            continue

        metrics["manual_backoff_eligible"] += 1
        if db.critical_backoff_due(row, now=now_utc):
            metrics["manual_backoff_due"] += 1
            due.append(row)
        else:
            metrics["manual_backoff_deferred"] += 1
    return candidates, due, metrics


async def critical_account_reconcile_loop(notify: NotifyFn | None = None) -> None:
    """Keep dangerous/incomplete states on the five-second safety cadence."""

    settings = get_settings()
    interval = float(settings.MONITOR_CRITICAL_INTERVAL_SEC)
    page_limit = 1000
    while True:
        started = time.monotonic()
        critical: list[dict[str, Any]] = []
        timeout_retry = False
        phase = "critical_select"
        with diagnostic_span(
            "cycle.critical_reconcile", emit=True
        ) as cycle_span:
            try:
                global _CRITICAL_SCAN_CURSOR
                select_started = time.monotonic()
                rows = await _bounded_db_call(
                    "critical_position_executions",
                    db.critical_position_executions(
                        limit=page_limit, after_id=_CRITICAL_SCAN_CURSOR
                    ),
                    timeout_sec=_CRITICAL_DB_TIMEOUT_SEC,
                )
                if not rows and _CRITICAL_SCAN_CURSOR:
                    _CRITICAL_SCAN_CURSOR = 0
                    phase = "critical_select_wrap"
                    rows = await _bounded_db_call(
                        "critical_position_executions_wrap",
                        db.critical_position_executions(
                            limit=page_limit, after_id=0
                        ),
                        timeout_sec=_CRITICAL_DB_TIMEOUT_SEC,
                    )
                cycle_span.add_ms(
                    "select_filter_ms", (time.monotonic() - select_started) * 1000
                )
                if rows and len(rows) >= page_limit:
                    _CRITICAL_SCAN_CURSOR = max(
                        int(row.get("id") or 0) for row in rows
                    )
                else:
                    # A short page proves the critical-status query reached the
                    # end. Start the next five-second pass at zero directly
                    # instead of issuing an empty cursor query and then a wrap
                    # query on every other cycle.
                    _CRITICAL_SCAN_CURSOR = 0

                candidates, critical, backoff_partition = _select_critical_rows(
                    rows, now=datetime.now(timezone.utc)
                )
                _log_manual_fast_snapshot(candidates)
                manual_backoff_deferred = int(
                    backoff_partition.get("manual_backoff_deferred") or 0
                )
                closed_history_deferred = int(
                    backoff_partition.get("closed_history_deferred") or 0
                )
                manual_cleanup_probe_deferred = int(
                    backoff_partition.get("manual_cleanup_probe_deferred") or 0
                )
                cycle_span.set("rows_scanned", len(rows))
                cycle_span.set("rows_selected", len(candidates))
                cycle_span.set("rows_due", len(critical))
                cycle_span.set(
                    "rows_skipped",
                    manual_backoff_deferred
                    + closed_history_deferred
                    + manual_cleanup_probe_deferred,
                )
                for key, value in backoff_partition.items():
                    cycle_span.set(key, int(value or 0))
                cycle_span.set("rows_source", "critical_status_query")
                result = 0
                if critical:
                    partial = [
                        row
                        for row in critical
                        if str(row.get("status") or "")
                        in {"partial_error", "partial_unrecoverable"}
                    ]
                    cycle_span.set("partial_rows", len(partial))
                    if partial:
                        phase = "partial_tp_recovery"
                        with diagnostic_span(
                            "critical.partial_tp_recovery",
                            emit=True,
                            metadata={
                                "rows_selected": len(partial),
                                "rows_source": "critical_override",
                            },
                        ) as partial_span:
                            partial_result = await process_partial_tp_recovery_once(
                                notify=notify, rows_override=partial
                            )
                            partial_span.set("result", int(partial_result))
                            result += int(partial_result)

                    phase = "position_lifecycle_guard"
                    with diagnostic_span(
                        "critical.position_lifecycle_guard",
                        emit=True,
                        metadata={
                            "rows_selected": len(critical),
                            "rows_source": "critical_override",
                        },
                    ) as lifecycle_span:
                        lifecycle_result = await process_position_lifecycle_guard_once(
                            notify=notify, rows_override=critical
                        )
                        lifecycle_span.set("result", int(lifecycle_result))
                        result += int(lifecycle_result)

                    manual_due_ids = [
                        int(row.get("id") or 0)
                        for row in critical
                        if str(row.get("status") or "") == "manual_required"
                        and int(row.get("id") or 0) > 0
                    ]
                    if manual_due_ids:
                        phase = "schedule_manual_required_backoff"
                        backoff_stats = await _bounded_db_call(
                            "schedule_manual_required_backoff",
                            db.schedule_manual_required_backoff(manual_due_ids),
                            timeout_sec=_CRITICAL_DB_TIMEOUT_SEC,
                        )
                        cycle_span.set(
                            "manual_backoff_scheduled",
                            int(backoff_stats.get("scheduled") or 0),
                        )
                        cycle_span.set(
                            "manual_backoff_changed",
                            int(backoff_stats.get("changed") or 0),
                        )
                        cycle_span.set(
                            "manual_backoff_unchanged",
                            int(backoff_stats.get("unchanged") or 0),
                        )
                        cycle_span.set(
                            "manual_backoff_conflicts",
                            int(backoff_stats.get("conflicts") or 0),
                        )
                        cycle_span.set(
                            "manual_backoff_max_delay_sec",
                            int(backoff_stats.get("max_delay_sec") or 0),
                        )
                cycle_span.set("result", result)
                mark_cycle_completed("critical_reconcile")
                duration = time.monotonic() - started
                if duration >= interval:
                    log.warning(
                        "slow critical reconcile rows=%s duration_ms=%s",
                        len(critical),
                        int(duration * 1000),
                    )
            except asyncio.CancelledError:
                raise
            except MonitorDBOperationTimeout as exc:
                timeout_retry = True
                cycle_span.errors += 1
                cycle_span.set("timeout_phase", phase)
                cycle_span.set("timeout_operation", exc.operation)
                record_monitor_error("critical_reconcile.db", exc)
                log.warning(
                    "CRITICAL_RECONCILE_DB_TIMEOUT phase=%s operation=%s "
                    "timeout_sec=%.1f cursor=%s rows_due=%s",
                    phase,
                    exc.operation,
                    exc.timeout_sec,
                    _CRITICAL_SCAN_CURSOR,
                    len(critical),
                )
            except asyncio.TimeoutError as exc:
                # Do not mislabel an exchange/network timeout from lifecycle or
                # partial recovery as a PostgreSQL failure.
                timeout_retry = True
                cycle_span.errors += 1
                cycle_span.set("timeout_phase", phase)
                record_monitor_error("critical_reconcile.operation", exc)
                log.warning(
                    "CRITICAL_RECONCILE_OPERATION_TIMEOUT phase=%s rows_due=%s",
                    phase,
                    len(critical),
                )
            except Exception as exc:
                cycle_span.errors += 1
                cycle_span.set("failure_phase", phase)
                record_monitor_error("critical_reconcile.loop", exc)
                log.exception("critical account reconcile failed phase=%s", phase)
        elapsed = time.monotonic() - started
        if timeout_retry:
            # The failed operation already consumed up to ten seconds. A short
            # one-second pause prevents immediate timeout stampedes while still
            # retrying much faster than the low-priority full fallback.
            await asyncio.sleep(1.0)
        else:
            await asyncio.sleep(max(0.1, interval - elapsed))
