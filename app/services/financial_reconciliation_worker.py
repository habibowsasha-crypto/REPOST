"""Low-priority durable worker for exact BingX trading-fee reconciliation.

The worker is deliberately isolated from ENTRY, STOP, TP, BE and lifecycle
hot paths.  It consumes only already-durable terminal-close jobs, performs
read-only ``fillHistory`` calls under ``PRIORITY_FINANCIAL``, and persists
exact fills through the idempotent financial reconciliation tables.

Jobs are received only from durable terminal-close markers. The worker also
recovers the narrow crash window between terminal execution persistence and the
separate queue insert, then dispatches the durable financial summary after a terminal result.  Step
g5b3g15 also lets this same worker project exact fills/fees and optional funding
into statistics-v2; no second worker is created. Runtime startup remains disabled by default through
``FINANCIAL_RECONCILIATION_ENABLED=false`` until controlled live validation.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from app.config import get_settings
from app.database import db
from app.exchanges.bingx.adapter import (
    BingxApiError,
    BingxExchangeRejected,
    BingxResponseIntegrityError,
)
from app.services.durable_notifications import NotifyFn
from app.services.exchange_factory import build_adapter
from app.services.financial_reconciliation_bingx import (
    BingxFinancialFillBindingError,
    fetch_and_bind_bingx_order_fills,
)
from app.services.financial_reconciliation_enqueue import (
    recover_pending_financial_reconciliation_enqueues,
)
from app.services.financial_worker_coordination import (
    is_primary_financial_worker,
)
from app.services.financial_reconciliation_backfill import (
    recover_terminal_financial_backfill_once,
)
from app.services.manual_execution_history_bridge import (
    recover_exact_manual_history_bridge_once,
)
from app.services.execution_duplicate_bridge import (
    recover_exact_duplicate_execution_once,
)
from app.services.funding_g60_recovery_rearm import (
    recover_g60_funding_rearm_once,
)
from app.services.funding_g62_superseded_overlap_rearm import (
    recover_g62_superseded_overlap_once,
)
from app.services.financial_reconciliation_models import (
    FINANCIAL_STATUS_AMBIGUOUS,
    FINANCIAL_STATUS_CONFIRMED,
    FINANCIAL_STATUS_PARTIAL,
    FINANCIAL_STATUS_PROCESSING,
    FINANCIAL_STATUS_UNAVAILABLE,
    FinancialOrderExpectation,
)
from app.services.financial_reconciliation_notifications import (
    process_due_financial_notifications_once,
)
from app.services.ttl_cache import get_api_key_cache
from app.services.statistics_financial_projection import (
    process_statistics_financial_projection_once,
)
from app.services.statistics_quality_gate import refresh_statistics_quality_gates

log = logging.getLogger(__name__)

# One exact-order query per second by default is below BingX's documented IP
# limit and is intentionally slower than every trading lane.
_DEFAULT_IDLE_SLEEP_SEC = 1.0
_QUERY_SAFETY_MARGIN_SEC = 300.0

_TASKS: list[asyncio.Task[None]] = []
_STOP_EVENT: asyncio.Event | None = None
_LIFECYCLE_LOCK: asyncio.Lock | None = None


@dataclass(frozen=True)
class FinancialWorkerOutcome:
    job_id: int
    action: str
    status: str
    attempts: int
    fill_count: int
    confirmed_order_count: int
    error: str = ""


class _RequestRateLimiter:
    """Cancellation-safe process-local serial rate limiter."""

    def __init__(self, requests_per_second: float) -> None:
        rate = float(requests_per_second)
        if not math.isfinite(rate) or rate <= 0:
            raise ValueError("financial reconciliation RPS must be positive")
        self._interval = 1.0 / rate
        self._next_allowed = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            delay = max(0.0, self._next_allowed - now)
            if delay > 0:
                await asyncio.sleep(delay)
            current = time.monotonic()
            self._next_allowed = max(current, self._next_allowed) + self._interval


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        if " " in text and "T" not in text:
            text = text.replace(" ", "T", 1)
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _metadata_mapping(order: Mapping[str, Any]) -> dict[str, Any]:
    raw = order.get("metadata_json")
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _millis_from_metadata(metadata: Mapping[str, Any]) -> int | None:
    for key in (
        "query_start_time_ms",
        "start_time_ms",
        "order_created_time_ms",
        "created_time_ms",
        "execution_started_time_ms",
        "opened_at_ms",
    ):
        value = metadata.get(key)
        if value in (None, "") or isinstance(value, bool):
            continue
        try:
            millis = int(str(value).strip())
        except (TypeError, ValueError, OverflowError):
            continue
        if millis > 0:
            return millis
    for key in (
        "query_start_at",
        "order_created_at",
        "created_at",
        "execution_started_at",
        "opened_at",
    ):
        parsed = _as_utc_datetime(metadata.get(key))
        if parsed is not None:
            return int(parsed.timestamp() * 1000)
    return None


def _query_window_ms(
    *,
    job: Mapping[str, Any],
    order: Mapping[str, Any],
    now: datetime,
    lookback_sec: float,
) -> tuple[int, int]:
    terminal = _as_utc_datetime(job.get("terminal_at")) or now
    bounded_lookback = max(3600.0, min(float(lookback_sec), 180.0 * 86400.0))
    fallback_start = terminal - timedelta(seconds=bounded_lookback)
    metadata_start_ms = _millis_from_metadata(_metadata_mapping(order))
    if metadata_start_ms is not None:
        metadata_start = datetime.fromtimestamp(
            metadata_start_ms / 1000.0, tz=timezone.utc
        ) - timedelta(seconds=_QUERY_SAFETY_MARGIN_SEC)
        # A corrupt ancient timestamp must not turn one accounting task into an
        # unbounded account-history scan.
        start = max(metadata_start, fallback_start)
    else:
        start = fallback_start
    end = now
    if terminal > end:
        raise RuntimeError("financial terminal_at is in the future")
    start_ms = max(1, int(start.timestamp() * 1000))
    end_ms = int(end.timestamp() * 1000)
    if end_ms <= start_ms:
        raise RuntimeError("financial fill query window is not yet available")
    return start_ms, end_ms


def _deadline_expired(job: Mapping[str, Any], now: datetime) -> bool:
    deadline = _as_utc_datetime(job.get("deadline_at"))
    return bool(deadline is not None and now >= deadline)


def _terminal_status_for_existing_fills(job: Mapping[str, Any]) -> str:
    return (
        FINANCIAL_STATUS_PARTIAL
        if int(job.get("fill_count") or 0) > 0
        else FINANCIAL_STATUS_UNAVAILABLE
    )


def _retry_delay_seconds(attempts: int) -> float:
    # The first retries cover normal exchange-history propagation; later ones
    # back off without exceeding the durable job deadline.
    schedule = (1.0, 3.0, 10.0, 30.0, 60.0, 120.0, 300.0)
    index = max(0, min(int(attempts or 1) - 1, len(schedule) - 1))
    return schedule[index]


def _safe_error(exc: BaseException | str, *, limit: int = 800) -> str:
    text = str(exc or "").replace("\x00", "").replace("\r", " ").replace("\n", " ")
    return " ".join(text.split())[:limit]


async def _load_bingx_adapter(user_id: int):
    api_row = await get_api_key_cache().get_or_fetch(
        (int(user_id), "api", "bingx", "financial"),
        lambda: db.get_api_key(int(user_id), "bingx", include_quarantine=True),
    )
    if not api_row:
        raise RuntimeError("enabled BingX API credentials are unavailable")
    if api_row.get("api_quarantined") is True:
        raise RuntimeError("BingX API credentials are permission-quarantined")
    return build_adapter(api_row)


async def _finalize_limit(
    job: Mapping[str, Any],
    *,
    lease_token: str,
    reason: str,
) -> FinancialWorkerOutcome:
    status = _terminal_status_for_existing_fills(job)
    updated = await db.finalize_financial_reconciliation_job(
        job_id=int(job["id"]),
        lease_token=lease_token,
        status=status,
        error=reason,
    )
    return FinancialWorkerOutcome(
        job_id=int(updated["id"]),
        action="finalized_limit",
        status=str(updated["status"]),
        attempts=int(updated.get("attempts") or 0),
        fill_count=int(updated.get("fill_count") or 0),
        confirmed_order_count=int(updated.get("confirmed_order_count") or 0),
        error=reason,
    )


async def _reschedule_or_finalize(
    job: Mapping[str, Any],
    *,
    lease_token: str,
    reason: str,
    now: datetime,
) -> FinancialWorkerOutcome:
    settings = get_settings()
    attempts = int(job.get("attempts") or 0)
    if attempts >= int(settings.FINANCIAL_RECONCILIATION_MAX_ATTEMPTS):
        return await _finalize_limit(
            job,
            lease_token=lease_token,
            reason=f"max_attempts_reached:{reason}",
        )
    if _deadline_expired(job, now):
        return await _finalize_limit(
            job,
            lease_token=lease_token,
            reason=f"deadline_reached:{reason}",
        )
    delay = _retry_delay_seconds(attempts)
    deadline = _as_utc_datetime(job.get("deadline_at"))
    if deadline is not None:
        remaining = max(1.0, (deadline - now).total_seconds())
        delay = min(delay, remaining)
    changed = await db.reschedule_financial_reconciliation_job(
        job_id=int(job["id"]),
        lease_token=lease_token,
        retry_after_sec=delay,
        error=reason,
    )
    if not changed:
        raise RuntimeError("financial reconciliation lease changed during reschedule")
    return FinancialWorkerOutcome(
        job_id=int(job["id"]),
        action="rescheduled",
        status="pending",
        attempts=attempts,
        fill_count=int(job.get("fill_count") or 0),
        confirmed_order_count=int(job.get("confirmed_order_count") or 0),
        error=reason,
    )


async def process_financial_reconciliation_job(
    job: Mapping[str, Any],
    *,
    adapter_loader: Callable[[int], Awaitable[Any]] = _load_bingx_adapter,
    rate_limiter: _RequestRateLimiter | None = None,
    now: datetime | None = None,
) -> FinancialWorkerOutcome:
    """Process one already-leased durable job with no lifecycle side effects."""

    current = now or _utc_now()
    job_id = int(job.get("id") or 0)
    lease_token = str(job.get("lease_token") or "").strip()
    if job_id <= 0 or not lease_token:
        raise ValueError("financial worker requires a leased durable job")
    if str(job.get("status") or "").lower() != FINANCIAL_STATUS_PROCESSING:
        raise ValueError("financial worker accepts only processing jobs")

    settings = get_settings()
    if _deadline_expired(job, current):
        return await _finalize_limit(
            job, lease_token=lease_token, reason="deadline_reached_before_query"
        )
    if int(job.get("attempts") or 0) > int(
        settings.FINANCIAL_RECONCILIATION_MAX_ATTEMPTS
    ):
        return await _finalize_limit(
            job, lease_token=lease_token, reason="max_attempts_exceeded_before_query"
        )
    if str(job.get("exchange") or "").lower() != "bingx":
        updated = await db.finalize_financial_reconciliation_job(
            job_id=job_id,
            lease_token=lease_token,
            status=FINANCIAL_STATUS_AMBIGUOUS,
            error="unsupported_exchange_for_financial_worker",
        )
        return FinancialWorkerOutcome(
            job_id=job_id,
            action="finalized_ambiguous",
            status=str(updated["status"]),
            attempts=int(updated.get("attempts") or 0),
            fill_count=int(updated.get("fill_count") or 0),
            confirmed_order_count=int(updated.get("confirmed_order_count") or 0),
            error="unsupported_exchange_for_financial_worker",
        )

    orders = await db.list_financial_reconciliation_orders(job_id)
    required_orders = [row for row in orders if bool(row.get("required", True))]
    if not required_orders:
        updated = await db.finalize_financial_reconciliation_job(
            job_id=job_id,
            lease_token=lease_token,
            status=FINANCIAL_STATUS_AMBIGUOUS,
            error="financial_job_has_no_required_orders",
        )
        return FinancialWorkerOutcome(
            job_id=job_id,
            action="finalized_ambiguous",
            status=str(updated["status"]),
            attempts=int(updated.get("attempts") or 0),
            fill_count=int(updated.get("fill_count") or 0),
            confirmed_order_count=int(updated.get("confirmed_order_count") or 0),
            error="financial_job_has_no_required_orders",
        )

    pending_orders = [
        row for row in required_orders if str(row.get("status") or "") != "confirmed"
    ]
    if not pending_orders:
        updated = await db.finalize_financial_reconciliation_job(
            job_id=job_id,
            lease_token=lease_token,
            status=FINANCIAL_STATUS_CONFIRMED,
        )
        return FinancialWorkerOutcome(
            job_id=job_id,
            action="finalized_confirmed",
            status=str(updated["status"]),
            attempts=int(updated.get("attempts") or 0),
            fill_count=int(updated.get("fill_count") or 0),
            confirmed_order_count=int(updated.get("confirmed_order_count") or 0),
        )

    unresolved_identity = [
        row for row in pending_orders if not str(row.get("exchange_order_id") or "").strip()
    ]
    if unresolved_identity:
        return await _reschedule_or_finalize(
            job,
            lease_token=lease_token,
            reason="exact_exchange_order_id_pending",
            now=current,
        )

    # Exact quantity is required to prove that delayed partial fills are complete.
    missing_qty = [row for row in pending_orders if row.get("expected_qty") in (None, "")]
    if missing_qty:
        return await _reschedule_or_finalize(
            job,
            lease_token=lease_token,
            reason="expected_order_quantity_missing",
            now=current,
        )

    limiter = rate_limiter or _RequestRateLimiter(
        settings.FINANCIAL_RECONCILIATION_REQUESTS_PER_SECOND
    )
    latest_job: Mapping[str, Any] = job
    try:
        adapter = await adapter_loader(int(job["user_id"]))
        for order in pending_orders:
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
            start_ms, end_ms = _query_window_ms(
                job=job,
                order=order,
                now=current,
                lookback_sec=float(settings.FINANCIAL_RECONCILIATION_LOOKBACK_SEC),
            )
            await limiter.wait()
            fills = await fetch_and_bind_bingx_order_fills(
                adapter,
                expectation=expectation,
                symbol=str(job["symbol"]),
                execution_side=str(job["side"]),
                start_time_ms=start_ms,
                end_time_ms=end_ms,
            )
            # Persist each exact order immediately. A later order may be
            # temporarily unavailable, and a Railway restart may interrupt the
            # remaining loop; already proved fills must survive both cases so
            # the terminal result can be classified as partial instead of
            # incorrectly unavailable.
            latest_job = await db.upsert_financial_reconciliation_fills(
                job_id=job_id,
                lease_token=lease_token,
                fills=fills,
            )
            if str(latest_job.get("status") or "") == FINANCIAL_STATUS_AMBIGUOUS:
                return FinancialWorkerOutcome(
                    job_id=job_id,
                    action="stored_ambiguous",
                    status=FINANCIAL_STATUS_AMBIGUOUS,
                    attempts=int(latest_job.get("attempts") or 0),
                    fill_count=int(latest_job.get("fill_count") or 0),
                    confirmed_order_count=int(
                        latest_job.get("confirmed_order_count") or 0
                    ),
                    error=_safe_error(
                        latest_job.get("last_error") or "fill ownership conflict"
                    ),
                )
    except (BingxResponseIntegrityError, BingxFinancialFillBindingError, ValueError) as exc:
        reason = f"fill_integrity_ambiguous:{_safe_error(exc)}"
        updated = await db.finalize_financial_reconciliation_job(
            job_id=job_id,
            lease_token=lease_token,
            status=FINANCIAL_STATUS_AMBIGUOUS,
            error=reason,
        )
        return FinancialWorkerOutcome(
            job_id=job_id,
            action="finalized_ambiguous",
            status=str(updated["status"]),
            attempts=int(updated.get("attempts") or 0),
            fill_count=int(updated.get("fill_count") or 0),
            confirmed_order_count=int(updated.get("confirmed_order_count") or 0),
            error=reason,
        )
    except (BingxExchangeRejected, BingxApiError, RuntimeError) as exc:
        return await _reschedule_or_finalize(
            latest_job,
            lease_token=lease_token,
            reason=f"exchange_read_retry:{_safe_error(exc)}",
            now=current,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.exception(
            "FINANCIAL_RECONCILIATION_UNEXPECTED job_id=%s execution_id=%s",
            job_id,
            int(job.get("execution_id") or 0),
        )
        return await _reschedule_or_finalize(
            latest_job,
            lease_token=lease_token,
            reason=f"unexpected_worker_error:{type(exc).__name__}:{_safe_error(exc)}",
            now=current,
        )

    updated = latest_job
    status = str(updated.get("status") or "")
    if status == FINANCIAL_STATUS_AMBIGUOUS:
        return FinancialWorkerOutcome(
            job_id=job_id,
            action="stored_ambiguous",
            status=status,
            attempts=int(updated.get("attempts") or 0),
            fill_count=int(updated.get("fill_count") or 0),
            confirmed_order_count=int(updated.get("confirmed_order_count") or 0),
            error=_safe_error(updated.get("last_error") or "fill ownership conflict"),
        )

    expected = int(updated.get("expected_order_count") or 0)
    confirmed = int(updated.get("confirmed_order_count") or 0)
    if expected > 0 and confirmed == expected:
        finalized = await db.finalize_financial_reconciliation_job(
            job_id=job_id,
            lease_token=lease_token,
            status=FINANCIAL_STATUS_CONFIRMED,
        )
        return FinancialWorkerOutcome(
            job_id=job_id,
            action="finalized_confirmed",
            status=str(finalized["status"]),
            attempts=int(finalized.get("attempts") or 0),
            fill_count=int(finalized.get("fill_count") or 0),
            confirmed_order_count=int(finalized.get("confirmed_order_count") or 0),
        )

    return await _reschedule_or_finalize(
        updated,
        lease_token=lease_token,
        reason="fill_history_incomplete",
        now=current,
    )


async def process_financial_reconciliation_once(
    *,
    adapter_loader: Callable[[int], Awaitable[Any]] = _load_bingx_adapter,
    rate_limiter: _RequestRateLimiter | None = None,
    now: datetime | None = None,
) -> FinancialWorkerOutcome | None:
    """Claim and process at most one due job; useful for tests and diagnostics."""

    settings = get_settings()
    claimed = await db.claim_due_financial_reconciliation_jobs(
        limit=1,
        stale_after_sec=float(
            settings.FINANCIAL_RECONCILIATION_STALE_PROCESSING_SEC
        ),
    )
    if not claimed:
        return None
    return await process_financial_reconciliation_job(
        claimed[0],
        adapter_loader=adapter_loader,
        rate_limiter=rate_limiter,
        now=now,
    )


async def _wait_for_stop(event: asyncio.Event, timeout: float) -> bool:
    try:
        await asyncio.wait_for(event.wait(), timeout=max(0.05, float(timeout)))
        return True
    except asyncio.TimeoutError:
        return False


async def _worker_loop(
    worker_index: int,
    stop_event: asyncio.Event,
    notify: NotifyFn | None,
) -> None:
    settings = get_settings()
    limiter = _RequestRateLimiter(
        settings.FINANCIAL_RECONCILIATION_REQUESTS_PER_SECOND
    )
    next_enqueue_recovery = 0.0
    next_terminal_backfill = 0.0
    next_manual_history_bridge = 0.0
    next_g59_duplicate_bridge = 0.0
    next_g60_funding_rearm = 0.0
    next_g62_superseded_overlap_rearm = 0.0
    next_quality_backlog_refresh = 0.0
    is_primary_worker = is_primary_financial_worker(worker_index)
    if is_primary_worker:
        log.info(
            "G56_FINANCIAL_PRIMARY_WORKER_ACTIVE worker=%s coordinator=1",
            worker_index,
        )
    while not stop_event.is_set():
        try:
            now_monotonic = time.monotonic()
            if now_monotonic >= next_enqueue_recovery:
                recovered = await recover_pending_financial_reconciliation_enqueues(
                    limit=20
                )
                if recovered:
                    log.info(
                        "FINANCIAL_RECONCILIATION_ENQUEUE_RECOVERED worker=%s count=%s",
                        worker_index,
                        recovered,
                    )
                next_enqueue_recovery = now_monotonic + 30.0
            if is_primary_worker and now_monotonic >= next_manual_history_bridge:
                try:
                    bridge = await recover_exact_manual_history_bridge_once()
                    if any(int(value or 0) for value in bridge.values()):
                        log.info(
                            "G55_EXACT_MANUAL_HISTORY_BRIDGE_SUMMARY worker=%s counters=%s",
                            worker_index,
                            bridge,
                        )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("G55_EXACT_MANUAL_HISTORY_BRIDGE_LOOP_FAILED_FAIL_OPEN")
                finally:
                    next_manual_history_bridge = now_monotonic + 30.0
            if is_primary_worker and now_monotonic >= next_g59_duplicate_bridge:
                try:
                    duplicate_bridge = await recover_exact_duplicate_execution_once()
                    if any(int(value or 0) for value in duplicate_bridge.values()):
                        log.info(
                            "G59_DUPLICATE_EXECUTION_BRIDGE_SUMMARY worker=%s counters=%s",
                            worker_index,
                            duplicate_bridge,
                        )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("G59_DUPLICATE_EXECUTION_BRIDGE_LOOP_FAILED_FAIL_OPEN")
                finally:
                    next_g59_duplicate_bridge = now_monotonic + 30.0
            if is_primary_worker and now_monotonic >= next_g60_funding_rearm:
                try:
                    funding_rearm = await recover_g60_funding_rearm_once(limit=20)
                    if any(int(value or 0) for value in funding_rearm.values()):
                        log.info(
                            "G60_FUNDING_GENERIC_REARM_SUMMARY worker=%s counters=%s",
                            worker_index,
                            funding_rearm,
                        )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("G60_FUNDING_GENERIC_REARM_LOOP_FAILED_FAIL_OPEN")
                finally:
                    next_g60_funding_rearm = now_monotonic + 30.0
            if is_primary_worker and now_monotonic >= next_g62_superseded_overlap_rearm:
                try:
                    overlap_rearm = await recover_g62_superseded_overlap_once(limit=20)
                    if any(int(value or 0) for value in overlap_rearm.values()):
                        log.info(
                            "G62_SUPERSEDED_OVERLAP_REARM_SUMMARY worker=%s counters=%s",
                            worker_index,
                            overlap_rearm,
                        )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("G62_SUPERSEDED_OVERLAP_REARM_LOOP_FAILED_FAIL_OPEN")
                finally:
                    next_g62_superseded_overlap_rearm = now_monotonic + 30.0
            if is_primary_worker and now_monotonic >= next_terminal_backfill:
                try:
                    backfill = await recover_terminal_financial_backfill_once(limit=20)
                    if any(int(value or 0) for value in backfill.values()):
                        log.info(
                            "G54_TERMINAL_FINANCIAL_BACKFILL_SUMMARY worker=%s counters=%s",
                            worker_index,
                            backfill,
                        )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("G54_TERMINAL_FINANCIAL_BACKFILL_LOOP_FAILED_FAIL_OPEN")
                finally:
                    next_terminal_backfill = now_monotonic + 30.0
            if (
                is_primary_worker
                and bool(get_settings().STATISTICS_QUALITY_ENABLED)
                and now_monotonic >= next_quality_backlog_refresh
            ):
                try:
                    refreshed = await refresh_statistics_quality_gates(
                        include_backlog=True,
                        limit=100,
                    )
                    if (
                        refreshed.executions_refreshed
                        or refreshed.signals_refreshed
                        or refreshed.execution_cas_conflicts
                        or refreshed.signal_cas_conflicts
                    ):
                        log.info(
                            "STATISTICS_QUALITY_GATE_BACKLOG_REFRESH "
                            "executions=%s signals=%s execution_cas=%s signal_cas=%s",
                            refreshed.executions_refreshed,
                            refreshed.signals_refreshed,
                            refreshed.execution_cas_conflicts,
                            refreshed.signal_cas_conflicts,
                        )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception(
                        "STATISTICS_QUALITY_GATE_BACKLOG_REFRESH_FAILED_FAIL_OPEN"
                    )
                finally:
                    # Metadata-only bounded repair. Keep it outside STOP/TP paths
                    # and avoid a tight loop when the database is unavailable.
                    next_quality_backlog_refresh = now_monotonic + 60.0
            outcome = await process_financial_reconciliation_once(
                rate_limiter=limiter
            )
            projection_outcome = await process_statistics_financial_projection_once(
                adapter_loader=_load_bingx_adapter,
                rate_limiter=limiter,
            )
            if projection_outcome is not None:
                log.info(
                    "STATISTICS_FINANCIAL_RESULT worker=%s execution_id=%s "
                    "action=%s projection=%s financial=%s funding=%s attempts=%s",
                    worker_index,
                    projection_outcome.execution_id,
                    projection_outcome.action,
                    projection_outcome.projection_status,
                    projection_outcome.financial_state,
                    projection_outcome.funding_state,
                    projection_outcome.attempts,
                )
                if bool(get_settings().STATISTICS_QUALITY_ENABLED):
                    try:
                        await refresh_statistics_quality_gates(
                            execution_ids=(projection_outcome.execution_id,),
                            limit=10,
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        log.exception(
                            "STATISTICS_QUALITY_GATE_FINANCIAL_REFRESH_FAILED_FAIL_OPEN "
                            "execution_id=%s",
                            projection_outcome.execution_id,
                        )
            notified = await process_due_financial_notifications_once(
                notify,
                limit=20,
            )
            if notified:
                log.info(
                    "FINANCIAL_RECONCILIATION_NOTIFICATIONS worker=%s delivered=%s",
                    worker_index,
                    notified,
                )
            if outcome is None and projection_outcome is None:
                await _wait_for_stop(stop_event, _DEFAULT_IDLE_SLEEP_SEC)
                continue
            if outcome is None:
                continue
            log.info(
                "FINANCIAL_RECONCILIATION_RESULT worker=%s job_id=%s action=%s status=%s attempts=%s fills=%s confirmed_orders=%s",
                worker_index,
                outcome.job_id,
                outcome.action,
                outcome.status,
                outcome.attempts,
                outcome.fill_count,
                outcome.confirmed_order_count,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            # Claim leases are durable. A process-level failure leaves the job in
            # processing and the existing stale-lease recovery will reclaim it.
            log.exception(
                "FINANCIAL_RECONCILIATION_LOOP_ERROR worker=%s", worker_index
            )
            await _wait_for_stop(stop_event, _DEFAULT_IDLE_SLEEP_SEC)


async def start_financial_reconciliation_dispatcher(
    notify: NotifyFn | None = None,
) -> list[asyncio.Task[None]]:
    """Start the single disabled-by-default durable financial worker."""

    global _STOP_EVENT, _LIFECYCLE_LOCK
    if _LIFECYCLE_LOCK is None:
        _LIFECYCLE_LOCK = asyncio.Lock()
    async with _LIFECYCLE_LOCK:
        settings = get_settings()
        if not settings.FINANCIAL_RECONCILIATION_ENABLED:
            log.info("FINANCIAL_RECONCILIATION_DISABLED")
            return []
        live = [task for task in _TASKS if not task.done()]
        if live:
            return list(live)
        _TASKS.clear()
        _STOP_EVENT = asyncio.Event()
        worker_count = int(settings.FINANCIAL_RECONCILIATION_WORKERS)
        try:
            snapshot = await db.financial_reconciliation_queue_snapshot()
        except Exception:
            # Optional accounting must never prevent trading-bot startup. The
            # worker can still begin and retry its durable DB reads later.
            log.exception("FINANCIAL_RECONCILIATION_START_SNAPSHOT_FAILED_FAIL_OPEN")
            snapshot = {}
        try:
            for index in range(worker_count):
                task = asyncio.create_task(
                    _worker_loop(index + 1, _STOP_EVENT, notify),
                    name=f"financial-reconciliation-{index + 1}",
                )
                _TASKS.append(task)
        except Exception:
            log.exception("FINANCIAL_RECONCILIATION_START_FAILED_FAIL_OPEN")
            for task in _TASKS:
                task.cancel()
            if _TASKS:
                await asyncio.gather(*_TASKS, return_exceptions=True)
            _TASKS.clear()
            _STOP_EVENT = None
            return []
        log.info(
            "FINANCIAL_RECONCILIATION_STARTED workers=%s rps=%.3f pending=%s processing=%s",
            worker_count,
            float(settings.FINANCIAL_RECONCILIATION_REQUESTS_PER_SECOND),
            int(snapshot.get("pending") or 0),
            int(snapshot.get("processing") or 0),
        )
        return list(_TASKS)


async def stop_financial_reconciliation_dispatcher() -> None:
    """Stop accepting work and wait boundedly for the read-only worker."""

    global _STOP_EVENT, _LIFECYCLE_LOCK
    if _LIFECYCLE_LOCK is None:
        _LIFECYCLE_LOCK = asyncio.Lock()
    async with _LIFECYCLE_LOCK:
        tasks = [task for task in _TASKS if not task.done()]
        if not tasks:
            _TASKS.clear()
            _STOP_EVENT = None
            return
        if _STOP_EVENT is not None:
            _STOP_EVENT.set()
        timeout = float(
            get_settings().FINANCIAL_RECONCILIATION_SHUTDOWN_TIMEOUT_SECONDS
        )
        done, pending = await asyncio.wait(tasks, timeout=timeout)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            try:
                task.result()
            except asyncio.CancelledError:
                pass
            except Exception:
                log.exception("financial reconciliation worker stopped with error")
        _TASKS.clear()
        _STOP_EVENT = None
        log.info(
            "FINANCIAL_RECONCILIATION_STOPPED completed=%s cancelled=%s",
            len(done),
            len(pending),
        )
