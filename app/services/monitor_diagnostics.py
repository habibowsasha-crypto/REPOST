"""Read-only monitor diagnostics for the v1.0.7a1 observability phase.

The module records process-local timings and counters only.  It never changes
trading state, never calls BingX on its own, and never participates in order
confirmation.  Context variables let concurrent asyncio tasks attribute BingX
HTTP calls to the monitor cycle/stage that initiated them without changing the
workload priority used by the exchange governor.
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import logging
import math
import re
import time
from collections import Counter, defaultdict, deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Awaitable, Iterator, Mapping, TypeVar

log = logging.getLogger(__name__)

_T = TypeVar("_T")
_ROLLING_WINDOW = 240


def _diagnostics_enabled() -> bool:
    """Return the cached runtime switch without creating side effects."""
    try:
        from app.config import get_settings

        return bool(getattr(get_settings(), "MONITOR_DIAGNOSTICS_ENABLED", True))
    except Exception:
        # Fail open for observability only. The caller still isolates all
        # diagnostic failures from trading behavior.
        return True


def _finite_non_negative(value: Any) -> float:
    try:
        number = float(value or 0.0)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    return number if math.isfinite(number) and number >= 0 else 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError, OverflowError):
        return 0


def _compact_json(value: Mapping[str, Any] | Counter[str]) -> str:
    try:
        return json.dumps(dict(value), ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    except Exception:
        return "{}"


def _endpoint_name(method: str, path: str) -> str:
    method_up = str(method or "").upper()
    path_l = str(path or "").split("?", 1)[0].lower()
    mapping = (
        ("/quote/premiumindex", "premium_index"),
        ("/quote/ticker", "ticker"),
        ("/user/positions", "positions"),
        ("/user/balance", "balance"),
        ("/trade/openorders", "open_orders"),
        ("/trade/allorders", "all_orders"),
        ("/trade/positionhistory", "position_history"),
    )
    for needle, label in mapping:
        if needle in path_l:
            return label
    if "/trade/order" in path_l:
        return "trade_order_write" if method_up in {"POST", "DELETE"} else "order_detail"
    return "other"


@dataclass
class DiagnosticSpan:
    label: str
    started_mono: float = field(default_factory=time.monotonic)
    metadata: dict[str, Any] = field(default_factory=dict)
    counters: Counter[str] = field(default_factory=Counter)
    durations_ms: Counter[str] = field(default_factory=Counter)
    endpoints: Counter[str] = field(default_factory=Counter)
    errors: int = 0
    finished_mono: float | None = None

    def set(self, key: str, value: Any) -> None:
        self.metadata[str(key)] = value

    def inc(self, key: str, amount: int = 1) -> None:
        self.counters[str(key)] += int(amount)

    def add_ms(self, key: str, value: float | int) -> None:
        self.durations_ms[str(key)] += int(round(_finite_non_negative(value)))

    @property
    def duration_ms(self) -> int:
        end = self.finished_mono if self.finished_mono is not None else time.monotonic()
        return int(max(0.0, end - self.started_mono) * 1000)

    def snapshot(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "label": self.label,
            "duration_ms": self.duration_ms,
            "errors": int(self.errors),
            **self.metadata,
            **dict(self.counters),
            **dict(self.durations_ms),
        }
        if self.endpoints:
            data["endpoints"] = dict(sorted(self.endpoints.items()))
        attributed = (
            int(self.durations_ms.get("network_ms", 0))
            + int(self.durations_ms.get("workload_wait_ms", 0))
            + max(
                int(self.durations_ms.get("direct_db_ms", 0)),
                int(self.durations_ms.get("db_scope_ms", 0)),
            )
            + int(self.durations_ms.get("lock_wait_ms", 0))
            + int(self.durations_ms.get("semaphore_wait_ms", 0))
            + int(self.durations_ms.get("sleep_backoff_ms", 0))
        )
        data["unattributed_ms"] = max(0, self.duration_ms - attributed)
        return data


_SPAN_STACK: contextvars.ContextVar[tuple[DiagnosticSpan, ...]] = contextvars.ContextVar(
    "monitor_diagnostic_span_stack", default=()
)


def current_origin() -> str:
    stack = _SPAN_STACK.get()
    return stack[-1].label if stack else "unattributed"


def _active_spans() -> tuple[DiagnosticSpan, ...]:
    return _SPAN_STACK.get()


class _RollingMetric:
    def __init__(self, maxlen: int = _ROLLING_WINDOW) -> None:
        self.values: deque[float] = deque(maxlen=max(20, int(maxlen)))

    def add(self, value: Any) -> None:
        number = _finite_non_negative(value)
        self.values.append(number)

    def snapshot(self) -> dict[str, int]:
        ordered = sorted(self.values)
        if not ordered:
            return {"count": 0, "avg_ms": 0, "p50_ms": 0, "p95_ms": 0, "max_ms": 0}

        def percentile(q: float) -> int:
            index = min(len(ordered) - 1, max(0, math.ceil(len(ordered) * q) - 1))
            return int(round(ordered[index]))

        return {
            "count": len(ordered),
            "avg_ms": int(round(sum(ordered) / len(ordered))),
            "p50_ms": percentile(0.50),
            "p95_ms": percentile(0.95),
            "max_ms": int(round(ordered[-1])),
        }


class _Aggregate:
    def __init__(self) -> None:
        self.lock = Lock()
        self.duration_by_label: dict[str, _RollingMetric] = defaultdict(_RollingMetric)
        self.requests_by_label: Counter[str] = Counter()
        self.errors_by_label: Counter[str] = Counter()
        self.endpoints_by_label: dict[str, Counter[str]] = defaultdict(Counter)
        self.selected_counters_by_label: dict[
            str, dict[str, deque[int]]
        ] = defaultdict(dict)
        self.last_span_at: dict[str, float] = {}

    def record(self, span: DiagnosticSpan) -> None:
        with self.lock:
            self.duration_by_label[span.label].add(span.duration_ms)
            self.requests_by_label[span.label] += int(span.counters.get("http_requests", 0))
            self.errors_by_label[span.label] += int(span.errors)
            self.endpoints_by_label[span.label].update(span.endpoints)
            for key in (
                "event_gate_suppressed",
                "event_gate_rearmed",
                "event_gate_safety_rechecks",
                "event_gate_deferred_suppressed",
                "event_tick_symbols",
                "manual_backoff_due",
                "manual_backoff_deferred",
                "manual_backoff_eligible",
                "manual_fast",
                "manual_backoff_scheduled",
                "manual_backoff_changed",
                "manual_backoff_unchanged",
                "manual_backoff_conflicts",
                "manual_backoff_max_delay_sec",
                "manual_fast_invalid_data",
                "manual_fast_no_api",
                "manual_fast_live_position",
                "manual_fast_opposite_position",
                "manual_fast_active_entry",
                "manual_fast_be_replacement",
                "manual_fast_unknown_stop",
                "manual_fast_residual",
                "manual_fast_cleanup_unresolved",
                "manual_fast_zero_proof_missing",
                "manual_fast_zero_proof_invalid",
                "manual_cleanup_probe_due",
                "manual_cleanup_probe_deferred",
                "closed_history_due",
                "closed_history_deferred",
                "critical_db_rows_loaded",
                "critical_db_rows_changed",
                "critical_db_writes_skipped",
                "critical_db_locked_reads_reused",
                "critical_db_update_connection_reused",
                "full_stage_advisory_session_opened",
                "full_stage_db_connection_reused",
                "full_execution_lock_connection_reused",
            ):
                value = span.metadata.get(key)
                if value is None and key in span.counters:
                    value = span.counters.get(key)
                if value is None:
                    continue
                bucket = self.selected_counters_by_label[span.label].get(key)
                if bucket is None:
                    bucket = deque(maxlen=_ROLLING_WINDOW)
                    self.selected_counters_by_label[span.label][key] = bucket
                bucket.append(_safe_int(value))
            self.last_span_at[span.label] = time.time()

    def snapshot(self, labels: tuple[str, ...] | None = None) -> dict[str, Any]:
        with self.lock:
            chosen = labels or tuple(sorted(self.duration_by_label))
            result: dict[str, Any] = {}
            for label in chosen:
                metric = self.duration_by_label.get(label)
                if metric is None:
                    continue
                selected_counters: dict[str, dict[str, int]] = {}
                for key, values in self.selected_counters_by_label.get(label, {}).items():
                    samples = list(values)
                    if not samples:
                        continue
                    selected_counters[key] = {
                        "sum": int(sum(samples)),
                        "avg": int(round(sum(samples) / len(samples))),
                        "max": int(max(samples)),
                    }
                result[label] = {
                    "duration": metric.snapshot(),
                    "http_requests": int(self.requests_by_label.get(label, 0)),
                    "errors": int(self.errors_by_label.get(label, 0)),
                    "endpoints": dict(sorted(self.endpoints_by_label.get(label, {}).items())),
                    "last_age_sec": (
                        None
                        if not self.last_span_at.get(label)
                        else int(max(0.0, time.time() - self.last_span_at[label]))
                    ),
                }
                if selected_counters:
                    result[label]["counters"] = selected_counters
            return result


_AGGREGATE = _Aggregate()


_LAST_ERROR_LOCK = Lock()
_LAST_MONITOR_ERRORS: deque[dict[str, Any]] = deque(maxlen=8)
_ERROR_SECRET_RE = re.compile(
    r"(?i)(signature|timestamp|recvwindow|api[-_]?(?:key|secret)|secret[-_]?key|"
    r"x-bx-apikey|authorization|passphrase|access[-_]?token|refresh[-_]?token|"
    r"bot[-_]?token)=([^&\s,;]+)"
)


def _sanitize_error_message(value: Any) -> str:
    text = str(value or "")[:1200]
    return _ERROR_SECRET_RE.sub(lambda match: f"{match.group(1)}=<redacted>", text)[:300]


def record_monitor_error(
    source: str,
    exc: BaseException,
    *,
    execution_id: int | None = None,
    symbol: str | None = None,
) -> None:
    """Remember a compact sanitized monitor error for later summaries."""

    try:
        entry = {
            "at": datetime.now(timezone.utc).isoformat(),
            "source": str(source or "unknown")[:120],
            "type": type(exc).__name__,
            "message": _sanitize_error_message(exc),
        }
        if execution_id is not None:
            entry["execution_id"] = int(execution_id)
        if symbol:
            entry["symbol"] = str(symbol).upper()[:40]
        with _LAST_ERROR_LOCK:
            _LAST_MONITOR_ERRORS.append(entry)
    except Exception:
        return


def monitor_error_snapshot() -> list[dict[str, Any]]:
    with _LAST_ERROR_LOCK:
        return [dict(item) for item in _LAST_MONITOR_ERRORS]


@contextmanager
def diagnostic_span(
    label: str,
    *,
    emit: bool = True,
    metadata: Mapping[str, Any] | None = None,
) -> Iterator[DiagnosticSpan]:
    """Attribute nested async work to a named read-only diagnostic span."""

    span = DiagnosticSpan(str(label or "unnamed"))
    if metadata:
        span.metadata.update(dict(metadata))
    if not _diagnostics_enabled():
        try:
            yield span
        except BaseException:
            raise
        finally:
            span.finished_mono = time.monotonic()
        return

    stack = _SPAN_STACK.get()
    token = _SPAN_STACK.set((*stack, span))
    try:
        yield span
    except BaseException:
        span.errors += 1
        raise
    finally:
        span.finished_mono = time.monotonic()
        _SPAN_STACK.reset(token)
        try:
            _AGGREGATE.record(span)
            if emit and not bool(span.metadata.get("_suppress_emit")):
                emit_span(span)
        except Exception:
            # Diagnostics must never change the wrapped operation's outcome.
            pass


def emit_span(span: DiagnosticSpan) -> None:
    data = span.snapshot()
    metadata_keys = (
        "result",
        "rows_selected",
        "rows_scanned",
        "rows_due",
        "rows_skipped",
        "rows_source",
        "groups",
        "symbols",
        "queued_events",
        "event_gate_suppressed",
        "event_gate_rearmed",
        "event_gate_safety_rechecks",
        "event_gate_deferred_suppressed",
        "event_tick_symbols",
        "manual_backoff_due",
        "manual_backoff_deferred",
        "manual_backoff_eligible",
        "manual_fast",
        "manual_backoff_scheduled",
        "manual_backoff_changed",
        "manual_backoff_unchanged",
        "manual_backoff_conflicts",
        "manual_backoff_max_delay_sec",
        "manual_fast_invalid_data",
        "manual_fast_no_api",
        "manual_fast_live_position",
        "manual_fast_opposite_position",
        "manual_fast_active_entry",
        "manual_fast_be_replacement",
        "manual_fast_unknown_stop",
        "manual_fast_residual",
        "manual_fast_cleanup_unresolved",
        "manual_fast_zero_proof_missing",
        "manual_fast_zero_proof_invalid",
        "manual_cleanup_probe_due",
        "manual_cleanup_probe_deferred",
        "closed_history_due",
        "closed_history_deferred",
        "critical_db_rows_loaded",
        "critical_db_rows_changed",
        "critical_db_writes_skipped",
        "critical_db_locked_reads_reused",
        "critical_db_update_connection_reused",
        "full_stage_advisory_session_opened",
        "full_stage_db_connection_reused",
        "full_execution_lock_connection_reused",
        "users",
        "event_type",
        "attempt",
        "price_stream",
    )
    parts = [
        f"label={span.label}",
        f"duration_ms={data.get('duration_ms', 0)}",
        f"errors={data.get('errors', 0)}",
    ]
    for key in metadata_keys:
        if key in data:
            parts.append(f"{key}={data[key]}")
    for key in (
        "http_requests",
        "http_errors",
        "network_ms",
        "request_total_ms",
        "workload_wait_ms",
        "gate_wait_ms",
        "trade_order_wait_ms",
        "direct_db_calls",
        "direct_db_ms",
        "db_scope_calls",
        "db_scope_ms",
        "lock_wait_ms",
        "semaphore_wait_ms",
        "sleep_backoff_ms",
        "unattributed_ms",
    ):
        value = data.get(key)
        if value not in (None, 0, "0"):
            parts.append(f"{key}={value}")
    if data.get("endpoints"):
        parts.append(f"endpoints={_compact_json(data['endpoints'])}")
    log.info("MONITOR_DIAGNOSTICS_SPAN %s", " ".join(parts))


def record_http_request(
    *,
    method: str,
    path: str,
    status_code: int | None,
    total_ms: float | int,
    network_ms: float | int,
    workload_metrics: Mapping[str, Any] | None = None,
    error: bool = False,
) -> None:
    """Record one already-executed BingX request without exposing its query."""

    endpoint = _endpoint_name(method, path)
    metrics = dict(workload_metrics or {})
    for span in _active_spans():
        span.inc("http_requests")
        span.endpoints[endpoint] += 1
        span.add_ms("request_total_ms", total_ms)
        span.add_ms("network_ms", network_ms)
        span.add_ms("workload_wait_ms", metrics.get("wait_ms", 0))
        span.add_ms("gate_wait_ms", metrics.get("gate_wait_ms", 0))
        span.add_ms("trade_order_wait_ms", metrics.get("trade_order_wait_ms", 0))
        if error or (status_code is not None and int(status_code) >= 400):
            span.inc("http_errors")
            span.errors += 1



def record_counter(key: str, amount: int = 1) -> None:
    """Increment one diagnostic counter on every active nested span."""

    try:
        for span in _active_spans():
            span.inc(str(key or "counter"), int(amount))
    except Exception:
        return


def record_db_scope(duration_ms: float | int, *, error: bool = False) -> None:
    """Record aggregate DB connection-scope time without double-counting named calls."""
    try:
        for span in _active_spans():
            span.inc("db_scope_calls")
            span.add_ms("db_scope_ms", duration_ms)
            if error:
                span.errors += 1
    except Exception:
        return

def record_db_call(name: str, duration_ms: float | int, *, error: bool = False) -> None:
    try:
        for span in _active_spans():
            span.inc("direct_db_calls")
            span.add_ms("direct_db_ms", duration_ms)
            span.counters[f"db_{str(name or 'unknown')}"] += 1
            if error:
                span.errors += 1
    except Exception:
        return


async def timed_db_call(name: str, awaitable: Awaitable[_T]) -> _T:
    started = time.monotonic()
    failed = False
    try:
        return await awaitable
    except BaseException:
        failed = True
        raise
    finally:
        try:
            record_db_call(name, (time.monotonic() - started) * 1000, error=failed)
        except Exception:
            pass


def record_wait(kind: str, duration_ms: float | int) -> None:
    try:
        key = str(kind or "wait_ms")
        for span in _active_spans():
            span.add_ms(key, duration_ms)
    except Exception:
        return


def record_sleep_backoff(duration_ms: float | int) -> None:
    try:
        for span in _active_spans():
            span.add_ms("sleep_backoff_ms", duration_ms)
    except Exception:
        return


def record_stage_rows(
    *,
    selected: int | None = None,
    scanned: int | None = None,
    due: int | None = None,
    skipped: int | None = None,
    lock_skipped: int | None = None,
    source: str | None = None,
) -> None:
    try:
        stack = _active_spans()
        if not stack:
            return
        span = stack[-1]
        if selected is not None:
            span.set("rows_selected", int(selected))
        if scanned is not None:
            span.set("rows_scanned", int(scanned))
        if due is not None:
            span.set("rows_due", int(due))
        if skipped is not None:
            span.set("rows_skipped", int(skipped))
        if lock_skipped is not None:
            span.set("lock_skipped", int(lock_skipped))
        if source:
            span.set("rows_source", str(source))
    except Exception:
        return


_HEARTBEAT_LOCK = Lock()
_HEARTBEAT_STATE: dict[str, Any] = {
    "last_heartbeat_at": None,
    "last_lag_ms": 0,
    "max_lag_ms": 0,
    "warning_count": 0,
    "critical_count": 0,
}
_LAST_CYCLE_COMPLETION: dict[str, float] = {}


def mark_cycle_completed(name: str) -> None:
    with _HEARTBEAT_LOCK:
        _LAST_CYCLE_COMPLETION[str(name)] = time.time()


def heartbeat_snapshot() -> dict[str, Any]:
    with _HEARTBEAT_LOCK:
        now = time.time()
        return {
            **dict(_HEARTBEAT_STATE),
            "cycle_last_age_sec": {
                key: int(max(0.0, now - value))
                for key, value in sorted(_LAST_CYCLE_COMPLETION.items())
            },
        }


async def event_loop_heartbeat_loop() -> None:
    """Measure scheduler lag only; never restart or cancel another task."""

    from app.config import get_settings

    settings = get_settings()
    if not bool(getattr(settings, "MONITOR_DIAGNOSTICS_ENABLED", True)):
        return
    interval = max(0.25, float(getattr(settings, "MONITOR_HEARTBEAT_INTERVAL_SEC", 1.0) or 1.0))
    warning_sec = max(interval, float(getattr(settings, "MONITOR_HEARTBEAT_WARNING_SEC", 2.0) or 2.0))
    critical_sec = max(warning_sec, float(getattr(settings, "MONITOR_HEARTBEAT_CRITICAL_SEC", 10.0) or 10.0))
    loop = asyncio.get_running_loop()
    expected = loop.time() + interval
    while True:
        try:
            await asyncio.sleep(max(0.0, expected - loop.time()))
            actual = loop.time()
            lag = max(0.0, actual - expected)
            lag_ms = int(lag * 1000)
            with _HEARTBEAT_LOCK:
                _HEARTBEAT_STATE["last_heartbeat_at"] = datetime.now(timezone.utc).isoformat()
                _HEARTBEAT_STATE["last_lag_ms"] = lag_ms
                _HEARTBEAT_STATE["max_lag_ms"] = max(
                    int(_HEARTBEAT_STATE.get("max_lag_ms") or 0), lag_ms
                )
                if lag >= warning_sec:
                    _HEARTBEAT_STATE["warning_count"] = int(
                        _HEARTBEAT_STATE.get("warning_count") or 0
                    ) + 1
                if lag >= critical_sec:
                    _HEARTBEAT_STATE["critical_count"] = int(
                        _HEARTBEAT_STATE.get("critical_count") or 0
                    ) + 1
            if lag >= critical_sec:
                log.error(
                    "MONITOR_EVENT_LOOP_LAG severity=critical lag_ms=%s warning_ms=%s critical_ms=%s",
                    lag_ms,
                    int(warning_sec * 1000),
                    int(critical_sec * 1000),
                )
            elif lag >= warning_sec:
                log.warning(
                    "MONITOR_EVENT_LOOP_LAG severity=warning lag_ms=%s warning_ms=%s critical_ms=%s",
                    lag_ms,
                    int(warning_sec * 1000),
                    int(critical_sec * 1000),
                )
            expected += interval
            if actual - expected > interval * 5:
                expected = actual + interval
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("monitor event-loop heartbeat failed")
            expected = loop.time() + interval
            await asyncio.sleep(interval)


def _json_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    try:
        parsed = json.loads(str(value or "{}"))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


async def _database_queue_snapshot() -> dict[str, Any]:
    """Read a compact queue/status snapshot from the existing schema."""

    from app.database import db

    started = time.monotonic()
    if db.is_postgres():
        async with db.connect() as conn:
            event_row = await conn.fetchrow(
                """
                SELECT
                  COUNT(*) FILTER (WHERE status='pending') AS pending,
                  COUNT(*) FILTER (WHERE status='processing') AS processing,
                  COUNT(*) FILTER (WHERE status='pending' AND next_attempt_at<=NOW()) AS due,
                  COALESCE(EXTRACT(EPOCH FROM (NOW()-MIN(updated_at) FILTER (WHERE status IN ('pending','processing')))),0) AS oldest_age
                FROM market_events
                """
            )
            notification_row = await conn.fetchrow(
                """
                SELECT
                  COUNT(*) FILTER (WHERE status='pending') AS pending,
                  COUNT(*) FILTER (WHERE status='pending' AND next_attempt_at<=NOW()) AS due,
                  COALESCE(EXTRACT(EPOCH FROM (NOW()-MIN(created_at) FILTER (WHERE status='pending'))),0) AS oldest_age
                FROM durable_notifications
                """
            )
            execution_rows = await conn.fetch(
                """
                SELECT status, exchange_order_ids_json
                FROM trade_executions
                WHERE status = ANY($1::text[])
                LIMIT 5000
                """,
                [
                    "protected",
                    "partial_error",
                    "partial_unrecoverable",
                    "manual_required",
                    "closed_pending_history",
                ],
            )
    else:
        async with db.connect() as conn:
            event_cur = await conn.execute(
                """
                SELECT
                  SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN status='processing' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN status='pending' AND datetime(next_attempt_at)<=datetime('now') THEN 1 ELSE 0 END),
                  COALESCE((julianday('now')-julianday(MIN(CASE WHEN status IN ('pending','processing') THEN updated_at END)))*86400,0)
                FROM market_events
                """
            )
            event_values = await event_cur.fetchone()
            event_row = {
                "pending": event_values[0] or 0,
                "processing": event_values[1] or 0,
                "due": event_values[2] or 0,
                "oldest_age": event_values[3] or 0,
            }
            notification_cur = await conn.execute(
                """
                SELECT
                  SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN status='pending' AND datetime(next_attempt_at)<=datetime('now') THEN 1 ELSE 0 END),
                  COALESCE((julianday('now')-julianday(MIN(CASE WHEN status='pending' THEN created_at END)))*86400,0)
                FROM durable_notifications
                """
            )
            notification_values = await notification_cur.fetchone()
            notification_row = {
                "pending": notification_values[0] or 0,
                "due": notification_values[1] or 0,
                "oldest_age": notification_values[2] or 0,
            }
            execution_cur = await conn.execute(
                """
                SELECT status, exchange_order_ids_json
                FROM trade_executions
                WHERE status IN ('protected','partial_error','partial_unrecoverable','manual_required','closed_pending_history')
                LIMIT 5000
                """
            )
            execution_rows = await execution_cur.fetchall()

    status_counts: Counter[str] = Counter()
    background_recoverable = 0
    for row in execution_rows:
        row_map = dict(row)
        status = str(row_map.get("status") or "")
        status_counts[status] += 1
        payload = _json_payload(row_map.get("exchange_order_ids_json"))
        background = payload.get("tp_background_v1_0_6a")
        if status == "protected" and isinstance(background, dict):
            state = str(background.get("state") or "").lower()
            if state in {"", "queued", "running", "retry", "error"}:
                background_recoverable += 1

    elapsed_ms = int((time.monotonic() - started) * 1000)
    return {
        "market_events": {
            "pending": _safe_int(event_row["pending"]),
            "processing": _safe_int(event_row["processing"]),
            "due": _safe_int(event_row["due"]),
            "oldest_age_sec": int(_finite_non_negative(event_row["oldest_age"])),
        },
        "notifications": {
            "pending": _safe_int(notification_row["pending"]),
            "due": _safe_int(notification_row["due"]),
            "oldest_age_sec": int(_finite_non_negative(notification_row["oldest_age"])),
        },
        "executions": {
            "critical_rows": int(
                status_counts.get("partial_error", 0)
                + status_counts.get("partial_unrecoverable", 0)
                + status_counts.get("manual_required", 0)
                + status_counts.get("closed_pending_history", 0)
            ),
            "partial_backlog": int(
                status_counts.get("partial_error", 0)
                + status_counts.get("partial_unrecoverable", 0)
            ),
            "manual_required": int(status_counts.get("manual_required", 0)),
            "closed_pending_history": int(status_counts.get("closed_pending_history", 0)),
            "background_tp_recoverable": int(background_recoverable),
        },
        "snapshot_db_ms": elapsed_ms,
    }


async def monitor_diagnostics_summary_loop() -> None:
    """Emit one compact, sanitized process/DB summary at a slow cadence."""

    from app import __version__ as app_version
    from app.config import get_settings

    settings = get_settings()
    if not bool(getattr(settings, "MONITOR_DIAGNOSTICS_ENABLED", True)):
        return
    interval = max(
        15.0,
        float(getattr(settings, "MONITOR_DIAGNOSTICS_SUMMARY_INTERVAL_SEC", 60.0) or 60.0),
    )
    labels = (
        "cycle.public_price",
        "cycle.critical_reconcile",
        "cycle.full_reconcile",
        "cycle.event_verifier_batch",
        "full.limit_tp_catchup",
        "full.background_tp_recovery",
        "full.partial_tp_recovery",
        "full.position_lifecycle_guard",
        "full.be_monitor",
    )
    while True:
        try:
            await asyncio.sleep(interval)
            db_snapshot = await asyncio.wait_for(
                _database_queue_snapshot(),
                timeout=10.0,
            )
            from app.services.execution_dispatcher import trade_dispatcher_stats
            from app.services.notification_dispatcher import notification_dispatcher_stats
            from app.services.workload_manager import bingx_workload_stats

            trade_stats = trade_dispatcher_stats()
            notification_stats = notification_dispatcher_stats()
            workload_stats = await bingx_workload_stats()
            heartbeat = heartbeat_snapshot()
            rolling = _AGGREGATE.snapshot(labels)
            last_errors = monitor_error_snapshot()
            log.info(
                "MONITOR_DIAGNOSTICS_SUMMARY version=%s heartbeat=%s queues=%s trade_dispatcher=%s "
                "notification_dispatcher=%s workload=%s rolling=%s last_errors=%s",
                app_version,
                _compact_json(heartbeat),
                _compact_json(db_snapshot),
                _compact_json(trade_stats),
                _compact_json(notification_stats),
                _compact_json(workload_stats),
                _compact_json(rolling),
                _compact_json({"items": last_errors}),
            )
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            log.warning("MONITOR_DIAGNOSTICS_DB_TIMEOUT timeout_sec=10.0")
            await asyncio.sleep(min(interval, 5.0))
        except Exception:
            log.exception("monitor diagnostics summary failed")
            await asyncio.sleep(min(interval, 5.0))


def monitor_diagnostics_snapshot() -> dict[str, Any]:
    return {
        "heartbeat": heartbeat_snapshot(),
        "rolling": _AGGREGATE.snapshot(),
        "last_errors": monitor_error_snapshot(),
    }


def reset_monitor_diagnostics_for_tests() -> None:
    global _AGGREGATE
    _SPAN_STACK.set(())
    _AGGREGATE = _Aggregate()
    with _HEARTBEAT_LOCK:
        _HEARTBEAT_STATE.update(
            {
                "last_heartbeat_at": None,
                "last_lag_ms": 0,
                "max_lag_ms": 0,
                "warning_count": 0,
                "critical_count": 0,
            }
        )
        _LAST_CYCLE_COMPLETION.clear()
    with _LAST_ERROR_LOCK:
        _LAST_MONITOR_ERRORS.clear()
