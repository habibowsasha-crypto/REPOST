"""Process-local execution timing metrics for safe 20-user scaling.

This module is intentionally read-only with respect to trading state.  It keeps
small rolling windows of dispatcher / exchange-governor timings so the bot can
measure the current bottleneck before changing concurrency or TP scheduling.
All values are best-effort diagnostics; failures here must never affect trade
execution.
"""

from __future__ import annotations

import time
from collections import Counter, deque
from threading import Lock
from typing import Any

_WINDOW = 200


class _RollingWindow:
    def __init__(self, maxlen: int = _WINDOW) -> None:
        self._items: deque[float] = deque(maxlen=max(10, int(maxlen)))

    def add(self, value: float | int | None) -> None:
        try:
            number = float(value if value is not None else 0.0)
        except Exception:
            return
        if number < 0:
            return
        self._items.append(number)

    def snapshot(self) -> dict[str, int]:
        values = sorted(self._items)
        count = len(values)
        if not count:
            return {"count": 0, "avg_ms": 0, "p50_ms": 0, "p95_ms": 0, "max_ms": 0}
        return {
            "count": count,
            "avg_ms": int(round(sum(values) / count)),
            "p50_ms": _percentile_ms(values, 0.50),
            "p95_ms": _percentile_ms(values, 0.95),
            "max_ms": int(round(values[-1])),
        }


def _percentile_ms(sorted_values: list[float], percentile: float) -> int:
    if not sorted_values:
        return 0
    if len(sorted_values) == 1:
        return int(round(sorted_values[0]))
    index = min(len(sorted_values) - 1, max(0, int(round((len(sorted_values) - 1) * percentile))))
    return int(round(sorted_values[index]))


class _ExecutionMetrics:
    def __init__(self) -> None:
        self._lock = Lock()
        self._started_at = time.time()
        self._last_trade_at = 0.0
        self._trade_count = 0
        self._status_counts: Counter[str] = Counter()
        self._dispatch_queue_ms = _RollingWindow()
        self._dispatch_execution_ms = _RollingWindow()
        self._dispatch_total_ms = _RollingWindow()
        self._executor_ms = _RollingWindow()
        self._entry_to_stop_ms = _RollingWindow()
        self._stop_to_tp_ms = _RollingWindow()
        self._full_trade_ms = _RollingWindow()
        self._workload_wait_ms = _RollingWindow()
        self._workload_trade_order_wait_ms = _RollingWindow()
        self._workload_stop_wait_ms = _RollingWindow()
        self._workload_tp_wait_ms = _RollingWindow()
        self._workload_entry_wait_ms = _RollingWindow()
        self._workload_status_counts: Counter[str] = Counter()

    def reset(self) -> None:
        with self._lock:
            self._started_at = time.time()
            self._last_trade_at = 0.0
            self._trade_count = 0
            self._status_counts.clear()
            self._dispatch_queue_ms = _RollingWindow()
            self._dispatch_execution_ms = _RollingWindow()
            self._dispatch_total_ms = _RollingWindow()
            self._executor_ms = _RollingWindow()
            self._entry_to_stop_ms = _RollingWindow()
            self._stop_to_tp_ms = _RollingWindow()
            self._full_trade_ms = _RollingWindow()
            self._workload_wait_ms = _RollingWindow()
            self._workload_trade_order_wait_ms = _RollingWindow()
            self._workload_stop_wait_ms = _RollingWindow()
            self._workload_tp_wait_ms = _RollingWindow()
            self._workload_entry_wait_ms = _RollingWindow()
            self._workload_status_counts.clear()

    def record_trade_result(self, *, status: str, payload: dict[str, Any] | None) -> None:
        data = payload if isinstance(payload, dict) else {}
        dispatch = data.get("dispatch") if isinstance(data.get("dispatch"), dict) else {}
        timing = data.get("execution_timing_v1_0_4")
        if not isinstance(timing, dict):
            timing = {}
        with self._lock:
            self._trade_count += 1
            self._last_trade_at = time.time()
            self._status_counts[str(status or "unknown")] += 1
            self._dispatch_queue_ms.add(dispatch.get("queue_wait_ms"))
            self._dispatch_execution_ms.add(dispatch.get("execution_ms"))
            self._dispatch_total_ms.add(dispatch.get("signal_to_result_ms"))
            self._executor_ms.add(dispatch.get("executor_duration_ms"))
            self._full_trade_ms.add(timing.get("result_ready_ms") or dispatch.get("execution_ms"))
            entry_ms = _first_number(
                timing,
                "entry_write_returned_ms",
                "entry_submitted_ms",
                "entry_confirmed_ms",
            )
            stop_ms = _first_number(
                timing,
                "stop_confirmed_ms",
                "market_stop_confirmed_ms",
                "limit_stop_attached_ms",
            )
            tp_ms = _first_number(timing, "tp_completed_ms", "tp_failed_ms")
            if entry_ms is not None and stop_ms is not None and stop_ms >= entry_ms:
                self._entry_to_stop_ms.add(stop_ms - entry_ms)
            if stop_ms is not None and tp_ms is not None and tp_ms >= stop_ms:
                self._stop_to_tp_ms.add(tp_ms - stop_ms)

    def record_workload_request(self, metrics: dict[str, Any] | None) -> None:
        data = metrics if isinstance(metrics, dict) else {}
        priority = int(_coerce_float(data.get("priority"), default=999) or 999)
        wait_ms = _coerce_float(data.get("wait_ms"), default=0.0)
        trade_order_wait_ms = _coerce_float(data.get("trade_order_wait_ms"), default=0.0)
        with self._lock:
            self._workload_wait_ms.add(wait_ms)
            self._workload_trade_order_wait_ms.add(trade_order_wait_ms)
            self._workload_status_counts[f"priority_{priority}"] += 1
            # Lower number = higher priority.  Keep buckets aligned with
            # workload_manager constants without importing it and risking cycles.
            if priority <= 5:
                self._workload_stop_wait_ms.add(wait_ms)
            elif priority >= 30 and priority < 50:
                self._workload_tp_wait_ms.add(wait_ms)
            elif priority >= 50:
                self._workload_entry_wait_ms.add(wait_ms)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "version": "v1.0.6b",
                "uptime_sec": int(max(0.0, time.time() - self._started_at)),
                "last_trade_age_sec": (
                    None if self._last_trade_at <= 0 else int(max(0.0, time.time() - self._last_trade_at))
                ),
                "trade_count": int(self._trade_count),
                "status_counts": dict(self._status_counts),
                "dispatch_queue_ms": self._dispatch_queue_ms.snapshot(),
                "dispatch_execution_ms": self._dispatch_execution_ms.snapshot(),
                "dispatch_total_ms": self._dispatch_total_ms.snapshot(),
                "executor_ms": self._executor_ms.snapshot(),
                "entry_to_stop_ms": self._entry_to_stop_ms.snapshot(),
                "stop_to_tp_ms": self._stop_to_tp_ms.snapshot(),
                "full_trade_ms": self._full_trade_ms.snapshot(),
                "workload_wait_ms": self._workload_wait_ms.snapshot(),
                "workload_trade_order_wait_ms": self._workload_trade_order_wait_ms.snapshot(),
                "workload_stop_wait_ms": self._workload_stop_wait_ms.snapshot(),
                "workload_tp_wait_ms": self._workload_tp_wait_ms.snapshot(),
                "workload_entry_wait_ms": self._workload_entry_wait_ms.snapshot(),
                "workload_priority_counts": dict(self._workload_status_counts),
            }


def _coerce_float(value: Any, *, default: float | None = None) -> float | None:
    try:
        return float(value)
    except Exception:
        return default


def _first_number(data: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _coerce_float(data.get(key), default=None)
        if value is not None:
            return value
    return None


_METRICS = _ExecutionMetrics()


def record_trade_result(*, status: str, payload: dict[str, Any] | None) -> None:
    try:
        _METRICS.record_trade_result(status=status, payload=payload)
    except Exception:
        # Metrics must never affect execution.
        return


def record_workload_request(metrics: dict[str, Any] | None) -> None:
    try:
        _METRICS.record_workload_request(metrics)
    except Exception:
        return


def execution_metrics_snapshot() -> dict[str, Any]:
    return _METRICS.snapshot()


def reset_execution_metrics_for_tests() -> None:
    _METRICS.reset()
