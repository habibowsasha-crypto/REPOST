"""Deterministic statistics-v2 metrics for signals, executions and portfolio risk.

This module is the read-only calculation layer introduced in statistics plan
step 6.  It accepts already-durable rows and performs no database writes, no
exchange requests and no work in ENTRY/STOP/TP/BE/public-price hot paths.

Design rules:
- incomplete or ambiguous rows are excluded rather than guessed;
- every report carries its denominator and exclusion reasons;
- Decimal is used for all money/rate calculations;
- ordering is explicit, so identical input produces identical output.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from statistics import median
from typing import Any, Iterable, Mapping, Sequence


_SIGNAL_TERMINAL = frozenset({"completed_stop", "completed_be", "completed_tp"})
_BAD_QUALITY = frozenset({"ambiguous", "unavailable", "needs_recovery", "quarantined"})
_FINAL = "FINAL"
_ZERO = Decimal("0")
_HUNDRED = Decimal("100")


@dataclass(frozen=True, slots=True)
class MetricScope:
    total_rows: int
    eligible_rows: int
    excluded_rows: int
    completeness_percent: Decimal
    exclusion_reasons: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SignalSegmentMetrics:
    total: int
    eligible_completed: int
    tp1_hits: int
    stop_before_tp1: int
    tp1_hit_rate_percent: Decimal | None


@dataclass(frozen=True, slots=True)
class SignalMetricsReport:
    scope: MetricScope
    total_unique_signals: int
    completed: int
    active: int
    waiting_entry: int
    needs_recovery: int
    ambiguous: int
    tp_hit_counts: dict[int, int]
    tp_hit_rates_percent: dict[int, Decimal | None]
    stop_before_tp1: int
    be_after_tp1: int
    be_after_tp2: int
    be_after_tp3_plus: int
    average_max_tp_index: Decimal | None
    median_max_tp_index: Decimal | None
    average_time_to_tp1_seconds: Decimal | None
    median_time_to_tp1_seconds: Decimal | None
    average_duration_seconds: Decimal | None
    median_duration_seconds: Decimal | None
    worst_stop_streak: int
    worst_stop_before_tp1_streak: int
    complete_trajectory_share_percent: Decimal
    by_side: dict[str, SignalSegmentMetrics]
    by_strategy: dict[str, SignalSegmentMetrics]
    by_timeframe: dict[str, SignalSegmentMetrics]
    by_symbol: dict[str, SignalSegmentMetrics]


@dataclass(frozen=True, slots=True)
class ExecutionSegmentMetrics:
    total: int
    final: int
    net_pnl: Decimal
    expectancy_r: Decimal | None


@dataclass(frozen=True, slots=True)
class ExecutionMetricsReport:
    scope: MetricScope
    total_executions: int
    final: int
    provisional: int
    ambiguous: int
    unavailable: int
    pending: int
    gross_pnl: Decimal
    trading_fees_signed: Decimal
    commission_drag_usd: Decimal
    funding_signed: Decimal
    funding_drag_usd: Decimal
    net_pnl: Decimal
    average_r: Decimal | None
    median_r: Decimal | None
    expectancy_r: Decimal | None
    profit_factor: Decimal | None
    average_win_r: Decimal | None
    average_loss_r: Decimal | None
    win_loss_ratio: Decimal | None
    maximum_drawdown_r: Decimal
    maximum_drawdown_usd: Decimal
    worst_losing_streak: int
    average_entry_slippage_bps: Decimal | None
    median_entry_slippage_bps: Decimal | None
    by_order_type: dict[str, ExecutionSegmentMetrics]
    by_user: dict[int, ExecutionSegmentMetrics]


@dataclass(frozen=True, slots=True)
class PeriodPnlPoint:
    period_key: str
    executions: int
    net_pnl: Decimal
    result_r: Decimal


@dataclass(frozen=True, slots=True)
class PortfolioMetricsReport:
    signal_rows_with_lifecycle: int
    execution_rows_with_lifecycle: int
    max_concurrent_active_signals: int
    max_executions_under_initial_risk: int
    confirmed_be_positions: int
    max_aggregate_open_risk_usd: Decimal
    max_aggregate_open_risk_percent: Decimal | None
    directional_same_side_overlap_percent: Decimal | None
    explicit_correlation_group_overlap_percent: Decimal | None
    max_signals_per_day: int
    average_signals_per_active_day: Decimal | None
    daily_net_pnl: tuple[PeriodPnlPoint, ...]
    weekly_net_pnl: tuple[PeriodPnlPoint, ...]
    monthly_net_pnl: tuple[PeriodPnlPoint, ...]


@dataclass(frozen=True, slots=True)
class StatisticsMetricsReport:
    signals: SignalMetricsReport
    executions: ExecutionMetricsReport
    portfolio: PortfolioMetricsReport


def _decimal(value: Any, *, allow_none: bool = True) -> Decimal | None:
    if value in (None, ""):
        return None if allow_none else _ZERO
    if isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _integer(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        result = value
    else:
        text = str(value).strip().replace("Z", "+00:00")
        if " " in text and "T" not in text:
            text = text.replace(" ", "T", 1)
        try:
            result = datetime.fromisoformat(text)
        except ValueError:
            return None
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def _text(value: Any, default: str = "unknown") -> str:
    result = str(value or "").strip()
    return result if result else default


def _percent(numerator: Decimal | int, denominator: Decimal | int) -> Decimal | None:
    den = Decimal(str(denominator))
    if den <= 0:
        return None
    return Decimal(str(numerator)) * _HUNDRED / den


def _mean(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, _ZERO) / Decimal(len(values))


def _median(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    return Decimal(median(values))


def _scope(total: int, eligible: int, reasons: Counter[str]) -> MetricScope:
    return MetricScope(
        total_rows=total,
        eligible_rows=eligible,
        excluded_rows=max(0, total - eligible),
        completeness_percent=_percent(eligible, total) or _ZERO,
        exclusion_reasons=dict(sorted(reasons.items())),
    )


def _event_index(
    event_rows: Iterable[Mapping[str, Any]],
) -> dict[int, dict[tuple[str, int], datetime]]:
    indexed: dict[int, dict[tuple[str, int], datetime]] = defaultdict(dict)
    ordered = sorted(
        (dict(row) for row in event_rows),
        key=lambda row: (
            _integer(row.get("signal_id")),
            _datetime(row.get("observed_at")) or datetime.min.replace(tzinfo=timezone.utc),
            _integer(row.get("id")),
        ),
    )
    for row in ordered:
        signal_id = _integer(row.get("signal_id"))
        when = _datetime(row.get("observed_at"))
        event_type = _text(row.get("event_type"), "").upper()
        level = _integer(row.get("level_index"))
        if signal_id <= 0 or when is None or not event_type:
            continue
        indexed[signal_id].setdefault((event_type, level), when)
    return indexed


def _signal_exclusion_reason(row: Mapping[str, Any]) -> str | None:
    status = _text(row.get("status"), "").lower()
    if status not in _SIGNAL_TERMINAL:
        return "not_terminal"
    if _integer(row.get("needs_recovery")):
        return "needs_recovery"
    quality = _text(row.get("data_quality_status"), "").lower()
    if quality in _BAD_QUALITY:
        return f"quality_{quality}"
    recovery_status = _text(row.get("recovery_status"), "").lower()
    if recovery_status in {"pending", "required", "unresolved", "ambiguous"}:
        return "recovery_unresolved"
    if _text(row.get("ambiguous_reason"), ""):
        return "ambiguous_reason"
    activated = _datetime(row.get("activated_at"))
    completed = _datetime(row.get("completed_at"))
    if activated is None:
        return "missing_activated_at"
    if completed is None:
        return "missing_completed_at"
    if completed < activated:
        return "negative_duration"
    return None


def _segment_metrics(rows: Sequence[Mapping[str, Any]]) -> SignalSegmentMetrics:
    eligible = [row for row in rows if _signal_exclusion_reason(row) is None]
    tp1 = sum(_integer(row.get("max_tp_index")) >= 1 for row in eligible)
    stop0 = sum(
        _text(row.get("status"), "").lower() == "completed_stop"
        and _integer(row.get("max_tp_index")) <= 0
        for row in eligible
    )
    return SignalSegmentMetrics(
        total=len(rows),
        eligible_completed=len(eligible),
        tp1_hits=tp1,
        stop_before_tp1=stop0,
        tp1_hit_rate_percent=_percent(tp1, len(eligible)),
    )


def _segments(
    rows: Sequence[Mapping[str, Any]],
    key: str,
    *,
    minimum_eligible: int = 1,
) -> dict[str, SignalSegmentMetrics]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[_text(row.get(key))].append(row)
    result: dict[str, SignalSegmentMetrics] = {}
    for name, items in sorted(grouped.items()):
        metrics = _segment_metrics(items)
        # Segments must meet the sample threshold with complete terminal
        # trajectories, not merely with active or unresolved rows.
        if metrics.eligible_completed >= minimum_eligible:
            result[name] = metrics
    return result


def calculate_signal_metrics(
    signal_rows: Iterable[Mapping[str, Any]],
    event_rows: Iterable[Mapping[str, Any]] = (),
    *,
    minimum_symbol_sample: int = 20,
) -> SignalMetricsReport:
    rows = sorted(
        (dict(row) for row in signal_rows),
        key=lambda row: (
            _datetime(row.get("published_at")) or datetime.min.replace(tzinfo=timezone.utc),
            _integer(row.get("id")),
        ),
    )
    events = _event_index(event_rows)
    reasons: Counter[str] = Counter()
    eligible: list[dict[str, Any]] = []
    for row in rows:
        reason = _signal_exclusion_reason(row)
        if reason:
            reasons[reason] += 1
        else:
            eligible.append(row)

    status_counts = Counter(_text(row.get("status"), "unknown").lower() for row in rows)
    tp_counts = {index: sum(_integer(row.get("max_tp_index")) >= index for row in eligible) for index in range(1, 5)}
    max_tps = [Decimal(_integer(row.get("max_tp_index"))) for row in eligible]
    time_to_tp1: list[Decimal] = []
    durations: list[Decimal] = []
    stop_flags: list[bool] = []
    stop0_flags: list[bool] = []
    be_counts = Counter()

    for row in eligible:
        signal_id = _integer(row.get("id"))
        activated = _datetime(row.get("activated_at"))
        completed = _datetime(row.get("completed_at"))
        if activated is not None and completed is not None:
            durations.append(Decimal(str((completed - activated).total_seconds())))
        if _integer(row.get("max_tp_index")) >= 1 and activated is not None:
            tp1_at = events.get(signal_id, {}).get(("TP", 1))
            if tp1_at is not None and tp1_at >= activated:
                time_to_tp1.append(Decimal(str((tp1_at - activated).total_seconds())))
        status = _text(row.get("status"), "").lower()
        max_tp = _integer(row.get("max_tp_index"))
        stop_flags.append(status == "completed_stop")
        stop0_flags.append(status == "completed_stop" and max_tp <= 0)
        if status == "completed_be":
            if max_tp <= 1:
                be_counts[1] += 1
            elif max_tp == 2:
                be_counts[2] += 1
            else:
                be_counts[3] += 1

    def longest(flags: Sequence[bool]) -> int:
        best = current = 0
        for flag in flags:
            current = current + 1 if flag else 0
            best = max(best, current)
        return best

    completed = sum(status in _SIGNAL_TERMINAL for status in status_counts.elements())
    return SignalMetricsReport(
        scope=_scope(len(rows), len(eligible), reasons),
        total_unique_signals=len(rows),
        completed=completed,
        active=status_counts.get("active", 0),
        waiting_entry=status_counts.get("waiting_entry", 0),
        needs_recovery=sum(bool(_integer(row.get("needs_recovery"))) for row in rows),
        ambiguous=status_counts.get("ambiguous", 0)
        + sum(bool(_text(row.get("ambiguous_reason"), "")) for row in rows if _text(row.get("status"), "").lower() != "ambiguous"),
        tp_hit_counts=tp_counts,
        tp_hit_rates_percent={index: _percent(count, len(eligible)) for index, count in tp_counts.items()},
        stop_before_tp1=sum(stop0_flags),
        be_after_tp1=be_counts[1],
        be_after_tp2=be_counts[2],
        be_after_tp3_plus=be_counts[3],
        average_max_tp_index=_mean(max_tps),
        median_max_tp_index=_median(max_tps),
        average_time_to_tp1_seconds=_mean(time_to_tp1),
        median_time_to_tp1_seconds=_median(time_to_tp1),
        average_duration_seconds=_mean(durations),
        median_duration_seconds=_median(durations),
        worst_stop_streak=longest(stop_flags),
        worst_stop_before_tp1_streak=longest(stop0_flags),
        complete_trajectory_share_percent=_percent(len(eligible), len(rows)) or _ZERO,
        by_side=_segments(rows, "side"),
        by_strategy=_segments(rows, "strategy"),
        by_timeframe=_segments(rows, "timeframe"),
        by_symbol=_segments(rows, "symbol", minimum_eligible=max(1, int(minimum_symbol_sample))),
    )


def _execution_exclusion_reason(row: Mapping[str, Any]) -> str | None:
    if _text(row.get("market_event_review_status"), "clear").lower() == "manual_review":
        return _text(row.get("market_event_exclusion_reason"), "market_event_manual_review") or "market_event_manual_review"
    state = _text(row.get("financial_state"), "PENDING").upper()
    if state != _FINAL:
        return f"state_{state.lower()}"
    if _text(row.get("data_quality_status"), "").lower() in _BAD_QUALITY:
        return "bad_data_quality"
    if _text(row.get("linkage_status"), "").lower() not in {"linked_exact", "exact"}:
        return "not_exact_linkage"
    net = _decimal(row.get("net_pnl"))
    result_r = _decimal(row.get("result_r"))
    risk = _decimal(row.get("initial_price_risk_usd"))
    if net is None:
        return "missing_net_pnl"
    if result_r is None:
        return "missing_result_r"
    if risk is None or risk <= 0:
        return "invalid_initial_risk"
    if _datetime(row.get("last_exit_fill_at") or row.get("finalized_at")) is None:
        return "missing_terminal_time"
    return None


def _drawdown(values: Sequence[Decimal]) -> Decimal:
    peak = equity = _ZERO
    worst = _ZERO
    for value in values:
        equity += value
        peak = max(peak, equity)
        worst = max(worst, peak - equity)
    return worst


def _losing_streak(values: Sequence[Decimal]) -> int:
    best = current = 0
    for value in values:
        current = current + 1 if value < 0 else 0
        best = max(best, current)
    return best


def _execution_segment(rows: Sequence[Mapping[str, Any]]) -> ExecutionSegmentMetrics:
    eligible = [row for row in rows if _execution_exclusion_reason(row) is None]
    net = sum((_decimal(row.get("net_pnl")) or _ZERO for row in eligible), _ZERO)
    r_values = [_decimal(row.get("result_r")) or _ZERO for row in eligible]
    return ExecutionSegmentMetrics(
        total=len(rows),
        final=len(eligible),
        net_pnl=net,
        expectancy_r=_mean(r_values),
    )


def calculate_execution_metrics(
    execution_rows: Iterable[Mapping[str, Any]],
    *,
    include_user_breakdown: bool = False,
) -> ExecutionMetricsReport:
    rows = sorted(
        (dict(row) for row in execution_rows),
        key=lambda row: (
            _datetime(row.get("last_exit_fill_at") or row.get("finalized_at"))
            or datetime.max.replace(tzinfo=timezone.utc),
            _integer(row.get("execution_id") or row.get("id")),
        ),
    )
    reasons: Counter[str] = Counter()
    eligible: list[dict[str, Any]] = []
    for row in rows:
        reason = _execution_exclusion_reason(row)
        if reason:
            reasons[reason] += 1
        else:
            eligible.append(row)

    states = Counter(_text(row.get("financial_state"), "PENDING").upper() for row in rows)
    gross = sum((_decimal(row.get("exchange_gross_pnl")) or _ZERO for row in eligible), _ZERO)
    fees = sum((_decimal(row.get("trading_fee_signed")) or _ZERO for row in eligible), _ZERO)
    funding = sum((_decimal(row.get("funding_signed")) or _ZERO for row in eligible), _ZERO)
    net = sum((_decimal(row.get("net_pnl")) or _ZERO for row in eligible), _ZERO)
    r_values = [_decimal(row.get("result_r")) or _ZERO for row in eligible]
    net_values = [_decimal(row.get("net_pnl")) or _ZERO for row in eligible]
    wins = [value for value in r_values if value > 0]
    losses = [value for value in r_values if value < 0]
    gross_profit = sum(wins, _ZERO)
    gross_loss = -sum(losses, _ZERO)
    avg_win = _mean(wins)
    avg_loss = _mean(losses)
    slippage = [value for row in eligible if (value := _decimal(row.get("entry_slippage_bps"))) is not None]

    by_type_group: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    by_user_group: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        by_type_group[_text(row.get("entry_order_type"), "UNKNOWN").upper()].append(row)
        by_user_group[_integer(row.get("user_id"))].append(row)

    return ExecutionMetricsReport(
        scope=_scope(len(rows), len(eligible), reasons),
        total_executions=len(rows),
        final=states.get("FINAL", 0),
        provisional=states.get("PROVISIONAL", 0),
        ambiguous=states.get("AMBIGUOUS", 0),
        unavailable=states.get("UNAVAILABLE", 0),
        pending=states.get("PENDING", 0),
        gross_pnl=gross,
        trading_fees_signed=fees,
        commission_drag_usd=max(_ZERO, -fees),
        funding_signed=funding,
        funding_drag_usd=max(_ZERO, -funding),
        net_pnl=net,
        average_r=_mean(r_values),
        median_r=_median(r_values),
        expectancy_r=_mean(r_values),
        profit_factor=(gross_profit / gross_loss if gross_loss > 0 else None),
        average_win_r=avg_win,
        average_loss_r=avg_loss,
        win_loss_ratio=(avg_win / abs(avg_loss) if avg_win is not None and avg_loss not in (None, _ZERO) else None),
        maximum_drawdown_r=_drawdown(r_values),
        maximum_drawdown_usd=_drawdown(net_values),
        worst_losing_streak=_losing_streak(r_values),
        average_entry_slippage_bps=_mean(slippage),
        median_entry_slippage_bps=_median(slippage),
        by_order_type={name: _execution_segment(items) for name, items in sorted(by_type_group.items())},
        by_user=(
            {user_id: _execution_segment(items) for user_id, items in sorted(by_user_group.items()) if user_id > 0}
            if include_user_breakdown
            else {}
        ),
    )


def _max_concurrent(intervals: Sequence[tuple[datetime, datetime]]) -> int:
    events: list[tuple[datetime, int]] = []
    for start, end in intervals:
        if end <= start:
            continue
        events.append((start, 1))
        events.append((end, -1))
    current = best = 0
    # Closing before opening at identical timestamps prevents artificial overlap.
    for _, delta in sorted(events, key=lambda item: (item[0], item[1])):
        current += delta
        best = max(best, current)
    return best


def _max_weighted_overlap(
    intervals: Sequence[tuple[datetime, datetime, Decimal, Decimal | None]],
) -> tuple[Decimal, Decimal | None]:
    events: list[tuple[datetime, int, Decimal, Decimal | None]] = []
    for start, end, usd, percent in intervals:
        if end <= start or usd <= 0:
            continue
        events.append((start, 1, usd, percent))
        events.append((end, -1, usd, percent))
    current_usd = max_usd = _ZERO
    current_percent = max_percent = _ZERO
    has_percent = False
    for _, delta, usd, percent in sorted(events, key=lambda item: (item[0], item[1])):
        current_usd += Decimal(delta) * usd
        max_usd = max(max_usd, current_usd)
        if percent is not None:
            has_percent = True
            current_percent += Decimal(delta) * percent
            max_percent = max(max_percent, current_percent)
    return max_usd, max_percent if has_percent else None


def _overlap_pair_shares(
    rows: Sequence[Mapping[str, Any]],
    *,
    analysis_as_of: datetime,
) -> tuple[Decimal | None, Decimal | None]:
    lifecycle: list[tuple[datetime, datetime, str, str]] = []
    for row in rows:
        start = _datetime(row.get("activated_at"))
        end = _datetime(row.get("completed_at")) or analysis_as_of
        if start is None or end < start:
            continue
        lifecycle.append((start, end, _text(row.get("side"), "").lower(), _text(row.get("correlation_group"), "")))
    overlaps = same_side = explicit_group = explicit_comparable = 0
    for index, left in enumerate(lifecycle):
        for right in lifecycle[index + 1 :]:
            if max(left[0], right[0]) >= min(left[1], right[1]):
                continue
            overlaps += 1
            same_side += int(bool(left[2]) and left[2] == right[2])
            if left[3] and right[3]:
                explicit_comparable += 1
                explicit_group += int(left[3] == right[3])
    if overlaps <= 0:
        return None, None
    explicit_share = (
        _percent(explicit_group, explicit_comparable)
        if explicit_comparable > 0
        else None
    )
    return _percent(same_side, overlaps), explicit_share


def _period_key(when: datetime, cadence: str) -> str:
    if cadence == "daily":
        return when.date().isoformat()
    if cadence == "weekly":
        year, week, _ = when.isocalendar()
        return f"{year}-W{week:02d}"
    if cadence == "monthly":
        return f"{when.year:04d}-{when.month:02d}"
    raise ValueError("unsupported cadence")


def _pnl_sequence(rows: Sequence[Mapping[str, Any]], cadence: str) -> tuple[PeriodPnlPoint, ...]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        if _execution_exclusion_reason(row) is not None:
            continue
        when = _datetime(row.get("last_exit_fill_at") or row.get("finalized_at"))
        if when is None:
            continue
        grouped[_period_key(when, cadence)].append(row)
    return tuple(
        PeriodPnlPoint(
            period_key=key,
            executions=len(items),
            net_pnl=sum((_decimal(row.get("net_pnl")) or _ZERO for row in items), _ZERO),
            result_r=sum((_decimal(row.get("result_r")) or _ZERO for row in items), _ZERO),
        )
        for key, items in sorted(grouped.items())
    )


def calculate_portfolio_metrics(
    signal_rows: Iterable[Mapping[str, Any]],
    execution_rows: Iterable[Mapping[str, Any]],
    *,
    analysis_as_of: datetime | None = None,
    confirmed_be_frees_initial_risk: bool = True,
) -> PortfolioMetricsReport:
    as_of = analysis_as_of or datetime.now(timezone.utc)
    if as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=timezone.utc)
    as_of = as_of.astimezone(timezone.utc)
    signals = [dict(row) for row in signal_rows]
    executions = [dict(row) for row in execution_rows]

    signal_intervals: list[tuple[datetime, datetime]] = []
    be_by_signal: dict[int, datetime] = {}
    signal_days: Counter[str] = Counter()
    for row in signals:
        start = _datetime(row.get("activated_at"))
        end = _datetime(row.get("completed_at")) or as_of
        if start is not None and end >= start:
            signal_intervals.append((start, end))
            signal_days[start.date().isoformat()] += 1
        be_at = _datetime(row.get("be_armed_at"))
        signal_id = _integer(row.get("id"))
        if signal_id > 0 and be_at is not None:
            be_by_signal[signal_id] = be_at

    execution_intervals: list[tuple[datetime, datetime]] = []
    risk_intervals: list[tuple[datetime, datetime, Decimal, Decimal | None]] = []
    confirmed_be_positions = 0
    for row in executions:
        start = _datetime(row.get("first_entry_fill_at"))
        end = _datetime(row.get("last_exit_fill_at") or row.get("finalized_at")) or as_of
        risk = _decimal(row.get("initial_price_risk_usd"))
        if start is None or end < start or risk is None or risk <= 0:
            continue
        signal_id = _integer(row.get("analytics_signal_id"))
        be_at = be_by_signal.get(signal_id)
        risk_end = end
        if be_at is not None and start <= be_at <= end:
            confirmed_be_positions += 1
            if confirmed_be_frees_initial_risk:
                risk_end = be_at
        execution_intervals.append((start, risk_end))
        equity = _decimal(row.get("equity_snapshot_usd"))
        risk_percent = risk / equity * _HUNDRED if equity is not None and equity > 0 else None
        risk_intervals.append((start, risk_end, risk, risk_percent))

    max_risk_usd, max_risk_percent = _max_weighted_overlap(risk_intervals)
    same_side_overlap, correlation_overlap = _overlap_pair_shares(signals, analysis_as_of=as_of)
    active_days = len(signal_days)
    eligible_exec = [row for row in executions if _execution_exclusion_reason(row) is None]
    return PortfolioMetricsReport(
        signal_rows_with_lifecycle=len(signal_intervals),
        execution_rows_with_lifecycle=len(execution_intervals),
        max_concurrent_active_signals=_max_concurrent(signal_intervals),
        max_executions_under_initial_risk=_max_concurrent(execution_intervals),
        confirmed_be_positions=confirmed_be_positions,
        max_aggregate_open_risk_usd=max_risk_usd,
        max_aggregate_open_risk_percent=max_risk_percent,
        directional_same_side_overlap_percent=same_side_overlap,
        explicit_correlation_group_overlap_percent=correlation_overlap,
        max_signals_per_day=max(signal_days.values(), default=0),
        average_signals_per_active_day=(Decimal(sum(signal_days.values())) / Decimal(active_days) if active_days else None),
        daily_net_pnl=_pnl_sequence(eligible_exec, "daily"),
        weekly_net_pnl=_pnl_sequence(eligible_exec, "weekly"),
        monthly_net_pnl=_pnl_sequence(eligible_exec, "monthly"),
    )


def calculate_statistics_metrics(
    signal_rows: Iterable[Mapping[str, Any]],
    event_rows: Iterable[Mapping[str, Any]],
    execution_rows: Iterable[Mapping[str, Any]],
    *,
    minimum_symbol_sample: int = 20,
    include_user_breakdown: bool = False,
    analysis_as_of: datetime | None = None,
) -> StatisticsMetricsReport:
    signals = [dict(row) for row in signal_rows]
    events = [dict(row) for row in event_rows]
    executions = [dict(row) for row in execution_rows]
    return StatisticsMetricsReport(
        signals=calculate_signal_metrics(signals, events, minimum_symbol_sample=minimum_symbol_sample),
        executions=calculate_execution_metrics(executions, include_user_breakdown=include_user_breakdown),
        portfolio=calculate_portfolio_metrics(signals, executions, analysis_as_of=analysis_as_of),
    )


def parse_json_decimal_list(value: Any) -> tuple[Decimal, ...]:
    """Strict helper shared with the simulation engine and export layer."""

    if isinstance(value, (list, tuple)):
        raw = list(value)
    else:
        try:
            raw = json.loads(str(value or "[]"))
        except (TypeError, ValueError, json.JSONDecodeError):
            return ()
    if not isinstance(raw, list):
        return ()
    result: list[Decimal] = []
    for item in raw:
        parsed = _decimal(item)
        if parsed is None:
            return ()
        result.append(parsed)
    return tuple(result)
