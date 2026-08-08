"""Fail-closed TP/BE/risk and Monte Carlo simulations for statistics-v2.

Simulations operate on unique signal trajectories.  User executions are used
only to derive a per-signal median signed cost in R (fees + funding), so one
source signal is never counted once per account.  Active, recovery, ambiguous
and censored trajectories are not silently promoted to wins.
"""

from __future__ import annotations

import heapq
import random
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from statistics import median
from typing import Any, Iterable, Mapping, Sequence

from app.services.statistics_metrics import MetricScope, parse_json_decimal_list


_ZERO = Decimal("0")
_HUNDRED = Decimal("100")
_TERMINAL = frozenset({"completed_stop", "completed_be", "completed_tp"})
_BAD_QUALITY = frozenset({"ambiguous", "unavailable", "needs_recovery", "quarantined"})


@dataclass(frozen=True, slots=True)
class TakeProfitScheme:
    name: str
    allocations_percent: tuple[Decimal, ...] = ()
    use_signal_allocations: bool = False

    def __post_init__(self) -> None:
        if self.use_signal_allocations:
            if self.allocations_percent:
                raise ValueError("signal-allocation scheme cannot define fixed allocations")
            return
        if not self.allocations_percent:
            raise ValueError("fixed scheme requires allocations")
        if any(value < 0 for value in self.allocations_percent):
            raise ValueError("allocations cannot be negative")
        if sum(self.allocations_percent, _ZERO) != _HUNDRED:
            raise ValueError("allocations must sum to 100")


@dataclass(frozen=True, slots=True)
class BreakevenPolicy:
    name: str
    trigger_tp_index: int | None

    def __post_init__(self) -> None:
        if self.trigger_tp_index is not None and self.trigger_tp_index <= 0:
            raise ValueError("BE trigger must be positive or None")


@dataclass(frozen=True, slots=True)
class SignalSimulationOutcome:
    signal_id: int
    opened_at: datetime
    closed_at: datetime
    gross_r: Decimal
    cost_r: Decimal
    net_r: Decimal
    be_release_at: datetime | None
    max_tp_index: int
    terminal_status: str


@dataclass(frozen=True, slots=True)
class SchemeSimulationResult:
    scheme_name: str
    be_policy_name: str
    scope: MetricScope
    total_r: Decimal
    average_r: Decimal | None
    median_r: Decimal | None
    win_rate_percent: Decimal | None
    profit_factor: Decimal | None
    maximum_drawdown_r: Decimal
    worst_losing_streak: int
    cost_coverage_percent: Decimal
    outcomes: tuple[SignalSimulationOutcome, ...]


@dataclass(frozen=True, slots=True)
class ValidationSimulationResult:
    scheme_name: str
    be_policy_name: str
    split_at_signal_index: int
    hypothesis: SchemeSimulationResult
    validation: SchemeSimulationResult
    validation_ready: bool


@dataclass(frozen=True, slots=True)
class RiskScenario:
    risk_percent: Decimal
    open_risk_limit_percent: Decimal
    max_trades_under_initial_risk: int
    confirmed_be_frees_risk_slot: bool = True

    def __post_init__(self) -> None:
        if self.risk_percent <= 0:
            raise ValueError("risk_percent must be positive")
        if self.open_risk_limit_percent <= 0:
            raise ValueError("open_risk_limit_percent must be positive")
        if self.max_trades_under_initial_risk <= 0:
            raise ValueError("max_trades_under_initial_risk must be positive")


@dataclass(frozen=True, slots=True)
class RiskSimulationResult:
    scenario: RiskScenario
    starting_equity: Decimal
    ending_equity: Decimal
    return_percent: Decimal
    maximum_drawdown_percent: Decimal
    accepted_signals: int
    skipped_open_risk_limit: int
    skipped_max_trades: int
    max_open_risk_percent_observed: Decimal
    max_trades_under_risk_observed: int
    worst_losing_streak: int
    max_accepted_signals_per_day: int


@dataclass(frozen=True, slots=True)
class MonteCarloResult:
    status: str
    source_sample_size: int
    simulated_trade_count: int
    iterations: int
    seed: int
    ending_return_percentiles: dict[str, Decimal]
    max_drawdown_percentiles: dict[str, Decimal]
    losing_streak_percentiles: dict[str, Decimal]
    drawdown_probability_percent: dict[str, Decimal]
    reason: str = ""


DEFAULT_TP_SCHEMES = (
    TakeProfitScheme("tp1_100", (Decimal("100"), Decimal("0"), Decimal("0"), Decimal("0"))),
    TakeProfitScheme("weighted_current", use_signal_allocations=True),
    TakeProfitScheme("70_15_10_5", (Decimal("70"), Decimal("15"), Decimal("10"), Decimal("5"))),
    TakeProfitScheme("65_20_10_5", (Decimal("65"), Decimal("20"), Decimal("10"), Decimal("5"))),
)

DEFAULT_BE_POLICIES = (
    BreakevenPolicy("be_after_tp1", 1),
    BreakevenPolicy("be_after_tp2", 2),
    BreakevenPolicy("no_be", None),
)

DEFAULT_RISK_SCENARIOS = tuple(
    RiskScenario(Decimal(risk), Decimal(limit), trades)
    for risk in ("0.25", "0.33", "0.5", "0.75", "1")
    for limit in ("5", "7.5", "10", "12.5", "15")
    for trades in (10, 20, 30)
)


def _decimal(value: Any) -> Decimal | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


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


def _text(value: Any) -> str:
    return str(value or "").strip()


def _percent(numerator: Decimal | int, denominator: Decimal | int) -> Decimal | None:
    den = Decimal(str(denominator))
    if den <= 0:
        return None
    return Decimal(str(numerator)) * _HUNDRED / den


def _mean(values: Sequence[Decimal]) -> Decimal | None:
    return sum(values, _ZERO) / Decimal(len(values)) if values else None


def _median(values: Sequence[Decimal]) -> Decimal | None:
    return Decimal(median(values)) if values else None


def _drawdown(values: Sequence[Decimal]) -> Decimal:
    equity = peak = worst = _ZERO
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


def _scope(total: int, eligible: int, reasons: Counter[str]) -> MetricScope:
    return MetricScope(
        total_rows=total,
        eligible_rows=eligible,
        excluded_rows=max(0, total - eligible),
        completeness_percent=_percent(eligible, total) or _ZERO,
        exclusion_reasons=dict(sorted(reasons.items())),
    )


def _event_times(event_rows: Iterable[Mapping[str, Any]]) -> dict[int, dict[int, datetime]]:
    indexed: dict[int, dict[int, datetime]] = defaultdict(dict)
    ordered = sorted(
        (dict(row) for row in event_rows),
        key=lambda row: (
            _integer(row.get("signal_id")),
            _datetime(row.get("observed_at")) or datetime.max.replace(tzinfo=timezone.utc),
            _integer(row.get("id")),
        ),
    )
    for row in ordered:
        if _text(row.get("event_type")).upper() != "TP":
            continue
        signal_id = _integer(row.get("signal_id"))
        level = _integer(row.get("level_index"))
        when = _datetime(row.get("observed_at"))
        if signal_id > 0 and level > 0 and when is not None:
            indexed[signal_id].setdefault(level, when)
    return indexed


def build_signal_cost_r(
    execution_rows: Iterable[Mapping[str, Any]],
) -> dict[int, Decimal]:
    """Return one median signed fee+funding R cost per unique signal.

    A signal with executions on ten accounts still contributes one cost estimate.
    Only exact FINAL rows with a positive initial risk are admitted.
    """

    grouped: dict[int, list[Decimal]] = defaultdict(list)
    for row in execution_rows:
        if _text(row.get("financial_state")).upper() != "FINAL":
            continue
        if _text(row.get("linkage_status")).lower() not in {"linked_exact", "exact"}:
            continue
        quality = _text(row.get("data_quality_status")).lower()
        if quality in _BAD_QUALITY or _text(row.get("ambiguity_reason")):
            continue
        signal_id = _integer(row.get("analytics_signal_id"))
        risk = _decimal(row.get("initial_price_risk_usd"))
        fee = _decimal(row.get("trading_fee_signed"))
        funding = _decimal(row.get("funding_signed"))
        if signal_id <= 0 or risk is None or risk <= 0 or fee is None or funding is None:
            continue
        grouped[signal_id].append((fee + funding) / risk)
    return {signal_id: Decimal(median(values)) for signal_id, values in grouped.items() if values}


def _trajectory_reason(row: Mapping[str, Any]) -> str | None:
    status = _text(row.get("status")).lower()
    if status not in _TERMINAL:
        return "not_terminal"
    if _integer(row.get("needs_recovery")):
        return "needs_recovery"
    quality = _text(row.get("data_quality_status")).lower()
    if quality in _BAD_QUALITY:
        return f"quality_{quality}"
    recovery = _text(row.get("recovery_status")).lower()
    if recovery in {"pending", "required", "unresolved", "ambiguous"}:
        return "recovery_unresolved"
    if _text(row.get("ambiguous_reason")):
        return "ambiguous_reason"
    opened = _datetime(row.get("activated_at"))
    closed = _datetime(row.get("completed_at"))
    if opened is None or closed is None:
        return "missing_lifecycle"
    if closed < opened:
        return "negative_duration"
    side = _text(row.get("side")).lower()
    if side not in {"long", "short"}:
        return "invalid_side"
    entry = _decimal(row.get("activated_price") or row.get("entry_reference"))
    stop = _decimal(row.get("stop_price"))
    targets = parse_json_decimal_list(row.get("targets_json"))
    if entry is None or stop is None or not targets:
        return "invalid_price_plan"
    risk_distance = entry - stop if side == "long" else stop - entry
    if risk_distance <= 0:
        return "invalid_stop_direction"
    for target in targets:
        reward = target - entry if side == "long" else entry - target
        if reward <= 0:
            return "invalid_target_direction"
    return None


def _allocations(row: Mapping[str, Any], scheme: TakeProfitScheme, target_count: int) -> tuple[Decimal, ...] | None:
    values = parse_json_decimal_list(row.get("target_percents_json")) if scheme.use_signal_allocations else scheme.allocations_percent
    if not values or sum(values, _ZERO) != _HUNDRED or any(value < 0 for value in values):
        return None
    if len(values) > target_count and any(value > 0 for value in values[target_count:]):
        return None
    padded = tuple(values) + tuple(_ZERO for _ in range(max(0, target_count - len(values))))
    return padded[:target_count]


def _target_r_values(row: Mapping[str, Any]) -> tuple[Decimal, ...] | None:
    side = _text(row.get("side")).lower()
    entry = _decimal(row.get("activated_price") or row.get("entry_reference"))
    stop = _decimal(row.get("stop_price"))
    targets = parse_json_decimal_list(row.get("targets_json"))
    if entry is None or stop is None or not targets:
        return None
    risk_distance = entry - stop if side == "long" else stop - entry
    if risk_distance <= 0:
        return None
    return tuple(((target - entry) if side == "long" else (entry - target)) / risk_distance for target in targets)


def _gross_result_r(
    row: Mapping[str, Any],
    *,
    allocations: Sequence[Decimal],
    target_r: Sequence[Decimal],
    be_policy: BreakevenPolicy,
) -> tuple[Decimal | None, str | None]:
    status = _text(row.get("status")).lower()
    max_tp = max(0, _integer(row.get("max_tp_index")))
    if max_tp > len(target_r):
        return None, "max_tp_out_of_range"
    realized = _ZERO
    realized_percent = _ZERO
    for index in range(min(max_tp, len(target_r))):
        weight = allocations[index] / _HUNDRED
        realized += weight * target_r[index]
        realized_percent += allocations[index]
    residual_percent = _HUNDRED - realized_percent
    if residual_percent < 0:
        return None, "allocation_overflow"
    if residual_percent == 0:
        return realized, None

    trigger = be_policy.trigger_tp_index
    if status == "completed_tp":
        # A terminal all-target trajectory must have reached every positively
        # allocated target; otherwise the plan is inconsistent for this scheme.
        if any(allocations[index] > 0 for index in range(max_tp, len(allocations))):
            return None, "terminal_tp_before_allocated_target"
        return realized, None

    if trigger is not None and max_tp >= trigger:
        # The path later reached either entry (actual BE) or the original STOP.
        # In both cases a hypothetical stop at entry would have closed residual
        # quantity at 0R after all observed targets up to max_tp.
        return realized, None

    if status == "completed_stop":
        return realized - residual_percent / _HUNDRED, None

    if status == "completed_be":
        # A later/no-BE counterfactual is censored: after the observed return to
        # entry we do not know whether price would later hit another TP or STOP.
        return None, "counterfactual_censored_after_be"

    return None, "unsupported_terminal_status"


def simulate_take_profit_scheme(
    signal_rows: Iterable[Mapping[str, Any]],
    event_rows: Iterable[Mapping[str, Any]],
    execution_rows: Iterable[Mapping[str, Any]],
    *,
    scheme: TakeProfitScheme,
    be_policy: BreakevenPolicy,
    require_costs: bool = True,
) -> SchemeSimulationResult:
    rows = sorted(
        (dict(row) for row in signal_rows),
        key=lambda row: (
            _datetime(row.get("activated_at")) or datetime.max.replace(tzinfo=timezone.utc),
            _integer(row.get("id")),
        ),
    )
    tp_times = _event_times(event_rows)
    costs = build_signal_cost_r(execution_rows)
    reasons: Counter[str] = Counter()
    outcomes: list[SignalSimulationOutcome] = []
    cost_eligible = 0

    for row in rows:
        reason = _trajectory_reason(row)
        if reason:
            reasons[reason] += 1
            continue
        signal_id = _integer(row.get("id"))
        target_r = _target_r_values(row)
        assert target_r is not None
        allocations = _allocations(row, scheme, len(target_r))
        if allocations is None:
            reasons["invalid_scheme_allocation_for_signal"] += 1
            continue
        gross_r, reason = _gross_result_r(row, allocations=allocations, target_r=target_r, be_policy=be_policy)
        if reason or gross_r is None:
            reasons[reason or "unknown_simulation_error"] += 1
            continue
        cost_r = costs.get(signal_id)
        if cost_r is None:
            if require_costs:
                reasons["missing_exact_financial_cost"] += 1
                continue
            cost_r = _ZERO
        else:
            cost_eligible += 1
        opened = _datetime(row.get("activated_at"))
        closed = _datetime(row.get("completed_at"))
        assert opened is not None and closed is not None
        trigger = be_policy.trigger_tp_index
        be_release = None
        if trigger is not None and _integer(row.get("max_tp_index")) >= trigger:
            be_release = tp_times.get(signal_id, {}).get(trigger)
            if be_release is not None and not (opened <= be_release <= closed):
                be_release = None
        outcomes.append(
            SignalSimulationOutcome(
                signal_id=signal_id,
                opened_at=opened,
                closed_at=closed,
                gross_r=gross_r,
                cost_r=cost_r,
                net_r=gross_r + cost_r,
                be_release_at=be_release,
                max_tp_index=_integer(row.get("max_tp_index")),
                terminal_status=_text(row.get("status")).lower(),
            )
        )

    values = [item.net_r for item in outcomes]
    wins = [value for value in values if value > 0]
    losses = [value for value in values if value < 0]
    gross_profit = sum(wins, _ZERO)
    gross_loss = -sum(losses, _ZERO)
    return SchemeSimulationResult(
        scheme_name=scheme.name,
        be_policy_name=be_policy.name,
        scope=_scope(len(rows), len(outcomes), reasons),
        total_r=sum(values, _ZERO),
        average_r=_mean(values),
        median_r=_median(values),
        win_rate_percent=_percent(len(wins), len(values)),
        profit_factor=(gross_profit / gross_loss if gross_loss > 0 else None),
        maximum_drawdown_r=_drawdown(values),
        worst_losing_streak=_losing_streak(values),
        cost_coverage_percent=_percent(cost_eligible, len(outcomes)) or _ZERO,
        outcomes=tuple(outcomes),
    )


def simulate_with_chronological_holdout(
    signal_rows: Iterable[Mapping[str, Any]],
    event_rows: Iterable[Mapping[str, Any]],
    execution_rows: Iterable[Mapping[str, Any]],
    *,
    scheme: TakeProfitScheme,
    be_policy: BreakevenPolicy,
    validation_fraction: Decimal = Decimal("0.30"),
    minimum_validation_signals: int = 30,
    require_costs: bool = True,
) -> ValidationSimulationResult:
    if not (Decimal("0") < validation_fraction < Decimal("1")):
        raise ValueError("validation_fraction must be between 0 and 1")
    all_rows = sorted(
        (dict(row) for row in signal_rows),
        key=lambda row: (
            _datetime(row.get("activated_at")) or datetime.max.replace(tzinfo=timezone.utc),
            _integer(row.get("id")),
        ),
    )
    # Split only complete terminal trajectories. Active/recovery rows must not
    # shift the validation boundary or be silently treated as observations.
    rows = [row for row in all_rows if _trajectory_reason(row) is None]
    split = int(Decimal(len(rows)) * (Decimal("1") - validation_fraction))
    split = max(0, min(len(rows), split))
    hypothesis_rows = rows[:split]
    validation_rows = rows[split:]
    signal_ids_h = {_integer(row.get("id")) for row in hypothesis_rows}
    signal_ids_v = {_integer(row.get("id")) for row in validation_rows}
    events = [dict(row) for row in event_rows]
    executions = [dict(row) for row in execution_rows]
    hypothesis = simulate_take_profit_scheme(
        hypothesis_rows,
        [row for row in events if _integer(row.get("signal_id")) in signal_ids_h],
        [row for row in executions if _integer(row.get("analytics_signal_id")) in signal_ids_h],
        scheme=scheme,
        be_policy=be_policy,
        require_costs=require_costs,
    )
    validation = simulate_take_profit_scheme(
        validation_rows,
        [row for row in events if _integer(row.get("signal_id")) in signal_ids_v],
        [row for row in executions if _integer(row.get("analytics_signal_id")) in signal_ids_v],
        scheme=scheme,
        be_policy=be_policy,
        require_costs=require_costs,
    )
    return ValidationSimulationResult(
        scheme_name=scheme.name,
        be_policy_name=be_policy.name,
        split_at_signal_index=split,
        hypothesis=hypothesis,
        validation=validation,
        validation_ready=validation.scope.eligible_rows >= max(1, int(minimum_validation_signals)),
    )


def simulate_risk_scenario(
    outcomes: Iterable[SignalSimulationOutcome],
    *,
    scenario: RiskScenario,
    starting_equity: Decimal = Decimal("100"),
) -> RiskSimulationResult:
    if starting_equity <= 0:
        raise ValueError("starting_equity must be positive")
    ordered = sorted(outcomes, key=lambda item: (item.opened_at, item.signal_id))
    signal_ids = [item.signal_id for item in ordered]
    if len(signal_ids) != len(set(signal_ids)):
        raise ValueError("risk simulation requires unique signal outcomes")
    equity = starting_equity
    peak_equity = equity
    max_drawdown = _ZERO
    open_risk = _ZERO
    risk_slots = 0
    max_open_risk_percent = _ZERO
    max_slots = 0
    skipped_cap = skipped_slots = accepted = 0
    pnl_sequence: list[Decimal] = []
    accepted_days: Counter[str] = Counter()
    positions: dict[int, dict[str, Any]] = {}
    queue: list[tuple[datetime, int, int, str]] = []

    def process_until(cutoff: datetime | None) -> None:
        nonlocal equity, peak_equity, max_drawdown, open_risk, risk_slots
        while queue and (cutoff is None or queue[0][0] <= cutoff):
            _, _, signal_id, kind = heapq.heappop(queue)
            position = positions.get(signal_id)
            if position is None:
                continue
            if kind == "release":
                if not position["risk_released"]:
                    open_risk -= position["risk_amount"]
                    risk_slots -= 1
                    position["risk_released"] = True
                continue
            if kind != "close" or position["closed"]:
                continue
            if not position["risk_released"]:
                open_risk -= position["risk_amount"]
                risk_slots -= 1
                position["risk_released"] = True
            pnl = position["risk_amount"] * position["net_r"]
            equity += pnl
            pnl_sequence.append(pnl)
            peak_equity = max(peak_equity, equity)
            if peak_equity > 0:
                max_drawdown = max(max_drawdown, (peak_equity - equity) / peak_equity * _HUNDRED)
            position["closed"] = True

    for outcome in ordered:
        process_until(outcome.opened_at)
        prospective_risk = equity * scenario.risk_percent / _HUNDRED
        if prospective_risk <= 0:
            skipped_cap += 1
            continue
        if risk_slots >= scenario.max_trades_under_initial_risk:
            skipped_slots += 1
            continue
        prospective_percent = (open_risk + prospective_risk) / equity * _HUNDRED if equity > 0 else Decimal("Infinity")
        if prospective_percent > scenario.open_risk_limit_percent:
            skipped_cap += 1
            continue
        accepted += 1
        accepted_days[outcome.opened_at.date().isoformat()] += 1
        open_risk += prospective_risk
        risk_slots += 1
        positions[outcome.signal_id] = {
            "risk_amount": prospective_risk,
            "net_r": outcome.net_r,
            "risk_released": False,
            "closed": False,
        }
        max_slots = max(max_slots, risk_slots)
        if equity > 0:
            max_open_risk_percent = max(max_open_risk_percent, open_risk / equity * _HUNDRED)
        heapq.heappush(queue, (outcome.closed_at, 0, outcome.signal_id, "close"))
        if (
            scenario.confirmed_be_frees_risk_slot
            and outcome.be_release_at is not None
            and outcome.opened_at <= outcome.be_release_at < outcome.closed_at
        ):
            heapq.heappush(queue, (outcome.be_release_at, 1, outcome.signal_id, "release"))

    process_until(None)
    return RiskSimulationResult(
        scenario=scenario,
        starting_equity=starting_equity,
        ending_equity=equity,
        return_percent=(equity / starting_equity - Decimal("1")) * _HUNDRED,
        maximum_drawdown_percent=max_drawdown,
        accepted_signals=accepted,
        skipped_open_risk_limit=skipped_cap,
        skipped_max_trades=skipped_slots,
        max_open_risk_percent_observed=max_open_risk_percent,
        max_trades_under_risk_observed=max_slots,
        worst_losing_streak=_losing_streak(pnl_sequence),
        max_accepted_signals_per_day=max(accepted_days.values(), default=0),
    )


def _percentile(values: Sequence[Decimal], percentile: Decimal) -> Decimal:
    if not values:
        return _ZERO
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (Decimal(len(ordered) - 1) * percentile / _HUNDRED)
    lower = int(position)
    upper = min(len(ordered) - 1, lower + 1)
    fraction = position - Decimal(lower)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def monte_carlo_bootstrap(
    result_r_values: Iterable[Decimal | int | float | str],
    *,
    risk_percent: Decimal = Decimal("0.5"),
    simulated_trade_count: int = 100,
    iterations: int = 2000,
    seed: int = 6071606,
    minimum_source_sample: int = 100,
    drawdown_thresholds_percent: Sequence[Decimal] = (Decimal("5"), Decimal("10"), Decimal("15")),
) -> MonteCarloResult:
    values = [value for raw in result_r_values if (value := _decimal(raw)) is not None]
    if len(values) < max(1, int(minimum_source_sample)):
        return MonteCarloResult(
            status="insufficient_sample",
            source_sample_size=len(values),
            simulated_trade_count=max(0, int(simulated_trade_count)),
            iterations=0,
            seed=int(seed),
            ending_return_percentiles={},
            max_drawdown_percentiles={},
            losing_streak_percentiles={},
            drawdown_probability_percent={},
            reason=f"requires_at_least_{max(1, int(minimum_source_sample))}_eligible_results",
        )
    if risk_percent <= 0 or simulated_trade_count <= 0 or iterations <= 0:
        raise ValueError("risk, trade count and iterations must be positive")
    if iterations > 10_000 or simulated_trade_count > 10_000:
        raise ValueError("Monte Carlo bounds exceeded")

    rng = random.Random(int(seed))
    ending_returns: list[Decimal] = []
    drawdowns: list[Decimal] = []
    streaks: list[Decimal] = []
    threshold_hits = Counter()
    for _ in range(int(iterations)):
        equity = peak = Decimal("100")
        worst_dd = _ZERO
        worst_streak = current_streak = 0
        for _trade in range(int(simulated_trade_count)):
            if equity <= 0:
                # Once the simulated account is ruined it cannot recover by
                # applying a negative risk amount to later losing trades.
                equity = _ZERO
                worst_dd = _HUNDRED
                break
            result_r = values[rng.randrange(len(values))]
            equity += equity * risk_percent / _HUNDRED * result_r
            if equity <= 0:
                equity = _ZERO
                worst_dd = _HUNDRED
            peak = max(peak, equity)
            if peak > 0:
                worst_dd = max(worst_dd, (peak - equity) / peak * _HUNDRED)
            current_streak = current_streak + 1 if result_r < 0 else 0
            worst_streak = max(worst_streak, current_streak)
        ending_returns.append(equity - Decimal("100"))
        drawdowns.append(worst_dd)
        streaks.append(Decimal(worst_streak))
        for threshold in drawdown_thresholds_percent:
            threshold_hits[str(threshold)] += int(worst_dd >= threshold)

    percentiles = (Decimal("5"), Decimal("50"), Decimal("95"))
    return MonteCarloResult(
        status="complete",
        source_sample_size=len(values),
        simulated_trade_count=int(simulated_trade_count),
        iterations=int(iterations),
        seed=int(seed),
        ending_return_percentiles={f"p{value}": _percentile(ending_returns, value) for value in percentiles},
        max_drawdown_percentiles={f"p{value}": _percentile(drawdowns, value) for value in percentiles},
        losing_streak_percentiles={f"p{value}": _percentile(streaks, value) for value in percentiles},
        drawdown_probability_percent={
            threshold: Decimal(count) * _HUNDRED / Decimal(iterations)
            for threshold, count in sorted(threshold_hits.items(), key=lambda item: Decimal(item[0]))
        },
    )
