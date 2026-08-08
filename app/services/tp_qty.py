from __future__ import annotations

import math
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, List, Tuple


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        parsed = float(value)
        return parsed if math.isfinite(parsed) else default
    except (TypeError, ValueError, OverflowError):
        return default


_STEP_UNIT_SNAP_TOLERANCE = Decimal("1e-9")


def step_aligned_decimal(qty: Any, step: Any | None) -> Decimal:
    """Return a non-negative quantity aligned safely to ``step``.

    Exchange quantities often arrive as a contract count multiplied by a small
    ``contractSize``.  Converting that exact decimal product through ``float``
    can produce values such as ``0.0066619999999999995`` instead of ``0.006662``.
    A blind Decimal floor then removes one *whole* exchange lot.

    Values that are within one billionth of a step unit from an integer unit are
    treated as float-conversion noise and snapped to that integer.  Everything
    else is still rounded down, so genuine over-risking is never introduced.
    """
    try:
        value_d = Decimal(str(qty))
    except Exception:
        return Decimal("0")
    if not value_d.is_finite() or value_d <= 0:
        return Decimal("0")
    try:
        step_d = Decimal(str(step)) if step not in (None, "") else Decimal("0")
    except Exception:
        step_d = Decimal("0")
    if not step_d.is_finite() or step_d <= 0:
        return value_d

    try:
        units = value_d / step_d
        nearest = units.to_integral_value(rounding=ROUND_HALF_UP)
        if abs(units - nearest) <= _STEP_UNIT_SNAP_TOLERANCE:
            aligned_units = nearest
        else:
            aligned_units = units.to_integral_value(rounding=ROUND_DOWN)
        aligned = aligned_units * step_d
    except Exception:
        # Decimal context overflow/DivisionImpossible is possible only for
        # absurdly large legacy values. Returning zero is fail-closed: no order
        # quantity will be created from data we cannot represent safely.
        return Decimal("0")
    return max(Decimal("0"), aligned)


def floor_qty_to_step(qty: float, step: float | None) -> float:
    """Round quantity down to exchange step, tolerating float representation noise."""
    return float(step_aligned_decimal(qty, step))


def order_normalized_qty(order: Any, fallback: float) -> float:
    """Return real qty submitted after adapter qty-step retry, if present."""
    if isinstance(order, dict):
        val = order.get("_normalized_quantity")
        got = _to_float(val, 0.0)
        if got > 0:
            return got
    return float(fallback)


def order_required_qty_step(order: Any, fallback: float | None = None) -> float | None:
    """Return qty step discovered from exchange validation retry, if present."""
    if isinstance(order, dict):
        got = _to_float(order.get("_required_qty_step"), 0.0)
        if got > 0:
            return got
    try:
        fb = float(fallback or 0.0)
        return fb if fb > 0 else None
    except Exception:
        return None


def max_placeable_tp_count(
    total_qty: float, step: float | None, n_requested: int
) -> int:
    """How many TPs can physically fit, without trusting persisted numerics."""
    try:
        requested = max(0, int(n_requested))
    except (TypeError, ValueError, OverflowError):
        return 0
    if requested == 0:
        return 0

    total_d = step_aligned_decimal(total_qty, step)
    if total_d <= 0:
        return 0
    try:
        step_d = Decimal(str(step)) if step not in (None, "") else Decimal("0")
    except Exception:
        step_d = Decimal("0")
    if not step_d.is_finite() or step_d <= 0:
        return requested
    try:
        lots = int(total_d // step_d)
    except Exception:
        return 0
    return max(0, min(lots, requested))


# Sentinel returned when trim path finds no targets passing the RR filter
# (fallback: all targets used without filter).
TP_PLAN_NO_VIABLE_RR = "no_viable_rr"


def build_tp_plan(
    total_qty: float,
    step: float | None,
    targets: List[float],
    pcts: List[float],
    *,
    entry: float = 0.0,
    stop: float = 0.0,
    min_rr: float = 0.5,
) -> Tuple[List[Tuple[float, float]], str]:
    """Compute exact step-aligned ``(target, qty)`` TP slices.

    Runtime recovery can encounter legacy/corrupted JSON.  This function is a
    safety boundary: malformed quantities or target prices fail closed with an
    empty plan, while an invalid percentage distribution falls back to equal
    weights.  No ``NaN``/``Infinity`` value is allowed into Decimal arithmetic.
    """
    try:
        raw_targets = list(targets)
    except TypeError:
        return [], "smart"
    n = len(raw_targets)
    if n == 0:
        return [], "smart"

    normalized_targets: list[float] = []
    for value in raw_targets:
        if isinstance(value, bool):
            return [], "smart"
        parsed = _to_float(value, 0.0)
        if parsed <= 0:
            return [], "smart"
        normalized_targets.append(parsed)

    total_d = step_aligned_decimal(total_qty, step)
    if total_d <= 0:
        return [], "smart"
    try:
        step_d_raw = Decimal(str(step)) if step not in (None, "") else Decimal("0")
    except Exception:
        step_d_raw = Decimal("0")
    step_d = step_d_raw if step_d_raw.is_finite() and step_d_raw > 0 else None

    # Never silently truncate targets when a persisted/manual distribution has
    # the wrong length. Invalid/non-finite weights fall back to a safe equal
    # distribution and valid non-negative weights are normalised to exactly 100%.
    pct_values: list[Decimal] = []
    try:
        for value in pcts:
            if isinstance(value, bool):
                raise ValueError("boolean TP weight")
            parsed = Decimal(str(value))
            if not parsed.is_finite() or parsed < 0:
                raise ValueError("invalid TP weight")
            pct_values.append(parsed)
    except Exception:
        pct_values = []
    pct_sum = sum(pct_values, Decimal("0"))
    if len(pct_values) != n or not pct_sum.is_finite() or pct_sum <= 0:
        pct_values = [Decimal("1") for _ in range(n)]
        pct_sum = Decimal(n)
    pct_values = [value / pct_sum * Decimal("100") for value in pct_values]

    try:
        lots = n if step_d is None else int(total_d // step_d)
    except Exception:
        return [], "smart"
    max_tp = max(1, min(lots, n))

    def floor_d(value: Decimal) -> Decimal:
        if step_d is None:
            return value
        try:
            return (value / step_d).to_integral_value(rounding=ROUND_DOWN) * step_d
        except Exception:
            return Decimal("0")

    smart_would_work = max_tp >= n
    if smart_would_work and step_d is not None:
        smart_would_work = all(
            floor_d(total_d * pct / Decimal("100")) > 0 for pct in pct_values
        )

    if max_tp >= n and smart_would_work:
        # Allocate every exchange-executable lot into the TP plan before any
        # order is submitted.  Older builds gave the rounding remainder to the
        # last TP by using ``remaining`` for the final target.  That could leave
        # a confusing final dust case when live exchange normalisation diverged
        # from the planned percentages: all targets were reported as completed
        # while one minimal lot remained.  Match the user's MaxBot policy: floor
        # each configured slice, then carry every leftover lot to TP1 so the
        # closest target absorbs rounding and the sum of TP quantities equals
        # the executable position quantity from the start.
        plan_dec: list[tuple[float, Decimal]] = []
        allocated = Decimal("0")
        for target, pct in zip(normalized_targets, pct_values, strict=True):
            qty_d = floor_d(total_d * pct / Decimal("100"))
            if qty_d > 0:
                plan_dec.append((target, qty_d))
                allocated += qty_d

        remainder = total_d - allocated
        if remainder > 0 and plan_dec:
            first_target, first_qty = plan_dec[0]
            plan_dec[0] = (first_target, first_qty + remainder)
            allocated += remainder

        if allocated != total_d:
            # This should be unreachable when smart_would_work is true, but keep
            # the safety boundary explicit: never return an under-covered plan.
            return [], "smart"

        return [(target, float(qty_d)) for target, qty_d in plan_dec], "smart"

    entry_value = _to_float(entry, 0.0)
    stop_value = _to_float(stop, 0.0)
    min_rr_value = max(0.0, _to_float(min_rr, 0.0))
    risk = abs(entry_value - stop_value) if entry_value > 0 and stop_value > 0 else 0.0
    if risk > 0 and min_rr_value > 0:
        viable_indexes = [
            index
            for index, target in enumerate(normalized_targets)
            if abs(entry_value - target) / risk >= min_rr_value
        ]
    else:
        viable_indexes = list(range(n))
    if not viable_indexes:
        viable_indexes = list(range(n))

    selected_indexes = viable_indexes[:max_tp]
    if not selected_indexes or step_d is None:
        return [], "trim"

    # A percentage tail can be smaller than one exchange lot even when the
    # position has enough lots to keep every selected target alive.  The old
    # trim fallback assigned one lot to every target except the last and put
    # the whole remainder on that last target.  For an early-fixation scheme
    # this inverted 70/15/10/5 into, for example, 10/10/10/70 on a ten-lot
    # position.  Allocate integer lots by the configured weights instead,
    # while reserving one lot per selected TP and preserving exact coverage.
    selected_weights = [pct_values[index] for index in selected_indexes]
    selected_weight_total = sum(selected_weights, Decimal("0"))
    if not selected_weight_total.is_finite() or selected_weight_total <= 0:
        selected_weights = [Decimal("1") for _ in selected_indexes]
        selected_weight_total = Decimal(len(selected_indexes))

    total_units = int((total_d / step_d).to_integral_value(rounding=ROUND_DOWN))
    if total_units < len(selected_indexes):
        return [], "trim"
    raw_units = [
        Decimal(total_units) * weight / selected_weight_total
        for weight in selected_weights
    ]
    allocated_units = [
        max(1, int(value.to_integral_value(rounding=ROUND_DOWN)))
        for value in raw_units
    ]

    # Minimum-one protection can temporarily over-allocate tiny tails. Remove
    # units only from slices that still remain executable, choosing the slice
    # currently furthest above its ideal quota.  Ties prefer the more distant
    # target so the closest target is not reduced unnecessarily.
    excess = sum(allocated_units) - total_units
    while excess > 0:
        candidates = [
            index for index, units in enumerate(allocated_units) if units > 1
        ]
        if not candidates:
            return [], "trim"
        index = max(
            candidates,
            key=lambda item: (
                Decimal(allocated_units[item]) - raw_units[item],
                item,
            ),
        )
        allocated_units[index] -= 1
        excess -= 1

    # Floors can leave a bounded remainder. Give each unit to the currently
    # most under-allocated slice; deterministic ties prefer the nearest TP.
    remainder = total_units - sum(allocated_units)
    while remainder > 0:
        index = max(
            range(len(allocated_units)),
            key=lambda item: (
                raw_units[item] - Decimal(allocated_units[item]),
                -item,
            ),
        )
        allocated_units[index] += 1
        remainder -= 1

    plan = [
        (
            normalized_targets[target_index],
            float(step_d * allocated_units[plan_index]),
        )
        for plan_index, target_index in enumerate(selected_indexes)
    ]
    return plan, "trim"


def viable_tp_count(total_qty: float, pcts: list[float], step: float | None) -> int:
    """Legacy helper kept for backward compatibility with existing tests."""
    try:
        weights = list(pcts)
    except TypeError:
        return 0
    if not weights:
        return 0
    total = _to_float(total_qty, 0.0)
    if total <= 0:
        return 0
    min_unit = _to_float(step, 0.0)
    if min_unit <= 0:
        return len(weights)
    count = 0
    for pct in weights:
        weight = _to_float(pct, 0.0)
        if weight <= 0:
            continue
        raw = total * weight / 100.0
        if math.isfinite(raw) and floor_qty_to_step(raw, min_unit) >= min_unit:
            count += 1
    return count


def planned_tp_qty(
    total_qty: float,
    pct: float,
    remaining_qty: float,
    *,
    is_last: bool,
    step: float | None = None,
) -> float:
    """Legacy per-TP qty helper. Prefer build_tp_plan() for new code."""
    total = _to_float(total_qty, 0.0)
    remaining = _to_float(remaining_qty, 0.0)
    weight = _to_float(pct, 0.0)
    if remaining <= 0 or total <= 0 or (not is_last and weight <= 0):
        return 0.0
    raw = remaining if is_last else min(remaining, total * weight / 100.0)
    if not math.isfinite(raw) or raw <= 0:
        return 0.0
    qty = floor_qty_to_step(raw, step)
    if qty <= 0:
        return 0.0
    return round(min(qty, remaining), 10)
