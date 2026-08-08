from __future__ import annotations

import math
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Any, Iterable

from app.services.tp_qty import build_tp_plan, step_aligned_decimal
from app.services.exchange_identity import clean_exchange_id

SNAPSHOT_VERSION = 1
SNAPSHOT_KEY = "tp_plan_snapshot"
POLICY_KEY = "tp_plan_policy"


def _f(value: Any, default: float = 0.0) -> float:
    """Parse a finite non-negative exchange scalar without repairing corruption."""
    try:
        if value in (None, "") or isinstance(value, bool):
            return default
        parsed = float(value)
        return parsed if math.isfinite(parsed) and parsed >= 0 else default
    except (TypeError, ValueError, OverflowError):
        return default


def _positive_finite_list(values: Iterable[Any]) -> list[float]:
    out: list[float] = []
    for value in values:
        if isinstance(value, bool):
            return []
        parsed = _f(value, 0.0)
        if parsed <= 0:
            return []
        out.append(parsed)
    return out


def _safe_distribution(values: Iterable[Any], count: int) -> list[float]:
    try:
        raw = list(values)
    except TypeError:
        raw = []
    out: list[float] = []
    for value in raw:
        if isinstance(value, bool):
            out = []
            break
        parsed = _f(value, -1.0)
        if parsed < 0:
            out = []
            break
        out.append(parsed)
    if len(out) != count or not out or sum(out) <= 0:
        return [100.0 / count for _ in range(count)] if count > 0 else []
    total = sum(out)
    return [value / total * 100.0 for value in out]


def build_policy(
    *,
    targets: Iterable[float],
    pcts: Iterable[float],
    qty_step: float | None,
    min_rr: float,
) -> dict[str, Any]:
    """Freeze validated TP inputs at entry time.

    A policy is durable recovery state, so it must never persist non-finite
    values. Invalid targets fail closed; a damaged distribution is repaired to
    equal weights before it reaches JSON/Decimal processing.
    """
    target_values = _positive_finite_list(targets)
    if not target_values:
        raise ValueError("TP policy requires positive finite targets")
    pct_values = _safe_distribution(pcts, len(target_values))
    step_value = _f(qty_step, 0.0)
    min_rr_value = max(0.0, _f(min_rr, 0.0))
    return {
        "version": SNAPSHOT_VERSION,
        "targets": target_values,
        "pcts": pct_values,
        "qty_step": step_value if step_value > 0 else None,
        "min_rr": min_rr_value,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def read_policy(
    payload: dict[str, Any],
    *,
    fallback_targets: Iterable[float],
    fallback_pcts: Iterable[float],
    fallback_qty_step: float | None,
    fallback_min_rr: float,
) -> dict[str, Any]:
    raw = payload.get(POLICY_KEY)
    if not isinstance(raw, dict):
        return build_policy(
            targets=fallback_targets,
            pcts=fallback_pcts,
            qty_step=fallback_qty_step,
            min_rr=fallback_min_rr,
        )

    fallback_targets_out = _positive_finite_list(fallback_targets)
    targets_raw = raw.get("targets")
    targets_out = (
        _positive_finite_list(targets_raw) if isinstance(targets_raw, list) else []
    )
    if not targets_out:
        targets_out = fallback_targets_out
    if not targets_out:
        raise ValueError("TP policy has no valid fallback targets")

    pcts_raw = raw.get("pcts")
    pcts_out = _safe_distribution(
        pcts_raw if isinstance(pcts_raw, list) else [],
        len(targets_out),
    )
    if not isinstance(pcts_raw, list) or len(pcts_raw) != len(targets_out):
        pcts_out = _safe_distribution(fallback_pcts, len(targets_out))

    qty_step = _f(raw.get("qty_step"), 0.0)
    if qty_step <= 0:
        qty_step = _f(fallback_qty_step, 0.0)
    min_rr = _f(raw.get("min_rr"), fallback_min_rr)

    return {
        "version": SNAPSHOT_VERSION,
        "targets": targets_out,
        "pcts": pcts_out,
        "qty_step": qty_step if qty_step > 0 else None,
        "min_rr": max(0.0, min_rr),
        "created_at": raw.get("created_at"),
    }


def _match_plan_to_original_indices(
    targets: list[float],
    plan: list[tuple[float, float]],
) -> list[dict[str, Any]]:
    """Map build_tp_plan output back to stable original TP indexes.

    Target prices are normally unique, but matching uses an ordered unused-index
    search so duplicate prices cannot collapse into one dictionary key.
    """
    unused = list(range(len(targets)))
    items: list[dict[str, Any]] = []
    for planned_price, planned_qty in plan:
        best: int | None = None
        tolerance = max(abs(float(planned_price)) * 1e-10, 1e-12)
        for index in unused:
            if abs(float(targets[index]) - float(planned_price)) <= tolerance:
                best = index
                break
        if best is None:
            continue
        unused.remove(best)
        qty = float(planned_qty)
        if qty <= 0:
            continue
        items.append(
            {
                "tp_index": best + 1,
                "price": float(targets[best]),
                "qty": qty,
            }
        )
    items.sort(key=lambda item: int(item["tp_index"]))
    return items


def build_snapshot_from_plan(
    *,
    total_qty: float,
    qty_step: float | None,
    targets: list[float],
    plan: list[tuple[float, float]],
    mode: str,
    entry: float,
    stop: float,
    min_rr: float,
    source: str,
    entry_order_id: str = "",
    entry_state: int | None = None,
    position_id: str = "",
) -> dict[str, Any]:
    items = _match_plan_to_original_indices(list(targets), list(plan))
    total = sum((Decimal(str(item["qty"])) for item in items), Decimal("0"))
    reported_total = _f(total_qty, 0.0)
    # The immutable quantity must be the exact sum of executable TP slices.
    # Keep the exchange-reported float separately for diagnostics; using it as
    # the invariant can invalidate an otherwise exact plan after JSON roundtrip.
    canonical_total = total
    return {
        "version": SNAPSHOT_VERSION,
        "locked": True,
        "source": str(source),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "entry_order_id": clean_exchange_id(entry_order_id),
        "entry_state": int(entry_state) if entry_state is not None else None,
        "position_id": clean_exchange_id(position_id),
        "final_filled_qty": float(canonical_total),
        "planned_total_qty": float(canonical_total),
        "reported_filled_qty": float(reported_total),
        "qty_step": float(qty_step) if qty_step and qty_step > 0 else None,
        "entry": float(entry),
        "stop": float(stop),
        "min_rr": max(0.0, float(min_rr)),
        "mode": str(mode),
        "items": items,
    }


def build_snapshot(
    *,
    total_qty: float,
    qty_step: float | None,
    targets: list[float],
    pcts: list[float],
    entry: float,
    stop: float,
    min_rr: float,
    source: str,
    entry_order_id: str = "",
    entry_state: int | None = None,
    position_id: str = "",
) -> dict[str, Any]:
    plan, mode = build_tp_plan(
        float(total_qty),
        qty_step,
        list(targets),
        list(pcts),
        entry=float(entry),
        stop=float(stop),
        min_rr=float(min_rr),
    )
    return build_snapshot_from_plan(
        total_qty=total_qty,
        qty_step=qty_step,
        targets=targets,
        plan=plan,
        mode=mode,
        entry=entry,
        stop=stop,
        min_rr=min_rr,
        source=source,
        entry_order_id=entry_order_id,
        entry_state=entry_state,
        position_id=position_id,
    )


def _strict_snapshot_is_valid(snapshot: Any) -> bool:
    if not isinstance(snapshot, dict) or snapshot.get("locked") is not True:
        return False
    if int(_f(snapshot.get("version"), 0.0)) != SNAPSHOT_VERSION:
        return False
    final_qty = _f(snapshot.get("final_filled_qty"), 0.0)
    items = snapshot.get("items")
    if final_qty <= 0 or not isinstance(items, list) or not items:
        return False

    step = Decimal(str(_f(snapshot.get("qty_step"), 0.0)))
    has_step = step.is_finite() and step > 0
    # JSON float roundtrips can add microscopic noise, but accepting half a lot
    # would let a genuinely under-covered/corrupt immutable plan pass validation.
    tolerance = (
        max(step * Decimal("1e-9"), Decimal("1e-18")) if has_step else Decimal("1e-10")
    )

    seen: set[int] = set()
    total = Decimal("0")
    for item in items:
        if not isinstance(item, dict):
            return False
        idx = int(_f(item.get("tp_index"), 0.0))
        price = _f(item.get("price"), 0.0)
        qty = _f(item.get("qty"), 0.0)
        if idx <= 0 or idx in seen or price <= 0 or qty <= 0:
            return False
        seen.add(idx)
        qty_d = Decimal(str(qty))
        if has_step and abs(qty_d - step_aligned_decimal(qty_d, step)) > tolerance:
            return False
        total += qty_d

    final_d = Decimal(str(final_qty))
    planned_d = Decimal(str(_f(snapshot.get("planned_total_qty"), final_qty)))
    if has_step:
        if abs(final_d - step_aligned_decimal(final_d, step)) > tolerance:
            return False
        if abs(planned_d - step_aligned_decimal(planned_d, step)) > tolerance:
            return False
    return abs(total - final_d) <= tolerance and abs(total - planned_d) <= tolerance


def normalize_snapshot(snapshot: Any) -> dict[str, Any] | None:
    """Return a valid snapshot, repairing only the known v1.6.4 one-lot defect.

    v1.6.4 could receive an exact exchange quantity through binary float as
    ``N*step - epsilon``. It then floored that value and produced a plan exactly
    one lot short while storing the unrounded reported quantity as
    ``final_filled_qty``. The repair is deliberately narrow: the reported value
    must be within 1e-9 step units of an exact lot, the stored item sum must match
    ``planned_total_qty``, and the deficit must be exactly one step. Corrupt or
    genuinely under-covered snapshots remain invalid/fail-closed.
    """
    if not isinstance(snapshot, dict):
        return None
    raw_items = snapshot.get("items")
    if not isinstance(raw_items, list) or not raw_items:
        return None
    if any(not isinstance(item, dict) for item in raw_items):
        return None
    copied = dict(snapshot)
    copied["items"] = [dict(item) for item in raw_items]
    if _strict_snapshot_is_valid(copied):
        return copied

    if copied.get("locked") is not True:
        return None
    if int(_f(copied.get("version"), 0.0)) != SNAPSHOT_VERSION:
        return None
    step = Decimal(str(_f(copied.get("qty_step"), 0.0)))
    items = copied.get("items")
    if step <= 0 or not isinstance(items, list) or not items:
        return None

    total = sum(
        (Decimal(str(_f(item.get("qty"), 0.0))) for item in items), Decimal("0")
    )
    if total <= 0 or any(
        int(_f(item.get("tp_index"), 0.0)) <= 0
        or _f(item.get("price"), 0.0) <= 0
        or _f(item.get("qty"), 0.0) <= 0
        for item in items
    ):
        return None
    if len({int(_f(item.get("tp_index"), 0.0)) for item in items}) != len(items):
        return None

    raw_reported = Decimal(
        str(
            _f(
                copied.get("reported_filled_qty"),
                _f(copied.get("final_filled_qty"), 0.0),
            )
        )
    )
    canonical_reported = step_aligned_decimal(raw_reported, step)
    if canonical_reported <= 0:
        return None
    unit_error = abs((raw_reported / step) - (canonical_reported / step))
    planned = Decimal(str(_f(copied.get("planned_total_qty"), 0.0)))
    strict_tolerance = max(step * Decimal("1e-9"), Decimal("1e-18"))
    if (
        unit_error > Decimal("1e-9")
        or abs(planned - total) > strict_tolerance
        or canonical_reported - total != step
    ):
        return None

    # Legacy v1.6.4 snapshots used the last surviving target as the
    # deterministic repair slice. New v1.6.81+ TP plans allocate fresh
    # rounding remainder to TP1 before orders are submitted, but this legacy
    # repair keeps old immutable snapshots backward-compatible.
    last_index = max(
        range(len(items)), key=lambda i: int(_f(items[i].get("tp_index"), 0.0))
    )
    items[last_index]["qty"] = float(
        Decimal(str(_f(items[last_index].get("qty"), 0.0))) + step
    )
    copied["items"] = items
    copied["reported_filled_qty"] = float(raw_reported)
    copied["final_filled_qty"] = float(canonical_reported)
    copied["planned_total_qty"] = float(canonical_reported)
    copied["quantity_repair"] = {
        "reason": "v1.6.4_float_floor_one_lot",
        "added_to_tp_index": int(items[last_index]["tp_index"]),
        "added_qty": float(step),
    }
    return copied if _strict_snapshot_is_valid(copied) else None


def snapshot_is_valid(snapshot: Any) -> bool:
    return normalize_snapshot(snapshot) is not None


def get_snapshot(payload: dict[str, Any]) -> dict[str, Any] | None:
    return normalize_snapshot(payload.get(SNAPSHOT_KEY))


def snapshot_items(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    normalized = normalize_snapshot(snapshot)
    if normalized is None:
        return []
    return [dict(item) for item in normalized.get("items") or []]


def snapshot_plan_map(snapshot: dict[str, Any]) -> dict[int, float]:
    return {
        int(item["tp_index"]): float(item["qty"]) for item in snapshot_items(snapshot)
    }


def snapshot_target_map(snapshot: dict[str, Any]) -> dict[int, float]:
    return {
        int(item["tp_index"]): float(item["price"]) for item in snapshot_items(snapshot)
    }


def snapshot_total_qty(snapshot: dict[str, Any]) -> float:
    normalized = normalize_snapshot(snapshot)
    return _f(normalized.get("final_filled_qty"), 0.0) if normalized else 0.0


def rebase_snapshot_items_to_qty(
    items: Iterable[dict[str, Any]],
    *,
    total_qty: float,
    qty_step: float | None,
) -> list[dict[str, Any]]:
    """Scale immutable snapshot slices to the live remaining position.

    Prices and original TP indexes never change. Only quantities are reduced
    when the live remainder differs from the exact sum of the untouched
    snapshot slices (for example after exchange rounding or a manual partial
    close). The allocation is deterministic and never exceeds ``total_qty``.
    """
    rows = [
        {
            "tp_index": int(_f(item.get("tp_index"), 0.0)),
            "price": _f(item.get("price"), 0.0),
            "qty": _f(item.get("qty"), 0.0),
        }
        for item in items
        if isinstance(item, dict)
    ]
    rows = [
        row
        for row in rows
        if row["tp_index"] > 0 and row["price"] > 0 and row["qty"] > 0
    ]
    rows.sort(key=lambda row: int(row["tp_index"]))
    total = step_aligned_decimal(max(0.0, float(total_qty)), qty_step)
    if not rows or total <= 0:
        return []

    original_total = sum((Decimal(str(row["qty"])) for row in rows), Decimal("0"))
    if original_total <= 0:
        return []

    step = Decimal(str(qty_step)) if qty_step and qty_step > 0 else Decimal("0")
    if step <= 0:
        out: list[dict[str, Any]] = []
        used = Decimal("0")
        for index, row in enumerate(rows):
            if index == len(rows) - 1:
                qty = max(Decimal("0"), total - used)
            else:
                qty = total * Decimal(str(row["qty"])) / original_total
                used += qty
            if qty > 0:
                out.append({**row, "qty": float(qty)})
        return out

    total_units = int((total / step).to_integral_value(rounding=ROUND_DOWN))
    if total_units <= 0:
        return []

    # If there are fewer tradable units than targets, keep the nearest targets
    # first. This mirrors entry trim semantics and avoids zero-quantity orders.
    if total_units < len(rows):
        return [{**row, "qty": float(step)} for row in rows[:total_units]]

    raw_units = [
        Decimal(total_units) * Decimal(str(row["qty"])) / original_total for row in rows
    ]
    allocated = [
        max(1, int(value.to_integral_value(rounding=ROUND_DOWN))) for value in raw_units
    ]

    # Min-one allocation can overrun total units for tiny weighted tails. Remove
    # excess from the largest allocations first while keeping every target alive.
    excess = sum(allocated) - total_units
    if excess > 0:
        for idx in sorted(
            range(len(allocated)), key=lambda i: allocated[i], reverse=True
        ):
            removable = max(0, allocated[idx] - 1)
            take = min(removable, excess)
            allocated[idx] -= take
            excess -= take
            if excess <= 0:
                break

    remainder = total_units - sum(allocated)
    if remainder > 0:
        fractions = [raw_units[i] - int(raw_units[i]) for i in range(len(rows))]
        order = sorted(range(len(rows)), key=lambda i: (fractions[i], -i), reverse=True)
        cursor = 0
        while remainder > 0:
            allocated[order[cursor % len(order)]] += 1
            remainder -= 1
            cursor += 1

    return [
        {**row, "qty": float(step * allocated[index])}
        for index, row in enumerate(rows)
        if allocated[index] > 0
    ]
