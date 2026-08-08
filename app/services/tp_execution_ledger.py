from __future__ import annotations

import math
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

from app.services.tp_plan_snapshot import normalize_snapshot
from app.services.exchange_identity import clean_exchange_id

TP_LEDGER_VERSION = 2

# These actions represent a real conditional TP order that should be tracked by
# lifecycle/history reconciliation. The action journal remains useful for audit,
# while the canonical top-level ``tp`` list is the single operational ledger.
_OPEN_TP_ACTION_TYPES = {
    "tp_created_after_fill",
    "tp_recovered_order",
    "tp_order",
    "tp_created",
}

# These actions already closed a TP slice with a confirmed reduce-only market
# order. They still belong in the canonical ledger so PnL/BE/close analysis has
# a complete chronological picture.
_FILLED_TP_ACTION_TYPES = {
    "tp_catchup_market_close",
    "tp_recovered_market_close",
}

_RECOGNIZED_TP_ACTION_TYPES = _OPEN_TP_ACTION_TYPES | _FILLED_TP_ACTION_TYPES


def _f(value: Any, default: float = 0.0) -> float:
    """Parse a finite non-negative exchange scalar without repairing corruption."""
    try:
        if value in (None, "") or isinstance(value, bool):
            return default
        parsed = float(value)
        return parsed if math.isfinite(parsed) and parsed >= 0 else default
    except (TypeError, ValueError, OverflowError):
        return default


def _i(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def _text(value: Any) -> str:
    return clean_exchange_id(value)


def _close(a: float, b: float) -> bool:
    return abs(float(a) - float(b)) <= max(abs(float(a)), abs(float(b)), 1.0) * 1e-9


def _legacy_fake_tp_success(item: Any) -> bool:
    """Reject old synthetic rows that never represented a live exchange TP."""

    if not isinstance(item, dict):
        return False
    candidates = [item]
    for key in ("order", "result"):
        nested = item.get(key)
        if isinstance(nested, dict):
            candidates.append(nested)
    return any(
        str(candidate.get("type") or "").upper() == "TAKE_PROFIT_SKIPPED_COVERED"
        or bool(candidate.get("_idempotent_coverage_full"))
        for candidate in candidates
    )


def _section_actions(payload: dict[str, Any]) -> list[tuple[str, int, dict[str, Any]]]:
    out: list[tuple[str, int, dict[str, Any]]] = []
    serial = 0
    for section in ("post_fill", "recovery"):
        value = payload.get(section)
        if isinstance(value, dict):
            value = value.get("actions")
        if not isinstance(value, list):
            continue
        for item in value:
            serial += 1
            if isinstance(item, dict):
                out.append((section, serial, item))
    return out


def _snapshot_items(payload: dict[str, Any]) -> dict[int, dict[str, Any]]:
    # Use only a strictly valid immutable snapshot. A malformed snapshot must not
    # silently override a confirmed exchange action during legacy repair.
    snapshot = normalize_snapshot(payload.get("tp_plan_snapshot"))
    items = snapshot.get("items") if isinstance(snapshot, dict) else None
    result: dict[int, dict[str, Any]] = {}
    if not isinstance(items, list):
        return result
    for item in items:
        if not isinstance(item, dict):
            continue
        index = _i(item.get("tp_index"), 0)
        if index > 0:
            result[index] = item
    return result


def _order_stop_id(order: Any) -> str:
    if not isinstance(order, dict):
        return ""
    data = order.get("data")
    data_dict = data if isinstance(data, dict) else {}
    raw = order.get("raw") if isinstance(order.get("raw"), dict) else {}
    for value in (
        order.get("_confirmed_stop_plan_id"),
        order.get("stopPlanOrderId"),
        order.get("stopOrderId"),
        data_dict.get("stopPlanOrderId"),
        data_dict.get("stopOrderId"),
        data_dict.get("id"),
        raw.get("stopPlanOrderId"),
        raw.get("stopOrderId"),
        raw.get("id"),
        data if not isinstance(data, (dict, list)) else None,
    ):
        found = _text(value)
        if found:
            return found
    return ""


def _order_position_id(order: Any) -> str:
    if not isinstance(order, dict):
        return ""
    data = order.get("data")
    data_dict = data if isinstance(data, dict) else {}
    raw = order.get("raw") if isinstance(order.get("raw"), dict) else {}
    for value in (
        order.get("_confirmed_position_id"),
        order.get("positionId"),
        data_dict.get("positionId"),
        raw.get("positionId"),
    ):
        found = _text(value)
        if found:
            return found
    return ""


def _merge_missing_dict(base: dict[str, Any], supplement: dict[str, Any]) -> bool:
    """Recursively add only absent/empty values without overwriting new data."""

    changed = False
    for key, value in supplement.items():
        current = base.get(key)
        if isinstance(current, dict) and isinstance(value, dict):
            changed |= _merge_missing_dict(current, value)
            continue
        if current in (None, "", [], {}) and value not in (None, "", [], {}):
            base[key] = deepcopy(value)
            changed = True
    return changed


def _row_is_recreated(row: dict[str, Any]) -> bool:
    return bool(
        row.get("recreated_after_be")
        or row.get("be_recreated_at")
        or row.get("previous_order_id_before_be")
    )


def _row_quality(row: dict[str, Any]) -> tuple[int, int, int, int, int]:
    order = row.get("order")
    exact_identity = bool(_order_stop_id(order))
    populated = sum(
        1 for value in row.values() if value not in (None, "", 0, 0.0, False, [], {})
    )
    return (
        int(_row_is_recreated(row)),
        int(exact_identity),
        int(row.get("filled_notified") is True),
        int(row.get("filled") is True),
        populated,
    )


def _set_missing(row: dict[str, Any], key: str, value: Any) -> bool:
    if value in (None, ""):
        return False
    current = row.get(key)
    if current not in (None, "", 0, 0.0):
        return False
    row[key] = deepcopy(value)
    return True


def _positive_values(rows: list[dict[str, Any]], key: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = _f(row.get(key), 0.0)
        if value > 0 and not any(_close(value, existing) for existing in values):
            values.append(value)
    return values


def _deduplicate_existing_rows(
    payload: dict[str, Any], *, timestamp: str
) -> tuple[list[dict[str, Any]], set[int]]:
    raw = payload.get("tp")
    if not isinstance(raw, list):
        return [], set()

    groups: dict[int, list[dict[str, Any]]] = {}
    reserved_indices = {
        index
        for item in raw
        if isinstance(item, dict) and (index := _i(item.get("tp_index"), 0)) > 0
    }
    next_fallback = 1
    for item in raw:
        if not isinstance(item, dict):
            continue
        row = deepcopy(item)
        index = _i(row.get("tp_index"), 0)
        if index <= 0:
            while next_fallback in reserved_indices or next_fallback in groups:
                next_fallback += 1
            index = next_fallback
            next_fallback += 1
        row["tp_index"] = index
        groups.setdefault(index, []).append(row)

    rows: list[dict[str, Any]] = []
    repaired: set[int] = set()
    for index in sorted(groups):
        candidates = groups[index]
        primary = deepcopy(max(candidates, key=_row_quality))
        if len(candidates) == 1:
            rows.append(primary)
            continue

        repaired.add(index)
        recreated_primary = _row_is_recreated(primary)
        exact_ids = sorted(
            {
                order_id
                for candidate in candidates
                if (order_id := _order_stop_id(candidate.get("order")))
            }
        )
        position_ids = sorted(
            {
                position_id
                for candidate in candidates
                if (position_id := _order_position_id(candidate.get("order")))
            }
        )

        for candidate in candidates:
            if candidate is primary:
                continue
            for key, value in candidate.items():
                if key == "order":
                    continue
                if key not in primary or primary.get(key) in (None, "", 0, 0.0):
                    primary[key] = deepcopy(value)
            order = candidate.get("order")
            if isinstance(order, dict) and order:
                current_order = primary.get("order")
                if not isinstance(current_order, dict):
                    primary["order"] = deepcopy(order)
                elif not recreated_primary:
                    _merge_missing_dict(current_order, order)

        # Notification/fill evidence may be merged only for the same logical
        # order. A post-BE recreated order starts a fresh lifecycle.
        if not recreated_primary:
            primary["filled_notified"] = any(
                candidate.get("filled_notified") is True for candidate in candidates
            )
            primary["filled"] = any(
                candidate.get("filled") is True for candidate in candidates
            )
            primary["price_seen"] = any(
                candidate.get("price_seen") is True for candidate in candidates
            )
            primary["notify_attempts"] = max(
                (_i(candidate.get("notify_attempts"), 0) for candidate in candidates),
                default=0,
            )
            filled_qtys = _positive_values(candidates, "exchange_filled_qty")
            if filled_qtys:
                primary["exchange_filled_qty"] = max(filled_qtys)

        duplicate_qtys = _positive_values(candidates, "qty")
        if len(duplicate_qtys) > 1 and not recreated_primary:
            # No external action/snapshot has been considered yet. Keep the
            # largest quantity and fail closed until canonicalization resolves it.
            primary["qty"] = max(duplicate_qtys)
            primary["planned_qty"] = max(
                _positive_values(candidates, "planned_qty") or duplicate_qtys
            )
            primary["qty_conflict_v1"] = {
                "unresolved": True,
                "candidate_qtys": sorted(duplicate_qtys),
                "source": "duplicate_top_level_tp_rows",
                "detected_at": timestamp,
            }

        if len(exact_ids) > 1 and not recreated_primary:
            primary["identity_conflict_v1"] = {
                "unresolved": True,
                "candidate_stop_plan_ids": exact_ids,
                "candidate_position_ids": position_ids,
                "source": "duplicate_top_level_tp_rows",
                "detected_at": timestamp,
            }
        elif recreated_primary:
            primary["identity_conflict_v1"] = None
            if len(exact_ids) > 1:
                primary["historical_stop_plan_ids"] = [
                    value
                    for value in exact_ids
                    if value != _order_stop_id(primary.get("order"))
                ]

        primary["duplicate_repair_v1"] = {
            "merged_rows": len(candidates),
            "repaired_at": timestamp,
        }
        rows.append(primary)

    return rows, repaired


def _best_actions_by_index(
    payload: dict[str, Any],
) -> dict[int, tuple[str, int, dict[str, Any]]]:
    result: dict[int, tuple[str, int, dict[str, Any]]] = {}
    for section, serial, action in _section_actions(payload):
        action_type = str(action.get("type") or "").strip()
        if action_type not in _RECOGNIZED_TP_ACTION_TYPES:
            continue
        if _legacy_fake_tp_success(action):
            continue
        index = _i(action.get("tp_index") or action.get("index"), 0)
        if index <= 0:
            continue
        current = result.get(index)
        # Recovery actions are newer/more authoritative than the original
        # post-fill journal. Within a section, the later list item wins.
        rank = (2 if section == "recovery" else 1, serial)
        current_rank = (
            (2 if current[0] == "recovery" else 1, current[1])
            if current is not None
            else (0, 0)
        )
        if rank >= current_rank:
            result[index] = (section, serial, action)
    return result


def _action_values(
    action: dict[str, Any], snapshot: dict[str, Any]
) -> tuple[float, float, float, dict[str, Any] | None]:
    target = _f(
        action.get("tp")
        or action.get("target")
        or action.get("price")
        or snapshot.get("price"),
        0.0,
    )
    qty = _f(
        action.get("qty")
        or action.get("actual_tp_qty")
        or action.get("planned_qty")
        or snapshot.get("qty"),
        0.0,
    )
    planned_qty = _f(
        action.get("planned_qty") or qty or snapshot.get("qty"),
        0.0,
    )
    order = action.get("result") or action.get("order")
    return target, qty, planned_qty, order if isinstance(order, dict) else None


def tp_row_has_unresolved_qty_conflict(row: Any) -> bool:
    if not isinstance(row, dict):
        return False
    conflict = row.get("qty_conflict_v1")
    return isinstance(conflict, dict) and bool(conflict.get("unresolved"))


def tp_row_has_unresolved_identity_conflict(row: Any) -> bool:
    if not isinstance(row, dict):
        return False
    conflict = row.get("identity_conflict_v1")
    return isinstance(conflict, dict) and bool(conflict.get("unresolved"))


def canonicalize_tp_ledger(
    payload: dict[str, Any],
    *,
    now_iso: str | None = None,
) -> tuple[list[dict[str, Any]], bool, list[int]]:
    """Repair and maintain the canonical top-level TP ledger.

    Integrity rules:
    - exactly one row per ``tp_index``;
    - a non-empty but identity-less legacy order is enriched from confirmed
      post-fill/recovery data;
    - current post-BE order identity is never overwritten by stale journals;
    - confirmed action quantities replace stale legacy quantities;
    - unresolved duplicate-only quantity/identity conflicts fail closed.
    """

    if not isinstance(payload, dict):
        return [], False, []

    timestamp = now_iso or datetime.now(timezone.utc).isoformat()
    original_value = deepcopy(payload.get("tp"))
    rows, duplicate_repairs = _deduplicate_existing_rows(payload, timestamp=timestamp)
    by_index = {_i(row.get("tp_index"), 0): row for row in rows}
    snapshot_by_index = _snapshot_items(payload)
    action_by_index = _best_actions_by_index(payload)
    repaired: set[int] = set(duplicate_repairs)

    for index, (section, _serial, action) in action_by_index.items():
        action_type = str(action.get("type") or "").strip()
        snapshot = snapshot_by_index.get(index, {})
        target, qty, planned_qty, order = _action_values(action, snapshot)

        # A row without target/qty cannot safely participate in history matching
        # or quantity arithmetic. Keep the action journal intact and fail closed.
        if target <= 0 or qty <= 0:
            continue

        row = by_index.get(index)
        created = row is None
        if row is None:
            row = {"tp_index": index}
            rows.append(row)
            by_index[index] = row

        changed_row = created
        recreated_current = _row_is_recreated(row)

        if recreated_current:
            changed_row |= _set_missing(row, "target", target)
            changed_row |= _set_missing(row, "qty", qty)
            changed_row |= _set_missing(row, "planned_qty", planned_qty or qty)
        else:
            old_target = _f(row.get("target") or row.get("price"), 0.0)
            old_qty = _f(row.get("qty") or row.get("actual_tp_qty"), 0.0)
            old_planned = _f(row.get("planned_qty"), 0.0)

            if old_target <= 0 or not _close(old_target, target):
                if old_target > 0 and not _close(old_target, target):
                    row["target_reconciliation_v1"] = {
                        "previous": old_target,
                        "canonical": target,
                        "source": f"{section}:{action_type}",
                        "reconciled_at": timestamp,
                    }
                row["target"] = target
                changed_row = True

            # A confirmed post-fill/recovery action describes the quantity of the
            # actual exchange order. It is more authoritative than a stale legacy
            # top-level row. This prevents a 0.4/1.0 mismatch from turning a 40%
            # fill into a false full TP.
            if old_qty <= 0 or not _close(old_qty, qty):
                if old_qty > 0 and not _close(old_qty, qty):
                    row["qty_reconciliation_v1"] = {
                        "previous_qty": old_qty,
                        "canonical_qty": qty,
                        "previous_planned_qty": old_planned,
                        "canonical_planned_qty": planned_qty or qty,
                        "source": f"{section}:{action_type}",
                        "reconciled_at": timestamp,
                    }
                row["qty"] = qty
                changed_row = True
            if old_planned <= 0 or not _close(old_planned, planned_qty or qty):
                row["planned_qty"] = planned_qty or qty
                changed_row = True
            if row.get("qty_conflict_v1") is not None:
                row["qty_conflict_v1"] = None
                changed_row = True

        if isinstance(order, dict) and order:
            existing_order = row.get("order")
            existing_stop_id = _order_stop_id(existing_order)
            action_stop_id = _order_stop_id(order)
            if not isinstance(existing_order, dict) or not existing_order:
                row["order"] = deepcopy(order)
                changed_row = True
            elif not existing_stop_id and action_stop_id:
                # Preserve diagnostics/status from the old object but import the
                # exact identity needed for BingX history and cleanup.
                changed_row |= _merge_missing_dict(existing_order, order)
                if not _order_stop_id(existing_order):
                    # Defensive fallback for unusual nested response shapes.
                    existing_order["_confirmed_stop_plan_id"] = action_stop_id
                    changed_row = True
                action_position_id = _order_position_id(order)
                if action_position_id and not _order_position_id(existing_order):
                    existing_order["_confirmed_position_id"] = action_position_id
                    changed_row = True
            elif (
                existing_stop_id
                and action_stop_id
                and existing_stop_id == action_stop_id
            ):
                # The exact stop-plan identity agrees. Enrich missing position and
                # response diagnostics without replacing the current order object.
                changed_row |= _merge_missing_dict(existing_order, order)
                action_position_id = _order_position_id(order)
                if action_position_id and not _order_position_id(existing_order):
                    existing_order["_confirmed_position_id"] = action_position_id
                    changed_row = True
                if row.get("identity_conflict_v1") is not None:
                    row["identity_conflict_v1"] = None
                    changed_row = True
            elif (
                existing_stop_id
                and action_stop_id
                and existing_stop_id != action_stop_id
            ):
                if recreated_current:
                    historical = set(row.get("historical_stop_plan_ids") or [])
                    historical.add(action_stop_id)
                    new_historical = sorted(value for value in historical if value)
                    if row.get("historical_stop_plan_ids") != new_historical:
                        row["historical_stop_plan_ids"] = new_historical
                        changed_row = True
                    if row.get("identity_conflict_v1") is not None:
                        row["identity_conflict_v1"] = None
                        changed_row = True
                else:
                    conflict_core = {
                        "unresolved": True,
                        "candidate_stop_plan_ids": sorted(
                            {existing_stop_id, action_stop_id}
                        ),
                        "source": f"canonical_vs_{section}:{action_type}",
                    }
                    existing_conflict = row.get("identity_conflict_v1")
                    existing_core = (
                        {
                            key: value
                            for key, value in existing_conflict.items()
                            if key != "detected_at"
                        }
                        if isinstance(existing_conflict, dict)
                        else {}
                    )
                    if existing_core != conflict_core:
                        row["identity_conflict_v1"] = {
                            **conflict_core,
                            "detected_at": timestamp,
                        }
                        changed_row = True
            elif row.get("identity_conflict_v1") is not None and action_stop_id:
                row["identity_conflict_v1"] = None
                changed_row = True

        if created or not row.get("canonical_source"):
            row["canonical_source"] = f"{section}:{action_type}"
            row["canonicalized_at"] = timestamp
            changed_row = True

        defaults = {
            "filled": False,
            "filled_notified": False,
            "notification_pending": False,
            "notify_attempts": 0,
        }
        for key, value in defaults.items():
            if key not in row:
                row[key] = value
                changed_row = True

        if action_type in _FILLED_TP_ACTION_TYPES:
            fill_fields = {
                "filled": True,
                "fill_source": action_type,
                "exchange_filled_qty": qty,
                "filled_at": action.get("completed_at")
                or action.get("created_at")
                or timestamp,
            }
            for key, value in fill_fields.items():
                if row.get(key) != value:
                    row[key] = value
                    changed_row = True

        if changed_row:
            repaired.add(index)

    # Existing rows without action evidence may still use a valid immutable
    # snapshot to fill missing target/quantity. Snapshot data does not prove that
    # an exchange order exists, so it never creates a new row by itself.
    for index, row in by_index.items():
        snapshot = snapshot_by_index.get(index)
        if not isinstance(snapshot, dict):
            continue
        changed_row = False
        changed_row |= _set_missing(row, "target", _f(snapshot.get("price"), 0.0))
        changed_row |= _set_missing(row, "qty", _f(snapshot.get("qty"), 0.0))
        changed_row |= _set_missing(row, "planned_qty", _f(snapshot.get("qty"), 0.0))
        if changed_row:
            repaired.add(index)

    rows.sort(key=lambda item: _i(item.get("tp_index"), 999999))
    if rows or isinstance(original_value, list):
        payload["tp"] = rows
    else:
        payload.pop("tp", None)
    changed = rows != original_value if isinstance(original_value, list) else bool(rows)
    return rows, changed, sorted(repaired)


def tp_ledger_repair_metadata(
    repaired_indices: list[int],
    *,
    now_iso: str | None = None,
    source: str,
) -> dict[str, Any]:
    """Build a compact durable audit marker for a canonicalization repair."""

    return {
        "version": TP_LEDGER_VERSION,
        "last_repair_at": now_iso or datetime.now(timezone.utc).isoformat(),
        "last_repair_source": str(source),
        "repaired_indices": sorted({int(i) for i in repaired_indices if int(i) > 0}),
    }
