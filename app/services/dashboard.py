"""Home dashboard aggregation and durable trade outcome classification."""

from __future__ import annotations

import math
import json
from typing import Any, Iterable

from app.services.tp_execution_ledger import canonicalize_tp_ledger
from app.services.execution_exposure import (
    execution_be_protection_confirmed,
    execution_zero_exposure_confirmed,
)

ACTIVE_POSITION_STATUSES = {
    "opened",
    "protected",
    "partial_error",
    "manual_required",
    "partial_unrecoverable",
}
ACTIVE_STATUSES = ACTIVE_POSITION_STATUSES | {"pending_limit"}
CLOSED_STATUSES = {
    "closed_on_exchange",
    "closed_stop_catchup",
    "closed_on_exchange_cleanup",
}
KNOWN_OUTCOMES = {"win", "breakeven", "loss"}


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    try:
        value = row.get("exchange_order_ids_json") or "{}"
        parsed = value if isinstance(value, dict) else json.loads(value)
    except Exception:
        return {}
    if not isinstance(parsed, dict):
        return {}
    canonicalize_tp_ledger(parsed)
    return parsed


def _float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError, OverflowError):
        return None


def outcome_from_close(
    *, close_type: str, realized_pnl: float, be_was_set: bool = False
) -> str:
    """Classify one completed trade for dashboard statistics.

    The user's primary rule is explicit: a BE close counts as a victory. We still
    store BE separately so the strict profitable-only winrate remains visible.
    """
    kind = str(close_type or "unknown").strip().lower()
    pnl = float(realized_pnl or 0.0)
    if kind == "be_stop":
        return "breakeven"
    if kind == "all_tps":
        return "win"
    if kind == "stop":
        return "loss"
    if pnl > 0.01:
        return "win"
    if pnl < -0.01:
        return "loss"
    # A near-zero result counts as BE only when the bot actually recorded BE
    # protection. Unknown/manual closes with a missing price often calculate to
    # 0.0; classifying those as wins would inflate the user's winrate.
    if be_was_set:
        return "breakeven"
    return "unknown"


def classify_closed_row(row: dict[str, Any]) -> str:
    """Return win/breakeven/loss/unknown, including safe legacy fallbacks."""
    stored = str(row.get("outcome") or "").strip().lower()
    if stored in KNOWN_OUTCOMES:
        return stored

    close_type = str(row.get("close_type") or "").strip().lower()
    pnl = _float(row.get("realized_pnl"))
    payload = _payload(row)
    be_moved = execution_be_protection_confirmed(payload)

    if close_type:
        return outcome_from_close(
            close_type=close_type,
            realized_pnl=float(pnl or 0.0),
            be_was_set=be_moved,
        )
    if pnl is not None:
        return outcome_from_close(
            close_type="unknown",
            realized_pnl=pnl,
            be_was_set=be_moved,
        )

    # Historical v1.5.8 rows did not have durable result columns. Infer only
    # high-confidence outcomes; ambiguous closes remain unknown and are excluded
    # from the denominator rather than fabricating a win/loss.
    if be_moved:
        return "breakeven"

    tp_rows = payload.get("tp") if isinstance(payload.get("tp"), list) else []
    filled = [
        tp for tp in tp_rows if isinstance(tp, dict) and tp.get("filled") is True
    ]
    try:
        targets = json.loads(row.get("targets_json") or "[]")
    except Exception:
        targets = []
    if targets and len(filled) >= len(targets):
        return "win"

    status = str(row.get("status") or "").strip().lower()
    reason = str(row.get("reason") or "").strip().lower()
    if status == "closed_stop_catchup":
        return "loss"
    if "stop" in reason and "before stop" not in reason and "stop не" not in reason:
        return "loss"
    return "unknown"


def row_is_closed(row: dict[str, Any]) -> bool:
    status = str(row.get("status") or "").strip().lower()
    # Active-family rows can carry a stale outcome/closed_at from an earlier
    # lifecycle pass. Never hide contradictory live/reverse exposure from the
    # dashboard. Conversely, legacy manual_required rows with durable zero-
    # exposure evidence must not remain displayed as open forever.
    if status in ACTIVE_STATUSES:
        return execution_zero_exposure_confirmed(row)
    outcome = str(row.get("outcome") or "").strip().lower()
    return (
        execution_zero_exposure_confirmed(row)
        or bool(row.get("closed_at"))
        or outcome in (KNOWN_OUTCOMES | {"unknown"})
        or status in CLOSED_STATUSES
    )


def row_has_confirmed_stop(row: dict[str, Any]) -> bool:
    """Conservative DB-side STOP confirmation for the home dashboard."""
    if row_is_closed(row):
        return False
    status = str(row.get("status") or "").strip().lower()
    if status == "protected":
        return True
    payload = _payload(row)
    if execution_be_protection_confirmed(payload):
        return True
    entry = payload.get("entry") if isinstance(payload.get("entry"), dict) else {}
    post_fill_stop = (
        entry.get("_post_fill_stop")
        if isinstance(entry.get("_post_fill_stop"), dict)
        else {}
    )
    if post_fill_stop.get("confirmed") is True:
        return True
    # partial_error occurs only after entry protection is confirmed and one or
    # more TP writes fail in the current executor flow.
    if status == "partial_error":
        return True
    return False


def summarize_dashboard(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    rows = list(rows)
    active_rows = [
        row
        for row in rows
        if str(row.get("status") or "").strip().lower() in ACTIVE_STATUSES
        and not row_is_closed(row)
    ]
    pending_limits = sum(
        1
        for row in active_rows
        if str(row.get("status") or "").lower() == "pending_limit"
    )
    position_rows = [
        row
        for row in active_rows
        if str(row.get("status") or "").strip().lower() in ACTIVE_POSITION_STATUSES
    ]
    protected = sum(1 for row in position_rows if row_has_confirmed_stop(row))
    unprotected = max(0, len(position_rows) - protected)
    manual_review = sum(
        1
        for row in active_rows
        if str(row.get("status") or "").strip().lower() == "manual_required"
    )

    closed_rows = [
        row
        for row in rows
        if "dashboard_stats_eligible" in row
        and bool(row.get("dashboard_stats_eligible"))
        and row_is_closed(row)
    ]
    # Unit tests and callers that provide plain rows (without the DB eligibility
    # marker) retain the intuitive fallback behavior.
    if not any("dashboard_stats_eligible" in row for row in rows):
        closed_rows = [row for row in rows if row_is_closed(row)]
    outcomes = [classify_closed_row(row) for row in closed_rows]
    wins = outcomes.count("win")
    breakevens = outcomes.count("breakeven")
    losses = outcomes.count("loss")
    unknown = outcomes.count("unknown")
    known = wins + breakevens + losses
    winrate = ((wins + breakevens) / known * 100.0) if known else None
    clean_known = wins + losses
    clean_winrate = (wins / clean_known * 100.0) if clean_known else None

    return {
        "active_positions": len(position_rows),
        "pending_limits": pending_limits,
        "active_total": len(active_rows),
        "protected_positions": protected,
        "unprotected_positions": unprotected,
        "manual_review": manual_review,
        "closed_total": len(closed_rows),
        "known_closed": known,
        "wins": wins,
        "breakevens": breakevens,
        "losses": losses,
        "unknown": unknown,
        "winrate": winrate,
        "clean_winrate": clean_winrate,
    }
