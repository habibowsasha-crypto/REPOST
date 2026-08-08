from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from app.services.execution_exposure import execution_zero_exposure_confirmed
from app.services.exchange_identity import clean_exchange_id

EVIDENCE_SCHEMA_VERSION = 1
SHADOW_MODEL_VERSION = 1

_ENTRY_TERMINAL_CANCEL_STATES = {"CANCELLED", "CANCELED", "EXPIRED", "REJECTED"}
_ENTRY_FILLED_STATUSES = {
    "opened",
    "protected",
    "partial_error",
    "partial_unrecoverable",
    "closed_pending_history",
    "closed_on_exchange",
    "closed_stop_catchup",
}

_TP_EXACT_FILL_SOURCES = {
    "bingx_order_history",
    "exchange_order_history",
    "fill_history",
    "mexc_stoporder_history",
    "order_detail",
    "stoporder_history",
}
_TP_COMPOSITE_FILL_SOURCES = {
    "position_qty_plus_price_touch",
}


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("exchange_order_ids_json")
    if isinstance(raw, dict):
        return dict(raw)
    try:
        parsed = json.loads(str(raw or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return float(default)
    return parsed if math.isfinite(parsed) else float(default)


def _positive(*values: Any) -> float:
    for value in values:
        parsed = _finite(value, 0.0)
        if parsed > 0:
            return parsed
    return 0.0


def _text(value: Any) -> str:
    return str(value or "").strip()


def _nested_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _order_id(value: Any) -> str:
    if not isinstance(value, dict):
        return ""
    data = _nested_dict(value.get("data"))
    return clean_exchange_id(
        value.get("orderId")
        or value.get("orderID")
        or value.get("order_id")
        or value.get("id")
        or value.get("_confirmed_order_id")
        or value.get("_confirmed_stop_plan_id")
        or data.get("orderId")
        or data.get("orderID")
        or data.get("order_id")
        or data.get("id")
    )


def _status_text(*values: Any) -> str:
    for value in values:
        text = _text(value).upper()
        if text:
            return text
    return ""


def _entry_fill_candidates(payload: dict[str, Any], entry: dict[str, Any]) -> Iterable[Any]:
    opening = _nested_dict(payload.get("opening_intent_reconciliation_v1"))
    reconciliation = _nested_dict(payload.get("limit_fill_reconciliation_v1"))
    fill_status = _nested_dict(payload.get("entry_fill_status"))
    yield entry.get("executedQty")
    yield entry.get("executed_qty")
    yield entry.get("filledQty")
    yield entry.get("filled_qty")
    yield entry.get("cumQty")
    yield entry.get("cum_qty")
    yield entry.get("dealSize")
    yield entry.get("deal_size")
    yield fill_status.get("filled_qty")
    yield reconciliation.get("filled_qty")
    yield opening.get("filled_qty")
    yield payload.get("actual_entry_qty")


def _entry_requested_candidates(row: dict[str, Any], payload: dict[str, Any], entry: dict[str, Any]) -> Iterable[Any]:
    opening = _nested_dict(payload.get("opening_intent_reconciliation_v1"))
    yield entry.get("origQty")
    yield entry.get("orig_qty")
    yield entry.get("quantity")
    yield entry.get("qty")
    yield entry.get("_submitted_quantity")
    yield opening.get("requested_qty")
    yield opening.get("intent_qty")
    yield row.get("planned_entry_qty")
    yield row.get("qty")


def _derive_entry_state(row: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    status = _text(row.get("status")).lower()
    entry = _nested_dict(payload.get("entry"))
    fill_status = _nested_dict(payload.get("entry_fill_status"))
    reconciliation = _nested_dict(payload.get("limit_fill_reconciliation_v1"))
    opening = _nested_dict(payload.get("opening_intent_reconciliation_v1"))

    requested_qty = _positive(*_entry_requested_candidates(row, payload, entry))
    filled_qty = max(
        [_finite(value, 0.0) for value in _entry_fill_candidates(payload, entry)]
        or [0.0]
    )
    if filled_qty <= 0 and status in _ENTRY_FILLED_STATUSES:
        filled_qty = _positive(payload.get("actual_entry_qty"), row.get("qty"))
    if requested_qty <= 0 and filled_qty > 0:
        requested_qty = filled_qty
    remaining_qty = max(0.0, requested_qty - filled_qty) if requested_qty > 0 else 0.0

    exchange_status = _status_text(
        entry.get("status"),
        entry.get("state"),
        fill_status.get("status"),
        fill_status.get("state"),
        reconciliation.get("status"),
        opening.get("status"),
    )
    terminal = bool(
        entry.get("terminal") is True
        or fill_status.get("terminal") is True
        or reconciliation.get("terminal") is True
        or opening.get("terminal") is True
    )
    prior_live = _nested_dict(payload.get("g55_prior_live_position_evidence_v1"))
    prior_live_confirmed = bool(
        prior_live.get("confirmed") is True
        and int(_finite(prior_live.get("execution_id"), 0.0))
        == int(_finite(row.get("id"), 0.0))
        and int(_finite(prior_live.get("user_id"), 0.0))
        == int(_finite(row.get("user_id"), 0.0))
        and _text(prior_live.get("symbol")).upper() == _text(row.get("symbol")).upper()
        and _text(prior_live.get("side")).lower() == _text(row.get("side")).lower()
        and _positive(prior_live.get("qty")) > 0
    )
    fully_filled = bool(
        entry.get("fully_filled") is True
        or fill_status.get("fully_filled") is True
        or reconciliation.get("fully_filled") is True
        or opening.get("fully_filled") is True
    )

    if fully_filled or (requested_qty > 0 and filled_qty >= requested_qty - 1e-12):
        state = "FILLED"
    elif prior_live_confirmed:
        # G55 target-specific reviewed production evidence: a real live position
        # proves ENTRY occurred even when the old runtime row never persisted the
        # exact fill snapshot. This prevents a later zero-exposure read from being
        # mislabeled as ENTRY_NEVER_FILLED while exact financial history is rebuilt.
        state = "FILLED"
    elif filled_qty > 0 and (remaining_qty > 1e-12 or status == "pending_limit"):
        # A cancelled/expired remainder does not erase the already filled part.
        state = "PARTIALLY_FILLED"
    elif exchange_status in _ENTRY_TERMINAL_CANCEL_STATES or (
        terminal and filled_qty <= 0
    ):
        # Exact persisted exchange terminal evidence must outrank a stale local
        # pending_limit/manual_required status when no entry quantity filled.
        if exchange_status in {"CANCELLED", "CANCELED"}:
            state = "CANCELLED"
        elif exchange_status in {"EXPIRED", "REJECTED"}:
            state = exchange_status
        else:
            state = "CANCELLED"
    elif status == "pending_limit":
        state = "PENDING"
    elif status == "opening_intent":
        state = "PENDING"
    elif status in _ENTRY_FILLED_STATUSES:
        state = "FILLED"
    elif status == "manual_required":
        state = "MANUAL_REVIEW"
    elif execution_zero_exposure_confirmed(row):
        state = "UNKNOWN"
    else:
        state = "UNKNOWN"

    return {
        "state": state,
        "order_id": _order_id(entry),
        "requested_qty": requested_qty,
        "filled_qty": filled_qty,
        "remaining_qty": remaining_qty,
        "exchange_status": exchange_status,
        "terminal": terminal,
        "fully_filled": fully_filled,
        "prior_live_position_confirmed": prior_live_confirmed,
    }


def _tp_item(payload: dict[str, Any], level: int) -> dict[str, Any]:
    wanted = max(1, int(level or 1))
    rows = payload.get("tp") if isinstance(payload.get("tp"), list) else []
    for fallback, item in enumerate(rows, 1):
        if not isinstance(item, dict):
            continue
        try:
            index = int(item.get("tp_index") or fallback)
        except (TypeError, ValueError, OverflowError):
            continue
        if index == wanted:
            return dict(item)
    return {}


def _tp_fill_evidence_kind(item: dict[str, Any], order: dict[str, Any], filled_qty: float) -> str:
    source = _text(item.get("fill_source")).lower()
    identity = _nested_dict(item.get("financial_fill_identity_v1"))
    exchange_check = _nested_dict(item.get("exchange_fill_check"))
    order_status = _status_text(order.get("status"), order.get("state"))
    order_executed = max(
        _finite(order.get("executedQty"), 0.0),
        _finite(order.get("filledQty"), 0.0),
    )
    if identity.get("ownership_confirmed") is True:
        return "EXACT"
    if exchange_check.get("accepted_as_owned_tp") is True and (
        exchange_check.get("terminal_filled") is True
        or _finite(exchange_check.get("filled_qty"), 0.0) > 0
    ):
        return "EXACT"
    if source in _TP_EXACT_FILL_SOURCES:
        return "EXACT"
    if source in _TP_COMPOSITE_FILL_SOURCES:
        return "COMPOSITE"
    if _finite(item.get("exchange_filled_qty"), 0.0) > 0 and (
        _order_id(order) or _order_id(item)
    ):
        return "EXACT"
    if order_executed > 0 and order_status in {
        "FILLED",
        "PARTIALLY_FILLED",
        "PARTIALLYFILLED",
        "SUCCESS",
        "COMPLETED",
    }:
        return "EXACT"
    if filled_qty > 0 or item.get("filled") is True:
        return "PERSISTED_FLAG"
    return "NONE"


def _derive_tp_state(row: dict[str, Any], payload: dict[str, Any], level: int) -> dict[str, Any]:
    item = _tp_item(payload, level)
    order = _nested_dict(item.get("order"))
    expected_qty = _positive(
        item.get("qty"),
        item.get("actual_tp_qty"),
        item.get("planned_qty"),
        order.get("quantity"),
        order.get("qty"),
    )
    filled_qty = max(
        _finite(item.get("exchange_filled_qty"), 0.0),
        _finite(item.get("filled_qty"), 0.0),
        _finite(item.get("executedQty"), 0.0),
        _finite(order.get("executedQty"), 0.0),
        _finite(order.get("filledQty"), 0.0),
    )
    if item.get("filled") is True and filled_qty <= 0:
        filled_qty = expected_qty
    if expected_qty <= 0 and filled_qty > 0:
        expected_qty = filled_qty
    remaining_qty = max(0.0, expected_qty - filled_qty) if expected_qty > 0 else 0.0
    exchange_status = _status_text(
        item.get("exchange_status"),
        item.get("status"),
        item.get("state"),
        order.get("status"),
        order.get("state"),
    )
    terminal = bool(item.get("terminal") is True or order.get("terminal") is True)
    fill_evidence_kind = _tp_fill_evidence_kind(item, order, filled_qty)

    if not item:
        state = "NOT_ARMED"
    elif item.get("filled") is True or (expected_qty > 0 and filled_qty >= expected_qty - 1e-12):
        state = "FILLED"
    elif filled_qty > 0:
        state = "PARTIALLY_FILLED"
    elif exchange_status in {"CANCELLED", "CANCELED", "EXPIRED"}:
        state = "CANCELLED" if exchange_status != "EXPIRED" else "EXPIRED"
    elif terminal:
        state = "NOT_FILLED"
    elif _order_id(order) or _order_id(item):
        state = "ARMED"
    else:
        state = "UNKNOWN"

    return {
        "level_index": max(1, int(level or 1)),
        "state": state,
        "order_id": _order_id(order) or _order_id(item),
        "expected_qty": expected_qty,
        "filled_qty": filled_qty,
        "remaining_qty": remaining_qty,
        "exchange_status": exchange_status,
        "terminal": terminal,
        "fill_source": _text(item.get("fill_source")),
        "fill_evidence_kind": fill_evidence_kind,
        "fill_confirmed_at": _text(
            item.get("fill_confirmed_at")
            or item.get("filled_at")
            or item.get("exchange_fill_at")
        ),
    }


def _decision(event_type: str, execution_states: list[dict[str, Any]]) -> tuple[str, str]:
    kind = _text(event_type).upper()
    if not execution_states:
        return "NO_EXECUTIONS_SHADOW", "no_linked_execution_rows"
    entry_states = {str(item["entry"]["state"]) for item in execution_states}
    tp_states = {str(item["tp"]["state"]) for item in execution_states}
    zero_exposure = all(bool(item.get("zero_exposure")) for item in execution_states)

    if kind == "TP":
        evidence_kinds = {
            str(item["tp"].get("fill_evidence_kind") or "NONE")
            for item in execution_states
        }
        all_filled = bool(tp_states and tp_states <= {"FILLED"})
        has_entry_remainder = bool(
            "PARTIALLY_FILLED" in entry_states or "PENDING" in entry_states
        )
        if all_filled and evidence_kinds and evidence_kinds <= {"EXACT"}:
            if has_entry_remainder:
                return (
                    "ENTRY_PARTIAL_TP_FILLED_SHADOW",
                    "tp_fill_exact_entry_remainder_separate",
                )
            return "TP_FILLED_EXACT_SHADOW", "all_linked_tp_rows_exactly_filled"
        if all_filled and evidence_kinds and evidence_kinds <= {"EXACT", "COMPOSITE"}:
            if has_entry_remainder:
                return (
                    "ENTRY_PARTIAL_TP_FILLED_COMPOSITE_SHADOW",
                    "tp_fill_composite_entry_remainder_separate",
                )
            return (
                "TP_FILLED_COMPOSITE_SHADOW",
                "all_linked_tp_rows_have_exact_or_composite_evidence",
            )
        if all_filled:
            return (
                "TP_FILL_FLAG_UNVERIFIED_SHADOW",
                "filled_flag_without_classified_exchange_or_composite_evidence",
            )
        if "PARTIALLY_FILLED" in tp_states or "FILLED" in tp_states:
            return "TP_PARTIAL_OR_MIXED_SHADOW", "some_linked_tp_rows_have_fill_evidence"
        if zero_exposure:
            return "MANUAL_REVIEW_CANDIDATE", "zero_exposure_without_exact_tp_fill"
        if entry_states and entry_states <= {"PENDING", "PARTIALLY_FILLED"}:
            return "ENTRY_REMAINDER_ACTIVE_SHADOW", "entry_pending_tp_fill_unconfirmed"
        return "TP_UNCONFIRMED_SHADOW", "exact_tp_fill_not_persisted"

    if kind == "ENTRY":
        if entry_states and entry_states <= {"FILLED"}:
            return "ENTRY_FILLED_SHADOW", "all_linked_entries_filled"
        if "PARTIALLY_FILLED" in entry_states:
            return "ENTRY_PARTIALLY_FILLED_SHADOW", "partial_entry_fill_persisted"
        if entry_states and entry_states <= {"PENDING"}:
            return "ENTRY_PENDING_SHADOW", "entry_still_pending"
        if zero_exposure:
            return "ENTRY_NO_EXPOSURE_SHADOW", "zero_exposure_without_entry_fill"
        return "ENTRY_UNKNOWN_SHADOW", "entry_state_not_exact"

    if kind == "STOP":
        if zero_exposure:
            return "STOP_OR_CLOSE_ZERO_EXPOSURE_SHADOW", "zero_exposure_confirmed"
        return "STOP_UNCONFIRMED_SHADOW", "live_exposure_or_missing_close_evidence"

    return "UNSUPPORTED_EVENT_SHADOW", "unsupported_event_type"


def build_market_event_evidence_snapshot(
    event: dict[str, Any], rows: list[dict[str, Any]]
) -> dict[str, Any]:
    level = max(1, int(event.get("level_index") or 1))
    execution_states: list[dict[str, Any]] = []
    for row in sorted(rows, key=lambda item: (int(item.get("user_id") or 0), int(item.get("id") or 0))):
        payload = _payload(row)
        entry = _derive_entry_state(row, payload)
        tp = _derive_tp_state(row, payload, level)
        execution_states.append(
            {
                "execution_id": int(row.get("id") or 0),
                "user_id": int(row.get("user_id") or 0),
                "symbol": _text(row.get("symbol")).upper(),
                "side": _text(row.get("side")).upper(),
                "execution_status": _text(row.get("status")).lower(),
                "row_updated_at": _text(row.get("updated_at")),
                "zero_exposure": bool(execution_zero_exposure_confirmed(row)),
                "entry": entry,
                "tp": tp,
            }
        )

    decision, reason = _decision(_text(event.get("event_type")), execution_states)
    fingerprint_executions = []
    for item in execution_states:
        stable = dict(item)
        # Database bookkeeping timestamps can change without any new exchange
        # evidence. Excluding them prevents false "changed" history revisions.
        stable.pop("row_updated_at", None)
        fingerprint_executions.append(stable)
    fingerprint_basis = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "event_type": _text(event.get("event_type")).upper(),
        "event_key": _text(event.get("event_key")),
        "level_index": level,
        "executions": fingerprint_executions,
    }
    canonical = json.dumps(
        fingerprint_basis,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    fingerprint = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "event_type": _text(event.get("event_type")).upper(),
        "event_key": _text(event.get("event_key")),
        "level_index": level,
        "executions": execution_states,
        "event_id": int(event.get("id") or 0),
        "trade_group_id": int(event.get("trade_group_id") or 0),
        "observed_price": _finite(event.get("observed_price"), 0.0),
        "trigger_price": _finite(event.get("trigger_price"), 0.0),
        "legacy_status": _text(event.get("status")),
        "legacy_attempts": int(event.get("attempts") or 0),
        "shadow_model_version": SHADOW_MODEL_VERSION,
        "shadow_decision": decision,
        "shadow_reason": reason,
        "evidence_fingerprint": fingerprint,
        "observed_at": datetime.now(timezone.utc).isoformat(),
    }


def execution_state_rows(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    event_id = int(snapshot.get("event_id") or 0)
    level = int(snapshot.get("level_index") or 1)
    result: list[dict[str, Any]] = []
    for item in snapshot.get("executions") or []:
        if not isinstance(item, dict):
            continue
        entry = _nested_dict(item.get("entry"))
        tp = _nested_dict(item.get("tp"))
        result.append(
            {
                "event_id": event_id,
                "execution_id": int(item.get("execution_id") or 0),
                "user_id": int(item.get("user_id") or 0),
                "entry_state": _text(entry.get("state")) or "UNKNOWN",
                "entry_order_id": _text(entry.get("order_id")),
                "entry_requested_qty": _finite(entry.get("requested_qty"), 0.0),
                "entry_filled_qty": _finite(entry.get("filled_qty"), 0.0),
                "entry_remaining_qty": _finite(entry.get("remaining_qty"), 0.0),
                "entry_exchange_status": _text(entry.get("exchange_status")),
                "tp_level_index": level,
                "tp_state": _text(tp.get("state")) or "UNKNOWN",
                "tp_order_id": _text(tp.get("order_id")),
                "tp_expected_qty": _finite(tp.get("expected_qty"), 0.0),
                "tp_filled_qty": _finite(tp.get("filled_qty"), 0.0),
                "tp_remaining_qty": _finite(tp.get("remaining_qty"), 0.0),
                "tp_exchange_status": _text(tp.get("exchange_status")),
                "zero_exposure": 1 if item.get("zero_exposure") else 0,
                "source_row_updated_at": _text(item.get("row_updated_at")),
                "evidence_fingerprint": _text(snapshot.get("evidence_fingerprint")),
                "evidence_json": json.dumps(
                    item,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ),
            }
        )
    return result


@dataclass(frozen=True)
class MarketEventStateMachineDecision:
    """Pure g40 decision for one ENTRY/TP evidence observation.

    The decision is intentionally independent from database and exchange I/O.
    It consumes the g39 immutable evidence snapshot plus durable counters and
    returns one finite transition. STOP events are explicitly excluded because
    protective STOP reconciliation must retain its existing safety path.
    """

    applicable: bool
    terminal: bool
    manual_review: bool
    phase: str
    outcome: str
    reason: str
    retry_after_sec: float
    fast_attempts: int
    deep_attempts: int
    final_attempts: int
    total_attempts: int


_EXACT_TERMINAL_OUTCOMES = {
    "TP_FILLED_EXACT_SHADOW": "TP_FILLED_EXACT",
    "ENTRY_PARTIAL_TP_FILLED_SHADOW": "ENTRY_PARTIAL_TP_FILLED",
    "TP_FILLED_COMPOSITE_SHADOW": "TP_FILLED_COMPOSITE",
    "ENTRY_PARTIAL_TP_FILLED_COMPOSITE_SHADOW": "ENTRY_PARTIAL_TP_FILLED_COMPOSITE",
    "ENTRY_FILLED_SHADOW": "ENTRY_FILLED",
}


def decide_market_event_state_machine(
    event: dict[str, Any],
    snapshot: dict[str, Any],
    *,
    max_fast_attempts: int = 3,
    max_deep_attempts: int = 2,
    max_final_attempts: int = 1,
) -> MarketEventStateMachineDecision:
    """Return the next finite g40 transition without mutating anything.

    Attempt schedule (after the current observation):
      fast #1 -> 10s; fast #2 -> 30s; fast #3 -> deep pending in 60s;
      deep #1 -> 180s; deep #2 -> final pending in 300s;
      final #1 unresolved -> terminal MANUAL_REVIEW.

    Exact exchange-backed TP evidence can terminate even while ENTRY has a
    remaining quantity. Price touch alone never maps to a terminal success.
    """

    event_type = _text(event.get("event_type")).upper()
    fast_limit = max(1, int(max_fast_attempts or 3))
    deep_limit = max(1, int(max_deep_attempts or 2))
    final_limit = max(1, int(max_final_attempts or 1))
    fast = max(0, int(event.get("fast_attempts") or 0))
    deep = max(0, int(event.get("deep_attempts") or 0))
    final = max(0, int(event.get("final_attempts") or 0))
    total_before = fast + deep + final

    if event_type not in {"ENTRY", "TP"}:
        return MarketEventStateMachineDecision(
            applicable=False,
            terminal=False,
            manual_review=False,
            phase="LEGACY",
            outcome="LEGACY_UNCHANGED",
            reason="protective_or_unsupported_event_type",
            retry_after_sec=0.0,
            fast_attempts=fast,
            deep_attempts=deep,
            final_attempts=final,
            total_attempts=total_before,
        )

    shadow_decision = _text(snapshot.get("shadow_decision")).upper()
    shadow_reason = _text(snapshot.get("shadow_reason")) or "evidence_unresolved"
    exact_outcome = _EXACT_TERMINAL_OUTCOMES.get(shadow_decision)
    if exact_outcome:
        return MarketEventStateMachineDecision(
            applicable=True,
            terminal=True,
            manual_review=False,
            phase="COMPLETED",
            outcome=exact_outcome,
            reason=shadow_reason,
            retry_after_sec=0.0,
            fast_attempts=fast,
            deep_attempts=deep,
            final_attempts=final,
            total_attempts=total_before,
        )

    # No execution can prove ENTRY never existed only when the evidence model
    # explicitly confirms no exposure. Generic missing rows remain ambiguous.
    if event_type == "ENTRY" and shadow_decision == "ENTRY_NO_EXPOSURE_SHADOW":
        return MarketEventStateMachineDecision(
            applicable=True,
            terminal=True,
            manual_review=False,
            phase="COMPLETED",
            outcome="ENTRY_NEVER_FILLED",
            reason=shadow_reason,
            retry_after_sec=0.0,
            fast_attempts=fast,
            deep_attempts=deep,
            final_attempts=final,
            total_attempts=total_before,
        )

    if fast < fast_limit:
        fast += 1
        if fast < fast_limit:
            retry = 10.0 if fast == 1 else 30.0
            phase = "FAST_CHECK_PENDING"
        else:
            retry = 60.0
            phase = "DEEP_CHECK_PENDING"
    elif deep < deep_limit:
        deep += 1
        if deep < deep_limit:
            retry = 180.0
            phase = "DEEP_CHECK_PENDING"
        else:
            retry = 300.0
            phase = "FINAL_CHECK_PENDING"
    elif final < final_limit:
        final += 1
        return MarketEventStateMachineDecision(
            applicable=True,
            terminal=True,
            manual_review=True,
            phase="MANUAL_REVIEW",
            outcome="UNKNOWN",
            reason=shadow_reason,
            retry_after_sec=0.0,
            fast_attempts=fast,
            deep_attempts=deep,
            final_attempts=final,
            total_attempts=fast + deep + final,
        )
    else:
        # A restart or older deployment may already have exhausted counters.
        return MarketEventStateMachineDecision(
            applicable=True,
            terminal=True,
            manual_review=True,
            phase="MANUAL_REVIEW",
            outcome="UNKNOWN",
            reason="attempt_budget_already_exhausted",
            retry_after_sec=0.0,
            fast_attempts=fast,
            deep_attempts=deep,
            final_attempts=final,
            total_attempts=total_before,
        )

    return MarketEventStateMachineDecision(
        applicable=True,
        terminal=False,
        manual_review=False,
        phase=phase,
        outcome="EVIDENCE_UNRESOLVED",
        reason=shadow_reason,
        retry_after_sec=retry,
        fast_attempts=fast,
        deep_attempts=deep,
        final_attempts=final,
        total_attempts=fast + deep + final,
    )
