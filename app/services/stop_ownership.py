from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from app.services.tp_plan_snapshot import SNAPSHOT_KEY
from app.services.exchange_identity import clean_exchange_id

LIMIT_ATTACHED_STOP_KEY = "limit_attached_stop_v1"


def _finite_positive(value: Any) -> float:
    """Return a strictly positive finite scalar; corrupted negatives fail closed."""

    try:
        if value in (None, "") or isinstance(value, bool):
            return 0.0
        parsed = float(value)
        return parsed if math.isfinite(parsed) and parsed > 0 else 0.0
    except (TypeError, ValueError, OverflowError):
        return 0.0


def algo_order_id(order: dict[str, Any]) -> str:
    if not isinstance(order, dict):
        return ""
    raw = order.get("raw") if isinstance(order.get("raw"), dict) else {}
    for value in (
        order.get("_confirmed_stop_plan_id"),
        order.get("stopPlanOrderId"),
        order.get("stopOrderId"),
        raw.get("stopPlanOrderId"),
        raw.get("stopOrderId"),
        raw.get("id"),
        order.get("id"),
    ):
        cleaned = clean_exchange_id(value)
        if cleaned:
            return cleaned
    return ""


def entry_order_ids(payload: dict[str, Any]) -> set[str]:
    """Return only regular LIMIT-entry identities stored by this execution."""

    out: set[str] = set()

    def add(value: Any) -> None:
        cleaned = clean_exchange_id(value)
        if cleaned:
            out.add(cleaned)

    entry = payload.get("entry") if isinstance(payload, dict) else None
    if isinstance(entry, dict):
        data = entry.get("data")
        data_dict = data if isinstance(data, dict) else {}
        raw = entry.get("raw") if isinstance(entry.get("raw"), dict) else {}
        for value in (
            entry.get("orderId"),
            entry.get("id"),
            data_dict.get("orderId"),
            data_dict.get("id"),
            raw.get("orderId"),
            raw.get("id"),
        ):
            add(value)
            # BingX order/create commonly returns a scalar order id in data.
        if not isinstance(data, (dict, list)):
            add(data)

    fill = payload.get("limit_fill_status") if isinstance(payload, dict) else None
    if isinstance(fill, dict):
        add(fill.get("order_id"))

    snapshot = payload.get(SNAPSHOT_KEY) if isinstance(payload, dict) else None
    if isinstance(snapshot, dict):
        add(snapshot.get("entry_order_id"))

    return out


def is_limit_execution(payload: dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    if payload.get("post_fill_required") is True:
        return True
    if isinstance(payload.get("limit_fill_status"), dict):
        return True
    if "limit_policy_v1" in payload:
        return True
    snapshot = payload.get(SNAPSHOT_KEY)
    source = str(snapshot.get("source") or "") if isinstance(snapshot, dict) else ""
    return source.startswith("limit_")


def _normalized_side(value: Any) -> str:
    side = str(value or "").strip().upper()
    return side if side in {"LONG", "SHORT"} else ""


def _row_position_matches(order: dict[str, Any], position_id: str, side: str) -> bool:
    wanted_position = clean_exchange_id(position_id)
    row_position = clean_exchange_id(order.get("positionId"))
    wanted_side = _normalized_side(side)
    row_side = _normalized_side(order.get("positionSide") or order.get("side"))
    # Ownership requires all three pieces of evidence.  Missing/unknown side must
    # never be coerced to SHORT and must never be accepted from positionId alone.
    return bool(
        wanted_position
        and row_position == wanted_position
        and wanted_side
        and row_side == wanted_side
    )


def _row_is_stop_only(order: dict[str, Any]) -> bool:
    stop = _finite_positive(
        order.get("stopLossPrice")
        or order.get("triggerPrice")
        or order.get("stopPrice")
    )
    take_profit = _finite_positive(order.get("takeProfitPrice"))
    if stop > 0 and take_profit <= 0:
        return True
    normalized_type = str(order.get("type") or "").upper()
    return "STOP" in normalized_type and "TAKE_PROFIT" not in normalized_type


def _row_stop_price(order: dict[str, Any]) -> float:
    return _finite_positive(
        order.get("stopLossPrice")
        or order.get("triggerPrice")
        or order.get("stopPrice")
    )


def _row_qty(order: dict[str, Any]) -> float:
    return _finite_positive(order.get("qty") or order.get("size"))


def _row_place_order_ids(order: dict[str, Any]) -> set[str]:
    raw = order.get("raw") if isinstance(order.get("raw"), dict) else {}
    return {
        cleaned
        for cleaned in (
            clean_exchange_id(order.get("placeOrderId")),
            clean_exchange_id(raw.get("placeOrderId")),
        )
        if cleaned
    }


def identify_limit_attached_stop(
    orders: list[dict[str, Any]],
    *,
    payload: dict[str, Any],
    position_id: str,
    side: str,
    original_stop: float,
    minimum_qty: float,
    price_tolerance: float,
    qty_tolerance: float,
    excluded_order_ids: set[str] | None = None,
    allow_unique_signature_fallback: bool = False,
) -> dict[str, Any]:
    """Identify the STOP generated from a LIMIT entry's attached SL.

    The strongest proof is BingX's ``placeOrderId`` linking the stop plan to the
    exact regular LIMIT entry id.  A unique position/side/price/quantity signature
    may be used only by the immediate terminal-fill reconciliation when the caller
    explicitly enables it.  Legacy BE cleanup never adopts an unlinked STOP, because
    it may be a manual order created by the user.  Every ambiguity fails closed.
    """

    result: dict[str, Any] = {
        "matched": None,
        "order_id": "",
        "basis": "",
        "candidate_ids": [],
        "ambiguous": False,
        "entry_order_ids": sorted(entry_order_ids(payload)),
    }
    if not is_limit_execution(payload):
        result["reason"] = "not_limit_execution"
        return result

    wanted_stop = _finite_positive(original_stop)
    wanted_qty = _finite_positive(minimum_qty)
    wanted_position = clean_exchange_id(position_id)
    wanted_side = _normalized_side(side)
    if not wanted_position or not wanted_side or wanted_stop <= 0 or wanted_qty <= 0:
        result["reason"] = "missing_or_invalid_identity"
        return result

    excluded = {clean_exchange_id(value) for value in (excluded_order_ids or set())}
    excluded.discard("")
    price_tol = max(_finite_positive(price_tolerance), wanted_stop * 1e-9, 1e-12)
    qty_tol = max(_finite_positive(qty_tolerance), 1e-12)

    candidates: list[dict[str, Any]] = []
    for row in orders or []:
        if not isinstance(row, dict):
            continue
        order_id = algo_order_id(row)
        if not order_id or order_id in excluded:
            continue
        if not _row_position_matches(row, wanted_position, wanted_side):
            continue
        if not _row_is_stop_only(row):
            continue
        row_stop = _row_stop_price(row)
        if row_stop <= 0 or abs(row_stop - wanted_stop) > price_tol:
            continue
        row_qty = _row_qty(row)
        if row_qty <= 0 or row_qty + qty_tol < wanted_qty:
            continue
        candidates.append(row)

    result["candidate_ids"] = sorted(
        {algo_order_id(row) for row in candidates if algo_order_id(row)}
    )
    if not candidates:
        result["reason"] = "no_exact_candidate"
        return result

    entry_ids = set(result["entry_order_ids"])
    linked = [
        row
        for row in candidates
        if entry_ids and _row_place_order_ids(row).intersection(entry_ids)
    ]
    if len(linked) == 1:
        matched = linked[0]
        result.update(
            {
                "matched": matched,
                "order_id": algo_order_id(matched),
                "basis": "entry_place_order_id",
                "reason": "exact_entry_link",
            }
        )
        return result
    if len(linked) > 1:
        result.update(
            {
                "ambiguous": True,
                "reason": "multiple_entry_linked_candidates",
            }
        )
        return result

    if len(candidates) == 1 and allow_unique_signature_fallback:
        matched = candidates[0]
        row_qty = _row_qty(matched)
        # The fallback is allowed only immediately after a terminal fill and only
        # when the stop quantity exactly matches the confirmed filled quantity.
        if abs(row_qty - wanted_qty) <= qty_tol:
            result.update(
                {
                    "matched": matched,
                    "order_id": algo_order_id(matched),
                    "basis": "unique_terminal_fill_signature",
                    "reason": "unique_exact_terminal_fill_candidate",
                }
            )
            return result
        result.update(
            {
                "ambiguous": True,
                "reason": "unlinked_candidate_qty_not_exact",
            }
        )
        return result

    result.update(
        {
            "ambiguous": True,
            "reason": (
                "unlinked_exact_candidate_not_owned"
                if len(candidates) == 1
                else "multiple_exact_candidates"
            ),
        }
    )
    return result



def identify_initial_protective_stop(
    orders: list[dict[str, Any]],
    *,
    payload: dict[str, Any],
    position_id: str,
    side: str,
    original_stop: float,
    minimum_qty: float,
    price_tolerance: float,
    qty_tolerance: float,
    excluded_order_ids: set[str] | None = None,
) -> dict[str, Any]:
    """Identify the initial MARKET/fallback protective STOP for BE replacement.

    BingX can later return the bot-created STOP without ``positionId``.  The
    BE writer may accept that unscoped STOP only when a unique live STOP row
    matches the immutable trade evidence: same symbol scope from the caller,
    same position side, original stop price, and enough quantity to protect the
    current remainder.  LIMIT-attached STOPs use ``identify_limit_attached_stop``
    because they have a separate entry/placeOrder identity path.  Every
    ambiguity fails closed.
    """

    result: dict[str, Any] = {
        "matched": None,
        "order_id": "",
        "basis": "",
        "candidate_ids": [],
        "ambiguous": False,
    }
    if is_limit_execution(payload):
        result["reason"] = "limit_execution_uses_limit_attached_identity"
        return result

    wanted_stop = _finite_positive(original_stop)
    wanted_qty = _finite_positive(minimum_qty)
    wanted_position = clean_exchange_id(position_id)
    wanted_side = _normalized_side(side)
    if not wanted_side or wanted_stop <= 0 or wanted_qty <= 0:
        result["reason"] = "missing_or_invalid_identity"
        return result

    excluded = {clean_exchange_id(value) for value in (excluded_order_ids or set())}
    excluded.discard("")
    price_tol = max(_finite_positive(price_tolerance), wanted_stop * 1e-9, 1e-12)
    qty_tol = max(_finite_positive(qty_tolerance), 1e-12)

    candidates: list[dict[str, Any]] = []
    for row in orders or []:
        if not isinstance(row, dict):
            continue
        order_id = algo_order_id(row)
        if not order_id or order_id in excluded:
            continue
        row_side = _normalized_side(row.get("positionSide") or row.get("side"))
        if row_side != wanted_side:
            continue
        row_position = clean_exchange_id(row.get("positionId"))
        if wanted_position and row_position and row_position != wanted_position:
            continue
        if not _row_is_stop_only(row):
            continue
        row_stop = _row_stop_price(row)
        if row_stop <= 0 or abs(row_stop - wanted_stop) > price_tol:
            continue
        row_qty = _row_qty(row)
        if row_qty <= 0 or row_qty + qty_tol < wanted_qty:
            continue
        candidates.append(row)

    result["candidate_ids"] = sorted(
        {algo_order_id(row) for row in candidates if algo_order_id(row)}
    )
    if len(candidates) == 1:
        matched = candidates[0]
        result.update(
            {
                "matched": matched,
                "order_id": algo_order_id(matched),
                "basis": "unique_initial_stop_signature",
                "reason": "unique_exact_initial_stop_candidate",
            }
        )
        return result
    if not candidates:
        result["reason"] = "no_exact_candidate"
        return result
    result.update({"ambiguous": True, "reason": "multiple_exact_candidates"})
    return result


def build_initial_stop_record(
    ownership: dict[str, Any],
    *,
    position_id: str,
    original_stop: float,
) -> dict[str, Any]:
    row = ownership.get("matched") if isinstance(ownership, dict) else None
    if not isinstance(row, dict):
        return {}
    order_id = algo_order_id(row)
    stop_price = _row_stop_price(row) or _finite_positive(original_stop)
    qty = _row_qty(row)
    position_side = _normalized_side(row.get("positionSide") or row.get("side"))
    if not order_id or not position_side or stop_price <= 0 or qty <= 0:
        return {}
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    return {
        "stopPlanOrderId": order_id,
        "positionId": clean_exchange_id(position_id),
        "positionSide": position_side,
        "stopLossPrice": stop_price,
        "qty": qty,
        "match_basis": str(ownership.get("basis") or ""),
        "claimed_at": datetime.now(timezone.utc).isoformat(),
        "raw_order_id": clean_exchange_id(row.get("id") or raw.get("id")),
    }

def build_limit_attached_stop_record(
    ownership: dict[str, Any],
    *,
    position_id: str,
    original_stop: float,
) -> dict[str, Any]:
    row = ownership.get("matched") if isinstance(ownership, dict) else None
    if not isinstance(row, dict):
        return {}
    order_id = algo_order_id(row)
    normalized_position_id = clean_exchange_id(position_id)
    stop_price = _row_stop_price(row) or _finite_positive(original_stop)
    qty = _row_qty(row)
    position_side = _normalized_side(row.get("positionSide") or row.get("side"))
    if (
        not order_id
        or not normalized_position_id
        or not position_side
        or stop_price <= 0
        or qty <= 0
    ):
        return {}
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    return {
        "stopPlanOrderId": order_id,
        "positionId": normalized_position_id,
        "positionSide": position_side,
        "stopLossPrice": stop_price,
        "qty": qty,
        "placeOrderId": clean_exchange_id(
            row.get("placeOrderId") or raw.get("placeOrderId")
        ),
        "match_basis": str(ownership.get("basis") or ""),
        "entry_order_ids": list(ownership.get("entry_order_ids") or []),
        "claimed_at": datetime.now(timezone.utc).isoformat(),
    }
