from __future__ import annotations

from typing import Any

from app.services.exchange_identity import clean_exchange_id


def _is_dict(value: Any) -> bool:
    return isinstance(value, dict)


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _order_id(order: Any) -> str:
    if not isinstance(order, dict):
        return ""
    data = order.get("data") if isinstance(order.get("data"), dict) else {}
    nested = data.get("order") if isinstance(data.get("order"), dict) else {}
    raw = order.get("raw") if isinstance(order.get("raw"), dict) else {}
    for value in (
        order.get("_confirmed_order_id"),
        order.get("orderId"),
        order.get("orderID"),
        order.get("id"),
        data.get("orderId"),
        data.get("orderID"),
        nested.get("orderId"),
        nested.get("orderID"),
        raw.get("orderId"),
        raw.get("orderID"),
    ):
        cleaned = clean_exchange_id(value)
        if cleaned:
            return cleaned
    return ""


def _client_id(order: Any) -> str:
    if not isinstance(order, dict):
        return ""
    data = order.get("data") if isinstance(order.get("data"), dict) else {}
    nested = data.get("order") if isinstance(data.get("order"), dict) else {}
    raw = order.get("raw") if isinstance(order.get("raw"), dict) else {}
    for value in (
        order.get("clientOrderID"),
        order.get("clientOrderId"),
        data.get("clientOrderID"),
        data.get("clientOrderId"),
        nested.get("clientOrderID"),
        nested.get("clientOrderId"),
        raw.get("clientOrderID"),
        raw.get("clientOrderId"),
    ):
        cleaned = clean_exchange_id(value)
        if cleaned:
            return cleaned
    return ""


def _qty(order: Any) -> float:
    if not isinstance(order, dict):
        return 0.0
    for key in ("_submitted_quantity", "executedQty", "origQty", "quantity", "qty"):
        value = order.get(key)
        try:
            if value not in (None, "") and not isinstance(value, bool):
                parsed = float(value)
                if parsed > 0:
                    return parsed
        except (TypeError, ValueError, OverflowError):
            pass
    return 0.0


def _bool(value: Any) -> bool:
    return bool(value) if value is not None else False


def summarize_write_order(order: Any, *, kind: str) -> dict[str, Any]:
    """Small stable audit for a private write response.

    It intentionally stores only non-secret confirmation signals.  This makes
    lifecycle/recovery diagnostics closer to the MEXC bot: future workers can
    see whether a row was merely acknowledged by REST or actually confirmed by
    a read-back path.
    """

    if not isinstance(order, dict):
        return {"kind": kind, "present": False, "confirmed": False}
    quantity_retry = order.get("_quantity_retry") if isinstance(order.get("_quantity_retry"), dict) else {}
    cancel_result = order.get("_exact_cancel_result") if isinstance(order.get("_exact_cancel_result"), dict) else {}
    confirmed = any(
        bool(order.get(key))
        for key in (
            "_tp_open_confirmed",
            "_stop_open_confirmed",
            "_entry_fill_confirmed",
            "_confirmed",
        )
    )
    if kind in {"cancel", "conditional_cancel", "regular_cancel"}:
        confirmed = bool(cancel_result.get("terminal") or cancel_result.get("filled") or cancel_result.get("response_audit"))
    if kind in {"emergency_close", "rollback_close"}:
        confirmed = bool(order.get("confirmed"))
    if kind == "entry":
        # Entry can be a pending LIMIT: order id/client id plus accepted response is
        # expected; fill confirmation is handled separately by LIMIT catch-up.
        confirmed = confirmed or bool(_order_id(order) or _client_id(order))
    return {
        "kind": kind,
        "present": True,
        "confirmed": bool(confirmed),
        "order_id": _order_id(order),
        "client_order_id": _client_id(order),
        "submitted_qty": _qty(order),
        "quantity_retry_attempted": bool(quantity_retry.get("retry_attempted")),
        "quantity_retry_blocked": bool(quantity_retry.get("blocked_reason")),
        "cancel_terminal": bool(cancel_result.get("terminal")),
        "cancel_filled": bool(cancel_result.get("filled")),
    }


def _action_count(value: Any) -> int:
    if isinstance(value, list):
        return sum(1 for item in value if isinstance(item, dict))
    if isinstance(value, dict):
        actions = value.get("actions")
        if isinstance(actions, list):
            return sum(1 for item in actions if isinstance(item, dict))
        return 1 if value else 0
    return 0


def _nested_dict(value: Any, key: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    nested = value.get(key)
    return nested if isinstance(nested, dict) else {}


def build_write_flow_audit(payload: dict[str, Any] | None, *, status: str = "", stage: str = "") -> dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    entry_intent = payload.get("entry_write_intent_v1") if isinstance(payload.get("entry_write_intent_v1"), dict) else {}
    be_payload = payload.get("be") if isinstance(payload.get("be"), dict) else {}
    recovery_payload = payload.get("recovery") if isinstance(payload.get("recovery"), dict) else {}
    lifecycle_payload = payload.get("lifecycle") if isinstance(payload.get("lifecycle"), dict) else {}
    tp_rows = [row for row in _as_list(payload.get("tp")) if isinstance(row, dict)]
    tp_audits = []
    for row in tp_rows:
        order = row.get("order") if isinstance(row.get("order"), dict) else row
        item = summarize_write_order(order, kind="tp")
        item["tp_index"] = row.get("tp_index") or row.get("index")
        item["target"] = row.get("target") or row.get("price")
        item["filled"] = bool(row.get("filled"))
        tp_audits.append(item)

    stop_candidates = []
    for key in ("post_fill_stop", "stop_order", "market_post_fill_stop_v1"):
        value = payload.get(key)
        if isinstance(value, dict):
            stop_candidates.append((key, value))
    stop_audits = [summarize_write_order(value, kind="stop") | {"source": key} for key, value in stop_candidates]

    cancel_audits = []
    for key in ("entry_cancel", "cancel", "cleanup"):
        value = payload.get(key)
        if isinstance(value, dict):
            cancel_audits.append(summarize_write_order(value, kind="cancel") | {"source": key})
    emergency = payload.get("emergency_close") if isinstance(payload.get("emergency_close"), dict) else {}

    confirmed_tp = sum(1 for item in tp_audits if bool(item.get("confirmed")))
    confirmed_stop = any(bool(item.get("confirmed")) for item in stop_audits)
    entry_audit = summarize_write_order(payload.get("entry"), kind="entry")
    emergency_audit = summarize_write_order(emergency, kind="emergency_close")
    manual_required_reason = ""
    if status in {"manual_required", "partial_error", "partial_unrecoverable"}:
        manual_required_reason = str(
            payload.get("reason")
            or payload.get("tp_error")
            or payload.get("market_protection_failed")
            or payload.get("open_order_guard_audit")
            or "manual review required"
        )[:1000]

    protective_client_order_id_enabled = any(
        bool(
            (row.get("order") if isinstance(row.get("order"), dict) else row).get(
                "_protective_client_order_id_enabled"
            )
        )
        for row in tp_rows
        if isinstance(row, dict)
    ) or any(
        bool(item.get("_protective_client_order_id_enabled"))
        for _key, item in stop_candidates
        if isinstance(item, dict)
    )

    return {
        "version": 1,
        "stage": stage,
        "status": str(status or ""),
        "entry": entry_audit,
        "entry_write_intent": {
            "present": bool(entry_intent),
            "client_order_id": clean_exchange_id(entry_intent.get("clientOrderID") or entry_intent.get("clientOrderId")),
            "attempt_key": str(entry_intent.get("attempt_key") or ""),
            "order_type": str(entry_intent.get("order_type") or ""),
        },
        "stops": stop_audits,
        "tp_total": len(tp_audits),
        "tp_confirmed": confirmed_tp,
        "tp_unconfirmed": max(0, len(tp_audits) - confirmed_tp),
        "tps": tp_audits,
        "emergency_close": emergency_audit,
        "cancels": cancel_audits,
        "confirmed_stop_present": bool(confirmed_stop),
        "protective_client_order_id_enabled": bool(protective_client_order_id_enabled),
        "be_actions_count": _action_count(be_payload.get("actions") or be_payload),
        "be_replacement_intent_present": bool(_nested_dict(be_payload, "replacement_write_intent_v1")),
        "recovery_actions_count": _action_count(recovery_payload.get("actions") or recovery_payload),
        "limit_catchup_actions_count": _action_count(payload.get("limit_catchup") or payload.get("limit_tp_catchup")),
        "lifecycle_cleanup_present": bool(_nested_dict(lifecycle_payload, "cleanup")),
        "manual_required_reason": manual_required_reason,
    }
