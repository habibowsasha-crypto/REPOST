from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timezone
from typing import Any

from app.database import db
from app.services.exchange_factory import build_adapter
from app.services.exchange_identity import clean_exchange_id
from app.services.positions_cache import get_global_positions_cache
from app.services.ttl_cache import get_api_key_cache
from app.services.position_lifecycle_guard import (
    _cancel_exact_with_symbol,
    _saved_conditional_identity,
)
from app.services.be_monitor import (
    _algo_order_id,
    _durable_exact_be_replacement_ids,
    _looks_stop_order,
    _order_client_ids,
    _order_confirmation_qty,
    _order_is_live_candidate,
    _matching_stop_candidates,
    _recovery_topology_fingerprint,
    _strict_exact_be_cleanup_topology,
    _tp_topology_fingerprint,
)

log = logging.getLogger(__name__)


def _json_dict(value: Any) -> dict[str, Any]:
    try:
        parsed = value if isinstance(value, dict) else json.loads(value or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, "") or isinstance(value, bool):
            return default
        parsed = float(value)
        return parsed if math.isfinite(parsed) and parsed >= 0 else default
    except (TypeError, ValueError, OverflowError):
        return default


def _position_size(position: dict[str, Any]) -> float:
    for key in ("size", "availableSize", "positionAmt", "qty", "total"):
        value = _f(position.get(key), 0.0)
        if value > 0:
            return value
    return 0.0


def _position_id(position: dict[str, Any]) -> str:
    return clean_exchange_id(position.get("positionId") or position.get("position_id"))


def _position_side(position: dict[str, Any]) -> str:
    value = str(position.get("positionSide") or position.get("side") or "").lower()
    return value if value in {"long", "short"} else ""


def _stop_price(row: dict[str, Any]) -> float:
    return _f(
        row.get("stopLossPrice")
        or row.get("triggerPrice")
        or row.get("stopPrice"),
        0.0,
    )


def _live_position(positions: list[dict[str, Any]], position_id: str) -> dict[str, Any] | None:
    wanted = clean_exchange_id(position_id)
    if not wanted:
        return None
    matches = [
        dict(row)
        for row in positions or []
        if isinstance(row, dict)
        and _position_id(row) == wanted
        and _position_size(row) > 0
    ]
    return matches[0] if len(matches) == 1 else None


def _tracked_tp_ids(payload: dict[str, Any], orders: list[dict[str, Any]]) -> set[str]:
    tracked_ids, _position_ids = _saved_conditional_identity(payload)
    live_stop_ids = {
        _algo_order_id(row)
        for row in orders or []
        if isinstance(row, dict) and _looks_stop_order(row) and _algo_order_id(row)
    }
    out = {clean_exchange_id(value) for value in tracked_ids}
    out.discard("")
    return out - live_stop_ids


def _relevant_live_stops(
    orders: list[dict[str, Any]], *, position_id: str, side: str
) -> list[dict[str, Any]]:
    wanted_pid = clean_exchange_id(position_id)
    wanted_side = str(side or "").upper()
    rows: list[dict[str, Any]] = []
    for row in orders or []:
        if not isinstance(row, dict) or not _looks_stop_order(row):
            continue
        if not _order_is_live_candidate(row):
            continue
        row_pid = clean_exchange_id(row.get("positionId"))
        row_side = str(row.get("positionSide") or row.get("side") or "").upper()
        if row_pid and row_pid != wanted_pid:
            continue
        if row_side and row_side != wanted_side:
            continue
        if row_pid == wanted_pid or row_side == wanted_side:
            rows.append(dict(row))
    return rows


def _prove_admin_cleanup_topology(
    orders: list[dict[str, Any]],
    *,
    write_intent: dict[str, Any],
    be_state: dict[str, Any],
    approved_old_ids: set[str],
    position_id: str,
    side: str,
    stop_price: float,
    qty: float,
    price_tolerance: float,
    qty_tolerance: float,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """Prove one exact replacement plus only the approved old STOP ids."""

    exact_ids = _durable_exact_be_replacement_ids(be_state)
    client_id = clean_exchange_id(write_intent.get("client_id"))
    candidate_ids: set[str] = set()
    for row in orders or []:
        if not isinstance(row, dict) or not _looks_stop_order(row):
            continue
        order_id = _algo_order_id(row)
        if not order_id:
            continue
        if order_id in exact_ids or (client_id and client_id in _order_client_ids(row)):
            candidate_ids.add(order_id)

    diagnostics: dict[str, Any] = {
        "candidate_ids": sorted(candidate_ids),
        "durable_exact_replacement_ids": sorted(exact_ids),
        "durable_client_id": client_id,
        "approved_old_stop_ids": sorted(approved_old_ids),
        "reason": "",
    }
    confirmed_candidates: list[dict[str, Any]] = []
    for candidate_id in sorted(candidate_ids):
        matches = _matching_stop_candidates(
            orders,
            position_id=position_id,
            side=side,
            stop_price=stop_price,
            qty=qty,
            price_tolerance=price_tolerance,
            qty_tolerance=qty_tolerance,
            expected_order_id=candidate_id,
        )
        if len(matches) == 1:
            confirmed_candidates.append(dict(matches[0]))
    diagnostics["confirmed_candidate_ids"] = sorted(
        {_algo_order_id(row) for row in confirmed_candidates if _algo_order_id(row)}
    )
    if len(confirmed_candidates) != 1:
        diagnostics["reason"] = (
            "replacement_id_not_live"
            if not confirmed_candidates
            else "multiple_replacement_candidates"
        )
        return None, diagnostics

    replacement_id = _algo_order_id(confirmed_candidates[0])
    replacement, live_old, topology = _strict_exact_be_cleanup_topology(
        orders,
        position_id=position_id,
        side=side,
        expected_replacement_id=replacement_id,
        owned_old_order_ids=set(approved_old_ids),
        stop_price=stop_price,
        qty=qty,
        price_tolerance=price_tolerance,
        qty_tolerance=qty_tolerance,
        require_old_absent=False,
    )
    diagnostics["topology"] = topology
    diagnostics["live_old_stop_ids"] = sorted(live_old)
    if replacement is None or not topology.get("confirmed"):
        diagnostics["reason"] = str(topology.get("reason") or "recovery_not_proven")
        return None, diagnostics
    if set(live_old) != set(approved_old_ids):
        diagnostics["reason"] = "approved_old_stop_not_live"
        return None, diagnostics
    diagnostics["reason"] = "confirmed_exact_admin_cleanup_topology"
    diagnostics["replacement_stop_id"] = replacement_id
    return dict(replacement), diagnostics


async def _adapter_for_user(user_id: int) -> Any | None:
    api_row = await get_api_key_cache().get_or_fetch(
        (int(user_id), "api", "bingx"),
        lambda: db.get_api_key(int(user_id), "bingx"),
    )
    return build_adapter(api_row) if api_row else None


async def inspect_existing_be_recovery(execution_id: int) -> dict[str, Any]:
    """Read-only fresh diagnostic for an admin-controlled BE cleanup.

    It never writes to BingX and never modifies the execution. The returned
    topology fingerprint must be bound to a short-lived confirmation token by
    the Telegram handler before a destructive action can be requested.
    """

    eid = int(execution_id or 0)
    if eid <= 0:
        return {"state": "not_found", "execution_id": eid}

    async with db.execution_lock(eid):
        row = await db.get_execution_by_id(eid)
        if not row:
            return {"state": "not_found", "execution_id": eid}
        if str(row.get("exchange") or "bingx").lower() != "bingx":
            return {"state": "wrong_exchange", "execution_id": eid}
        payload = _json_dict(row.get("exchange_order_ids_json"))
        be_state = payload.get("be") if isinstance(payload.get("be"), dict) else {}
        blocked = (
            be_state.get("existing_be_recovery_blocked_v1")
            if isinstance(be_state.get("existing_be_recovery_blocked_v1"), dict)
            else {}
        )
        write_intent = (
            be_state.get("replacement_write_intent_v1")
            if isinstance(be_state.get("replacement_write_intent_v1"), dict)
            else {}
        )
        reason = str(blocked.get("reason") or "")
        user_id = int(row.get("user_id") or 0)
        symbol = str(row.get("symbol") or "").upper()
        position_id = clean_exchange_id(write_intent.get("position_id"))
        side = str(write_intent.get("side") or row.get("side") or "").lower()
        if not write_intent or not position_id or side not in {"long", "short"}:
            return {
                "state": "missing_checkpoint",
                "execution_id": eid,
                "reason": reason,
            }

        adapter = await _adapter_for_user(user_id)
        if adapter is None:
            return {"state": "api_missing", "execution_id": eid, "user_id": user_id}
        try:
            positions = list(await adapter.fetch_open_positions() or [])
            position = _live_position(positions, position_id)
            if position is None:
                return {
                    "state": "position_closed_or_ambiguous",
                    "execution_id": eid,
                    "user_id": user_id,
                    "symbol": symbol,
                    "position_id": position_id,
                    "reason": reason,
                }
            qty = _position_size(position)
            live_side = _position_side(position)
            if live_side != side:
                return {
                    "state": "position_identity_conflict",
                    "execution_id": eid,
                    "symbol": symbol,
                    "position_id": position_id,
                    "expected_side": side,
                    "live_side": live_side,
                }
            info = await adapter.instrument_info(symbol)
            price_tick = _f(getattr(info, "price_tick", 0.0), 0.0)
            qty_step = _f(getattr(info, "qty_step", 0.0), 0.0)
            price_tol = max(price_tick * 0.51, abs(_f(write_intent.get("stop"), 0.0)) * 1e-9, 1e-12)
            qty_tol = max(qty_step * 0.51, abs(qty) * 1e-12, 1e-12)
            orders = [
                dict(item)
                for item in list(await adapter.fetch_open_algo_orders(symbol) or [])
                if isinstance(item, dict)
            ]
            fingerprint, topology = _recovery_topology_fingerprint(
                orders,
                symbol=symbol,
                position_id=position_id,
                side=side,
            )
            intent_old_ids = {
                clean_exchange_id(value)
                for value in (
                    write_intent.get("old_stop_ids")
                    if isinstance(write_intent.get("old_stop_ids"), list)
                    else []
                )
            }
            intent_old_ids.discard("")
            relevant_stops = _relevant_live_stops(
                orders, position_id=position_id, side=side
            )
            live_stop_ids = {_algo_order_id(item) for item in relevant_stops if _algo_order_id(item)}
            exact_replacement_ids = _durable_exact_be_replacement_ids(be_state)
            durable_client_id = clean_exchange_id(write_intent.get("client_id"))
            tracked_ids, _ = _saved_conditional_identity(payload)
            tracked_ids = {clean_exchange_id(value) for value in tracked_ids}
            tracked_ids.discard("")

            stop_rows: list[dict[str, Any]] = []
            for stop in relevant_stops:
                order_id = _algo_order_id(stop)
                qty_value, qty_explicit = _order_confirmation_qty(stop)
                if order_id in exact_replacement_ids or (
                    durable_client_id and durable_client_id in _order_client_ids(stop)
                ):
                    ownership = "replacement_exact"
                elif order_id in intent_old_ids and order_id in tracked_ids:
                    ownership = "old_exact"
                elif order_id in intent_old_ids:
                    ownership = "old_unproven"
                else:
                    ownership = "unexpected"
                stop_rows.append(
                    {
                        "order_id": order_id,
                        "price": _stop_price(stop),
                        "qty": qty_value if qty_explicit else None,
                        "position_id": clean_exchange_id(stop.get("positionId")),
                        "side": str(stop.get("positionSide") or stop.get("side") or "").upper(),
                        "ownership": ownership,
                    }
                )

            # For preview only, treat durable intent old ids as the proposed
            # admin-approved ownership set. The destructive path repeats this
            # proof after token validation and immediately before cancel.
            replacement, recovery_diag = _prove_admin_cleanup_topology(
                orders,
                write_intent=write_intent,
                be_state=be_state,
                approved_old_ids=set(intent_old_ids),
                position_id=position_id,
                side=side,
                stop_price=_f(write_intent.get("stop"), 0.0),
                qty=qty,
                price_tolerance=price_tol,
                qty_tolerance=qty_tol,
            )
            allowed_old_ids = sorted(intent_old_ids.intersection(live_stop_ids))
            tp_ids = _tracked_tp_ids(payload, orders)
            tp_fingerprint, tp_snapshot = _tp_topology_fingerprint(
                orders,
                position_id=position_id,
                side=side,
                tracked_tp_order_ids=tp_ids,
            )
            ready = bool(
                str(row.get("status") or "") == "manual_required"
                and reason == "old_stop_not_exactly_owned"
                and replacement is not None
                and allowed_old_ids
                and set(allowed_old_ids) == intent_old_ids
            )
            return {
                "state": "ready" if ready else "blocked",
                "execution_id": eid,
                "user_id": user_id,
                "status": str(row.get("status") or ""),
                "symbol": symbol,
                "side": side,
                "position_id": position_id,
                "position_qty": qty,
                "reason": reason,
                "topology_fingerprint": fingerprint,
                "topology_snapshot": topology,
                "tp_fingerprint": tp_fingerprint,
                "tp_snapshot": tp_snapshot,
                "live_stops": stop_rows,
                "allowed_old_stop_ids": allowed_old_ids,
                "replacement_stop_id": _algo_order_id(replacement or {}),
                "recovery_diagnostics": recovery_diag,
            }
        finally:
            try:
                await adapter.close()
            except Exception:
                pass


async def execute_admin_existing_be_cleanup(
    *,
    execution_id: int,
    selected_old_stop_ids: set[str],
    expected_topology_fingerprint: str,
    admin_user_id: int,
) -> dict[str, Any]:
    """Cancel explicitly approved old STOP ids after complete fresh re-proof.

    This is the only g7a destructive path. It is fail-closed, exact-id only,
    persists a cleanup intent before the exchange call, performs a second fresh
    pre-cancel read, and requires a final read proving one replacement STOP and
    unchanged TP topology before success.
    """

    eid = int(execution_id or 0)
    selected_ids = {clean_exchange_id(value) for value in selected_old_stop_ids}
    selected_ids.discard("")
    expected_fp = str(expected_topology_fingerprint or "")
    if eid <= 0 or not selected_ids or not expected_fp:
        return {"state": "invalid_request", "execution_id": eid}

    async with db.execution_lock(eid):
        row = await db.get_execution_by_id(eid)
        if not row:
            return {"state": "not_found", "execution_id": eid}
        status = str(row.get("status") or "")
        if status != "manual_required":
            return {"state": "stale_status", "execution_id": eid, "status": status}
        payload = _json_dict(row.get("exchange_order_ids_json"))
        be_state = payload.get("be") if isinstance(payload.get("be"), dict) else {}
        blocked = (
            be_state.get("existing_be_recovery_blocked_v1")
            if isinstance(be_state.get("existing_be_recovery_blocked_v1"), dict)
            else {}
        )
        if str(blocked.get("reason") or "") != "old_stop_not_exactly_owned":
            return {
                "state": "not_eligible",
                "execution_id": eid,
                "reason": str(blocked.get("reason") or ""),
            }
        write_intent = (
            be_state.get("replacement_write_intent_v1")
            if isinstance(be_state.get("replacement_write_intent_v1"), dict)
            else {}
        )
        intent_old_ids = {
            clean_exchange_id(value)
            for value in (
                write_intent.get("old_stop_ids")
                if isinstance(write_intent.get("old_stop_ids"), list)
                else []
            )
        }
        intent_old_ids.discard("")
        if selected_ids != intent_old_ids:
            return {
                "state": "selection_mismatch",
                "execution_id": eid,
                "expected_old_stop_ids": sorted(intent_old_ids),
                "selected_old_stop_ids": sorted(selected_ids),
            }

        user_id = int(row.get("user_id") or 0)
        symbol = str(row.get("symbol") or "").upper()
        position_id = clean_exchange_id(write_intent.get("position_id"))
        side = str(write_intent.get("side") or row.get("side") or "").lower()
        adapter = await _adapter_for_user(user_id)
        if adapter is None:
            return {"state": "api_missing", "execution_id": eid, "user_id": user_id}
        try:
            positions = list(await adapter.fetch_open_positions() or [])
            position = _live_position(positions, position_id)
            if position is None:
                return {
                    "state": "position_closed_or_ambiguous",
                    "execution_id": eid,
                    "position_id": position_id,
                }
            qty = _position_size(position)
            if _position_side(position) != side:
                return {"state": "position_identity_conflict", "execution_id": eid}
            stop_price = _f(write_intent.get("stop"), 0.0)
            info = await adapter.instrument_info(symbol)
            price_tick = _f(getattr(info, "price_tick", 0.0), 0.0)
            qty_step = _f(getattr(info, "qty_step", 0.0), 0.0)
            price_tol = max(price_tick * 0.51, abs(stop_price) * 1e-9, 1e-12)
            qty_tol = max(qty_step * 0.51, abs(qty) * 1e-12, 1e-12)

            initial_rows = [
                dict(item)
                for item in list(await adapter.fetch_open_algo_orders(symbol) or [])
                if isinstance(item, dict)
            ]
            current_fp, current_snapshot = _recovery_topology_fingerprint(
                initial_rows,
                symbol=symbol,
                position_id=position_id,
                side=side,
            )
            if current_fp != expected_fp:
                return {
                    "state": "topology_changed",
                    "execution_id": eid,
                    "expected_fingerprint": expected_fp,
                    "actual_fingerprint": current_fp,
                    "topology_snapshot": current_snapshot,
                }
            replacement, recovery_diag = _prove_admin_cleanup_topology(
                initial_rows,
                write_intent=write_intent,
                be_state=be_state,
                approved_old_ids=set(selected_ids),
                position_id=position_id,
                side=side,
                stop_price=stop_price,
                qty=qty,
                price_tolerance=price_tol,
                qty_tolerance=qty_tol,
            )
            if replacement is None:
                return {
                    "state": "recovery_not_proven",
                    "execution_id": eid,
                    "diagnostics": recovery_diag,
                }
            replacement_id = _algo_order_id(replacement)
            tp_ids = _tracked_tp_ids(payload, initial_rows)
            tp_before, tp_before_snapshot = _tp_topology_fingerprint(
                initial_rows,
                position_id=position_id,
                side=side,
                tracked_tp_order_ids=tp_ids,
            )
            reserved_at = datetime.now(timezone.utc).isoformat()
            reservation = {
                "version": 1,
                "requested_by_admin_user_id": int(admin_user_id),
                "execution_id": eid,
                "position_id": position_id,
                "side": side,
                "selected_old_stop_ids": sorted(selected_ids),
                "replacement_stop_id": replacement_id,
                "topology_fingerprint": current_fp,
                "tp_fingerprint_before": tp_before,
                "created_at": reserved_at,
            }
            standard_cleanup_intent = {
                "version": 1,
                "order_ids": sorted(selected_ids),
                "replacement_stop_id": replacement_id,
                "dispatch_state": "reserved_unknown",
                "reserved_at": reserved_at,
                "source": "admin_exact_cleanup_v1_0_7g7a",
            }
            saved = await db.merge_execution_metadata(
                eid,
                {
                    "be": {
                        "admin_exact_cleanup_intent_v1": reservation,
                        "cleanup_cancel_intent_v1": standard_cleanup_intent,
                    }
                },
                expected_status="manual_required",
                write_flow_audit_stage="g7a_admin_exact_cleanup_reserved",
                write_flow_audit_status="manual_required",
            )
            if not saved:
                return {"state": "stale_status", "execution_id": eid}

            # Direct fresh read immediately before the destructive call.
            pre_cancel_rows = [
                dict(item)
                for item in list(await adapter.fetch_open_algo_orders(symbol) or [])
                if isinstance(item, dict)
            ]
            pre_cancel_fp, pre_cancel_snapshot = _recovery_topology_fingerprint(
                pre_cancel_rows,
                symbol=symbol,
                position_id=position_id,
                side=side,
            )
            if pre_cancel_fp != current_fp:
                await db.merge_execution_metadata(
                    eid,
                    {
                        "be": {
                            "admin_exact_cleanup_result_v1": {
                                "state": "topology_changed_before_cancel",
                                "expected_fingerprint": current_fp,
                                "actual_fingerprint": pre_cancel_fp,
                                "topology_snapshot": pre_cancel_snapshot,
                                "finished_at": datetime.now(timezone.utc).isoformat(),
                            }
                        }
                    },
                    expected_status="manual_required",
                )
                return {
                    "state": "topology_changed_before_cancel",
                    "execution_id": eid,
                    "expected_fingerprint": current_fp,
                    "actual_fingerprint": pre_cancel_fp,
                }
            pre_replacement, pre_diag = _prove_admin_cleanup_topology(
                pre_cancel_rows,
                write_intent=write_intent,
                be_state=be_state,
                approved_old_ids=set(selected_ids),
                position_id=position_id,
                side=side,
                stop_price=stop_price,
                qty=qty,
                price_tolerance=price_tol,
                qty_tolerance=qty_tol,
            )
            if pre_replacement is None or _algo_order_id(pre_replacement) != replacement_id:
                return {
                    "state": "pre_cancel_recovery_not_proven",
                    "execution_id": eid,
                    "diagnostics": pre_diag,
                }

            cancel_result: dict[str, Any] | None = None
            cancel_error = ""
            try:
                cancel_result = await _cancel_exact_with_symbol(
                    adapter.cancel_conditional_orders_exact,
                    selected_ids,
                    symbol,
                )
            except Exception as exc:  # final fresh read determines the outcome
                cancel_error = f"{type(exc).__name__}: {exc}"
                log.exception(
                    "g7a admin exact STOP cancel returned error execution_id=%s ids=%s",
                    eid,
                    sorted(selected_ids),
                )

            final_rows = [
                dict(item)
                for item in list(await adapter.fetch_open_algo_orders(symbol) or [])
                if isinstance(item, dict)
            ]
            replacement_after, remaining_old, topology_diag = _strict_exact_be_cleanup_topology(
                final_rows,
                position_id=position_id,
                side=side,
                expected_replacement_id=replacement_id,
                owned_old_order_ids=set(selected_ids),
                stop_price=stop_price,
                qty=qty,
                price_tolerance=price_tol,
                qty_tolerance=qty_tol,
                require_old_absent=True,
            )
            tp_after, tp_after_snapshot = _tp_topology_fingerprint(
                final_rows,
                position_id=position_id,
                side=side,
                tracked_tp_order_ids=tp_ids,
            )
            tp_unchanged = bool(tp_before == tp_after)
            success = bool(
                replacement_after is not None
                and topology_diag.get("confirmed")
                and not remaining_old
                and tp_unchanged
            )
            result_record = {
                "version": 1,
                "state": "success" if success else "final_verification_failed",
                "requested_by_admin_user_id": int(admin_user_id),
                "selected_old_stop_ids": sorted(selected_ids),
                "replacement_stop_id": replacement_id,
                "cancel_result": cancel_result,
                "cancel_error": cancel_error,
                "topology_diagnostics": topology_diag,
                "tp_fingerprint_before": tp_before,
                "tp_fingerprint_after": tp_after,
                "tp_unchanged": tp_unchanged,
                "tp_snapshot_before": tp_before_snapshot,
                "tp_snapshot_after": tp_after_snapshot,
                "finished_at": datetime.now(timezone.utc).isoformat(),
            }
            if not success:
                await db.merge_execution_metadata(
                    eid,
                    {
                        "be": {
                            "manual_required": True,
                            "admin_exact_cleanup_result_v1": result_record,
                        }
                    },
                    expected_status="manual_required",
                    write_flow_audit_stage="g7a_admin_exact_cleanup_failed",
                    write_flow_audit_status="manual_required",
                )
                return {
                    "state": "final_verification_failed",
                    "execution_id": eid,
                    "replacement_stop_id": replacement_id,
                    "remaining_old_stop_ids": sorted(remaining_old),
                    "tp_unchanged": tp_unchanged,
                    "topology_diagnostics": topology_diag,
                    "cancel_error": cancel_error,
                }

            final_patch = {
                "be": {
                    "moved": True,
                    "manual_required": False,
                    "manual_requested": False,
                    "replacement_in_progress": False,
                    "replacement_write_intent_v1": None,
                    "cleanup_cancel_intent_v1": None,
                    "admin_exact_cleanup_intent_v1": None,
                    "replacement_stop_id": replacement_id,
                    "verify_matching_stop_order_id": replacement_id,
                    "replacement_stop": dict(replacement_after),
                    "existing_be_recovery_blocked_v1": None,
                    "existing_be_recovery_next_retry_after": None,
                    "existing_be_recovery_owned_replacement_ids": None,
                    "admin_exact_cleanup_result_v1": result_record,
                    "error": None,
                    "skipped": None,
                }
            }
            written = await db.update_execution_status_merge(
                eid,
                "protected",
                "admin-confirmed exact old STOP cleanup completed and verified",
                final_patch,
                expected_status="manual_required",
                write_flow_audit_stage="g7a_admin_exact_cleanup_verified",
                write_flow_audit_status="protected",
            )
            if not written:
                return {"state": "stale_status_after_cleanup", "execution_id": eid}
            get_global_positions_cache().invalidate(user_id, "bingx")
            log.warning(
                "BE_ADMIN_EXACT_CLEANUP_VERIFIED execution_id=%s admin_user_id=%s old_stop_ids=%s replacement_stop_id=%s tp_unchanged=%s",
                eid,
                int(admin_user_id),
                sorted(selected_ids),
                replacement_id,
                tp_unchanged,
            )
            return {
                "state": "success",
                "execution_id": eid,
                "symbol": symbol,
                "position_id": position_id,
                "replacement_stop_id": replacement_id,
                "cancelled_old_stop_ids": sorted(selected_ids),
                "tp_unchanged": True,
            }
        finally:
            try:
                await adapter.close()
            except Exception:
                pass
