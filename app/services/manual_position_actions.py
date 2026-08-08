from __future__ import annotations

import hashlib
import json
import logging
import math
from datetime import datetime, timezone
from typing import Any

from app.database import db
from app.services.be_monitor import process_be_monitor_once
from app.services.dashboard import outcome_from_close
from app.services.exchange_factory import build_adapter
from app.services.exchange_identity import clean_exchange_id
from app.services.execution_exposure import execution_be_protection_confirmed
from app.services.limit_tp_catchup import (
    PendingEntryCancelDisposition,
    _cancel_opening_order_remainder_confirmed,
)
from app.services.position_lifecycle_guard import _cleanup_stale_orders
from app.services.positions_cache import get_global_positions_cache
from app.services.trade_notifications import analyze_close_result
from app.services.ttl_cache import get_api_key_cache

log = logging.getLogger(__name__)

_ACTIVE_EXECUTION_STATUSES = {
    "opened",
    "pending_limit",
    "protected",
    "partial_error",
    "manual_required",
    "partial_unrecoverable",
}
_FORCE_BE_STATUSES = {"opened", "protected", "partial_error", "manual_required"}


def _f(value: Any, default: float = 0.0) -> float:
    """Parse a finite non-negative exchange scalar without repairing corruption."""
    try:
        if value in (None, "") or isinstance(value, bool):
            return default
        parsed = float(value)
        return parsed if math.isfinite(parsed) and parsed >= 0 else default
    except (TypeError, ValueError, OverflowError):
        return default


def _json_dict(value: Any) -> dict[str, Any]:
    try:
        parsed = value if isinstance(value, dict) else json.loads(value or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _position_id(position: dict[str, Any]) -> str:
    return clean_exchange_id(position.get("positionId") or position.get("position_id"))


def _position_size(position: dict[str, Any]) -> float:
    for key in ("size", "availableSize", "positionAmt", "qty", "total"):
        value = _f(position.get(key), 0.0)
        if value > 0:
            return value
    return 0.0


def _position_side(position: dict[str, Any]) -> str:
    value = str(position.get("side") or position.get("positionSide") or "").lower()
    if value in {"long", "short"}:
        return value
    return ""


def _payload_position_ids(payload: Any) -> set[str]:
    """Collect exact BingX position identities stored by one execution."""
    found: set[str] = set()

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                normalized = str(key).replace("_", "").lower()
                if normalized in {"positionid", "confirmedpositionid"}:
                    text = clean_exchange_id(child)
                    if text:
                        found.add(text)
                else:
                    walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(payload)
    return found


def execution_matches_position(row: dict[str, Any], position: dict[str, Any]) -> bool:
    if int(row.get("user_id") or 0) <= 0:
        return False
    if (
        str(row.get("symbol") or "").upper()
        != str(position.get("symbol") or "").upper()
    ):
        return False
    if str(row.get("side") or "").lower() != _position_side(position):
        return False
    pid = _position_id(position)
    if not pid:
        return False
    payload = _json_dict(row.get("exchange_order_ids_json"))
    return pid in _payload_position_ids(payload)


async def managed_execution_for_position(
    user_id: int, position: dict[str, Any]
) -> tuple[dict[str, Any] | None, bool]:
    """Return ``(row, ambiguous)`` using exact user+symbol+side+positionId."""
    rows = await db.active_position_executions_for_user(int(user_id), limit=200)
    matches = [
        row
        for row in rows
        if int(row.get("user_id") or 0) == int(user_id)
        and execution_matches_position(row, position)
    ]
    if len(matches) == 1:
        return matches[0], False
    return None, len(matches) > 1


def _row_matches_symbol_side(row: dict[str, Any], position: dict[str, Any]) -> bool:
    return (
        int(row.get("user_id") or 0) > 0
        and str(row.get("symbol") or "").upper()
        == str(position.get("symbol") or "").upper()
        and str(row.get("side") or "").lower() == _position_side(position)
    )


async def _pending_limit_fill_binding(
    adapter: Any, user_id: int, position: dict[str, Any]
) -> tuple[dict[str, Any] | None, bool, str]:
    """Resolve a monitor-lagged partial LIMIT without guessing ownership.

    A partial LIMIT may create a live BingX position before the monitor persists
    ``positionId`` into the execution JSON.  We bind only when exact entry-order
    status proves a positive fill.  Zero-fill pending orders are unrelated future
    entries and remain untouched; unreadable or identity-less candidates block
    MARKET close fail-closed because they could be the unpersisted partial fill.
    """
    rows = await db.pending_limit_executions_for_user(int(user_id), limit=500)
    relevant = [row for row in rows if _row_matches_symbol_side(row, position)]
    if not relevant:
        return None, False, ""

    live_pid = _position_id(position)
    proven: list[dict[str, Any]] = []
    ambiguous_reasons: list[str] = []
    for row in relevant:
        payload = _json_dict(row.get("exchange_order_ids_json"))
        entry = payload.get("entry") if isinstance(payload.get("entry"), dict) else {}
        if not entry:
            ambiguous_reasons.append(
                f"execution {int(row.get('id')or 0)} has no saved entry identity"
            )
            continue
        try:
            status = await adapter.fetch_entry_order_fill_status(
                symbol=str(row.get("symbol") or "").upper(),
                order_response=entry,
            )
        except Exception as exc:
            ambiguous_reasons.append(
                f"execution {int(row.get('id')or 0)} status read failed: "
                f"{type(exc).__name__}: {exc}"
            )
            continue
        status = status if isinstance(status, dict) else {}
        filled_qty = _f(status.get("filled_qty"), 0.0)
        status_pid = clean_exchange_id(
            status.get("position_id") or status.get("positionId")
        )
        if status_pid and live_pid and status_pid != live_pid:
            continue
        if filled_qty > 0 or (status_pid and status_pid == live_pid):
            candidate = dict(row)
            candidate["_manual_close_entry_status"] = status
            proven.append(candidate)

    if len(proven) == 1 and not ambiguous_reasons:
        return proven[0], False, "entry fill proved before positionId persistence"
    if len(proven) > 1:
        return (
            None,
            True,
            "multiple partially filled LIMIT executions match this position",
        )
    if ambiguous_reasons:
        return None, True, "; ".join(ambiguous_reasons)[:1000]
    return None, False, ""


async def fetch_exact_live_position(
    adapter: Any, position_id: str
) -> dict[str, Any] | None:
    wanted = clean_exchange_id(position_id)
    if not wanted:
        return None
    rows = await adapter.fetch_open_positions()
    for row in rows or []:
        if isinstance(row, dict) and _position_id(row) == wanted:
            return row
    return None


def _stable_action_id(prefix: str, *parts: Any) -> str:
    digest = hashlib.sha256(":".join(str(part) for part in parts).encode()).hexdigest()
    return f"{prefix}-{digest[:20]}"[:36]



def _manual_be_unconfirmed_reason(be_state: dict[str, Any]) -> str:
    """Explain an impossible no-result return from the shared BE engine."""
    reason_parts: list[str] = [
        "manual BE engine returned without confirmed BE protection or manual_result"
    ]
    waiting_reason = str(be_state.get("waiting_reason") or "").strip()
    if waiting_reason:
        reason_parts.append(f"waiting_reason={waiting_reason}")
    retry_after = str(be_state.get("waiting_next_retry_after") or "").strip()
    if retry_after:
        reason_parts.append(f"next_retry_after={retry_after}")
    error = str(be_state.get("error") or "").strip()
    if error:
        reason_parts.append(f"error={error}")
    skipped = str(be_state.get("skipped") or "").strip()
    if skipped:
        reason_parts.append(f"skipped={skipped}")
    return "; ".join(reason_parts)[:1000]

def _close_price_details(result: dict[str, Any]) -> tuple[float, str]:
    """Return only an exchange-confirmed close fill price and its source.

    A public ticker is never a fill price.  If BingX confirms the position-size
    reduction but its order history does not expose an average fill, callers
    must keep the exit price unknown instead of fabricating PnL from the latest
    market quote.
    """
    for key in (
        "_avg_fill_price",
        "avgFillPrice",
        "dealAvgPrice",
    ):
        value = _f(result.get(key), 0.0)
        if value > 0:
            return value, f"order.{key}"
    data = result.get("data") if isinstance(result.get("data"), dict) else {}
    for key in ("dealAvgPrice", "avgFillPrice"):
        value = _f(data.get(key), 0.0)
        if value > 0:
            return value, f"order.data.{key}"
    return 0.0, "unavailable"


def _close_price(result: dict[str, Any]) -> float:
    """Compatibility helper returning only a confirmed exchange fill price."""
    return _close_price_details(result)[0]


def _targets(row: dict[str, Any]) -> list[float]:
    try:
        raw = json.loads(row.get("targets_json") or "[]")
        return [float(value) for value in raw] if isinstance(raw, list) else []
    except (TypeError, ValueError, json.JSONDecodeError):
        return []


async def _persist_managed_manual_close(
    *,
    row: dict[str, Any],
    payload: dict[str, Any],
    close_result: dict[str, Any],
    cleanup: dict[str, Any],
    exit_price: float,
    exit_price_source: str,
    qty: float,
    pending_entry_remainder: dict[str, Any] | None = None,
) -> tuple[str, bool]:
    execution_id = int(row.get("id") or 0)
    status = str(row.get("status") or "")
    side = str(row.get("side") or "").lower()
    entry = _f(payload.get("actual_entry"), 0.0) or _f(row.get("entry"), 0.0)
    be_info = payload.get("be") if isinstance(payload.get("be"), dict) else {}
    be_moved = be_info.get("moved") is True
    be_stop = _f(be_info.get("stop"), 0.0)
    tp_rows = payload.get("tp") if isinstance(payload.get("tp"), list) else []
    price_confirmed = bool(exit_price > 0 and exit_price_source != "unavailable")
    if price_confirmed:
        analysis = analyze_close_result(
            side=side,
            entry=entry,
            stop=_f(row.get("stop"), 0.0),
            targets=_targets(row),
            original_qty=_f(row.get("qty"), qty) or qty,
            qty_now=0.0,
            current_price=exit_price,
            tp_orders_payload=tp_rows,
            be_moved=be_moved,
            be_stop_price=be_stop,
        )
        realized_pnl = float(analysis.get("total_pnl") or 0.0)
        outcome = outcome_from_close(
            close_type="manual_bot_close",
            realized_pnl=realized_pnl,
            be_was_set=be_moved,
        )
        durable_close_type = "manual_bot_close"
    else:
        # The position is confirmed closed, but BingX did not expose an average
        # fill.  Do not turn a current ticker into fictitious realized PnL.
        analysis = {
            "close_type": "manual_bot_close_price_unknown",
            "total_pnl": 0.0,
            "price_confirmed": False,
        }
        realized_pnl = 0.0
        outcome = "unknown"
        durable_close_type = "manual_bot_close_price_unknown"
    cleanup_ok = cleanup.get("verified_clean") is True and not bool(cleanup.get("errors"))
    # The position close itself was already confirmed by a fresh exact-position
    # read. Cleanup uncertainty must not keep portfolio/day risk or an open slot
    # occupied forever. ``closed_on_exchange`` remains lifecycle-monitored and
    # same-symbol blocking until exact stale-order cleanup is verified.
    new_status = "closed_on_exchange_cleanup" if cleanup_ok else "closed_on_exchange"
    reason = (
        "position fully closed manually through bot; exact tracked protection cleaned"
        if cleanup_ok
        else "position fully closed manually through bot; position risk released, exact protection cleanup still pending"
    )
    now = datetime.now(timezone.utc).isoformat()
    patch = {
        "manual_position_close_v1": {
            "requested_by_user_id": int(row.get("user_id") or 0),
            "confirmed": True,
            "confirmed_at": now,
            "position_id": clean_exchange_id(close_result.get("_manual_position_id")),
            "requested_qty": float(qty),
            "filled_qty": float(close_result.get("_filled_quantity") or qty),
            "exit_price": float(exit_price or 0.0),
            "exit_price_source": str(exit_price_source or "unavailable"),
            "exit_price_confirmed": price_confirmed,
            "order": close_result,
            "cleanup": cleanup,
            "pending_entry_remainder": pending_entry_remainder,
        },
        "lifecycle": {
            "closed_cleanup_done": cleanup_ok,
            "previous_status": status,
            "manual_bot_close": True,
            "cleanup": cleanup,
            "close_result": {
                "outcome": outcome,
                "close_type": durable_close_type,
                "realized_pnl": realized_pnl,
                "exit_price": float(exit_price or 0.0),
                "exit_price_source": str(exit_price_source or "unavailable"),
                "exit_price_confirmed": price_confirmed,
            },
        },
    }
    saved = await db.update_execution_status_merge(
        execution_id,
        new_status,
        reason,
        patch,
        expected_status=status,
    )
    if saved:
        try:
            await db.record_execution_outcome(
                execution_id,
                outcome=outcome,
                realized_pnl=realized_pnl,
                close_type=durable_close_type,
            )
        except Exception:
            # The exchange close and durable lifecycle status are already
            # confirmed. Statistics can be repaired later; never report the
            # position as still open just because the dashboard write failed.
            log.exception(
                "manual close outcome persistence failed execution_id=%s",
                execution_id,
            )
    return new_status, bool(saved)


async def close_position_fully(user_id: int, position_id: str) -> dict[str, Any]:
    """Close exactly one live BingX position in full and confirm the result."""
    uid = int(user_id)
    pid = clean_exchange_id(position_id)
    if uid <= 0 or not pid:
        return {"state": "not_found"}

    api_row = await get_api_key_cache().get_or_fetch(
        (uid, "api", "bingx"), lambda: db.get_api_key(uid, "bingx")
    )
    if not api_row:
        return {"state": "api_missing", "position_id": pid}

    adapter = build_adapter(api_row)
    try:
        initial = await fetch_exact_live_position(adapter, pid)
        if initial is None:
            return {"state": "already_closed", "position_id": pid}
        symbol = str(initial.get("symbol") or "").upper()
        side = _position_side(initial)
        if not symbol or side not in {"long", "short"}:
            return {"state": "invalid_position", "position_id": pid}
        managed_row, ambiguous = await managed_execution_for_position(uid, initial)
        fallback_pending_binding = False
        binding_reason = ""
        if ambiguous:
            return {
                "state": "ambiguous_execution",
                "position_id": pid,
                "symbol": symbol,
                "side": side,
            }
        if managed_row is None:
            managed_row, pending_ambiguous, binding_reason = (
                await _pending_limit_fill_binding(adapter, uid, initial)
            )
            if pending_ambiguous:
                return {
                    "state": "pending_entry_ambiguous",
                    "position_id": pid,
                    "symbol": symbol,
                    "side": side,
                    "reason": binding_reason,
                }
            fallback_pending_binding = managed_row is not None

        async def perform(latest_row: dict[str, Any] | None) -> dict[str, Any]:
            async with db.symbol_action_lock(uid, symbol):
                live = await fetch_exact_live_position(adapter, pid)
                if live is None:
                    return {
                        "state": "already_closed",
                        "position_id": pid,
                        "symbol": symbol,
                        "side": side,
                    }
                live_symbol = str(live.get("symbol") or "").upper()
                live_side = _position_side(live)
                if live_symbol != symbol or live_side != side:
                    return {
                        "state": "stale_identity",
                        "position_id": pid,
                        "symbol": symbol,
                        "side": side,
                    }
                qty = _position_size(live)
                if qty <= 0:
                    return {
                        "state": "already_closed",
                        "position_id": pid,
                        "symbol": symbol,
                        "side": side,
                    }
                pending_entry_remainder: dict[str, Any] | None = None
                if (
                    latest_row is not None
                    and str(latest_row.get("status") or "") == "pending_limit"
                ):
                    latest_payload = _json_dict(
                        latest_row.get("exchange_order_ids_json")
                    )
                    entry_order = (
                        latest_payload.get("entry")
                        if isinstance(latest_payload.get("entry"), dict)
                        else {}
                    )
                    remainder = await _cancel_opening_order_remainder_confirmed(
                        adapter,
                        symbol=symbol,
                        side=side,
                        entry_order=entry_order,
                    )
                    pending_entry_remainder = {
                        "disposition": remainder.disposition.value,
                        "reason": remainder.reason,
                        "order_status": remainder.order_status,
                    }
                    if remainder.disposition != PendingEntryCancelDisposition.FILLED:
                        return {
                            "state": "pending_entry_unconfirmed",
                            "position_id": pid,
                            "execution_id": int(latest_row.get("id") or 0),
                            "symbol": symbol,
                            "side": side,
                            "reason": remainder.reason,
                            "entry_disposition": remainder.disposition.value,
                        }
                        # The exact opening order is now terminal. Refresh the live
                        # position because a last fill may have landed during the
                        # cancellation handshake; close the entire refreshed size.
                    live = await fetch_exact_live_position(adapter, pid)
                    if live is None:
                        return {
                            "state": "already_closed",
                            "position_id": pid,
                            "symbol": symbol,
                            "side": side,
                        }
                    live_symbol = str(live.get("symbol") or "").upper()
                    live_side = _position_side(live)
                    if live_symbol != symbol or live_side != side:
                        return {
                            "state": "stale_identity",
                            "position_id": pid,
                            "symbol": symbol,
                            "side": side,
                        }
                    qty = _position_size(live)
                    if qty <= 0:
                        return {
                            "state": "already_closed",
                            "position_id": pid,
                            "symbol": symbol,
                            "side": side,
                        }

                close_result = await adapter.emergency_close_market_confirmed(
                    symbol=symbol,
                    side=side,
                    qty=qty,
                    client_id=_stable_action_id("manual-close", uid, pid),
                    position_id=pid,
                    open_type=int(live.get("openType") or 0) or None,
                )
                close_result = dict(close_result or {})
                close_result["_manual_position_id"] = pid
                get_global_positions_cache().invalidate(uid, "bingx")
                remaining = await fetch_exact_live_position(adapter, pid)
                if remaining is not None and _position_size(remaining) > 1e-12:
                    return {
                        "state": "unconfirmed",
                        "position_id": pid,
                        "symbol": symbol,
                        "side": side,
                        "remaining_qty": _position_size(remaining),
                    }

                exit_price, exit_price_source = _close_price_details(close_result)

                cleanup: dict[str, Any] = {
                    "verified_clean": False,
                    "not_attempted": True,
                    "reason": "no exact bot execution matched this live position",
                }
                status = "untracked"
                db_saved = True
                execution_id = 0
                if latest_row is not None:
                    execution_id = int(latest_row.get("id") or 0)
                    payload = _json_dict(latest_row.get("exchange_order_ids_json"))
                    cleanup = await _cleanup_stale_orders(
                        adapter, symbol, payload=payload, attempts=3
                    )
                    status, db_saved = await _persist_managed_manual_close(
                        row=latest_row,
                        payload=payload,
                        close_result=close_result,
                        cleanup=cleanup,
                        exit_price=exit_price,
                        exit_price_source=exit_price_source,
                        qty=qty,
                        pending_entry_remainder=pending_entry_remainder,
                    )
                return {
                    "state": "closed",
                    "position_id": pid,
                    "execution_id": execution_id,
                    "symbol": symbol,
                    "side": side,
                    "qty": qty,
                    "exit_price": exit_price,
                    "exit_price_source": exit_price_source,
                    "exit_price_confirmed": bool(exit_price > 0),
                    "binding_source": (
                        "pending_limit_fill_fallback"
                        if fallback_pending_binding
                        else (
                            "exact_position_id"
                            if latest_row is not None
                            else "untracked"
                        )
                    ),
                    "binding_reason": binding_reason,
                    "cleanup_verified": cleanup.get("verified_clean") is True,
                    "cleanup": cleanup,
                    "execution_status": status,
                    "db_saved": db_saved,
                    "pending_entry_remainder": pending_entry_remainder,
                }

        if managed_row is None:
            return await perform(None)

        execution_id = int(managed_row.get("id") or 0)
        async with db.execution_lock(execution_id):
            latest = await db.get_execution_by_id(execution_id)
            if (
                latest is None
                or int(latest.get("user_id") or 0) != uid
                or str(latest.get("status") or "") not in _ACTIVE_EXECUTION_STATUSES
            ):
                return {"state": "stale", "position_id": pid, "symbol": symbol}
            if fallback_pending_binding:
                if str(
                    latest.get("status") or ""
                ) != "pending_limit" or not _row_matches_symbol_side(latest, initial):
                    return {
                        "state": "stale",
                        "position_id": pid,
                        "symbol": symbol,
                        "side": side,
                    }
            elif not execution_matches_position(latest, initial):
                return {
                    "state": "ambiguous_execution",
                    "position_id": pid,
                    "symbol": symbol,
                    "side": side,
                }
            return await perform(latest)
    except Exception as exc:
        log.exception("manual full position close failed uid=%s position=%s", uid, pid)
        return {
            "state": "error",
            "position_id": pid,
            "reason": f"{type(exc).__name__}: {exc}",
        }
    finally:
        try:
            await adapter.close()
        except Exception:
            pass


async def _silent_confirmed_notify(_user_id: int, _text: str) -> bool:
    """Let the menu callback own the visible result without durable duplicates."""
    return True


async def force_position_break_even(user_id: int, position_id: str) -> dict[str, Any]:
    """Request the shared BE engine to move one exact managed position now."""
    uid = int(user_id)
    pid = clean_exchange_id(position_id)
    if uid <= 0 or not pid:
        return {"state": "not_found"}

    api_row = await get_api_key_cache().get_or_fetch(
        (uid, "api", "bingx"), lambda: db.get_api_key(uid, "bingx")
    )
    if not api_row:
        return {"state": "api_missing", "position_id": pid}

    adapter = build_adapter(api_row)
    try:
        position = await fetch_exact_live_position(adapter, pid)
        if position is None:
            return {"state": "already_closed", "position_id": pid}
        symbol = str(position.get("symbol") or "").upper()
        side = _position_side(position)
        row, ambiguous = await managed_execution_for_position(uid, position)
        if ambiguous:
            return {
                "state": "ambiguous_execution",
                "position_id": pid,
                "symbol": symbol,
                "side": side,
            }
        if row is None:
            return {
                "state": "unmanaged",
                "position_id": pid,
                "symbol": symbol,
                "side": side,
            }

        execution_id = int(row.get("id") or 0)
        async with db.execution_lock(execution_id):
            latest = await db.get_execution_by_id(execution_id)
            if latest is None or int(latest.get("user_id") or 0) != uid:
                return {"state": "stale", "position_id": pid, "symbol": symbol}
            status = str(latest.get("status") or "")
            if status not in _FORCE_BE_STATUSES:
                return {
                    "state": "not_eligible",
                    "position_id": pid,
                    "execution_id": execution_id,
                    "symbol": symbol,
                    "status": status,
                }
            if not execution_matches_position(latest, position):
                return {
                    "state": "ambiguous_execution",
                    "position_id": pid,
                    "symbol": symbol,
                    "side": side,
                }
            payload = _json_dict(latest.get("exchange_order_ids_json"))
            be_state = payload.get("be") if isinstance(payload.get("be"), dict) else {}
            if be_state.get("moved") is True:
                return {
                    "state": "already_be",
                    "position_id": pid,
                    "execution_id": execution_id,
                    "symbol": symbol,
                    "side": side,
                    "stop": _f(be_state.get("stop"), 0.0),
                }
            log.info(
                "manual force-BE requested uid=%s execution_id=%s symbol=%s side=%s position_id=%s status=%s",
                uid,
                execution_id,
                symbol,
                side,
                pid,
                status,
            )
            request = {
                "manual_requested": True,
                "manual_required": False,
                "skipped": None,
                "error": None,
                "manual_requested_at": datetime.now(timezone.utc).isoformat(),
                "manual_requested_by_user_id": uid,
                "manual_position_id": pid,
                "manual_result": None,
            }
            saved = await db.merge_execution_metadata(
                execution_id,
                {"be": request},
                expected_status=status,
            )
            if not saved:
                return {"state": "stale", "position_id": pid, "symbol": symbol}

        latest = await db.get_execution_by_id(execution_id)
        if latest is None:
            return {"state": "stale", "position_id": pid, "symbol": symbol}
        await process_be_monitor_once(
            notify=_silent_confirmed_notify,
            rows_override=[latest],
        )
        final_row = await db.get_execution_by_id(execution_id)
        final_payload = _json_dict(
            (final_row or {}).get("exchange_order_ids_json") if final_row else "{}"
        )
        final_be = (
            final_payload.get("be") if isinstance(final_payload.get("be"), dict) else {}
        )
        if execution_be_protection_confirmed(final_payload):
            return {
                "state": "moved",
                "position_id": pid,
                "execution_id": execution_id,
                "symbol": symbol,
                "side": side,
                "stop": _f(final_be.get("stop"), 0.0),
                "qty": _f(final_be.get("qty"), 0.0),
                "source": str(final_be.get("source") or ""),
            }
        manual_result = final_be.get("manual_result")
        if isinstance(manual_result, dict):
            result = {
                "state": str(manual_result.get("state") or "error"),
                "position_id": pid,
                "execution_id": execution_id,
                "symbol": symbol,
                "side": side,
                "reason": str(manual_result.get("reason") or ""),
                "stop": _f(manual_result.get("stop"), 0.0),
                "qty": _f(manual_result.get("qty"), 0.0),
                "position_qty": _f(manual_result.get("position_qty"), 0.0),
                "uncovered_qty": _f(manual_result.get("uncovered_qty"), 0.0),
                "market_price": _f(manual_result.get("market_price"), 0.0),
                "fair_price": _f(manual_result.get("fair_price"), 0.0),
            }
            log.info(
                "manual force-BE finished uid=%s execution_id=%s symbol=%s state=%s reason=%s",
                uid,
                execution_id,
                symbol,
                result["state"],
                result.get("reason") or "",
            )
            return result

        reason = _manual_be_unconfirmed_reason(final_be)
        fallback_result = {
            "state": "verification_failed",
            "reason": reason,
            "stop": _f(final_be.get("stop"), 0.0),
            "qty": _f(final_be.get("qty"), 0.0),
            "position_qty": _f(final_be.get("position_qty"), 0.0),
        }
        final_status = str((final_row or {}).get("status") or "")
        if final_status:
            try:
                await db.merge_execution_metadata(
                    execution_id,
                    {
                        "be": {
                            "manual_requested": False,
                            "manual_required": True,
                            "source": "manual",
                            "manual_result": fallback_result,
                        }
                    },
                    expected_status=final_status,
                    write_flow_audit_stage="manual_be_no_result_guard",
                    write_flow_audit_status=final_status,
                )
            except Exception:
                log.exception(
                    "failed to persist manual force-BE no-result guard execution_id=%s",
                    execution_id,
                )
        log.warning(
            "manual force-BE returned no explicit result uid=%s execution_id=%s symbol=%s reason=%s",
            uid,
            execution_id,
            symbol,
            reason,
        )
        return {
            "state": "verification_failed",
            "position_id": pid,
            "execution_id": execution_id,
            "symbol": symbol,
            "side": side,
            "reason": reason,
            "stop": fallback_result["stop"],
            "qty": fallback_result["qty"],
        }
    except Exception as exc:
        log.exception("manual force-BE failed uid=%s position=%s", uid, pid)
        return {
            "state": "error",
            "position_id": pid,
            "reason": f"{type(exc).__name__}: {exc}",
        }
    finally:
        try:
            await adapter.close()
        except Exception:
            pass
