from __future__ import annotations

import math
import asyncio
import json
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Awaitable, Callable

from datetime import datetime, timezone, timedelta
from app.config import get_settings
from app.services.monitor_diagnostics import record_stage_rows
from app.services.signal_analytics_ingress import submit_statistics_execution_linkage
from app.services.ttl_cache import get_api_key_cache, get_user_settings_cache
from app.database import db
from app.services.exchange_factory import build_adapter, exchange_title
from app.services.notification_style import card, fmt_price, fmt_qty
from app.services.durable_notifications import (
    send_or_enqueue,
    set_notification_event_key,
)
from app.services.async_utils import StaleExecutionPass, null_async_context
from app.services.transient_errors import (
    is_transient_exchange_error,
    record_transient_error,
    should_notify_transient,
    transient_retry_exhausted,
)
from app.services.tp_qty import (
    max_placeable_tp_count,
    order_normalized_qty,
    order_required_qty_step,
)
from app.services.limit_policy import (
    POLICY_KEY as LIMIT_POLICY_KEY,
    RUNTIME_KEY as LIMIT_POLICY_RUNTIME_KEY,
    record_tp_touch,
    read_policy as read_limit_policy,
    tp_mode_label as limit_tp_mode_label,
)
from app.services.tp_plan_snapshot import (
    POLICY_KEY,
    SNAPSHOT_KEY,
    build_snapshot,
    get_snapshot,
    read_policy,
    snapshot_items,
    snapshot_plan_map,
    snapshot_total_qty,
)
from app.services.tp_ambiguous_recheck import find_tp_order_after_ambiguous_write
from app.services.stop_ownership import (
    LIMIT_ATTACHED_STOP_KEY,
    build_limit_attached_stop_record,
    clean_exchange_id,
    identify_limit_attached_stop,
)
from app.services.tp_execution_ledger import (
    canonicalize_tp_ledger,
    tp_ledger_repair_metadata,
)
from app.exchanges.bingx.adapter import BingxTpCoverageError as BingxTpCoverageError

log = logging.getLogger(__name__)

NotifyFn = Callable[[int, str], Awaitable[object] | object]
_LOOP_LOCK = asyncio.Lock()
_SCAN_CURSOR = 0


def _f(value: Any, default: float = 0.0) -> float:
    """Parse a finite non-negative exchange scalar without repairing corruption."""
    try:
        if value in (None, "") or isinstance(value, bool):
            return default
        parsed = float(value)
        return parsed if math.isfinite(parsed) and parsed >= 0 else default
    except (TypeError, ValueError, OverflowError):
        return default


def _signed_f(value: Any, default: float = 0.0) -> float:
    """Parse a stored scalar without turning corrupted negatives positive."""
    try:
        if value in (None, "") or isinstance(value, bool):
            return default
        parsed = float(value)
        return parsed if math.isfinite(parsed) else default
    except (TypeError, ValueError, OverflowError):
        return default


def _position_size(pos: dict[str, Any]) -> float:
    for key in ("size", "availableSize", "positionAmt", "qty", "total"):
        val = _f(pos.get(key), 0.0)
        if val > 0:
            return val
    return 0.0


def _total_position_size(positions: list[dict[str, Any]]) -> float:
    return sum(_position_size(p) for p in positions)


def _position_id(positions: list[dict[str, Any]]) -> str:
    ids = {
        clean_exchange_id(pos.get("positionId"))
        for pos in positions or []
        if isinstance(pos, dict) and clean_exchange_id(pos.get("positionId"))
    }
    return next(iter(ids)) if len(ids) == 1 else ""


def _position_entry_price(pos: dict[str, Any]) -> float:
    for key in (
        "avgFillPrice",
        "entryPrice",
        "avgEntryPrice",
        "positionAvgPrice",
        "avgEntry",
        "openPrice",
        "avgPrice",
    ):
        try:
            val = pos.get(key)
            if val not in (None, "") and float(val) > 0:
                return float(val)
        except Exception:
            pass
    return 0.0


def _crossed_tp(side: str, current: float, tp: float) -> bool:
    return current >= tp if side.lower() == "long" else current <= tp


def _breached_stop(side: str, current: float, stop: float) -> bool:
    return current <= stop if side.lower() == "long" else current >= stop


def _tp_plan_reduction_warning(
    *,
    snapshot: dict[str, Any],
    requested_count: int,
    actual_qty: float,
) -> str:
    """Explain why fewer executable TP orders exist than signal targets."""
    requested = max(0, int(requested_count or 0))
    planned = len(snapshot_items(snapshot))
    if requested <= 0 or planned >= requested:
        return ""
    step = _f(snapshot.get("qty_step"), 0.0)
    lot_cap = max_placeable_tp_count(actual_qty, step, requested)
    if step > 0 and planned == lot_cap and lot_cap < requested:
        return (
            f"⚠️ <b>TP сокращены:</b> {planned}/{requested} - объём позиции "
            f"позволяет разместить только {lot_cap} TP по минимальному шагу BingX."
        )
    return (
        f"⚠️ <b>TP сокращены:</b> {planned}/{requested} после проверки "
        "минимального объёма и допустимого RR."
    )


async def _persist_limit_policy_runtime(
    execution_id: int, runtime: dict[str, Any]
) -> bool:
    """Persist touch evidence without ever rewriting execution status/reason."""
    return await db.merge_execution_metadata(
        int(execution_id),
        {LIMIT_POLICY_RUNTIME_KEY: dict(runtime)},
        expected_status="pending_limit",
        write_flow_audit_stage="limit_tp_catchup_policy_runtime",
        write_flow_audit_status="pending_limit",
    )


_TP_INDEX_DONE_TYPES = {
    "tp_created_after_fill",
    "tp_catchup_market_close",
    # Ambiguous writes are not completion. TP-order ambiguity is resolved by
    # live BingX dedup in recovery; market-close ambiguity requires manual review.
    "manual_tp_confirmed",
}


def _legacy_fake_tp_success(item: Any) -> bool:
    """Detect v1.6.2 synthetic TP rows that never represented a live order."""
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


def _done_tp_indices(actions: list[dict[str, Any]]) -> set[int]:
    done: set[int] = set()
    for item in actions:
        if not isinstance(item, dict):
            continue
        if str(
            item.get("type") or ""
        ) not in _TP_INDEX_DONE_TYPES or _legacy_fake_tp_success(item):
            continue
        try:
            idx = int(item.get("tp_index") or 0)
        except Exception:
            idx = 0
        if idx > 0:
            done.add(idx)
    return done


def _open_tp_qty_already_journaled(
    actions: list[dict[str, Any]], tp_qty_by_index: dict[int, float]
) -> float:
    """Quantity already reserved by TP orders from earlier catch-up cycles.

    Market catch-up closes have already reduced the live position and therefore
    must not be subtracted a second time. Only still-open TP order actions reserve
    quantity against the unchanged position size.
    """
    reserved = 0.0
    done: set[int] = set()
    for item in actions:
        if (
            not isinstance(item, dict)
            or str(item.get("type") or "") != "tp_created_after_fill"
        ):
            continue
        try:
            idx = int(item.get("tp_index") or 0)
        except Exception:
            idx = 0
        if idx <= 0 or idx in done:
            continue
        done.add(idx)
        actual = _f(item.get("qty"), 0.0)
        reserved += (
            actual if actual > 0 else max(0.0, float(tp_qty_by_index.get(idx, 0.0)))
        )
    return reserved


class PendingEntryCancelDisposition(str, Enum):
    CANCELED_NO_FILL = "canceled_no_fill"
    TERMINAL_NO_FILL = "terminal_no_fill"
    FILLED = "filled"
    POSITION_EXISTS = "position_exists"
    AMBIGUOUS = "ambiguous"
    CANCEL_REJECTED = "cancel_rejected"
    RETRY_DEFERRED = "retry_deferred"
    RETRY_EXHAUSTED = "retry_exhausted"
    BLOCKED_BY_OTHER = "blocked_by_other"
    IDENTITY_MISSING = "identity_missing"


@dataclass(frozen=True)
class PendingEntryCancelResult:
    disposition: PendingEntryCancelDisposition
    reason: str
    order_status: dict[str, Any] | None = None
    position_count: int = 0
    write_attempted: bool = False
    cancel_error_code: int | None = None
    cancel_error_message: str = ""
    cancel_retryable: bool | None = None
    cancel_response_audit: dict[str, Any] | None = None
    confirmation_audit: dict[str, Any] | None = None
    write_reservation: dict[str, Any] | None = None
    read_only: bool = False

    @property
    def canceled(self) -> bool:
        return self.disposition == PendingEntryCancelDisposition.CANCELED_NO_FILL

    @property
    def filled_or_position(self) -> bool:
        return self.disposition in {
            PendingEntryCancelDisposition.FILLED,
            PendingEntryCancelDisposition.POSITION_EXISTS,
        }


_LIMIT_CANCEL_MAX_WRITES = 3
_LIMIT_CANCEL_BACKOFF_SECONDS = (30, 120, 600)


def _parse_cancel_datetime(value: Any) -> datetime | None:
    try:
        text = str(value or "").strip().replace("Z", "+00:00")
        if not text:
            return None
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return None


def _limit_cancel_control(
    payload: dict[str, Any] | None,
    *,
    policy_reason: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return durable cancel retry state for one invalidation policy.

    The same LIMIT may be invalidated by TTL, STOP breach and TP progress.  A
    policy change starts a fresh diagnostic record, but an already exhausted
    exact order must not receive an unlimited new series of writes under a
    different label.  Therefore total writes are also retained when the exact
    order identity is unchanged.
    """
    current = (payload or {}).get("limit_cancel_pending")
    if not isinstance(current, dict):
        current = {}
    attempts = 0
    try:
        attempts = max(0, int(current.get("write_attempts") or 0))
    except (TypeError, ValueError, OverflowError):
        attempts = 0
    exhausted = bool(current.get("exhausted")) or attempts >= _LIMIT_CANCEL_MAX_WRITES
    next_retry_at = _parse_cancel_datetime(current.get("next_retry_at"))
    current_now = now or datetime.now(timezone.utc)
    allow_write = not exhausted and (
        next_retry_at is None or current_now >= next_retry_at
    )
    return {
        "existing": current,
        "policy_reason": str(policy_reason or "").strip().lower(),
        "write_attempts": attempts,
        "exhausted": exhausted,
        "allow_write": allow_write,
        "next_retry_at": next_retry_at,
    }


def _limit_cancel_pending_record(
    *,
    previous: dict[str, Any] | None,
    policy_reason: str,
    result: PendingEntryCancelResult,
    now: datetime | None = None,
) -> dict[str, Any]:
    current_now = now or datetime.now(timezone.utc)
    reserved = (
        dict(result.write_reservation)
        if isinstance(result.write_reservation, dict)
        else None
    )
    prev = reserved or (dict(previous) if isinstance(previous, dict) else {})
    try:
        prior_attempts = max(0, int(prev.get("write_attempts") or 0))
    except (TypeError, ValueError, OverflowError):
        prior_attempts = 0
        # v1.6.38 reserves the attempt durably before network I/O. Older direct
        # tests/callers without a reservation retain the legacy increment behavior.
    attempts = prior_attempts + (
        1 if result.write_attempted and reserved is None else 0
    )
    non_retryable_rejection = (
        result.disposition == PendingEntryCancelDisposition.CANCEL_REJECTED
        and result.cancel_retryable is False
    )
    exhausted = (
        bool(prev.get("exhausted"))
        or non_retryable_rejection
        or attempts >= _LIMIT_CANCEL_MAX_WRITES
    )
    if exhausted:
        next_retry_at = None
    elif not result.write_attempted:
        next_retry_at = _parse_cancel_datetime(prev.get("next_retry_at"))
    else:
        delay_index = max(0, min(attempts - 1, len(_LIMIT_CANCEL_BACKOFF_SECONDS) - 1))
        delay = _LIMIT_CANCEL_BACKOFF_SECONDS[delay_index]
        next_retry_at = current_now + timedelta(seconds=delay)
    first_detected = (
        str(prev.get("first_detected_at") or "").strip() or current_now.isoformat()
    )
    record = {
        "reason": str(policy_reason or "").strip().lower(),
        "error": str(result.reason or "")[:1000],
        "disposition": result.disposition.value,
        "write_attempts": attempts,
        "max_write_attempts": _LIMIT_CANCEL_MAX_WRITES,
        "write_attempted_last_pass": bool(result.write_attempted),
        "first_detected_at": first_detected,
        "last_checked_at": current_now.isoformat(),
        "last_write_at": (
            current_now.isoformat()
            if result.write_attempted
            else prev.get("last_write_at")
        ),
        "next_retry_at": next_retry_at.isoformat() if next_retry_at else None,
        "exhausted": exhausted,
        "cancel_error_code": result.cancel_error_code,
        "cancel_error_message": str(result.cancel_error_message or "")[:500],
        "cancel_retryable": result.cancel_retryable,
        "cancel_response_v1": (
            dict(result.cancel_response_audit)
            if isinstance(result.cancel_response_audit, dict)
            else prev.get("cancel_response_v1")
        ),
        "confirmation_v1": (
            dict(result.confirmation_audit)
            if isinstance(result.confirmation_audit, dict)
            else prev.get("confirmation_v1")
        ),
        "read_only_last_pass": bool(result.read_only),
        "manual_action_required": exhausted,
        "reservation_v1": (
            {
                **dict(prev.get("reservation_v1") or {}),
                "state": (
                    "write_dispatched"
                    if result.write_attempted
                    else str(
                        (prev.get("reservation_v1") or {}).get("state") or "reserved"
                    )
                ),
                "completed_at": (
                    current_now.isoformat()
                    if result.write_attempted
                    else (prev.get("reservation_v1") or {}).get("completed_at")
                ),
                "result_disposition": result.disposition.value,
            }
            if isinstance(prev.get("reservation_v1"), dict)
            else None
        ),
    }
    return record


def _limit_cancel_should_notify(
    previous: dict[str, Any] | None,
    current: dict[str, Any],
) -> bool:
    prev = previous if isinstance(previous, dict) else {}
    if not prev:
        return True
    if bool(current.get("exhausted")) and not bool(prev.get("exhausted")):
        return True
        # A concrete exchange code is materially new information.
    return current.get("cancel_error_code") not in (None, prev.get("cancel_error_code"))


def _entry_identity_values(entry_payload: dict[str, Any]) -> set[str]:
    values: set[str] = set()
    candidates = [entry_payload]
    nested = entry_payload.get("data")
    if isinstance(nested, dict):
        candidates.append(nested)
    for candidate in candidates:
        for key in (
            "_entry_external_oid",
            "externalOid",
            "clientOrderId",
            "newClientOrderId",
            "clientId",
            "clOrdId",
        ):
            value = clean_exchange_id(candidate.get(key))
            if value:
                values.add(value)
    return values


def _status_has_fill(status: dict[str, Any] | None) -> bool:
    if not isinstance(status, dict):
        return False
    return _f(status.get("filled_qty"), 0.0) > 0 or bool(status.get("fully_filled"))


def _pending_cancel_reason(payload: dict[str, Any] | None) -> str:
    raw = (payload or {}).get("limit_cancel_pending")
    if not isinstance(raw, dict):
        return ""
    return str(raw.get("reason") or "").strip().lower()


def _terminal_no_fill_classification(
    status: dict[str, Any],
    *,
    policy_reason: str = "",
) -> tuple[str, str, str]:
    """Map a terminal zero-fill BingX entry to an honest local outcome."""
    try:
        state = int(status.get("state") or 0)
    except (TypeError, ValueError):
        state = 0
    if state == 4:
        normalized_reason = str(policy_reason or "").strip().lower()
        if normalized_reason == "ttl":
            return (
                "canceled_expired",
                "Previously requested TTL cancellation is now confirmed by terminal BingX state with zero fill",
                "⏰ LIMIT ордер отменён по истечению срока",
            )
        if normalized_reason == "stop_breached":
            return (
                "canceled_stop_invalidated",
                "Previously requested STOP-invalidation cancellation is now confirmed by terminal BingX state with zero fill",
                "🚫 LIMIT ордер отменён после пробоя STOP",
            )
        if normalized_reason == "tp_progress":
            return (
                "canceled_tp_progress",
                "Previously requested TP-progress cancellation is now confirmed by terminal BingX state with zero fill",
                "📈 LIMIT ордер отменён по прогрессу целей",
            )
        return (
            "canceled_external",
            "LIMIT entry was canceled on BingX without any fill",
            "ℹ️ LIMIT ордер отменён на BingX без исполнения",
        )
    if state == 5:
        return (
            "error",
            "LIMIT entry became invalid/rejected on BingX without any fill",
            "❌ LIMIT ордер отклонён или признан недействительным BingX",
        )
    return (
        "error",
        f"LIMIT entry reached unexpected terminal state={state} without any fill",
        "⚠️ LIMIT ордер завершён BingX без исполнения с неизвестным статусом",
    )


async def _resolve_exact_entry_order_id_from_open_orders(
    adapter: Any,
    *,
    symbol: str,
    entry_payload: dict[str, Any],
) -> tuple[str, str]:
    """Resolve an external/client id to one exact live BingX order id.

    This is a read-only fallback for old rows that saved ``externalOid`` but not
    ``orderId``.  It deliberately refuses fuzzy price/quantity matching because
    a manual order can have identical values.
    """
    identities = _entry_identity_values(entry_payload)
    if not identities:
        return "", "entry payload has no external/client identity"
    try:
        orders = await adapter.fetch_open_orders(symbol)
    except Exception as exc:
        return "", f"open-order identity read failed: {type(exc).__name__}: {exc}"
    matches: list[dict[str, Any]] = []
    for order in orders or []:
        if not isinstance(order, dict):
            continue
        client_id = clean_exchange_id(order.get("clientOrderId"))
        if client_id and client_id in identities:
            matches.append(order)
    if len(matches) != 1:
        return "", f"external/client identity matched {len(matches)} live orders"
    order_id = clean_exchange_id(matches[0].get("orderId"))
    if not order_id:
        return "", "matched live order has no orderId"
    return order_id, "resolved exact orderId from live external/client identity"


def _entry_price_from_payload(entry_payload: dict[str, Any]) -> float:
    candidates = [entry_payload]
    nested = entry_payload.get("data") if isinstance(entry_payload, dict) else None
    if isinstance(nested, dict):
        candidates.append(nested)
    for candidate in candidates:
        price = _f(
            candidate.get("price")
            or candidate.get("entry")
            or candidate.get("triggerPrice")
            or candidate.get("stopPrice"),
            0.0,
        )
        if price > 0:
            return price
    return 0.0


def _open_order_could_be_identityless_entry(order: dict[str, Any], *, side: str, entry_price: float) -> bool:
    typ = str(order.get("type") or "").upper()
    if typ in {"STOP", "STOP_MARKET", "TAKE_PROFIT", "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET", "TRAILING_TP_SL"}:
        return False
    wanted = str(side or "").strip().lower()
    row_side = str(order.get("positionSide") or order.get("side") or "").strip().lower()
    if row_side in {"buy", "sell"}:
        row_side = "long" if row_side == "buy" else "short"
    if wanted in {"long", "short"} and row_side in {"long", "short"} and row_side != wanted:
        return False
    if entry_price > 0:
        order_price = _f(order.get("price") or (order.get("raw") or {}).get("price"), 0.0)
        tolerance = max(abs(entry_price) * 1e-8, 1e-12)
        if order_price > 0 and abs(order_price - entry_price) > tolerance:
            return False
    return True


async def _prove_live_position_without_open_entry(
    adapter: Any,
    *,
    symbol: str,
    side: str,
    entry_payload: dict[str, Any],
    live_qty: float,
) -> dict[str, Any]:
    """Read-only proof for BingX LIMIT fills when order detail identity is weak.

    BingX can return/keep an entry order identity in shapes that older releases
    did not persist.  When a same-side position is already visible and BingX has
    no plausible regular entry order left open for the same symbol/side/entry
    price, waiting forever for a terminal order-detail row leaves the position
    without TP management.  This proof never sends writes; it only decides
    whether the already visible position may be treated as the final filled
    quantity for TP planning.
    """

    entry_price = _entry_price_from_payload(entry_payload)
    snapshot: dict[str, Any] = {
        "version": 1,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "side": side,
        "entry_price": entry_price,
        "observed_live_qty": float(max(0.0, live_qty)),
        "can_adopt": False,
        "reason": "not_checked",
    }
    if live_qty <= 0:
        snapshot["reason"] = "no_live_position"
        return snapshot
    try:
        open_orders = await adapter.fetch_open_orders(symbol)
    except Exception as exc:
        snapshot["reason"] = "open_orders_read_failed"
        snapshot["error"] = f"{type(exc).__name__}: {exc}"[:500]
        return snapshot

    rows = [row for row in (open_orders or []) if isinstance(row, dict)]
    plausible = [
        row
        for row in rows
        if _open_order_could_be_identityless_entry(
            row, side=side, entry_price=entry_price
        )
    ]
    snapshot["open_orders"] = {
        "symbol_order_count": len(rows),
        "plausible_entry_count": len(plausible),
        "plausible_entries": [_compact_cancel_open_order(row) for row in plausible[:3]],
    }
    if plausible:
        snapshot["reason"] = "plausible_entry_order_still_open"
        return snapshot
    snapshot["can_adopt"] = True
    snapshot["reason"] = "live_position_and_no_plausible_open_entry"
    return snapshot


async def _prove_identityless_pending_entry_absent(
    adapter: Any,
    *,
    symbol: str,
    side: str,
    entry_payload: dict[str, Any],
) -> PendingEntryCancelResult:
    """Read-only cleanup proof for legacy LIMIT rows with no order identity.

    Old BingX rows created before client/order-id persistence cannot be cancelled
    exactly.  The safe action is not to send a broad cancel, but to remove the
    local pending row only when BingX proves: no same-side position exists and no
    plausible regular entry order remains open for this symbol/side/entry price.
    """

    entry_price = _entry_price_from_payload(entry_payload)
    open_result, positions_result = await asyncio.gather(
        adapter.fetch_open_orders(symbol),
        adapter.fetch_open_positions(symbol, str(side or "").upper()),
        return_exceptions=True,
    )
    errors: dict[str, str] = {}
    if isinstance(open_result, BaseException):
        open_orders: list[dict[str, Any]] | None = None
        errors["open_orders"] = f"{type(open_result).__name__}: {open_result}"[:500]
    else:
        open_orders = [row for row in (open_result or []) if isinstance(row, dict)]
    if isinstance(positions_result, BaseException):
        positions: list[dict[str, Any]] = []
        errors["positions"] = f"{type(positions_result).__name__}: {positions_result}"[:500]
    else:
        positions = [row for row in (positions_result or []) if isinstance(row, dict)]

    plausible = []
    if open_orders is not None:
        plausible = [
            row
            for row in open_orders
            if _open_order_could_be_identityless_entry(
                row, side=side, entry_price=entry_price
            )
        ]
    snapshot = {
        "version": 1,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "identityless": True,
        "entry_price": entry_price,
        "open_orders": {
            "state": "error" if open_orders is None else "present" if plausible else "absent",
            "symbol_order_count": len(open_orders or []),
            "plausible_entry_count": len(plausible),
            "plausible_entries": [_compact_cancel_open_order(row) for row in plausible[:3]],
        },
        "position": {
            "count": len(positions),
            "total_qty": float(_total_position_size(positions)),
            "position_id": _position_id(positions),
        },
        "errors": errors,
    }
    if positions:
        return PendingEntryCancelResult(
            PendingEntryCancelDisposition.POSITION_EXISTS,
            "Legacy LIMIT has no exact orderId, but BingX still shows a same-side position.",
            {"state": 0, "terminal": False, "fully_filled": False, "filled_qty": 0.0},
            len(positions),
            confirmation_audit=snapshot,
            read_only=True,
        )
    if errors:
        return PendingEntryCancelResult(
            PendingEntryCancelDisposition.AMBIGUOUS,
            "Legacy LIMIT has no exact orderId and BingX state could not be fully read.",
            {"state": 0, "terminal": False, "fully_filled": False, "filled_qty": 0.0},
            confirmation_audit=snapshot,
            read_only=True,
        )
    if plausible:
        return PendingEntryCancelResult(
            PendingEntryCancelDisposition.AMBIGUOUS,
            "Legacy LIMIT has no exact orderId and a plausible live entry order still exists on BingX.",
            {"state": 1, "terminal": False, "fully_filled": False, "filled_qty": 0.0},
            confirmation_audit=snapshot,
            read_only=True,
        )
    return PendingEntryCancelResult(
        PendingEntryCancelDisposition.CANCELED_NO_FILL,
        "Legacy LIMIT has no exact orderId, but BingX shows no same-side position and no plausible live entry order; local phantom can be removed safely.",
        {
            "state": 4,
            "state_name": "ABSENT_IDENTITYLESS",
            "status": "ABSENT_IDENTITYLESS",
            "terminal": True,
            "fully_filled": False,
            "filled_qty": 0.0,
            "requested_qty": 0.0,
            "order_id": "",
        },
        confirmation_audit=snapshot,
        read_only=True,
    )


def _compact_cancel_status(status: dict[str, Any] | None) -> dict[str, Any]:
    source = status if isinstance(status, dict) else {}
    return {
        key: source.get(key)
        for key in (
            "order_id",
            "state",
            "terminal",
            "fully_filled",
            "filled_qty",
            "requested_qty",
            "avg_fill_price",
            "position_id",
            "external_oid",
            "qty_step",
        )
    }


def _compact_cancel_open_order(order: dict[str, Any]) -> dict[str, Any]:
    raw = order.get("raw") if isinstance(order.get("raw"), dict) else {}
    return {
        "order_id": clean_exchange_id(order.get("orderId") or raw.get("orderId")),
        "client_order_id": clean_exchange_id(
            order.get("clientOrderId") or raw.get("externalOid")
        ),
        "status": str(order.get("status") or raw.get("state") or "")[:80],
        "qty": _f(order.get("qty"), 0.0),
        "contracts": _f(order.get("contracts") or raw.get("vol"), 0.0),
        "price": _f(order.get("price") or raw.get("price"), 0.0),
    }


async def _read_pending_cancel_snapshot(
    adapter: Any,
    *,
    symbol: str,
    side: str,
    exact_payload: dict[str, Any],
    resolved_order_id: str,
) -> tuple[dict[str, Any], dict[str, Any] | None, list[dict[str, Any]]]:
    """Read order detail, exact open-order presence and live position together."""

    async def _missing_read(name: str):
        raise AttributeError(f"adapter has no {name}")

    fetch_open_orders = getattr(adapter, "fetch_open_orders", None)
    open_orders_call = (
        fetch_open_orders(symbol)
        if callable(fetch_open_orders)
        else _missing_read("fetch_open_orders")
    )

    status_result, open_result, positions_result = await asyncio.gather(
        adapter.fetch_entry_order_fill_status(
            symbol=symbol, order_response=exact_payload
        ),
        open_orders_call,
        adapter.fetch_open_positions(symbol, side.upper()),
        return_exceptions=True,
    )

    errors: dict[str, str] = {}
    status: dict[str, Any] | None
    if isinstance(status_result, BaseException):
        status = None
        errors["order_get"] = f"{type(status_result).__name__}: {status_result}"[
            :500
        ]
    else:
        status = status_result if isinstance(status_result, dict) else None
        if status is None:
            errors["order_get"] = "invalid order/get response"

    open_orders: list[dict[str, Any]] | None
    if isinstance(open_result, BaseException):
        open_orders = None
        errors["open_orders"] = f"{type(open_result).__name__}: {open_result}"[:500]
    else:
        open_orders = [row for row in (open_result or []) if isinstance(row, dict)]

    positions: list[dict[str, Any]]
    if isinstance(positions_result, BaseException):
        positions = []
        errors["positions"] = (
            f"{type(positions_result).__name__}: {positions_result}"[:500]
        )
    else:
        positions = [row for row in (positions_result or []) if isinstance(row, dict)]

    exact_matches: list[dict[str, Any]] = []
    if open_orders is not None:
        for row in open_orders:
            raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
            row_id = clean_exchange_id(row.get("orderId") or raw.get("orderId"))
            if row_id == resolved_order_id:
                exact_matches.append(row)

    open_state = (
        "error" if open_orders is None else "present" if exact_matches else "absent"
    )
    snapshot = {
        "version": 1,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "order_id": resolved_order_id,
        "order_get": _compact_cancel_status(status),
        "open_orders": {
            "state": open_state,
            "exact_match_count": len(exact_matches),
            "exact_matches": [
                _compact_cancel_open_order(row) for row in exact_matches[:3]
            ],
            "symbol_order_count": len(open_orders or []),
        },
        "position": {
            "count": len(positions),
            "total_qty": float(_total_position_size(positions)),
            "position_id": _position_id(positions),
        },
        "errors": errors,
    }
    return snapshot, status, positions


def _plain_cancel_diagnosis(
    snapshot: dict[str, Any] | None,
    *,
    cancel_error_code: int | None = None,
    cancel_error_message: str = "",
    read_only: bool = False,
) -> str:
    """Build one clear Russian explanation without hiding failed reads."""

    snap = snapshot if isinstance(snapshot, dict) else {}
    order_get = snap.get("order_get") if isinstance(snap.get("order_get"), dict) else {}
    open_orders = (
        snap.get("open_orders") if isinstance(snap.get("open_orders"), dict) else {}
    )
    position = snap.get("position") if isinstance(snap.get("position"), dict) else {}
    errors = snap.get("errors") if isinstance(snap.get("errors"), dict) else {}
    state = order_get.get("state")
    filled = _f(order_get.get("filled_qty"), 0.0)
    open_state = str(open_orders.get("state") or "unknown")
    position_count = int(_f(position.get("count"), 0.0))

    unconfirmed_detail = ""
    if cancel_error_code is None and str(cancel_error_message or "").strip():
        raw_cancel_detail = str(cancel_error_message).strip()[:300]
        lowered_detail = raw_cancel_detail.lower()
        if "no unique per-order result" in lowered_detail:
            raw_cancel_detail = (
                "BingX не вернула единственную строку результата для точного orderId"
            )
        elif "no valid per-order errorcode" in lowered_detail:
            raw_cancel_detail = (
                "в строке ответа BingX отсутствует корректный per-order errorCode"
            )
        unconfirmed_detail = (
            " Ответ точного cancel не подтвердил результат: " f"{raw_cancel_detail}."
        )

    read_error_detail = ""
    if errors:
        parts = []
        for key in sorted(str(item) for item in errors):
            raw = str(errors.get(key) or "неизвестная ошибка").strip()[:220]
            parts.append(f"{key}: {raw}")
        read_error_detail = (
            " Дополнительно не удалось полностью проверить: " + "; ".join(parts) + "."
        )

    if position_count > 0:
        base = "Во время отмены BingX уже показывает открытую позицию по этой заявке."
    elif filled > 0:
        base = f"Во время отмены ордер успел исполниться на объём {filled:g}."
    elif bool(order_get.get("terminal")) and open_state == "absent" and not errors:
        base = f"Ордер уже находится в конечном состоянии BingX (state={state})."
    elif cancel_error_code is not None:
        message = str(cancel_error_message or "без пояснения BingX").strip()
        base = f"BingX отклонила точную отмену: код {cancel_error_code}, {message}."
    elif open_state == "present" and not bool(order_get.get("terminal")):
        base = (
            f"BingX всё ещё показывает точный LIMIT активным: state={state}; "
            "он присутствует в списке открытых ордеров."
        )
    elif open_state == "absent" and state not in (None, 3, 4, 5):
        base = (
            f"Данные BingX противоречат друг другу: order/get показывает state={state}, "
            "но точного ордера уже нет в open_orders."
        )
    elif errors:
        base = "BingX не дала полностью проверить отмену."
    elif read_only:
        base = (
            "Новых cancel-запросов не отправлялось; выполнена только безопасная "
            "проверка состояния."
        )
    else:
        base = (
            "BingX приняла запрос, но конечное состояние точного ордера пока "
            "не подтверждено."
        )
    return f"{base}{unconfirmed_detail}{read_error_detail}"


async def _cancel_pending_entry_confirmed(
    adapter: Any,
    *,
    user_id: int,
    execution_id: int,
    symbol: str,
    side: str,
    entry_order: dict[str, Any] | None,
    other_active_execution: bool,
    attempts: int = 6,
    allow_write: bool = True,
    write_suppressed_exhausted: bool = False,
    policy_reason: str = "unspecified",
) -> PendingEntryCancelResult:
    """Cancel one exact stale LIMIT and prove the result from three read sources.

    Every write is exact-order only. Confirmation combines ``order/get``, the
    exact row in ``open_orders`` and the live position/fill state. Once durable
    backoff or the three-write ceiling suppresses a write, the same function
    remains useful as a read-only reconciliation path.
    """

    async with db.symbol_action_lock(int(user_id), symbol):
        entry_payload = dict(entry_order) if isinstance(entry_order, dict) else {}
        direct_identity = getattr(adapter, "entry_order_id", None)
        resolved_order_id = ""
        if entry_payload and callable(direct_identity):
            try:
                resolved_order_id = clean_exchange_id(direct_identity(entry_payload))
            except Exception:
                resolved_order_id = ""

        identities = _entry_identity_values(entry_payload)
        if not resolved_order_id and identities:
            (
                resolved_order_id,
                resolve_reason,
            ) = await _resolve_exact_entry_order_id_from_open_orders(
                adapter,
                symbol=symbol,
                entry_payload=entry_payload,
            )
            if not resolved_order_id:
                return PendingEntryCancelResult(
                    PendingEntryCancelDisposition.AMBIGUOUS,
                    resolve_reason,
                    read_only=not allow_write,
                )

        if not resolved_order_id:
            fresh_other = await db.other_active_symbol_executions(
                int(user_id), symbol, int(execution_id), limit=1
            )
            if other_active_execution or fresh_other:
                return PendingEntryCancelResult(
                    PendingEntryCancelDisposition.BLOCKED_BY_OTHER,
                    "У LIMIT нет точного orderId, а этот символ уже используется другой активной сделкой.",
                    read_only=not allow_write,
                )
            identityless = await _prove_identityless_pending_entry_absent(
                adapter,
                symbol=symbol,
                side=side,
                entry_payload=entry_payload,
            )
            if identityless.disposition == PendingEntryCancelDisposition.CANCELED_NO_FILL:
                return identityless
            return PendingEntryCancelResult(
                PendingEntryCancelDisposition.IDENTITY_MISSING,
                "У LIMIT нет точного orderId/externalOid; широкая отмена по символу запрещена. "
                + identityless.reason,
                identityless.order_status,
                identityless.position_count,
                confirmation_audit=identityless.confirmation_audit,
                read_only=not allow_write,
            )

        exact_payload = dict(entry_payload)
        exact_payload["orderId"] = resolved_order_id

        # First authoritative snapshot prevents a cancel write when a fill or a
        # position has already appeared. It also lets read-only invocations make
        # progress after the write ceiling is exhausted.
        (
            first_snapshot,
            first_status,
            first_positions,
        ) = await _read_pending_cancel_snapshot(
            adapter,
            symbol=symbol,
            side=side,
            exact_payload=exact_payload,
            resolved_order_id=resolved_order_id,
        )
        if first_positions:
            return PendingEntryCancelResult(
                PendingEntryCancelDisposition.POSITION_EXISTS,
                _plain_cancel_diagnosis(first_snapshot, read_only=not allow_write),
                first_status,
                len(first_positions),
                confirmation_audit=first_snapshot,
                read_only=not allow_write,
            )
        if _status_has_fill(first_status):
            return PendingEntryCancelResult(
                PendingEntryCancelDisposition.FILLED,
                _plain_cancel_diagnosis(first_snapshot, read_only=not allow_write),
                first_status,
                confirmation_audit=first_snapshot,
                read_only=not allow_write,
            )

        if isinstance(first_status, dict) and bool(first_status.get("terminal")):
            await asyncio.sleep(0.20)
            (
                confirm_snapshot,
                confirm_status,
                confirm_positions,
            ) = await _read_pending_cancel_snapshot(
                adapter,
                symbol=symbol,
                side=side,
                exact_payload=exact_payload,
                resolved_order_id=resolved_order_id,
            )
            if confirm_positions:
                return PendingEntryCancelResult(
                    PendingEntryCancelDisposition.POSITION_EXISTS,
                    _plain_cancel_diagnosis(
                        confirm_snapshot, read_only=not allow_write
                    ),
                    confirm_status,
                    len(confirm_positions),
                    confirmation_audit=confirm_snapshot,
                    read_only=not allow_write,
                )
            if _status_has_fill(confirm_status):
                return PendingEntryCancelResult(
                    PendingEntryCancelDisposition.FILLED,
                    _plain_cancel_diagnosis(
                        confirm_snapshot, read_only=not allow_write
                    ),
                    confirm_status,
                    confirmation_audit=confirm_snapshot,
                    read_only=not allow_write,
                )
            first_open = (
                first_snapshot.get("open_orders")
                if isinstance(first_snapshot.get("open_orders"), dict)
                else {}
            )
            first_errors = (
                first_snapshot.get("errors")
                if isinstance(first_snapshot.get("errors"), dict)
                else {}
            )
            confirm_open = (
                confirm_snapshot.get("open_orders")
                if isinstance(confirm_snapshot.get("open_orders"), dict)
                else {}
            )
            confirm_errors = (
                confirm_snapshot.get("errors")
                if isinstance(confirm_snapshot.get("errors"), dict)
                else {}
            )
            if (
                isinstance(confirm_status, dict)
                and bool(confirm_status.get("terminal"))
                and str(first_open.get("state") or "unknown") == "absent"
                and str(confirm_open.get("state") or "unknown") == "absent"
                and not first_errors
                and not confirm_errors
            ):
                try:
                    terminal_state = int(confirm_status.get("state") or 0)
                except (TypeError, ValueError, OverflowError):
                    terminal_state = 0
                return PendingEntryCancelResult(
                    (
                        PendingEntryCancelDisposition.CANCELED_NO_FILL
                        if terminal_state == 4
                        else PendingEntryCancelDisposition.TERMINAL_NO_FILL
                    ),
                    _plain_cancel_diagnosis(
                        confirm_snapshot, read_only=not allow_write
                    ),
                    confirm_status,
                    confirmation_audit=confirm_snapshot,
                    read_only=not allow_write,
                )
                # order/get already says terminal. Never send a redundant cancel just
                # because open_orders is stale or contradictory.
            return PendingEntryCancelResult(
                PendingEntryCancelDisposition.AMBIGUOUS,
                _plain_cancel_diagnosis(confirm_snapshot, read_only=True),
                confirm_status,
                confirmation_audit=confirm_snapshot,
                read_only=True,
            )

        reservation_record: dict[str, Any] | None = None
        reservation_note = ""
        if allow_write:
            try:
                reservation_outcome = await db.reserve_limit_cancel_write(
                    int(execution_id),
                    policy_reason=str(policy_reason or "unspecified"),
                    expected_status="pending_limit",
                    max_writes=_LIMIT_CANCEL_MAX_WRITES,
                    backoff_seconds=_LIMIT_CANCEL_BACKOFF_SECONDS,
                )
            except Exception as exc:
                reservation_outcome = {
                    "reserved": False,
                    "reason": f"reservation_error:{type(exc).__name__}",
                    "record": {},
                }
                reservation_note = (
                    " Durable-резервирование cancel не удалось; внешний запрос "
                    f"не отправлен ({type(exc).__name__}: {exc})."
                )
            raw_record = reservation_outcome.get("record")
            if isinstance(raw_record, dict):
                reservation_record = dict(raw_record)
            if not bool(reservation_outcome.get("reserved")):
                reserve_reason = str(
                    reservation_outcome.get("reason") or "reservation_denied"
                )
                if not reservation_note:
                    reservation_note = (
                        " Новый cancel-запрос не отправлен: durable-резервирование "
                        f"запрещено ({reserve_reason})."
                    )
                allow_write = False
                if reserve_reason == "exhausted":
                    write_suppressed_exhausted = True

        cancel_error = ""
        cancel_error_code: int | None = None
        cancel_error_message = ""
        cancel_retryable: bool | None = None
        cancel_response_audit: dict[str, Any] | None = None
        write_attempted = False

        if allow_write:
            write_attempted = True
            try:
                response = await adapter.cancel_entry_order(exact_payload)
                exact_result = (
                    response.get("_exact_cancel_result")
                    if isinstance(response, dict)
                    and isinstance(response.get("_exact_cancel_result"), dict)
                    else {}
                )
                audit = exact_result.get("response_audit")
                if isinstance(audit, dict):
                    cancel_response_audit = dict(audit)
                cancel_error_message = str(
                    exact_result.get("error_message") or ""
                ).strip()
            except Exception as exc:
                # The write may have raced with a fill/terminal transition. Never
                # infer failure from the exception alone; exact reads below decide.
                cancel_error = f"{type(exc).__name__}: {exc}"
                raw_code = getattr(exc, "error_code", None)
                try:
                    cancel_error_code = (
                        int(raw_code)
                        if raw_code not in (None, "") and not isinstance(raw_code, bool)
                        else None
                    )
                except (TypeError, ValueError, OverflowError):
                    cancel_error_code = None
                cancel_error_message = str(
                    getattr(exc, "error_message", "") or str(exc)
                ).strip()
                raw_retryable = getattr(exc, "retryable", None)
                cancel_retryable = (
                    bool(raw_retryable) if isinstance(raw_retryable, bool) else None
                )
                raw_audit = getattr(exc, "response_audit", None)
                if isinstance(raw_audit, dict):
                    cancel_response_audit = dict(raw_audit)

                    # A post-write window of about 15 seconds catches BingX propagation lag.
                    # Read-only passes are deliberately shorter and recur durably in later
                    # monitor cycles without issuing a fourth cancel write.
        write_delays = (0.0, 0.5, 1.0, 2.0, 4.0, 8.0)
        read_only_delays = (0.0, 0.5, 1.5, 3.0)
        delays = write_delays if allow_write else read_only_delays
        delays = delays[: max(2, min(int(attempts or 2), len(delays)))]

        latest_snapshot = first_snapshot
        latest_status = first_status
        terminal_confirmations = 0
        for delay in delays:
            if delay > 0:
                await asyncio.sleep(delay)
            snapshot, status, positions = await _read_pending_cancel_snapshot(
                adapter,
                symbol=symbol,
                side=side,
                exact_payload=exact_payload,
                resolved_order_id=resolved_order_id,
            )
            latest_snapshot = snapshot
            latest_status = status
            if positions:
                return PendingEntryCancelResult(
                    PendingEntryCancelDisposition.POSITION_EXISTS,
                    _plain_cancel_diagnosis(
                        snapshot,
                        cancel_error_code=cancel_error_code,
                        cancel_error_message=cancel_error_message,
                        read_only=not allow_write,
                    ),
                    status,
                    len(positions),
                    write_attempted=write_attempted,
                    cancel_error_code=cancel_error_code,
                    cancel_error_message=cancel_error_message,
                    cancel_retryable=cancel_retryable,
                    cancel_response_audit=cancel_response_audit,
                    confirmation_audit=snapshot,
                    write_reservation=reservation_record,
                    read_only=not allow_write,
                )
            if _status_has_fill(status):
                return PendingEntryCancelResult(
                    PendingEntryCancelDisposition.FILLED,
                    _plain_cancel_diagnosis(
                        snapshot,
                        cancel_error_code=cancel_error_code,
                        cancel_error_message=cancel_error_message,
                        read_only=not allow_write,
                    ),
                    status,
                    write_attempted=write_attempted,
                    cancel_error_code=cancel_error_code,
                    cancel_error_message=cancel_error_message,
                    cancel_retryable=cancel_retryable,
                    cancel_response_audit=cancel_response_audit,
                    confirmation_audit=snapshot,
                    write_reservation=reservation_record,
                    read_only=not allow_write,
                )

            open_info = (
                snapshot.get("open_orders")
                if isinstance(snapshot.get("open_orders"), dict)
                else {}
            )
            exact_open_state = str(open_info.get("state") or "unknown")
            snapshot_errors = (
                snapshot.get("errors")
                if isinstance(snapshot.get("errors"), dict)
                else {}
            )
            if (
                isinstance(status, dict)
                and bool(status.get("terminal"))
                and exact_open_state == "absent"
                and not snapshot_errors
            ):
                terminal_confirmations += 1
                if terminal_confirmations >= 2:
                    try:
                        terminal_state = int(status.get("state") or 0)
                    except (TypeError, ValueError, OverflowError):
                        terminal_state = 0
                    return PendingEntryCancelResult(
                        (
                            PendingEntryCancelDisposition.CANCELED_NO_FILL
                            if terminal_state == 4
                            else PendingEntryCancelDisposition.TERMINAL_NO_FILL
                        ),
                        _plain_cancel_diagnosis(
                            snapshot,
                            cancel_error_code=cancel_error_code,
                            cancel_error_message=cancel_error_message,
                            read_only=not allow_write,
                        ),
                        status,
                        write_attempted=write_attempted,
                        cancel_error_code=cancel_error_code,
                        cancel_error_message=cancel_error_message,
                        cancel_retryable=cancel_retryable,
                        cancel_response_audit=cancel_response_audit,
                        confirmation_audit=snapshot,
                        write_reservation=reservation_record,
                        read_only=not allow_write,
                    )
            else:
                terminal_confirmations = 0

        reason = _plain_cancel_diagnosis(
            latest_snapshot,
            cancel_error_code=cancel_error_code,
            cancel_error_message=cancel_error_message,
            read_only=not allow_write,
        )
        if reservation_note:
            reason = f"{reason}{reservation_note}"
        if cancel_error and cancel_error_code is None and not cancel_response_audit:
            reason = f"{reason} Техническая деталь: {cancel_error [:500]}"

        if not allow_write:
            disposition = (
                PendingEntryCancelDisposition.RETRY_EXHAUSTED
                if write_suppressed_exhausted
                else PendingEntryCancelDisposition.RETRY_DEFERRED
            )
        else:
            disposition = (
                PendingEntryCancelDisposition.CANCEL_REJECTED
                if cancel_error_code is not None
                else PendingEntryCancelDisposition.AMBIGUOUS
            )
        return PendingEntryCancelResult(
            disposition,
            reason,
            latest_status,
            write_attempted=write_attempted,
            cancel_error_code=cancel_error_code,
            cancel_error_message=cancel_error_message,
            cancel_retryable=cancel_retryable,
            cancel_response_audit=cancel_response_audit,
            confirmation_audit=latest_snapshot,
            write_reservation=reservation_record,
            read_only=not allow_write,
        )


async def _cancel_opening_order_remainder_confirmed(
    adapter: Any,
    *,
    symbol: str,
    side: str,
    entry_order: dict[str, Any] | None,
    attempts: int = 6,
    execution_id: int | None = None,
    policy_reason: str = "partial_remainder",
    allow_write: bool = True,
    write_suppressed_exhausted: bool = False,
) -> PendingEntryCancelResult:
    """Cancel only this execution's remaining LIMIT quantity after partial fill.

    A live position is expected in this path, therefore position presence does
    not block exact cancellation. Success requires the exact entry order to be
    terminal; the returned status preserves the final filled quantity.

    G65: automatic TTL/TP-progress remainder cancellation can opt into the same
    durable three-write reservation/backoff used by ordinary stale LIMIT
    cancellation. Manual callers may omit ``execution_id`` and retain the
    previous one-shot exact-order behavior.
    """
    entry_payload = dict(entry_order) if isinstance(entry_order, dict) else {}
    direct_identity = getattr(adapter, "entry_order_id", None)
    resolved_order_id = ""
    if entry_payload and callable(direct_identity):
        try:
            resolved_order_id = str(direct_identity(entry_payload) or "").strip()
        except Exception:
            resolved_order_id = ""

    status: dict[str, Any] | None = None
    read_error = ""
    if entry_payload:
        try:
            status = await adapter.fetch_entry_order_fill_status(
                symbol=symbol, order_response=entry_payload
            )
            resolved_order_id = str(
                status.get("order_id") or resolved_order_id or ""
            ).strip()
            if bool(status.get("terminal")):
                if not _status_has_fill(status):
                    return PendingEntryCancelResult(
                        PendingEntryCancelDisposition.AMBIGUOUS,
                        "exact entry is terminal with zero fill while a live position exists",
                        status,
                        read_only=not allow_write,
                    )
                return PendingEntryCancelResult(
                    PendingEntryCancelDisposition.FILLED,
                    f"entry already terminal with final fill state={status.get('state')}",
                    status,
                    read_only=not allow_write,
                )
        except Exception as exc:
            read_error = f"{type(exc).__name__}: {exc}"

    if not resolved_order_id and _entry_identity_values(entry_payload):
        resolved_order_id, resolve_reason = (
            await _resolve_exact_entry_order_id_from_open_orders(
                adapter,
                symbol=symbol,
                entry_payload=entry_payload,
            )
        )
        if not resolved_order_id:
            reason = resolve_reason
            if read_error:
                reason = f"entry status ambiguous after {read_error}; {reason}"
            return PendingEntryCancelResult(
                PendingEntryCancelDisposition.AMBIGUOUS,
                reason,
                status,
                read_only=not allow_write,
            )

    if not resolved_order_id:
        return PendingEntryCancelResult(
            PendingEntryCancelDisposition.IDENTITY_MISSING,
            "partial LIMIT has no exact order identity; broad cancellation is disabled",
            status,
            read_only=not allow_write,
        )

    exact_payload = dict(entry_payload)
    exact_payload["orderId"] = resolved_order_id

    reservation_record: dict[str, Any] | None = None
    reservation_note = ""
    if allow_write and execution_id not in (None, 0):
        try:
            reservation_outcome = await db.reserve_limit_cancel_write(
                int(execution_id),
                policy_reason=str(policy_reason or "partial_remainder"),
                expected_status="pending_limit",
                max_writes=_LIMIT_CANCEL_MAX_WRITES,
                backoff_seconds=_LIMIT_CANCEL_BACKOFF_SECONDS,
            )
        except Exception as exc:
            reservation_outcome = {
                "reserved": False,
                "reason": f"reservation_error:{type(exc).__name__}",
                "record": {},
            }
            reservation_note = (
                " Durable-reservation for exact remainder cancel failed; "
                f"no exchange write sent ({type(exc).__name__}: {exc})."
            )
        raw_record = reservation_outcome.get("record")
        if isinstance(raw_record, dict):
            reservation_record = dict(raw_record)
        if not bool(reservation_outcome.get("reserved")):
            reserve_reason = str(
                reservation_outcome.get("reason") or "reservation_denied"
            )
            if not reservation_note:
                reservation_note = (
                    " Exact remainder cancel write suppressed by durable reservation "
                    f"({reserve_reason})."
                )
            allow_write = False
            if reserve_reason == "exhausted":
                write_suppressed_exhausted = True

    cancel_error = ""
    cancel_error_code: int | None = None
    cancel_error_message = ""
    cancel_retryable: bool | None = None
    cancel_response_audit: dict[str, Any] | None = None
    write_attempted = False
    if allow_write:
        write_attempted = True
        try:
            response = await adapter.cancel_entry_order(exact_payload)
            exact_result = (
                response.get("_exact_cancel_result")
                if isinstance(response, dict)
                and isinstance(response.get("_exact_cancel_result"), dict)
                else {}
            )
            raw_audit = exact_result.get("response_audit")
            if isinstance(raw_audit, dict):
                cancel_response_audit = dict(raw_audit)
            cancel_error_message = str(
                exact_result.get("error_message") or ""
            ).strip()
        except Exception as exc:
            cancel_error = f"{type(exc).__name__}: {exc}"
            raw_code = getattr(exc, "error_code", None)
            try:
                cancel_error_code = (
                    int(raw_code)
                    if raw_code not in (None, "") and not isinstance(raw_code, bool)
                    else None
                )
            except (TypeError, ValueError, OverflowError):
                cancel_error_code = None
            cancel_error_message = str(
                getattr(exc, "error_message", "") or str(exc)
            ).strip()
            raw_retryable = getattr(exc, "retryable", None)
            cancel_retryable = (
                bool(raw_retryable) if isinstance(raw_retryable, bool) else None
            )
            raw_audit = getattr(exc, "response_audit", None)
            if isinstance(raw_audit, dict):
                cancel_response_audit = dict(raw_audit)

    write_meta = {
        "write_attempted": write_attempted,
        "cancel_error_code": cancel_error_code,
        "cancel_error_message": cancel_error_message,
        "cancel_retryable": cancel_retryable,
        "cancel_response_audit": cancel_response_audit,
        "write_reservation": reservation_record,
        "read_only": not allow_write,
    }

    last_reason = (
        f"exact remainder cancel response ambiguous: {cancel_error}"
        if cancel_error
        else (
            reservation_note.strip()
            if reservation_note
            else "exact remainder cancel not yet terminal"
        )
    )
    if allow_write:
        delays = (0.0, 0.25, 0.5, 1.0, 1.5, 2.0)
    else:
        delays = (0.0, 0.5, 1.5, 3.0)
    delays = delays[: max(2, min(int(attempts or 2), len(delays)))]
    for delay in delays:
        if delay:
            await asyncio.sleep(delay)
        try:
            status = await adapter.fetch_entry_order_fill_status(
                symbol=symbol, order_response=exact_payload
            )
        except Exception as exc:
            last_reason = (
                f"remainder confirmation read failed: {type(exc).__name__}: {exc}"
            )
            continue
        if bool(status.get("terminal")):
            if not _status_has_fill(status):
                return PendingEntryCancelResult(
                    PendingEntryCancelDisposition.AMBIGUOUS,
                    "exact entry became terminal with zero fill while a live position exists",
                    status,
                    **write_meta,
                )
            return PendingEntryCancelResult(
                PendingEntryCancelDisposition.FILLED,
                f"exact entry remainder terminal (state={status.get('state')}, filled_qty={_f(status.get('filled_qty'), 0.0):.12g})",
                status,
                **write_meta,
            )
        last_reason = f"entry remainder state {status.get('state')} is not terminal yet"

    if reservation_note:
        last_reason = f"{last_reason}. {reservation_note.strip()}"
    if not allow_write:
        disposition = (
            PendingEntryCancelDisposition.RETRY_EXHAUSTED
            if write_suppressed_exhausted
            else PendingEntryCancelDisposition.RETRY_DEFERRED
        )
    else:
        disposition = (
            PendingEntryCancelDisposition.CANCEL_REJECTED
            if cancel_error_code is not None
            else PendingEntryCancelDisposition.AMBIGUOUS
        )
    return PendingEntryCancelResult(
        disposition,
        last_reason,
        status,
        **write_meta,
    )


async def _finalize_terminal_no_fill_result(
    *,
    result: PendingEntryCancelResult,
    execution_id: int,
    user_id: int,
    symbol: str,
    side: str,
    notify: NotifyFn | None,
    policy_reason: str = "",
    prior_cancel_pending: dict[str, Any] | None = None,
    expected_status: str | None = "pending_limit",
) -> bool:
    status_payload = (
        result.order_status if isinstance(result.order_status, dict) else {}
    )
    status, reason, title = _terminal_no_fill_classification(
        status_payload,
        policy_reason=policy_reason,
    )
    compact = {
        key: status_payload.get(key)
        for key in (
            "order_id",
            "state",
            "terminal",
            "fully_filled",
            "filled_qty",
            "requested_qty",
            "avg_fill_price",
            "position_id",
            "external_oid",
            "qty_step",
        )
    }
    prior_pending = (
        prior_cancel_pending if isinstance(prior_cancel_pending, dict) else {}
    )
    cancel_response_audit = (
        result.cancel_response_audit
        if isinstance(result.cancel_response_audit, dict)
        else prior_pending.get("cancel_response_v1")
    )
    confirmation_audit = (
        result.confirmation_audit
        if isinstance(result.confirmation_audit, dict)
        else prior_pending.get("confirmation_v1")
    )
    ok = await db.update_execution_status_merge(
        execution_id,
        status,
        reason,
        {
            "limit_fill_status": compact,
            "limit_cancel_pending": None,
            "limit_cancel_confirmed": {
                "reason": str(policy_reason or "").strip().lower(),
                "confirmed_at": datetime.now(timezone.utc).isoformat(),
                "cancel_response_v1": cancel_response_audit,
                "confirmation_v1": confirmation_audit,
            },
        },
        expected_status=expected_status,
        write_flow_audit_stage="limit_tp_catchup_terminal_no_fill",
        write_flow_audit_status=status,
    )
    if not ok:
        raise StaleExecutionPass(
            source="limit_tp_catchup.finalize_terminal_no_fill",
            execution_id=execution_id,
            expected_status=str(expected_status or ""),
            attempted_status=status,
        )
    await _notify(
        notify,
        user_id,
        f"{title}\nСделка: {symbol} {side.upper()}\n"
        f"ID исполнения: {execution_id}\n"
        f"Состояние BingX: {status_payload.get('state')}",
        event_key=f"execution:{execution_id}",
    )
    return True


async def _notify(
    notify: NotifyFn | None,
    user_id: int,
    text: str,
    *,
    event_key: str | None = None,
    reply_markup_spec: dict[str, Any] | None = None,
) -> bool:
    return await send_or_enqueue(
        notify,
        user_id,
        text,
        source="limit_tp_catchup",
        event_key=event_key,
        reply_markup_spec=reply_markup_spec,
    )


def _limit_cancel_recheck_markup_spec(
    *, execution_id: int, user_id: int
) -> dict[str, Any]:
    callback_data = f"limitactive:recheck:{int(execution_id)}|u{int(user_id)}"
    if len(callback_data.encode("utf-8")) > 64:
        return {}
    return {
        "inline_keyboard": [
            [
                {
                    "text": "🔄 Безопасно перепроверить",
                    "callback_data": callback_data,
                }
            ]
        ]
    }


def _limit_cancel_warning_text(
    *,
    symbol: str,
    side: str,
    result: PendingEntryCancelResult,
    pending: dict[str, Any],
) -> str:
    attempts = int(_f(pending.get("write_attempts"), 0.0))
    maximum = int(_f(pending.get("max_write_attempts"), _LIMIT_CANCEL_MAX_WRITES))
    exhausted = bool(pending.get("exhausted"))
    confirmation = (
        result.confirmation_audit
        if isinstance(result.confirmation_audit, dict)
        else (
            pending.get("confirmation_v1")
            if isinstance(pending.get("confirmation_v1"), dict)
            else {}
        )
    )
    response_audit = (
        result.cancel_response_audit
        if isinstance(result.cancel_response_audit, dict)
        else (
            pending.get("cancel_response_v1")
            if isinstance(pending.get("cancel_response_v1"), dict)
            else {}
        )
    )
    exact_order_id = clean_exchange_id(
        confirmation.get("order_id") or response_audit.get("order_id")
    )
    order_line = f"Точный orderId: {exact_order_id}\n" if exact_order_id else ""
    mode_line = (
        "Автоматические cancel-запросы остановлены. Бот продолжит только read-only "
        "проверку; кнопка ниже также ничего не отменяет повторно."
        if exhausted
        else "Бот повторит точную отмену только после безопасной паузы."
    )
    return (
        "⚠️ УСЛОВИЕ УДАЛЕНИЯ LIMIT ВЫПОЛНЕНО, НО ОТМЕНА НЕ ПОДТВЕРЖДЕНА\n"
        f"Сделка: {symbol} {side.upper()}\n\n"
        f"Причина: {result.reason}\n"
        f"{order_line}"
        f"Попыток отмены: {attempts}/{maximum}\n"
        f"{mode_line}"
    )


def _mark_market_catchup_summary_notified(
    ids_payload: dict[str, Any],
    actions: list[dict[str, Any]],
    *,
    notified_at: str | None = None,
) -> bool:
    """Delegate market catch-up TP notices to the durable LIMIT summary.

    A catch-up slice is already described in the final LIMIT-filled card. After
    that card has either been delivered or durably queued, recording a second
    dedicated TP card would be a duplicate. This pure helper marks only those
    market-closed slices; regular TP orders remain independently notifyable.
    """

    rows = ids_payload.get("tp")
    if not isinstance(rows, list):
        return False
    delegated_indices: set[int] = set()
    for item in actions:
        if not isinstance(item, dict):
            continue
        if str(item.get("type") or "") != "tp_catchup_market_close":
            continue
        index = int(_f(item.get("tp_index"), 0.0))
        if index > 0:
            delegated_indices.add(index)
    if not delegated_indices:
        return False

    now_iso = notified_at or datetime.now(timezone.utc).isoformat()
    changed = False
    for fallback_index, row_item in enumerate(rows, 1):
        if not isinstance(row_item, dict):
            continue
        index = int(_f(row_item.get("tp_index"), float(fallback_index)))
        if index not in delegated_indices:
            continue
        if row_item.get("filled_notified") is not True:
            row_item["filled_notified"] = True
            row_item["notified_at"] = now_iso
            row_item["notification_pending"] = False
            row_item["next_notify_retry_at"] = None
            row_item["notification_source"] = "limit_catchup_durable_summary"
            changed = True
    return changed


def _fmt_actions(actions: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    for a in actions:
        typ = a.get("type")
        if typ == "tp_created_after_fill":
            lines.append(
                f"🎯 TP{a.get('tp_index')} выставлен: {a.get('tp')} | qty {a.get('qty')}"
            )
        elif typ == "tp_catchup_market_close":
            lines.append(
                f"⚡ TP{a.get('tp_index')} уже пройден - закрыта доля market reduceOnly: qty {a.get('qty')}"
            )
        elif typ == "stop_catchup_market_close":
            lines.append(
                f"🚨 STOP уже пробит - позиция закрыта market reduceOnly: qty {a.get('qty')}"
            )
        elif typ == "closed_on_exchange":
            lines.append("ℹ️ Позиция уже закрыта на бирже до установки TP")
        elif typ == "no_post_fill_action":
            lines.append(
                "ℹ️ Позиция открыта, дополнительных TP-действий не потребовалось"
            )
        elif typ == "partial_error":
            lines.append(f"⚠️ Ошибка после частичных действий: {a.get('error')}")
        elif typ == "transient_error":
            lines.append(
                f"⏳ Временная ошибка биржи, бот повторит позже: {a.get('error')}"
            )
        elif typ == "transient_give_up":
            lines.append(
                f"🛑 Лимит повторов временной ошибки исчерпан, нужна ручная проверка: {a.get('error')}"
            )
        elif typ == "tp_write_ambiguous":
            lines.append(
                f"⚠️ TP{a.get('tp_index')} под вопросом: временная ошибка во время создания TP. Проверь ордер вручную."
            )
        elif typ == "tp_market_close_ambiguous":
            lines.append(
                f"⚠️ TP{a.get('tp_index')} market-close под вопросом: временная ошибка. Проверь позицию вручную."
            )
    return "\n".join(lines) if lines else "действий нет"


async def process_pending_limit_tp_catchup_once(
    notify: NotifyFn | None = None,
    *,
    rows_override: list[dict[str, Any]] | None = None,
    market_prices: dict[str, float] | None = None,
    shared_adapter_cache: dict[tuple[int, str], Any] | None = None,
    market_event_exchange_context: Any | None = None,
) -> int:
    """Post-fill protection for LIMIT entries.

    LIMIT orders are submitted with STOP only. Once a real position exists, this
    worker places planned TP orders. If price already crossed a planned TP before
    TP placement, it closes only that planned fraction with reduceOnly MARKET.

    v1.0.12 safety changes:
    - cap catch-up quantity by the bot logged qty, so manual positions on the
      same symbol+side are not accidentally included;
    - use short in-pass position/price cache to reduce API calls;
    - persist partial post-fill actions before and after each exchange action;
    - on partial TP failure, keep a full action trail in DB and notify user;
    - explicit no-action status text instead of a confusing empty protection log.
    """
    use_global_lock = rows_override is None
    if use_global_lock and _LOOP_LOCK.locked():
        record_stage_rows(selected=0, scanned=0, lock_skipped=1, source="global_lock")
        return 0
    async with _LOOP_LOCK if use_global_lock else null_async_context():
        global _SCAN_CURSOR
        if rows_override is not None:
            rows = list(rows_override)
        else:
            rows = await db.pending_limit_executions(limit=50, after_id=_SCAN_CURSOR)
            if not rows and _SCAN_CURSOR:
                _SCAN_CURSOR = 0
                rows = await db.pending_limit_executions(limit=50, after_id=0)
            if rows:
                _SCAN_CURSOR = max(int(row.get("id") or 0) for row in rows)
        record_stage_rows(
            selected=len(rows),
            scanned=len(rows),
            source="override" if rows_override is not None else "database",
        )
        if not rows:
            return 0
        processed = 0
        owns_adapter_cache = shared_adapter_cache is None
        adapter_cache: dict[tuple[int, str], Any] = (
            {} if shared_adapter_cache is None else shared_adapter_cache
        )
        try:
            for row in rows:
                execution_id = int(row.get("id"))
                if not execution_id:
                    continue
                set_notification_event_key(f"execution:{execution_id}")
                async with db.execution_lock(execution_id) as lock_acquired:
                    if lock_acquired is False:
                        log.warning(
                            "LIMIT_TP_LOCK_DEFERRED execution_id=%s stage=%s",
                            execution_id, db.monitor_workload_stage(),
                        )
                        continue
                    latest = await db.get_execution_by_id(execution_id)
                    if latest is not None:
                        row = latest
                    if str(row.get("status") or "") != "pending_limit":
                        continue

                        # v1.6.18: track this iteration's last-known-good status so
                        # every write below (via persist(), persist_transient() or
                        # a direct call) refuses if a concurrent worker (or an
                        # old/new process briefly overlapping a Railway redeploy)
                        # already moved this row to a different status.
                    _known_status = "pending_limit"

                    user_id = int(row.get("user_id"))
                    symbol = str(row.get("symbol") or "").upper()
                    side = str(row.get("side") or "").lower()
                    stop = _signed_f(row.get("stop"), 0.0)
                    intended_qty = _signed_f(row.get("qty"), 0.0)
                    created_at_raw = str(row.get("created_at") or "")
                    reason_prefix = f"pending_limit#{execution_id} {symbol} {side}"
                    ids_payload: dict[str, Any] = {}
                    actions: list[dict[str, Any]] = []

                    async def _write_status(new_status, reason, patch=None):
                        nonlocal _known_status
                        ok = await db.update_execution_status_merge(
                            execution_id,
                            new_status,
                            reason,
                            patch,
                            expected_status=_known_status,
                            write_flow_audit_stage="limit_tp_catchup",
                            write_flow_audit_status=new_status,
                        )
                        if ok:
                            _known_status = new_status
                            if new_status in {
                                "canceled_expired",
                                "canceled_tp_progress",
                                "canceled_stop_invalidated",
                                "canceled_external",
                            }:
                                cleanup = await db.finish_stale_market_events_if_group_inactive(
                                    execution_id
                                )
                                if int(cleanup.get("finished") or 0) > 0:
                                    log.info(
                                        "G69_MARKET_EVENT_EAGER_TERMINAL execution_id=%s "
                                        "group_id=%s status=%s finished=%s reason=no_active_executions",
                                        execution_id,
                                        int(cleanup.get("trade_group_id") or 0),
                                        new_status,
                                        int(cleanup.get("finished") or 0),
                                    )
                        else:
                            log.info(
                                "limit_tp_catchup: abort stale execution pass execution_id=%s "
                                "attempted=%s expected=%s",
                                execution_id,
                                new_status,
                                _known_status,
                            )
                            raise StaleExecutionPass(
                                source="limit_tp_catchup",
                                execution_id=execution_id,
                                expected_status=_known_status,
                                attempted_status=new_status,
                            )
                        return True

                    async def persist(status: str, reason: str) -> None:
                        ids_payload["post_fill"] = actions
                        _, ledger_changed, repaired_indices = canonicalize_tp_ledger(
                            ids_payload
                        )
                        if ledger_changed:
                            ids_payload["tp_ledger_v1"] = tp_ledger_repair_metadata(
                                repaired_indices, source="limit_tp_catchup.persist"
                            )
                        await _write_status(
                            status,
                            reason,
                            ids_payload,
                        )

                    async def mark_catchup_summary_as_tp_notification() -> None:
                        """Persist summary ownership for market catch-up TP slices."""

                        if _mark_market_catchup_summary_notified(ids_payload, actions):
                            await db.merge_execution_metadata(
                                execution_id,
                                {"tp": ids_payload.get("tp") or []},
                                write_flow_audit_stage="limit_tp_catchup_summary_tp_notification",
                                write_flow_audit_status=_known_status,
                            )

                    async def persist_transient(
                        area: str, exc: Exception, *, has_side_effects: bool = False
                    ) -> int:
                        attempts = record_transient_error(ids_payload, area, exc)
                        exhausted = transient_retry_exhausted(
                            attempts,
                            max_retries=get_settings().EXCHANGE_TRANSIENT_ERROR_MAX_RETRIES,
                        )
                        # If TP/close actions have already happened, keep the row in partial_error
                        # so partial_tp_recovery can continue without duplicating earlier TP orders.
                        # If retry cap is exhausted before any side effect, stop endless API polling and
                        # move the execution to manual_required. It still counts as active risk.
                        if exhausted and not has_side_effects:
                            status = "manual_required"
                        else:
                            status = (
                                "partial_error" if has_side_effects else "pending_limit"
                            )
                        typ = "transient_give_up" if exhausted else "transient_error"
                        actions.append(
                            {
                                "type": typ,
                                "area": area,
                                "error": f"{type(exc).__name__}: {exc}",
                                "attempt": attempts,
                            }
                        )
                        ids_payload["post_fill"] = actions
                        _, ledger_changed, repaired_indices = canonicalize_tp_ledger(
                            ids_payload
                        )
                        if ledger_changed:
                            ids_payload["tp_ledger_v1"] = tp_ledger_repair_metadata(
                                repaired_indices, source="limit_tp_catchup.transient"
                            )
                        reason = (
                            f"Retry cap exhausted in LIMIT catch-up ({area}), manual check required after {attempts} attempts: {type(exc).__name__}: {exc}"
                            if exhausted
                            else f"Retryable exchange error in LIMIT catch-up ({area}), attempt {attempts}: {type(exc).__name__}: {exc}"
                        )
                        await _write_status(status, reason, ids_payload)
                        return attempts

                    try:
                        invalid_fields: list[str] = []
                        if not user_id:
                            invalid_fields.append("user_id")
                        if not symbol:
                            invalid_fields.append("symbol")
                        if side not in {"long", "short"}:
                            invalid_fields.append("side")
                        if stop <= 0:
                            invalid_fields.append("stop")
                        if intended_qty <= 0:
                            invalid_fields.append("qty")
                        if invalid_fields:
                            await _write_status(
                                "manual_required",
                                "pending_limit row has invalid execution fields: "
                                + ", ".join(invalid_fields)
                                + "; live order state requires manual check",
                                {
                                    "limit_integrity": {
                                        "reason": "invalid_execution_fields",
                                        "invalid_fields": invalid_fields,
                                    }
                                },
                            )
                            continue
                        targets = [
                            _signed_f(x, 0.0)
                            for x in json.loads(row.get("targets_json") or "[]")
                        ]
                        pcts = [
                            _signed_f(x, 0.0)
                            for x in json.loads(row.get("tp_distribution_json") or "[]")
                        ]
                        if (
                            not targets
                            or len(targets) != len(pcts)
                            or any(value <= 0 for value in targets)
                            or any(value <= 0 for value in pcts)
                            or abs(sum(pcts) - 100.0) > 0.001
                        ):
                            await _write_status(
                                "manual_required",
                                "pending_limit row has an invalid TP plan; live order state requires manual check",
                                {
                                    "limit_integrity": {
                                        "reason": "invalid_tp_plan",
                                        "target_count": len(targets),
                                        "distribution_count": len(pcts),
                                    }
                                },
                            )
                            continue
                        ids_payload = json.loads(
                            row.get("exchange_order_ids_json") or "{}"
                        )
                        # Resume the durable action journal instead of starting with an
                        # empty list on every polling cycle/redeploy. Starting from []
                        # was the direct cause of the same TP1..TPN set being submitted
                        # repeatedly after a LIMIT fill.
                        saved_post_fill = ids_payload.get("post_fill")
                        if isinstance(saved_post_fill, list):
                            actions = [
                                dict(item)
                                for item in saved_post_fill
                                if isinstance(item, dict)
                            ]
                        else:
                            actions = []
                        user_settings = await get_user_settings_cache().get_or_fetch(
                            (user_id, "settings"),
                            lambda: db.get_user_settings(user_id),
                        )
                        exchange = str(
                            ids_payload.get("exchange")
                            or user_settings.exchange
                            or get_settings().safe_default_exchange
                        ).lower()
                        api_row = await get_api_key_cache().get_or_fetch(
                            (user_id, "api", exchange),
                            lambda: db.get_api_key(user_id, exchange),
                        )
                        if not api_row:
                            msg = f"нет API для {exchange_title(exchange)}"
                            await _write_status("manual_required", msg)
                            await _notify(
                                notify,
                                user_id,
                                f"❌ LIMIT catch-up не выполнен по {symbol}\nПричина: {msg}",
                            )
                            continue

                            # Build adapter once and reuse across all invalidation checks.
                        cache_key = (user_id, exchange)
                        adapter = adapter_cache.get(cache_key)
                        if adapter is None:
                            adapter = build_adapter(api_row)
                            if market_event_exchange_context is not None:
                                adapter = market_event_exchange_context.wrap_adapter(
                                    adapter, cache_key
                                )
                            adapter_cache[cache_key] = adapter

                            # A LIMIT may fill between monitor cycles. Whole-order stale
                            # cancellation is blocked once a live position exists; G65 may
                            # still terminalize only the exact unfilled remainder later.
                        _pre_position_read_ok = True
                        try:
                            _pre_invalidation_positions = (
                                await adapter.fetch_open_positions(symbol, side.upper())
                            )
                        except Exception as _pre_pos_exc:
                            # A failed position read is not proof that no position
                            # exists.  Fail closed: skip stale cancellation and let
                            # the normal reconciliation retry the private endpoint.
                            _pre_position_read_ok = False
                            _pre_invalidation_positions = []
                            log.warning(
                                "pre-invalidation position read failed for %s: %s",
                                symbol,
                                _pre_pos_exc,
                            )

                        _has_live_position_pre = bool(_pre_invalidation_positions)
                        _skip_limit_invalidation = (
                            _has_live_position_pre or not _pre_position_read_ok
                        )
                        _entry_order_payload = (
                            ids_payload.get("entry")
                            if isinstance(ids_payload.get("entry"), dict)
                            else {}
                        )
                        _pre_entry_status: dict[str, Any] | None = None
                        _partial_remainder_candidate = False
                        # G65: order detail is authoritative even when a partial fill
                        # already created a live position. The old gate skipped this
                        # read whenever a position existed, which meant TTL/TP policy
                        # could never terminalize the still-open remainder.
                        if _entry_order_payload:
                            try:
                                _pre_entry_status = (
                                    await adapter.fetch_entry_order_fill_status(
                                        symbol=symbol,
                                        order_response=_entry_order_payload,
                                    )
                                )
                                ids_payload["limit_fill_status"] = {
                                    key: _pre_entry_status.get(key)
                                    for key in (
                                        "order_id",
                                        "state",
                                        "terminal",
                                        "fully_filled",
                                        "filled_qty",
                                        "requested_qty",
                                        "avg_fill_price",
                                        "position_id",
                                        "external_oid",
                                        "qty_step",
                                    )
                                }
                                if _status_has_fill(_pre_entry_status):
                                    _skip_limit_invalidation = True
                                    _pre_filled_qty = _f(
                                        _pre_entry_status.get("filled_qty"), 0.0
                                    )
                                    _pre_requested_qty = _f(
                                        _pre_entry_status.get("requested_qty"), 0.0
                                    )
                                    _partial_remainder_candidate = bool(
                                        _has_live_position_pre
                                        and not bool(_pre_entry_status.get("terminal"))
                                        and not bool(
                                            _pre_entry_status.get("fully_filled")
                                        )
                                        and _pre_filled_qty > 0
                                        and (
                                            _pre_requested_qty <= 0
                                            or _pre_filled_qty + 1e-10
                                            < _pre_requested_qty
                                        )
                                    )
                                    _fill_patch = {
                                        "limit_fill_status": ids_payload[
                                            "limit_fill_status"
                                        ]
                                    }
                                    # Preserve durable cancel backoff while a partial
                                    # remainder is still live. Clear it only after the
                                    # exact entry is already terminal.
                                    if bool(_pre_entry_status.get("terminal")):
                                        _fill_patch["limit_cancel_pending"] = None
                                    await _write_status(
                                        "pending_limit",
                                        (
                                            "Partial LIMIT fill detected; evaluating exact remainder policy"
                                            if _partial_remainder_candidate
                                            else "Entry fill detected before stale-policy evaluation; continuing post-fill reconciliation"
                                        ),
                                        _fill_patch,
                                    )
                                elif bool(_pre_entry_status.get("terminal")):
                                    (
                                        _terminal_status,
                                        _terminal_reason,
                                        _terminal_title,
                                    ) = _terminal_no_fill_classification(
                                        _pre_entry_status,
                                        policy_reason=_pending_cancel_reason(
                                            ids_payload
                                        ),
                                    )
                                    await _write_status(
                                        _terminal_status,
                                        _terminal_reason,
                                        {
                                            "limit_fill_status": ids_payload[
                                                "limit_fill_status"
                                            ],
                                            "limit_cancel_pending": None,
                                        },
                                    )
                                    await _notify(
                                        notify,
                                        user_id,
                                        f"{_terminal_title}\n"
                                        f"Сделка: {symbol} {side.upper()}\n"
                                        f"ID исполнения: {execution_id}\n"
                                        f"Состояние BingX: {_pre_entry_status.get('state')}",
                                    )
                                    processed += 1
                                    continue
                            except Exception as _pre_order_exc:
                                # Keep the row active.  Exact cancellation may still
                                # be possible by a saved orderId; an externalOid-only
                                # row remains ambiguous and is never broadly cancelled.
                                log.warning(
                                    "pre-invalidation entry read failed for %s: %s",
                                    symbol,
                                    _pre_order_exc,
                                )

                                # Immutable per-user LIMIT policy. Legacy rows without a
                                # snapshot use the project default 24h + final-TP behavior.
                        _limit_policy = read_limit_policy(
                            ids_payload,
                            fallback_ttl=int(
                                getattr(get_settings(), "LIMIT_ORDER_TTL_HOURS", 24)
                                or 0
                            ),
                            fallback_mode="last",
                            targets=targets,
                        )
                        raw_limit_policy = ids_payload.get(LIMIT_POLICY_KEY)
                        if isinstance(raw_limit_policy, dict):
                            try:
                                raw_threshold = int(
                                    raw_limit_policy.get("tp_threshold_index") or 0
                                )
                            except (TypeError, ValueError):
                                raw_threshold = 0
                            repaired_threshold = int(
                                _limit_policy.get("tp_threshold_index") or 0
                            )
                            if raw_threshold != repaired_threshold:
                                # Durable repair for the v1.6.11 TP2→TP1 snapshot
                                # defect.  Runtime behavior is already safe through
                                # read_policy(); persisting the repair prevents the old
                                # value from resurfacing after future upgrades.
                                ids_payload[LIMIT_POLICY_KEY] = _limit_policy
                                await _write_status(
                                    "pending_limit",
                                    str(row.get("reason") or ""),
                                    {LIMIT_POLICY_KEY: _limit_policy},
                                )
                        _ttl_hours = int(_limit_policy.get("ttl_hours") or 0)
                        _tp_threshold_index = int(
                            _limit_policy.get("tp_threshold_index") or 0
                        )
                        _tp_mode = str(_limit_policy.get("tp_mode") or "last")

                        # G65: a live position does NOT make an unfinished entry
                        # remainder valid forever. The already-filled quantity is
                        # preserved, while only the exact unfilled remainder may be
                        # cancelled once the immutable LIMIT policy says the stale
                        # entry is no longer allowed (TTL or configured TP progress).
                        if _partial_remainder_candidate:
                            _partial_due_reason = ""
                            _partial_due_details: dict[str, Any] = {}

                            if _ttl_hours > 0 and created_at_raw:
                                try:
                                    _created_partial = datetime.fromisoformat(
                                        created_at_raw.replace("Z", "+00:00")
                                    )
                                    if _created_partial.tzinfo is None:
                                        _created_partial = _created_partial.replace(
                                            tzinfo=timezone.utc
                                        )
                                    _partial_age = (
                                        datetime.now(timezone.utc) - _created_partial
                                    )
                                    if _partial_age > timedelta(hours=_ttl_hours):
                                        _partial_due_reason = "ttl"
                                        _partial_due_details.update(
                                            {
                                                "ttl_hours": _ttl_hours,
                                                "age_hours": int(
                                                    _partial_age.total_seconds() / 3600
                                                ),
                                            }
                                        )
                                except (TypeError, ValueError, OverflowError):
                                    pass

                            # Record TP touch evidence even while an entry remainder
                            # is still open. Older builds skipped this whenever a live
                            # position existed, losing the exact policy trigger.
                            if targets:
                                try:
                                    _partial_price = float(
                                        (market_prices or {}).get(symbol)
                                        or await adapter.fetch_last_price(symbol)
                                    )
                                    (
                                        _partial_runtime,
                                        _partial_touch_changed,
                                        _partial_max_passed,
                                    ) = record_tp_touch(
                                        ids_payload.get(LIMIT_POLICY_RUNTIME_KEY),
                                        side=side,
                                        current_price=_partial_price,
                                        targets=targets,
                                    )
                                    if _partial_touch_changed:
                                        ids_payload[LIMIT_POLICY_RUNTIME_KEY] = (
                                            _partial_runtime
                                        )
                                        await _persist_limit_policy_runtime(
                                            execution_id, _partial_runtime
                                        )
                                    if (
                                        _tp_threshold_index > 0
                                        and _partial_max_passed >= _tp_threshold_index
                                    ):
                                        _partial_due_reason = "tp_progress"
                                        _partial_due_details.update(
                                            {
                                                "tp_mode": _tp_mode,
                                                "tp_threshold_index": _tp_threshold_index,
                                                "max_tp_passed": _partial_max_passed,
                                                "observed_price": _partial_price,
                                            }
                                        )
                                except Exception as _partial_tp_exc:
                                    log.warning(
                                        "G65_PARTIAL_LIMIT_REMAINDER phase=tp_evidence_failed "
                                        "execution_id=%s user_id=%s symbol=%s error_type=%s",
                                        execution_id,
                                        user_id,
                                        symbol,
                                        type(_partial_tp_exc).__name__,
                                    )

                            if _partial_due_reason:
                                _partial_cancel_control = _limit_cancel_control(
                                    ids_payload, policy_reason=_partial_due_reason
                                )
                                async with db.symbol_action_lock(user_id, symbol):
                                    _partial_cancel_result = (
                                        await _cancel_opening_order_remainder_confirmed(
                                            adapter,
                                            symbol=symbol,
                                            side=side,
                                            entry_order=_entry_order_payload,
                                            attempts=(
                                                6
                                                if _partial_cancel_control["allow_write"]
                                                else 2
                                            ),
                                            execution_id=execution_id,
                                            policy_reason=_partial_due_reason,
                                            allow_write=bool(
                                                _partial_cancel_control["allow_write"]
                                            ),
                                            write_suppressed_exhausted=bool(
                                                _partial_cancel_control["exhausted"]
                                            ),
                                        )
                                    )

                                if (
                                    _partial_cancel_result.disposition
                                    == PendingEntryCancelDisposition.FILLED
                                    and isinstance(
                                        _partial_cancel_result.order_status, dict
                                    )
                                    and bool(
                                        _partial_cancel_result.order_status.get(
                                            "terminal"
                                        )
                                    )
                                ):
                                    _terminal_partial_status = (
                                        _partial_cancel_result.order_status
                                    )
                                    ids_payload["limit_fill_status"] = {
                                        key: _terminal_partial_status.get(key)
                                        for key in (
                                            "order_id",
                                            "state",
                                            "terminal",
                                            "fully_filled",
                                            "filled_qty",
                                            "requested_qty",
                                            "avg_fill_price",
                                            "position_id",
                                            "external_oid",
                                            "qty_step",
                                        )
                                    }
                                    _partial_terminal_patch = {
                                        "limit_fill_status": ids_payload[
                                            "limit_fill_status"
                                        ],
                                        "limit_cancel_pending": None,
                                        "partial_limit_remainder_terminal_v1": {
                                            "policy_reason": _partial_due_reason,
                                            "filled_qty": _f(
                                                _terminal_partial_status.get(
                                                    "filled_qty"
                                                ),
                                                0.0,
                                            ),
                                            "requested_qty": _f(
                                                _terminal_partial_status.get(
                                                    "requested_qty"
                                                ),
                                                0.0,
                                            ),
                                            "terminal_state": _terminal_partial_status.get(
                                                "state"
                                            ),
                                            "confirmed_at": datetime.now(
                                                timezone.utc
                                            ).isoformat(),
                                            **_partial_due_details,
                                        },
                                    }
                                    await _write_status(
                                        "pending_limit",
                                        "Partial LIMIT remainder terminalized exactly; continuing post-fill protection for confirmed filled quantity",
                                        _partial_terminal_patch,
                                    )
                                    _pre_entry_status = _terminal_partial_status
                                    _partial_remainder_candidate = False
                                    log.info(
                                        "G65_PARTIAL_LIMIT_REMAINDER phase=terminalized "
                                        "execution_id=%s user_id=%s symbol=%s reason=%s "
                                        "filled_qty=%.12g requested_qty=%.12g",
                                        execution_id,
                                        user_id,
                                        symbol,
                                        _partial_due_reason,
                                        _f(
                                            _terminal_partial_status.get("filled_qty"),
                                            0.0,
                                        ),
                                        _f(
                                            _terminal_partial_status.get(
                                                "requested_qty"
                                            ),
                                            0.0,
                                        ),
                                    )
                                else:
                                    _partial_pending = _limit_cancel_pending_record(
                                        previous=_partial_cancel_control["existing"],
                                        policy_reason=_partial_due_reason,
                                        result=_partial_cancel_result,
                                    )
                                    _partial_pending[
                                        "partial_remainder"
                                    ] = True
                                    _partial_pending.update(_partial_due_details)
                                    _partial_exhausted = bool(
                                        _partial_pending.get("exhausted")
                                    )
                                    await _write_status(
                                        (
                                            "manual_required"
                                            if _partial_exhausted
                                            else "pending_limit"
                                        ),
                                        (
                                            "Partial LIMIT remainder exact cancellation exhausted; manual check required: "
                                            if _partial_exhausted
                                            else "Partial LIMIT remainder cancellation is not yet confirmed: "
                                        )
                                        + str(_partial_cancel_result.reason),
                                        {
                                            "limit_cancel_pending": _partial_pending,
                                            "limit_fill_status": ids_payload.get(
                                                "limit_fill_status"
                                            ),
                                        },
                                    )
                                    if _partial_exhausted:
                                        await _notify(
                                            notify,
                                            user_id,
                                            f"🚨 LIMIT remainder требует проверки\n"
                                            f"Сделка: {symbol} {side.upper()}\n"
                                            f"ID исполнения: {execution_id}\n"
                                            "Часть LIMIT уже исполнена, но точную отмену оставшегося объёма "
                                            "не удалось подтвердить после ограниченного числа попыток.\n"
                                            "Проверь входной LIMIT и защиту позиции на BingX вручную.",
                                            event_key=f"partial-limit-remainder-manual:{execution_id}",
                                        )
                                    # Do not continue into TP locking while an exact
                                    # entry remainder may still reopen exposure.
                                    continue

                        # ── Инвалидация 1: пользовательский TTL ───────────────────────────
                        if (
                            not _skip_limit_invalidation
                            and _ttl_hours > 0
                            and created_at_raw
                        ):
                            try:
                                _created = datetime.fromisoformat(
                                    created_at_raw.replace("Z", "+00:00")
                                )
                                if _created.tzinfo is None:
                                    _created = _created.replace(tzinfo=timezone.utc)
                                _age = datetime.now(timezone.utc) - _created
                                if _age > timedelta(hours=_ttl_hours):
                                    _age_h = int(_age.total_seconds() / 3600)
                                    _other_execs_ttl = (
                                        await db.other_active_symbol_executions(
                                            user_id, symbol, execution_id, limit=1
                                        )
                                    )
                                    _cancel_control = _limit_cancel_control(
                                        ids_payload, policy_reason="ttl"
                                    )
                                    _cancel_result = (
                                        await _cancel_pending_entry_confirmed(
                                            adapter,
                                            user_id=user_id,
                                            execution_id=execution_id,
                                            symbol=symbol,
                                            side=side,
                                            entry_order=ids_payload.get("entry"),
                                            other_active_execution=bool(
                                                _other_execs_ttl
                                            ),
                                            attempts=(
                                                6
                                                if _cancel_control["allow_write"]
                                                else 2
                                            ),
                                            allow_write=bool(
                                                _cancel_control["allow_write"]
                                            ),
                                            write_suppressed_exhausted=bool(
                                                _cancel_control["exhausted"]
                                            ),
                                            policy_reason="ttl",
                                        )
                                    )
                                    _cancel_reason = _cancel_result.reason
                                    if _cancel_result.filled_or_position:
                                        _skip_limit_invalidation = True
                                        _fill_patch: dict[str, Any] = {
                                            "limit_cancel_pending": None,
                                            "limit_cancel_race": {
                                                "policy_reason": "ttl",
                                                "disposition": _cancel_result.disposition.value,
                                                "cancel_response_v1": (
                                                    _cancel_result.cancel_response_audit
                                                    or _cancel_control["existing"].get(
                                                        "cancel_response_v1"
                                                    )
                                                ),
                                                "confirmation_v1": _cancel_result.confirmation_audit,
                                                "detected_at": datetime.now(
                                                    timezone.utc
                                                ).isoformat(),
                                            },
                                        }
                                        if isinstance(
                                            _cancel_result.order_status, dict
                                        ):
                                            _fill_patch["limit_fill_status"] = {
                                                key: _cancel_result.order_status.get(
                                                    key
                                                )
                                                for key in (
                                                    "order_id",
                                                    "state",
                                                    "terminal",
                                                    "fully_filled",
                                                    "filled_qty",
                                                    "requested_qty",
                                                    "avg_fill_price",
                                                    "position_id",
                                                    "external_oid",
                                                    "qty_step",
                                                )
                                            }
                                        await _write_status(
                                            "pending_limit",
                                            "Fill/position detected during TTL cancellation; continuing post-fill reconciliation",
                                            _fill_patch,
                                        )
                                    elif (
                                        _cancel_result.disposition
                                        == PendingEntryCancelDisposition.TERMINAL_NO_FILL
                                    ):
                                        await _finalize_terminal_no_fill_result(
                                            result=_cancel_result,
                                            execution_id=execution_id,
                                            user_id=user_id,
                                            symbol=symbol,
                                            side=side,
                                            notify=notify,
                                            policy_reason=_pending_cancel_reason(
                                                ids_payload
                                            ),
                                            prior_cancel_pending=_cancel_control[
                                                "existing"
                                            ],
                                            expected_status=_known_status,
                                        )
                                        processed += 1
                                        continue
                                    elif not _cancel_result.canceled:
                                        _cancel_pending = _limit_cancel_pending_record(
                                            previous=_cancel_control["existing"],
                                            policy_reason="ttl",
                                            result=_cancel_result,
                                        )
                                        await _write_status(
                                            "pending_limit",
                                            (
                                                f"LIMIT TTL exceeded, but cancellation is not confirmed: "
                                                f"{_cancel_reason}"
                                            ),
                                            {"limit_cancel_pending": _cancel_pending},
                                        )
                                        if _limit_cancel_should_notify(
                                            _cancel_control["existing"], _cancel_pending
                                        ):
                                            await _notify(
                                                notify,
                                                user_id,
                                                _limit_cancel_warning_text(
                                                    symbol=symbol,
                                                    side=side,
                                                    result=_cancel_result,
                                                    pending=_cancel_pending,
                                                ),
                                                event_key=(
                                                    f"limit-cancel:{execution_id}:"
                                                    f"{_cancel_pending.get('write_attempts')}:"
                                                    f"{int(bool(_cancel_pending.get('exhausted')))}"
                                                ),
                                                reply_markup_spec=_limit_cancel_recheck_markup_spec(
                                                    execution_id=execution_id,
                                                    user_id=user_id,
                                                ),
                                            )
                                        continue
                                    else:
                                        await _write_status(
                                            "canceled_expired",
                                            f"LIMIT ордер устарел: {_age_h}ч > {_ttl_hours}ч TTL. "
                                            f"Отмена подтверждена: входного ордера и позиции нет.",
                                            {
                                                "limit_cancel_pending": None,
                                                "limit_cancel_confirmed": {
                                                    "reason": "ttl",
                                                    "ttl_hours": _ttl_hours,
                                                    "age_hours": _age_h,
                                                    "confirmed_at": datetime.now(
                                                        timezone.utc
                                                    ).isoformat(),
                                                    "cancel_response_v1": (
                                                        _cancel_result.cancel_response_audit
                                                        or _cancel_control[
                                                            "existing"
                                                        ].get("cancel_response_v1")
                                                    ),
                                                    "confirmation_v1": _cancel_result.confirmation_audit,
                                                },
                                            },
                                        )
                                        await _notify(
                                            notify,
                                            user_id,
                                            f"⏰ LIMIT ордер отменён (устарел)\n"
                                            f"Сделка: {symbol} {side.upper()}\n"
                                            f"Ордер висел {_age_h}ч без исполнения (лимит {_ttl_hours}ч).\n"
                                            f"Вход: {row.get('entry')}, Стоп: {stop}\n"
                                            f"Цена входа более не актуальна.",
                                        )
                                        processed += 1
                                        continue
                            except Exception as _ttl_exc:
                                log.debug(
                                    "TTL check failed for %s: %s", symbol, _ttl_exc
                                )

                                # ── Инвалидация 2: стоп пробит (уровень сломан) ──────────────────
                                # Если цена закрылась ниже стопа (LONG) или выше (SHORT) —
                                # уровень входа пробит, смысла в лимитке нет.
                        _price_now = 0.0  # инициализация до try чтобы check3 мог переиспользовать
                        try:
                            _price_now = float(
                                (market_prices or {}).get(symbol)
                                or await adapter.fetch_last_price(symbol)
                            )
                            _stop_breached = _breached_stop(side, _price_now, stop)
                            if _stop_breached and not _skip_limit_invalidation:
                                _other_execs = await db.other_active_symbol_executions(
                                    user_id, symbol, execution_id, limit=1
                                )
                                _cancel_control = _limit_cancel_control(
                                    ids_payload, policy_reason="stop_breached"
                                )
                                _cancel_result = await _cancel_pending_entry_confirmed(
                                    adapter,
                                    user_id=user_id,
                                    execution_id=execution_id,
                                    symbol=symbol,
                                    side=side,
                                    entry_order=ids_payload.get("entry"),
                                    other_active_execution=bool(_other_execs),
                                    attempts=(
                                        6 if _cancel_control["allow_write"] else 2
                                    ),
                                    allow_write=bool(_cancel_control["allow_write"]),
                                    write_suppressed_exhausted=bool(
                                        _cancel_control["exhausted"]
                                    ),
                                    policy_reason="stop_breached",
                                )
                                _cancel_reason = _cancel_result.reason
                                if _cancel_result.filled_or_position:
                                    _skip_limit_invalidation = True
                                    _fill_patch = {
                                        "limit_cancel_pending": None,
                                        "limit_cancel_race": {
                                            "policy_reason": "stop_breached",
                                            "disposition": _cancel_result.disposition.value,
                                            "cancel_response_v1": (
                                                _cancel_result.cancel_response_audit
                                                or _cancel_control["existing"].get(
                                                    "cancel_response_v1"
                                                )
                                            ),
                                            "confirmation_v1": _cancel_result.confirmation_audit,
                                            "detected_at": datetime.now(
                                                timezone.utc
                                            ).isoformat(),
                                        },
                                    }
                                    if isinstance(_cancel_result.order_status, dict):
                                        _fill_patch["limit_fill_status"] = {
                                            key: _cancel_result.order_status.get(key)
                                            for key in (
                                                "order_id",
                                                "state",
                                                "terminal",
                                                "fully_filled",
                                                "filled_qty",
                                                "requested_qty",
                                                "avg_fill_price",
                                                "position_id",
                                                "external_oid",
                                                "qty_step",
                                            )
                                        }
                                    await _write_status(
                                        "pending_limit",
                                        "Fill/position detected during STOP invalidation; continuing post-fill reconciliation",
                                        _fill_patch,
                                    )
                                elif (
                                    _cancel_result.disposition
                                    == PendingEntryCancelDisposition.TERMINAL_NO_FILL
                                ):
                                    await _finalize_terminal_no_fill_result(
                                        result=_cancel_result,
                                        execution_id=execution_id,
                                        user_id=user_id,
                                        symbol=symbol,
                                        side=side,
                                        notify=notify,
                                        policy_reason=_pending_cancel_reason(
                                            ids_payload
                                        ),
                                        prior_cancel_pending=_cancel_control[
                                            "existing"
                                        ],
                                        expected_status=_known_status,
                                    )
                                    processed += 1
                                    continue
                                elif not _cancel_result.canceled:
                                    _cancel_pending = _limit_cancel_pending_record(
                                        previous=_cancel_control["existing"],
                                        policy_reason="stop_breached",
                                        result=_cancel_result,
                                    )
                                    await _write_status(
                                        "pending_limit",
                                        f"STOP level breached, but LIMIT cancellation is not confirmed: {_cancel_reason}",
                                        {"limit_cancel_pending": _cancel_pending},
                                    )
                                    if _limit_cancel_should_notify(
                                        _cancel_control["existing"], _cancel_pending
                                    ):
                                        await _notify(
                                            notify,
                                            user_id,
                                            _limit_cancel_warning_text(
                                                symbol=symbol,
                                                side=side,
                                                result=_cancel_result,
                                                pending=_cancel_pending,
                                            ),
                                            event_key=(
                                                f"limit-cancel:{execution_id}:"
                                                f"{_cancel_pending.get('write_attempts')}:"
                                                f"{int(bool(_cancel_pending.get('exhausted')))}"
                                            ),
                                            reply_markup_spec=_limit_cancel_recheck_markup_spec(
                                                execution_id=execution_id,
                                                user_id=user_id,
                                            ),
                                        )
                                    continue
                                else:
                                    await _write_status(
                                        "canceled_stop_invalidated",
                                        f"LIMIT ордер отменён: цена {_price_now} пробила стоп {stop} "
                                        f"до исполнения; отмена подтверждена, позиции нет.",
                                        {
                                            "limit_cancel_pending": None,
                                            "limit_cancel_confirmed": {
                                                "reason": "stop_breached",
                                                "observed_price": float(_price_now),
                                                "stop": float(stop),
                                                "confirmed_at": datetime.now(
                                                    timezone.utc
                                                ).isoformat(),
                                                "cancel_response_v1": (
                                                    _cancel_result.cancel_response_audit
                                                    or _cancel_control["existing"].get(
                                                        "cancel_response_v1"
                                                    )
                                                ),
                                                "confirmation_v1": _cancel_result.confirmation_audit,
                                            },
                                        },
                                    )
                                    await _notify(
                                        notify,
                                        user_id,
                                        f"🚫 LIMIT ордер отменён (уровень сломан)\n"
                                        f"Сделка: {symbol} {side.upper()}\n"
                                        f"Цена {_price_now} пробила STOP {stop} до входа.\n"
                                        f"Ордер снят — вход больше не актуален.",
                                    )
                                    processed += 1
                                    continue
                        except Exception as _sb_exc:
                            log.debug(
                                "stop-breach check failed for %s: %s", symbol, _sb_exc
                            )

                            # ── Инвалидация 3: пользовательский прогресс TP ───────────────
                            # Every observed TP touch is persisted. A brief spike therefore
                            # remains evidence even if price returns before the next 30s pass.
                        if targets and not _skip_limit_invalidation:
                            try:
                                _entry_price = float(row.get("entry") or 0.0)
                                _price_for_tp = _price_now if _price_now > 0 else None
                                if _price_for_tp is None:
                                    _price_for_tp = float(
                                        (market_prices or {}).get(symbol)
                                        or await adapter.fetch_last_price(symbol)
                                    )
                                _runtime, _touch_changed, _max_passed = record_tp_touch(
                                    ids_payload.get(LIMIT_POLICY_RUNTIME_KEY),
                                    side=side,
                                    current_price=_price_for_tp,
                                    targets=targets,
                                )
                                if _touch_changed:
                                    ids_payload[LIMIT_POLICY_RUNTIME_KEY] = _runtime
                                    await _persist_limit_policy_runtime(
                                        execution_id, _runtime
                                    )
                                if (
                                    _tp_threshold_index > 0
                                    and _entry_price > 0
                                    and _price_for_tp > 0
                                    and _max_passed >= _tp_threshold_index
                                    and not _skip_limit_invalidation
                                ):
                                    _threshold_price = float(
                                        targets[_tp_threshold_index - 1]
                                    )
                                    _other_execs3 = (
                                        await db.other_active_symbol_executions(
                                            user_id, symbol, execution_id, limit=1
                                        )
                                    )
                                    _cancel_control = _limit_cancel_control(
                                        ids_payload, policy_reason="tp_progress"
                                    )
                                    _cancel_result = (
                                        await _cancel_pending_entry_confirmed(
                                            adapter,
                                            user_id=user_id,
                                            execution_id=execution_id,
                                            symbol=symbol,
                                            side=side,
                                            entry_order=ids_payload.get("entry"),
                                            other_active_execution=bool(_other_execs3),
                                            attempts=(
                                                6
                                                if _cancel_control["allow_write"]
                                                else 2
                                            ),
                                            allow_write=bool(
                                                _cancel_control["allow_write"]
                                            ),
                                            write_suppressed_exhausted=bool(
                                                _cancel_control["exhausted"]
                                            ),
                                            policy_reason="tp_progress",
                                        )
                                    )
                                    _cancel_reason = _cancel_result.reason
                                    if _cancel_result.filled_or_position:
                                        _skip_limit_invalidation = True
                                        _fill_patch = {
                                            "limit_cancel_pending": None,
                                            "limit_cancel_race": {
                                                "policy_reason": "tp_progress",
                                                "disposition": _cancel_result.disposition.value,
                                                "cancel_response_v1": (
                                                    _cancel_result.cancel_response_audit
                                                    or _cancel_control["existing"].get(
                                                        "cancel_response_v1"
                                                    )
                                                ),
                                                "confirmation_v1": _cancel_result.confirmation_audit,
                                                "detected_at": datetime.now(
                                                    timezone.utc
                                                ).isoformat(),
                                            },
                                        }
                                        if isinstance(
                                            _cancel_result.order_status, dict
                                        ):
                                            _fill_patch["limit_fill_status"] = {
                                                key: _cancel_result.order_status.get(
                                                    key
                                                )
                                                for key in (
                                                    "order_id",
                                                    "state",
                                                    "terminal",
                                                    "fully_filled",
                                                    "filled_qty",
                                                    "requested_qty",
                                                    "avg_fill_price",
                                                    "position_id",
                                                    "external_oid",
                                                    "qty_step",
                                                )
                                            }
                                        await _write_status(
                                            "pending_limit",
                                            "Fill/position detected during TP-progress cancellation; continuing post-fill reconciliation",
                                            _fill_patch,
                                        )
                                    elif (
                                        _cancel_result.disposition
                                        == PendingEntryCancelDisposition.TERMINAL_NO_FILL
                                    ):
                                        await _finalize_terminal_no_fill_result(
                                            result=_cancel_result,
                                            execution_id=execution_id,
                                            user_id=user_id,
                                            symbol=symbol,
                                            side=side,
                                            notify=notify,
                                            policy_reason=_pending_cancel_reason(
                                                ids_payload
                                            ),
                                            prior_cancel_pending=_cancel_control[
                                                "existing"
                                            ],
                                            expected_status=_known_status,
                                        )
                                        processed += 1
                                        continue
                                    elif not _cancel_result.canceled:
                                        _cancel_pending = _limit_cancel_pending_record(
                                            previous=_cancel_control["existing"],
                                            policy_reason="tp_progress",
                                            result=_cancel_result,
                                        )
                                        _cancel_pending.update(
                                            {
                                                "tp_threshold_index": _tp_threshold_index,
                                                "max_tp_passed": _max_passed,
                                            }
                                        )
                                        await _write_status(
                                            "pending_limit",
                                            f"Configured TP progress reached, but LIMIT cancellation is not confirmed: {_cancel_reason}",
                                            {"limit_cancel_pending": _cancel_pending},
                                        )
                                        # If a fill appeared during cancellation, the next
                                        # cycle must enter normal post-fill protection; do not
                                        # spam the user with a false cancellation warning.
                                        if (
                                            "fill" not in _cancel_reason.lower()
                                            and "position" not in _cancel_reason.lower()
                                            and _limit_cancel_should_notify(
                                                _cancel_control["existing"],
                                                _cancel_pending,
                                            )
                                        ):
                                            await _notify(
                                                notify,
                                                user_id,
                                                _limit_cancel_warning_text(
                                                    symbol=symbol,
                                                    side=side,
                                                    result=_cancel_result,
                                                    pending=_cancel_pending,
                                                ),
                                                event_key=(
                                                    f"limit-cancel:{execution_id}:"
                                                    f"{_cancel_pending.get('write_attempts')}:"
                                                    f"{int(bool(_cancel_pending.get('exhausted')))}"
                                                ),
                                                reply_markup_spec=_limit_cancel_recheck_markup_spec(
                                                    execution_id=execution_id,
                                                    user_id=user_id,
                                                ),
                                            )
                                        continue
                                    else:
                                        await _write_status(
                                            "canceled_tp_progress",
                                            (
                                                f"LIMIT отменён по пользовательской политике: прошли "
                                                f"{_max_passed} TP, порог TP{_tp_threshold_index} "
                                                f"({_threshold_price}); отмена подтверждена, позиции нет."
                                            ),
                                            {
                                                "limit_cancel_pending": None,
                                                "limit_cancel_confirmed": {
                                                    "reason": "tp_progress",
                                                    "tp_mode": _tp_mode,
                                                    "tp_threshold_index": _tp_threshold_index,
                                                    "max_tp_passed": _max_passed,
                                                    "observed_price": float(
                                                        _price_for_tp
                                                    ),
                                                    "confirmed_at": datetime.now(
                                                        timezone.utc
                                                    ).isoformat(),
                                                    "cancel_response_v1": (
                                                        _cancel_result.cancel_response_audit
                                                        or _cancel_control[
                                                            "existing"
                                                        ].get("cancel_response_v1")
                                                    ),
                                                    "confirmation_v1": _cancel_result.confirmation_audit,
                                                },
                                            },
                                        )
                                        await _notify(
                                            notify,
                                            user_id,
                                            f"📈 СИГНАЛ СТАЛ НЕАКТУАЛЬНЫМ\n"
                                            f"Сделка: {symbol} {side.upper()}\n"
                                            f"Цена прошла {_max_passed} из {len(targets)} целей без исполнения входа.\n"
                                            f"Правило: {limit_tp_mode_label(_tp_mode)}.\n"
                                            "Лимитный ордер отменён, позиция не открыта.",
                                        )
                                        processed += 1
                                        continue
                            except Exception as _tp_exc:
                                log.debug(
                                    "tp-progress check failed for %s: %s",
                                    symbol,
                                    _tp_exc,
                                )

                                # Short in-row cache: enough to avoid 10-15 REST calls per TP loop,
                                # but still refreshes after every real exchange action.
                        cache: dict[str, Any] = {"positions": None, "price": None}

                        async def get_positions_cached(
                            force: bool = False,
                        ) -> list[dict[str, Any]]:
                            if force or cache.get("positions") is None:
                                cache["positions"] = await adapter.fetch_open_positions(
                                    symbol, side.upper()
                                )
                            return cache["positions"] or []

                        async def get_price_cached(force: bool = False) -> float:
                            event_price = float(
                                (market_prices or {}).get(symbol) or 0.0
                            )
                            if event_price > 0:
                                cache["price"] = event_price
                            elif force or cache.get("price") is None:
                                cache["price"] = float(
                                    await adapter.fetch_last_price(symbol)
                                )
                            return float(cache["price"])

                        def cap_qty(qty: float) -> float:
                            if intended_qty > 0:
                                return min(qty, intended_qty)
                            return qty

                        positions = await get_positions_cached(force=True)
                        if not positions:
                            # LIMIT still pending or position not reflected yet (API lag).
                            # Also verify the LIMIT order itself still exists on BingX.
                            # If no open order matches our clientOrderId and no position
                            # appeared, BingX silently rejected or expired the order.
                            try:
                                _price_now = await get_price_cached(force=True)
                                if _breached_stop(side, _price_now, stop):
                                    log.warning(
                                        "limit_tp_catchup: %s %s stop breached (%.6f vs %.6f) "
                                        "but no position visible — possible fill+stop-out gap",
                                        symbol,
                                        side,
                                        _price_now,
                                        stop,
                                    )
                            except Exception:
                                pass

                                # Order detail is authoritative. A filled LIMIT can
                                # disappear from open orders before the position endpoint
                                # reflects it; do not classify that propagation gap as a
                                # rejected order.
                            _entry_order = ids_payload.get("entry") or {}
                            try:
                                _fill_probe = (
                                    await adapter.fetch_entry_order_fill_status(
                                        symbol=symbol, order_response=_entry_order
                                    )
                                )
                            except Exception:
                                _fill_probe = None
                            if isinstance(_fill_probe, dict):
                                ids_payload["limit_fill_status"] = {
                                    key: _fill_probe.get(key)
                                    for key in (
                                        "order_id",
                                        "state",
                                        "terminal",
                                        "fully_filled",
                                        "filled_qty",
                                        "requested_qty",
                                        "avg_fill_price",
                                        "position_id",
                                        "external_oid",
                                        "qty_step",
                                    )
                                }
                                _probe_filled = _f(_fill_probe.get("filled_qty"), 0.0)
                                if _probe_filled > 0:
                                    if bool(_fill_probe.get("terminal")):
                                        missing_count = (
                                            int(
                                                ids_payload.get(
                                                    "_terminal_fill_position_missing_count"
                                                )
                                                or 0
                                            )
                                            + 1
                                        )
                                        ids_payload[
                                            "_terminal_fill_position_missing_count"
                                        ] = missing_count
                                        # A terminal entry can disappear immediately if
                                        # the attached STOP closed it before the position
                                        # endpoint was observed. Allow a generous sync
                                        # window, then stop polling a position that no
                                        # longer exists instead of leaving it pending forever.
                                        if missing_count >= 6:
                                            await persist(
                                                "closed_on_exchange",
                                                "Terminal LIMIT fill was confirmed, but no live position appeared after six reconciliation cycles; position is treated as already closed on BingX",
                                            )
                                            await _notify(
                                                notify,
                                                user_id,
                                                f"ℹ️ LIMIT {symbol} уже закрыт на BingX\n"
                                                f"Биржа подтвердила исполненный объём {_probe_filled:.12g}, "
                                                "но позиция не появилась в шести контрольных циклах. "
                                                "Новые TP не создавались.",
                                            )
                                            processed += 1
                                            continue
                                    await persist(
                                        "pending_limit",
                                        f"LIMIT order reports filled qty {_probe_filled:.12g}; waiting for position endpoint synchronization",
                                    )
                                    continue
                                if bool(_fill_probe.get("terminal")):
                                    (
                                        _terminal_status,
                                        _terminal_reason,
                                        _terminal_title,
                                    ) = _terminal_no_fill_classification(
                                        _fill_probe,
                                        policy_reason=_pending_cancel_reason(
                                            ids_payload
                                        ),
                                    )
                                    await persist(
                                        _terminal_status,
                                        _terminal_reason,
                                    )
                                    await _notify(
                                        notify,
                                        user_id,
                                        f"{_terminal_title}\n"
                                        f"Сделка: {symbol} {side.upper()}\n"
                                        f"ID исполнения: {execution_id}\n"
                                        f"Состояние BingX: {_fill_probe.get('state')}",
                                    )
                                    processed += 1
                                    continue

                                    # Check if the original LIMIT order still exists on exchange.
                            _entry_client_id = ""
                            _entry_order_id = ""
                            if isinstance(_entry_order, dict):
                                _entry_order_id = clean_exchange_id(
                                    _entry_order.get("orderId")
                                    or (_entry_order.get("data") or {}).get("orderId")
                                    or _entry_order.get("orderID")
                                    or (_entry_order.get("data") or {}).get("orderID")
                                )
                                for _k in (
                                    "_entry_external_oid",
                                    "externalOid",
                                    "clientOrderId",
                                    "newClientOrderId",
                                    "clientId",
                                    "clOrdId",
                                ):
                                    _v = clean_exchange_id(_entry_order.get(_k))
                                    if _v:
                                        _entry_client_id = _v
                                        break
                            _entry_identity = _entry_client_id or _entry_order_id
                            if _entry_identity:
                                try:
                                    _open_orders = await adapter.fetch_open_orders(
                                        symbol
                                    )
                                    _open_order_ids = {
                                        clean_exchange_id(
                                            o.get("orderId")
                                            or o.get("id")
                                            or (o.get("raw") or {}).get("orderId")
                                            or (o.get("raw") or {}).get("orderID")
                                        )
                                        for o in (_open_orders or [])
                                        if isinstance(o, dict)
                                    }
                                    _open_client_ids = {
                                        clean_exchange_id(
                                            o.get("clientOrderId")
                                            or o.get("newClientOrderId")
                                            or (o.get("raw") or {}).get("clientOrderId")
                                            or (o.get("raw") or {}).get("externalOid")
                                        )
                                        for o in (_open_orders or [])
                                        if isinstance(o, dict)
                                    }
                                    _entry_still_open = (
                                        bool(_entry_order_id and _entry_order_id in _open_order_ids)
                                        or bool(_entry_client_id and _entry_client_id in _open_client_ids)
                                    )
                                    if _entry_still_open:
                                        # Order found — reset missing counter if it was incremented before.
                                        if (
                                            ids_payload.get(
                                                "_limit_order_missing_count", 0
                                            )
                                            > 0
                                        ):
                                            ids_payload[
                                                "_limit_order_missing_count"
                                            ] = 0
                                            await _write_status(
                                                "pending_limit",
                                                "",
                                                {"_limit_order_missing_count": 0},
                                            )
                                    else:
                                        # The exact LIMIT is no longer on exchange and no
                                        # position is visible.  After repeated confirmations
                                        # remove it from active bot limits instead of showing
                                        # a stale phantom LIMIT in the menu.
                                        _miss_count = (
                                            int(
                                                ids_payload.get(
                                                    "_limit_order_missing_count"
                                                )
                                                or 0
                                            )
                                            + 1
                                        )
                                        ids_payload["_limit_order_missing_count"] = (
                                            _miss_count
                                        )
                                        log.warning(
                                            "limit_tp_catchup: %s %s exact LIMIT %s not in open "
                                            "orders and no position is visible (missing_count=%d)",
                                            symbol,
                                            side,
                                            _entry_identity,
                                            _miss_count,
                                        )
                                        if _miss_count >= 3:
                                            await _write_status(
                                                "canceled_external",
                                                f"LIMIT ордер {_entry_identity} отсутствует на BingX "
                                                f"и позиция не найдена (проверено {_miss_count} раз). "
                                                f"Запись снята из активных лимиток как внешне отменённая/исчезнувшая.",
                                                {
                                                    "_limit_order_missing_count": _miss_count,
                                                    "limit_cancel_confirmed": {
                                                        "reason": "exchange_order_missing_no_position",
                                                        "order_id": _entry_order_id,
                                                        "client_order_id": _entry_client_id,
                                                        "confirmed_at": datetime.now(timezone.utc).isoformat(),
                                                    },
                                                },
                                            )
                                            await _notify(
                                                notify,
                                                user_id,
                                                f"ℹ️ LIMIT ордер снят из активных\n"
                                                f"Сделка: {symbol} {side.upper()}\n"
                                                f"Точный ордер {_entry_identity} не найден на BingX {_miss_count} раза подряд, "
                                                "позиции тоже нет. Запись больше не будет висеть в лимитках.",
                                            )
                                            processed += 1
                                            continue
                                        await _write_status(
                                            "pending_limit",
                                            "",
                                            {"_limit_order_missing_count": _miss_count},
                                        )
                                except Exception as _oo_exc:
                                    log.debug(
                                        "limit_tp_catchup: could not verify open orders: %s",
                                        _oo_exc,
                                    )
                            else:
                                try:
                                    _identityless_result = await _prove_identityless_pending_entry_absent(
                                        adapter,
                                        symbol=symbol,
                                        side=side,
                                        entry_payload=(
                                            _entry_order if isinstance(_entry_order, dict) else {}
                                        ),
                                    )
                                    if (
                                        _identityless_result.disposition
                                        == PendingEntryCancelDisposition.CANCELED_NO_FILL
                                    ):
                                        _miss_count = (
                                            int(
                                                ids_payload.get(
                                                    "_identityless_limit_missing_count"
                                                )
                                                or 0
                                            )
                                            + 1
                                        )
                                        ids_payload[
                                            "_identityless_limit_missing_count"
                                        ] = _miss_count
                                        if _miss_count >= 3:
                                            await _write_status(
                                                "canceled_external",
                                                "Legacy LIMIT has no orderId/clientOrderId, but BingX shows no position and no plausible open entry order after repeated checks; local phantom removed.",
                                                {
                                                    "_identityless_limit_missing_count": _miss_count,
                                                    "limit_cancel_confirmed": {
                                                        "reason": "identityless_exchange_order_missing_no_position",
                                                        "confirmed_at": datetime.now(timezone.utc).isoformat(),
                                                        "confirmation_v1": _identityless_result.confirmation_audit,
                                                    },
                                                },
                                            )
                                            await _notify(
                                                notify,
                                                user_id,
                                                f"ℹ️ LIMIT ордер снят из активных\n"
                                                f"Сделка: {symbol} {side.upper()}\n"
                                                "У старой записи нет точного orderId/clientOrderId, но BingX трижды показала: "
                                                "позиции нет и похожего активного entry-ордера нет.",
                                            )
                                            processed += 1
                                            continue
                                        await _write_status(
                                            "pending_limit",
                                            "",
                                            {
                                                "_identityless_limit_missing_count": _miss_count,
                                                "identityless_limit_absence_v1": _identityless_result.confirmation_audit,
                                            },
                                        )
                                except Exception as _idless_exc:
                                    log.debug(
                                        "limit_tp_catchup: identityless LIMIT absence check failed: %s",
                                        _idless_exc,
                                    )

                                    # Keep status as pending_limit — next cycle may see position.
                            continue
                        live_qty = cap_qty(_total_position_size(positions))
                        if ids_payload.get("_terminal_fill_position_missing_count"):
                            ids_payload["_terminal_fill_position_missing_count"] = 0
                        if positions:
                            actual_entry_from_position = _position_entry_price(
                                positions[0]
                            )
                            if actual_entry_from_position > 0:
                                ids_payload["actual_entry"] = actual_entry_from_position
                        if live_qty <= 0:
                            continue
                        current = await get_price_cached(force=True)

                        entry_payload = (
                            ids_payload.get("entry")
                            if isinstance(ids_payload, dict)
                            else None
                        )
                        tp_qty_step = order_required_qty_step(entry_payload, None)

                        # A partial LIMIT fill can leave the remaining entry order open.
                        # If STOP is already breached, cancel that remainder first and
                        # only then close the visible position. Otherwise the leftover
                        # LIMIT could reopen exposure after the emergency close.
                        if _breached_stop(side, current, stop):
                            async with db.symbol_action_lock(user_id, symbol):
                                cancel_result = (
                                    await _cancel_opening_order_remainder_confirmed(
                                        adapter,
                                        symbol=symbol,
                                        side=side,
                                        entry_order=entry_payload,
                                    )
                                )
                                cancel_reason = cancel_result.reason
                                if (
                                    cancel_result.disposition
                                    != PendingEntryCancelDisposition.FILLED
                                ):
                                    await persist(
                                        "manual_required",
                                        "STOP breached during partial LIMIT fill, but remaining entry cancellation was not confirmed: "
                                        + cancel_reason,
                                    )
                                    await _notify(
                                        notify,
                                        user_id,
                                        f"🚨 LIMIT-защита {symbol}\n"
                                        f"Цена уже за STOP, но остаток входного LIMIT не удалось подтвердить отменённым.\n"
                                        f"{cancel_reason}\nПроверь BingX вручную.",
                                    )
                                    processed += 1
                                    continue
                                final_fill_qty = 0.0
                                final_position_id: str | int | None = None
                                if isinstance(cancel_result.order_status, dict):
                                    ids_payload["limit_fill_status"] = {
                                        key: cancel_result.order_status.get(key)
                                        for key in (
                                            "order_id",
                                            "state",
                                            "terminal",
                                            "fully_filled",
                                            "filled_qty",
                                            "requested_qty",
                                            "avg_fill_price",
                                            "position_id",
                                            "external_oid",
                                            "qty_step",
                                        )
                                    }
                                    final_fill_qty = _f(
                                        cancel_result.order_status.get("filled_qty"),
                                        0.0,
                                    )
                                    raw_position_id = cancel_result.order_status.get(
                                        "position_id"
                                    )
                                    if raw_position_id not in (None, "", 0, "0"):
                                        final_position_id = raw_position_id

                                        # The order can receive one last fill while its exact
                                        # cancellation is in flight.  Never close the stale
                                        # pre-cancel ``live_qty`` snapshot: re-read the position
                                        # after terminal order confirmation and keep checking until
                                        # the position endpoint catches up or the attached STOP has
                                        # already removed the exposure.
                                bot_qty_cap = (
                                    intended_qty if intended_qty > 0 else final_fill_qty
                                )
                                if final_fill_qty > 0:
                                    bot_qty_cap = (
                                        min(bot_qty_cap, final_fill_qty)
                                        if bot_qty_cap > 0
                                        else final_fill_qty
                                    )
                                qty_tolerance = max(
                                    _f(
                                        (cancel_result.order_status or {}).get(
                                            "qty_step"
                                        ),
                                        0.0,
                                    )
                                    * 0.51,
                                    1e-10,
                                )
                                close_results: list[dict[str, Any]] = []
                                last_visible_qty = 0.0
                                for close_round in range(3):
                                    if close_round:
                                        await asyncio.sleep(
                                            (0.35, 0.75)[close_round - 1]
                                        )
                                    fresh_positions = (
                                        await adapter.fetch_open_positions(
                                            symbol, side.upper()
                                        )
                                    )
                                    visible_total = _total_position_size(
                                        fresh_positions
                                    )
                                    last_visible_qty = (
                                        min(visible_total, bot_qty_cap)
                                        if bot_qty_cap > 0
                                        else visible_total
                                    )
                                    if last_visible_qty <= qty_tolerance:
                                        break
                                    try:
                                        close_res = await adapter.emergency_close_market_confirmed(
                                            symbol=symbol,
                                            side=side,
                                            qty=last_visible_qty,
                                            client_id=(
                                                f"catch-sl-{execution_id}-{close_round +1 }"
                                            ),
                                            position_id=final_position_id,
                                        )
                                        close_results.append(
                                            {
                                                "round": close_round + 1,
                                                "qty": last_visible_qty,
                                                "result": close_res,
                                            }
                                        )
                                    except Exception as close_exc:
                                        # A concurrently executed attached STOP can make the
                                        # emergency close return an error even though no
                                        # position remains.  Re-read before declaring failure.
                                        verify_positions = (
                                            await adapter.fetch_open_positions(
                                                symbol, side.upper()
                                            )
                                        )
                                        verify_total = _total_position_size(
                                            verify_positions
                                        )
                                        verify_qty = (
                                            min(verify_total, bot_qty_cap)
                                            if bot_qty_cap > 0
                                            else verify_total
                                        )
                                        if verify_qty <= qty_tolerance:
                                            close_results.append(
                                                {
                                                    "round": close_round + 1,
                                                    "qty": last_visible_qty,
                                                    "resolved_by_position_absence": True,
                                                    "error": f"{type(close_exc).__name__}: {close_exc}",
                                                }
                                            )
                                            last_visible_qty = 0.0
                                            break
                                        raise

                                final_positions = await adapter.fetch_open_positions(
                                    symbol, side.upper()
                                )
                                final_total = _total_position_size(final_positions)
                                final_remaining = (
                                    min(final_total, bot_qty_cap)
                                    if bot_qty_cap > 0
                                    else final_total
                                )
                                if final_remaining > qty_tolerance:
                                    ids_payload["partial_stop_close"] = {
                                        "entry_final_fill_qty": final_fill_qty,
                                        "bot_qty_cap": bot_qty_cap,
                                        "remaining_qty": final_remaining,
                                        "close_results": close_results,
                                    }
                                    await persist(
                                        "manual_required",
                                        "STOP breached after partial LIMIT fill; exact entry remainder is terminal, but residual position remains after confirmed close attempts",
                                    )
                                    await _notify(
                                        notify,
                                        user_id,
                                        f"🚨 LIMIT-защита {symbol}\n"
                                        f"Остаток входной LIMIT отменён точно, но после закрытия остаётся позиция {final_remaining:.12g}.\n"
                                        "Нужна немедленная ручная проверка BingX.",
                                    )
                                    processed += 1
                                    continue

                            total_closed_qty = sum(
                                _f(item.get("qty"), 0.0) for item in close_results
                            )
                            actions.append(
                                {
                                    "type": "stop_catchup_market_close",
                                    "qty": total_closed_qty,
                                    "current": current,
                                    "stop": stop,
                                    "entry_final_fill_qty": final_fill_qty,
                                    "close_results": close_results,
                                    "position_absent_after_cancel": not close_results,
                                }
                            )
                            await persist(
                                "closed_stop_catchup",
                                "LIMIT fill detected after STOP breach; exact entry remainder became terminal and no residual position remains",
                            )
                            await _notify(
                                notify,
                                user_id,
                                f"🚨 LIMIT-защита {symbol}\n"
                                "Цена уже за STOP после fill. Остаток точной LIMIT отменён; "
                                "позиция закрыта либо уже была закрыта attached STOP.\n"
                                f"{_fmt_actions(actions)}",
                            )
                            processed += 1
                            continue

                        snapshot = get_snapshot(ids_payload)
                        if snapshot is None:
                            # Never upgrade a legacy pre-v1.6.4 partial TP journal by
                            # recalculating it against a different final quantity. That
                            # is exactly how 0.6 vs 0.7 same-price conflicts were created.
                            legacy_side_effects = any(
                                str(item.get("type") or "")
                                in {
                                    "tp_created_after_fill",
                                    "tp_catchup_market_close",
                                    "stop_catchup_market_close",
                                }
                                for item in actions
                                if isinstance(item, dict)
                            )
                            if legacy_side_effects:
                                await persist(
                                    "manual_required",
                                    "Legacy LIMIT TP actions exist without an immutable final-fill plan; automatic resizing is blocked",
                                )
                                await _notify(
                                    notify,
                                    user_id,
                                    f"🚨 Нужна ручная проверка {symbol}\n"
                                    "Сделка была частично обработана старой версией до фиксации окончательного fill. "
                                    "Бот не будет пересчитывать или дублировать TP автоматически.",
                                )
                                processed += 1
                                continue

                            try:
                                fill_status = (
                                    await adapter.fetch_entry_order_fill_status(
                                        symbol=symbol,
                                        order_response=entry_payload or {},
                                    )
                                )
                            except Exception as fill_exc:
                                if is_transient_exchange_error(fill_exc):
                                    attempts = await persist_transient(
                                        "entry_fill_status", fill_exc
                                    )
                                    if should_notify_transient(
                                        attempts,
                                        every=get_settings().EXCHANGE_TRANSIENT_ERROR_NOTIFY_EVERY,
                                    ):
                                        await _notify(
                                            notify,
                                            user_id,
                                            f"⏳ Проверка окончательного fill {symbol} временно недоступна\n"
                                            f"{type(fill_exc).__name__}: {str(fill_exc)[:300]}\n"
                                            "TP пока не создаются; STOP продолжает защищать позицию.",
                                        )
                                    continue
                                await persist(
                                    "manual_required",
                                    f"Cannot verify final LIMIT fill: {type(fill_exc).__name__}: {fill_exc}",
                                )
                                await _notify(
                                    notify,
                                    user_id,
                                    f"🚨 Не удалось подтвердить окончательный fill {symbol}\n"
                                    f"{type(fill_exc).__name__}: {str(fill_exc)[:400]}\n"
                                    "TP не создаются до ручной проверки, STOP остаётся активным.",
                                )
                                processed += 1
                                continue

                            compact_fill = {
                                key: fill_status.get(key)
                                for key in (
                                    "order_id",
                                    "state",
                                    "terminal",
                                    "fully_filled",
                                    "filled_qty",
                                    "requested_qty",
                                    "avg_fill_price",
                                    "position_id",
                                    "external_oid",
                                    "qty_step",
                                )
                            }
                            ids_payload["limit_fill_status"] = compact_fill
                            if not bool(fill_status.get("terminal")):
                                adoption = await _prove_live_position_without_open_entry(
                                    adapter,
                                    symbol=symbol,
                                    side=side,
                                    entry_payload=entry_payload or {},
                                    live_qty=live_qty,
                                )
                                ids_payload["live_position_entry_adoption_v1"] = adoption
                                if bool(adoption.get("can_adopt")):
                                    adopted_qty = min(
                                        live_qty,
                                        intended_qty if intended_qty > 0 else live_qty,
                                    )
                                    fill_status = dict(fill_status)
                                    fill_status.update(
                                        {
                                            "filled": True,
                                            "terminal": True,
                                            "fully_filled": True,
                                            "state": 3,
                                            "state_name": "LIVE_POSITION_NO_OPEN_ENTRY_FALLBACK",
                                            "status": "LIVE_POSITION_NO_OPEN_ENTRY_FALLBACK",
                                            "filled_qty": adopted_qty,
                                            "requested_qty": intended_qty or adopted_qty,
                                        }
                                    )
                                    compact_fill = {
                                        key: fill_status.get(key)
                                        for key in (
                                            "order_id",
                                            "state",
                                            "terminal",
                                            "fully_filled",
                                            "filled_qty",
                                            "requested_qty",
                                            "avg_fill_price",
                                            "position_id",
                                            "external_oid",
                                            "qty_step",
                                        )
                                    }
                                    ids_payload["limit_fill_status"] = compact_fill
                                    actions.append(
                                        {
                                            "type": "entry_fill_adopted_from_live_position",
                                            "filled_qty": adopted_qty,
                                            "reason": adoption.get("reason"),
                                            "open_orders": adoption.get("open_orders"),
                                        }
                                    )
                                else:
                                    await persist(
                                        "pending_limit",
                                        f"LIMIT partially filled ({live_qty:.12g}); waiting for terminal entry state before locking TP plan",
                                    )
                                    continue

                            final_filled_qty = min(
                                _f(fill_status.get("filled_qty"), 0.0),
                                (
                                    intended_qty
                                    if intended_qty > 0
                                    else _f(fill_status.get("filled_qty"), 0.0)
                                ),
                            )
                            if final_filled_qty <= 0:
                                await persist(
                                    "manual_required" if live_qty > 0 else "error",
                                    "LIMIT reached terminal state without a confirmed filled quantity",
                                )
                                processed += 1
                                continue

                            if not tp_qty_step:
                                tp_qty_step = (
                                    _f(fill_status.get("qty_step"), 0.0) or None
                                )
                            qty_tolerance = max(_f(tp_qty_step, 0.0) * 0.51, 1e-10)

                            # The order detail can become terminal before the position
                            # endpoint catches up. Wait inside the same pass first; if
                            # it is still short, leave the row pending and retry later.
                            if live_qty + qty_tolerance < final_filled_qty:
                                for sync_delay in (0.25, 0.50, 1.0, 1.5):
                                    await asyncio.sleep(sync_delay)
                                    positions = await get_positions_cached(force=True)
                                    live_qty = cap_qty(_total_position_size(positions))
                                    if live_qty + qty_tolerance >= final_filled_qty:
                                        break
                            if live_qty + qty_tolerance < final_filled_qty:
                                ids_payload["limit_fill_sync"] = {
                                    "expected_final_qty": final_filled_qty,
                                    "observed_position_qty": live_qty,
                                    "waiting": True,
                                }
                                await persist(
                                    "pending_limit",
                                    f"Terminal LIMIT fill is {final_filled_qty:.12g}, but position endpoint shows {live_qty:.12g}; waiting for BingX synchronization",
                                )
                                continue

                                # Persist the exact stop-plan id generated from the
                                # original LIMIT attached STOP. BingX's LIMIT create
                                # response contains only the regular entry order id;
                                # without this reconciliation BE later sees the bot's
                                # own initial STOP as manual/untracked and leaves two
                                # STOPs on the position. Failure to identify is non-fatal
                                # and remains fail-closed: the STOP is preserved.
                            try:
                                ownership_info = await adapter.instrument_info(symbol)
                                ownership_rows = list(
                                    await adapter.fetch_open_algo_orders(symbol) or []
                                )
                                attached_stop_ownership = identify_limit_attached_stop(
                                    ownership_rows,
                                    payload=ids_payload,
                                    position_id=clean_exchange_id(
                                        fill_status.get("position_id")
                                        or _position_id(positions)
                                    ),
                                    side=side,
                                    original_stop=stop,
                                    minimum_qty=final_filled_qty,
                                    price_tolerance=max(
                                        _f(
                                            getattr(ownership_info, "price_tick", 0.0),
                                            0.0,
                                        )
                                        * 0.51,
                                        abs(float(stop)) * 1e-9,
                                        1e-12,
                                    ),
                                    qty_tolerance=max(
                                        _f(
                                            getattr(ownership_info, "qty_step", 0.0),
                                            0.0,
                                        )
                                        * 0.51,
                                        1e-12,
                                    ),
                                    # Immediate terminal-fill reconciliation is the
                                    # only place where a unique unlinked signature may
                                    # be adopted.  Legacy BE cleanup remains exact-link
                                    # only so a manual STOP is never claimed later.
                                    allow_unique_signature_fallback=True,
                                )
                                attached_stop_record = build_limit_attached_stop_record(
                                    attached_stop_ownership,
                                    position_id=clean_exchange_id(
                                        fill_status.get("position_id")
                                        or _position_id(positions)
                                    ),
                                    original_stop=stop,
                                )
                                if attached_stop_record:
                                    ids_payload[LIMIT_ATTACHED_STOP_KEY] = (
                                        attached_stop_record
                                    )
                                    actions.append(
                                        {
                                            "type": "limit_attached_stop_identity_confirmed",
                                            "stop_order_id": attached_stop_record.get(
                                                "stopPlanOrderId"
                                            ),
                                            "basis": attached_stop_record.get(
                                                "match_basis"
                                            ),
                                        }
                                    )
                                elif attached_stop_ownership.get("ambiguous"):
                                    actions.append(
                                        {
                                            "type": "limit_attached_stop_identity_ambiguous",
                                            "candidate_ids": attached_stop_ownership.get(
                                                "candidate_ids"
                                            ),
                                            "reason": attached_stop_ownership.get(
                                                "reason"
                                            ),
                                        }
                                    )
                                else:
                                    # v1.6.56: if a LIMIT fill is already confirmed/adopted
                                    # but BingX does not expose the attached SL in open orders,
                                    # create an explicit same-side protective STOP before any TP
                                    # writes.  This avoids the live failure mode where a position
                                    # becomes visible as manual/external with zero tracked STOP/TP.
                                    try:
                                        fallback_stop = await adapter.set_position_stop_loss(
                                            symbol=symbol,
                                            side=side,
                                            qty=final_filled_qty,
                                            stop=stop,
                                            client_id=f"limit-fill-stop-{execution_id}",
                                            position_id=clean_exchange_id(
                                                fill_status.get("position_id")
                                                or _position_id(positions)
                                            ),
                                        )
                                    except Exception as stop_exc:
                                        ids_payload["limit_fill_stop_fallback_error_v1"] = {
                                            "error": f"{type(stop_exc).__name__}: {stop_exc}"[:500],
                                            "created_at": datetime.now(timezone.utc).isoformat(),
                                            "reason": attached_stop_ownership.get("reason"),
                                        }
                                        await persist(
                                            "manual_required",
                                            "LIMIT filled but no attached STOP identity was found and explicit fallback STOP creation failed: "
                                            f"{type(stop_exc).__name__}: {stop_exc}",
                                        )
                                        await _notify(
                                            notify,
                                            user_id,
                                            f"🚨 STOP не подтверждён по {symbol}\n"
                                            "LIMIT уже исполнен, но бот не нашёл защитный STOP и не смог поставить fallback STOP.\n"
                                            f"Ошибка: {type(stop_exc).__name__}: {str(stop_exc)[:300]}\n"
                                            "Проверь BingX вручную немедленно.",
                                        )
                                        processed += 1
                                        continue
                                    normalized_stop = (
                                        fallback_stop.get("normalized")
                                        if isinstance(fallback_stop, dict)
                                        else {}
                                    )
                                    fallback_stop_id = clean_exchange_id(
                                        (fallback_stop or {}).get("stopPlanOrderId")
                                        or (fallback_stop or {}).get("orderId")
                                        or (fallback_stop or {}).get("orderID")
                                    )
                                    fallback_record = {
                                        "stopPlanOrderId": fallback_stop_id,
                                        "positionId": clean_exchange_id(
                                            fill_status.get("position_id")
                                            or _position_id(positions)
                                        ),
                                        "positionSide": side,
                                        "stopLossPrice": _f(
                                            (normalized_stop or {}).get("price"), stop
                                        ),
                                        "qty": _f(
                                            (normalized_stop or {}).get("qty"),
                                            final_filled_qty,
                                        ),
                                        "placeOrderId": clean_exchange_id(
                                            fill_status.get("order_id")
                                        ),
                                        "match_basis": "explicit_fallback_created_after_missing_attached_stop",
                                        "entry_order_ids": list(
                                            attached_stop_ownership.get("entry_order_ids")
                                            or []
                                        ),
                                        "claimed_at": datetime.now(timezone.utc).isoformat(),
                                        "created_response_v1": fallback_stop,
                                    }
                                    ids_payload[LIMIT_ATTACHED_STOP_KEY] = fallback_record
                                    actions.append(
                                        {
                                            "type": "limit_fallback_stop_created_after_fill",
                                            "stop_order_id": fallback_stop_id,
                                            "qty": fallback_record.get("qty"),
                                            "stop": fallback_record.get("stopLossPrice"),
                                            "reason": attached_stop_ownership.get("reason"),
                                        }
                                    )
                            except Exception as ownership_exc:
                                log.warning(
                                    "LIMIT attached STOP identity reconciliation failed "
                                    "execution_id=%s symbol=%s: %s: %s",
                                    execution_id,
                                    symbol,
                                    type(ownership_exc).__name__,
                                    ownership_exc,
                                )

                            policy = read_policy(
                                ids_payload,
                                fallback_targets=targets,
                                fallback_pcts=pcts,
                                fallback_qty_step=tp_qty_step,
                                fallback_min_rr=float(
                                    getattr(get_settings(), "MIN_TP_RR", 0.1)
                                ),
                            )
                            final_entry = (
                                _f(fill_status.get("avg_fill_price"), 0.0)
                                or _f(ids_payload.get("actual_entry"), 0.0)
                                or _f(row.get("entry"), 0.0)
                            )
                            snapshot = build_snapshot(
                                total_qty=final_filled_qty,
                                qty_step=policy.get("qty_step"),
                                targets=list(policy.get("targets") or targets),
                                pcts=list(policy.get("pcts") or pcts),
                                entry=final_entry,
                                stop=stop,
                                min_rr=float(policy.get("min_rr") or 0.0),
                                source="limit_terminal_fill",
                                entry_order_id=clean_exchange_id(
                                    fill_status.get("order_id")
                                ),
                                entry_state=int(fill_status.get("state") or 0),
                                position_id=clean_exchange_id(
                                    fill_status.get("position_id")
                                ),
                            )
                            if not snapshot_items(snapshot):
                                await persist(
                                    "manual_required",
                                    "Final LIMIT fill was confirmed, but immutable TP plan is empty",
                                )
                                processed += 1
                                continue
                            ids_payload[SNAPSHOT_KEY] = snapshot
                            ids_payload[POLICY_KEY] = policy
                            ids_payload["actual_entry"] = final_entry
                            exact_items = snapshot_items(snapshot)
                            exact_targets = [float(item["price"]) for item in exact_items]
                            exact_qtys = [float(item["qty"]) for item in exact_items]
                            exact_total = sum(exact_qtys)
                            exact_pcts = (
                                [round(qty / exact_total * 100.0, 6) for qty in exact_qtys]
                                if exact_total > 0
                                else []
                            )
                            try:
                                tp_locked = await db.finalize_execution_tp_distribution(
                                    int(execution_id),
                                    targets_json=json.dumps(exact_targets),
                                    tp_distribution_json=json.dumps(exact_pcts),
                                    source="limit_rounded_plan",
                                    version=1,
                                )
                                if tp_locked:
                                    submit_statistics_execution_linkage(execution_id)
                                else:
                                    log.error(
                                        "STATISTICS_LIMIT_TP_DISTRIBUTION_LOCK_REJECTED "
                                        "execution_id=%s uid=%s symbol=%s",
                                        int(execution_id),
                                        int(user_id),
                                        str(symbol).upper(),
                                    )
                            except Exception as tp_snapshot_exc:
                                # Reporting-only persistence is deliberately
                                # fail-open and cannot block STOP/TP recovery.
                                log.exception(
                                    "STATISTICS_LIMIT_TP_DISTRIBUTION_LOCK_FAILED "
                                    "execution_id=%s uid=%s symbol=%s error=%s",
                                    int(execution_id),
                                    int(user_id),
                                    str(symbol).upper(),
                                    type(tp_snapshot_exc).__name__,
                                )
                            ids_payload["limit_fill_sync"] = {
                                "expected_final_qty": final_filled_qty,
                                "observed_position_qty": live_qty,
                                "waiting": False,
                            }
                            # Persist the immutable plan BEFORE the first exchange TP
                            # write. Recovery and BE must reuse this exact snapshot.
                            await persist(
                                "pending_limit",
                                f"Terminal LIMIT fill confirmed; immutable TP plan locked for {final_filled_qty:.12g}",
                            )

                        plan_items = snapshot_items(snapshot)
                        tp_qty_by_index = snapshot_plan_map(snapshot)
                        managed_qty = min(live_qty, snapshot_total_qty(snapshot))
                        actual_qty = managed_qty
                        if managed_qty <= 0:
                            continue
                        if not any(
                            str(item.get("type") or "") == "catchup_tp_plan"
                            for item in actions
                        ):
                            actions.append(
                                {
                                    "type": "catchup_tp_plan",
                                    "mode": snapshot.get("mode"),
                                    "plan": [
                                        {
                                            "tp_idx": int(item["tp_index"]),
                                            "qty": float(item["qty"]),
                                        }
                                        for item in plan_items
                                    ],
                                    "snapshot_locked": True,
                                }
                            )

                        done_tp_indices = _done_tp_indices(actions)
                        reserved_open_tp_qty = _open_tp_qty_already_journaled(
                            actions, tp_qty_by_index
                        )
                        remaining = max(0.0, managed_qty - reserved_open_tp_qty)
                        for plan_item in plan_items:
                            idx = int(plan_item["tp_index"])
                            tp = float(plan_item["price"])
                            if idx in done_tp_indices:
                                log.info(
                                    "LIMIT catch-up duplicate TP suppressed from journal: execution=%s TP%s",
                                    execution_id,
                                    idx,
                                )
                                continue
                            if remaining <= 0:
                                break

                            async with db.symbol_action_lock(user_id, symbol):
                                # Refresh before every TP action, but reuse values within this TP step.
                                positions_now = await get_positions_cached(force=True)
                                qty_now = cap_qty(_total_position_size(positions_now))
                                if qty_now <= 0:
                                    actions.append(
                                        {"type": "closed_on_exchange", "tp_index": idx}
                                    )
                                    await persist(
                                        "closed_on_exchange",
                                        "LIMIT position disappeared before TP placement",
                                    )
                                    await _notify(
                                        notify,
                                        user_id,
                                        f"ℹ️ LIMIT catch-up {symbol}\nПозиция уже закрыта на бирже, TP больше не ставлю.",
                                    )
                                    processed += 1
                                    break

                                current = await get_price_cached(force=True)
                                if _breached_stop(side, current, stop):
                                    close_res = await adapter.emergency_close_market_confirmed(
                                        symbol=symbol,
                                        side=side,
                                        qty=qty_now,
                                        client_id=f"catch-sl-{execution_id}-{idx}",
                                    )
                                    actions.append(
                                        {
                                            "type": "stop_catchup_market_close",
                                            "qty": qty_now,
                                            "current": current,
                                            "stop": stop,
                                            "result": close_res,
                                        }
                                    )
                                    await persist(
                                        "closed_stop_catchup",
                                        "STOP breached during post-fill TP placement; position closed reduceOnly market",
                                    )
                                    await _notify(
                                        notify,
                                        user_id,
                                        f"🚨 LIMIT-защита {symbol}\nSTOP пробит во время установки TP.\n{_fmt_actions(actions)}",
                                    )
                                    processed += 1
                                    break

                                    # Use precomputed qty from build_tp_plan instead of pct-based per-TP.
                                    # If this TP was filtered out (trim mode with RR filter), tp_qty_by_index
                                    # will not contain this index → skip it correctly.
                                planned_qty_raw = tp_qty_by_index.get(idx, 0.0)
                                planned_qty = min(planned_qty_raw, qty_now, remaining)
                                if planned_qty <= 0:
                                    continue
                                client_id = f"catch-tp{idx}-{execution_id}"
                                operation = ""
                                try:
                                    if current is not None and _crossed_tp(
                                        side, current, float(tp)
                                    ):
                                        operation = "tp_market_close"
                                        res = await adapter.emergency_close_market_confirmed(
                                            symbol=symbol,
                                            side=side,
                                            qty=planned_qty,
                                            client_id=client_id,
                                        )
                                        actions.append(
                                            {
                                                "type": "tp_catchup_market_close",
                                                "tp_index": idx,
                                                "tp": tp,
                                                "qty": planned_qty,
                                                "current": current,
                                                "result": res,
                                            }
                                        )
                                        remaining = max(0.0, remaining - planned_qty)
                                        await persist(
                                            "pending_limit",
                                            f"partial post-fill action saved after TP{idx} catch-up close",
                                        )
                                        cache["positions"] = None
                                        cache["price"] = None
                                    else:
                                        # Last safety check: still not stopped and still has position.
                                        positions_before_tp = (
                                            await get_positions_cached(force=True)
                                        )
                                        qty_before_tp = cap_qty(
                                            _total_position_size(positions_before_tp)
                                        )
                                        if qty_before_tp <= 0:
                                            actions.append(
                                                {
                                                    "type": "closed_on_exchange",
                                                    "tp_index": idx,
                                                }
                                            )
                                            await persist(
                                                "closed_on_exchange",
                                                "LIMIT position disappeared before TP order create",
                                            )
                                            await _notify(
                                                notify,
                                                user_id,
                                                f"ℹ️ LIMIT catch-up {symbol}\nПозиция закрылась до создания TP{idx}.",
                                            )
                                            processed += 1
                                            break
                                        operation = "tp_order_create"
                                        submitted_qty = min(planned_qty, qty_before_tp)
                                        res = await adapter.create_take_profit(
                                            symbol=symbol,
                                            side=side,
                                            qty=submitted_qty,
                                            price=float(tp),
                                            client_id=client_id,
                                        )
                                        actual_tp_qty = order_normalized_qty(
                                            res, submitted_qty
                                        )
                                        actions.append(
                                            {
                                                "type": "tp_created_after_fill",
                                                "tp_index": idx,
                                                "tp": tp,
                                                "qty": actual_tp_qty,
                                                "planned_qty": submitted_qty,
                                                "current": current,
                                                "result": res,
                                            }
                                        )
                                        remaining = max(0.0, remaining - actual_tp_qty)
                                        await persist(
                                            "pending_limit",
                                            f"partial post-fill action saved after TP{idx} order create",
                                        )
                                        cache["positions"] = None
                                        cache["price"] = None
                                except Exception as tp_exc:
                                    has_side_effects = any(
                                        a.get("type")
                                        in {
                                            "tp_created_after_fill",
                                            "tp_catchup_market_close",
                                            "stop_catchup_market_close",
                                        }
                                        for a in actions
                                    )
                                    if isinstance(tp_exc, BingxTpCoverageError):
                                        actions.append(
                                            {
                                                "type": "tp_coverage_conflict",
                                                "tp_index": idx,
                                                "tp": tp,
                                                "error": f"{type(tp_exc).__name__}: {tp_exc}",
                                            }
                                        )
                                        await persist(
                                            "manual_required",
                                            f"LIMIT TP coverage conflict on TP{idx}: {tp_exc}",
                                        )
                                        await _notify(
                                            notify,
                                            user_id,
                                            f"🚨 Конфликт TP на BingX {symbol}\n"
                                            f"TP{idx} не был создан, чтобы не превысить объём позиции.\n"
                                            f"Проверь существующие TP вручную.\n"
                                            f"{type(tp_exc).__name__}: {str(tp_exc)[:400]}",
                                        )
                                    elif is_transient_exchange_error(tp_exc):
                                        # If the temporary error happened during a WRITE action, the exchange
                                        # may still have accepted it. Do not blindly create a duplicate TP on retry.
                                        if operation in {
                                            "tp_order_create",
                                            "tp_market_close",
                                        }:
                                            if operation == "tp_order_create":
                                                found = await find_tp_order_after_ambiguous_write(
                                                    adapter,
                                                    symbol=symbol,
                                                    side=side,
                                                    tp_index=idx,
                                                    target=float(tp),
                                                    qty=float(planned_qty),
                                                    client_id=client_id,
                                                    position_id=clean_exchange_id(
                                                        fill_status.get("position_id")
                                                        or _position_id(positions)
                                                    ),
                                                )
                                                if found:
                                                    actual_tp_qty = (
                                                        order_normalized_qty(
                                                            found, planned_qty
                                                        )
                                                    )
                                                    actions.append(
                                                        {
                                                            "type": "tp_created_after_fill",
                                                            "tp_index": idx,
                                                            "tp": tp,
                                                            "qty": actual_tp_qty,
                                                            "planned_qty": planned_qty,
                                                            "current": current,
                                                            "result": found,
                                                            "verified_after_ambiguous": True,
                                                        }
                                                    )
                                                    remaining = max(
                                                        0.0, remaining - actual_tp_qty
                                                    )
                                                    await persist(
                                                        "pending_limit",
                                                        f"TP{idx} verified on BingX after ambiguous write; no duplicate created",
                                                    )
                                                    await _notify(
                                                        notify,
                                                        user_id,
                                                        f"✅ LIMIT catch-up {symbol}\nTP{idx} был под вопросом, но бот нашёл ордер на BingX и не создал дубль.\n{_fmt_actions(actions)}",
                                                    )
                                                    continue
                                            typ = (
                                                "tp_write_ambiguous"
                                                if operation == "tp_order_create"
                                                else "tp_market_close_ambiguous"
                                            )
                                            actions.append(
                                                {
                                                    "type": typ,
                                                    "tp_index": idx,
                                                    "tp": tp,
                                                    "qty": planned_qty,
                                                    "current": current,
                                                    "error": f"{type(tp_exc).__name__}: {tp_exc}",
                                                }
                                            )
                                            await persist(
                                                "partial_error",
                                                f"LIMIT TP catch-up ambiguous write on TP{idx}: {type(tp_exc).__name__}: {tp_exc}",
                                            )
                                            await _notify(
                                                notify,
                                                user_id,
                                                f"⚠️ LIMIT catch-up под вопросом {symbol}\n"
                                                f"TP{idx}: биржа вернула временную ошибку во время действия с ордером.\n"
                                                f"Бот проверил open orders/algo orders и не нашёл подтверждённый TP. Авто-дубль не создаю, чтобы не закрыть лишний объём. Проверь ордера вручную.\n"
                                                f"{_fmt_actions(actions)}",
                                            )
                                        else:
                                            attempts = await persist_transient(
                                                f"tp{idx}",
                                                tp_exc,
                                                has_side_effects=has_side_effects,
                                            )
                                            if should_notify_transient(
                                                attempts,
                                                every=get_settings().EXCHANGE_TRANSIENT_ERROR_NOTIFY_EVERY,
                                            ):
                                                await _notify(
                                                    notify,
                                                    user_id,
                                                    f"⏳ Временная ошибка BingX при LIMIT catch-up {symbol}\n"
                                                    f"TP{idx}: {type(tp_exc).__name__}: {str(tp_exc)[:300]}\n"
                                                    f"Попытка: {attempts}. Бот повторит позже.\n"
                                                    f"{_fmt_actions(actions)}",
                                                )
                                    else:
                                        actions.append(
                                            {
                                                "type": "partial_error",
                                                "tp_index": idx,
                                                "error": f"{type(tp_exc).__name__}: {tp_exc}",
                                            }
                                        )
                                        await persist(
                                            "partial_error",
                                            f"LIMIT TP catch-up partially completed, failed on TP{idx}: {type(tp_exc).__name__}: {tp_exc}",
                                        )
                                        await _notify(
                                            notify,
                                            user_id,
                                            f"⚠️ LIMIT catch-up частично выполнен {symbol}\n"
                                            f"Ошибка на TP{idx}: {type(tp_exc).__name__}: {str(tp_exc)[:300]}\n"
                                            f"Уже сделано:\n{_fmt_actions(actions)}",
                                        )
                                    processed += 1
                                    break
                        else:
                            if not actions:
                                actions.append({"type": "no_post_fill_action"})
                            await persist(
                                "protected",
                                "LIMIT filled: TP post-fill protection/catch-up completed",
                            )
                            policy_for_notice = read_policy(
                                ids_payload,
                                fallback_targets=targets,
                                fallback_pcts=pcts,
                                fallback_qty_step=snapshot.get("qty_step"),
                                fallback_min_rr=float(snapshot.get("min_rr") or 0.0),
                            )
                            requested_target_count = len(
                                list(policy_for_notice.get("targets") or targets)
                            )
                            reduction_warning = _tp_plan_reduction_warning(
                                snapshot=snapshot,
                                requested_count=requested_target_count,
                                actual_qty=actual_qty,
                            )
                            protection_facts = [
                                "✅ STOP подтверждён",
                                "✅ TP установлены или выполнены catch-up",
                                "🔒 Позиция защищена",
                            ]
                            if reduction_warning:
                                protection_facts.append(reduction_warning)
                            await _notify(
                                notify,
                                user_id,
                                card(
                                    "🟢 <b>LIMIT ИСПОЛНЕН</b>",
                                    symbol=symbol,
                                    side=side,
                                    blocks=(
                                        [
                                            f"💵 <b>Фактический вход:</b> {fmt_price(ids_payload.get('actual_entry')or row.get('entry'))}",
                                            f"📦 <b>Исполненный объём:</b> {fmt_qty(actual_qty)}",
                                            f"🛡 <b>STOP:</b> {fmt_price(stop)}",
                                            f"🎯 <b>Целей в зафиксированном плане:</b> {len(plan_items)}",
                                        ],
                                        protection_facts,
                                        [_fmt_actions(actions)],
                                    ),
                                ),
                            )
                            await mark_catchup_summary_as_tp_notification()
                            processed += 1
                    except Exception as exc:
                        log.exception("Limit TP catch-up failed: %s", reason_prefix)
                        try:
                            has_side_effects = any(
                                a.get("type")
                                in {
                                    "tp_created_after_fill",
                                    "tp_catchup_market_close",
                                    "stop_catchup_market_close",
                                }
                                for a in actions
                            )
                            if is_transient_exchange_error(exc):
                                attempts = await persist_transient(
                                    "main", exc, has_side_effects=has_side_effects
                                )
                                if should_notify_transient(
                                    attempts,
                                    every=get_settings().EXCHANGE_TRANSIENT_ERROR_NOTIFY_EVERY,
                                ):
                                    await _notify(
                                        notify,
                                        user_id,
                                        f"⏳ Временная ошибка биржи при LIMIT catch-up {symbol}\n"
                                        f"{type(exc).__name__}: {str(exc)[:500]}\n"
                                        f"Попытка: {attempts}. Бот повторит позже.\n"
                                        f"{_fmt_actions(actions)}",
                                    )
                            else:
                                actions.append(
                                    {
                                        "type": "partial_error",
                                        "error": f"{type(exc).__name__}: {exc}",
                                    }
                                )
                                ids_payload["post_fill"] = actions
                                _, ledger_changed, repaired_indices = (
                                    canonicalize_tp_ledger(ids_payload)
                                )
                                if ledger_changed:
                                    ids_payload["tp_ledger_v1"] = (
                                        tp_ledger_repair_metadata(
                                            repaired_indices,
                                            source="limit_tp_catchup.exception",
                                        )
                                    )
                                await _write_status(
                                    "manual_required",
                                    (
                                        "LIMIT TP catch-up failed after an exchange order may already "
                                        f"exist: {type(exc).__name__}: {exc}"
                                    ),
                                    ids_payload,
                                )
                                await _notify(
                                    notify,
                                    user_id,
                                    f"❌ LIMIT catch-up ошибка {symbol}\n{type(exc).__name__}: {str(exc)[:500]}\n{_fmt_actions(actions)}",
                                )
                        except Exception:
                            pass
            return processed
        except StaleExecutionPass as stale:
            log.info(
                "%s: stale monitor batch stopped safely execution_id=%s expected=%s attempted=%s",
                stale.source,
                stale.execution_id,
                stale.expected_status,
                stale.attempted_status,
            )
            return processed
        finally:
            set_notification_event_key("")
            if owns_adapter_cache:
                for adapter in adapter_cache.values():
                    try:
                        await adapter.close()
                    except Exception:
                        pass


async def limit_tp_catchup_loop(notify: NotifyFn | None = None) -> None:
    settings = get_settings()
    interval = max(5, int(getattr(settings, "MONITOR_ACTIVE_INTERVAL_SEC", 15) or 15))
    while True:
        try:
            await process_pending_limit_tp_catchup_once(notify=notify)
        except Exception:
            log.exception("limit_tp_catchup_loop iteration failed")
        await asyncio.sleep(interval)
