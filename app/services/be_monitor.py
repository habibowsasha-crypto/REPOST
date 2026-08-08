from __future__ import annotations

import math
import asyncio
import re
import hashlib
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from typing import Any, Awaitable, Callable

from app.config import get_settings
from app.services.monitor_diagnostics import record_stage_rows
from app.services.async_utils import StaleExecutionPass, null_async_context
from app.services.ttl_cache import get_api_key_cache, get_user_settings_cache
from app.services.signal_executor import is_symbol_opening
from app.database import db
from app.services.exchange_factory import NetworkAmbiguousErrors, build_adapter
from app.services.positions_cache import get_global_positions_cache
from app.services.full_reconcile_locality import account_local_full_pass_rows
from app.services.notification_style import (
    card,
    details_line,
    fmt_price,
    fmt_qty,
)
from app.services.durable_notifications import (
    send_or_enqueue,
    set_notification_event_key,
)
from app.services.tp_execution_ledger import (
    canonicalize_tp_ledger,
    tp_ledger_repair_metadata,
    tp_row_has_unresolved_identity_conflict,
    tp_row_has_unresolved_qty_conflict,
)
from app.services.tp_qty import order_normalized_qty
from app.services.tp_plan_snapshot import (
    get_snapshot,
    rebase_snapshot_items_to_qty,
    snapshot_items,
    snapshot_total_qty,
)
from app.services.position_lifecycle_guard import (
    _LifecyclePositionsContext,
    _apply_history_fills,
    _cancel_exact_with_symbol,
    _execution_start_ms,
    _recover_exact_tp_child_identities,
    _saved_conditional_identity,
)
from app.services.stop_ownership import (
    LIMIT_ATTACHED_STOP_KEY,
    algo_order_id as strict_algo_order_id,
    clean_exchange_id,
    build_initial_stop_record,
    build_limit_attached_stop_record,
    identify_initial_protective_stop,
    identify_limit_attached_stop,
)

log = logging.getLogger(__name__)

# G63: a short post-start window allows the BE monitor to recover an exact TP
# fill directly from BingX history when Railway restarted after the fill and
# before the public-price event gate could observe the TP touch.  The recovery
# is read-only until the existing BE safety path independently verifies the
# position reduction and performs the normal STOP replacement sequence.
_PROCESS_STARTED_MONO = time.monotonic()
_RESTART_TP_HISTORY_RECOVERY_WINDOW_SEC = 180.0
_RESTART_TP_HISTORY_PAGE_SIZE = 100
_RESTART_TP_HISTORY_MAX_PAGES = 5


class BeCheckpointedStopVisibilityPending(RuntimeError):
    """A BE STOP was durably checkpointed but not visible in the current live read."""

    def __init__(self, *, stop_ids: set[str], stop_price: float) -> None:
        self.stop_ids = set(stop_ids)
        self.stop_price = float(stop_price)
        super().__init__(
            "checkpointed BE STOP is not currently visible; "
            "duplicate STOP creation was blocked"
        )


class BeExistingRecoveryBlocked(RuntimeError):
    """Expected fail-closed result for an unresolved existing-BE recovery.

    The exception carries bounded, non-secret diagnostics so the caller can
    persist an exact reason, topology fingerprint and progressive cooldown
    without falling through the generic replacement-error path.
    """

    def __init__(
        self,
        *,
        reason_code: str,
        diagnostics: dict[str, Any],
        topology_fingerprint: str,
        message: str = "existing BE recovery is not safely provable",
    ) -> None:
        self.reason_code = str(reason_code or "recovery_not_proven")
        self.diagnostics = dict(diagnostics or {})
        self.topology_fingerprint = str(topology_fingerprint or "")
        super().__init__(f"{message}: {self.reason_code}")


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


def _market_fallback_stop_ids(ids_payload: dict[str, Any]) -> set[str]:
    """Return exact fallback STOP ids created by MARKET post-fill protection.

    These ids are stronger evidence than generic ``candidate_ids`` because the
    bot received and read-back confirmed them from its own explicit fallback
    write.  They can therefore be cancelled exactly when a second matching
    protective STOP is already visible, without touching the unknown/attached
    order and without creating a protection gap.
    """

    result: set[str] = set()
    branch = (
        ids_payload.get("market_post_fill_stop_v1")
        if isinstance(ids_payload, dict)
        else None
    )
    if not isinstance(branch, dict):
        return result
    for value in (
        branch.get("fallback_stop_order_id"),
        branch.get("_confirmed_stop_plan_id"),
    ):
        cleaned = clean_exchange_id(value)
        if cleaned:
            result.add(cleaned)
    fallback = branch.get("fallback_response_v1")
    if isinstance(fallback, dict):
        for value in (
            fallback.get("_confirmed_stop_plan_id"),
            fallback.get("_confirmed_order_id"),
            fallback.get("stopPlanOrderId"),
            fallback.get("stopOrderId"),
            fallback.get("orderId"),
        ):
            cleaned = clean_exchange_id(value)
            if cleaned:
                result.add(cleaned)
    return result


class _LegacyDuplicateStopCleanupError(RuntimeError):
    def __init__(self, audit: dict[str, Any], cause: BaseException) -> None:
        self.audit = dict(audit or {})
        super().__init__(
            "legacy duplicate protective STOP cleanup was not proven; "
            "new BE STOP was not created"
        )
        self.__cause__ = cause


def _matching_initial_stop_ids(
    rows: list[dict[str, Any]],
    *,
    side: str,
    position_id: str,
    original_stop: float,
    minimum_qty: float,
    price_tolerance: float,
    qty_tolerance: float,
) -> set[str]:
    result: set[str] = set()
    wanted_side = str(side or "").upper()
    wanted_position = clean_exchange_id(position_id)
    for candidate in rows or []:
        if not isinstance(candidate, dict) or not _looks_stop_order(candidate):
            continue
        candidate_id = _algo_order_id(candidate)
        if not candidate_id:
            continue
        candidate_side = str(
            candidate.get("positionSide") or candidate.get("side") or ""
        ).upper()
        if candidate_side and wanted_side and candidate_side != wanted_side:
            continue
        candidate_position = clean_exchange_id(candidate.get("positionId"))
        if candidate_position and wanted_position and candidate_position != wanted_position:
            continue
        candidate_stop = _signed_f(
            candidate.get("stopLossPrice")
            or candidate.get("triggerPrice")
            or candidate.get("stopPrice"),
            0.0,
        )
        candidate_qty, candidate_qty_explicit = _order_confirmation_qty(candidate)
        if candidate_stop <= 0 or abs(candidate_stop - original_stop) > price_tolerance:
            continue
        if (
            candidate_qty_explicit
            and candidate_qty > 0
            and candidate_qty + qty_tolerance < minimum_qty
        ):
            continue
        result.add(candidate_id)
    return result


async def _collapse_legacy_market_duplicate_initial_stops(
    adapter: Any,
    *,
    rows: list[dict[str, Any]],
    ids_payload: dict[str, Any],
    symbol: str,
    side: str,
    position_id: str,
    original_stop: float,
    minimum_qty: float,
    price_tolerance: float,
    qty_tolerance: float,
    user_id: int,
    exchange: str,
    invalidate_reads: Callable[[Any, int, str, str], Awaitable[None]],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]], set[str], dict[str, Any] | None]:
    """Collapse only a provably bot-created legacy MARKET fallback duplicate.

    g12 could create an explicit fallback STOP before BingX exposed the attached
    MARKET STOP.  We cancel only the exact fallback id durably recorded by this
    execution, and only while a second full-coverage matching STOP is live.
    """

    fallback_stop_ids = _market_fallback_stop_ids(ids_payload)
    same_price_ids = _matching_initial_stop_ids(
        rows,
        side=side,
        position_id=position_id,
        original_stop=original_stop,
        minimum_qty=minimum_qty,
        price_tolerance=price_tolerance,
        qty_tolerance=qty_tolerance,
    )
    removable_ids = sorted(fallback_stop_ids.intersection(same_price_ids))
    retained_ids = sorted(same_price_ids - set(removable_ids))
    live_rows_by_id = {
        _algo_order_id(item): item
        for item in rows
        if isinstance(item, dict) and _algo_order_id(item)
    }
    if not removable_ids or not retained_ids:
        return rows, live_rows_by_id, set(), None

    audit: dict[str, Any] = {
        "type": "legacy_market_duplicate_stop_cleanup_v1",
        "duplicate_ids_before": sorted(same_price_ids),
        "exact_fallback_ids": removable_ids,
        "retained_ids_before": retained_ids,
        "original_stop": original_stop,
        "position_qty": minimum_qty,
    }
    try:
        audit["cancel_result"] = await _cancel_exact_with_symbol(
            adapter.cancel_conditional_orders_exact,
            removable_ids,
            symbol,
        )
        await invalidate_reads(adapter, user_id, exchange, symbol)
        refreshed_rows = [
            item
            for item in list(await adapter.fetch_open_algo_orders(symbol) or [])
            if isinstance(item, dict)
        ]
        remaining_ids = _matching_initial_stop_ids(
            refreshed_rows,
            side=side,
            position_id=position_id,
            original_stop=original_stop,
            minimum_qty=minimum_qty,
            price_tolerance=price_tolerance,
            qty_tolerance=qty_tolerance,
        )
        audit["remaining_ids"] = sorted(remaining_ids)
        audit["removed_ids_absent"] = not bool(set(removable_ids).intersection(remaining_ids))
        audit["protection_retained"] = bool(remaining_ids)
        if set(removable_ids).intersection(remaining_ids) or len(remaining_ids) != 1:
            raise RuntimeError(
                "legacy duplicate STOP cleanup did not leave exactly one full-coverage STOP"
            )
        refreshed_by_id = {
            _algo_order_id(item): item
            for item in refreshed_rows
            if _algo_order_id(item)
        }
        return refreshed_rows, refreshed_by_id, set(removable_ids), audit
    except Exception as exc:
        audit["error"] = f"{type(exc).__name__}: {exc}"
        raise _LegacyDuplicateStopCleanupError(audit, exc) from exc


def _resolve_be_trigger_index(
    ids_payload: dict[str, Any], user_settings: Any | None
) -> int:
    """Resolve the immutable per-trade BE trigger, with a legacy fallback."""
    saved = ids_payload.get("be_trigger_tp_index")
    if saved in (None, ""):
        raw = getattr(user_settings, "be_trigger_tp_index", 1) if user_settings else 1
    else:
        raw = saved
    if isinstance(raw, bool) or raw in (None, ""):
        return 0
    try:
        parsed = float(raw)
    except (TypeError, ValueError, OverflowError):
        return 0
    if not math.isfinite(parsed) or not parsed.is_integer():
        return 0
    value = int(parsed)
    return value if value in {1, 2, 3} else 0


def _json_float_list(value: Any) -> list[float]:
    try:
        parsed = value if isinstance(value, list) else json.loads(value or "[]")
        if not isinstance(parsed, list):
            return []
        return [float(x) for x in parsed]
    except (TypeError, ValueError, json.JSONDecodeError):
        return []


def _json_dict(value: Any) -> dict[str, Any]:
    try:
        parsed = value if isinstance(value, dict) else json.loads(value or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def _position_size(pos: dict[str, Any]) -> float:
    for key in ("size", "availableSize", "positionAmt", "qty", "total"):
        val = _f(pos.get(key), 0.0)
        if val > 0:
            return val
    return 0.0


def _total_position_size(positions: list[dict[str, Any]]) -> float:
    return sum(_position_size(p) for p in positions)


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
            if val not in (None, "") and not isinstance(val, bool):
                price = float(val)
                if math.isfinite(price) and price > 0:
                    return price
        except Exception:
            pass
    return 0.0


def _position_id(positions: list[dict[str, Any]]) -> str:
    for pos in positions or []:
        value = pos.get("positionId") if isinstance(pos, dict) else None
        cleaned = clean_exchange_id(value)
        if cleaned:
            return cleaned
    return ""


def _order_matches_position(order: dict[str, Any], position_id: str, side: str) -> bool:
    if not isinstance(order, dict):
        return False
    row_pid = clean_exchange_id(order.get("positionId"))
    wanted_pid = clean_exchange_id(position_id)
    if position_id and not wanted_pid:
        return False
    if wanted_pid:
        # Exact replacement must never infer ownership from side alone. Two
        # same-symbol positions/orders can coexist in hedge/manual scenarios;
        # a missing or malformed positionId is ambiguous and must fail closed.
        if not row_pid or row_pid != wanted_pid:
            return False
            # positionId is the primary identity. When BingX also supplies an
            # explicit positionSide, a contradictory side is corrupted/ambiguous
            # evidence and must not confirm a protective STOP. Missing side remains
            # compatible with legacy rows because the exact positionId is present.
        side_l = str(side or "").strip().lower()
        if side_l in {"long", "short"}:
            wanted = "LONG" if side_l == "long" else "SHORT"
            row_position_side = str(order.get("positionSide") or "").upper()
            if row_position_side and row_position_side != wanted:
                return False
        return True
    side_l = str(side or "").strip().lower()
    if side_l not in {"long", "short"}:
        return False
    wanted = "LONG" if side_l == "long" else "SHORT"
    row_side = str(order.get("positionSide") or order.get("side") or "").upper()
    return not row_side or row_side == wanted


def _be_final_verify_candidate_orders(
    orders: list[dict[str, Any]],
    *,
    position_id: str,
    side: str,
    expected_order_id: str = "",
) -> list[dict[str, Any]]:
    """Rows eligible for final BE verification after a replacement write.

    BingX can return the new STOP with exact ``stopPlanOrderId`` but without
    ``positionId`` in ``openOrders``.  A pre-filter by position would discard the
    very row that the strict exact-id matcher is designed to validate, creating
    a false "BE not confirmed" while diagnostics show an exact match.

    This helper keeps normal position-scoped rows plus the exact expected
    replacement id.  It does not adopt unknown same-side orders; the downstream
    matcher still requires STOP-like shape, exact price, qty/missing-qty rules
    and non-contradictory side evidence.
    """

    expected_id = clean_exchange_id(expected_order_id)
    result: list[dict[str, Any]] = []
    seen: set[int] = set()
    for order in orders or []:
        if not isinstance(order, dict):
            continue
        include = _order_matches_position(order, position_id, side)
        if not include and expected_id and _algo_order_id(order) == expected_id:
            include = True
        if include:
            marker = id(order)
            if marker not in seen:
                result.append(order)
                seen.add(marker)
    return result


async def _wait_position_conditionals_cleared(
    adapter: Any,
    *,
    symbol: str,
    position_id: str,
    side: str,
    attempts: int = 6,
    delay_sec: float = 0.35,
) -> tuple[bool, list[dict[str, Any]], str]:
    """Wait until BingX no longer reports TP/SL orders for one position."""
    last_relevant: list[dict[str, Any]] = []
    last_error = ""
    for attempt in range(max(1, int(attempts))):
        try:
            rows = list(await adapter.fetch_open_algo_orders(symbol) or [])
            last_relevant = [
                row
                for row in rows
                if isinstance(row, dict)
                and _order_matches_position(row, position_id, side)
            ]
            if not last_relevant:
                return True, [], ""
            last_error = ""
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        if attempt + 1 < max(1, int(attempts)):
            await asyncio.sleep(max(0.05, float(delay_sec)))
    return False, last_relevant, last_error


def _algo_order_id(order: dict[str, Any]) -> str:
    """Return a strict exact BingX stop-plan id required by stoporder/cancel."""

    return strict_algo_order_id(order)


async def _wait_exact_conditionals_replaced(
    adapter: Any,
    *,
    symbol: str,
    position_id: str,
    side: str,
    old_order_ids: set[str],
    stop_price: float,
    qty: float,
    price_tolerance: float,
    qty_tolerance: float,
    expected_new_order_id: str = "",
    excluded_new_order_ids: set[str] | None = None,
    require_unique_new_stop: bool = False,
    require_only_expected_stop: bool = False,
    expected_tp_fingerprint: str = "",
    tracked_tp_order_ids: set[str] | None = None,
    enforce_tp_unchanged: bool = False,
    attempts: int = 12,
    delay_sec: float = 0.30,
) -> tuple[
    bool,
    dict[str, Any] | None,
    list[dict[str, Any]],
    str,
    dict[str, Any],
]:
    """Confirm every old protective STOP id is gone while the new BE STOP remains."""

    last_remaining_old: list[dict[str, Any]] = []
    last_error = ""
    last_stop: dict[str, Any] | None = None
    last_diagnostics: dict[str, Any] = {}
    for attempt in range(max(1, int(attempts))):
        try:
            rows = [
                row
                for row in list(await adapter.fetch_open_algo_orders(symbol) or [])
                if isinstance(row, dict)
            ]
            all_live_ids = {_algo_order_id(row) for row in rows}
            all_live_ids.discard("")
            (
                live_topology_fingerprint,
                live_topology_snapshot,
            ) = _recovery_topology_fingerprint(
                rows,
                symbol=symbol,
                position_id=position_id,
                side=side,
            )
            last_remaining_old = [
                row for row in rows if _algo_order_id(row) in old_order_ids
            ]
            last_stop = _matching_stop_order(
                rows,
                position_id=position_id,
                side=side,
                stop_price=stop_price,
                qty=qty,
                price_tolerance=price_tolerance,
                qty_tolerance=qty_tolerance,
                expected_order_id=expected_new_order_id,
                excluded_order_ids=excluded_new_order_ids,
                require_unique=require_unique_new_stop,
            )
            last_diagnostics = _stop_confirmation_diagnostics(
                rows,
                position_id=position_id,
                side=side,
                stop_price=stop_price,
                qty=qty,
                price_tolerance=price_tolerance,
                qty_tolerance=qty_tolerance,
                expected_order_id=expected_new_order_id,
                old_order_ids=old_order_ids,
            )
            last_diagnostics["topology_fingerprint"] = live_topology_fingerprint
            last_diagnostics["topology_snapshot"] = live_topology_snapshot
            excluded_ids = {
                clean_exchange_id(value) for value in (excluded_new_order_ids or set())
            }
            excluded_ids.discard("")
            exact_new_candidates = _matching_stop_candidates(
                rows,
                position_id=position_id,
                side=side,
                stop_price=stop_price,
                qty=qty,
                price_tolerance=price_tolerance,
                qty_tolerance=qty_tolerance,
                expected_order_id=expected_new_order_id,
                excluded_order_ids=excluded_ids,
            )
            last_diagnostics["excluded_pre_write_ids"] = sorted(excluded_ids)
            last_diagnostics["exact_new_candidate_ids"] = [
                _algo_order_id(item) for item in exact_new_candidates
            ]
            last_diagnostics["unique_new_candidate_required"] = bool(
                require_unique_new_stop
            )
            strict_topology_ok = True
            if enforce_tp_unchanged:
                current_tp_fingerprint, current_tp_snapshot = _tp_topology_fingerprint(
                    rows,
                    position_id=position_id,
                    side=side,
                    tracked_tp_order_ids=set(tracked_tp_order_ids or set()),
                )
                last_diagnostics["tp_fingerprint_expected"] = str(
                    expected_tp_fingerprint or ""
                )
                last_diagnostics["tp_fingerprint_actual"] = current_tp_fingerprint
                last_diagnostics["tp_snapshot_actual"] = current_tp_snapshot
                tp_unchanged = bool(
                    expected_tp_fingerprint
                    and current_tp_fingerprint == expected_tp_fingerprint
                )
                last_diagnostics["tp_unchanged"] = tp_unchanged
                if not tp_unchanged:
                    strict_topology_ok = False
                    last_diagnostics["reason"] = "tp_topology_changed_during_cleanup"
            if require_only_expected_stop:
                strict_expected_id = clean_exchange_id(
                    expected_new_order_id or _algo_order_id(last_stop or {})
                )
                _, _, strict_topology = _strict_exact_be_cleanup_topology(
                    rows,
                    position_id=position_id,
                    side=side,
                    expected_replacement_id=strict_expected_id,
                    owned_old_order_ids=old_order_ids,
                    stop_price=stop_price,
                    qty=qty,
                    price_tolerance=price_tolerance,
                    qty_tolerance=qty_tolerance,
                    require_old_absent=True,
                )
                last_diagnostics["strict_cleanup_topology"] = strict_topology
                strict_topology_ok = bool(
                    strict_topology_ok and strict_topology.get("confirmed")
                )
            if (
                not old_order_ids.intersection(all_live_ids)
                and last_stop is not None
                and strict_topology_ok
            ):
                return True, last_stop, [], "", last_diagnostics
            last_error = ""
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        if attempt + 1 < max(1, int(attempts)):
            await asyncio.sleep(
                min(1.5, max(0.05, float(delay_sec)) * float(attempt + 1))
            )
    return False, last_stop, last_remaining_old, last_error, last_diagnostics


_ORDER_QTY_KEYS = ("qty", "size", "origQty", "quantity", "vol", "amount", "volume")


def _order_confirmation_qty(order: dict[str, Any]) -> tuple[float, bool]:
    """Return (qty, explicit) without treating normalized zero as evidence.

    BingX openOrders can return live STOP rows without a quantity field.  The
    adapter then normalizes missing quantity aliases to 0.0, which must not be
    confused with an explicit zero-quantity STOP.  When raw exchange payload is
    present, raw fields are the source of truth for whether qty was supplied.
    """

    if not isinstance(order, dict):
        return 0.0, False
    raw = order.get("raw") if isinstance(order.get("raw"), dict) else {}
    sources = [raw] if raw else [order]
    for source in sources:
        for key in _ORDER_QTY_KEYS:
            if key not in source:
                continue
            value = source.get(key)
            if value in (None, "") or isinstance(value, bool):
                continue
            return _signed_f(value, 0.0), True
    return 0.0, False


def _matching_stop_candidates(
    orders: list[dict[str, Any]],
    *,
    position_id: str,
    side: str,
    stop_price: float,
    qty: float,
    price_tolerance: float,
    qty_tolerance: float,
    expected_order_id: str = "",
    excluded_order_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Return every exact live STOP candidate after strict identity checks."""

    expected_id = clean_exchange_id(expected_order_id)
    excluded_ids = {clean_exchange_id(value) for value in (excluded_order_ids or set())}
    excluded_ids.discard("")
    matches: list[dict[str, Any]] = []
    for order in orders or []:
        if not isinstance(order, dict) or not _looks_stop_order(order):
            continue
        order_id = _algo_order_id(order)
        if not order_id or order_id in excluded_ids:
            continue
        if expected_id and order_id != expected_id:
            continue
        if not _order_matches_position(order, position_id, side):
            wanted_pid = clean_exchange_id(position_id)
            row_pid = clean_exchange_id(order.get("positionId"))
            row_side = str(order.get("positionSide") or order.get("side") or "").upper()
            wanted_side = str(side or "").upper()
            if row_pid and wanted_pid and row_pid != wanted_pid:
                continue
            if row_side and wanted_side and row_side != wanted_side:
                continue
            # BingX can omit positionId on STOP rows. Accept that only when the
            # exact plan id was just returned by our write, or when reconciling a
            # unique post-write id that was absent from the durable pre-write
            # snapshot. Never infer ownership from side alone.
            if not (
                (expected_id and order_id == expected_id)
                or (excluded_ids and order_id and order_id not in excluded_ids and not row_pid)
            ):
                continue
        try:
            raw_stop = (
                order.get("stopLossPrice")
                or order.get("triggerPrice")
                or order.get("stopPrice")
                or 0.0
            )
            if isinstance(raw_stop, bool):
                continue
            order_stop = float(raw_stop)
        except (TypeError, ValueError, OverflowError):
            continue
        order_qty, qty_explicit = _order_confirmation_qty(order)
        if not math.isfinite(order_stop) or not math.isfinite(order_qty):
            continue
        stop_matches = (
            order_stop > 0
            and abs(order_stop - float(stop_price)) <= price_tolerance
        )
        qty_matches = order_qty > 0 and order_qty + qty_tolerance >= float(qty)
        missing_qty_exact_write = (
            bool(expected_id)
            and order_id == expected_id
            and not qty_explicit
            and stop_matches
        )
        if stop_matches and (qty_matches or missing_qty_exact_write):
            if missing_qty_exact_write:
                order = dict(order)
                order["_be_confirmation_mode"] = "exact_id_price_missing_qty"
            matches.append(order)
    return matches


def _matching_stop_order(
    orders: list[dict[str, Any]],
    *,
    position_id: str,
    side: str,
    stop_price: float,
    qty: float,
    price_tolerance: float,
    qty_tolerance: float,
    expected_order_id: str = "",
    excluded_order_ids: set[str] | None = None,
    require_unique: bool = False,
) -> dict[str, Any] | None:
    matches = _matching_stop_candidates(
        orders,
        position_id=position_id,
        side=side,
        stop_price=stop_price,
        qty=qty,
        price_tolerance=price_tolerance,
        qty_tolerance=qty_tolerance,
        expected_order_id=expected_order_id,
        excluded_order_ids=excluded_order_ids,
    )
    if require_unique and len(matches) != 1:
        return None
    return matches[0] if matches else None




def _order_client_ids(order: dict[str, Any]) -> set[str]:
    """Return exact client-order identities from normalized and raw rows."""

    if not isinstance(order, dict):
        return set()
    raw = order.get("raw") if isinstance(order.get("raw"), dict) else {}
    return {
        cleaned
        for cleaned in (
            clean_exchange_id(order.get("clientOrderID")),
            clean_exchange_id(order.get("clientOrderId")),
            clean_exchange_id(order.get("client_order_id")),
            clean_exchange_id(raw.get("clientOrderID")),
            clean_exchange_id(raw.get("clientOrderId")),
            clean_exchange_id(raw.get("client_order_id")),
        )
        if cleaned
    }


def _stable_scalar(value: Any) -> str:
    """Return a deterministic scalar representation for durable fingerprints."""

    if value in (None, "") or isinstance(value, bool):
        return ""
    try:
        number = Decimal(str(value))
        if not number.is_finite():
            return ""
        normalized = number.normalize()
        return format(normalized, "f")
    except (InvalidOperation, TypeError, ValueError):
        return str(value).strip()


def _recovery_topology_snapshot(
    orders: list[dict[str, Any]],
    *,
    symbol: str,
    position_id: str,
    side: str,
) -> list[dict[str, str]]:
    """Build a stable, non-secret snapshot of every live STOP on the symbol.

    The snapshot intentionally includes foreign/unscoped STOPs because their
    appearance or disappearance must change the fingerprint and wake a later
    exact recovery attempt. API credentials and raw response payloads are never
    included.
    """

    wanted_pid = clean_exchange_id(position_id)
    wanted_side = str(side or "").strip().upper()
    snapshot: list[dict[str, str]] = []
    for row in orders or []:
        if not isinstance(row, dict) or not _looks_stop_order(row):
            continue
        if not _order_is_live_candidate(row):
            continue
        order_id = _algo_order_id(row)
        raw_stop = (
            row.get("stopLossPrice")
            or row.get("triggerPrice")
            or row.get("stopPrice")
            or 0
        )
        order_qty, qty_explicit = _order_confirmation_qty(row)
        snapshot.append(
            {
                "symbol": str(symbol or "").upper(),
                "order_id": order_id,
                "position_id": clean_exchange_id(row.get("positionId")),
                "position_side": str(
                    row.get("positionSide") or row.get("side") or ""
                ).upper(),
                "wanted_position_id": wanted_pid,
                "wanted_side": wanted_side,
                "type": str(
                    row.get("type")
                    or row.get("orderType")
                    or row.get("algoType")
                    or ""
                ).upper(),
                "stop": _stable_scalar(raw_stop),
                "qty": _stable_scalar(order_qty) if qty_explicit else "missing",
                "reduce_only": str(
                    row.get("reduceOnly")
                    if row.get("reduceOnly") is not None
                    else (row.get("raw") or {}).get("reduceOnly")
                    if isinstance(row.get("raw"), dict)
                    else ""
                ).lower(),
                "close_position": str(
                    row.get("closePosition")
                    if row.get("closePosition") is not None
                    else (row.get("raw") or {}).get("closePosition")
                    if isinstance(row.get("raw"), dict)
                    else ""
                ).lower(),
                "status": str(
                    row.get("status")
                    or row.get("state_name")
                    or row.get("orderStatus")
                    or ""
                ).upper(),
            }
        )
    return sorted(
        snapshot,
        key=lambda item: (
            item["order_id"],
            item["position_id"],
            item["position_side"],
            item["stop"],
            item["qty"],
            item["type"],
        ),
    )


def _recovery_topology_fingerprint(
    orders: list[dict[str, Any]],
    *,
    symbol: str,
    position_id: str,
    side: str,
) -> tuple[str, list[dict[str, str]]]:
    snapshot = _recovery_topology_snapshot(
        orders,
        symbol=symbol,
        position_id=position_id,
        side=side,
    )
    encoded = json.dumps(snapshot, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest(), snapshot


def _tp_topology_fingerprint(
    orders: list[dict[str, Any]],
    *,
    position_id: str,
    side: str,
    tracked_tp_order_ids: set[str],
) -> tuple[str, list[dict[str, str]]]:
    """Fingerprint live TP identity/price/qty without an extra exchange read."""

    tracked_ids = {clean_exchange_id(value) for value in tracked_tp_order_ids}
    tracked_ids.discard("")
    snapshot: list[dict[str, str]] = []
    for row in orders or []:
        if not isinstance(row, dict) or _looks_stop_order(row):
            continue
        if not _order_is_live_candidate(row):
            continue
        order_id = _algo_order_id(row)
        if not (
            (order_id and order_id in tracked_ids)
            or _order_matches_position(row, position_id, side)
        ):
            continue
        raw_price = (
            row.get("takeProfitPrice")
            or row.get("triggerPrice")
            or row.get("stopPrice")
            or row.get("price")
            or 0
        )
        order_qty, qty_explicit = _order_confirmation_qty(row)
        snapshot.append(
            {
                "order_id": order_id,
                "position_id": clean_exchange_id(row.get("positionId")),
                "position_side": str(
                    row.get("positionSide") or row.get("side") or ""
                ).upper(),
                "type": str(
                    row.get("type")
                    or row.get("orderType")
                    or row.get("algoType")
                    or ""
                ).upper(),
                "price": _stable_scalar(raw_price),
                "qty": _stable_scalar(order_qty) if qty_explicit else "missing",
                "status": str(
                    row.get("status")
                    or row.get("state_name")
                    or row.get("orderStatus")
                    or ""
                ).upper(),
            }
        )
    snapshot.sort(
        key=lambda item: (
            item["order_id"],
            item["position_id"],
            item["position_side"],
            item["price"],
            item["qty"],
            item["type"],
        )
    )
    encoded = json.dumps(snapshot, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest(), snapshot


def _canonical_recovery_reason(diagnostics: dict[str, Any]) -> str:
    raw = str((diagnostics or {}).get("reason") or "")
    mapping = {
        "missing_write_intent": "missing_durable_replacement_identity",
        "missing_pre_write_or_old_stop_snapshot": "missing_durable_replacement_identity",
        "empty_pre_write_or_old_stop_snapshot": "missing_durable_replacement_identity",
        "missing_exact_replacement_ownership": "missing_durable_replacement_identity",
        "invalid_position_identity": "position_id_conflict",
        "write_intent_position_identity_mismatch": "position_id_conflict",
        "write_intent_stop_mismatch": "replacement_price_mismatch",
        "write_intent_qty_under_current_position": "replacement_qty_under_position",
        "write_intent_qty_over_current_position": "replacement_qty_over_position",
        "intent_old_stop_not_exactly_owned": "old_stop_not_exactly_owned",
        "intent_old_stop_not_live": "old_stop_not_live",
        "idless_same_side_stop_present": "idless_or_unscoped_stop",
        "idless_or_unscoped_stop_present": "idless_or_unscoped_stop",
        "no_exact_owned_existing_be_candidate": "replacement_id_not_live",
        "expected_replacement_stop_not_confirmed": "replacement_id_not_live",
        "multiple_exact_owned_existing_be_candidates": "multiple_replacement_candidates",
        "unexpected_same_side_stop_topology": "unexpected_same_side_stop",
        "contradictory_stop_identity": "position_id_conflict",
        "old_stop_still_live": "old_stop_still_live_after_cancel",
        "tp_topology_changed_during_cleanup": "tp_topology_changed_during_cleanup",
    }
    return mapping.get(raw, raw or "recovery_not_proven")


def _recovery_backoff_state(
    be_state: dict[str, Any],
    *,
    reason_code: str,
    topology_fingerprint: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return durable progressive cooldown state for an unchanged topology."""

    current = now or _utc_now()
    prior = (
        be_state.get("existing_be_recovery_blocked_v1")
        if isinstance(be_state, dict)
        and isinstance(be_state.get("existing_be_recovery_blocked_v1"), dict)
        else {}
    )
    same_topology = bool(
        topology_fingerprint
        and topology_fingerprint == str(prior.get("topology_fingerprint") or "")
    )
    rate_limit_pending = _rate_limit_write_confirmation_pending(be_state)
    if same_topology:
        attempt = int(prior.get("same_topology_attempt") or 0) + 1
    elif rate_limit_pending:
        marker = be_state.get("rate_limit_write_confirmation_v1") or {}
        attempt = max(2, int(marker.get("readback_attempt") or 1) + 1)
    else:
        attempt = 1
    schedule = (
        _BE_RATE_LIMIT_CONFIRMATION_BACKOFF_SEC
        if rate_limit_pending
        else (300, 900, 1800, 7200)
    )
    backoff_sec = schedule[min(attempt - 1, len(schedule) - 1)]
    return {
        "version": 1,
        "reason": str(reason_code or "recovery_not_proven"),
        "topology_fingerprint": topology_fingerprint,
        "same_topology_attempt": attempt,
        "backoff_sec": backoff_sec,
        "checked_at": _iso_utc(current),
        "next_retry_after": _iso_utc(current + timedelta(seconds=backoff_sec)),
        "topology_changed": not same_topology,
    }


def _recovery_notification_cooldown_sec(reason_code: str) -> int:
    # A replacement STOP that is smaller than the live remainder is a direct
    # protection mismatch.  Keep the fail-closed behavior, but remind hourly
    # rather than allowing the condition to stay silent for twelve hours.
    if str(reason_code or "") in {
        "replacement_qty_under_position",
        "replacement_qty_over_position",
    }:
        return 3600
    return 12 * 3600


def _recovery_notification_due(
    be_state: dict[str, Any],
    *,
    reason_code: str,
    topology_fingerprint: str,
    now: datetime | None = None,
) -> bool:
    current = now or _utc_now()
    prior_reason = str(be_state.get("existing_be_recovery_last_notified_reason") or "")
    prior_fingerprint = str(
        be_state.get("existing_be_recovery_last_notified_topology_fingerprint") or ""
    )
    prior_at = _parse_iso_utc(be_state.get("existing_be_recovery_last_notified_at"))
    if prior_reason != reason_code or prior_fingerprint != topology_fingerprint:
        return True
    return prior_at is None or (current - prior_at).total_seconds() >= (
        _recovery_notification_cooldown_sec(reason_code)
    )


def _durable_exact_be_replacement_ids(be_state: dict[str, Any]) -> set[str]:
    """Collect only exact replacement STOP ids written by bot-owned BE paths.

    Generic ids found elsewhere in the execution payload are deliberately not
    accepted.  Each source below is either the durable replacement checkpoint,
    the exact cleanup checkpoint, or a whitelisted BE action that records an id
    returned/confirmed by the bot's own STOP write flow.
    """

    if not isinstance(be_state, dict):
        return set()
    out: set[str] = set()

    def add(value: Any) -> None:
        cleaned = clean_exchange_id(value)
        if cleaned:
            out.add(cleaned)

    add(be_state.get("replacement_stop_id"))
    add(be_state.get("verify_matching_stop_order_id"))
    recovery_owned_ids = be_state.get("existing_be_recovery_owned_replacement_ids")
    if isinstance(recovery_owned_ids, list):
        for value in recovery_owned_ids:
            add(value)
    replacement_stop = be_state.get("replacement_stop")
    if isinstance(replacement_stop, dict):
        add(_algo_order_id(replacement_stop))

    cleanup_intent = be_state.get("cleanup_cancel_intent_v1")
    if isinstance(cleanup_intent, dict):
        add(cleanup_intent.get("replacement_stop_id"))

    exact_action_fields: dict[str, tuple[str, ...]] = {
        "be_stop_exact_submitted_values": ("stop_plan_id",),
        "be_stop_write_reconciled_without_retry": ("replacement_stop_id",),
        "be_replacement_reconciled_after_unknown_write": ("replacement_stop_id",),
        "be_replacement_reused_after_checkpoint": ("replacement_stop_id",),
        "be_exact_replacement_confirmation": ("replacement_stop_id",),
        "be_exact_replacement_history_recheck": ("replacement_stop_id",),
        "old_conditionals_cancelled_exactly": ("replacement_stop_id",),
        "no_owned_old_conditionals_to_cancel": ("replacement_stop_id",),
    }
    actions = be_state.get("actions")
    if isinstance(actions, list):
        for action in actions:
            if not isinstance(action, dict):
                continue
            action_type = str(action.get("type") or "")
            for field in exact_action_fields.get(action_type, ()):
                add(action.get(field))
            if action_type == "be_stop_created_before_exact_cleanup":
                result = action.get("result")
                if isinstance(result, dict):
                    add(_algo_order_id(result))

    return out


def _recover_existing_be_stop_from_write_intent(
    orders: list[dict[str, Any]],
    *,
    write_intent: dict[str, Any],
    be_state: dict[str, Any],
    owned_old_order_ids: set[str],
    position_id: str,
    side: str,
    stop_price: float,
    qty: float,
    price_tolerance: float,
    qty_tolerance: float,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """Recover one exact bot-owned existing BE STOP without a third write.

    The candidate is never adopted from price/quantity/topology alone.  It must
    carry an exact id already present in a durable bot-owned BE checkpoint/action,
    or an exact clientOrderID equal to the durable write intent.  The exact old
    STOP ids must also remain bot-owned and live.  The complete same-side STOP
    topology must be exactly ``owned old ids + one exact-owned BE candidate``.
    Every uncertainty remains fail-closed/manual review.
    """

    diagnostics: dict[str, Any] = {
        "reason": "",
        "candidate_ids": [],
        "baseline_ids": [],
        "intent_old_stop_ids": [],
        "owned_old_stop_ids": [],
        "durable_exact_replacement_ids": [],
        "durable_client_id": "",
        "live_same_side_stop_ids": [],
        "idless_same_side_stop_count": 0,
    }
    if not isinstance(write_intent, dict):
        diagnostics["reason"] = "missing_write_intent"
        return None, diagnostics

    baseline_raw = write_intent.get("pre_write_live_ids")
    intent_old_raw = write_intent.get("old_stop_ids")
    if not isinstance(baseline_raw, list) or not isinstance(intent_old_raw, list):
        diagnostics["reason"] = "missing_pre_write_or_old_stop_snapshot"
        return None, diagnostics

    baseline_ids = {clean_exchange_id(value) for value in baseline_raw}
    baseline_ids.discard("")
    intent_old_ids = {clean_exchange_id(value) for value in intent_old_raw}
    intent_old_ids.discard("")
    owned_old_ids = {clean_exchange_id(value) for value in owned_old_order_ids}
    owned_old_ids.discard("")
    exact_replacement_ids = _durable_exact_be_replacement_ids(be_state)
    durable_client_id = clean_exchange_id(write_intent.get("client_id"))
    diagnostics["baseline_ids"] = sorted(baseline_ids)
    diagnostics["intent_old_stop_ids"] = sorted(intent_old_ids)
    diagnostics["owned_old_stop_ids"] = sorted(owned_old_ids)
    diagnostics["durable_exact_replacement_ids"] = sorted(exact_replacement_ids)
    diagnostics["durable_client_id"] = durable_client_id

    if not baseline_ids or not intent_old_ids:
        diagnostics["reason"] = "empty_pre_write_or_old_stop_snapshot"
        return None, diagnostics
    if not intent_old_ids.issubset(owned_old_ids):
        diagnostics["reason"] = "intent_old_stop_not_exactly_owned"
        return None, diagnostics

    wanted_pid = clean_exchange_id(position_id)
    wanted_side = str(side or "").strip().upper()
    intent_pid = clean_exchange_id(write_intent.get("position_id"))
    intent_side = str(write_intent.get("side") or "").strip().upper()
    intent_stop = _signed_f(write_intent.get("stop"), 0.0)
    intent_qty = _signed_f(write_intent.get("qty"), 0.0)
    if not wanted_pid or wanted_side not in {"LONG", "SHORT"}:
        diagnostics["reason"] = "invalid_position_identity"
        return None, diagnostics
    if intent_pid != wanted_pid or intent_side != wanted_side:
        diagnostics["reason"] = "write_intent_position_identity_mismatch"
        return None, diagnostics
    if intent_stop <= 0 or abs(intent_stop - float(stop_price)) > float(price_tolerance):
        diagnostics["reason"] = "write_intent_stop_mismatch"
        return None, diagnostics
    if intent_qty <= 0 or intent_qty + float(qty_tolerance) < float(qty):
        diagnostics["reason"] = "write_intent_qty_under_current_position"
        return None, diagnostics

    rows_by_id: dict[str, dict[str, Any]] = {}
    same_side_stop_ids: set[str] = set()
    idless_same_side = 0
    candidates: list[dict[str, Any]] = []
    candidate_ownership: dict[str, str] = {}

    for row in orders or []:
        if not isinstance(row, dict) or not _looks_stop_order(row):
            continue
        if not _order_is_live_candidate(row):
            continue
        row_side = str(row.get("positionSide") or row.get("side") or "").upper()
        if row_side != wanted_side:
            continue
        row_pid = clean_exchange_id(row.get("positionId"))
        if row_pid and row_pid != wanted_pid:
            continue
        order_id = _algo_order_id(row)
        if not order_id:
            idless_same_side += 1
            continue
        rows_by_id[order_id] = row
        same_side_stop_ids.add(order_id)

        if order_id not in baseline_ids or order_id in intent_old_ids:
            continue
        ownership_basis = ""
        if order_id in exact_replacement_ids:
            ownership_basis = "durable_exact_replacement_id"
        elif durable_client_id and durable_client_id in _order_client_ids(row):
            ownership_basis = "durable_exact_client_order_id"
        if not ownership_basis:
            continue
        raw_stop = (
            row.get("stopLossPrice")
            or row.get("triggerPrice")
            or row.get("stopPrice")
            or 0.0
        )
        order_stop = _signed_f(raw_stop, 0.0)
        order_qty, qty_explicit = _order_confirmation_qty(row)
        if not qty_explicit or order_qty <= 0:
            continue
        if abs(order_stop - float(stop_price)) > float(price_tolerance):
            continue
        if order_qty + float(qty_tolerance) < float(qty):
            continue
        candidates.append(row)
        candidate_ownership[order_id] = ownership_basis

    diagnostics["live_same_side_stop_ids"] = sorted(same_side_stop_ids)
    diagnostics["idless_same_side_stop_count"] = idless_same_side
    diagnostics["candidate_ids"] = sorted(
        {_algo_order_id(row) for row in candidates if _algo_order_id(row)}
    )
    diagnostics["candidate_ownership"] = dict(sorted(candidate_ownership.items()))

    if idless_same_side:
        diagnostics["reason"] = "idless_same_side_stop_present"
        return None, diagnostics

    live_intent_old_ids = {
        order_id
        for order_id in intent_old_ids
        if isinstance(rows_by_id.get(order_id), dict)
        and _looks_stop_order(rows_by_id[order_id])
    }
    diagnostics["live_intent_old_stop_ids"] = sorted(live_intent_old_ids)
    if live_intent_old_ids != intent_old_ids:
        diagnostics["reason"] = "intent_old_stop_not_live"
        return None, diagnostics
    if len(candidates) != 1:
        if not exact_replacement_ids and not durable_client_id:
            diagnostics["reason"] = "missing_exact_replacement_ownership"
        else:
            diagnostics["reason"] = (
                "no_exact_owned_existing_be_candidate"
                if not candidates
                else "multiple_exact_owned_existing_be_candidates"
            )
        return None, diagnostics

    candidate = dict(candidates[0])
    candidate_id = _algo_order_id(candidate)
    expected_topology = set(intent_old_ids) | {candidate_id}
    diagnostics["expected_same_side_stop_ids"] = sorted(expected_topology)
    if same_side_stop_ids != expected_topology:
        diagnostics["reason"] = "unexpected_same_side_stop_topology"
        diagnostics["unexpected_same_side_stop_ids"] = sorted(
            same_side_stop_ids - expected_topology
        )
        return None, diagnostics

    ownership_basis = candidate_ownership.get(candidate_id, "")
    candidate["_be_confirmation_mode"] = (
        f"existing_exact_owned_be_recovery:{ownership_basis}"
    )
    candidate["_be_recovery_from_existing_stop"] = True
    diagnostics["reason"] = "confirmed_unique_exact_owned_existing_be_with_exact_old_stops"
    diagnostics["replacement_stop_id"] = candidate_id
    diagnostics["replacement_ownership_basis"] = ownership_basis
    return candidate, diagnostics



def _admin_exact_cleanup_approved_old_ids(
    be_state: dict[str, Any], write_intent: dict[str, Any]
) -> set[str]:
    """Return only the exact old ids explicitly approved by a g7a admin token."""

    admin_intent = (
        be_state.get("admin_exact_cleanup_intent_v1")
        if isinstance(be_state, dict)
        and isinstance(be_state.get("admin_exact_cleanup_intent_v1"), dict)
        else {}
    )
    cleanup_intent = (
        be_state.get("cleanup_cancel_intent_v1")
        if isinstance(be_state, dict)
        and isinstance(be_state.get("cleanup_cancel_intent_v1"), dict)
        else {}
    )
    raw_selected = admin_intent.get("selected_old_stop_ids")
    raw_cleanup = cleanup_intent.get("order_ids")
    raw_expected = write_intent.get("old_stop_ids") if isinstance(write_intent, dict) else None
    if not all(isinstance(value, list) for value in (raw_selected, raw_cleanup, raw_expected)):
        return set()
    selected = {clean_exchange_id(value) for value in raw_selected}
    cleanup_ids = {clean_exchange_id(value) for value in raw_cleanup}
    expected = {clean_exchange_id(value) for value in raw_expected}
    selected.discard("")
    cleanup_ids.discard("")
    expected.discard("")
    replacement_id = clean_exchange_id(admin_intent.get("replacement_stop_id"))
    cleanup_replacement_id = clean_exchange_id(cleanup_intent.get("replacement_stop_id"))
    if (
        int(admin_intent.get("version") or 0) == 1
        and int(admin_intent.get("requested_by_admin_user_id") or 0) > 0
        and str(admin_intent.get("topology_fingerprint") or "")
        and str(admin_intent.get("tp_fingerprint_before") or "")
        and int(cleanup_intent.get("version") or 0) == 1
        and str(cleanup_intent.get("source") or "")
        == "admin_exact_cleanup_v1_0_7g7a"
        and str(cleanup_intent.get("dispatch_state") or "")
        in {"reserved_unknown", "write_error_or_unknown", "response_received"}
        and selected
        and selected == cleanup_ids == expected
        and replacement_id
        and replacement_id == cleanup_replacement_id
        and clean_exchange_id(admin_intent.get("position_id"))
        == clean_exchange_id(write_intent.get("position_id"))
        and str(admin_intent.get("side") or "").lower()
        == str(write_intent.get("side") or "").lower()
    ):
        return selected
    return set()


def _resume_admin_cleanup_after_cancel(
    orders: list[dict[str, Any]],
    *,
    be_state: dict[str, Any],
    write_intent: dict[str, Any],
    position_id: str,
    side: str,
    stop_price: float,
    qty: float,
    price_tolerance: float,
    qty_tolerance: float,
    tracked_tp_order_ids: set[str],
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """Resume a crash after admin exact cancel using read-only proof only."""

    approved_old_ids = _admin_exact_cleanup_approved_old_ids(be_state, write_intent)
    admin_intent = (
        be_state.get("admin_exact_cleanup_intent_v1")
        if isinstance(be_state.get("admin_exact_cleanup_intent_v1"), dict)
        else {}
    )
    cleanup_intent = (
        be_state.get("cleanup_cancel_intent_v1")
        if isinstance(be_state.get("cleanup_cancel_intent_v1"), dict)
        else {}
    )
    replacement_id = clean_exchange_id(cleanup_intent.get("replacement_stop_id"))
    diagnostics: dict[str, Any] = {
        "reason": "",
        "approved_old_stop_ids": sorted(approved_old_ids),
        "replacement_stop_id": replacement_id,
    }
    if not approved_old_ids or not replacement_id:
        diagnostics["reason"] = "missing_valid_admin_cleanup_intent"
        return None, diagnostics
    replacement, remaining_old, topology = _strict_exact_be_cleanup_topology(
        orders,
        position_id=position_id,
        side=side,
        expected_replacement_id=replacement_id,
        owned_old_order_ids=approved_old_ids,
        stop_price=stop_price,
        qty=qty,
        price_tolerance=price_tolerance,
        qty_tolerance=qty_tolerance,
        require_old_absent=True,
    )
    diagnostics["topology"] = topology
    diagnostics["remaining_old_stop_ids"] = sorted(remaining_old)
    expected_tp = str(admin_intent.get("tp_fingerprint_before") or "")
    if not expected_tp:
        diagnostics["reason"] = "missing_admin_tp_fingerprint"
        return None, diagnostics
    actual_tp, actual_tp_snapshot = _tp_topology_fingerprint(
        orders,
        position_id=position_id,
        side=side,
        tracked_tp_order_ids=tracked_tp_order_ids,
    )
    diagnostics["tp_fingerprint_expected"] = expected_tp
    diagnostics["tp_fingerprint_actual"] = actual_tp
    diagnostics["tp_snapshot_actual"] = actual_tp_snapshot
    if expected_tp and actual_tp != expected_tp:
        diagnostics["reason"] = "tp_topology_changed_during_cleanup"
        return None, diagnostics
    if replacement is None or not topology.get("confirmed") or remaining_old:
        diagnostics["reason"] = str(topology.get("reason") or "recovery_not_proven")
        return None, diagnostics
    diagnostics["reason"] = "admin_cleanup_cancel_already_confirmed"
    return dict(replacement), diagnostics


def _exact_old_stop_fallback_after_missing_replacement(
    orders: list[dict[str, Any]],
    *,
    write_intent: dict[str, Any],
    owned_old_order_ids: set[str],
    position_id: str,
    side: str,
    qty: float,
    qty_tolerance: float,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """Prove that a stale BE replacement checkpoint can be abandoned safely.

    This path is intentionally conservative.  It is accepted only when the
    durable replacement id is absent, exactly one live same-position STOP
    remains, that STOP is one of the exact bot-owned pre-write old ids, and it
    explicitly covers the full fresh position remainder.  The STOP is kept in
    place; no exchange write is performed.
    """

    diagnostics: dict[str, Any] = {
        "reason": "",
        "intent_old_stop_ids": [],
        "owned_old_stop_ids": [],
        "live_relevant_stop_ids": [],
        "idless_relevant_stop_count": 0,
        "unexpected_stop_ids": [],
    }
    if not isinstance(write_intent, dict):
        diagnostics["reason"] = "missing_write_intent"
        return None, diagnostics
    wanted_pid = clean_exchange_id(position_id)
    wanted_side = str(side or "").strip().upper()
    if not wanted_pid or wanted_side not in {"LONG", "SHORT"}:
        diagnostics["reason"] = "invalid_position_identity"
        return None, diagnostics

    raw_intent_old = write_intent.get("old_stop_ids")
    if not isinstance(raw_intent_old, list):
        diagnostics["reason"] = "missing_pre_write_or_old_stop_snapshot"
        return None, diagnostics
    intent_old_ids = {clean_exchange_id(value) for value in raw_intent_old}
    intent_old_ids.discard("")
    owned_old_ids = {clean_exchange_id(value) for value in owned_old_order_ids}
    owned_old_ids.discard("")
    exact_old_ids = intent_old_ids.intersection(owned_old_ids)
    diagnostics["intent_old_stop_ids"] = sorted(intent_old_ids)
    diagnostics["owned_old_stop_ids"] = sorted(owned_old_ids)
    if not intent_old_ids or exact_old_ids != intent_old_ids:
        diagnostics["reason"] = "old_stop_not_exactly_owned"
        return None, diagnostics

    relevant: list[dict[str, Any]] = []
    relevant_ids: set[str] = set()
    idless_count = 0
    for row in orders or []:
        if not isinstance(row, dict) or not _looks_stop_order(row):
            continue
        if not _order_is_live_candidate(row):
            continue
        row_pid = clean_exchange_id(row.get("positionId"))
        row_side = str(row.get("positionSide") or row.get("side") or "").upper()
        order_id = _algo_order_id(row)

        # An exact durable old id with contradictory identity is never accepted
        # as protection for the current execution.
        if order_id in intent_old_ids and row_pid != wanted_pid:
            diagnostics["reason"] = "position_id_conflict"
            diagnostics["conflicting_stop_id"] = order_id
            return None, diagnostics
        if order_id in intent_old_ids and row_side != wanted_side:
            diagnostics["reason"] = "side_conflict"
            diagnostics["conflicting_stop_id"] = order_id
            return None, diagnostics

        # Opposite hedge-side / other exact-position STOPs are outside this
        # execution. Any row that partially matches the target but lacks the
        # complete exact identity is an unscoped hazard and fails closed.
        touches_target = bool(row_pid == wanted_pid or row_side == wanted_side)
        if not touches_target:
            continue
        if row_pid != wanted_pid or row_side != wanted_side or not order_id:
            idless_count += 1
            continue
        relevant.append(row)
        relevant_ids.add(order_id)

    diagnostics["live_relevant_stop_ids"] = sorted(relevant_ids)
    diagnostics["idless_relevant_stop_count"] = idless_count
    diagnostics["unexpected_stop_ids"] = sorted(relevant_ids - intent_old_ids)
    if idless_count:
        diagnostics["reason"] = "idless_or_unscoped_stop_present"
        return None, diagnostics
    # Missing old STOP ids are not a hazard by themselves.  A stale checkpoint
    # may list several exact pre-write ids even though BingX now exposes only
    # one survivor.  It is safe to retire the checkpoint when that sole live
    # relevant STOP is still one of the durable bot-owned old ids, no unexpected
    # or id-less STOP exists, and the survivor covers the full fresh position.
    if len(relevant) != 1 or not relevant_ids.issubset(intent_old_ids):
        diagnostics["reason"] = (
            "protective_stop_missing_after_stale_replacement"
            if not relevant
            else "unexpected_same_side_stop"
        )
        return None, diagnostics
    diagnostics["missing_old_stop_ids"] = sorted(intent_old_ids - relevant_ids)

    stop_row = dict(relevant[0])
    stop_qty, qty_explicit = _order_confirmation_qty(stop_row)
    diagnostics["protective_stop_id"] = _algo_order_id(stop_row)
    diagnostics["protective_stop_qty"] = stop_qty if qty_explicit else None
    diagnostics["position_qty"] = float(qty)
    raw_row = stop_row.get("raw") if isinstance(stop_row.get("raw"), dict) else {}
    diagnostics["protective_stop_snapshot"] = {
        "order_id": _algo_order_id(stop_row),
        "position_id": clean_exchange_id(stop_row.get("positionId")),
        "position_side": str(
            stop_row.get("positionSide") or stop_row.get("side") or ""
        ).upper(),
        "qty": stop_qty if qty_explicit else None,
        "trigger_price": _stable_scalar(
            stop_row.get("stopLossPrice")
            or stop_row.get("triggerPrice")
            or stop_row.get("stopPrice")
            or 0
        ),
        "reduce_only": (
            stop_row.get("reduceOnly")
            if stop_row.get("reduceOnly") is not None
            else raw_row.get("reduceOnly")
        ),
        "close_position": (
            stop_row.get("closePosition")
            if stop_row.get("closePosition") is not None
            else raw_row.get("closePosition")
        ),
        "status": str(
            stop_row.get("status")
            or stop_row.get("state_name")
            or stop_row.get("orderStatus")
            or ""
        ).upper(),
    }
    if not qty_explicit or stop_qty <= 0 or stop_qty + float(qty_tolerance) < float(qty):
        diagnostics["reason"] = "old_stop_qty_under_position"
        return None, diagnostics

    diagnostics["reason"] = "confirmed_single_exact_old_stop_full_coverage"
    return stop_row, diagnostics

def _be_recovery_checkpoint_present(be_state: dict[str, Any]) -> bool:
    """Return True only for durable BE replacement/cleanup checkpoints.

    This predicate is intentionally independent from the user's current BE
    setting. Once an exchange replacement write may have happened, cleanup is
    a crash-recovery obligation rather than a new trading decision.
    """

    if not isinstance(be_state, dict) or be_state.get("moved") is True:
        return False
    if isinstance(be_state.get("replacement_write_intent_v1"), dict):
        return True
    if isinstance(be_state.get("cleanup_cancel_intent_v1"), dict):
        return True
    if be_state.get("replacement_in_progress") is True and clean_exchange_id(
        be_state.get("replacement_stop_id")
        or be_state.get("verify_matching_stop_order_id")
    ):
        return True
    return False


def _strict_exact_be_cleanup_topology(
    orders: list[dict[str, Any]],
    *,
    position_id: str,
    side: str,
    expected_replacement_id: str,
    owned_old_order_ids: set[str],
    stop_price: float,
    qty: float,
    price_tolerance: float,
    qty_tolerance: float,
    require_old_absent: bool,
) -> tuple[dict[str, Any] | None, set[str], dict[str, Any]]:
    """Prove the complete same-position STOP topology before/after cleanup.

    The exact replacement id must be present with the expected BE price and
    enough quantity. Every other relevant live STOP must be one of the exact
    durable old ids. Unknown, id-less or extra same-side STOPs fail closed.
    """

    wanted_pid = clean_exchange_id(position_id)
    wanted_side = str(side or "").strip().upper()
    replacement_id = clean_exchange_id(expected_replacement_id)
    old_ids = {clean_exchange_id(value) for value in owned_old_order_ids}
    old_ids.discard("")
    diagnostics: dict[str, Any] = {
        "confirmed": False,
        "expected_replacement_id": replacement_id,
        "owned_old_stop_ids": sorted(old_ids),
        "live_relevant_stop_ids": [],
        "live_old_stop_ids": [],
        "unexpected_stop_ids": [],
        "idless_relevant_stop_count": 0,
        "contradictory_identity_ids": [],
        "require_old_absent": bool(require_old_absent),
        "reason": "",
    }
    if not wanted_pid or wanted_side not in {"LONG", "SHORT"} or not replacement_id:
        diagnostics["reason"] = "invalid_expected_cleanup_identity"
        return None, set(), diagnostics

    relevant_rows: list[dict[str, Any]] = []
    relevant_ids: set[str] = set()
    contradictory_ids: set[str] = set()
    idless_count = 0
    for row in orders or []:
        if not isinstance(row, dict) or not _looks_stop_order(row):
            continue
        if not _order_is_live_candidate(row):
            continue
        order_id = _algo_order_id(row)
        row_pid = clean_exchange_id(row.get("positionId"))
        row_side = str(row.get("positionSide") or row.get("side") or "").upper()
        exact_checkpoint_id = bool(
            order_id and (order_id == replacement_id or order_id in old_ids)
        )
        position_conflict = bool(row_pid and row_pid != wanted_pid)
        side_conflict = bool(row_side and row_side != wanted_side)

        # A different exact position may share the symbol and is unrelated.
        # Exact checkpoint ids, however, may never disappear from the topology
        # merely because BingX returned contradictory side/position fields.
        if position_conflict and not exact_checkpoint_id:
            continue
        if side_conflict and not exact_checkpoint_id and row_pid != wanted_pid:
            continue
        if position_conflict or (side_conflict and (exact_checkpoint_id or row_pid == wanted_pid)):
            if order_id:
                contradictory_ids.add(order_id)
                relevant_ids.add(order_id)
            else:
                idless_count += 1
            relevant_rows.append(row)
            continue

        # Exact positionId, exact side, or an exact durable checkpoint id makes
        # a symbol-level STOP relevant. A completely unscoped STOP is ambiguous
        # and therefore blocks success.
        identity_relevant = bool(
            row_pid == wanted_pid
            or row_side == wanted_side
            or exact_checkpoint_id
        )
        if not identity_relevant:
            idless_count += 1
            continue
        relevant_rows.append(row)
        if not order_id:
            idless_count += 1
            continue
        relevant_ids.add(order_id)

    diagnostics["live_relevant_stop_ids"] = sorted(relevant_ids)
    diagnostics["contradictory_identity_ids"] = sorted(contradictory_ids)
    diagnostics["idless_relevant_stop_count"] = idless_count
    live_old_ids = relevant_ids.intersection(old_ids)
    diagnostics["live_old_stop_ids"] = sorted(live_old_ids)
    allowed_ids = old_ids | {replacement_id}
    unexpected_ids = relevant_ids - allowed_ids
    diagnostics["unexpected_stop_ids"] = sorted(unexpected_ids)

    replacement = _matching_stop_order(
        relevant_rows,
        position_id=position_id,
        side=side,
        stop_price=stop_price,
        qty=qty,
        price_tolerance=price_tolerance,
        qty_tolerance=qty_tolerance,
        expected_order_id=replacement_id,
    )
    if idless_count:
        diagnostics["reason"] = "idless_or_unscoped_stop_present"
        return None, live_old_ids, diagnostics
    if contradictory_ids:
        diagnostics["reason"] = "contradictory_stop_identity"
        return None, live_old_ids, diagnostics
    if unexpected_ids:
        diagnostics["reason"] = "unexpected_same_side_stop_topology"
        return None, live_old_ids, diagnostics
    if replacement is None:
        diagnostics["reason"] = "expected_replacement_stop_not_confirmed"
        return None, live_old_ids, diagnostics
    if require_old_absent and live_old_ids:
        diagnostics["reason"] = "old_stop_still_live"
        return replacement, live_old_ids, diagnostics

    diagnostics["confirmed"] = True
    diagnostics["reason"] = (
        "confirmed_replacement_only"
        if require_old_absent
        else "confirmed_replacement_with_only_exact_old_stops"
    )
    return replacement, live_old_ids, diagnostics


def _order_is_live_candidate(order: dict[str, Any]) -> bool:
    """Return True only when an exchange/history row is not terminal."""

    if not isinstance(order, dict):
        return False
    if bool(order.get("terminal")):
        return False
    state = _signed_f(order.get("state"), 0.0)
    if state in {3.0, 4.0, 5.0}:
        return False
    status_text = str(
        order.get("status")
        or order.get("state_name")
        or order.get("orderStatus")
        or ""
    ).strip().upper()
    terminal_words = {
        "FILLED",
        "FULLY_FILLED",
        "EXECUTED",
        "COMPLETED",
        "DONE",
        "CANCELED",
        "CANCELLED",
        "CANCEL",
        "REJECTED",
        "EXPIRED",
        "FAILED",
    }
    return status_text not in terminal_words


async def _find_checkpointed_stop_from_history(
    adapter: Any,
    *,
    symbol: str,
    stop_ids: set[str],
    position_id: str,
    side: str,
    stop_price: float,
    qty: float,
    price_tolerance: float,
    qty_tolerance: float,
) -> tuple[dict[str, Any] | None, str]:
    """Use BingX allOrders/history as a read-only fallback for a checkpointed STOP.

    openOrders can temporarily omit a live STOP plan that was already confirmed and
    durably checkpointed.  The fallback remains strict: it accepts only the exact
    checkpointed stop-plan id and exact BE price, and rejects terminal rows.
    """

    cleaned_ids = {clean_exchange_id(value) for value in stop_ids}
    cleaned_ids.discard("")
    if not cleaned_ids:
        return None, "no_checkpoint_ids"
    fetch_history = getattr(adapter, "fetch_position_tpsl_history", None)
    if not callable(fetch_history):
        return None, "history_endpoint_unavailable"
    try:
        rows = list(await fetch_history(symbol, page_size=100, max_pages=3) or [])
    except Exception as exc:
        return None, f"history_read_error:{type(exc).__name__}: {exc}"
    for stop_id in sorted(cleaned_ids):
        candidates = _matching_stop_candidates(
            [row for row in rows if isinstance(row, dict)],
            position_id=position_id,
            side=side,
            stop_price=stop_price,
            qty=qty,
            price_tolerance=price_tolerance,
            qty_tolerance=qty_tolerance,
            expected_order_id=stop_id,
        )
        for candidate in candidates:
            if not _order_is_live_candidate(candidate):
                continue
            candidate = dict(candidate)
            candidate["_be_confirmation_mode"] = "checkpoint_history_exact_id_price"
            return candidate, "confirmed_from_history"
    return None, f"not_found_in_history_rows:{len(rows)}"


def _stop_confirmation_diagnostics(
    orders: list[dict[str, Any]],
    *,
    position_id: str,
    side: str,
    stop_price: float,
    qty: float,
    price_tolerance: float,
    qty_tolerance: float,
    expected_order_id: str = "",
    old_order_ids: set[str] | None = None,
) -> dict[str, Any]:
    """Build bounded, non-secret diagnostics for failed STOP confirmation."""

    expected_id = clean_exchange_id(expected_order_id)
    wanted_pid = clean_exchange_id(position_id)
    candidates: list[dict[str, Any]] = []
    for order in [row for row in (orders or []) if isinstance(row, dict)][:20]:
        order_id = _algo_order_id(order)
        row_pid = clean_exchange_id(order.get("positionId"))
        row_side = str(order.get("positionSide") or order.get("side") or "").upper()
        raw_stop = (
            order.get("stopLossPrice")
            or order.get("triggerPrice")
            or order.get("stopPrice")
            or 0.0
        )
        parsed_stop = _signed_f(raw_stop, 0.0)
        parsed_qty, qty_explicit = _order_confirmation_qty(order)
        reasons: list[str] = []
        if not _looks_stop_order(order):
            reasons.append("not_stop")
        if expected_id and order_id != expected_id:
            reasons.append("different_stop_plan_id")
        if wanted_pid and row_pid != wanted_pid:
            if not (expected_id and order_id == expected_id and not row_pid):
                reasons.append("different_position_id")
        if side and row_side and row_side != side.upper():
            reasons.append("different_side")
        if parsed_stop <= 0:
            reasons.append("invalid_stop_price")
        elif abs(parsed_stop - float(stop_price)) > price_tolerance:
            reasons.append("different_stop_price")
        if parsed_qty <= 0:
            if (
                expected_id
                and order_id == expected_id
                and not qty_explicit
                and parsed_stop > 0
                and abs(parsed_stop - float(stop_price)) <= price_tolerance
            ):
                reasons.append("missing_qty_exact_id_price")
            else:
                reasons.append("invalid_qty")
        elif parsed_qty + qty_tolerance < float(qty):
            reasons.append("qty_below_required")
        candidates.append(
            {
                "stop_plan_id": order_id,
                "position_id": row_pid,
                "side": row_side,
                "stop": parsed_stop,
                "qty": parsed_qty,
                "mismatch": reasons or ["match"],
            }
        )

    def _candidate_rank(item: dict[str, Any]) -> tuple[Any, ...]:
        mismatch = {str(value) for value in (item.get("mismatch") or [])}
        item_id = clean_exchange_id(item.get("stop_plan_id"))
        item_pid = clean_exchange_id(item.get("position_id"))
        item_side = str(item.get("side") or "").upper()
        item_stop = _signed_f(item.get("stop"), 0.0)
        item_qty = _signed_f(item.get("qty"), 0.0)
        # Show the row that best explains the failed confirmation, not an
        # unrelated TP that happened to be returned first by BingX.
        return (
            0 if expected_id and item_id == expected_id else 1,
            0 if "not_stop" not in mismatch else 1,
            0 if wanted_pid and item_pid == wanted_pid else 1,
            0 if str(side or "").upper() and item_side == str(side).upper() else 1,
            len(mismatch - {"different_stop_plan_id"}),
            abs(item_stop - float(stop_price)) if item_stop > 0 else float("inf"),
            max(0.0, float(qty) - item_qty) if item_qty > 0 else float("inf"),
            item_id,
        )

    candidates.sort(key=_candidate_rank)
    old_ids = {clean_exchange_id(value) for value in (old_order_ids or set())}
    old_ids.discard("")
    live_ids = {_algo_order_id(row) for row in orders or [] if isinstance(row, dict)}
    live_ids.discard("")
    return {
        "expected": {
            "stop_plan_id": expected_id,
            "position_id": wanted_pid,
            "side": str(side or "").upper(),
            "stop": float(stop_price),
            "qty": float(qty),
            "price_tolerance": float(price_tolerance),
            "qty_tolerance": float(qty_tolerance),
        },
        "total_rows": len([row for row in orders or [] if isinstance(row, dict)]),
        "remaining_old_ids": sorted(old_ids.intersection(live_ids)),
        "candidates": candidates,
    }




def _bingx_stop_only_cleanup_ids(
    order_ids: set[str] | list[str] | tuple[str, ...],
    live_rows_by_id: dict[str, dict[str, Any]],
) -> tuple[set[str], list[str], list[str]]:
    """Return only live STOP-like ids that may be cancelled during BingX BE.

    v1.6.96 made new BingX BE flows preserve TP orders.  This v1.6.98 guard
    also protects resumed/legacy cleanup intents: if an older Railway process
    had persisted TP ids in ``cleanup_cancel_intent_v1``, the current process
    must not cancel them and must not wait for those TP ids to disappear before
    marking the BE STOP as confirmed.
    """

    cleaned_ids = {clean_exchange_id(value) for value in (order_ids or [])}
    cleaned_ids.discard("")
    live_rows = live_rows_by_id if isinstance(live_rows_by_id, dict) else {}
    stop_ids: set[str] = set()
    preserved_non_stop_ids: list[str] = []
    already_absent_ids: list[str] = []
    for order_id in sorted(cleaned_ids):
        row = live_rows.get(order_id)
        if not isinstance(row, dict):
            already_absent_ids.append(order_id)
            continue
        if _looks_stop_order(row):
            stop_ids.add(order_id)
        else:
            preserved_non_stop_ids.append(order_id)
    return stop_ids, preserved_non_stop_ids, already_absent_ids

def _stop_diagnostics_summary(diagnostics: dict[str, Any]) -> str:
    if not isinstance(diagnostics, dict):
        return "no diagnostics"
    expected = (
        diagnostics.get("expected")
        if isinstance(diagnostics.get("expected"), dict)
        else {}
    )
    candidates = (
        diagnostics.get("candidates")
        if isinstance(diagnostics.get("candidates"), list)
        else []
    )
    first = candidates[0] if candidates and isinstance(candidates[0], dict) else {}
    mismatch_labels = {
        "not_stop": "строка не является STOP",
        "different_stop_plan_id": "другой stopPlanOrderId",
        "different_position_id": "другая позиция",
        "different_side": "другая сторона",
        "invalid_stop_price": "некорректная цена STOP",
        "different_stop_price": "другая цена STOP",
        "invalid_qty": "некорректный объём",
        "missing_qty_exact_id_price": "объём не отдан BingX, но ID и STOP совпали",
        "qty_below_required": "объём STOP меньше остатка",
        "match": "совпадение",
    }
    mismatch = ", ".join(
        mismatch_labels.get(str(value), str(value))
        for value in (first.get("mismatch") or [])
    )
    return (
        f"ожидалось ID={expected.get('stop_plan_id')or '-'}, "
        f"STOP={expected.get('stop')or '-'}, объём={expected.get('qty')or '-'}; "
        f"строк BingX={diagnostics.get('total_rows', 0)}; "
        f"найдено ID={first.get('stop_plan_id')or '-'}, "
        f"STOP={first.get('stop')or '-'}, объём={first.get('qty')or '-'}; "
        f"расхождение: {mismatch or 'точный кандидат не найден'}"
    )[:900]


def _exchange_error_details(exc: Exception) -> dict[str, Any]:
    """Extract structured BingX write diagnostics without parsing secrets."""

    audit = getattr(exc, "response_audit", None)
    return {
        "exception": type(exc).__name__,
        "message": str(exc)[:1000],
        "order_id": clean_exchange_id(getattr(exc, "order_id", "")),
        "error_code": getattr(exc, "error_code", None),
        "error_message": str(getattr(exc, "error_message", "") or "")[:1000],
        "retryable": getattr(exc, "retryable", None),
        "response_audit": dict(audit) if isinstance(audit, dict) else None,
    }


def _replacement_stop_identity(stop_row: dict[str, Any] | None) -> dict[str, Any]:
    """Return the durable exact identity of a confirmed replacement STOP."""

    if not isinstance(stop_row, dict):
        return {}
    stop_id = _algo_order_id(stop_row)
    if not stop_id:
        return {}
    return {
        "replacement_stop_id": stop_id,
        "verify_matching_stop_order_id": stop_id,
        "replacement_stop": dict(stop_row),
        "replacement_confirmation_mode": str(
            stop_row.get("_be_confirmation_mode") or "exact_id_price_qty"
        ),
    }


def _be_ownership_patch(
    claimed_limit_attached_stop: dict[str, Any] | None,
    replacement_stop: dict[str, Any] | None,
    *,
    replacement_in_progress: bool | None = None,
    clear_cleanup_intent: bool = False,
) -> dict[str, Any]:
    """Build a merge-safe patch preserving STOP ownership on every BE outcome."""

    patch: dict[str, Any] = {}
    if isinstance(claimed_limit_attached_stop, dict) and claimed_limit_attached_stop:
        patch[LIMIT_ATTACHED_STOP_KEY] = dict(claimed_limit_attached_stop)
    be_patch = _replacement_stop_identity(replacement_stop)
    if be_patch:
        # Once the exact exchange plan id is durable, the pre-write intent is no
        # longer needed.  Setting None intentionally clears it through deep merge.
        be_patch["replacement_write_intent_v1"] = None
        be_patch["rate_limit_write_confirmation_v1"] = None
    if replacement_in_progress is not None:
        be_patch["replacement_in_progress"] = bool(replacement_in_progress)
    if clear_cleanup_intent:
        be_patch["cleanup_cancel_intent_v1"] = None
    if be_patch:
        patch["be"] = be_patch
    return patch


def _position_breakeven_price(pos: dict[str, Any]) -> float:
    """Read the exchange-calculated break-even price from a position.

    BingX calculate the true break-even price including
    actual taker/maker fees applied to this specific position. Using this value
    is more accurate than approximating with BE_FEE_BUFFER_PERCENT.

    Returns 0.0 if the exchange does not provide this field.
    """
    if not isinstance(pos, dict):
        return 0.0
    for key in (
        "breakEvenPrice",
        "breakevenPrice",
        "break_even_price",
        "breakEven",
        "breakeven",  # exchange naming variants
    ):
        try:
            val = pos.get(key)
            if val not in (None, "") and not isinstance(val, bool):
                price = float(val)
                if math.isfinite(price) and price > 0:
                    return price
        except Exception:
            pass
    return 0.0


def _stable_client_id(prefix: str, *parts: Any) -> str:
    raw = hashlib.sha256(":".join(str(p) for p in parts).encode()).hexdigest()[:20]
    return f"{prefix}-{raw}"[:36]


def _price_reached_tp(side: str, current: float, target: float) -> bool:
    if current <= 0 or target <= 0:
        return False
    if side.lower() == "long":
        return current >= target
    return current <= target


def _tick_at_or_better(side: str, boundary: float, tick: float) -> float:
    """Return the nearest exchange tick that does not cross the no-loss boundary.

    LONG protection must remain at or above the boundary, while SHORT protection
    must remain at or below it.  Decimal arithmetic avoids float artefacts that
    could otherwise move the result one tick to the losing side.
    """

    if not math.isfinite(float(boundary)) or float(boundary) <= 0:
        return 0.0
    if not math.isfinite(float(tick)) or float(tick) <= 0:
        return float(boundary)
    try:
        boundary_d = Decimal(str(boundary))
        tick_d = Decimal(str(tick))
        rounding = ROUND_CEILING if str(side).lower() == "long" else ROUND_FLOOR
        units = (boundary_d / tick_d).to_integral_value(rounding=rounding)
        return float(units * tick_d)
    except (InvalidOperation, ValueError, OverflowError):
        return 0.0


def _be_stop_market_safety(
    side: str,
    *,
    stop_price: float,
    last_price: float,
    fair_price: float,
    price_tick: float,
) -> dict[str, Any]:
    """Validate that a fresh BE STOP cannot trigger immediately.

    BingX STOP plans in this project use fair price as the loss trigger.  Last
    price is checked too because a large fair/last divergence is a dangerous
    state for replacing protection.  Missing or non-finite prices fail closed.
    """

    stop = _f(stop_price, 0.0)
    last = _f(last_price, 0.0)
    fair = _f(fair_price, 0.0)
    tick = _f(price_tick, 0.0)
    gap = max(tick * 2.0, abs(stop) * 1e-8, 1e-12)
    side_l = str(side or "").lower()
    if stop <= 0 or last <= 0 or fair <= 0 or side_l not in {"long", "short"}:
        return {
            "safe": False,
            "reason": "fresh last/fair price is unavailable or invalid",
            "stop": stop,
            "last": last,
            "fair": fair,
            "required_gap": gap,
        }
    if side_l == "long":
        last_safe = last > stop + gap
        fair_safe = fair > stop + gap
    else:
        last_safe = last < stop - gap
        fair_safe = fair < stop - gap
    safe = bool(last_safe and fair_safe)
    reason = "ok" if safe else "market is not safely beyond the new BE STOP"
    return {
        "safe": safe,
        "reason": reason,
        "stop": stop,
        "last": last,
        "fair": fair,
        "required_gap": gap,
        "last_safe": bool(last_safe),
        "fair_safe": bool(fair_safe),
    }






_BE_QTY_COVERAGE_RETRY_SEC = 3600.0
_BE_MARKET_SAFE_RETRY_SEC = 300.0
_BE_RATE_LIMIT_RETRY_SEC = 900.0
_BE_RATE_LIMIT_CONFIRMATION_BACKOFF_SEC = (10, 30, 60, 120, 900, 1800, 7200)
_TP_TO_BE_SLA_WARN_SEC = 60.0


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _parse_iso_utc(value: Any) -> datetime | None:
    if not value or isinstance(value, bool):
        return None
    try:
        text = str(value).strip()
        if not text:
            return None
        if isinstance(value, (int, float)) or (
            text.replace(".", "", 1).isdigit() and len(text.split(".", 1)[0]) >= 10
        ):
            epoch = float(value)
            magnitude = abs(epoch)
            if magnitude >= 1e14:
                epoch /= 1_000_000.0
            elif magnitude >= 1e11:
                epoch /= 1_000.0
            return datetime.fromtimestamp(epoch, tz=timezone.utc)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _be_retry_after_iso(seconds: float) -> str:
    return _iso_utc(_utc_now() + timedelta(seconds=max(1.0, float(seconds or 1.0))))


def _rate_limit_write_confirmation_pending(be_state: dict[str, Any]) -> bool:
    marker = (
        be_state.get("rate_limit_write_confirmation_v1")
        if isinstance(be_state, dict)
        and isinstance(be_state.get("rate_limit_write_confirmation_v1"), dict)
        else {}
    )
    return bool(
        str(marker.get("state") or "").strip().lower() == "pending"
        and str(marker.get("error_code") or "").strip() == "100410"
    )


def _rate_limit_write_confirmation_marker(
    *,
    error_details: dict[str, Any],
    old_stop_ids: list[str],
    exchange_retry_after: datetime,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build the durable read-only recovery checkpoint for BingX code 100410.

    A trigger-endpoint rate-limit response is an ambiguous write outcome: BingX
    may have accepted the STOP immediately before returning the business error.
    Preserve the pre-write identity checkpoint and schedule a bounded fresh
    read-back instead of sending a second STOP.
    """

    current = now or _utc_now()
    first_retry_sec = int(_BE_RATE_LIMIT_CONFIRMATION_BACKOFF_SEC[0])
    return {
        "version": 1,
        "state": "pending",
        "error_code": "100410",
        "created_at": _iso_utc(current),
        "last_checked_at": _iso_utc(current),
        "readback_attempt": 1,
        "next_readback_after": _iso_utc(
            current + timedelta(seconds=first_retry_sec)
        ),
        "exchange_retry_after": _iso_utc(exchange_retry_after),
        "old_stop_ids": list(old_stop_ids),
        "old_stop_exactly_proven": bool(old_stop_ids),
        "exchange_error": dict(error_details or {}),
        "exchange_writes_after_error": 0,
    }


def _be_waiting_retry_deferred_until(be_state: dict[str, Any]) -> datetime | None:
    if not isinstance(be_state, dict) or not be_state.get("waiting_retry"):
        return None
    retry_at = _parse_iso_utc(be_state.get("waiting_next_retry_after"))
    if retry_at is None:
        return None
    return retry_at if _utc_now() < retry_at else None


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or default)
    except (TypeError, ValueError, OverflowError):
        return default


def _be_waiting_tp_event_bypass_decision(
    be_state: dict[str, Any],
    *,
    event_level_index: int | None,
    trigger_plan_index: int,
    rows_override_present: bool,
) -> dict[str, Any]:
    """Decide whether a TP event may wake a delayed BE retry.

    v1.6.104 hardening: a cooldown bypass is one-shot per later TP level.
    TP2 may wake an old TP1 wait; the same TP2 event/verification retry may not
    keep bypassing the durable cooldown and hammer BingX trigger endpoints.
    """

    deferred_until = _be_waiting_retry_deferred_until(be_state)
    event_idx = _safe_int(event_level_index, 0)
    trigger_idx = _safe_int(trigger_plan_index, 0)
    waiting_trigger_idx = _safe_int(
        be_state.get("waiting_trigger_tp_index")
        or be_state.get("trigger_tp_index")
        or 0,
        0,
    )
    consumed_idx = _safe_int(
        be_state.get("waiting_bypass_consumed_event_tp_index")
        or be_state.get("waiting_bypass_event_tp_index")
        or 0,
        0,
    )
    decision = {
        "deferred_until": deferred_until,
        "event_idx": event_idx,
        "trigger_idx": trigger_idx,
        "waiting_trigger_idx": waiting_trigger_idx,
        "consumed_idx": consumed_idx,
        "bypass": False,
        "reason": "not_deferred" if deferred_until is None else "cooldown_active",
    }
    if deferred_until is None:
        return decision
    if not rows_override_present:
        decision["reason"] = "background_scan"
        return decision
    if event_idx <= 0 or trigger_idx <= 0 or event_idx < trigger_idx:
        decision["reason"] = "event_before_trigger"
        return decision
    if event_idx <= waiting_trigger_idx:
        decision["reason"] = "same_or_older_waiting_trigger"
        return decision
    if event_idx <= consumed_idx:
        decision["reason"] = "tp_event_bypass_already_consumed"
        return decision
    decision["bypass"] = True
    decision["reason"] = "later_tp_event"
    return decision


def _be_waiting_tp_bypass_marker(
    decision: dict[str, Any], *, position_qty: float | None = None
) -> dict[str, Any]:
    if not decision.get("bypass"):
        return {}
    marker = {
        "waiting_retry_bypassed_by_tp_event_v1_6_103": True,
        "waiting_retry_bypass_single_use_v1_6_104": True,
        "waiting_bypass_event_tp_index": int(decision.get("event_idx") or 0),
        "waiting_bypass_trigger_tp_index": int(decision.get("trigger_idx") or 0),
        "waiting_bypass_waiting_trigger_tp_index": int(decision.get("waiting_trigger_idx") or 0),
        "waiting_bypass_consumed_event_tp_index": int(decision.get("event_idx") or 0),
        "waiting_bypass_reason": str(decision.get("reason") or "later_tp_event"),
    }
    deferred_until = decision.get("deferred_until")
    if isinstance(deferred_until, datetime):
        marker["waiting_bypass_previous_next_retry_after"] = _iso_utc(deferred_until)
    if position_qty is not None:
        marker["waiting_bypass_position_qty"] = float(position_qty)
    return marker


def _bingx_trigger_rate_limit_retry_after(exc: BaseException) -> datetime | None:
    """Return BingX trigger-endpoint unblock time for code 100410, if available.

    BingX sometimes returns HTTP 200 with business code 100410 for the trigger
    order endpoint.  This is not a BE logic failure and should keep the old STOP
    live while the retry sleeps until the exchange unblock timestamp.
    """

    details = _exchange_error_details(exc) if isinstance(exc, Exception) else {}
    code = str(details.get("error_code") or getattr(exc, "error_code", "") or "").strip()
    message = " ".join(
        str(part or "")
        for part in (
            details.get("message"),
            details.get("error_message"),
            details.get("response_audit"),
            str(exc),
        )
    )
    lower = message.lower()
    if code != "100410" and "100410" not in lower and "rate limit" not in lower and "frequency limit" not in lower:
        return None
    now = _utc_now()
    # BingX message example: "unblocked after <timestamp_ms>".
    for raw in re.findall(r"(?:unblock(?:ed)?\s+after|after)\s*(\d{10,16})", lower):
        try:
            value = int(raw)
        except ValueError:
            continue
        seconds = value / 1000.0 if value > 10_000_000_000 else float(value)
        try:
            candidate = datetime.fromtimestamp(seconds, timezone.utc)
        except (OverflowError, OSError, ValueError):
            continue
        if candidate > now:
            return candidate + timedelta(seconds=3)
    return now + timedelta(seconds=_BE_RATE_LIMIT_RETRY_SEC)


def _is_bingx_trigger_rate_limit(exc: BaseException) -> bool:
    return _bingx_trigger_rate_limit_retry_after(exc) is not None

def _be_waiting_market_safe_dedup_key(execution_id: int | str) -> str:
    """Stable durable-notification key for one BE market-safety wait incident.

    The visible waiting card includes fresh last/fair prices for diagnostics.
    Those values change every monitor pass, so the generic content-based
    durable-notification key cannot be used here or Telegram will be spammed.
    """

    return f"be_waiting_market_safe:{int(execution_id or 0)}"


def _is_be_market_not_safe_exchange_rejection(exc: BaseException) -> bool:
    """Return True for BingX STOP rejections caused by current-price safety.

    BingX can still reject a BE STOP after our local pre-write safety check
    because price moved between readback and POST /trade/order.  This is not
    an ownership failure and should keep the old STOP live while the BE retry
    waits for a safe market again.
    """

    code = str(getattr(exc, "error_code", "") or "").strip()
    message = " ".join(
        str(part or "")
        for part in (
            getattr(exc, "error_message", ""),
            str(exc),
            getattr(exc, "response_audit", ""),
        )
    ).lower()
    if code == "110411":
        return True
    return (
        "stop loss price" in message
        and "current price" in message
        and ("lower" in message or "higher" in message)
    )


def _weighted_trigger_tp(
    targets: list[float], pcts: list[float], trigger_idx: int
) -> float:
    pairs = []
    for tp, pct in zip(targets[:trigger_idx], pcts[:trigger_idx]):
        if tp > 0 and pct > 0:
            pairs.append((float(tp), float(pct)))
    total_pct = sum(p for _, p in pairs)
    if not pairs or total_pct <= 0:
        return float(targets[max(0, trigger_idx - 1)])
    return sum(tp * pct for tp, pct in pairs) / total_pct


def _legacy_entry_fee_plus_price(side: str, entry: float) -> float:
    settings = get_settings()
    buffer_pct = (
        max(
            0.0, float(settings.BE_FEE_BUFFER_PERCENT) + float(settings.BE_PLUS_PERCENT)
        )
        / 100.0
    )
    if side.lower() == "long":
        return entry * (1.0 + buffer_pct)
    return entry * (1.0 - buffer_pct)


def _real_trade_breakeven_stop(
    side: str,
    entry: float,
    original_qty: float,
    qty_now: float,
    targets: list[float],
    pcts: list[float],
    trigger_idx: int,
    exchange_be_price: float = 0.0,
) -> dict[str, float]:
    """Calculate a real total-trade BE stop after partial TP.

    The calculation includes approximate entry/exit fees using BE_FEE_BUFFER_PERCENT
    as one-way fee allowance. BE_PLUS_PERCENT is then added as a small profit
    cushion. The final stop is never worse than the old conservative
    entry+fee+plus rule.
    """
    settings = get_settings()
    side_l = side.lower()
    entry = float(entry)
    original_qty = float(original_qty)
    qty_now = float(qty_now)
    realized_qty = max(0.0, original_qty - qty_now)
    tp_avg = _weighted_trigger_tp(targets, pcts, trigger_idx)
    fee = max(0.0, float(settings.BE_FEE_BUFFER_PERCENT)) / 100.0
    plus = max(0.0, float(settings.BE_PLUS_PERCENT)) / 100.0

    real_be_raw = _legacy_entry_fee_plus_price(side_l, entry)
    real_be = real_be_raw
    if (
        entry > 0
        and original_qty > 0
        and qty_now > 0
        and realized_qty > 0
        and tp_avg > 0
    ):
        # Approximate total PnL after TP(s) and future SL.
        # LONG: R*(TP-E) + q*(S-E) - f*(Q*E + R*TP + q*S) = 0
        # SHORT: R*(E-TP) + q*(E-S) - f*(Q*E + R*TP + q*S) = 0
        if side_l == "long":
            denom = qty_now * max(1e-12, (1.0 - fee))
            real_be_raw = (
                qty_now * entry
                + fee * original_qty * entry
                + fee * realized_qty * tp_avg
                - realized_qty * (tp_avg - entry)
            ) / denom
            real_be = real_be_raw + entry * plus
        else:
            denom = qty_now * (1.0 + fee)
            real_be_raw = (
                qty_now * entry
                + realized_qty * (entry - tp_avg)
                - fee * original_qty * entry
                - fee * realized_qty * tp_avg
            ) / max(1e-12, denom)
            real_be = real_be_raw - entry * plus

    conservative = _legacy_entry_fee_plus_price(side_l, entry)
    conservative_no_loss = (
        entry * (1.0 + fee) if side_l == "long" else entry * (1.0 - fee)
    )

    # If exchange provides its own break-even price, use it as the most accurate
    # base and add our profit cushion (BE_PLUS_PERCENT). This guarantees the
    # stop is ABOVE the true break-even point (LONG) or BELOW (SHORT).
    exchange_based = 0.0
    if exchange_be_price > 0:
        if side_l == "long":
            exchange_based = exchange_be_price * (1.0 + plus)
        else:
            exchange_based = exchange_be_price * (1.0 - plus)

            # Choose the safest stop:
            # LONG: take the highest of all candidates (further from loss)
            # SHORT: take the lowest of all candidates
    candidates = [real_be, conservative]
    if exchange_based > 0:
        candidates.append(exchange_based)
    if side_l == "long":
        final = max(candidates)
        no_loss_boundary = max(
            real_be_raw,
            conservative_no_loss,
            exchange_be_price if exchange_be_price > 0 else 0.0,
        )
    else:
        final = min(candidates)
        no_loss_candidates = [real_be_raw, conservative_no_loss]
        if exchange_be_price > 0:
            no_loss_candidates.append(exchange_be_price)
        no_loss_boundary = min(no_loss_candidates)

    return {
        "final_stop": float(final),
        "real_breakeven_stop": float(real_be),
        "real_breakeven_raw": float(real_be_raw),
        "conservative_entry_fee_plus_stop": float(conservative),
        "conservative_no_loss_stop": float(conservative_no_loss),
        "exchange_breakeven_based": float(exchange_based),
        "exchange_breakeven_raw": float(exchange_be_price),
        "no_loss_boundary": float(no_loss_boundary),
        "realized_qty": float(realized_qty),
        "tp_avg": float(tp_avg),
        "fee_pct": float(fee * 100.0),
        "plus_pct": float(plus * 100.0),
    }

    # Known BingX stop-order type strings that indicate a stop order.


_STOP_ORDER_TYPES = frozenset(
    {"stop", "stop_market", "stop_loss", "stop_loss_market", "stop_limit"}
)
_TP_ORDER_TYPES = frozenset({"take_profit", "take_profit_market", "tp", "take profit"})


def _looks_stop_order(order: Any) -> bool:
    """Return True when an algo order looks like a protective stop.

    Uses explicit field checks first (order type field), then falls back to
    keyword search. Keyword search is kept as fallback because BingX API may
    return different field names across versions. The function deliberately
    avoids false-positives on TP orders.
    """
    if not isinstance(order, dict):
        return False
        # Check explicit type fields first.
    for key in ("type", "orderType", "order_type", "algoType", "algo_type", "stopType"):
        val = str(order.get(key) or "").lower().strip().replace("-", "_")
        if val in _STOP_ORDER_TYPES:
            return True
        if val in _TP_ORDER_TYPES:
            return False  # Explicitly a TP order — not a stop.
            # Check client order id for bot-generated stop IDs.
    client_id = str(
        order.get("clientOrderId")
        or order.get("clientAlgoId")
        or order.get("clOrdId")
        or ""
    ).lower()
    if any(x in client_id for x in ("avc-be", "avc-sl", "avc-pre", "avc-emerg")):
        return True
    if "avc-tp" in client_id or "avc-rec" in client_id:
        return False
        # Fallback: keyword search on JSON.
    text = json.dumps(order, ensure_ascii=False).lower()
    has_stop = "stop" in text or "loss" in text
    has_tp = "take_profit" in text or "takeprofit" in text or "avc-tp" in text
    return has_stop and not has_tp


def _mark_filled_tps_by_qty(
    ids_payload: dict, original_qty: float, qty_now: float, side: str
) -> bool:
    """Mark TP orders as filled based on position qty reduction.

    closed_qty = original_qty - qty_now is distributed across TPs in order of
    proximity to entry (tp_index). TPs whose total qty is covered by closed_qty
    are marked filled=True so later cleanup can attribute realized PnL correctly.

    Returns True if any TP status changed (caller should persist payload).
    """
    tp_list = ids_payload.get("tp") if isinstance(ids_payload, dict) else None
    if not tp_list:
        return False
    closed_qty = max(0.0, original_qty - qty_now)
    if closed_qty <= 1e-9:
        return False

    tps = sorted(
        [(i, t) for i, t in enumerate(tp_list) if isinstance(t, dict)],
        key=lambda x: int(x[1].get("tp_index") or x[0] + 1),
    )

    changed = False
    consumed = 0.0
    for _, tp in tps:
        if tp_row_has_unresolved_qty_conflict(
            tp
        ) or tp_row_has_unresolved_identity_conflict(tp):
            log.error(
                "BE fill inference blocked by unresolved TP ledger conflict tp_index=%s",
                tp.get("tp_index"),
            )
            break
        tp_qty = float(tp.get("qty") or 0.0)
        if tp_qty <= 0:
            continue
        if closed_qty - consumed >= tp_qty - 1e-9:
            # Position reduction alone can be a manual partial close. Only turn
            # it into a TP fill when the lifecycle guard has recorded price
            # evidence or exact BingX stop-order history.
            has_tp_evidence = (
                tp.get("price_seen") is True
                or str(tp.get("fill_source") or "") == "mexc_stoporder_history"
            )
            if has_tp_evidence and tp.get("filled") is not True:
                tp["filled"] = True
                tp.setdefault("fill_source", "position_qty_plus_price_touch")
                changed = True
            consumed += tp_qty
        else:
            break
    return changed


def _tp_order_id_from_payload(order: Any) -> str:
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
        raw.get("id"),
        data if not isinstance(data, (dict, list)) else None,
    ):
        text = clean_exchange_id(value)
        if text:
            return text
    return ""


def _merge_recreated_tp_orders(
    ids_payload: dict[str, Any], recreated: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Replace stale pre-BE TP order references with recreated orders.

    For BingX v1.6.96 and newer, BE preserves existing TP orders and passes an
    empty ``recreated`` list, so this function returns the original TP ledger
    unchanged. Non-BingX flows may still use recreated TP rows.
    """
    original = ids_payload.get("tp") if isinstance(ids_payload, dict) else None
    rows = [dict(item) for item in (original or []) if isinstance(item, dict)]
    by_index = {int(item.get("tp_index") or 0): item for item in rows}
    now = datetime.now(timezone.utc).isoformat()
    for item in recreated or []:
        if not isinstance(item, dict) or item.get("error"):
            continue
        index = int(item.get("tp_index") or 0)
        result = item.get("result")
        if index <= 0 or not isinstance(result, dict):
            continue
        row = by_index.get(index)
        if row is None:
            row = {
                "tp_index": index,
                "target": float(item.get("tp") or 0.0),
            }
            rows.append(row)
            by_index[index] = row
        previous_order_id = _tp_order_id_from_payload(row.get("order"))
        row["order"] = result
        row["qty"] = float(item.get("qty") or 0.0)
        row["planned_qty"] = float(item.get("planned_qty") or 0.0)
        row["recreated_after_be"] = True
        row["be_recreated_at"] = now
        if previous_order_id:
            row["previous_order_id_before_be"] = previous_order_id
            # Any old history/touch evidence belongs to the cancelled pre-BE order.
            # update_execution_status_merge is a deep merge, so omitted keys would
            # survive. Clear them explicitly for the newly created order.
        row["exchange_fill_check"] = None
        row["partial_fill_warning"] = None
        row["price_seen"] = False
        row["price_seen_at"] = None
        row["price_seen_value"] = None
        row["filled"] = False
        row["fill_source"] = None
        row["exchange_filled_qty"] = None
        row["filled_at"] = None
        row["filled_notified"] = False
        row["notification_pending"] = False
        row["next_notify_retry_at"] = None
        row["notify_attempts"] = 0
    rows.sort(key=lambda item: int(item.get("tp_index") or 999))
    return rows


def _filled_tp_indexes(ids_payload: dict[str, Any]) -> set[int]:
    result: set[int] = set()
    for fallback_index, item in enumerate(ids_payload.get("tp") or [], 1):
        if not isinstance(item, dict) or item.get("filled") is not True:
            continue
        index = int(item.get("tp_index") or fallback_index)
        if index > 0:
            result.add(index)
    return result


def _algo_order_count(orders: Any) -> int:
    return len(orders) if isinstance(orders, list) else 0


async def _notify(
    notify: NotifyFn | None,
    user_id: int,
    text: str,
    *,
    event_key: str | None = None,
    dedup_key_override: str | None = None,
) -> bool:
    return await send_or_enqueue(
        notify,
        user_id,
        text,
        source="be_monitor",
        event_key=event_key,
        dedup_key_override=dedup_key_override,
    )


class _BePreReadContext:
    """Pass-local coalescing for BE monitor pre-write reads only.

    Positions are isolated by exact user/exchange/adapter identity and market
    prices by the same account identity plus normalized symbol.  The context
    exists for one ``process_be_monitor_once`` invocation only.  It is never
    used for STOP/open-order/history read-back after an exchange write.
    """

    def __init__(
        self,
        *,
        positions_max_age_seconds: float = 15.0,
        market_max_age_seconds: float = 5.0,
        fallback_positions_cache: Any | None = None,
    ) -> None:
        self._positions = _LifecyclePositionsContext(
            max_age_seconds=positions_max_age_seconds
        )
        self._fallback_positions_cache = fallback_positions_cache
        self._market_max_age = max(0.0, float(market_max_age_seconds))
        self._market_cache: dict[
            tuple[int, str, int, str], tuple[float, dict[str, float]]
        ] = {}
        self._market_locks: dict[tuple[int, str, int, str], asyncio.Lock] = {}
        self._market_generation: dict[tuple[int, str, int, str], int] = {}
        self._market_stats: dict[str, int] = {}

    @staticmethod
    def _symbol_key(symbol: str) -> str:
        return str(symbol or "").upper().replace("-", "").replace("_", "")

    @classmethod
    def _market_key(
        cls, adapter: Any, user_id: int, exchange: str, symbol: str
    ) -> tuple[int, str, int, str]:
        return (
            int(user_id),
            str(exchange or "").lower(),
            id(adapter),
            cls._symbol_key(symbol),
        )

    def _inc(self, key: str, amount: int = 1) -> None:
        self._market_stats[key] = int(self._market_stats.get(key, 0)) + int(amount)

    def _market_lock(self, key: tuple[int, str, int, str]) -> asyncio.Lock:
        lock = self._market_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._market_locks[key] = lock
        return lock

    async def get_positions(
        self,
        adapter: Any,
        user_id: int,
        exchange: str,
        symbol: str,
        side: str,
        *,
        force_refresh: bool = False,
    ) -> list[dict[str, Any]]:
        if not hasattr(adapter, "fetch_open_positions") and self._fallback_positions_cache is not None:
            self._inc("positions_legacy_fallback_requests")
            if force_refresh:
                self._fallback_positions_cache.invalidate(user_id, exchange)
                self._inc("positions_legacy_fallback_refreshes")
            rows = await self._fallback_positions_cache.get(
                adapter, user_id, exchange, symbol, side
            )
        else:
            rows = await self._positions.get_positions(
                adapter,
                user_id,
                exchange,
                symbol,
                force_refresh=force_refresh,
            )
        wanted_side = str(side or "").upper()
        filtered: list[dict[str, Any]] = []
        for row in rows:
            row_side = str(row.get("side") or row.get("positionSide") or "").upper()
            if wanted_side and row_side and row_side != wanted_side:
                continue
            if wanted_side and not row_side:
                # Legacy/test adapters may already return one symbol-scoped row
                # without side metadata. Keeping it is fail-safe: it can only
                # defer a zero-position conclusion, not manufacture one.
                pass
            if _position_size(row) > 0:
                filtered.append(dict(row))
        return filtered

    async def _get_market(
        self,
        adapter: Any,
        user_id: int,
        exchange: str,
        symbol: str,
        *,
        require_fair: bool,
        force_refresh: bool = False,
    ) -> dict[str, float]:
        self._inc("market_requests")
        key = self._market_key(adapter, user_id, exchange, symbol)
        lock = self._market_lock(key)
        if lock.locked():
            self._inc("market_singleflight_waits")
        async with lock:
            if force_refresh:
                self._market_generation[key] = int(self._market_generation.get(key, 0)) + 1
                self._market_cache.pop(key, None)
                self._inc("market_refreshes")

            cached = self._market_cache.get(key)
            now = time.monotonic()
            if cached is not None:
                cached_at, cached_prices = cached
                fresh = self._market_max_age > 0 and now - cached_at <= self._market_max_age
                has_required = _f(cached_prices.get("last"), 0.0) > 0 and (
                    not require_fair or _f(cached_prices.get("fair"), 0.0) > 0
                )
                if fresh and has_required:
                    self._inc("market_hits")
                    return dict(cached_prices)
                self._market_cache.pop(key, None)
                self._inc("market_expirations")

            generation = int(self._market_generation.get(key, 0))
            try:
                if require_fair:
                    fetcher = getattr(adapter, "fetch_market_prices", None)
                    if callable(fetcher):
                        raw = await fetcher(symbol)
                        self._inc("ticker_fetches")
                        self._inc("premium_index_fetches")
                        prices = {
                            "last": _f((raw or {}).get("last"), 0.0),
                            "fair": _f((raw or {}).get("fair"), 0.0),
                        }
                    else:
                        last = _f(await adapter.fetch_last_price(symbol), 0.0)
                        self._inc("ticker_fetches")
                        prices = {"last": last, "fair": last}
                else:
                    last = _f(await adapter.fetch_last_price(symbol), 0.0)
                    self._inc("ticker_fetches")
                    prices = {"last": last, "fair": 0.0}
                self._inc("market_fetches")
            except Exception:
                self._inc("market_errors")
                raise

            if int(self._market_generation.get(key, 0)) == generation:
                self._market_cache[key] = (time.monotonic(), dict(prices))
            return dict(prices)

    async def get_last_price(
        self,
        adapter: Any,
        user_id: int,
        exchange: str,
        symbol: str,
        *,
        provided_last: Any = None,
    ) -> float:
        provided = _f(provided_last, 0.0)
        if provided > 0:
            self._inc("market_requests")
            self._inc("market_provided_hits")
            return provided
        prices = await self._get_market(
            adapter, user_id, exchange, symbol, require_fair=False
        )
        return _f(prices.get("last"), 0.0)

    async def get_market_prices(
        self, adapter: Any, user_id: int, exchange: str, symbol: str
    ) -> dict[str, float]:
        return await self._get_market(
            adapter, user_id, exchange, symbol, require_fair=True
        )

    async def invalidate_account(
        self, adapter: Any, user_id: int, exchange: str, symbol: str | None = None
    ) -> None:
        await self._positions.invalidate_positions(adapter, user_id, exchange)
        account_prefix = (int(user_id), str(exchange or "").lower(), id(adapter))
        symbol_key = self._symbol_key(symbol or "")
        matching = [
            key
            for key in list(self._market_cache) + list(self._market_locks)
            if key[:3] == account_prefix and (not symbol_key or key[3] == symbol_key)
        ]
        for key in set(matching):
            lock = self._market_lock(key)
            async with lock:
                self._market_generation[key] = int(self._market_generation.get(key, 0)) + 1
                self._market_cache.pop(key, None)
                self._inc("market_invalidations")

    def stats(self) -> dict[str, int]:
        stats = self._positions.stats()
        stats.update(self._market_stats)
        for key in (
            "market_requests",
            "market_fetches",
            "market_hits",
            "market_provided_hits",
            "market_singleflight_waits",
            "market_refreshes",
            "market_invalidations",
            "market_expirations",
            "market_errors",
            "ticker_fetches",
            "premium_index_fetches",
            "positions_legacy_fallback_requests",
            "positions_legacy_fallback_refreshes",
        ):
            stats.setdefault(key, 0)
        return dict(sorted(stats.items()))


def _restart_tp_history_recovery_active(*, now_mono: float | None = None) -> bool:
    now = time.monotonic() if now_mono is None else float(now_mono)
    age = max(0.0, now - _PROCESS_STARTED_MONO)
    return age <= _RESTART_TP_HISTORY_RECOVERY_WINDOW_SEC


async def _recover_restart_tp_history_exact(
    *,
    adapter: Any,
    execution_id: int,
    user_id: int,
    symbol: str,
    side: str,
    created_at: Any,
    status: str,
    ids_payload: dict[str, Any],
) -> tuple[set[int], int]:
    """Recover exact TP history during the short Railway restart window.

    This is deliberately narrow and fail-closed. It never treats quantity or
    price alone as a TP fill. A fill is promoted only through the same exact
    plan/child identity bridge and terminal-history matcher used by the
    lifecycle guard. The caller still owns all BE write/read-back safety.
    """

    if not _restart_tp_history_recovery_active():
        return set(), 0
    tp_rows = [
        item
        for item in (ids_payload.get("tp") or [])
        if isinstance(item, dict)
    ]
    if not tp_rows or not any(item.get("filled") is not True for item in tp_rows):
        return set(), 0
    fetch_history = getattr(adapter, "fetch_position_tpsl_history", None)
    if not callable(fetch_history):
        return set(), 0

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    try:
        history_rows = list(
            await fetch_history(
                symbol,
                is_finished=1,
                start_time_ms=_execution_start_ms(created_at),
                end_time_ms=now_ms + 60_000,
                page_size=_RESTART_TP_HISTORY_PAGE_SIZE,
                max_pages=_RESTART_TP_HISTORY_MAX_PAGES,
            )
            or []
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.warning(
            "G63_RESTART_TP_HISTORY_RECOVERY phase=history_read_failed "
            "execution_id=%s user_id=%s symbol=%s error_type=%s error=%s",
            execution_id,
            user_id,
            symbol,
            type(exc).__name__,
            str(exc)[:240],
        )
        return set(), 0

    if not history_rows:
        return set(), 0

    recovered_indices = await _recover_exact_tp_child_identities(
        adapter=adapter,
        tp_rows=tp_rows,
        history_rows=history_rows,
        side=side,
        symbol=symbol,
    )
    filled_indices = _apply_history_fills(
        tp_rows, history_rows, side=side, symbol=symbol
    )
    changed_indices = set(recovered_indices).union(filled_indices)
    if changed_indices:
        saved = await db.merge_execution_metadata(
            execution_id,
            {"tp": tp_rows},
            expected_status=status,
            write_flow_audit_stage="g63_restart_tp_history_recovery",
            write_flow_audit_status=status,
        )
        if not saved:
            raise StaleExecutionPass(
                source="be_monitor.g63_restart_tp_history_recovery",
                execution_id=execution_id,
                expected_status=status,
                attempted_status="merge_restart_tp_history",
            )
        ids_payload["tp"] = tp_rows
        exact_filled = [
            int(item.get("tp_index") or pos)
            for pos, item in enumerate(tp_rows, 1)
            if item.get("filled") is True
            and str(item.get("fill_source") or "") == "mexc_stoporder_history"
        ]
        log.info(
            "G63_RESTART_TP_HISTORY_RECOVERY phase=applied execution_id=%s "
            "user_id=%s symbol=%s changed=%s exact_filled=%s history_rows=%s",
            execution_id,
            user_id,
            symbol,
            sorted(changed_indices),
            sorted(exact_filled),
            len(history_rows),
        )
    return changed_indices, len(history_rows)


async def process_be_monitor_once(
    notify: NotifyFn | None = None,
    *,
    rows_override: list[dict[str, Any]] | None = None,
    market_prices: dict[str, float] | None = None,
    event_level_index: int | None = None,
    shared_adapter_cache: dict[tuple[int, str], Any] | None = None,
    market_event_exchange_context: Any | None = None,
    scan_limit: int = 100,
) -> int:
    """Move STOP to breakeven after configured TP trigger.

    v1.0.14 safety:
    - shares per-execution lock with LIMIT catch-up;
    - re-reads execution row under the lock;
    - merges only the 'be' section into exchange_order_ids_json;
    - uses actual fill/position avg entry when available, not just signal entry;
    - skips BE when trigger TP fully closes the whole position.
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
            bounded_scan_limit = max(1, min(int(scan_limit or 100), 100))
            rows = await db.be_monitor_executions(
                limit=bounded_scan_limit, after_id=_SCAN_CURSOR
            )
            if not rows and _SCAN_CURSOR:
                _SCAN_CURSOR = 0
                rows = await db.be_monitor_executions(
                    limit=bounded_scan_limit, after_id=0
                )
            if rows:
                _SCAN_CURSOR = max(int(row.get("id") or 0) for row in rows)
                # v1.0.7g7h2f5g5b: preserve the exact rotating page while
                # processing account-local rows together.  This improves the
                # existing 15-second pass cache hit rate without extending its
                # freshness window or sharing snapshots across lifecycle/BE.
                rows = account_local_full_pass_rows(rows)
        record_stage_rows(
            selected=len(rows),
            scanned=len(rows),
            source="override" if rows_override is not None else "database",
        )
        if not rows:
            return 0
        moved = 0
        owns_adapter_cache = shared_adapter_cache is None
        adapter_cache: dict[tuple[int, str], Any] = (
            {} if shared_adapter_cache is None else shared_adapter_cache
        )
        shared_positions_cache = get_global_positions_cache()
        pre_read_context = _BePreReadContext(
            positions_max_age_seconds=15.0,
            market_max_age_seconds=5.0,
            fallback_positions_cache=shared_positions_cache,
        )

        async def _invalidate_pre_reads(
            adapter: Any, user_id: int, exchange: str, symbol: str
        ) -> None:
            # Preserve cross-worker invalidation for the legacy shared cache,
            # while all BE reads in this pass use the stricter pass-local context.
            shared_positions_cache.invalidate(user_id, exchange)
            await pre_read_context.invalidate_account(
                adapter, user_id, exchange, symbol
            )

        try:
            for original_row in rows:
                execution_id = int(original_row.get("id") or 0)
                if not execution_id:
                    continue
                set_notification_event_key(f"execution:{execution_id}")
                async with db.execution_lock(execution_id) as lock_acquired:
                    if lock_acquired is False:
                        log.warning(
                            "BE_LOCK_DEFERRED execution_id=%s stage=%s",
                            execution_id, db.monitor_workload_stage(),
                        )
                        continue
                    row = await db.get_execution_by_id(execution_id) or original_row
                    status = str(row.get("status") or "opened")
                    status_payload = _json_dict(row.get("exchange_order_ids_json"))
                    status_be_state = (
                        status_payload.get("be")
                        if isinstance(status_payload.get("be"), dict)
                        else {}
                    )
                    # A manual button request is passed via rows_override and must
                    # wake the BE engine even when the execution is still in a normal
                    # live status (opened/protected/partial_error).  Older code only
                    # bypassed cooldown for manual_required rows, so a user-clicked
                    # manual BE could silently defer on an old waiting_next_retry_after
                    # and the UI fell back to "pending".  Keep the status allow-list
                    # explicit so unrelated/stale rows cannot be revived by metadata.
                    manual_be_override = bool(
                        rows_override is not None
                        and status in {
                            "opened",
                            "protected",
                            "partial_error",
                            "manual_required",
                        }
                        and status_be_state.get("manual_requested") is True
                        and status_be_state.get("moved") is not True
                    )
                    recovery_retry_at = _parse_iso_utc(
                        status_be_state.get("existing_be_recovery_next_retry_after")
                    )
                    recovery_checkpoint_present = _be_recovery_checkpoint_present(
                        status_be_state
                    )
                    rate_limit_confirmation_pending = (
                        _rate_limit_write_confirmation_pending(status_be_state)
                    )
                    recovery_be_override = bool(
                        status in {
                            "opened",
                            "protected",
                            "partial_error",
                            "manual_required",
                        }
                        and recovery_checkpoint_present
                        and status_be_state.get("moved") is not True
                        and (
                            (
                                rows_override is not None
                                and not rate_limit_confirmation_pending
                            )
                            or recovery_retry_at is None
                            or _utc_now() >= recovery_retry_at
                        )
                    )
                    if (
                        status not in {"opened", "protected", "partial_error"}
                        and not manual_be_override
                        and not recovery_be_override
                    ):
                        continue

                        # v1.6.18: track this iteration's last-known-good status so
                        # every write below refuses if a concurrent worker (or an
                        # old/new process briefly overlapping a Railway redeploy)
                        # already moved this row to a different status. Closes the
                        # gap where update_execution_status_merge previously had no
                        # way to detect a stale conclusion the way
                        # merge_execution_metadata(expected_status=...) already did.
                    _known_status = status

                    async def _write_status(new_status, reason, patch=None):
                        nonlocal _known_status
                        ok = await db.update_execution_status_merge(
                            execution_id,
                            new_status,
                            reason,
                            patch,
                            expected_status=_known_status,
                            write_flow_audit_stage="be_monitor",
                            write_flow_audit_status=new_status,
                        )
                        if ok:
                            _known_status = new_status
                        else:
                            log.info(
                                "be_monitor: abort stale execution pass execution_id=%s "
                                "attempted=%s expected=%s",
                                execution_id,
                                new_status,
                                _known_status,
                            )
                            raise StaleExecutionPass(
                                source="be_monitor",
                                execution_id=execution_id,
                                expected_status=_known_status,
                                attempted_status=new_status,
                            )
                        return True

                    # v1.6.103: BE waiting cooldown is evaluated only after
                    # the immutable trigger index is known.  A fresh TP event at
                    # or beyond the BE threshold (for example TP2 after an old
                    # TP1 quantity-coverage wait) must be allowed to re-check the
                    # live remaining position immediately; otherwise the bot can
                    # announce TP2 but keep the stale TP1 wait asleep until the
                    # old retry timestamp.

                    user_id = int(row.get("user_id") or 0)
                    symbol = str(row.get("symbol") or "").upper()
                    side = str(row.get("side") or "").lower()
                    signal_entry = _signed_f(row.get("entry"), 0.0)
                    original_qty = _signed_f(row.get("qty"), 0.0)
                    invalid_fields: list[str] = []
                    if not user_id:
                        invalid_fields.append("user_id")
                    if not symbol:
                        invalid_fields.append("symbol")
                    if side not in {"long", "short"}:
                        invalid_fields.append("side")
                    if signal_entry <= 0:
                        invalid_fields.append("entry")
                    if original_qty <= 0:
                        invalid_fields.append("qty")
                    if invalid_fields:
                        await _write_status(
                            "manual_required",
                            "BE monitor blocked: corrupted execution fields "
                            + ", ".join(invalid_fields),
                            {
                                "be": {
                                    "manual_required": True,
                                    "reason": "invalid_execution_fields",
                                    "invalid_fields": invalid_fields,
                                }
                            },
                        )
                        moved += 1
                        continue

                        # Load the durable execution payload before choosing the BE
                        # trigger. New executions freeze this setting at entry time so
                        # a later menu/Railway change cannot alter an already-open plan.
                    ids_payload = _json_dict(row.get("exchange_order_ids_json"))
                    _, ledger_changed, repaired_indices = canonicalize_tp_ledger(
                        ids_payload
                    )
                    if ledger_changed:
                        await db.merge_execution_metadata(
                            execution_id,
                            {
                                "tp": ids_payload.get("tp") or [],
                                "tp_ledger_v1": tp_ledger_repair_metadata(
                                    repaired_indices,
                                    source="be_monitor.execution_repair",
                                ),
                            },
                            write_flow_audit_stage="be_monitor_ledger_repair",
                            write_flow_audit_status=status,
                        )
                        log.info(
                            "BE monitor repaired TP ledger execution_id=%s user_id=%s symbol=%s indices=%s",
                            execution_id,
                            user_id,
                            symbol,
                            repaired_indices,
                        )
                    be_state = (
                        ids_payload.get("be")
                        if isinstance(ids_payload.get("be"), dict)
                        else {}
                    )
                    manual_requested = (
                        be_state.get("manual_requested") is True
                        and be_state.get("moved") is not True
                    )
                    recovery_be_override = bool(
                        recovery_be_override
                        and _be_recovery_checkpoint_present(be_state)
                    )
                    user_settings = None
                    snapshot = get_snapshot(ids_payload)
                    plan_items = (
                        snapshot_items(snapshot) if snapshot is not None else []
                    )
                    if recovery_be_override:
                        # A durable exchange-write checkpoint is a crash-recovery
                        # obligation. It must not be disabled by a later menu change,
                        # a missing legacy TP snapshot or a changed BE trigger.
                        trigger_idx = max(
                            0,
                            int(
                                be_state.get("trigger_ordinal")
                                or be_state.get("waiting_trigger_ordinal")
                                or 0
                            ),
                        )
                        if plan_items:
                            trigger_idx = min(trigger_idx, len(plan_items))
                            original_qty = max(
                                original_qty, snapshot_total_qty(snapshot)
                            )
                            targets = [float(item["price"]) for item in plan_items]
                            plan_qtys = [float(item["qty"]) for item in plan_items]
                            plan_total = sum(plan_qtys)
                            pcts = [
                                (qty / plan_total * 100.0) if plan_total > 0 else 0.0
                                for qty in plan_qtys
                            ]
                        else:
                            targets = []
                            pcts = []
                        trigger_plan_index = int(
                            be_state.get("trigger_tp_index")
                            or be_state.get("waiting_trigger_tp_index")
                            or (
                                int(plan_items[trigger_idx - 1]["tp_index"])
                                if trigger_idx > 0 and plan_items
                                else 0
                            )
                        )
                    else:
                        if ids_payload.get("be_trigger_tp_index") in (None, ""):
                            # Legacy rows created before the setting was persisted use
                            # the current user setting as a compatibility fallback.
                            user_settings = await get_user_settings_cache().get_or_fetch(
                                (user_id, "settings"),
                                lambda: db.get_user_settings(user_id),
                            )
                        trigger_idx = _resolve_be_trigger_index(
                            ids_payload, user_settings
                        )
                        if trigger_idx <= 0 and not manual_requested:
                            continue

                        if plan_items:
                            if manual_requested:
                                # A manual BE request must preserve every TP that has
                                # not actually been confirmed filled.
                                filled_indexes = _filled_tp_indexes(ids_payload)
                                manual_prefix = 0
                                for item in plan_items:
                                    if int(item["tp_index"]) not in filled_indexes:
                                        break
                                    manual_prefix += 1
                                trigger_idx = manual_prefix
                            else:
                                trigger_idx = min(trigger_idx, len(plan_items))
                            original_qty = snapshot_total_qty(snapshot)
                            targets = [float(item["price"]) for item in plan_items]
                            plan_qtys = [float(item["qty"]) for item in plan_items]
                            plan_total = sum(plan_qtys)
                            pcts = [
                                (qty / plan_total * 100.0) if plan_total > 0 else 0.0
                                for qty in plan_qtys
                            ]
                            trigger_plan_index = (
                                int(plan_items[trigger_idx - 1]["tp_index"])
                                if trigger_idx > 0
                                else 0
                            )
                        else:
                            if manual_requested:
                                await _write_status(
                                    "manual_required",
                                    "manual BE blocked: immutable TP plan snapshot is missing",
                                    {
                                        "be": {
                                            "moved": False,
                                            "manual_requested": False,
                                            "manual_required": True,
                                            "source": "manual",
                                            "manual_result": {
                                                "state": "snapshot_missing",
                                                "reason": "В базе нет неизменяемого TP-плана этой сделки.",
                                            },
                                        }
                                    },
                                )
                                continue
                            targets = _json_float_list(row.get("targets_json"))
                            pcts = _json_float_list(
                                row.get("tp_distribution_json")
                            )
                            trigger_plan_index = trigger_idx
                            if len(targets) < trigger_idx or len(pcts) < trigger_idx:
                                await _write_status(
                                    "manual_required",
                                    (
                                        "BE monitor cannot read a complete TP plan: "
                                        f"targets={len(targets)} pcts={len(pcts)} trigger={trigger_idx}"
                                    ),
                                    {
                                        "be": {
                                            "moved": False,
                                            "manual_required": True,
                                            "error": "invalid_tp_plan",
                                        }
                                    },
                                )
                                continue

                    # v1.6.104: durable BE retry cooldowns are respected for
                    # background scans and for repeated verifier attempts of the same
                    # TP event.  A genuinely later TP event may wake an older wait once
                    # (for example TP2 after a TP1 quantity-coverage wait), but the
                    # consumed TP index is stored so repeated TP2/TP3 touches cannot
                    # keep bypassing cooldown and hammer BingX trigger endpoints.
                    be_bypass_decision: dict[str, Any] = {"bypass": False}
                    if not manual_be_override and not recovery_be_override:
                        be_bypass_decision = _be_waiting_tp_event_bypass_decision(
                            be_state,
                            event_level_index=event_level_index,
                            trigger_plan_index=trigger_plan_index,
                            rows_override_present=rows_override is not None,
                        )
                        deferred_until = be_bypass_decision.get("deferred_until")
                        if deferred_until is not None:
                            event_idx = int(be_bypass_decision.get("event_idx") or 0)
                            if be_bypass_decision.get("bypass"):
                                log.info(
                                    "BE retry cooldown bypassed once by later TP event "
                                    "execution_id=%s event_tp=%s trigger_tp=%s waiting_trigger_tp=%s until=%s reason=%s",
                                    execution_id,
                                    event_idx,
                                    trigger_plan_index,
                                    be_bypass_decision.get("waiting_trigger_idx"),
                                    _iso_utc(deferred_until),
                                    be_state.get("waiting_reason"),
                                )
                                be_state = {
                                    **be_state,
                                    **_be_waiting_tp_bypass_marker(be_bypass_decision),
                                }
                            else:
                                log.info(
                                    "BE retry deferred execution_id=%s until=%s reason=%s event_tp=%s trigger_tp=%s defer_reason=%s",
                                    execution_id,
                                    _iso_utc(deferred_until),
                                    be_state.get("waiting_reason"),
                                    event_idx,
                                    trigger_plan_index,
                                    be_bypass_decision.get("reason"),
                                )
                                continue

                            # If the configured trigger would close 100% of the position, BE is pointless.
                    cumulative_pct = (
                        sum(max(0.0, p) for p in pcts[:trigger_idx]) / 100.0
                    )
                    _tp_list = ids_payload.get("tp") or []
                    conflict_source_rows = (
                        _tp_list if manual_requested else _tp_list[:trigger_idx]
                    )
                    conflict_rows = [
                        item
                        for item in conflict_source_rows
                        if isinstance(item, dict)
                        and (
                            tp_row_has_unresolved_qty_conflict(item)
                            or tp_row_has_unresolved_identity_conflict(item)
                        )
                    ]
                    if conflict_rows and not recovery_be_override:
                        conflict_indices = [
                            int(item.get("tp_index") or index + 1)
                            for index, item in enumerate(conflict_rows)
                        ]
                        current_be_state = ids_payload.get("be") or {}
                        if current_be_state.get("manual_required") is not True:
                            await _write_status(
                                "manual_required",
                                "BE blocked: unresolved TP ledger identity/quantity conflict",
                                {
                                    "be": {
                                        "moved": False,
                                        "manual_required": True,
                                        "error": "tp_ledger_conflict",
                                        "trigger_tp_index": trigger_plan_index,
                                        "trigger_ordinal": trigger_idx,
                                        "conflict_tp_indices": conflict_indices,
                                        **(
                                            {
                                                "manual_requested": False,
                                                "source": "manual",
                                                "manual_result": {
                                                    "state": "tp_ledger_conflict",
                                                    "reason": "Сохранённые TP имеют конфликт идентичности или объёма.",
                                                },
                                            }
                                            if manual_requested
                                            else {}
                                        ),
                                    }
                                },
                            )
                            await _notify(
                                notify,
                                user_id,
                                card(
                                    "🚨 <b>Б/У ЗАБЛОКИРОВАН</b>",
                                    symbol=symbol,
                                    side=side,
                                    blocks=(
                                        [
                                            "⚠️ Обнаружен конфликт сохранённых TP-данных",
                                            f"🎯 <b>Проблемные TP:</b> {', '.join(map(str, conflict_indices))}",
                                        ],
                                        [
                                            "🛡 Биржевые TP/SL автоматически не изменялись",
                                            "📱 Проверьте позицию вручную",
                                        ],
                                    ),
                                ),
                            )
                            moved += 1
                        continue
                    if plan_items:
                        # The immutable snapshot contains exact exchange-sized slices,
                        # including lot-step rounding and any MIN_TP_RR filtering.
                        _placed_through_trigger = sum(
                            float(item["qty"]) for item in plan_items[:trigger_idx]
                        )
                    else:
                        # Legacy fallback: prefer actual placed TP quantities.
                        _placed_through_trigger = sum(
                            float(t.get("qty") or t.get("actual_tp_qty") or 0.0)
                            for t in _tp_list[:trigger_idx]
                            if isinstance(t, dict)
                        )
                    if _placed_through_trigger > 0:
                        expected_qty_after_trigger = max(
                            0.0, original_qty - _placed_through_trigger
                        )
                    else:
                        expected_qty_after_trigger = max(
                            0.0, original_qty * (1.0 - cumulative_pct)
                        )
                    tolerance = max(original_qty * 0.005, 1e-10)

                    # This state is shared by automatic and manual BE. Once one
                    # path has confirmed the replacement, every later monitor
                    # pass must stop before any skip/write branch can regress
                    # ``moved=True`` or create a duplicate STOP.
                    be_state = ids_payload.get("be") or {}
                    if be_state.get("moved") is True or (
                        be_state.get("skipped") is True and not manual_requested
                    ):
                        continue

                    if (
                        expected_qty_after_trigger <= tolerance
                        and not manual_requested
                        and not recovery_be_override
                    ):
                        await _write_status(
                            status,
                            f"БУ после TP{trigger_plan_index} пропущено: после этого TP позиция должна быть закрыта полностью",
                            {
                                "be": {
                                    "moved": False,
                                    "skipped": "trigger_closes_full_position",
                                    "trigger_tp_index": trigger_plan_index,
                                    "trigger_ordinal": trigger_idx,
                                }
                            },
                        )
                        continue

                    exchange = str(
                        ids_payload.get("exchange")
                        or getattr(user_settings, "exchange", "")
                        or get_settings().safe_default_exchange
                    ).lower()
                    api_row = await get_api_key_cache().get_or_fetch(
                        (user_id, "api", exchange),
                        lambda: db.get_api_key(user_id, exchange),
                    )
                    if not api_row:
                        if manual_requested:
                            await _write_status(
                                status,
                                "manual BE blocked: BingX API is not connected",
                                {
                                    "be": {
                                        "moved": False,
                                        "manual_requested": False,
                                        "manual_required": False,
                                        "source": "manual",
                                        "skipped": None,
                                        "error": None,
                                        "manual_result": {
                                            "state": "api_missing",
                                            "reason": "BingX API не подключён.",
                                        },
                                    }
                                },
                            )
                        continue
                    cache_key = (user_id, exchange)
                    adapter = adapter_cache.get(cache_key)
                    if adapter is None:
                        adapter = build_adapter(api_row)
                        if market_event_exchange_context is not None:
                            adapter = market_event_exchange_context.wrap_adapter(
                                adapter, cache_key
                            )
                        adapter_cache[cache_key] = adapter

                        # Extra price audit: touching TP alone is NOT enough for BE,
                        # but we remember that price really reached the configured TP.
                    trigger_target = (
                        float(targets[trigger_idx - 1]) if trigger_idx > 0 else 0.0
                    )
                    current_price = 0.0
                    stop_reference_price = 0.0
                    price_seen_now = False
                    try:
                        if recovery_be_override:
                            current_price = 0.0
                            stop_reference_price = 0.0
                        elif manual_requested:
                            # BingX STOP plans in this project trigger from fair
                            # price.  A forced BE therefore verifies both latest
                            # and fair price before creating the replacement STOP.
                            prices = await pre_read_context.get_market_prices(
                                adapter, user_id, exchange, symbol
                            )
                            current_price = float(
                                (prices or {}).get("last")
                                or (market_prices or {}).get(symbol)
                                or 0.0
                            )
                            stop_reference_price = float(
                                (prices or {}).get("fair") or 0.0
                            )
                        else:
                            current_price = await pre_read_context.get_last_price(
                                adapter,
                                user_id,
                                exchange,
                                symbol,
                                provided_last=(market_prices or {}).get(symbol),
                            )
                            stop_reference_price = current_price
                        if trigger_idx > 0:
                            price_seen_now = _price_reached_tp(
                                side, current_price, trigger_target
                            )
                    except Exception as price_exc:
                        current_price = 0.0
                        stop_reference_price = 0.0
                        be_state.setdefault("price_check_errors", []).append(
                            f"{type(price_exc).__name__}: {str(price_exc)[:200]}"
                        )

                    price_seen_before = be_state.get("tp_price_seen") is True
                    price_seen = bool(price_seen_before or price_seen_now)
                    if trigger_idx > 0 and price_seen and not price_seen_before:
                        await _write_status(
                            status,
                            f"TP{trigger_plan_index} price touch recorded; waiting for actual position reduction",
                            {
                                "be": {
                                    "moved": False,
                                    "trigger_tp_index": trigger_plan_index,
                                    "trigger_ordinal": trigger_idx,
                                    "tp_price_seen": True,
                                    "tp_price_seen_at": current_price,
                                    "tp_target": trigger_target,
                                }
                            },
                        )
                        row = await db.get_execution_by_id(execution_id) or row
                        ids_payload = _json_dict(row.get("exchange_order_ids_json"))
                        be_state = ids_payload.get("be") or {}

                    positions = await pre_read_context.get_positions(
                        adapter, user_id, exchange, symbol, side.upper()
                    )
                    qty_now = _total_position_size(positions)
                    if not recovery_be_override:
                        qty_now = min(qty_now, original_qty)

                    # Track which TPs have been filled (qty decreased through them).
                    # Used by lifecycle_guard to compute realized PnL on close.
                    if _mark_filled_tps_by_qty(
                        ids_payload, original_qty, qty_now, side
                    ):
                        try:
                            await db.merge_execution_metadata(
                                execution_id,
                                {"tp": ids_payload.get("tp")},
                                write_flow_audit_stage="be_monitor_mark_filled_tps",
                                write_flow_audit_status=status,
                            )
                        except Exception as _mf_exc:
                            log.debug("filled-mark persist failed: %s", _mf_exc)

                    if manual_requested and plan_items:
                        # Position reduction plus durable TP evidence may have
                        # confirmed another TP during this same pass. Recompute
                        # the contiguous completed prefix before BE calculation
                        # and TP recreation so a filled target is never recreated.
                        filled_indexes = _filled_tp_indexes(ids_payload)
                        trigger_idx = 0
                        for item in plan_items:
                            if int(item["tp_index"]) not in filled_indexes:
                                break
                            trigger_idx += 1
                        trigger_plan_index = (
                            int(plan_items[trigger_idx - 1]["tp_index"])
                            if trigger_idx > 0
                            else 0
                        )

                    if qty_now <= tolerance:
                        # A cached zero can only be used as a hint. Confirm it
                        # with a direct fresh private positions read before
                        # changing lifecycle state to closed_on_exchange.
                        positions = await pre_read_context.get_positions(
                            adapter,
                            user_id,
                            exchange,
                            symbol,
                            side.upper(),
                            force_refresh=True,
                        )
                        qty_now = _total_position_size(positions)
                    if not recovery_be_override:
                        qty_now = min(qty_now, original_qty)

                    if qty_now <= tolerance:
                        await _write_status(
                            "closed_on_exchange",
                            "позиция закрыта на бирже до переноса STOP в БУ",
                            {
                                "be": {
                                    "moved": False,
                                    "skipped": "position_closed_before_be",
                                    "trigger_tp_index": trigger_plan_index,
                                    "trigger_ordinal": trigger_idx,
                                    "tp_price_seen": price_seen,
                                    "last_price": current_price,
                                    **(
                                        {
                                            "manual_requested": False,
                                            "source": "manual",
                                            "manual_result": {
                                                "state": "already_closed",
                                                "reason": "Позиция уже закрыта на BingX.",
                                            },
                                        }
                                        if manual_requested
                                        else {}
                                    ),
                                }
                            },
                        )
                        continue

                        # BE trigger needs two different facts:
                        # A) qty_now dropped to or below the expected threshold;
                        # B) TP execution is supported by a durable price touch or by
                        #    exact BingX stop-order history. Quantity reduction alone is
                        #    not enough because the trader may close part manually.
                    qty_trigger = (
                        True
                        if manual_requested or recovery_be_override
                        else qty_now <= expected_qty_after_trigger + tolerance
                    )
                    strong_qty = (
                        False
                        if manual_requested
                        else qty_now <= expected_qty_after_trigger - tolerance
                    )

                    # Check the first trigger_idx TPs. A filled flag created
                    # only from quantity arithmetic is not independent evidence:
                    # a manual partial close can produce the same position delta.
                    filled_through_trigger = True
                    history_confirmed_through_trigger = True
                    _tp_check_list = ids_payload.get("tp") or []
                    for _i in range(trigger_idx):
                        if _i >= len(_tp_check_list):
                            filled_through_trigger = False
                            history_confirmed_through_trigger = False
                            break
                        _tp_item = _tp_check_list[_i]
                        if not isinstance(_tp_item, dict) or _tp_item.get("filled") is not True:
                            filled_through_trigger = False
                            history_confirmed_through_trigger = False
                            break
                        if (
                            str(_tp_item.get("fill_source") or "")
                            != "mexc_stoporder_history"
                        ):
                            history_confirmed_through_trigger = False

                    if not qty_trigger:
                        continue

                    # G63: a Railway restart can happen after the TP fill but
                    # before the public-price event gate observed the touch. In
                    # that short startup window, quantity reduction is only a
                    # reason to perform an exact read-only BingX history check.
                    # The same lifecycle exact plan->child identity bridge is
                    # required; quantity/price alone still cannot authorize BE.
                    if (
                        not manual_requested
                        and not recovery_be_override
                        and not (price_seen or history_confirmed_through_trigger)
                        and _restart_tp_history_recovery_active()
                    ):
                        await _recover_restart_tp_history_exact(
                            adapter=adapter,
                            execution_id=execution_id,
                            user_id=user_id,
                            symbol=symbol,
                            side=side,
                            created_at=row.get("created_at"),
                            status=status,
                            ids_payload=ids_payload,
                        )
                        _tp_check_list = ids_payload.get("tp") or []
                        filled_through_trigger = True
                        history_confirmed_through_trigger = True
                        for _i in range(trigger_idx):
                            if _i >= len(_tp_check_list):
                                filled_through_trigger = False
                                history_confirmed_through_trigger = False
                                break
                            _tp_item = _tp_check_list[_i]
                            if (
                                not isinstance(_tp_item, dict)
                                or _tp_item.get("filled") is not True
                            ):
                                filled_through_trigger = False
                                history_confirmed_through_trigger = False
                                break
                            if (
                                str(_tp_item.get("fill_source") or "")
                                != "mexc_stoporder_history"
                            ):
                                history_confirmed_through_trigger = False

                    if (
                        not manual_requested
                        and not recovery_be_override
                        and not (price_seen or history_confirmed_through_trigger)
                    ):
                        # Fail safe: quantity reduction, even a large one, can be
                        # manual. Move to BE only after a TP price touch or exact
                        # BingX stop-order history confirmation.
                        log.info(
                            "be_monitor: %s %s qty_trigger=True but TP execution is not confirmed "
                            "(price_seen=%s, history_confirmed=%s, filled_through_trigger=%s, strong_qty=%s); "
                            "qty=%.6f threshold=%.6f; waiting",
                            symbol,
                            side,
                            price_seen,
                            history_confirmed_through_trigger,
                            filled_through_trigger,
                            strong_qty,
                            qty_now,
                            expected_qty_after_trigger,
                        )
                        continue

                        # Never cancel/recreate live TP/SL for a trade that has no
                        # immutable final-fill plan. Older executions remain protected
                        # by their current exchange orders and require manual review.
                    if snapshot is None and not recovery_be_override:
                        await _write_status(
                            "manual_required",
                            f"BE after TP{trigger_plan_index} blocked: immutable TP plan snapshot is missing",
                            {
                                "be": {
                                    "moved": False,
                                    "manual_required": True,
                                    "trigger_tp_index": trigger_plan_index,
                                    "trigger_ordinal": trigger_idx,
                                    "qty": qty_now,
                                    "error": "missing_tp_plan_snapshot",
                                }
                            },
                        )
                        await _notify(
                            notify,
                            user_id,
                            card(
                                "🚨 <b>Б/У ТРЕБУЕТ РУЧНОЙ ПРОВЕРКИ</b>",
                                symbol=symbol,
                                side=side,
                                blocks=(
                                    [
                                        f"🎯 <b>Сработал:</b> TP{trigger_plan_index}",
                                        f"📦 <b>Остаток позиции:</b> {fmt_qty(qty_now)}",
                                    ],
                                    [
                                        "❌ В базе нет зафиксированного TP-плана этой сделки",
                                        "🛡 Текущие биржевые TP/SL не отменялись",
                                        "📱 Проверьте позицию вручную",
                                    ],
                                ),
                            ),
                        )
                        moved += 1
                        continue

                    # The bounded pass-local snapshot is only a pre-filter.
                    # Any row that is about to enter BE replacement gets one
                    # direct fresh positions read so STOP qty/positionId are
                    # never derived from a 15-second-old snapshot.
                    positions = await pre_read_context.get_positions(
                        adapter,
                        user_id,
                        exchange,
                        symbol,
                        side.upper(),
                        force_refresh=True,
                    )
                    qty_now = _total_position_size(positions)
                    if not recovery_be_override:
                        qty_now = min(qty_now, original_qty)
                    if qty_now <= tolerance:
                        await _write_status(
                            "closed_on_exchange",
                            "позиция закрыта на бирже до переноса STOP в БУ (fresh confirmation)",
                            {
                                "be": {
                                    "moved": False,
                                    "skipped": "position_closed_before_be",
                                    "trigger_tp_index": trigger_plan_index,
                                    "trigger_ordinal": trigger_idx,
                                    "tp_price_seen": price_seen,
                                    "last_price": current_price,
                                    **(
                                        {
                                            "manual_requested": False,
                                            "source": "manual",
                                            "manual_result": {
                                                "state": "already_closed",
                                                "reason": "Позиция уже закрыта на BingX.",
                                            },
                                        }
                                        if manual_requested
                                        else {}
                                    ),
                                }
                            },
                        )
                        continue
                    if (
                        not manual_requested
                        and not recovery_be_override
                        and qty_now > expected_qty_after_trigger + tolerance
                    ):
                        log.info(
                            "be_monitor: %s %s fresh qty no longer satisfies BE trigger; "
                            "qty=%.6f threshold=%.6f; retry later",
                            symbol,
                            side,
                            qty_now,
                            expected_qty_after_trigger,
                        )
                        continue
                    if _mark_filled_tps_by_qty(
                        ids_payload, original_qty, qty_now, side
                    ):
                        try:
                            await db.merge_execution_metadata(
                                execution_id,
                                {"tp": ids_payload.get("tp")},
                                write_flow_audit_stage="be_monitor_mark_filled_tps_fresh",
                                write_flow_audit_status=status,
                            )
                        except Exception as _mf_exc:
                            log.debug("fresh filled-mark persist failed: %s", _mf_exc)
                    if manual_requested and plan_items:
                        filled_indexes = _filled_tp_indexes(ids_payload)
                        trigger_idx = 0
                        for item in plan_items:
                            if int(item["tp_index"]) not in filled_indexes:
                                break
                            trigger_idx += 1
                        trigger_plan_index = (
                            int(plan_items[trigger_idx - 1]["tp_index"])
                            if trigger_idx > 0
                            else 0
                        )

                    position_id = _position_id(positions)
                    if not position_id:
                        await _write_status(
                            "manual_required",
                            f"BE after TP{trigger_plan_index} blocked: BingX positionId is missing",
                            {
                                "be": {
                                    "moved": False,
                                    "manual_required": True,
                                    "trigger_tp_index": trigger_plan_index,
                                    "trigger_ordinal": trigger_idx,
                                    "qty": qty_now,
                                    "error": "position_id_missing",
                                    **(
                                        {
                                            "manual_requested": False,
                                            "source": "manual",
                                            "manual_result": {
                                                "state": "position_id_missing",
                                                "reason": "BingX не вернула точный positionId.",
                                            },
                                        }
                                        if manual_requested
                                        else {}
                                    ),
                                }
                            },
                        )
                        await _notify(
                            notify,
                            user_id,
                            card(
                                "🔴 <b>Б/У НЕ УСТАНОВЛЕН</b>",
                                symbol=symbol,
                                side=side,
                                blocks=(
                                    [
                                        f"📦 <b>Остаток позиции:</b> {fmt_qty(qty_now)}"
                                    ],
                                    [
                                        "❌ BingX не вернула positionId",
                                        "📱 Проверьте STOP и TP вручную",
                                    ],
                                ),
                            ),
                        )
                        moved += 1
                        continue

                    actual_entry = _f(ids_payload.get("actual_entry"), 0.0)
                    if actual_entry <= 0 and positions:
                        actual_entry = _position_entry_price(positions[0])
                    if actual_entry <= 0:
                        actual_entry = signal_entry

                        # Try to read exchange-calculated break-even price from position.
                        # If available, it's used as the authoritative base for our stop.
                    exchange_be = 0.0
                    if positions:
                        exchange_be = _position_breakeven_price(positions[0])

                    if recovery_be_override:
                        recovery_intent = (
                            be_state.get("replacement_write_intent_v1")
                            if isinstance(
                                be_state.get("replacement_write_intent_v1"), dict
                            )
                            else {}
                        )
                        replacement_checkpoint = (
                            be_state.get("replacement_stop")
                            if isinstance(be_state.get("replacement_stop"), dict)
                            else {}
                        )
                        durable_recovery_stop = _f(
                            recovery_intent.get("stop")
                            or be_state.get("stop")
                            or replacement_checkpoint.get("stopLossPrice")
                            or replacement_checkpoint.get("triggerPrice")
                            or replacement_checkpoint.get("stopPrice"),
                            0.0,
                        )
                        if durable_recovery_stop <= 0:
                            raise RuntimeError(
                                "durable BE recovery checkpoint has no exact positive STOP price"
                            )
                        be_calc = {
                            "final_stop": durable_recovery_stop,
                            "recovery_checkpoint_driven": True,
                            "source": "durable_replacement_checkpoint",
                            "no_loss_boundary": durable_recovery_stop,
                        }
                        raw_calculated_price = durable_recovery_stop
                    else:
                        be_calc = _real_trade_breakeven_stop(
                            side,
                            actual_entry,
                            original_qty,
                            qty_now,
                            targets,
                            pcts,
                            trigger_idx,
                            exchange_be_price=exchange_be,
                        )
                        raw_calculated_price = float(be_calc["final_stop"])
                    price = raw_calculated_price
                    stop_qty = qty_now
                    replacement_response_id = ""
                    if manual_requested and not recovery_be_override:
                        # A forced BE must never submit a STOP that is already on
                        # the wrong side of the live market and would trigger
                        # immediately. Automatic BE reaches this area only after
                        # TP evidence; the manual path needs an explicit market
                        # safety gate.
                        try:
                            safety_info = await adapter.instrument_info(symbol)
                            safety_tick = float(
                                getattr(safety_info, "price_tick", 0.0) or 0.0
                            )
                        except Exception:
                            safety_tick = 0.0
                        safety_gap = max(
                            safety_tick * 2.0,
                            abs(float(price)) * 1e-8,
                            1e-12,
                        )
                        latest_safe = current_price > 0 and (
                            (side == "long" and current_price > price + safety_gap)
                            or (side == "short" and current_price < price - safety_gap)
                        )
                        fair_safe = stop_reference_price > 0 and (
                            (
                                side == "long"
                                and stop_reference_price > price + safety_gap
                            )
                            or (
                                side == "short"
                                and stop_reference_price < price - safety_gap
                            )
                        )
                        market_safe = bool(latest_safe and fair_safe)
                        if not market_safe:
                            await _write_status(
                                status,
                                "manual BE rejected because the market is not beyond the calculated BE stop",
                                {
                                    "be": {
                                        "moved": False,
                                        "manual_requested": False,
                                        "manual_required": False,
                                        "source": "manual",
                                        "skipped": None,
                                        "error": None,
                                        "manual_result": {
                                            "state": "market_not_safe",
                                            "reason": (
                                                "Текущая цена ещё не позволяет безопасно поставить Б/У: "
                                                "STOP мог бы сработать сразу."
                                            ),
                                            "stop": price,
                                            "market_price": current_price,
                                            "fair_price": stop_reference_price,
                                            "required_gap": safety_gap,
                                        },
                                    }
                                },
                            )
                            continue
                    client_id = _stable_client_id(
                        "avc-be-manual" if manual_requested else "avc-be",
                        execution_id,
                        user_id,
                        trigger_plan_index,
                    )
                    pre_client_id = _stable_client_id(
                        "avc-be-manual-pre" if manual_requested else "avc-be-pre",
                        execution_id,
                        user_id,
                        trigger_plan_index,
                    )
                    if recovery_be_override:
                        recovery_intent_client_id = clean_exchange_id(
                            (
                                be_state.get("replacement_write_intent_v1")
                                if isinstance(
                                    be_state.get("replacement_write_intent_v1"), dict
                                )
                                else {}
                            ).get("client_id")
                        )
                        if recovery_intent_client_id:
                            pre_client_id = recovery_intent_client_id
                    be_actions: list[dict[str, Any]] = []
                    remaining_tp_orders: list[dict[str, Any]] = []
                    claimed_limit_attached_stop: dict[str, Any] = {}
                    saved_replacement_payload = be_state.get("replacement_stop")
                    confirmed_replacement_stop: dict[str, Any] = (
                        dict(saved_replacement_payload)
                        if isinstance(saved_replacement_payload, dict)
                        and _algo_order_id(saved_replacement_payload)
                        else {}
                    )
                    updated_tp_rows: list[dict[str, Any]] = [
                        dict(item)
                        for item in (ids_payload.get("tp") or [])
                        if isinstance(item, dict)
                    ]
                    # Explicitly initialized for the later rate-limit handler.
                    # Avoid dynamic local lookups here: missing a future refactor
                    # should be a visible variable change, not a silent empty fallback.
                    live_owned_stop_ids: list[str] = []
                    live_owned_stop_ids_for_coverage: list[str] = []

                    # Exact TP/SL cancellation is isolated by durable BingX
                    # stop-plan ids.  Another same-symbol execution is therefore
                    # diagnostic context, not a reason to use/skip broad cancel.
                    other_rows = await db.other_active_symbol_executions(
                        user_id, symbol, execution_id, limit=5
                    )
                    if other_rows:
                        be_actions.append(
                            {
                                "type": "same_symbol_execution_present_exact_cleanup_safe",
                                "other_execution_ids": [
                                    int(r.get("id") or 0) for r in other_rows
                                ],
                            }
                        )

                    try:
                        async with db.symbol_action_lock(user_id, symbol):
                            # Re-check every safety predicate inside the same
                            # user+symbol critical section used by signal_executor.
                            # This closes the old time-of-check/time-of-use window.
                            if is_symbol_opening(user_id, symbol):
                                raise RuntimeError(
                                    f"BE exact replacement deferred: signal_executor is opening {symbol}"
                                )
                            normalize_request = getattr(
                                adapter, "normalize_position_tpsl_request", None
                            )
                            if callable(normalize_request):
                                normalized_stop = await normalize_request(
                                    symbol=symbol,
                                    side=side,
                                    qty=qty_now,
                                    price=raw_calculated_price,
                                    kind="sl",
                                )
                                price = _f(normalized_stop.get("price"), 0.0)
                                stop_qty = _f(normalized_stop.get("qty"), 0.0)
                                price_tick = _f(normalized_stop.get("price_tick"), 0.0)
                                qty_step = _f(normalized_stop.get("qty_step"), 0.0)
                            else:
                                # Compatibility fallback for test/dummy adapters.
                                # Production BingX uses normalize_position_tpsl_request
                                # so the durable intent and wire payload are identical.
                                verify_info = await adapter.instrument_info(symbol)
                                price_tick = _f(
                                    getattr(verify_info, "price_tick", 0.0), 0.0
                                )
                                qty_step = _f(
                                    getattr(verify_info, "qty_step", 0.0), 0.0
                                )
                                if price_tick > 0:
                                    if side == "long":
                                        price = (
                                            math.floor(
                                                raw_calculated_price / price_tick
                                                + 1e-12
                                            )
                                            * price_tick
                                        )
                                    else:
                                        price = (
                                            math.ceil(
                                                raw_calculated_price / price_tick
                                                - 1e-12
                                            )
                                            * price_tick
                                        )
                                if qty_step > 0:
                                    stop_qty = (
                                        math.floor(qty_now / qty_step + 1e-12)
                                        * qty_step
                                    )
                            if price <= 0 or stop_qty <= 0:
                                raise RuntimeError(
                                    "BE STOP normalization produced a non-positive price or quantity"
                                )
                            no_loss_boundary = _f(be_calc.get("no_loss_boundary"), 0.0)
                            boundary_tolerance = max(
                                price_tick * 1e-9,
                                abs(no_loss_boundary) * 1e-12,
                                1e-12,
                            )
                            normalized_is_worse = bool(
                                no_loss_boundary > 0
                                and (
                                    (
                                        side == "long"
                                        and price + boundary_tolerance
                                        < no_loss_boundary
                                    )
                                    or (
                                        side == "short"
                                        and price - boundary_tolerance
                                        > no_loss_boundary
                                    )
                                )
                            )
                            no_loss_tick_adjusted = False
                            if normalized_is_worse:
                                if price_tick <= 0:
                                    raise RuntimeError(
                                        "BE STOP rounding crossed the no-loss boundary and BingX price tick is unavailable"
                                    )
                                adjusted_target = _tick_at_or_better(
                                    side, no_loss_boundary, price_tick
                                )
                                if adjusted_target <= 0:
                                    raise RuntimeError(
                                        "BE STOP no-loss tick adjustment failed"
                                    )
                                if callable(normalize_request):
                                    normalized_stop = await normalize_request(
                                        symbol=symbol,
                                        side=side,
                                        qty=qty_now,
                                        price=adjusted_target,
                                        kind="sl",
                                    )
                                    price = _f(normalized_stop.get("price"), 0.0)
                                    stop_qty = _f(normalized_stop.get("qty"), 0.0)
                                else:
                                    price = adjusted_target
                                no_loss_tick_adjusted = True
                                still_worse = bool(
                                    (
                                        side == "long"
                                        and price + boundary_tolerance
                                        < no_loss_boundary
                                    )
                                    or (
                                        side == "short"
                                        and price - boundary_tolerance
                                        > no_loss_boundary
                                    )
                                )
                                if price <= 0 or stop_qty <= 0 or still_worse:
                                    raise RuntimeError(
                                        "BE STOP could not be normalized to an exchange tick without crossing the no-loss boundary"
                                    )
                            be_calc = {
                                **be_calc,
                                "raw_final_stop": raw_calculated_price,
                                "final_stop": price,
                                "submitted_stop": price,
                                "submitted_qty": stop_qty,
                                "price_tick": price_tick,
                                "qty_step": qty_step,
                                "no_loss_tick_adjusted": no_loss_tick_adjusted,
                            }
                            verify_price_tolerance = max(
                                price_tick * 0.51,
                                abs(float(price)) * 1e-9,
                                1e-12,
                            )
                            verify_qty_tolerance = max(
                                qty_step * 0.51,
                                abs(float(stop_qty)) * 1e-12,
                                1e-12,
                            )

                            # Snapshot only conditional ids that this execution
                            # durably owns.  Manual/external TP/SL attached to the
                            # same position are deliberately preserved.
                            all_algo_rows = [
                                row
                                for row in list(
                                    await adapter.fetch_open_algo_orders(symbol) or []
                                )
                                if isinstance(row, dict)
                            ]
                            tracked_algo_ids, _tracked_position_ids = (
                                _saved_conditional_identity(ids_payload)
                            )
                            live_rows_by_id = {
                                _algo_order_id(row): row
                                for row in all_algo_rows
                                if _algo_order_id(row)
                            }
                            tracked_live_algo_ids = {
                                order_id
                                for order_id in tracked_algo_ids
                                if order_id in live_rows_by_id
                            }
                            if exchange == "bingx":
                                # v1.6.96: BingX BE must not cancel/recreate TP.
                                # Only old protective STOP ids participate in BE
                                # replacement cleanup; tracked TP ids stay live.
                                old_order_ids = {
                                    order_id
                                    for order_id in tracked_live_algo_ids
                                    if _looks_stop_order(live_rows_by_id[order_id])
                                }
                                tracked_tp_ids_preserved_for_be = sorted(
                                    tracked_live_algo_ids - old_order_ids
                                )
                            else:
                                old_order_ids = set(tracked_live_algo_ids)
                                tracked_tp_ids_preserved_for_be = []

                            # LIMIT order/create returns only the regular entry id.
                            # After fill BingX exposes the attached STOP as a separate
                            # stop-plan id, so older rows did not durably own that STOP
                            # and BE preserved it as if it were manual. Claim it only
                            # with exact position/side/original-price/quantity evidence
                            # (or an exact placeOrderId -> entry link). Ambiguity still
                            # fails closed and preserves every untracked order.
                            original_stop_for_ownership = _f(
                                (snapshot or {}).get("stop"), 0.0
                            ) or _f(row.get("stop"), 0.0)
                            # Legacy g12 MARKET entries could briefly miss the
                            # attached STOP, create one explicit fallback, and later
                            # expose both at the same original price. Collapse only a
                            # durably known fallback while another full-coverage STOP
                            # remains live; ambiguity still fails closed.
                            try:
                                (
                                    all_algo_rows,
                                    live_rows_by_id,
                                    removed_fallback_ids,
                                    legacy_cleanup_audit,
                                ) = await _collapse_legacy_market_duplicate_initial_stops(
                                    adapter,
                                    rows=all_algo_rows,
                                    ids_payload=ids_payload,
                                    symbol=symbol,
                                    side=side,
                                    position_id=position_id,
                                    original_stop=original_stop_for_ownership,
                                    minimum_qty=qty_now,
                                    price_tolerance=verify_price_tolerance,
                                    qty_tolerance=verify_qty_tolerance,
                                    user_id=user_id,
                                    exchange=exchange,
                                    invalidate_reads=_invalidate_pre_reads,
                                )
                                old_order_ids.difference_update(removed_fallback_ids)
                                if legacy_cleanup_audit:
                                    be_actions.append(legacy_cleanup_audit)
                            except _LegacyDuplicateStopCleanupError as cleanup_exc:
                                be_actions.append(cleanup_exc.audit)
                                raise RuntimeError(str(cleanup_exc)) from cleanup_exc

                            attached_stop_ownership = identify_limit_attached_stop(
                                all_algo_rows,
                                payload=ids_payload,
                                position_id=position_id,
                                side=side,
                                original_stop=original_stop_for_ownership,
                                minimum_qty=qty_now,
                                price_tolerance=verify_price_tolerance,
                                qty_tolerance=verify_qty_tolerance,
                                excluded_order_ids=old_order_ids,
                            )
                            claimed_stop_id = str(
                                attached_stop_ownership.get("order_id") or ""
                            ).strip()
                            if claimed_stop_id:
                                old_order_ids.add(claimed_stop_id)
                                claimed_limit_attached_stop = (
                                    build_limit_attached_stop_record(
                                        attached_stop_ownership,
                                        position_id=position_id,
                                        original_stop=original_stop_for_ownership,
                                    )
                                )
                                be_actions.append(
                                    {
                                        "type": "limit_attached_stop_claimed",
                                        "stop_order_id": claimed_stop_id,
                                        "basis": attached_stop_ownership.get("basis"),
                                        "entry_order_ids": attached_stop_ownership.get(
                                            "entry_order_ids"
                                        ),
                                    }
                                )
                            elif attached_stop_ownership.get("ambiguous"):
                                be_actions.append(
                                    {
                                        "type": "limit_attached_stop_ambiguous",
                                        "candidate_ids": attached_stop_ownership.get(
                                            "candidate_ids"
                                        ),
                                        "reason": attached_stop_ownership.get("reason"),
                                    }
                                )

                            initial_stop_ownership = identify_initial_protective_stop(
                                all_algo_rows,
                                payload=ids_payload,
                                position_id=position_id,
                                side=side,
                                original_stop=original_stop_for_ownership,
                                minimum_qty=qty_now,
                                price_tolerance=verify_price_tolerance,
                                qty_tolerance=verify_qty_tolerance,
                                excluded_order_ids=old_order_ids,
                            )
                            claimed_initial_stop_id = str(
                                initial_stop_ownership.get("order_id") or ""
                            ).strip()
                            claimed_initial_stop = {}
                            if claimed_initial_stop_id:
                                old_order_ids.add(claimed_initial_stop_id)
                                claimed_initial_stop = build_initial_stop_record(
                                    initial_stop_ownership,
                                    position_id=position_id,
                                    original_stop=original_stop_for_ownership,
                                )
                                be_actions.append(
                                    {
                                        "type": "initial_stop_claimed_for_be",
                                        "stop_order_id": claimed_initial_stop_id,
                                        "basis": initial_stop_ownership.get("basis"),
                                    }
                                )
                            elif initial_stop_ownership.get("ambiguous"):
                                be_actions.append(
                                    {
                                        "type": "initial_stop_ambiguous_for_be",
                                        "candidate_ids": initial_stop_ownership.get(
                                            "candidate_ids"
                                        ),
                                        "reason": initial_stop_ownership.get("reason"),
                                    }
                                )

                            be_actions.append(
                                {
                                    "type": "be_old_stop_ownership_v1",
                                    "tracked_metadata_ids": sorted(tracked_algo_ids),
                                    "live_algo_ids": sorted(live_rows_by_id),
                                    "old_stop_ids_for_be_write": sorted(old_order_ids),
                                    "tracked_tp_ids_preserved_for_be": tracked_tp_ids_preserved_for_be,
                                    "claimed_limit_attached_stop_id": claimed_stop_id,
                                    "claimed_initial_stop_id": claimed_initial_stop_id,
                                    "claimed_initial_stop": claimed_initial_stop,
                                }
                            )

                            # v1.6.100: full-quantity coverage is a precondition
                            # for every replacement path, including resumed write intents
                            # and checkpointed/reused BE STOPs.  v1.6.99 guarded only the
                            # fresh create branch; an under-covered checkpoint could still
                            # be reused and cause the old full STOP to be cancelled.
                            live_owned_stop_ids_for_coverage = sorted(
                                order_id
                                for order_id in old_order_ids
                                if isinstance(live_rows_by_id.get(order_id), dict)
                                and _looks_stop_order(live_rows_by_id[order_id])
                            )
                            coverage_tolerance_pre = max(
                                abs(float(qty_now)) * 1e-9,
                                abs(float(stop_qty)) * 1e-9,
                                1e-12,
                            )
                            uncovered_qty_pre = max(0.0, float(qty_now) - float(stop_qty))
                            if uncovered_qty_pre > coverage_tolerance_pre:
                                coverage_audit_pre = {
                                    "type": "be_stop_full_qty_coverage_pre_reuse_block_v1_6_100",
                                    "position_qty": float(qty_now),
                                    "normalized_stop_qty": float(stop_qty),
                                    "uncovered_qty": float(uncovered_qty_pre),
                                    "qty_step": float(qty_step),
                                    "coverage_tolerance": float(coverage_tolerance_pre),
                                }
                                be_actions.append(coverage_audit_pre)
                                if not live_owned_stop_ids_for_coverage:
                                    raise RuntimeError(
                                        "new BE STOP quantity would not cover the full live position "
                                        "before replacement reconciliation and no exact old protective STOP is visible: "
                                        f"position_qty={qty_now}, stop_qty={stop_qty}, uncovered={uncovered_qty_pre}"
                                    )
                                now_iso = datetime.now(timezone.utc).isoformat()
                                waiting_since = str(
                                    be_state.get("waiting_since")
                                    or be_state.get("waiting_qty_coverage_since")
                                    or now_iso
                                )
                                waiting_patch = {
                                    "be": {
                                        "moved": False,
                                        "manual_required": False,
                                        "replacement_in_progress": False,
                                        "waiting_qty_coverage": True,
                                        "waiting_retry": True,
                                        "waiting_reason": "be_stop_qty_below_live_position",
                                        "waiting_since": waiting_since,
                                        "waiting_last_checked_at": now_iso,
                                        "waiting_next_retry_after": _be_retry_after_iso(_BE_QTY_COVERAGE_RETRY_SEC),
                                        "waiting_backoff_sec": _BE_QTY_COVERAGE_RETRY_SEC,
                                        "waiting_trigger_tp_index": trigger_plan_index,
                                        "waiting_trigger_ordinal": trigger_idx,
                                        "waiting_old_stop_ids": live_owned_stop_ids_for_coverage,
                                        "waiting_target_stop": price,
                                        "waiting_qty": stop_qty,
                                        "waiting_qty_coverage_audit": coverage_audit_pre,
                                        "source": "manual" if manual_requested else "automatic",
                                        "trigger_tp_index": trigger_plan_index,
                                        "trigger_ordinal": trigger_idx,
                                        "stop": price,
                                        "qty": stop_qty,
                                        "position_qty": qty_now,
                                        "original_qty": original_qty,
                                        "basis_entry": actual_entry,
                                        "signal_entry": signal_entry,
                                        "tp_price_seen": price_seen,
                                        "tp_price_seen_at": current_price,
                                        "tp_target": trigger_target,
                                        "calculation": be_calc,
                                        "actions": be_actions,
                                        **_be_waiting_tp_bypass_marker(be_bypass_decision, position_qty=qty_now),
                                        "error": None,
                                        **(
                                            {
                                                "manual_requested": False,
                                                "manual_result": {
                                                    "state": "qty_coverage_waiting",
                                                    "reason": (
                                                        "Новый BE STOP сейчас не покрывает весь остаток позиции; "
                                                        "старый STOP оставлен активным."
                                                    ),
                                                    "stop": price,
                                                    "qty": stop_qty,
                                                    "position_qty": qty_now,
                                                    "uncovered_qty": uncovered_qty_pre,
                                                    "next_retry_after": _be_retry_after_iso(_BE_QTY_COVERAGE_RETRY_SEC),
                                                },
                                            }
                                            if manual_requested
                                            else {}
                                        ),
                                    },
                                    "actual_entry": actual_entry,
                                }
                                await _write_status(
                                    status,
                                    (
                                        "manual BE waiting for full STOP quantity coverage"
                                        if manual_requested
                                        else f"BE waiting for full STOP quantity coverage after TP{trigger_plan_index}"
                                    ),
                                    waiting_patch,
                                )
                                await _notify(
                                    notify,
                                    user_id,
                                    card(
                                        "⏳ <b>Б/У ОТЛОЖЕН</b>",
                                        symbol=symbol,
                                        side=side,
                                        blocks=(
                                            [
                                                f"🆔 <b>Execution:</b> {execution_id}",
                                                (
                                                    "🔒 <b>Условие:</b> принудительный Б/У"
                                                    if manual_requested
                                                    else f"🎯 <b>Сработал:</b> TP{trigger_plan_index}"
                                                ),
                                                f"🛡 <b>Расчётный новый STOP:</b> {fmt_price(price)}",
                                                f"📦 <b>Остаток позиции:</b> {fmt_qty(qty_now)}",
                                            ],
                                            [
                                                "⏳ Новый BE STOP сейчас не покрывает весь остаток позиции",
                                                "✅ Старый STOP подтверждён и остаётся активным",
                                                "🔁 Бот повторит перенос в Б/У автоматически",
                                            ],
                                            [
                                                f"🧷 <b>Старый STOP:</b> {', '.join(live_owned_stop_ids_for_coverage[:3])}",
                                                f"📦 <b>STOP qty:</b> {fmt_qty(stop_qty)}",
                                                f"⚠️ <b>Без покрытия:</b> {fmt_qty(uncovered_qty_pre)}",
                                            ],
                                        ),
                                    ),
                                    event_key=f"execution:{execution_id}:be_qty_coverage_waiting",
                                )
                                await _invalidate_pre_reads(adapter, user_id, exchange, symbol)
                                moved += 1
                                continue

                            # A pre-write intent is persisted immediately before
                            # the BingX STOP write. v1.0.7g7 reconciles an unresolved
                            # checkpoint from the single fresh openOrders snapshot that
                            # was already fetched for this protected critical section.
                            # It never performs the old 12-read polling loop and never
                            # sends a third STOP while the checkpoint exists.
                            replacement_id = ""
                            existing_replacement: dict[str, Any] | None = None
                            existing_write_intent = be_state.get(
                                "replacement_write_intent_v1"
                            )
                            if isinstance(existing_write_intent, dict):
                                existing_write_intent = dict(existing_write_intent)
                                rate_limit_marker = (
                                    be_state.get("rate_limit_write_confirmation_v1")
                                    if _rate_limit_write_confirmation_pending(be_state)
                                    else {}
                                )
                                marker_old_stop_ids = (
                                    rate_limit_marker.get("old_stop_ids")
                                    if isinstance(rate_limit_marker, dict)
                                    and isinstance(
                                        rate_limit_marker.get("old_stop_ids"), list
                                    )
                                    else None
                                )
                                if marker_old_stop_ids is not None:
                                    # Deep-merge persistence intentionally unions
                                    # lists, so the original generic topology may
                                    # still contain TP ids. For 100410 recovery use
                                    # only the separately persisted exact STOP set.
                                    existing_write_intent["old_stop_ids"] = [
                                        clean_exchange_id(value)
                                        for value in marker_old_stop_ids
                                        if clean_exchange_id(value)
                                    ]
                                intent_position_id = clean_exchange_id(
                                    existing_write_intent.get("position_id")
                                )
                                intent_side = str(
                                    existing_write_intent.get("side") or ""
                                ).lower()
                                intent_stop = _signed_f(
                                    existing_write_intent.get("stop"), 0.0
                                )
                                intent_qty = _signed_f(
                                    existing_write_intent.get("qty"), 0.0
                                )
                                intent_matches = bool(
                                    intent_position_id == position_id
                                    and intent_side == side
                                    and intent_stop > 0
                                    and intent_qty > 0
                                    and abs(intent_stop - price)
                                    <= verify_price_tolerance
                                    and abs(intent_qty - stop_qty)
                                    <= verify_qty_tolerance
                                )
                                baseline_values = existing_write_intent.get(
                                    "pre_write_live_ids"
                                )
                                baseline_ids = {
                                    clean_exchange_id(value)
                                    for value in (
                                        baseline_values
                                        if isinstance(baseline_values, list)
                                        else []
                                    )
                                }
                                baseline_ids.discard("")
                                admin_approved_old_ids = (
                                    _admin_exact_cleanup_approved_old_ids(
                                        be_state, existing_write_intent
                                    )
                                )
                                if admin_approved_old_ids:
                                    # Explicit admin approval is durable and bound
                                    # to the standard cleanup intent. It augments
                                    # ownership only for the exact pre-write old
                                    # ids shown in the token preview.
                                    old_order_ids.update(admin_approved_old_ids)
                                recovery_fingerprint, recovery_snapshot = (
                                    _recovery_topology_fingerprint(
                                        all_algo_rows,
                                        symbol=symbol,
                                        position_id=position_id,
                                        side=side,
                                    )
                                )
                                if not intent_matches:
                                    qty_under_current = bool(
                                        intent_qty > 0
                                        and intent_qty + verify_qty_tolerance < stop_qty
                                    )
                                    qty_over_current = bool(
                                        intent_qty > stop_qty + verify_qty_tolerance
                                    )
                                    mismatch_reason = (
                                        "side_conflict"
                                        if intent_side != side
                                        else "position_id_conflict"
                                        if intent_position_id != position_id
                                        else "replacement_price_mismatch"
                                        if abs(intent_stop - price)
                                        > verify_price_tolerance
                                        else "replacement_qty_under_position"
                                        if qty_under_current
                                        else "replacement_qty_over_position"
                                        if qty_over_current
                                        else "replacement_qty_mismatch"
                                    )
                                    diagnostics = {
                                        "reason": mismatch_reason,
                                        "intent_position_id": intent_position_id,
                                        "current_position_id": position_id,
                                        "intent_side": intent_side,
                                        "current_side": side,
                                        "intent_stop": intent_stop,
                                        "current_stop": price,
                                        "intent_qty": intent_qty,
                                        "current_qty": stop_qty,
                                        "position_qty": qty_now,
                                        "qty_gap": max(0.0, stop_qty - intent_qty),
                                        "replacement_stop_id": clean_exchange_id(
                                            existing_write_intent.get("replacement_stop_id")
                                            or be_state.get("replacement_stop_id")
                                        ),
                                        "old_stop_ids": sorted(old_order_ids),
                                        "topology_snapshot": recovery_snapshot,
                                    }
                                    be_actions.append(
                                        {
                                            "type": "bounded_existing_be_recovery_blocked_v1_0_7g7",
                                            "reason": mismatch_reason,
                                            "topology_fingerprint": recovery_fingerprint,
                                            "diagnostics": diagnostics,
                                        }
                                    )
                                    qty_only_over_position = bool(
                                        intent_position_id == position_id
                                        and intent_side == side
                                        and intent_stop > 0
                                        and abs(intent_stop - price)
                                        <= verify_price_tolerance
                                        and intent_qty > stop_qty + verify_qty_tolerance
                                    )
                                    if not qty_only_over_position:
                                        # An intent smaller than the live remainder is
                                        # an actual protection gap and must stay blocked.
                                        # Only a stale over-sized intent caused by partial
                                        # TP reduction may proceed to the existing strict,
                                        # read-only STOP topology proof below.
                                        raise BeExistingRecoveryBlocked(
                                            reason_code=mismatch_reason,
                                            diagnostics=diagnostics,
                                            topology_fingerprint=recovery_fingerprint,
                                            message="durable BE write intent does not match current plan",
                                        )
                                    # G53: do not rewrite the durable intent and do not
                                    # send another STOP. A stale over-sized quantity can
                                    # be retired only if one exact bot-owned old STOP is
                                    # proven live and explicitly covers the fresh remainder.
                                    be_actions.append(
                                        {
                                            "type": "stale_be_over_qty_intent_deferred_to_live_topology_v1_0_7g7h2f5g5b3g53",
                                            "reason": mismatch_reason,
                                            "intent_qty": intent_qty,
                                            "current_qty": stop_qty,
                                            "exchange_writes": 0,
                                        }
                                    )
                                if not isinstance(baseline_values, list):
                                    diagnostics = {
                                        "reason": "missing_durable_replacement_identity",
                                        "topology_snapshot": recovery_snapshot,
                                    }
                                    be_actions.append(
                                        {
                                            "type": "bounded_existing_be_recovery_blocked_v1_0_7g7",
                                            "reason": diagnostics["reason"],
                                            "topology_fingerprint": recovery_fingerprint,
                                            "diagnostics": diagnostics,
                                        }
                                    )
                                    raise BeExistingRecoveryBlocked(
                                        reason_code=diagnostics["reason"],
                                        diagnostics=diagnostics,
                                        topology_fingerprint=recovery_fingerprint,
                                        message="durable BE write intent has no pre-write snapshot",
                                    )

                                # First accept the exact unique post-write identity that
                                # was absent from the durable pre-write snapshot. This is
                                # the same crash-reconciliation proof as before, but from
                                # one bounded fresh read instead of 12 polling attempts.
                                direct_candidates = _matching_stop_candidates(
                                    all_algo_rows,
                                    position_id=position_id,
                                    side=side,
                                    stop_price=price,
                                    qty=stop_qty,
                                    price_tolerance=verify_price_tolerance,
                                    qty_tolerance=verify_qty_tolerance,
                                    excluded_order_ids=baseline_ids,
                                )
                                direct_candidate_ids = sorted(
                                    {
                                        _algo_order_id(item)
                                        for item in direct_candidates
                                        if _algo_order_id(item)
                                    }
                                )
                                direct_diagnostics = {
                                    "reason": (
                                        "confirmed_unique_post_write_candidate"
                                        if len(direct_candidates) == 1
                                        else "replacement_id_not_live"
                                        if not direct_candidates
                                        else "multiple_replacement_candidates"
                                    ),
                                    "candidate_ids": direct_candidate_ids,
                                    "baseline_ids": sorted(baseline_ids),
                                    "topology_fingerprint": recovery_fingerprint,
                                    "topology_snapshot": recovery_snapshot,
                                    "open_orders_reads": 1,
                                }
                                be_actions.append(
                                    {
                                        "type": "bounded_unresolved_be_write_intent_readback_v1_0_7g7",
                                        "confirmed": len(direct_candidates) == 1,
                                        "diagnostics": direct_diagnostics,
                                        "intent": dict(existing_write_intent),
                                    }
                                )
                                if len(direct_candidates) == 1:
                                    existing_replacement = dict(direct_candidates[0])
                                    be_actions.append(
                                        {
                                            "type": "be_replacement_reconciled_after_unknown_write",
                                            "replacement_stop_id": _algo_order_id(
                                                existing_replacement
                                            ),
                                        }
                                    )
                                else:
                                    (
                                        recovered_existing_stop,
                                        existing_stop_recovery_diagnostics,
                                    ) = _recover_existing_be_stop_from_write_intent(
                                        all_algo_rows,
                                        write_intent=existing_write_intent,
                                        be_state=be_state,
                                        owned_old_order_ids=old_order_ids,
                                        position_id=position_id,
                                        side=side,
                                        stop_price=price,
                                        qty=stop_qty,
                                        price_tolerance=verify_price_tolerance,
                                        qty_tolerance=verify_qty_tolerance,
                                    )
                                    reason_code = _canonical_recovery_reason(
                                        existing_stop_recovery_diagnostics
                                    )
                                    existing_stop_recovery_diagnostics.update(
                                        {
                                            "reason_code": reason_code,
                                            "topology_fingerprint": recovery_fingerprint,
                                            "topology_snapshot": recovery_snapshot,
                                            "open_orders_reads": 1,
                                        }
                                    )
                                    recovery_action_payload = {
                                        "confirmed": recovered_existing_stop is not None,
                                        "diagnostics": existing_stop_recovery_diagnostics,
                                    }
                                    # Preserve the historical g5 audit marker so old
                                    # durable evidence/tests remain readable while g7
                                    # adds its bounded-recovery marker.
                                    be_actions.append(
                                        {
                                            "type": "be_existing_exact_stop_recovery_v1_0_7g5",
                                            **recovery_action_payload,
                                        }
                                    )
                                    be_actions.append(
                                        {
                                            "type": "be_existing_exact_stop_recovery_v1_0_7g7",
                                            **recovery_action_payload,
                                        }
                                    )
                                    if recovered_existing_stop is None and admin_approved_old_ids:
                                        admin_resume_stop, admin_resume_diagnostics = (
                                            _resume_admin_cleanup_after_cancel(
                                                all_algo_rows,
                                                be_state=be_state,
                                                write_intent=existing_write_intent,
                                                position_id=position_id,
                                                side=side,
                                                stop_price=price,
                                                qty=stop_qty,
                                                price_tolerance=verify_price_tolerance,
                                                qty_tolerance=verify_qty_tolerance,
                                                tracked_tp_order_ids={
                                                    clean_exchange_id(value)
                                                    for value in tracked_tp_ids_preserved_for_be
                                                    if clean_exchange_id(value)
                                                },
                                            )
                                        )
                                        be_actions.append(
                                            {
                                                "type": "admin_exact_cleanup_read_only_resume_v1_0_7g7a",
                                                "confirmed": admin_resume_stop is not None,
                                                "diagnostics": admin_resume_diagnostics,
                                            }
                                        )
                                        if admin_resume_stop is not None:
                                            recovered_existing_stop = dict(admin_resume_stop)
                                    if recovered_existing_stop is None:
                                        # g7a: a durable replacement id can disappear
                                        # because the write never became live or BingX
                                        # removed it.  If one exact old bot-owned STOP
                                        # still protects the full fresh remainder, keep
                                        # that STOP and clear only the stale recovery
                                        # checkpoint.  No exchange write is performed.
                                        stale_old_stop = None
                                        stale_old_diagnostics: dict[str, Any] = {}
                                        if reason_code in {
                                            "replacement_id_not_live",
                                            "replacement_qty_over_position",
                                        }:
                                            (
                                                stale_old_stop,
                                                stale_old_diagnostics,
                                            ) = _exact_old_stop_fallback_after_missing_replacement(
                                                all_algo_rows,
                                                write_intent=existing_write_intent,
                                                owned_old_order_ids=old_order_ids,
                                                position_id=position_id,
                                                side=side,
                                                qty=qty_now,
                                                qty_tolerance=verify_qty_tolerance,
                                            )
                                        if stale_old_stop is not None:
                                            retained_stop_id = _algo_order_id(stale_old_stop)
                                            be_actions.append(
                                                {
                                                    "type": "stale_be_replacement_checkpoint_cleared_v1_0_7g7a",
                                                    "retained_protective_stop_id": retained_stop_id,
                                                    "diagnostics": stale_old_diagnostics,
                                                    "exchange_writes": 0,
                                                }
                                            )
                                            await _write_status(
                                                "protected",
                                                "stale BE replacement checkpoint cleared; exact old STOP remains active",
                                                {
                                                    "be": {
                                                        "moved": False,
                                                        "manual_required": False,
                                                        "manual_requested": False,
                                                        "replacement_in_progress": False,
                                                        "replacement_write_intent_v1": None,
                                                        "rate_limit_write_confirmation_v1": None,
                                                        "cleanup_cancel_intent_v1": None,
                                                        "admin_exact_cleanup_intent_v1": None,
                                                        "replacement_stop_id": None,
                                                        "verify_matching_stop_order_id": None,
                                                        "replacement_stop": None,
                                                        "error": None,
                                                        "skipped": None,
                                                        "existing_be_recovery_blocked_v1": None,
                                                        "existing_be_recovery_next_retry_after": None,
                                                        "existing_be_recovery_owned_replacement_ids": None,
                                                        "existing_be_recovery_stale_checkpoint_resolution_v1": {
                                                            "version": 1,
                                                            "resolved_at": _iso_utc(_utc_now()),
                                                            "retained_protective_stop_id": retained_stop_id,
                                                            "topology_fingerprint": recovery_fingerprint,
                                                            "diagnostics": stale_old_diagnostics,
                                                            "exchange_writes": 0,
                                                        },
                                                        "actions": be_actions,
                                                    }
                                                },
                                            )
                                            log.warning(
                                                "BE_STALE_REPLACEMENT_CHECKPOINT_CLEARED execution_id=%s user_id=%s symbol=%s retained_stop_id=%s fingerprint=%s",
                                                execution_id,
                                                user_id,
                                                symbol,
                                                retained_stop_id,
                                                recovery_fingerprint,
                                            )
                                            await _invalidate_pre_reads(
                                                adapter, user_id, exchange, symbol
                                            )
                                            moved += 1
                                            continue
                                        if stale_old_diagnostics:
                                            existing_stop_recovery_diagnostics[
                                                "stale_old_stop_fallback"
                                            ] = stale_old_diagnostics
                                        raise BeExistingRecoveryBlocked(
                                            reason_code=reason_code,
                                            diagnostics=existing_stop_recovery_diagnostics,
                                            topology_fingerprint=recovery_fingerprint,
                                        )
                                    existing_replacement = dict(recovered_existing_stop)
                                    recovered_action_payload = {
                                        "replacement_stop_id": _algo_order_id(
                                            recovered_existing_stop
                                        ),
                                        "old_stop_ids": sorted(
                                            {
                                                clean_exchange_id(value)
                                                for value in (
                                                    existing_write_intent.get("old_stop_ids")
                                                    if isinstance(
                                                        existing_write_intent.get("old_stop_ids"),
                                                        list,
                                                    )
                                                    else []
                                                )
                                                if clean_exchange_id(value)
                                            }
                                        ),
                                    }
                                    be_actions.append(
                                        {
                                            "type": "be_replacement_recovered_from_exact_ownership_v1_0_7g5",
                                            **recovered_action_payload,
                                        }
                                    )
                                    be_actions.append(
                                        {
                                            "type": "be_replacement_recovered_from_exact_ownership_v1_0_7g7",
                                            **recovered_action_payload,
                                        }
                                    )

                                # A previous process may have created and durably
                                # checkpointed the new BE STOP before Railway restarted.
                                # Reuse that exact plan instead of creating a duplicate.
                            saved_replacement_ids: set[str] = set()
                            for saved_value in (
                                be_state.get("replacement_stop_id"),
                                be_state.get("verify_matching_stop_order_id"),
                                _algo_order_id(be_state.get("replacement_stop") or {}),
                            ):
                                saved_id = clean_exchange_id(saved_value)
                                if saved_id:
                                    saved_replacement_ids.add(saved_id)

                            if existing_replacement is None:
                                for saved_id in sorted(saved_replacement_ids):
                                    saved_row = live_rows_by_id.get(saved_id)
                                    if (
                                        saved_row
                                        and _matching_stop_order(
                                            [saved_row],
                                            position_id=position_id,
                                            side=side,
                                            stop_price=price,
                                            qty=stop_qty,
                                            price_tolerance=verify_price_tolerance,
                                            qty_tolerance=verify_qty_tolerance,
                                        )
                                        is not None
                                    ):
                                        existing_replacement = saved_row
                                        break

                            if existing_replacement is None and saved_replacement_ids:
                                history_stop, history_reason = (
                                    await _find_checkpointed_stop_from_history(
                                        adapter,
                                        symbol=symbol,
                                        stop_ids=saved_replacement_ids,
                                        position_id=position_id,
                                        side=side,
                                        stop_price=price,
                                        qty=stop_qty,
                                        price_tolerance=verify_price_tolerance,
                                        qty_tolerance=verify_qty_tolerance,
                                    )
                                )
                                be_actions.append(
                                    {
                                        "type": "checkpointed_be_stop_history_recheck",
                                        "replacement_stop_ids": sorted(
                                            saved_replacement_ids
                                        ),
                                        "reason": history_reason,
                                        "confirmed": history_stop is not None,
                                    }
                                )
                                if history_stop is not None:
                                    existing_replacement = dict(history_stop)
                                else:
                                    # A durable checkpoint proves a replacement write was
                                    # already confirmed.  If BingX temporarily omits that
                                    # exact plan from the read endpoint, creating another
                                    # STOP would be unsafe.  Do not report a false hard
                                    # BE failure; mark the checkpoint for manual visibility
                                    # review and never create a duplicate STOP.
                                    be_actions.append(
                                        {
                                            "type": "checkpointed_be_stop_not_visible_blocks_duplicate",
                                            "replacement_stop_ids": sorted(
                                                saved_replacement_ids
                                            ),
                                        }
                                    )
                                    raise BeCheckpointedStopVisibilityPending(
                                        stop_ids=saved_replacement_ids,
                                        stop_price=price,
                                    )

                            if existing_replacement is not None:
                                confirmed_replacement_stop = dict(existing_replacement)
                                replacement_id = _algo_order_id(existing_replacement)
                                old_order_ids.discard(replacement_id)
                                be_actions.append(
                                    {
                                        "type": "be_replacement_reused_after_checkpoint",
                                        "replacement_stop_id": replacement_id,
                                    }
                                )
                            else:
                                # If an exact BE STOP already exists but is not owned
                                # by this execution, it may be manual or may come from
                                # an uncheckpointed crash.  Never create a second one.
                                untracked_existing_be = _matching_stop_order(
                                    [
                                        candidate
                                        for candidate in all_algo_rows
                                        if _algo_order_id(candidate)
                                        not in old_order_ids
                                    ],
                                    position_id=position_id,
                                    side=side,
                                    stop_price=price,
                                    qty=stop_qty,
                                    price_tolerance=verify_price_tolerance,
                                    qty_tolerance=verify_qty_tolerance,
                                )
                                if untracked_existing_be is not None:
                                    untracked_be_id = _algo_order_id(
                                        untracked_existing_be
                                    )
                                    be_actions.append(
                                        {
                                            "type": "untracked_exact_be_stop_blocks_duplicate",
                                            "stop_order_id": untracked_be_id,
                                        }
                                    )
                                    raise RuntimeError(
                                        "an untracked exact BE STOP already exists; "
                                        "duplicate STOP creation was blocked for manual review"
                                    )

                                    # Re-read fresh last and fair prices immediately
                                    # before the write.  TP evidence can be several
                                    # seconds old and the market may already have
                                    # crossed back through the intended BE trigger.
                                market_fetcher = getattr(
                                    adapter, "fetch_market_prices", None
                                )
                                if callable(market_fetcher):
                                    fresh_prices = await market_fetcher(symbol)
                                    fresh_last = _f(
                                        (fresh_prices or {}).get("last"), 0.0
                                    )
                                    fresh_fair = _f(
                                        (fresh_prices or {}).get("fair"), 0.0
                                    )
                                else:
                                    fresh_last = _f(
                                        await adapter.fetch_last_price(symbol), 0.0
                                    )
                                    fresh_fair = fresh_last
                                market_safety = _be_stop_market_safety(
                                    side,
                                    stop_price=price,
                                    last_price=fresh_last,
                                    fair_price=fresh_fair,
                                    price_tick=price_tick,
                                )
                                be_actions.append(
                                    {
                                        "type": "be_prewrite_market_safety",
                                        **market_safety,
                                    }
                                )
                                current_price = fresh_last
                                stop_reference_price = fresh_fair
                                live_owned_stop_ids = sorted(
                                    order_id
                                    for order_id in old_order_ids
                                    if isinstance(live_rows_by_id.get(order_id), dict)
                                    and _looks_stop_order(live_rows_by_id[order_id])
                                )
                                coverage_tolerance = max(abs(float(qty_now)) * 1e-9, 1e-12)
                                uncovered_qty = max(0.0, float(qty_now) - float(stop_qty))
                                if uncovered_qty > coverage_tolerance:
                                    coverage_audit = {
                                        "type": "be_stop_full_qty_coverage_block_v1_6_99",
                                        "position_qty": float(qty_now),
                                        "normalized_stop_qty": float(stop_qty),
                                        "uncovered_qty": float(uncovered_qty),
                                        "qty_step": float(qty_step),
                                        "coverage_tolerance": float(coverage_tolerance),
                                    }
                                    be_actions.append(coverage_audit)
                                    if not live_owned_stop_ids:
                                        raise RuntimeError(
                                            "new BE STOP quantity would not cover the full live position "
                                            f"and no exact old protective STOP is visible: position_qty={qty_now}, "
                                            f"stop_qty={stop_qty}, uncovered={uncovered_qty}"
                                        )
                                    now_iso = datetime.now(timezone.utc).isoformat()
                                    waiting_since = str(
                                        be_state.get("waiting_since")
                                        or be_state.get("waiting_qty_coverage_since")
                                        or now_iso
                                    )
                                    waiting_patch = {
                                        "be": {
                                            "moved": False,
                                            "manual_required": False,
                                            "replacement_in_progress": False,
                                            "waiting_qty_coverage": True,
                                            "waiting_retry": True,
                                            "waiting_reason": "be_stop_qty_below_live_position",
                                            "waiting_since": waiting_since,
                                            "waiting_last_checked_at": now_iso,
                                            "waiting_next_retry_after": _be_retry_after_iso(_BE_QTY_COVERAGE_RETRY_SEC),
                                            "waiting_backoff_sec": _BE_QTY_COVERAGE_RETRY_SEC,
                                            "waiting_trigger_tp_index": trigger_plan_index,
                                            "waiting_trigger_ordinal": trigger_idx,
                                            "waiting_old_stop_ids": live_owned_stop_ids,
                                            "waiting_target_stop": price,
                                            "waiting_qty": stop_qty,
                                            "waiting_qty_coverage_audit": coverage_audit,
                                            "source": "manual" if manual_requested else "automatic",
                                            "trigger_tp_index": trigger_plan_index,
                                            "trigger_ordinal": trigger_idx,
                                            "stop": price,
                                            "qty": stop_qty,
                                            "position_qty": qty_now,
                                            "original_qty": original_qty,
                                            "basis_entry": actual_entry,
                                            "signal_entry": signal_entry,
                                            "tp_price_seen": price_seen,
                                            "tp_price_seen_at": current_price,
                                            "tp_target": trigger_target,
                                            "calculation": be_calc,
                                            "actions": be_actions,
                                            **_be_waiting_tp_bypass_marker(be_bypass_decision, position_qty=qty_now),
                                            "error": None,
                                            **(
                                                {
                                                    "manual_requested": False,
                                                    "manual_result": {
                                                        "state": "qty_coverage_waiting",
                                                        "reason": (
                                                            "Новый BE STOP сейчас не покрывает весь остаток позиции; "
                                                            "старый STOP оставлен активным."
                                                        ),
                                                        "stop": price,
                                                        "qty": stop_qty,
                                                        "position_qty": qty_now,
                                                        "uncovered_qty": uncovered_qty,
                                                        "next_retry_after": _be_retry_after_iso(_BE_QTY_COVERAGE_RETRY_SEC),
                                                    },
                                                }
                                                if manual_requested
                                                else {}
                                            ),
                                        },
                                        "actual_entry": actual_entry,
                                    }
                                    await _write_status(
                                        status,
                                        (
                                            "manual BE waiting for full STOP quantity coverage"
                                            if manual_requested
                                            else f"BE waiting for full STOP quantity coverage after TP{trigger_plan_index}"
                                        ),
                                        waiting_patch,
                                    )
                                    await _notify(
                                        notify,
                                        user_id,
                                        card(
                                            "⏳ <b>Б/У ОТЛОЖЕН</b>",
                                            symbol=symbol,
                                            side=side,
                                            blocks=(
                                                [
                                                    f"🆔 <b>Execution:</b> {execution_id}",
                                                    (
                                                        "🔒 <b>Условие:</b> принудительный Б/У"
                                                        if manual_requested
                                                        else f"🎯 <b>Сработал:</b> TP{trigger_plan_index}"
                                                    ),
                                                    f"🛡 <b>Расчётный новый STOP:</b> {fmt_price(price)}",
                                                    f"📦 <b>Остаток позиции:</b> {fmt_qty(qty_now)}",
                                                ],
                                                [
                                                    "⏳ Новый BE STOP сейчас не покрывает весь остаток позиции",
                                                    "✅ Старый STOP подтверждён и остаётся активным",
                                                    "🔁 Бот повторит перенос в Б/У автоматически",
                                                ],
                                                [
                                                    f"🧷 <b>Старый STOP:</b> {', '.join(live_owned_stop_ids[:3])}",
                                                    f"📦 <b>STOP qty:</b> {fmt_qty(stop_qty)}",
                                                    f"⚠️ <b>Без покрытия:</b> {fmt_qty(uncovered_qty)}",
                                                ],
                                            ),
                                        ),
                                        event_key=f"execution:{execution_id}:be_qty_coverage_waiting",
                                    )
                                    await _invalidate_pre_reads(adapter, user_id, exchange, symbol)
                                    moved += 1
                                    continue
                                if not market_safety.get("safe"):
                                    if not live_owned_stop_ids:
                                        raise RuntimeError(
                                            "new BE STOP write blocked by fresh market safety check "
                                            "and no exact old protective STOP is visible: "
                                            f"{market_safety.get('reason')}; "
                                            f"last={fresh_last} fair={fresh_fair} "
                                            f"stop={price} gap={market_safety.get('required_gap')}"
                                        )

                                    now_iso = datetime.now(timezone.utc).isoformat()
                                    waiting_since = str(
                                        be_state.get("waiting_since")
                                        or be_state.get("waiting_market_safe_since")
                                        or now_iso
                                    )
                                    waiting_patch = {
                                        "be": {
                                            "moved": False,
                                            "manual_required": False,
                                            "replacement_in_progress": False,
                                            "waiting_market_safe": True,
                                            "waiting_retry": True,
                                            "waiting_reason": "market_not_safely_beyond_stop",
                                            "waiting_since": waiting_since,
                                            "waiting_last_checked_at": now_iso,
                                            "waiting_next_retry_after": _be_retry_after_iso(_BE_MARKET_SAFE_RETRY_SEC),
                                            "waiting_backoff_sec": _BE_MARKET_SAFE_RETRY_SEC,
                                            "waiting_trigger_tp_index": trigger_plan_index,
                                            "waiting_trigger_ordinal": trigger_idx,
                                            "waiting_old_stop_ids": live_owned_stop_ids,
                                            "waiting_target_stop": price,
                                            "waiting_qty": stop_qty,
                                            "waiting_market_safety": dict(market_safety),
                                            "source": "manual" if manual_requested else "automatic",
                                            "trigger_tp_index": trigger_plan_index,
                                            "trigger_ordinal": trigger_idx,
                                            "stop": price,
                                            "qty": stop_qty,
                                            "position_qty": qty_now,
                                            "original_qty": original_qty,
                                            "basis_entry": actual_entry,
                                            "signal_entry": signal_entry,
                                            "tp_price_seen": price_seen,
                                            "tp_price_seen_at": current_price,
                                            "tp_target": trigger_target,
                                            "calculation": be_calc,
                                            "actions": be_actions,
                                            **_be_waiting_tp_bypass_marker(be_bypass_decision, position_qty=qty_now),
                                            "error": None,
                                            **(
                                                {
                                                    "manual_requested": False,
                                                    "manual_result": {
                                                        "state": "market_not_safe",
                                                        "reason": (
                                                            "Рынок сейчас не безопасен для нового BE STOP; "
                                                            "старый STOP оставлен активным."
                                                        ),
                                                        "stop": price,
                                                        "market_price": fresh_last,
                                                        "fair_price": fresh_fair,
                                                        "required_gap": _f(market_safety.get("required_gap"), 0.0),
                                                        "next_retry_after": _be_retry_after_iso(_BE_MARKET_SAFE_RETRY_SEC),
                                                    },
                                                }
                                                if manual_requested
                                                else {}
                                            ),
                                        },
                                        "actual_entry": actual_entry,
                                    }
                                    await _write_status(
                                        status,
                                        (
                                            "manual BE waiting for market-safe STOP replacement"
                                            if manual_requested
                                            else f"BE waiting for market-safe STOP replacement after TP{trigger_plan_index}"
                                        ),
                                        waiting_patch,
                                    )
                                    await _notify(
                                        notify,
                                        user_id,
                                        card(
                                            "⏳ <b>Б/У ОТЛОЖЕН</b>",
                                            symbol=symbol,
                                            side=side,
                                            blocks=(
                                                [
                                                    f"🆔 <b>Execution:</b> {execution_id}",
                                                    (
                                                        "🔒 <b>Условие:</b> принудительный Б/У"
                                                        if manual_requested
                                                        else f"🎯 <b>Сработал:</b> TP{trigger_plan_index}"
                                                    ),
                                                    f"🛡 <b>Расчётный новый STOP:</b> {fmt_price(price)}",
                                                    f"📦 <b>Остаток позиции:</b> {fmt_qty(qty_now)}",
                                                ],
                                                [
                                                    "⏳ Рынок сейчас не безопасен для нового BE STOP",
                                                    "✅ Старый STOP подтверждён и остаётся активным",
                                                    "🔁 Бот повторит перенос в Б/У автоматически",
                                                ],
                                                [
                                                    f"🧷 <b>Старый STOP:</b> {', '.join(live_owned_stop_ids[:3])}",
                                                    f"📊 <b>last/fair:</b> {fmt_price(fresh_last)} / {fmt_price(fresh_fair)}",
                                                    f"📏 <b>gap:</b> {fmt_price(_f(market_safety.get('required_gap'), 0.0))}",
                                                ],
                                            ),
                                        ),
                                        event_key=f"execution:{execution_id}:be_market_safety_waiting",
                                        dedup_key_override=_be_waiting_market_safe_dedup_key(execution_id),
                                    )
                                    await _invalidate_pre_reads(adapter, user_id, exchange, symbol)
                                    moved += 1
                                    continue

                                    # 1) Persist a write intent before touching BingX.
                                    # A hard process kill after the exchange accepts the
                                    # STOP but before its plan id is saved must not cause
                                    # the next process to create another STOP blindly.
                                write_intent = {
                                    "version": 1,
                                    "client_id": pre_client_id,
                                    "position_id": position_id,
                                    "side": side,
                                    "stop": price,
                                    "qty": stop_qty,
                                    "pre_write_live_ids": sorted(live_rows_by_id),
                                    "old_stop_ids": sorted(old_order_ids),
                                    "created_at": datetime.now(
                                        timezone.utc
                                    ).isoformat(),
                                }
                                intent_patch = {
                                    "be": {
                                        "moved": False,
                                        "manual_required": False,
                                        "replacement_in_progress": True,
                                        "replacement_write_intent_v1": write_intent,
                                    }
                                }
                                await _write_status(
                                    status,
                                    str(row.get("reason") or ""),
                                    intent_patch,
                                )
                                local_be_state = (
                                    dict(ids_payload.get("be") or {})
                                    if isinstance(ids_payload.get("be"), dict)
                                    else {}
                                )
                                local_be_state.update(intent_patch["be"])
                                ids_payload["be"] = local_be_state
                                be_state = local_be_state

                                # 2) Create the new BE STOP while the old STOP still
                                # protects the position.  This deliberately permits a
                                # short overlap of two protective STOPs, but never a
                                # gap with zero STOPs.
                                pre_write_ids = {
                                    clean_exchange_id(value)
                                    for value in write_intent["pre_write_live_ids"]
                                }
                                pre_write_ids.discard("")
                                pre_res: dict[str, Any] = {}
                                replacement_stop: dict[str, Any] | None = None
                                replacement_response_id = ""
                                await _invalidate_pre_reads(
                                    adapter, user_id, exchange, symbol
                                )
                                try:
                                    try:
                                        pre_res = await adapter.set_position_stop_loss(
                                            symbol=symbol,
                                            side=side,
                                            qty=stop_qty,
                                            stop=price,
                                            client_id=pre_client_id,
                                            position_id=position_id,
                                            adopt_existing=False,
                                            owned_order_ids=sorted(old_order_ids),
                                        )
                                    except TypeError as type_exc:
                                        if "owned_order_ids" not in str(type_exc):
                                            raise
                                        pre_res = await adapter.set_position_stop_loss(
                                            symbol=symbol,
                                            side=side,
                                            qty=stop_qty,
                                            stop=price,
                                            client_id=pre_client_id,
                                            position_id=position_id,
                                            adopt_existing=False,
                                        )
                                    await _invalidate_pre_reads(
                                        adapter, user_id, exchange, symbol
                                    )
                                    be_actions.append(
                                        {
                                            "type": "be_stop_created_before_exact_cleanup",
                                            "result": pre_res,
                                        }
                                    )
                                    response_price = _f(
                                        (
                                            pre_res.get("_normalized_price")
                                            if isinstance(pre_res, dict)
                                            else None
                                        ),
                                        price,
                                    )
                                    response_qty = _f(
                                        (
                                            pre_res.get("_normalized_quantity")
                                            if isinstance(pre_res, dict)
                                            else None
                                        ),
                                        stop_qty,
                                    )
                                    replacement_response_id = _tp_order_id_from_payload(
                                        pre_res
                                    )
                                    if (
                                        abs(response_price - price)
                                        > verify_price_tolerance
                                        or abs(response_qty - stop_qty)
                                        > verify_qty_tolerance
                                    ):
                                        price = response_price
                                        stop_qty = response_qty
                                        be_calc.update(
                                            {
                                                "final_stop": price,
                                                "submitted_stop": price,
                                                "submitted_qty": stop_qty,
                                            }
                                        )
                                        exact_result_patch = {
                                            "be": {
                                                "replacement_write_intent_v1": {
                                                    **write_intent,
                                                    "stop": price,
                                                    "qty": stop_qty,
                                                    "exchange_normalized_after_write": True,
                                                }
                                            }
                                        }
                                        await _write_status(
                                            status,
                                            str(row.get("reason") or ""),
                                            exact_result_patch,
                                        )
                                    be_actions.append(
                                        {
                                            "type": "be_stop_exact_submitted_values",
                                            "stop": price,
                                            "qty": stop_qty,
                                            "stop_plan_id": replacement_response_id,
                                        }
                                    )
                                except StaleExecutionPass:
                                    raise
                                except NetworkAmbiguousErrors as create_exc:
                                    await _invalidate_pre_reads(
                                        adapter, user_id, exchange, symbol
                                    )
                                    if _is_bingx_trigger_rate_limit(create_exc):
                                        # BingX code 100410 is itself a signal to stop
                                        # hitting the trigger endpoint.  Do not run the
                                        # legacy immediate 12-read ambiguity poll here:
                                        # preserve the original exception so the outer
                                        # handler can durably checkpoint the exact old
                                        # STOP set and schedule bounded read-only
                                        # confirmation (10/30/60/120s) without a second
                                        # write.
                                        be_actions.append(
                                            {
                                                "type": "be_trigger_rate_limit_ambiguous_write_deferred_without_poll_v1_0_7g7h2f5g5b3e",
                                                "error": _exchange_error_details(create_exc),
                                                "exchange_reads_after_error": 0,
                                                "exchange_writes_after_error": 0,
                                            }
                                        )
                                        raise
                                    # The request may have reached BingX even when its
                                    # response was lost or rejected by a gateway.
                                    # Reconcile one unique exact STOP that was absent
                                    # from the durable pre-write snapshot. Never retry
                                    # the write in this pass.
                                    (
                                        write_reconciled,
                                        reconciled_stop,
                                        _write_remaining,
                                        write_read_error,
                                        write_diagnostics,
                                    ) = await _wait_exact_conditionals_replaced(
                                        adapter,
                                        symbol=symbol,
                                        position_id=position_id,
                                        side=side,
                                        old_order_ids=set(),
                                        stop_price=price,
                                        qty=stop_qty,
                                        price_tolerance=verify_price_tolerance,
                                        qty_tolerance=verify_qty_tolerance,
                                        excluded_new_order_ids=pre_write_ids,
                                        require_unique_new_stop=True,
                                        attempts=12,
                                    )
                                    be_actions.append(
                                        {
                                            "type": "be_stop_write_error_readback",
                                            "write_error": _exchange_error_details(
                                                create_exc
                                            ),
                                            "confirmed": write_reconciled,
                                            "read_error": write_read_error,
                                            "diagnostics": write_diagnostics,
                                        }
                                    )
                                    if (
                                        not write_reconciled
                                        or reconciled_stop is None
                                        or not _algo_order_id(reconciled_stop)
                                    ):
                                        raise RuntimeError(
                                            "new BE STOP write outcome is unresolved; "
                                            "no second write was sent; "
                                            f"write_error={type(create_exc).__name__}: "
                                            f"{str(create_exc)[:300]}; "
                                            f"read_error={write_read_error or '-'}; "
                                            f"{_stop_diagnostics_summary(write_diagnostics)}"
                                        ) from create_exc
                                    replacement_stop = dict(reconciled_stop)
                                    replacement_response_id = _algo_order_id(
                                        reconciled_stop
                                    )
                                    be_actions.append(
                                        {
                                            "type": "be_stop_write_reconciled_without_retry",
                                            "replacement_stop_id": replacement_response_id,
                                        }
                                    )

                                except Exception as create_exc:
                                    await _invalidate_pre_reads(
                                        adapter, user_id, exchange, symbol
                                    )
                                    if not _is_be_market_not_safe_exchange_rejection(create_exc):
                                        raise
                                    post_reject_last = fresh_last
                                    post_reject_fair = fresh_fair
                                    try:
                                        market_fetcher = getattr(adapter, "fetch_market_prices", None)
                                        if callable(market_fetcher):
                                            post_prices = await market_fetcher(symbol)
                                            post_reject_last = _f((post_prices or {}).get("last"), fresh_last)
                                            post_reject_fair = _f((post_prices or {}).get("fair"), fresh_fair)
                                        else:
                                            post_reject_last = _f(await adapter.fetch_last_price(symbol), fresh_last)
                                            post_reject_fair = post_reject_last
                                    except Exception as price_exc:
                                        be_actions.append(
                                            {
                                                "type": "be_exchange_reject_price_refresh_failed",
                                                "error": f"{type(price_exc).__name__}: {str(price_exc)[:200]}",
                                            }
                                        )
                                    reject_safety = _be_stop_market_safety(
                                        side,
                                        stop_price=price,
                                        last_price=post_reject_last,
                                        fair_price=post_reject_fair,
                                        price_tick=price_tick,
                                    )
                                    be_actions.append(
                                        {
                                            "type": "be_stop_exchange_rejected_market_not_safe",
                                            "error": _exchange_error_details(create_exc),
                                            **reject_safety,
                                        }
                                    )
                                    if not live_owned_stop_ids:
                                        raise RuntimeError(
                                            "new BE STOP exchange rejection looked market-safety related, "
                                            "but no exact old protective STOP is visible: "
                                            f"{_exchange_error_details(create_exc)}"
                                        ) from create_exc
                                    now_iso = datetime.now(timezone.utc).isoformat()
                                    waiting_since = str(
                                        be_state.get("waiting_since")
                                        or be_state.get("waiting_market_safe_since")
                                        or now_iso
                                    )
                                    waiting_patch = {
                                        "be": {
                                            "moved": False,
                                            "manual_required": False,
                                            "replacement_in_progress": False,
                                            "waiting_market_safe": True,
                                            "waiting_retry": True,
                                            "waiting_reason": "exchange_rejected_stop_not_market_safe",
                                            "waiting_since": waiting_since,
                                            "waiting_last_checked_at": now_iso,
                                            "waiting_trigger_tp_index": trigger_plan_index,
                                            "waiting_trigger_ordinal": trigger_idx,
                                            "waiting_old_stop_ids": live_owned_stop_ids,
                                            "waiting_target_stop": price,
                                            "waiting_qty": stop_qty,
                                            "waiting_market_safety": dict(reject_safety),
                                            "waiting_exchange_error": _exchange_error_details(create_exc),
                                            "waiting_next_retry_after": _be_retry_after_iso(_BE_MARKET_SAFE_RETRY_SEC),
                                            "waiting_backoff_sec": _BE_MARKET_SAFE_RETRY_SEC,
                                            **_be_waiting_tp_bypass_marker(be_bypass_decision, position_qty=qty_now),
                                            "source": "manual" if manual_requested else "automatic",
                                            "trigger_tp_index": trigger_plan_index,
                                            "trigger_ordinal": trigger_idx,
                                            "stop": price,
                                            "qty": stop_qty,
                                            "position_qty": qty_now,
                                            "original_qty": original_qty,
                                            "basis_entry": actual_entry,
                                            "signal_entry": signal_entry,
                                            "tp_price_seen": price_seen,
                                            "tp_price_seen_at": current_price,
                                            "tp_target": trigger_target,
                                            "calculation": be_calc,
                                            "actions": be_actions,
                                            "error": None,
                                            **(
                                                {
                                                    "manual_requested": False,
                                                    "manual_result": {
                                                        "state": "market_not_safe",
                                                        "reason": (
                                                            "BingX отклонила новый BE STOP как небезопасный/слишком близкий; "
                                                            "старый STOP оставлен активным."
                                                        ),
                                                        "stop": price,
                                                        "market_price": post_reject_last,
                                                        "fair_price": post_reject_fair,
                                                        "next_retry_after": _be_retry_after_iso(_BE_MARKET_SAFE_RETRY_SEC),
                                                    },
                                                }
                                                if manual_requested
                                                else {}
                                            ),
                                        },
                                        "actual_entry": actual_entry,
                                    }
                                    await _write_status(
                                        status,
                                        (
                                            "manual BE waiting after exchange market-safety rejection"
                                            if manual_requested
                                            else f"BE waiting after exchange market-safety rejection after TP{trigger_plan_index}"
                                        ),
                                        waiting_patch,
                                    )
                                    await _notify(
                                        notify,
                                        user_id,
                                        card(
                                            "⏳ <b>Б/У ОТЛОЖЕН</b>",
                                            symbol=symbol,
                                            side=side,
                                            blocks=(
                                                [
                                                    f"🆔 <b>Execution:</b> {execution_id}",
                                                    (
                                                        "🔒 <b>Условие:</b> принудительный Б/У"
                                                        if manual_requested
                                                        else f"🎯 <b>Сработал:</b> TP{trigger_plan_index}"
                                                    ),
                                                    f"🛡 <b>Расчётный новый STOP:</b> {fmt_price(price)}",
                                                    f"📦 <b>Остаток позиции:</b> {fmt_qty(qty_now)}",
                                                ],
                                                [
                                                    "⏳ BingX отклонил новый BE STOP как небезопасный к текущей цене",
                                                    "✅ Старый STOP подтверждён и остаётся активным",
                                                    "🔁 Бот повторит перенос в Б/У автоматически",
                                                ],
                                                [
                                                    f"🧷 <b>Старый STOP:</b> {', '.join(live_owned_stop_ids[:3])}",
                                                    f"📊 <b>last/fair:</b> {fmt_price(post_reject_last)} / {fmt_price(post_reject_fair)}",
                                                    f"📏 <b>gap:</b> {fmt_price(_f(reject_safety.get('required_gap'), 0.0))}",
                                                ],
                                            ),
                                        ),
                                        event_key=f"execution:{execution_id}:be_market_safety_waiting",
                                        dedup_key_override=_be_waiting_market_safe_dedup_key(execution_id),
                                    )
                                    await _invalidate_pre_reads(adapter, user_id, exchange, symbol)
                                    moved += 1
                                    continue

                                if replacement_stop is None:
                                    (
                                        replacement_ok,
                                        replacement_stop,
                                        rows_after_create,
                                        create_error,
                                        create_diagnostics,
                                    ) = await _wait_exact_conditionals_replaced(
                                        adapter,
                                        symbol=symbol,
                                        position_id=position_id,
                                        side=side,
                                        old_order_ids=set(),
                                        stop_price=price,
                                        qty=stop_qty,
                                        price_tolerance=verify_price_tolerance,
                                        qty_tolerance=verify_qty_tolerance,
                                        expected_new_order_id=replacement_response_id,
                                        excluded_new_order_ids=(
                                            pre_write_ids
                                            if not replacement_response_id
                                            else None
                                        ),
                                        require_unique_new_stop=not bool(
                                            replacement_response_id
                                        ),
                                        attempts=12,
                                    )
                                    be_actions.append(
                                        {
                                            "type": "be_new_stop_confirmation",
                                            "confirmed": replacement_ok,
                                            "error": create_error,
                                            "diagnostics": create_diagnostics,
                                        }
                                    )
                                    if not replacement_ok or replacement_stop is None:
                                        raise RuntimeError(
                                            "new BE STOP was not confirmed before old protection cleanup; "
                                            f"expected_id={replacement_response_id or '-'} "
                                            f"stop={price} qty={stop_qty} "
                                            f"error={create_error or '-'} remaining_old={len(rows_after_create)}; "
                                            f"{_stop_diagnostics_summary(create_diagnostics)}"
                                        )
                                confirmed_replacement_stop = dict(replacement_stop)
                                replacement_id = _algo_order_id(replacement_stop)
                                if not replacement_id:
                                    raise RuntimeError(
                                        "new BE STOP was confirmed without an exact stop-plan id"
                                    )
                                old_order_ids.discard(replacement_id)

                                # Persist both the newly claimed original STOP and the
                                # confirmed replacement before cancelling anything. If
                                # Railway restarts after this checkpoint, the next pass
                                # reuses the exact replacement id instead of creating a
                                # second BE STOP.
                            checkpoint_patch = _be_ownership_patch(
                                claimed_limit_attached_stop,
                                confirmed_replacement_stop,
                                replacement_in_progress=True,
                            )
                            checkpoint_be = checkpoint_patch.setdefault("be", {})
                            checkpoint_be.update(
                                {
                                    "moved": False,
                                    "manual_required": False,
                                    "source": (
                                        "manual" if manual_requested else "automatic"
                                    ),
                                    "stop": price,
                                    "qty": stop_qty,
                                    "trigger_tp_index": trigger_plan_index,
                                    "trigger_ordinal": trigger_idx,
                                }
                            )
                            await _write_status(
                                status,
                                str(row.get("reason") or ""),
                                checkpoint_patch,
                            )
                            if claimed_limit_attached_stop:
                                ids_payload[LIMIT_ATTACHED_STOP_KEY] = (
                                    claimed_limit_attached_stop
                                )
                            local_be_state = (
                                dict(ids_payload.get("be") or {})
                                if isinstance(ids_payload.get("be"), dict)
                                else {}
                            )
                            local_be_state.update(checkpoint_be)
                            ids_payload["be"] = local_be_state
                            be_state = local_be_state

                            preserved_untracked = [
                                candidate
                                for candidate in all_algo_rows
                                if _order_matches_position(candidate, position_id, side)
                                and _algo_order_id(candidate) not in old_order_ids
                                and _algo_order_id(candidate) != replacement_id
                            ]
                            preserved_untracked_ids = {
                                _algo_order_id(candidate)
                                for candidate in preserved_untracked
                                if _algo_order_id(candidate)
                            }
                            preserved_untracked_stop_ids = sorted(
                                {
                                    _algo_order_id(candidate)
                                    for candidate in preserved_untracked
                                    if _algo_order_id(candidate)
                                    and _looks_stop_order(candidate)
                                }
                            )
                            if preserved_untracked:
                                be_actions.append(
                                    {
                                        "type": "untracked_conditionals_preserved",
                                        "count": len(preserved_untracked),
                                        "order_ids": sorted(
                                            {
                                                _algo_order_id(candidate)
                                                for candidate in preserved_untracked
                                                if _algo_order_id(candidate)
                                            }
                                        ),
                                    }
                                )

                                # 3) Reserve the exact cleanup write durably before
                                # touching BingX.  If Railway dies after dispatch, the
                                # next process performs read-only reconciliation and
                                # never repeats an unknown cancel blindly.
                            recovery_tp_before_fingerprint = ""
                            recovery_tp_before_snapshot: list[dict[str, str]] = []
                            recovery_stop_topology_fingerprint = ""
                            recovery_tracked_tp_ids = {
                                clean_exchange_id(value)
                                for value in tracked_tp_ids_preserved_for_be
                            }
                            recovery_tracked_tp_ids.discard("")
                            strict_recovery_action_types = {
                                "be_replacement_recovered_from_exact_ownership_v1_0_7g5",
                                "be_replacement_recovered_from_exact_ownership_v1_0_7g7",
                                "be_replacement_reconciled_after_unknown_write",
                            }
                            strict_recovery_topology_required = bool(
                                isinstance(
                                    be_state.get("cleanup_cancel_intent_v1"), dict
                                )
                                or any(
                                    isinstance(item, dict)
                                    and str(item.get("type") or "")
                                    in strict_recovery_action_types
                                    for item in be_actions
                                )
                            )
                            if strict_recovery_topology_required:
                                fresh_pre_cancel_rows = [
                                    item
                                    for item in list(
                                        await adapter.fetch_open_algo_orders(symbol) or []
                                    )
                                    if isinstance(item, dict)
                                ]
                                (
                                    recovery_stop_topology_fingerprint,
                                    recovery_stop_topology_snapshot,
                                ) = _recovery_topology_fingerprint(
                                    fresh_pre_cancel_rows,
                                    symbol=symbol,
                                    position_id=position_id,
                                    side=side,
                                )
                                (
                                    recovery_tp_before_fingerprint,
                                    recovery_tp_before_snapshot,
                                ) = _tp_topology_fingerprint(
                                    fresh_pre_cancel_rows,
                                    position_id=position_id,
                                    side=side,
                                    tracked_tp_order_ids=recovery_tracked_tp_ids,
                                )
                                be_actions.append(
                                    {
                                        "type": "be_recovery_tp_fingerprint_before_cleanup_v1_0_7g7",
                                        "fingerprint": recovery_tp_before_fingerprint,
                                        "snapshot": recovery_tp_before_snapshot,
                                    }
                                )
                                (
                                    fresh_replacement,
                                    fresh_live_old_ids,
                                    fresh_cleanup_topology,
                                ) = _strict_exact_be_cleanup_topology(
                                    fresh_pre_cancel_rows,
                                    position_id=position_id,
                                    side=side,
                                    expected_replacement_id=replacement_id,
                                    owned_old_order_ids=set(old_order_ids),
                                    stop_price=price,
                                    qty=stop_qty,
                                    price_tolerance=verify_price_tolerance,
                                    qty_tolerance=verify_qty_tolerance,
                                    require_old_absent=False,
                                )
                                be_actions.append(
                                    {
                                        "type": "be_recovery_fresh_pre_cancel_topology_v1_0_7g6",
                                        "confirmed": bool(
                                            fresh_cleanup_topology.get("confirmed")
                                        ),
                                        "diagnostics": fresh_cleanup_topology,
                                    }
                                )
                                if (
                                    fresh_replacement is None
                                    or not fresh_cleanup_topology.get("confirmed")
                                ):
                                    reason_code = _canonical_recovery_reason(
                                        fresh_cleanup_topology
                                    )
                                    fresh_cleanup_topology.update(
                                        {
                                            "reason_code": reason_code,
                                            "topology_fingerprint": recovery_stop_topology_fingerprint,
                                            "topology_snapshot": recovery_stop_topology_snapshot,
                                            "tp_fingerprint_before": recovery_tp_before_fingerprint,
                                        }
                                    )
                                    raise BeExistingRecoveryBlocked(
                                        reason_code=reason_code,
                                        diagnostics=fresh_cleanup_topology,
                                        topology_fingerprint=recovery_stop_topology_fingerprint,
                                        message="fresh pre-cancel recovery topology is not exact",
                                    )
                                confirmed_replacement_stop = dict(fresh_replacement)
                                old_order_ids = set(fresh_live_old_ids)
                                all_algo_rows = fresh_pre_cancel_rows
                                live_rows_by_id = {
                                    _algo_order_id(item): item
                                    for item in fresh_pre_cancel_rows
                                    if _algo_order_id(item)
                                }

                            cancel_res: dict[str, Any] | None = None
                            cancel_error_details: dict[str, Any] | None = None
                            cleanup_intent = be_state.get("cleanup_cancel_intent_v1")
                            cleanup_target_ids = set(old_order_ids)
                            if exchange == "bingx":
                                (
                                    cleanup_target_ids,
                                    preserved_non_stop_ids,
                                    already_absent_cleanup_ids,
                                ) = _bingx_stop_only_cleanup_ids(
                                    cleanup_target_ids,
                                    live_rows_by_id,
                                )
                                if preserved_non_stop_ids or already_absent_cleanup_ids:
                                    be_actions.append(
                                        {
                                            "type": "bingx_be_cleanup_targets_stop_only_v1_6_98",
                                            "stop_order_ids": sorted(cleanup_target_ids),
                                            "preserved_non_stop_order_ids": preserved_non_stop_ids,
                                            "already_absent_order_ids": already_absent_cleanup_ids,
                                        }
                                    )
                            cleanup_already_reserved = isinstance(cleanup_intent, dict)
                            if cleanup_already_reserved:
                                intent_ids_raw = cleanup_intent.get("order_ids")
                                intent_ids = {
                                    clean_exchange_id(value)
                                    for value in (
                                        intent_ids_raw
                                        if isinstance(intent_ids_raw, list)
                                        else []
                                    )
                                }
                                intent_ids.discard("")
                                intent_replacement_id = clean_exchange_id(
                                    cleanup_intent.get("replacement_stop_id")
                                )
                                if (
                                    not isinstance(intent_ids_raw, list)
                                    or not intent_ids
                                    or intent_replacement_id != replacement_id
                                ):
                                    be_actions.append(
                                        {
                                            "type": "cleanup_cancel_intent_mismatch",
                                            "intent": dict(cleanup_intent),
                                            "replacement_stop_id": replacement_id,
                                        }
                                    )
                                    raise RuntimeError(
                                        "durable BE cleanup intent is malformed or does "
                                        "not match the confirmed replacement STOP; no "
                                        "cancel write was sent"
                                    )
                                if exchange == "bingx":
                                    (
                                        cleanup_target_ids,
                                        preserved_non_stop_ids,
                                        already_absent_cleanup_ids,
                                    ) = _bingx_stop_only_cleanup_ids(
                                        intent_ids,
                                        live_rows_by_id,
                                    )
                                    if (
                                        cleanup_target_ids != intent_ids
                                        or preserved_non_stop_ids
                                        or already_absent_cleanup_ids
                                    ):
                                        be_actions.append(
                                            {
                                                "type": "bingx_legacy_cleanup_intent_sanitized_to_stop_ids_v1_6_98",
                                                "original_intent_order_ids": sorted(intent_ids),
                                                "stop_order_ids": sorted(cleanup_target_ids),
                                                "preserved_non_stop_order_ids": preserved_non_stop_ids,
                                                "already_absent_order_ids": already_absent_cleanup_ids,
                                                "replacement_stop_id": replacement_id,
                                            }
                                        )
                                else:
                                    cleanup_target_ids = intent_ids
                                old_order_ids = set(cleanup_target_ids)
                                prior_write_error = cleanup_intent.get("write_error")
                                if isinstance(prior_write_error, dict):
                                    cancel_error_details = dict(prior_write_error)
                                be_actions.append(
                                    {
                                        "type": "cleanup_cancel_intent_read_only_resume",
                                        "order_ids": sorted(cleanup_target_ids),
                                        "replacement_stop_id": replacement_id,
                                    }
                                )
                            elif cleanup_target_ids:
                                cleanup_intent = {
                                    "version": 1,
                                    "order_ids": sorted(cleanup_target_ids),
                                    "replacement_stop_id": replacement_id,
                                    "dispatch_state": "reserved_unknown",
                                    "reserved_at": datetime.now(
                                        timezone.utc
                                    ).isoformat(),
                                }
                                await _write_status(
                                    status,
                                    str(row.get("reason") or ""),
                                    {
                                        "be": {
                                            "cleanup_cancel_intent_v1": cleanup_intent
                                        }
                                    },
                                )
                                local_be_state = (
                                    dict(ids_payload.get("be") or {})
                                    if isinstance(ids_payload.get("be"), dict)
                                    else {}
                                )
                                local_be_state["cleanup_cancel_intent_v1"] = (
                                    cleanup_intent
                                )
                                ids_payload["be"] = local_be_state
                                be_state = local_be_state

                                await _invalidate_pre_reads(
                                    adapter, user_id, exchange, symbol
                                )
                                try:
                                    cancel_res = (
                                        await _cancel_exact_with_symbol(
                                            adapter.cancel_conditional_orders_exact,
                                            sorted(cleanup_target_ids),
                                            symbol,
                                        )
                                    )
                                    await _invalidate_pre_reads(
                                        adapter, user_id, exchange, symbol
                                    )
                                except Exception as cancel_exc:
                                    await _invalidate_pre_reads(
                                        adapter, user_id, exchange, symbol
                                    )
                                    # The write may have been rejected, ambiguous or
                                    # accepted with a lost response.  Persist the
                                    # exact exchange error and never retry;
                                    # authoritative readback below decides the state.
                                    cancel_error_details = _exchange_error_details(
                                        cancel_exc
                                    )
                                    cleanup_intent = {
                                        **cleanup_intent,
                                        "dispatch_state": "write_error_or_unknown",
                                        "write_error": cancel_error_details,
                                        "response_at": datetime.now(
                                            timezone.utc
                                        ).isoformat(),
                                    }
                                    await _write_status(
                                        status,
                                        str(row.get("reason") or ""),
                                        {
                                            "be": {
                                                "cleanup_cancel_intent_v1": cleanup_intent
                                            }
                                        },
                                    )
                                    be_state["cleanup_cancel_intent_v1"] = (
                                        cleanup_intent
                                    )
                                    be_actions.append(
                                        {
                                            "type": "old_conditionals_cancel_write_error",
                                            "order_ids": sorted(cleanup_target_ids),
                                            "replacement_stop_id": replacement_id,
                                            "error": cancel_error_details,
                                        }
                                    )
                                else:
                                    cleanup_intent = {
                                        **cleanup_intent,
                                        "dispatch_state": "response_received",
                                        "response": cancel_res,
                                        "response_at": datetime.now(
                                            timezone.utc
                                        ).isoformat(),
                                    }
                                    # Keep the exact exchange result durable before
                                    # any subsequent readback or TP recreation. A DB
                                    # failure here is not mislabeled as a BingX error.
                                    await _write_status(
                                        status,
                                        str(row.get("reason") or ""),
                                        {
                                            "be": {
                                                "cleanup_cancel_intent_v1": cleanup_intent
                                            }
                                        },
                                    )
                                    be_state["cleanup_cancel_intent_v1"] = (
                                        cleanup_intent
                                    )
                                    be_actions.append(
                                        {
                                            "type": "old_conditionals_cancelled_exactly",
                                            "order_ids": sorted(cleanup_target_ids),
                                            "replacement_stop_id": replacement_id,
                                            "result": cancel_res,
                                        }
                                    )
                            else:
                                be_actions.append(
                                    {
                                        "type": "no_owned_old_conditionals_to_cancel",
                                        "replacement_stop_id": replacement_id,
                                    }
                                )

                                # 4) Confirm every old id disappeared and the new BE
                                # STOP still exists.  A failed read/cancel never leads
                                # to blind TP recreation.
                            (
                                replaced,
                                confirmed_be_stop,
                                remaining_old,
                                replace_error,
                                replace_diagnostics,
                            ) = await _wait_exact_conditionals_replaced(
                                adapter,
                                symbol=symbol,
                                position_id=position_id,
                                side=side,
                                old_order_ids=old_order_ids,
                                stop_price=price,
                                qty=stop_qty,
                                price_tolerance=verify_price_tolerance,
                                qty_tolerance=verify_qty_tolerance,
                                expected_new_order_id=replacement_id,
                                require_only_expected_stop=strict_recovery_topology_required,
                                expected_tp_fingerprint=recovery_tp_before_fingerprint,
                                tracked_tp_order_ids=recovery_tracked_tp_ids,
                                enforce_tp_unchanged=strict_recovery_topology_required,
                                attempts=(1 if strict_recovery_topology_required else 12),
                            )
                            be_actions.append(
                                {
                                    "type": "be_exact_replacement_confirmation",
                                    "confirmed": replaced,
                                    "remaining_count": len(remaining_old),
                                    "replacement_stop_id": _algo_order_id(
                                        confirmed_be_stop or {}
                                    ),
                                    "error": replace_error,
                                    "cancel_error": cancel_error_details,
                                    "diagnostics": replace_diagnostics,
                                }
                            )
                            if strict_recovery_topology_required:
                                be_actions.append(
                                    {
                                        "type": "be_recovery_tp_fingerprint_after_cleanup_v1_0_7g7",
                                        "expected": recovery_tp_before_fingerprint,
                                        "actual": replace_diagnostics.get(
                                            "tp_fingerprint_actual"
                                        ),
                                        "unchanged": bool(
                                            replace_diagnostics.get("tp_unchanged")
                                        ),
                                    }
                                )
                                if not replaced or confirmed_be_stop is None:
                                    strict_diag = (
                                        replace_diagnostics.get(
                                            "strict_cleanup_topology"
                                        )
                                        if isinstance(
                                            replace_diagnostics.get(
                                                "strict_cleanup_topology"
                                            ),
                                            dict,
                                        )
                                        else {}
                                    )
                                    raw_reason = str(
                                        replace_diagnostics.get("reason")
                                        or strict_diag.get("reason")
                                        or "recovery_not_proven"
                                    )
                                    reason_code = _canonical_recovery_reason(
                                        {"reason": raw_reason}
                                    )
                                    recovery_diagnostics = {
                                        **replace_diagnostics,
                                        "reason": raw_reason,
                                        "reason_code": reason_code,
                                        "tp_fingerprint_before": recovery_tp_before_fingerprint,
                                        "tp_snapshot_before": recovery_tp_before_snapshot,
                                        "open_orders_reads": 3,
                                    }
                                    raise BeExistingRecoveryBlocked(
                                        reason_code=reason_code,
                                        diagnostics=recovery_diagnostics,
                                        topology_fingerprint=str(
                                            replace_diagnostics.get(
                                                "topology_fingerprint"
                                            )
                                            or recovery_stop_topology_fingerprint
                                        ),
                                        message="final existing-BE recovery verification failed",
                                    )

                            if not replaced or confirmed_be_stop is None:
                                history_stop, history_reason = (
                                    await _find_checkpointed_stop_from_history(
                                        adapter,
                                        symbol=symbol,
                                        stop_ids={replacement_id},
                                        position_id=position_id,
                                        side=side,
                                        stop_price=price,
                                        qty=stop_qty,
                                        price_tolerance=verify_price_tolerance,
                                        qty_tolerance=verify_qty_tolerance,
                                    )
                                )
                                be_actions.append(
                                    {
                                        "type": "be_exact_replacement_history_recheck",
                                        "replacement_stop_id": replacement_id,
                                        "reason": history_reason,
                                        "confirmed": history_stop is not None,
                                    }
                                )
                                # History can prove the exact replacement write, but
                                # it cannot prove the *live* symbol STOP topology.  A
                                # recovery cleanup requires openOrders to show that no
                                # extra same-side STOP remains; never let a history row
                                # bypass that uniqueness proof.
                                if (
                                    history_stop is not None
                                    and not remaining_old
                                    and not strict_recovery_topology_required
                                ):
                                    replaced = True
                                    confirmed_be_stop = dict(history_stop)

                            if not replaced or confirmed_be_stop is None:
                                cancel_reason = ""
                                if cancel_error_details:
                                    cancel_reason = (
                                        "; cancel_response="
                                        f"{cancel_error_details.get('exception')} "
                                        f"code={cancel_error_details.get('error_code')} "
                                        f"message={cancel_error_details.get('error_message')or cancel_error_details.get('message')}"
                                    )
                                if cleanup_already_reserved:
                                    cancel_reason += (
                                        "; cleanup_resume=read_only_no_repeat"
                                    )
                                strict_reason = ""
                                if isinstance(
                                    replace_diagnostics.get("strict_cleanup_topology"),
                                    dict,
                                ):
                                    strict_reason = str(
                                        replace_diagnostics["strict_cleanup_topology"].get(
                                            "reason"
                                        )
                                        or ""
                                    )
                                raise RuntimeError(
                                    "exact BE replacement was not confirmed; "
                                    f"remaining={len(remaining_old)} error={replace_error or '-'}"
                                    f"{cancel_reason}; strict_topology={strict_reason or '-'}; "
                                    f"{_stop_diagnostics_summary(replace_diagnostics)}"
                                )

                                # 5) Recreate only the remaining targets from the
                                # immutable entry snapshot. Prices, target indexes and
                                # target membership never come from current Railway
                                # settings. Quantities may only be reduced to fit the
                                # confirmed live remainder after exchange rounding.
                                # A fast move can fill TP1 and TP2 before BE-after-TP1
                                # starts. Recreating from trigger_idx would recreate TP2
                                # even though it already executed, potentially closing an
                                # extra slice immediately. Exclude every TP already
                                # confirmed filled, while always treating the configured
                                # trigger prefix as completed.
                            filled_tp_indexes = _filled_tp_indexes(ids_payload)
                            filled_tp_indexes.update(
                                int(item["tp_index"])
                                for item in plan_items[:trigger_idx]
                            )
                            qty_step = (
                                float((snapshot or {}).get("qty_step") or 0.0) or None
                            )
                            if not qty_step:
                                try:
                                    info = await adapter.instrument_info(symbol)
                                    qty_step = (
                                        float(getattr(info, "qty_step", 0.0) or 0.0)
                                        or None
                                    )
                                except Exception:
                                    qty_step = None

                            # v1.6.96 BingX live safety fix: moving STOP to BE must
                            # never delete or recreate existing TP orders.  The previous
                            # MEXC-parity flow cancelled all tracked conditionals and
                            # then attempted to rebuild remaining TPs.  On BingX that
                            # produced the live ONDO failure: the BE STOP moved, but most
                            # TPs were cancelled and recreation stopped on a same-side TP
                            # row without positionId.  For BingX, BE now only replaces
                            # the protective STOP and preserves every existing TP exact-id.
                            preserve_existing_tp_on_be = exchange == "bingx"
                            if preserve_existing_tp_on_be:
                                remaining_base_items = [
                                    item
                                    for item in plan_items
                                    if int(item["tp_index"]) not in filled_tp_indexes
                                ]
                                recreated_items: list[dict[str, Any]] = []
                            else:
                                remaining_base_items = [
                                    item
                                    for item in plan_items
                                    if int(item["tp_index"]) not in filled_tp_indexes
                                ]
                                recreated_items = rebase_snapshot_items_to_qty(
                                    remaining_base_items,
                                    total_qty=qty_now,
                                    qty_step=qty_step,
                                )
                                if remaining_base_items and not recreated_items:
                                    raise RuntimeError(
                                        "BE STOP was set, but remaining position is below the executable TP quantity step"
                                    )
                                if not remaining_base_items and qty_now > tolerance:
                                    raise RuntimeError(
                                        "BE STOP was set, but immutable TP plan has no remaining targets for a live position"
                                    )

                            be_actions.append(
                                {
                                    "type": (
                                        "be_existing_tp_preserved_without_recreation_v1_6_96"
                                        if preserve_existing_tp_on_be
                                        else "be_remaining_tp_plan"
                                    ),
                                    "snapshot_locked": True,
                                    "trigger_ordinal": trigger_idx,
                                    "trigger_tp_index": trigger_plan_index,
                                    "filled_tp_indexes": sorted(filled_tp_indexes),
                                    "qty_step": qty_step,
                                    "live_qty": qty_now,
                                    "tracked_tp_ids_preserved_for_be": tracked_tp_ids_preserved_for_be,
                                    "remaining_snapshot_targets": [
                                        dict(item) for item in remaining_base_items
                                    ],
                                    "plan": [dict(item) for item in recreated_items],
                                }
                            )

                            tp_parallel_limit = max(
                                1, int(getattr(get_settings(), "TP_PARALLEL_LIMIT", 5))
                            )
                            if exchange == "mexc":
                                tp_parallel_limit = min(tp_parallel_limit, 4)
                            tp_sem = asyncio.Semaphore(tp_parallel_limit)

                            async def _place_be_tp(
                                sequence: int,
                                tp_index: int,
                                tp_target: float,
                                tp_planned: float,
                            ):
                                if exchange == "mexc":
                                    await asyncio.sleep(((sequence - 1) // 4) * 2.10)
                                async with tp_sem:
                                    tp_client_id = _stable_client_id(
                                        "avc-be-tp", execution_id, user_id, tp_index
                                    )
                                    try:
                                        await _invalidate_pre_reads(
                                            adapter, user_id, exchange, symbol
                                        )
                                        tp_res = await adapter.create_take_profit(
                                            symbol=symbol,
                                            side=side,
                                            qty=tp_planned,
                                            price=float(tp_target),
                                            client_id=tp_client_id,
                                            position_id=position_id,
                                            adopt_existing=False,
                                        )
                                        await _invalidate_pre_reads(
                                            adapter, user_id, exchange, symbol
                                        )
                                        # The adapter intentionally suppresses an
                                        # identical live TP as an idempotent retry.
                                        # During BE replacement, however, an
                                        # identical order can be a manual/external
                                        # TP that was deliberately preserved above.
                                        # Never adopt such an order into the bot's
                                        # durable ledger: that would make a later
                                        # cleanup treat the user's order as bot-owned.
                                        returned_tp_id = _tp_order_id_from_payload(
                                            tp_res
                                        )
                                        if (
                                            returned_tp_id
                                            and returned_tp_id
                                            in preserved_untracked_ids
                                        ):
                                            raise RuntimeError(
                                                "BE TP recreation found a matching manual/external "
                                                f"order {returned_tp_id}; it was preserved and not "
                                                "adopted as bot-owned"
                                            )
                                        return (
                                            tp_index,
                                            tp_target,
                                            tp_planned,
                                            tp_client_id,
                                            tp_res,
                                            None,
                                        )
                                    except Exception as exc:
                                        await _invalidate_pre_reads(
                                            adapter, user_id, exchange, symbol
                                        )
                                        return (
                                            tp_index,
                                            tp_target,
                                            tp_planned,
                                            tp_client_id,
                                            None,
                                            exc,
                                        )

                            be_tp_results = []
                            be_tp_plan_rows = [
                                item
                                for item in recreated_items
                                if float(item.get("qty") or 0.0) > 0
                            ]
                            if exchange == "bingx":
                                # v1.6.68 MEXC parity: BE remaining TP recreation
                                # must be sequential fail-fast too.  If TP3 fails,
                                # do not let already scheduled TP4/TP5 writes continue
                                # after the state has become partial/manual.
                                for sequence, item in enumerate(be_tp_plan_rows, start=1):
                                    result = await _place_be_tp(
                                        sequence,
                                        int(item["tp_index"]),
                                        float(item["price"]),
                                        float(item["qty"]),
                                    )
                                    be_tp_results.append(result)
                                    if result[5] is not None:
                                        break
                                be_actions.append(
                                    {
                                        "type": "be_tp_sequential_parity_v1",
                                        "enabled": True,
                                        "attempted": len(be_tp_results),
                                        "planned": len(be_tp_plan_rows),
                                        "fail_fast": any(r[5] is not None for r in be_tp_results),
                                    }
                                )
                            else:
                                be_tp_tasks = [
                                    _place_be_tp(
                                        sequence,
                                        int(item["tp_index"]),
                                        float(item["price"]),
                                        float(item["qty"]),
                                    )
                                    for sequence, item in enumerate(
                                        be_tp_plan_rows, start=1
                                    )
                                ]
                                be_tp_results = (
                                    await asyncio.gather(*be_tp_tasks)
                                    if be_tp_tasks
                                    else []
                                )
                            be_tp_results.sort(key=lambda result: int(result[0]))
                            for (
                                tp_index,
                                tp,
                                planned,
                                tp_client_id,
                                tp_res,
                                tp_exc,
                            ) in be_tp_results:
                                if tp_exc is not None:
                                    log.warning(
                                        "BE recreate TP%d failed for %s: %s: %s",
                                        tp_index,
                                        symbol,
                                        type(tp_exc).__name__,
                                        tp_exc,
                                    )
                                    remaining_tp_orders.append(
                                        {
                                            "tp_index": tp_index,
                                            "tp": float(tp),
                                            "qty": 0.0,
                                            "planned_qty": float(planned),
                                            "client_id": tp_client_id,
                                            "result": None,
                                            "error": f"{type(tp_exc).__name__}: {tp_exc}",
                                        }
                                    )
                                    continue
                                actual_tp_qty = order_normalized_qty(tp_res, planned)
                                remaining_tp_orders.append(
                                    {
                                        "tp_index": tp_index,
                                        "tp": float(tp),
                                        "qty": actual_tp_qty,
                                        "planned_qty": float(planned),
                                        "client_id": tp_client_id,
                                        "result": tp_res,
                                    }
                                )

                            failed_recreated = [
                                item
                                for item in remaining_tp_orders
                                if item.get("error")
                            ]
                            tp_recreation_failed = bool(failed_recreated)
                            tp_recreation_failure_details = ""
                            if failed_recreated:
                                tp_recreation_failure_details = "; ".join(
                                    f"TP{item.get('tp_index')}: {str(item.get('error') or 'unknown error')[:300]}"
                                    for item in failed_recreated
                                )
                                safe_be_partial_success = all(
                                    "BingxTpOwnershipError" in str(item.get("error") or "")
                                    and "same-side TP order(s) without positionId" in str(item.get("error") or "")
                                    for item in failed_recreated
                                )
                                if not safe_be_partial_success:
                                    raise RuntimeError(
                                        "BE STOP was set, but remaining TP recreation failed: "
                                        f"{tp_recreation_failure_details}"
                                    )
                                # v1.6.94: A confirmed BE STOP is the primary safety
                                # outcome for this BingX-specific edge case. BingX can
                                # return same-side TP rows without positionId, and strict
                                # TP recreation may then fail after the STOP was already
                                # moved to BE. Do not report "BE not installed" and do
                                # not retry BE forever. Keep the BE success path, record
                                # the TP issue durably, and ask the user to inspect TP
                                # manually. Other TP errors still fail closed.
                                be_actions.append(
                                    {
                                        "type": "be_tp_recreation_partial_failure_v1",
                                        "be_stop_confirmed": True,
                                        "manual_review_required_for_tp": True,
                                        "failure_class": "bingx_same_side_tp_missing_position_id",
                                        "failed": [
                                            {
                                                "tp_index": item.get("tp_index"),
                                                "tp": item.get("tp"),
                                                "planned_qty": item.get("planned_qty"),
                                                "error": item.get("error"),
                                            }
                                            for item in failed_recreated
                                        ],
                                    }
                                )

                            updated_tp_rows = _merge_recreated_tp_orders(
                                ids_payload, remaining_tp_orders
                            )

                            # 6) Verify the exact BE stop for this position.
                            # BingX can acknowledge the write before open_orders
                            # reflects it, so use bounded eventual-consistency
                            # polling instead of one immediate read.
                            algo_orders: list = []
                            relevant_algo_orders: list[dict[str, Any]] = []
                            matching_be_stop: dict[str, Any] | None = (
                                dict(confirmed_be_stop)
                                if strict_recovery_topology_required
                                and isinstance(confirmed_be_stop, dict)
                                else None
                            )
                            verify_endpoint_available = bool(
                                strict_recovery_topology_required
                                and matching_be_stop is not None
                            )
                            verify_last_error = ""
                            try:
                                verify_info = await adapter.instrument_info(symbol)
                                verify_price_tolerance = max(
                                    float(
                                        getattr(verify_info, "price_tick", 0.0) or 0.0
                                    )
                                    * 0.51,
                                    abs(float(price)) * 1e-9,
                                    1e-12,
                                )
                                verify_qty_tolerance = max(
                                    float(getattr(verify_info, "qty_step", 0.0) or 0.0)
                                    * 0.51,
                                    1e-12,
                                )
                            except Exception:
                                verify_price_tolerance = max(
                                    abs(float(price)) * 1e-8, 1e-10
                                )
                                verify_qty_tolerance = max(
                                    abs(float(stop_qty)) * 0.001, 1e-10
                                )

                            for verify_attempt in range(
                                0 if strict_recovery_topology_required else 8
                            ):
                                if verify_attempt:
                                    await asyncio.sleep(min(1.0, 0.20 * verify_attempt))
                                try:
                                    algo_orders = list(
                                        await adapter.fetch_open_algo_orders(symbol)
                                        or []
                                    )
                                    verify_endpoint_available = True
                                    verify_last_error = ""
                                except Exception as verify_exc:
                                    verify_endpoint_available = False
                                    verify_last_error = (
                                        f"{type(verify_exc).__name__}: {verify_exc}"
                                    )
                                    continue
                                relevant_algo_orders = _be_final_verify_candidate_orders(
                                    [
                                        order
                                        for order in algo_orders
                                        if isinstance(order, dict)
                                    ],
                                    position_id=position_id,
                                    side=side,
                                    expected_order_id=replacement_id,
                                )
                                matching_be_stop = _matching_stop_order(
                                    relevant_algo_orders,
                                    position_id=position_id,
                                    side=side,
                                    stop_price=price,
                                    qty=stop_qty,
                                    price_tolerance=verify_price_tolerance,
                                    qty_tolerance=verify_qty_tolerance,
                                    expected_order_id=replacement_id,
                                )
                                if matching_be_stop is not None:
                                    break

                            if matching_be_stop is None and replacement_id:
                                history_stop, history_reason = (
                                    await _find_checkpointed_stop_from_history(
                                        adapter,
                                        symbol=symbol,
                                        stop_ids={replacement_id},
                                        position_id=position_id,
                                        side=side,
                                        stop_price=price,
                                        qty=stop_qty,
                                        price_tolerance=verify_price_tolerance,
                                        qty_tolerance=verify_qty_tolerance,
                                    )
                                )
                                be_actions.append(
                                    {
                                        "type": "final_be_stop_history_recheck",
                                        "replacement_stop_id": replacement_id,
                                        "reason": history_reason,
                                        "confirmed": history_stop is not None,
                                    }
                                )
                                if history_stop is not None:
                                    matching_be_stop = dict(history_stop)
                                    verify_endpoint_available = True

                            strict_final_topology: dict[str, Any] = {
                                "confirmed": True,
                                "reason": "not_required_for_normal_be",
                            }
                            if strict_recovery_topology_required:
                                strict_final_topology = (
                                    replace_diagnostics.get("strict_cleanup_topology")
                                    if isinstance(
                                        replace_diagnostics.get(
                                            "strict_cleanup_topology"
                                        ),
                                        dict,
                                    )
                                    else {
                                        "confirmed": False,
                                        "reason": "missing_bounded_final_topology",
                                    }
                                )
                                if not strict_final_topology.get("confirmed"):
                                    matching_be_stop = None
                                be_actions.append(
                                    {
                                        "type": "be_recovery_final_unique_stop_topology_v1_0_7g7",
                                        "confirmed": bool(
                                            strict_final_topology.get("confirmed")
                                        ),
                                        "diagnostics": strict_final_topology,
                                        "reused_bounded_final_readback": True,
                                    }
                                )

                            final_verify_diagnostics = (
                                dict(replace_diagnostics)
                                if strict_recovery_topology_required
                                else _stop_confirmation_diagnostics(
                                    [
                                        order
                                        for order in algo_orders
                                        if isinstance(order, dict)
                                    ],
                                    position_id=position_id,
                                    side=side,
                                    stop_price=price,
                                    qty=stop_qty,
                                    price_tolerance=verify_price_tolerance,
                                    qty_tolerance=verify_qty_tolerance,
                                    expected_order_id=replacement_id,
                                    old_order_ids=set(),
                                )
                            )

                            if verify_last_error:
                                be_actions.append(
                                    {
                                        "type": "verify_algo_failed",
                                        "reason": verify_last_error,
                                    }
                                )
                            stop_like_count = sum(
                                1
                                for order in relevant_algo_orders
                                if _looks_stop_order(order)
                            )
                            # Fail closed when the read endpoint failed or when the exact
                            # position/price/quantity STOP is absent.
                            if (
                                not verify_endpoint_available
                                or matching_be_stop is None
                                or not strict_final_topology.get("confirmed", False)
                            ):
                                ownership_patch = _be_ownership_patch(
                                    claimed_limit_attached_stop,
                                    confirmed_replacement_stop,
                                    replacement_in_progress=False,
                                )
                                ownership_be = ownership_patch.pop("be", {})
                                be_patch = {
                                    **ownership_patch,
                                    "be": {
                                        **ownership_be,
                                        "moved": False,
                                        "manual_required": True,
                                        "manual_requested": (
                                            False
                                            if manual_requested
                                            else be_state.get("manual_requested")
                                        ),
                                        "source": (
                                            "manual"
                                            if manual_requested
                                            else "automatic"
                                        ),
                                        "trigger_tp_index": trigger_plan_index,
                                        "trigger_ordinal": trigger_idx,
                                        "stop": price,
                                        "qty": stop_qty,
                                        "position_qty": qty_now,
                                        "original_qty": original_qty,
                                        "basis_entry": actual_entry,
                                        "signal_entry": signal_entry,
                                        "tp_price_seen": price_seen,
                                        "tp_price_seen_at": current_price,
                                        "tp_target": trigger_target,
                                        "calculation": be_calc,
                                        "actions": be_actions,
                                        "remaining_tp_recreated": remaining_tp_orders,
                                        "verify_algo_orders": algo_orders,
                                        "verify_stop_diagnostics": final_verify_diagnostics,
                                        **(
                                            {
                                                "manual_result": {
                                                    "state": "verification_failed",
                                                    "reason": "BingX не подтвердила точный новый STOP.",
                                                    "stop": price,
                                                    "market_price": current_price,
                                                }
                                            }
                                            if manual_requested
                                            else {}
                                        ),
                                    },
                                    "tp": updated_tp_rows,
                                    "actual_entry": actual_entry,
                                }
                                await _write_status(
                                    "manual_required",
                                    (
                                        "manual BE replace failed verification: exact live STOP was not confirmed"
                                        if manual_requested
                                        else f"BE replace failed verification after TP{trigger_plan_index}: exact live STOP was not confirmed"
                                    ),
                                    be_patch,
                                )
                                await _notify(
                                    notify,
                                    user_id,
                                    card(
                                        "🟡 <b>Б/У НЕ ПОДТВЕРЖДЁН</b>",
                                        symbol=symbol,
                                        side=side,
                                        blocks=(
                                            [
                                                (
                                                    "🔒 <b>Условие:</b> принудительный Б/У"
                                                    if manual_requested
                                                    else f"🎯 <b>Сработал:</b> TP{trigger_plan_index}"
                                                ),
                                                f"📤 <b>Новый STOP отправлен:</b> {fmt_price(price)}",
                                                f"📦 <b>Остаток позиции:</b> {fmt_qty(qty_now)}",
                                            ],
                                            [
                                                "❓ BingX не подтвердила точный STOP по позиции, цене и объёму",
                                                details_line(
                                                    _stop_diagnostics_summary(
                                                        final_verify_diagnostics
                                                    )
                                                ),
                                                "🔄 Автоматическая проверка завершилась без подтверждения",
                                                "📱 Срочно проверьте позицию, STOP и TP вручную",
                                            ],
                                        ),
                                    ),
                                )
                                await _invalidate_pre_reads(adapter, user_id, exchange, symbol)
                                moved += 1
                                continue
                    except BeExistingRecoveryBlocked as be_exc:
                        now = _utc_now()
                        reason_code = str(
                            be_exc.reason_code or "recovery_not_proven"
                        )
                        topology_fingerprint = str(
                            be_exc.topology_fingerprint or ""
                        )
                        backoff_state = _recovery_backoff_state(
                            be_state,
                            reason_code=reason_code,
                            topology_fingerprint=topology_fingerprint,
                            now=now,
                        )
                        notify_due = _recovery_notification_due(
                            be_state,
                            reason_code=reason_code,
                            topology_fingerprint=topology_fingerprint,
                            now=now,
                        )
                        bounded_diagnostics = dict(be_exc.diagnostics or {})
                        bounded_diagnostics.setdefault("reason_code", reason_code)
                        bounded_diagnostics.setdefault(
                            "topology_fingerprint", topology_fingerprint
                        )
                        blocked_record = {
                            **backoff_state,
                            "diagnostics": bounded_diagnostics,
                        }
                        rate_limit_confirmation_pending = (
                            _rate_limit_write_confirmation_pending(be_state)
                        )
                        rate_limit_confirmation_patch: dict[str, Any] | None = None
                        if rate_limit_confirmation_pending:
                            prior_rate_marker = dict(
                                be_state.get("rate_limit_write_confirmation_v1") or {}
                            )
                            rate_limit_confirmation_patch = {
                                **prior_rate_marker,
                                "state": "pending",
                                "last_checked_at": backoff_state["checked_at"],
                                "readback_attempt": backoff_state[
                                    "same_topology_attempt"
                                ],
                                "next_readback_after": backoff_state[
                                    "next_retry_after"
                                ],
                                "last_reason": reason_code,
                                "last_topology_fingerprint": topology_fingerprint,
                                "exchange_writes_after_error": 0,
                            }
                        ownership_patch = _be_ownership_patch(
                            claimed_limit_attached_stop,
                            confirmed_replacement_stop,
                            replacement_in_progress=rate_limit_confirmation_pending,
                        )
                        ownership_be = ownership_patch.pop("be", {})
                        blocked_actions = [
                            *be_actions,
                            {
                                "type": "existing_be_recovery_blocked_v1_0_7g7",
                                "reason": reason_code,
                                "topology_fingerprint": topology_fingerprint,
                                "same_topology_attempt": backoff_state[
                                    "same_topology_attempt"
                                ],
                                "backoff_sec": backoff_state["backoff_sec"],
                                "next_retry_after": backoff_state[
                                    "next_retry_after"
                                ],
                                "notification_sent": notify_due,
                            },
                        ]
                        be_patch = {
                            **ownership_patch,
                            "be": {
                                **ownership_be,
                                "moved": False,
                                "manual_required": True,
                                "manual_requested": False,
                                "source": "automatic_recovery",
                                "trigger_tp_index": trigger_plan_index,
                                "trigger_ordinal": trigger_idx,
                                "stop": price,
                                "qty": stop_qty,
                                "position_qty": qty_now,
                                "original_qty": original_qty,
                                "basis_entry": actual_entry,
                                "signal_entry": signal_entry,
                                "calculation": be_calc,
                                "actions": blocked_actions,
                                "error": (
                                    "BeExistingRecoveryBlocked: "
                                    + reason_code
                                    + "; raw_reason="
                                    + str(
                                        bounded_diagnostics.get("reason")
                                        or reason_code
                                    )
                                ),
                                "existing_be_recovery_blocked_v1": blocked_record,
                                "existing_be_recovery_last_checked_at": backoff_state[
                                    "checked_at"
                                ],
                                "existing_be_recovery_next_retry_after": backoff_state[
                                    "next_retry_after"
                                ],
                                **(
                                    {
                                        "rate_limit_write_confirmation_v1": rate_limit_confirmation_patch
                                    }
                                    if rate_limit_confirmation_patch is not None
                                    else {}
                                ),
                                "existing_be_recovery_owned_replacement_ids": sorted(
                                    {
                                        *_durable_exact_be_replacement_ids(
                                            {
                                                **be_state,
                                                "actions": [
                                                    *(
                                                        be_state.get("actions")
                                                        if isinstance(
                                                            be_state.get("actions"), list
                                                        )
                                                        else []
                                                    ),
                                                    *blocked_actions,
                                                ],
                                            }
                                        ),
                                        *(
                                            clean_exchange_id(value)
                                            for value in bounded_diagnostics.get(
                                                "durable_exact_replacement_ids", []
                                            )
                                            if clean_exchange_id(value)
                                        ),
                                        *(
                                            [
                                                clean_exchange_id(
                                                    bounded_diagnostics.get(
                                                        "replacement_stop_id"
                                                    )
                                                )
                                            ]
                                            if clean_exchange_id(
                                                bounded_diagnostics.get(
                                                    "replacement_stop_id"
                                                )
                                            )
                                            else []
                                        ),
                                    }
                                ),
                                "existing_be_recovery_last_notified_reason": (
                                    reason_code
                                    if notify_due
                                    else be_state.get(
                                        "existing_be_recovery_last_notified_reason"
                                    )
                                ),
                                "existing_be_recovery_last_notified_topology_fingerprint": (
                                    topology_fingerprint
                                    if notify_due
                                    else be_state.get(
                                        "existing_be_recovery_last_notified_topology_fingerprint"
                                    )
                                ),
                                "existing_be_recovery_last_notified_at": (
                                    _iso_utc(now)
                                    if notify_due
                                    else be_state.get(
                                        "existing_be_recovery_last_notified_at"
                                    )
                                ),
                            },
                            "actual_entry": actual_entry,
                        }
                        await _write_status(
                            "manual_required",
                            (
                                "existing BE recovery blocked safely: "
                                f"{reason_code}; retry_after="
                                f"{backoff_state['next_retry_after']}"
                            ),
                            be_patch,
                        )
                        if reason_code == "replacement_qty_under_position":
                            intent_qty_diag = _signed_f(
                                bounded_diagnostics.get("intent_qty"), 0.0
                            )
                            required_qty_diag = _signed_f(
                                bounded_diagnostics.get("current_qty"), stop_qty
                            )
                            qty_gap_diag = max(
                                0.0, required_qty_diag - intent_qty_diag
                            )
                            log.error(
                                "BE_REPLACEMENT_QTY_UNDER_POSITION "
                                "execution_id=%s user_id=%s symbol=%s side=%s "
                                "position_id=%s intent_qty=%s required_stop_qty=%s "
                                "position_qty=%s qty_gap=%s replacement_stop_id=%s "
                                "old_stop_ids=%s attempt=%s next_retry_after=%s notify=%s",
                                execution_id,
                                user_id,
                                symbol,
                                side,
                                clean_exchange_id(
                                    bounded_diagnostics.get("current_position_id")
                                ),
                                fmt_qty(intent_qty_diag),
                                fmt_qty(required_qty_diag),
                                fmt_qty(
                                    _signed_f(
                                        bounded_diagnostics.get("position_qty"),
                                        qty_now,
                                    )
                                ),
                                fmt_qty(qty_gap_diag),
                                clean_exchange_id(
                                    bounded_diagnostics.get("replacement_stop_id")
                                )
                                or "unknown",
                                ",".join(
                                    str(value)
                                    for value in list(
                                        bounded_diagnostics.get("old_stop_ids") or []
                                    )[:5]
                                )
                                or "none",
                                backoff_state["same_topology_attempt"],
                                backoff_state["next_retry_after"],
                                notify_due,
                            )
                        elif reason_code == "replacement_qty_over_position":
                            intent_qty_diag = _signed_f(
                                bounded_diagnostics.get("intent_qty"), 0.0
                            )
                            required_qty_diag = _signed_f(
                                bounded_diagnostics.get("current_qty"), stop_qty
                            )
                            qty_excess_diag = max(
                                0.0, intent_qty_diag - required_qty_diag
                            )
                            log.warning(
                                "BE_REPLACEMENT_QTY_OVER_POSITION_READ_ONLY_BLOCKED "
                                "execution_id=%s user_id=%s symbol=%s side=%s "
                                "position_id=%s intent_qty=%s required_stop_qty=%s "
                                "position_qty=%s qty_excess=%s replacement_stop_id=%s "
                                "old_stop_ids=%s attempt=%s next_retry_after=%s notify=%s",
                                execution_id,
                                user_id,
                                symbol,
                                side,
                                clean_exchange_id(
                                    bounded_diagnostics.get("current_position_id")
                                ),
                                fmt_qty(intent_qty_diag),
                                fmt_qty(required_qty_diag),
                                fmt_qty(
                                    _signed_f(
                                        bounded_diagnostics.get("position_qty"),
                                        qty_now,
                                    )
                                ),
                                fmt_qty(qty_excess_diag),
                                clean_exchange_id(
                                    bounded_diagnostics.get("replacement_stop_id")
                                )
                                or "unknown",
                                ",".join(
                                    str(value)
                                    for value in list(
                                        bounded_diagnostics.get("old_stop_ids") or []
                                    )[:5]
                                )
                                or "none",
                                backoff_state["same_topology_attempt"],
                                backoff_state["next_retry_after"],
                                notify_due,
                            )
                        log.warning(
                            "BE_EXISTING_RECOVERY_BLOCKED execution_id=%s user_id=%s "
                            "symbol=%s reason=%s fingerprint=%s attempt=%s "
                            "backoff_sec=%s notify=%s",
                            execution_id,
                            user_id,
                            symbol,
                            reason_code,
                            topology_fingerprint,
                            backoff_state["same_topology_attempt"],
                            backoff_state["backoff_sec"],
                            notify_due,
                        )
                        if notify_due:
                            await _notify(
                                notify,
                                user_id,
                                card(
                                    "🟡 <b>ВОССТАНОВЛЕНИЕ Б/У ОСТАНОВЛЕНО БЕЗОПАСНО</b>",
                                    symbol=symbol,
                                    side=side,
                                    blocks=(
                                        [
                                            f"🆔 <b>Execution:</b> {execution_id}",
                                            f"🛡 <b>Расчётный STOP:</b> {fmt_price(price)}",
                                            f"📦 <b>Остаток позиции:</b> {fmt_qty(qty_now)}",
                                        ],
                                        [
                                            "🚫 Новый STOP не создавался",
                                            "🚫 Неизвестные STOP не отменялись",
                                            "✅ TP не изменялись ботом",
                                        ],
                                        [
                                            *(
                                                [
                                                    f"📏 <b>STOP intent qty:</b> {fmt_qty(_signed_f(bounded_diagnostics.get('intent_qty'), 0.0))}",
                                                    f"📦 <b>Требуемый STOP qty:</b> {fmt_qty(_signed_f(bounded_diagnostics.get('current_qty'), stop_qty))}",
                                                    f"⚠️ <b>Разница покрытия:</b> {fmt_qty(max(0.0, _signed_f(bounded_diagnostics.get('current_qty'), stop_qty) - _signed_f(bounded_diagnostics.get('intent_qty'), 0.0)))}",
                                                    "🧭 <b>Действие:</b> проверьте остаток позиции и активный STOP на BingX",
                                                ]
                                                if reason_code == "replacement_qty_under_position"
                                                else [
                                                    f"📏 <b>Старый STOP intent qty:</b> {fmt_qty(_signed_f(bounded_diagnostics.get('intent_qty'), 0.0))}",
                                                    f"📦 <b>Текущий остаток:</b> {fmt_qty(_signed_f(bounded_diagnostics.get('current_qty'), stop_qty))}",
                                                    "🔎 <b>Действие:</b> бот выполнил только read-only проверку точного старого STOP",
                                                ]
                                                if reason_code == "replacement_qty_over_position"
                                                else []
                                            ),
                                            details_line(
                                                f"reason={reason_code}; retry_after={backoff_state['next_retry_after']}"
                                            ),
                                        ],
                                    ),
                                ),
                                event_key=(
                                    f"execution:{execution_id}:existing_be_recovery_blocked"
                                ),
                                dedup_key_override=(
                                    "existing_be_recovery_blocked:"
                                    f"{execution_id}:{reason_code}:{topology_fingerprint}"
                                ),
                            )
                        await _invalidate_pre_reads(
                            adapter, user_id, exchange, symbol
                        )
                        moved += 1
                        continue

                    except BeCheckpointedStopVisibilityPending as be_exc:
                        replacement_snapshot = (
                            be_state.get("replacement_stop")
                            if isinstance(be_state.get("replacement_stop"), dict)
                            else confirmed_replacement_stop
                        )
                        ownership_patch = _be_ownership_patch(
                            claimed_limit_attached_stop,
                            replacement_snapshot if isinstance(replacement_snapshot, dict) else None,
                            replacement_in_progress=False,
                        )
                        ownership_be = ownership_patch.pop("be", {})
                        saved_ids = sorted(be_exc.stop_ids)
                        saved_id = saved_ids[0] if saved_ids else ""
                        be_patch = {
                            **ownership_patch,
                            "be": {
                                **ownership_be,
                                "moved": True,
                                "manual_required": False,
                                "manual_requested": False,
                                "waiting_market_safe": False,
                                "waiting_retry": False,
                                "waiting_reason": None,
                                "waiting_since": None,
                                "waiting_last_checked_at": None,
                                "waiting_next_retry_after": None,
                                "waiting_backoff_sec": None,
                                "waiting_rate_limited": False,
                                "waiting_trigger_tp_index": None,
                                "waiting_trigger_ordinal": None,
                                "waiting_old_stop_ids": [],
                                "waiting_target_stop": None,
                                "waiting_qty": None,
                                "waiting_market_safety": None,
                                "error": None,
                                "source": ("manual" if manual_requested else "automatic"),
                                "trigger_tp_index": trigger_plan_index,
                                "trigger_ordinal": trigger_idx,
                                "stop": price,
                                "qty": stop_qty,
                                "position_qty": qty_now,
                                "original_qty": original_qty,
                                "basis_entry": actual_entry,
                                "signal_entry": signal_entry,
                                "actions": be_actions,
                                "replacement_stop_id": saved_id,
                                "verify_matching_stop_order_id": saved_id,
                                "replacement_visibility_pending": True,
                                "visibility_manual_review": True,
                                "visibility_warning": (
                                    "checkpointed BE STOP was not visible in the current "
                                    "BingX openOrders read; duplicate STOP creation was blocked"
                                ),
                                **(
                                    {
                                        "manual_result": {
                                            "state": "moved_visibility_review",
                                            "reason": (
                                                "Б/У STOP уже был подтверждён чекпоинтом, "
                                                "но текущий read-back BingX его не показал. "
                                                "Дубликат STOP не создан; проверьте STOP/TP вручную."
                                            ),
                                            "stop": price,
                                            "market_price": current_price,
                                        }
                                    }
                                    if manual_requested
                                    else {}
                                ),
                            },
                            "tp": _merge_recreated_tp_orders(
                                ids_payload, remaining_tp_orders
                            ),
                            "actual_entry": actual_entry,
                        }
                        next_status = "protected" if status == "manual_required" else status
                        await _write_status(
                            next_status,
                            (
                                "manual BE checkpointed STOP visibility pending; duplicate STOP creation blocked"
                                if manual_requested
                                else f"BE checkpointed STOP visibility pending after TP{trigger_plan_index}; duplicate STOP creation blocked"
                            ),
                            be_patch,
                        )
                        await _notify(
                            notify,
                            user_id,
                            card(
                                "🟡 <b>Б/У ТРЕБУЕТ ПРОВЕРКИ</b>",
                                symbol=symbol,
                                side=side,
                                blocks=(
                                    [
                                        (
                                            "🔒 <b>Условие:</b> принудительный Б/У"
                                            if manual_requested
                                            else f"🎯 <b>Сработал:</b> TP{trigger_plan_index}"
                                        ),
                                        f"🛡 <b>Расчётный STOP Б/У:</b> {fmt_price(price)}",
                                        f"📦 <b>Остаток позиции:</b> {fmt_qty(qty_now)}",
                                        *(
                                            [f"🆔 <b>STOP ID:</b> {saved_id}"]
                                            if saved_id
                                            else []
                                        ),
                                    ],
                                    [
                                        "✅ Новый STOP был ранее подтверждён чекпоинтом",
                                        "⚠️ Но текущий read-back BingX не показал этот STOP",
                                        "🚫 Дубликат STOP не создан",
                                        "📱 Проверьте STOP и TP на BingX вручную",
                                    ],
                                ),
                            ),
                        )
                        await _invalidate_pre_reads(adapter, user_id, exchange, symbol)
                        moved += 1
                        continue
                    except Exception as be_exc:
                        rate_limit_retry_at = _bingx_trigger_rate_limit_retry_after(be_exc)
                        if rate_limit_retry_at is not None:
                            write_intent_for_rate_limit = be_state.get(
                                "replacement_write_intent_v1"
                            )
                            if isinstance(write_intent_for_rate_limit, dict):
                                old_stop_ids_for_rate_limit = sorted(
                                    {
                                        clean_exchange_id(value)
                                        for value in (
                                            live_owned_stop_ids
                                            or live_owned_stop_ids_for_coverage
                                            or []
                                        )
                                        if clean_exchange_id(value)
                                    }
                                )
                                old_stop_exactly_proven = bool(
                                    old_stop_ids_for_rate_limit
                                )
                                rate_limit_write_intent = {
                                    **write_intent_for_rate_limit,
                                    # The generic pre-write topology may contain
                                    # TP identities as well as STOP identities.
                                    # Recovery cleanup must be scoped to the exact
                                    # bot-owned STOP set observed in this pass.
                                    "old_stop_ids": old_stop_ids_for_rate_limit,
                                    "rate_limit_old_stop_scope_exact_v1_0_7g7h2f5g5b3d": True,
                                }
                                now = _utc_now()
                                now_iso = _iso_utc(now)
                                retry_after_iso = _iso_utc(rate_limit_retry_at)
                                rate_limit_details = _exchange_error_details(be_exc)
                                confirmation_marker = (
                                    _rate_limit_write_confirmation_marker(
                                        error_details=rate_limit_details,
                                        old_stop_ids=old_stop_ids_for_rate_limit,
                                        exchange_retry_after=rate_limit_retry_at,
                                        now=now,
                                    )
                                )
                                confirmation_retry_iso = str(
                                    confirmation_marker["next_readback_after"]
                                )
                                ownership_patch = _be_ownership_patch(
                                    claimed_limit_attached_stop,
                                    confirmed_replacement_stop,
                                    replacement_in_progress=True,
                                )
                                ownership_be = ownership_patch.pop("be", {})
                                pending_manual_required = not old_stop_exactly_proven
                                pending_status = (
                                    "manual_required"
                                    if pending_manual_required
                                    else status
                                )
                                pending_reason = (
                                    "BE STOP write confirmation pending after BingX "
                                    "trigger rate limit; protection is not exactly proven"
                                    if pending_manual_required
                                    else "BE STOP write confirmation pending after BingX "
                                    "trigger rate limit; exact old STOP remains active"
                                )
                                actions = [
                                    *be_actions,
                                    {
                                        "type": "be_trigger_rate_limited_wait_v1_6_102",
                                        "retry_after": retry_after_iso,
                                        "old_stop_ids": old_stop_ids_for_rate_limit,
                                        "error": rate_limit_details,
                                    },
                                    {
                                        "type": "be_trigger_rate_limit_write_confirmation_pending_v1_0_7g7h2f5g5b3d",
                                        "confirmation_retry_after": confirmation_retry_iso,
                                        "exchange_retry_after": retry_after_iso,
                                        "old_stop_exactly_proven": old_stop_exactly_proven,
                                        "exchange_writes_after_error": 0,
                                    },
                                ]
                                be_patch = {
                                    **ownership_patch,
                                    "be": {
                                        **ownership_be,
                                        "moved": False,
                                        "manual_required": pending_manual_required,
                                        "manual_requested": (
                                            False
                                            if manual_requested
                                            else be_state.get("manual_requested")
                                        ),
                                        # Keep the durable pre-write baseline. A
                                        # 100410 response can arrive after BingX
                                        # accepted the STOP, so a second write is
                                        # forbidden until fresh exact read-back.
                                        "replacement_in_progress": True,
                                        "replacement_write_intent_v1": dict(
                                            rate_limit_write_intent
                                        ),
                                        "rate_limit_write_confirmation_v1": confirmation_marker,
                                        "existing_be_recovery_last_checked_at": now_iso,
                                        "existing_be_recovery_next_retry_after": confirmation_retry_iso,
                                        "waiting_rate_limited": True,
                                        "waiting_retry": True,
                                        "waiting_reason": "bingx_trigger_rate_limited_write_confirmation_pending",
                                        "waiting_since": str(
                                            be_state.get("waiting_since") or now_iso
                                        ),
                                        "waiting_last_checked_at": now_iso,
                                        # This timestamp controls when a new write
                                        # may be attempted after an exact-old-STOP
                                        # fallback clears the ambiguous checkpoint.
                                        "waiting_next_retry_after": retry_after_iso,
                                        "waiting_backoff_sec": max(
                                            1.0,
                                            (rate_limit_retry_at - now).total_seconds(),
                                        ),
                                        "waiting_trigger_tp_index": trigger_plan_index,
                                        "waiting_trigger_ordinal": trigger_idx,
                                        "waiting_old_stop_ids": old_stop_ids_for_rate_limit,
                                        "waiting_target_stop": price,
                                        "waiting_qty": stop_qty,
                                        "waiting_exchange_error": rate_limit_details,
                                        "source": (
                                            "manual" if manual_requested else "automatic"
                                        ),
                                        "trigger_tp_index": trigger_plan_index,
                                        "trigger_ordinal": trigger_idx,
                                        "stop": price,
                                        "qty": stop_qty,
                                        "position_qty": qty_now,
                                        "original_qty": original_qty,
                                        "basis_entry": actual_entry,
                                        "signal_entry": signal_entry,
                                        "tp_price_seen": price_seen,
                                        "tp_price_seen_at": current_price,
                                        "tp_target": trigger_target,
                                        "calculation": be_calc,
                                        "actions": actions,
                                        **_be_waiting_tp_bypass_marker(
                                            be_bypass_decision,
                                            position_qty=qty_now,
                                        ),
                                        "error": (
                                            None
                                            if old_stop_exactly_proven
                                            else f"{type(be_exc).__name__}: {be_exc}"
                                        ),
                                        **(
                                            {
                                                "manual_result": {
                                                    "state": "rate_limit_confirmation_pending",
                                                    "reason": (
                                                        "BingX ограничил STOP/trigger endpoint. "
                                                        "Выполняется отложенная точная "
                                                        "проверка уже отправленного STOP; "
                                                        "повторный STOP не создаётся."
                                                    ),
                                                    "stop": price,
                                                    "market_price": current_price,
                                                }
                                            }
                                            if manual_requested
                                            else {}
                                        ),
                                    },
                                    "actual_entry": actual_entry,
                                }
                                await _write_status(
                                    pending_status,
                                    pending_reason,
                                    be_patch,
                                )
                                notification_lines = [
                                    "⏳ BingX временно ограничил STOP/trigger endpoint",
                                    "🔎 Бот выполнит отложенный fresh read-back уже отправленного STOP",
                                    "🚫 Повторный STOP до точного подтверждения не создаётся",
                                ]
                                if old_stop_exactly_proven:
                                    notification_lines.insert(
                                        1,
                                        "✅ Точный старый STOP подтверждён и остаётся активным",
                                    )
                                else:
                                    notification_lines.insert(
                                        1,
                                        "⚠️ Активная защита не доказана по exact identity — требуется ручная проверка",
                                    )
                                details = [
                                    f"🔎 <b>Первая перепроверка:</b> {confirmation_retry_iso}",
                                    f"🕒 <b>Новая запись не раньше:</b> {retry_after_iso}",
                                    details_line(
                                        f"{type(be_exc).__name__}: {str(be_exc)[:300]}"
                                    ),
                                ]
                                if old_stop_ids_for_rate_limit:
                                    details.insert(
                                        0,
                                        f"🧷 <b>Старый STOP:</b> {', '.join(old_stop_ids_for_rate_limit[:3])}",
                                    )
                                await _notify(
                                    notify,
                                    user_id,
                                    card(
                                        (
                                            "⏳ <b>Б/У: ПРОВЕРКА STOP</b>"
                                            if old_stop_exactly_proven
                                            else "🟡 <b>Б/У ТРЕБУЕТ ПРОВЕРКИ</b>"
                                        ),
                                        symbol=symbol,
                                        side=side,
                                        blocks=(
                                            [
                                                (
                                                    "🔒 <b>Условие:</b> принудительный Б/У"
                                                    if manual_requested
                                                    else f"🎯 <b>Сработал:</b> TP{trigger_plan_index}"
                                                ),
                                                f"🛡 <b>Расчётный новый STOP:</b> {fmt_price(price)}",
                                                f"📦 <b>Остаток позиции:</b> {fmt_qty(qty_now)}",
                                            ],
                                            notification_lines,
                                            details,
                                        ),
                                    ),
                                    event_key=f"execution:{execution_id}:be_trigger_rate_limit_confirmation_pending",
                                    dedup_key_override=(
                                        f"be_trigger_rate_limit_confirmation_pending:{execution_id}"
                                    ),
                                )
                                await _invalidate_pre_reads(
                                    adapter, user_id, exchange, symbol
                                )
                                moved += 1
                                continue

                        ownership_patch = _be_ownership_patch(
                            claimed_limit_attached_stop,
                            confirmed_replacement_stop,
                            replacement_in_progress=False,
                        )
                        ownership_be = ownership_patch.pop("be", {})
                        be_patch = {
                            **ownership_patch,
                            "be": {
                                **ownership_be,
                                "moved": False,
                                "manual_required": True,
                                "manual_requested": (
                                    False
                                    if manual_requested
                                    else be_state.get("manual_requested")
                                ),
                                "source": (
                                    "manual" if manual_requested else "automatic"
                                ),
                                "trigger_tp_index": trigger_plan_index,
                                "trigger_ordinal": trigger_idx,
                                "stop": price,
                                "qty": stop_qty,
                                "position_qty": qty_now,
                                "original_qty": original_qty,
                                "basis_entry": actual_entry,
                                "signal_entry": signal_entry,
                                "tp_price_seen": price_seen,
                                "tp_price_seen_at": current_price,
                                "tp_target": trigger_target,
                                "calculation": be_calc,
                                "actions": be_actions,
                                "remaining_tp_recreated": remaining_tp_orders,
                                "error": f"{type(be_exc).__name__}: {be_exc}",
                                "existing_be_recovery_owned_replacement_ids": sorted(
                                    _durable_exact_be_replacement_ids(
                                        {
                                            **be_state,
                                            "actions": [
                                                *(
                                                    be_state.get("actions")
                                                    if isinstance(be_state.get("actions"), list)
                                                    else []
                                                ),
                                                *be_actions,
                                            ],
                                        }
                                    )
                                ),
                                "existing_be_recovery_last_checked_at": (
                                    _iso_utc(_utc_now())
                                    if (
                                        recovery_be_override
                                        or _be_recovery_checkpoint_present(be_state)
                                    )
                                    else be_state.get(
                                        "existing_be_recovery_last_checked_at"
                                    )
                                ),
                                "existing_be_recovery_next_retry_after": (
                                    _be_retry_after_iso(_BE_MARKET_SAFE_RETRY_SEC)
                                    if (
                                        recovery_be_override
                                        or _be_recovery_checkpoint_present(be_state)
                                    )
                                    else be_state.get(
                                        "existing_be_recovery_next_retry_after"
                                    )
                                ),
                                **(
                                    {
                                        "manual_result": {
                                            "state": "error",
                                            "reason": f"{type(be_exc).__name__}: {str(be_exc)[:400]}",
                                            "stop": price,
                                            "market_price": current_price,
                                        }
                                    }
                                    if manual_requested
                                    else {}
                                ),
                            },
                            "tp": _merge_recreated_tp_orders(
                                ids_payload, remaining_tp_orders
                            ),
                            "actual_entry": actual_entry,
                        }
                        await _write_status(
                            "manual_required",
                            (
                                f"manual BE replace error: {type(be_exc).__name__}: {be_exc}"
                                if manual_requested
                                else f"BE replace error after TP{trigger_plan_index}: {type(be_exc).__name__}: {be_exc}"
                            ),
                            be_patch,
                        )
                        await _notify(
                            notify,
                            user_id,
                            card(
                                "🔴 <b>ОШИБКА ПЕРЕНОСА В Б/У</b>",
                                symbol=symbol,
                                side=side,
                                blocks=(
                                    [
                                        (
                                            "🔒 <b>Условие:</b> принудительный Б/У"
                                            if manual_requested
                                            else f"🎯 <b>Сработал:</b> TP{trigger_plan_index}"
                                        ),
                                        f"🛡 <b>Расчётный новый STOP:</b> {fmt_price(price)}",
                                        f"📦 <b>Остаток позиции:</b> {fmt_qty(qty_now)}",
                                    ],
                                    [
                                        "❌ Замена STOP завершилась ошибкой",
                                        "📱 Проверьте позицию, STOP и TP вручную",
                                    ],
                                    [
                                        details_line(
                                            f"{type(be_exc).__name__}: {str(be_exc)[:400]}"
                                        )
                                    ],
                                ),
                            ),
                        )
                        await _invalidate_pre_reads(adapter, user_id, exchange, symbol)
                        moved += 1
                        continue

                    if isinstance(matching_be_stop, dict):
                        confirmed_replacement_stop = dict(matching_be_stop)
                    ownership_patch = _be_ownership_patch(
                        claimed_limit_attached_stop,
                        confirmed_replacement_stop,
                        replacement_in_progress=False,
                        clear_cleanup_intent=True,
                    )
                    ownership_be = ownership_patch.pop("be", {})
                    be_confirmed_dt = _utc_now()
                    trigger_fill_dt: datetime | None = None
                    for tp_position, tp_item in enumerate(updated_tp_rows, 1):
                        if not isinstance(tp_item, dict) or tp_item.get("filled") is not True:
                            continue
                        try:
                            tp_index_value = int(tp_item.get("tp_index") or tp_position)
                        except (TypeError, ValueError, OverflowError):
                            continue
                        if tp_index_value != int(trigger_plan_index or trigger_idx or 0):
                            continue
                        trigger_fill_dt = _parse_iso_utc(tp_item.get("filled_at"))
                        if trigger_fill_dt is not None:
                            break
                    fill_to_be_latency_sec: float | None = None
                    if trigger_fill_dt is not None:
                        fill_to_be_latency_sec = max(
                            0.0, (be_confirmed_dt - trigger_fill_dt).total_seconds()
                        )

                    be_patch = {
                        **ownership_patch,
                        "be": {
                            **ownership_be,
                            "moved": True,
                            "manual_requested": False,
                            "manual_required": False,
                            "waiting_market_safe": False,
                            "waiting_retry": False,
                            "waiting_reason": None,
                            "waiting_since": None,
                            "waiting_last_checked_at": None,
                            "waiting_next_retry_after": None,
                            "waiting_backoff_sec": None,
                            "waiting_rate_limited": False,
                            "waiting_trigger_tp_index": None,
                            "waiting_trigger_ordinal": None,
                            "waiting_old_stop_ids": [],
                            "waiting_target_stop": None,
                            "waiting_qty": None,
                            "waiting_market_safety": None,
                            "existing_be_recovery_last_checked_at": (
                                _iso_utc(_utc_now())
                                if recovery_be_override
                                else be_state.get(
                                    "existing_be_recovery_last_checked_at"
                                )
                            ),
                            "existing_be_recovery_next_retry_after": None,
                            "rate_limit_write_confirmation_v1": None,
                            "existing_be_recovery_owned_replacement_ids": None,
                            "existing_be_recovery_blocked_v1": None,
                            "existing_be_recovery_last_notified_reason": None,
                            "existing_be_recovery_last_notified_topology_fingerprint": None,
                            "existing_be_recovery_last_notified_at": None,
                            "existing_be_recovery_tp_fingerprint_before": (
                                recovery_tp_before_fingerprint
                                if recovery_be_override
                                else None
                            ),
                            "existing_be_recovery_tp_fingerprint_after": (
                                replace_diagnostics.get("tp_fingerprint_actual")
                                if recovery_be_override
                                and isinstance(replace_diagnostics, dict)
                                else None
                            ),
                            "existing_be_recovery_tp_unchanged": (
                                bool(replace_diagnostics.get("tp_unchanged"))
                                if recovery_be_override
                                and isinstance(replace_diagnostics, dict)
                                else None
                            ),
                            "skipped": None,
                            "error": None,
                            "source": ("manual" if manual_requested else "automatic"),
                            "confirmed_at": _iso_utc(be_confirmed_dt),
                            "trigger_fill_at": (
                                _iso_utc(trigger_fill_dt)
                                if trigger_fill_dt is not None
                                else None
                            ),
                            "fill_to_be_confirm_latency_sec": fill_to_be_latency_sec,
                            "manual_confirmed_at": (
                                datetime.now(timezone.utc).isoformat()
                                if manual_requested
                                else None
                            ),
                            "manual_result": (
                                {
                                    "state": (
                                        "moved_tp_recreation_warning"
                                        if tp_recreation_failed
                                        else "moved"
                                    ),
                                    "reason": (
                                        "Позиция переведена в Б/У, но оставшиеся TP требуют ручной проверки."
                                        if tp_recreation_failed
                                        else "Позиция принудительно переведена в Б/У."
                                    ),
                                    "stop": price,
                                    "market_price": current_price,
                                    "tp_recreation_error": (
                                        tp_recreation_failure_details
                                        if tp_recreation_failed
                                        else None
                                    ),
                                }
                                if manual_requested
                                else None
                            ),
                            "trigger_tp_index": trigger_plan_index,
                            "trigger_ordinal": trigger_idx,
                            "stop": price,
                            "qty": qty_now,
                            "original_qty": original_qty,
                            "basis_entry": actual_entry,
                            "signal_entry": signal_entry,
                            "client_id": client_id,
                            "tp_price_seen": price_seen,
                            "tp_price_seen_at": current_price,
                            "tp_target": trigger_target,
                            "calculation": be_calc,
                            "actions": be_actions,
                            "remaining_tp_recreated": remaining_tp_orders,
                            "tp_recreation_failed": tp_recreation_failed,
                            "tp_recreation_error": (
                                tp_recreation_failure_details
                                if tp_recreation_failed
                                else None
                            ),
                            "tp_recreation_manual_review": tp_recreation_failed,
                            "verify_stop_like_count": stop_like_count,
                            "verify_algo_order_count": _algo_order_count(
                                relevant_algo_orders
                            ),
                            "verify_matching_stop_order_id": (
                                _algo_order_id(matching_be_stop)
                                if isinstance(matching_be_stop, dict)
                                else ""
                            ),
                            "untracked_stop_warning": bool(
                                preserved_untracked_stop_ids
                            ),
                            "untracked_stop_order_ids": (preserved_untracked_stop_ids),
                        },
                        "tp": updated_tp_rows,
                        "actual_entry": actual_entry,
                    }
                    success_status = (
                        "protected" if status == "manual_required" else status
                    )
                    await _write_status(
                        success_status,
                        (
                            "STOP safely replaced to BE by manual user request; remaining TP recreation requires manual review"
                            if manual_requested and tp_recreation_failed
                            else (
                                f"STOP safely replaced to BE after TP{trigger_plan_index}; remaining TP recreation requires manual review"
                                if tp_recreation_failed
                                else (
                                    "STOP safely replaced to BE by manual user request"
                                    if manual_requested
                                    else f"STOP safely replaced to BE after TP{trigger_plan_index}"
                                )
                            )
                        ),
                        be_patch,
                    )
                    if (
                        not manual_requested
                        and fill_to_be_latency_sec is not None
                        and fill_to_be_latency_sec > _TP_TO_BE_SLA_WARN_SEC
                    ):
                        log.error(
                            "TP_TO_BE_SLA_BREACH execution_id=%s user_id=%s symbol=%s "
                            "tp=%s latency_sec=%.3f threshold_sec=%.1f "
                            "tp_filled_at=%s be_confirmed_at=%s",
                            int(row.get("id") or 0),
                            user_id,
                            symbol,
                            int(trigger_plan_index or trigger_idx or 0),
                            fill_to_be_latency_sec,
                            _TP_TO_BE_SLA_WARN_SEC,
                            _iso_utc(trigger_fill_dt),
                            _iso_utc(be_confirmed_dt),
                        )
                        if notify:
                            alert = (
                                "🚨 ЗАДЕРЖКА TP → Б/У ВЫШЕ SLA\n"
                                f"Execution: {int(row.get('id') or 0)}\n"
                                f"Пользователь: {user_id}\n"
                                f"Пара: {symbol} {side.upper()}\n"
                                f"TP: {int(trigger_plan_index or trigger_idx or 0)}\n"
                                f"Задержка: {fill_to_be_latency_sec:.1f} сек\n"
                                f"Лимит: {_TP_TO_BE_SLA_WARN_SEC:.0f} сек"
                            )
                            for admin_id in sorted(
                                {
                                    int(value)
                                    for value in getattr(get_settings(), "admin_ids", [])
                                    if int(value) > 0
                                }
                            ):
                                await _notify(notify, admin_id, alert)
                    # Use rich BE notification with realized PnL.
                    try:
                        from app.services.trade_notifications import be_set_message

                        # Compute realized PnL from every TP already confirmed
                        # filled. A fast price jump may complete more than the
                        # configured BE trigger before replacement starts.
                        realized_pnl = 0.0
                        _tp_list2 = ids_payload.get("tp") or []
                        filled_count = 0
                        for _tp_item in _tp_list2:
                            if not isinstance(_tp_item, dict):
                                continue
                            if _tp_item.get("filled") is not True:
                                continue
                            _tp_q = float(_tp_item.get("qty") or 0.0)
                            _tp_p = float(
                                _tp_item.get("target") or _tp_item.get("price") or 0.0
                            )
                            if _tp_q > 0 and _tp_p > 0 and actual_entry > 0:
                                filled_count += 1
                                if side.lower() == "long":
                                    realized_pnl += (_tp_p - actual_entry) * _tp_q
                                else:
                                    realized_pnl += (actual_entry - _tp_p) * _tp_q
                        if manual_requested:
                            msg = card(
                                "🔒 <b>ПОЗИЦИЯ ПРИНУДИТЕЛЬНО ПЕРЕВЕДЕНА В Б/У</b>",
                                symbol=symbol,
                                side=side,
                                blocks=(
                                    [
                                        f"💵 <b>Фактический вход:</b> {fmt_price(actual_entry)}",
                                        f"🔒 <b>Новый STOP:</b> {fmt_price(price)}",
                                        f"📦 <b>Остаток позиции:</b> {fmt_qty(qty_now)}",
                                    ],
                                    [
                                        "✅ Новый STOP подтверждён",
                                        "✅ Автоматический Б/У повторно не сработает",
                                        *(
                                            [
                                                "⚠️ Оставшиеся TP не пересозданы полностью",
                                                "📱 Проверьте TP на BingX вручную",
                                                details_line(tp_recreation_failure_details),
                                            ]
                                            if tp_recreation_failed
                                            else []
                                        ),
                                        *(
                                            [
                                                "⚠️ На позиции сохранён дополнительный неотслеживаемый STOP",
                                                "📱 Проверьте старый/ручной STOP на BingX вручную",
                                            ]
                                            if preserved_untracked_stop_ids
                                            else []
                                        ),
                                    ],
                                ),
                            )
                        else:
                            msg = be_set_message(
                                symbol=symbol,
                                side=side,
                                be_price=price,
                                entry=actual_entry,
                                realized_pnl_usdt=realized_pnl,
                                tp_count_done=max(trigger_idx, filled_count),
                                old_stop=_f(row.get("stop"), 0.0),
                                remaining_qty=qty_now,
                                tp_filled_at=trigger_fill_dt,
                                be_confirmed_at=be_confirmed_dt,
                            )
                            if tp_recreation_failed:
                                msg += (
                                    "\n\n⚠️ <b>Внимание:</b> Б/У STOP подтверждён, "
                                    "но оставшиеся TP не пересозданы полностью. "
                                    "Проверьте TP на BingX вручную.\n"
                                    f"Детали: {tp_recreation_failure_details[:500]}"
                                )
                            if preserved_untracked_stop_ids:
                                msg += (
                                    "\n\n⚠️ <b>Внимание:</b> на позиции сохранён "
                                    "дополнительный неотслеживаемый STOP. "
                                    "Проверьте старый/ручной STOP на BingX вручную."
                                )
                        await _notify(notify, user_id, msg)
                    except Exception as _be_notify_exc:
                        log.warning("rich BE notification failed: %s", _be_notify_exc)
                        await _notify(
                            notify,
                            user_id,
                            f"🛡 STOP в безубыток\n{symbol} {side.upper()}\n"
                            f"Новый STOP: {price:.10g}",
                        )
                    await _invalidate_pre_reads(adapter, user_id, exchange, symbol)
                    moved += 1
            return moved
        except StaleExecutionPass as stale:
            log.info(
                "%s: stale monitor batch stopped safely execution_id=%s expected=%s attempted=%s",
                stale.source,
                stale.execution_id,
                stale.expected_status,
                stale.attempted_status,
            )
            return moved
        finally:
            set_notification_event_key("")
            context_stats = pre_read_context.stats()
            context_mode = (
                "background"
                if rows_override is None
                else ("tp_event" if event_level_index is not None else "override")
            )
            log.info(
                "BE_PRE_READ_CONTEXT mode=%s rows=%s %s",
                context_mode,
                len(rows),
                " ".join(f"{key}={value}" for key, value in context_stats.items()),
            )
            if owns_adapter_cache:
                for adapter in adapter_cache.values():
                    try:
                        await adapter.close()
                    except Exception:
                        pass


async def be_monitor_loop(notify: NotifyFn | None = None) -> None:
    settings = get_settings()
    interval = max(5, int(getattr(settings, "MONITOR_ACTIVE_INTERVAL_SEC", 15) or 15))
    while True:
        try:
            await process_be_monitor_once(notify=notify)
        except Exception:
            log.exception("be_monitor_loop iteration failed")
        await asyncio.sleep(interval)
