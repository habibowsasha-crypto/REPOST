from __future__ import annotations

import math
import asyncio
import inspect
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

from app.config import get_settings
from app.services.monitor_diagnostics import record_counter, record_stage_rows
from app.services.ttl_cache import get_api_key_cache, get_user_settings_cache
from app.database import db
from app.services.exchange_factory import build_adapter
from app.services.async_utils import StaleExecutionPass, null_async_context
from app.database.db import symbol_action_lock
from app.services.notification_style import (
    ensure_visual_card,
    card,
    details_line,
    fmt_qty,
    tree_lines,
)
from app.services.trade_notifications import tp_filled_message
from app.services.durable_notifications import (
    send_or_enqueue,
    set_notification_event_key,
)
from app.services.stop_ownership import LIMIT_ATTACHED_STOP_KEY, clean_exchange_id
from app.services.tp_execution_ledger import (
    canonicalize_tp_ledger,
    tp_ledger_repair_metadata,
    tp_row_has_unresolved_identity_conflict,
    tp_row_has_unresolved_qty_conflict,
)
from app.services.tp_plan_snapshot import get_snapshot
from app.services.write_flow_audit import build_write_flow_audit
from app.services.full_reconcile_locality import account_local_full_pass_rows
from app.services.financial_reconciliation_enqueue import (
    MARKER_KEY as FINANCIAL_RECONCILIATION_MARKER_KEY,
    enqueue_prepared_financial_reconciliation,
    prepare_financial_reconciliation_marker,
)

log = logging.getLogger(__name__)
NotifyFn = Callable[[int, str], Awaitable[object] | object]
_LOOP_LOCK = asyncio.Lock()
_SCAN_CURSOR = 0
_POSITION_ZERO_EPSILON = 1e-12
_UNRESOLVED_PARTIAL_TP_LOG_STATE: dict[int, tuple[str, float]] = {}
_UNRESOLVED_PARTIAL_TP_LOG_INTERVAL_SEC = 900.0
_UNRESOLVED_PARTIAL_TP_LOG_MAX = 2048


def _symbol_key(value: Any) -> str:
    return str(value or "").strip().upper().replace("-", "").replace("_", "")


def _f(value: Any, default: float = 0.0) -> float:
    """Parse a finite non-negative exchange scalar without repairing corruption."""
    try:
        if value in (None, "") or isinstance(value, bool):
            return default
        parsed = float(value)
        return parsed if math.isfinite(parsed) and parsed >= 0 else default
    except (TypeError, ValueError, OverflowError):
        return default


def _confirmed_tp_qty_from_payload(
    payload: dict[str, Any],
) -> tuple[list[dict[str, Any]], float]:
    """Return the current canonical TP rows and their confirmed filled quantity.

    The TP synchronizer is allowed to replace ``payload["tp"]`` with freshly
    canonicalized dictionaries.  Callers must therefore derive progress from
    the payload *after* synchronization rather than from an older list alias.
    """

    raw_rows = payload.get("tp") if isinstance(payload, dict) else None
    rows = (
        [item for item in raw_rows if isinstance(item, dict)]
        if isinstance(raw_rows, list)
        else []
    )
    # Reuse the canonical fill-quantity rule.  ``exchange_filled_qty`` is the
    # authoritative amount when BingX reports it and can differ from the planned
    # TP quantity because of contract rounding or a partial terminal fill.
    return rows, _confirmed_tp_qty(rows)


def _position_qty_is_live(value: Any) -> bool:
    """True for every real positive exchange quantity, including one qty_step.

    The old 0.2% tolerance remains useful for TP-progress inference, but it must
    never decide whether a position is closed.  For 411.4 ARB it incorrectly
    treated a live 0.1 ARB lot as zero.
    """
    return _f(value, 0.0) > _POSITION_ZERO_EPSILON


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


def _position_side(pos: dict[str, Any]) -> str:
    return str(
        pos.get("side") or pos.get("positionSide") or pos.get("holdSide") or ""
    ).lower()


def _filter_positions_by_side(
    positions: list[dict[str, Any]], side: str
) -> list[dict[str, Any]]:
    side = (side or "").lower()
    out: list[dict[str, Any]] = []
    for pos in positions or []:
        ps = _position_side(pos)
        if not ps or ps == side:
            out.append(pos)
    return out


def _filter_positions_by_symbol(
    positions: list[dict[str, Any]], symbol: str
) -> list[dict[str, Any]]:
    """Keep only the requested contract, while failing closed on missing symbols."""

    wanted = _symbol_key(symbol)
    if not wanted:
        return list(positions or [])
    filtered: list[dict[str, Any]] = []
    for pos in positions or []:
        raw_symbol = str(pos.get("symbol") or "").strip()
        if not raw_symbol:
            # A legacy/test adapter may already return a symbol-scoped row with
            # no symbol field. Keeping it is conservative: it can only defer
            # cleanup, never authorize a false zero.
            filtered.append(pos)
            continue
        normalized = _symbol_key(raw_symbol)
        if normalized == wanted:
            filtered.append(pos)
    return filtered


def _total_position_size(positions: list[dict[str, Any]]) -> float:
    return sum(_position_size(p) for p in positions or [])


def _delivery_confirmed(value: Any) -> bool:
    """Return True only when the notification layer explicitly confirms delivery."""
    if isinstance(value, bool):
        return value
    delivered = getattr(value, "delivered", None)
    if delivered is not None:
        return bool(delivered)
    if isinstance(value, dict) and "delivered" in value:
        return bool(value.get("delivered"))
        # ``None`` used to be treated as success, which could permanently set
        # filled_notified=True even after Telegram delivery had failed.
    return False


def _parse_utc_datetime(value: Any) -> datetime | None:
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


def _tp_notification_retry_due(
    tp: dict[str, Any], *, now: datetime | None = None
) -> bool:
    next_retry = _parse_utc_datetime(tp.get("next_notify_retry_at"))
    if next_retry is None:
        return True
    current = now or datetime.now(timezone.utc)
    return current >= next_retry


def _tp_notification_retry_delay(attempts: int) -> float:
    # Durable exponential backoff. This avoids hammering Telegram every 5 seconds
    # when a user has blocked the bot, while still retrying transient failures.
    normalized = max(1, int(attempts or 1))
    return float(min(300, 5 * (2 ** min(normalized - 1, 6))))


_CLOSED_HISTORY_RETRY_DELAYS_SEC = (60, 300, 900, 3600)


def _closed_history_retry_patch(
    history_reconcile: dict[str, Any],
    *,
    now: datetime,
    started_at: datetime,
    timeout_sec: int,
    pending_notification: bool,
    history_rows: int,
    resolution: str | None = None,
) -> dict[str, Any]:
    """Build one bounded durable retry schedule for a settled zero position."""

    attempts = max(0, int(history_reconcile.get("attempts") or 0)) + 1
    if pending_notification:
        delay = 60
    else:
        delay = _CLOSED_HISTORY_RETRY_DELAYS_SEC[
            min(attempts - 1, len(_CLOSED_HISTORY_RETRY_DELAYS_SEC) - 1)
        ]
    deadline = started_at + timedelta(seconds=max(0, int(timeout_sec or 0)))
    proposed = now + timedelta(seconds=delay)
    # History evidence is bounded by the close-classification deadline, but a
    # pending Telegram delivery is independent of that deadline. Keep its
    # durable 60-second cadence even after history reconciliation timed out.
    if pending_notification:
        next_attempt = proposed
    else:
        next_attempt = min(proposed, deadline) if deadline > now else now
    patch = {
        "started_at": started_at.isoformat(),
        "attempts": attempts,
        "last_attempt_at": now.isoformat(),
        "last_history_rows": int(history_rows),
        "deadline_seconds": int(timeout_sec),
        "next_attempt_at": next_attempt.isoformat(),
        "retry_delay_seconds": max(0, int((next_attempt - now).total_seconds())),
        "tp_notification_pending": bool(pending_notification),
    }
    if resolution:
        patch["resolution"] = resolution
    return patch


def _critical_zero_first_patch(
    payload: dict[str, Any],
    *,
    now: datetime,
    same_side_qty: float,
    any_position_qty: float,
) -> dict[str, Any]:
    previous = (
        payload.get("critical_zero_exposure_v1")
        if isinstance(payload.get("critical_zero_exposure_v1"), dict)
        else {}
    )
    first_at = _parse_utc_datetime(previous.get("first_zero_at")) or now
    return {
        "version": 1,
        "confirmed": False,
        "confirmations": 1,
        "first_zero_at": first_at.isoformat(),
        "second_zero_at": None,
        "second_read_forced": False,
        "same_side_position_qty": float(same_side_qty),
        "any_position_qty": float(any_position_qty),
        "last_checked_at": now.isoformat(),
        "invalidated_at": None,
        "invalidated_reason": None,
        "source": "position_lifecycle_guard",
    }


def _critical_zero_confirmed_patch(
    payload: dict[str, Any],
    *,
    now: datetime,
    zero_seen_at: datetime,
) -> dict[str, Any]:
    previous = (
        payload.get("critical_zero_exposure_v1")
        if isinstance(payload.get("critical_zero_exposure_v1"), dict)
        else {}
    )
    first_at = _parse_utc_datetime(previous.get("first_zero_at")) or zero_seen_at
    if first_at >= now:
        first_at = (
            zero_seen_at if zero_seen_at < now else now - timedelta(microseconds=1)
        )
    return {
        "version": 1,
        "confirmed": True,
        "confirmations": max(2, int(previous.get("confirmations") or 0) + 1),
        "first_zero_at": first_at.isoformat(),
        "second_zero_at": now.isoformat(),
        "second_read_forced": True,
        "same_side_position_qty": 0.0,
        "any_position_qty": 0.0,
        "last_checked_at": now.isoformat(),
        "invalidated_at": None,
        "invalidated_reason": None,
        "source": "position_lifecycle_guard",
    }


def _critical_zero_invalidated_patch(
    *, now: datetime, same_side_qty: float, any_position_qty: float, reason: str
) -> dict[str, Any]:
    return {
        "version": 1,
        "confirmed": False,
        "confirmations": 0,
        "first_zero_at": None,
        "second_zero_at": None,
        "second_read_forced": False,
        "same_side_position_qty": float(same_side_qty),
        "any_position_qty": float(any_position_qty),
        "last_checked_at": now.isoformat(),
        "invalidated_at": now.isoformat(),
        "invalidated_reason": str(reason),
        "source": "position_lifecycle_guard",
    }


def _critical_live_state_write_required(
    payload: dict[str, Any],
    *,
    same_side_qty: float,
    any_position_qty: float,
    reason: str,
) -> bool:
    """Return True only when a live-position invalidation changes durable state."""

    lifecycle = (
        payload.get("lifecycle") if isinstance(payload.get("lifecycle"), dict) else {}
    )
    proof = (
        payload.get("critical_zero_exposure_v1")
        if isinstance(payload.get("critical_zero_exposure_v1"), dict)
        else {}
    )
    expected_opposite = bool(
        _position_qty_is_live(any_position_qty)
        and not _position_qty_is_live(same_side_qty)
    )

    def same_qty(value: Any, expected: float) -> bool:
        parsed = _f(value, -1.0)
        tolerance = max(_POSITION_ZERO_EPSILON, abs(float(expected)) * 1e-12)
        return parsed >= 0.0 and abs(parsed - float(expected)) <= tolerance

    lifecycle_matches = bool(
        lifecycle.get("zero_candidate_at") in (None, "")
        and same_qty(lifecycle.get("position_qty"), same_side_qty)
        and same_qty(lifecycle.get("any_position_qty"), any_position_qty)
        and bool(lifecycle.get("opposite_or_unknown_position_detected"))
        == expected_opposite
    )
    proof_matches = bool(
        proof
        and proof.get("confirmed") is False
        and int(_f(proof.get("confirmations"), 0.0)) == 0
        and proof.get("first_zero_at") in (None, "")
        and proof.get("second_zero_at") in (None, "")
        and proof.get("second_read_forced") is False
        and proof.get("invalidated_at") not in (None, "")
        and str(proof.get("invalidated_reason") or "") == str(reason)
        and same_qty(proof.get("same_side_position_qty"), same_side_qty)
        and same_qty(proof.get("any_position_qty"), any_position_qty)
    )
    return not (lifecycle_matches and proof_matches)


def _manual_cleanup_reconcile_marker(
    payload: dict[str, Any],
    *,
    now: datetime,
    state: str,
    cleanup: dict[str, Any] | None = None,
    reason: str = "",
    next_attempt_at: datetime | None = None,
) -> dict[str, Any]:
    previous = (
        payload.get("critical_cleanup_reconcile_v1")
        if isinstance(payload.get("critical_cleanup_reconcile_v1"), dict)
        else {}
    )
    previous_state = str(previous.get("state") or "").strip().lower()
    try:
        previous_retry_attempts = max(
            0,
            int(
                previous.get("retry_attempts")
                if previous.get("retry_attempts") is not None
                else previous.get("attempts") or 0
            ),
        )
    except (TypeError, ValueError, OverflowError):
        previous_retry_attempts = 0
    normalized_state = str(state or "pending").strip().lower()
    retry_attempts = (
        previous_retry_attempts + 1
        if normalized_state == "pending_retry" and previous_state == "pending_retry"
        else 1
        if normalized_state == "pending_retry"
        else 0
    )
    cleanup_data = cleanup if isinstance(cleanup, dict) else {}
    return {
        "version": 1,
        "state": normalized_state,
        # ``attempts`` is kept for compatibility with the first Step 2.2 build.
        # It now counts only transient cleanup retries, not zero-proof states.
        "attempts": retry_attempts,
        "retry_attempts": retry_attempts,
        "checked_at": now.isoformat(),
        "next_attempt_at": (
            next_attempt_at.isoformat() if next_attempt_at is not None else None
        ),
        "reason": str(reason or "")[:500],
        "verified_clean": cleanup_data.get("verified_clean") is True,
        "identity_missing": cleanup_data.get("identity_missing") is True,
        "remaining_tracked_algo_ids": list(
            cleanup_data.get("remaining_tracked_algo_ids") or []
        ),
        "remaining_tracked_regular_ids": list(
            cleanup_data.get("remaining_tracked_regular_ids") or []
        ),
        "unidentified_relevant_algo_count": len(
            cleanup_data.get("unidentified_relevant_algo_orders") or []
        ),
        "unidentified_relevant_regular_count": len(
            cleanup_data.get("unidentified_relevant_regular_orders") or []
        ),
        "errors": [str(item)[:300] for item in cleanup_data.get("errors") or []],
    }


def _manual_cleanup_has_unknown_hazard(cleanup: dict[str, Any]) -> bool:
    return bool(
        cleanup.get("identity_missing") is True
        or cleanup.get("remaining_tracked_algo_ids")
        or cleanup.get("remaining_tracked_regular_ids")
        or cleanup.get("unidentified_relevant_algo_orders")
        or cleanup.get("unidentified_relevant_regular_orders")
    )


def _manual_cleanup_next_attempt(payload: dict[str, Any], *, now: datetime) -> datetime:
    previous = (
        payload.get("critical_cleanup_reconcile_v1")
        if isinstance(payload.get("critical_cleanup_reconcile_v1"), dict)
        else {}
    )
    previous_state = str(previous.get("state") or "").strip().lower()
    try:
        previous_retry_attempts = max(
            0,
            int(
                previous.get("retry_attempts")
                if previous.get("retry_attempts") is not None
                else previous.get("attempts") or 0
            ),
        )
    except (TypeError, ValueError, OverflowError):
        previous_retry_attempts = 0
    attempts = previous_retry_attempts + 1 if previous_state == "pending_retry" else 1
    return now + timedelta(seconds=db.critical_cleanup_retry_delay(attempts))


def _closed_history_anchor(
    row: dict[str, Any], history_reconcile: dict[str, Any]
) -> tuple[datetime | None, str]:
    """Return the best UTC anchor for a pending closed-history row.

    Fresh closures written by this worker store ``history_reconcile.started_at``
    when the position first becomes ``closed_pending_history``.  Legacy/stale rows
    can be missing that field, so we fall back to the DB timestamps to avoid
    presenting old cleanup tails as new live events after a deploy/restart.
    """

    candidates = (
        (history_reconcile.get("started_at"), "history_reconcile.started_at"),
        (row.get("updated_at"), "row.updated_at"),
        (row.get("created_at"), "row.created_at"),
    )
    for value, source in candidates:
        parsed = _parse_utc_datetime(value)
        if parsed is not None:
            return parsed, source
    return None, "unknown"


def _stale_closed_history_timeout(
    row: dict[str, Any],
    history_reconcile: dict[str, Any],
    *,
    now: datetime,
    timeout_sec: int,
    stale_silent_after_sec: int,
) -> dict[str, Any]:
    """Classify whether an unknown close should be archived silently.

    The lifecycle guard must still alert on fresh closes whose TP/STOP history
    fails to settle.  The bug this patch fixes is the opposite case: old
    ``closed_pending_history`` rows from previous deployments can time out later
    and look like a brand-new BTC/LINK close.  Fresh rows in v1.6.88 carry
    ``fresh_close_observed=True`` and are never silenced by age.
    """

    anchor, source = _closed_history_anchor(row, history_reconcile)
    age = 0.0
    if anchor is not None:
        age = max(0.0, (now - anchor.astimezone(timezone.utc)).total_seconds())

    effective_threshold = max(int(timeout_sec or 0), int(stale_silent_after_sec or 0))
    fresh_observed = history_reconcile.get("fresh_close_observed") is True
    historical_recovery = history_reconcile.get("historical_recovery") is True
    already_silenced = history_reconcile.get("stale_silenced") is True
    should_silence = bool(
        (
            historical_recovery
            or already_silenced
            or (effective_threshold > 0 and age >= effective_threshold)
        )
        and not fresh_observed
    )
    return {
        "should_silence": should_silence,
        "age_seconds": age,
        "anchor_source": source,
        "threshold_seconds": effective_threshold,
        "fresh_close_observed": fresh_observed,
        "historical_recovery": historical_recovery,
        "already_silenced": already_silenced,
    }


async def _notify(notify: NotifyFn | None, user_id: int, text: str) -> bool:
    if not notify:
        return False
    try:
        result = notify(user_id, ensure_visual_card(text))
        if hasattr(result, "__await__"):
            result = await result  # type: ignore[misc]
        delivered = _delivery_confirmed(result)
        if not delivered:
            log.warning(
                "lifecycle notification was not explicitly delivered user_id=%s result_type=%s",
                user_id,
                type(result).__name__,
            )
        return delivered
    except Exception:
        log.exception("Failed to send lifecycle notification to user_id=%s", user_id)
        return False


async def _notify_critical(
    notify: NotifyFn | None,
    user_id: int,
    text: str,
    *,
    event_key: str | None = None,
) -> bool:
    return await send_or_enqueue(
        notify,
        user_id,
        text,
        source="position_lifecycle",
        event_key=event_key,
    )


def _tp_target(tp: dict[str, Any]) -> float:
    return _f(tp.get("target") or tp.get("price"), 0.0)


def _tp_confirmed_target(tp: dict[str, Any]) -> float:
    """Prefer the exact exchange-confirmed TP trigger over signal input."""

    order = _tp_order_payload(tp)
    verification = (
        order.get("verification") if isinstance(order.get("verification"), dict) else {}
    )
    recovery = (
        order.get("identity_recovery_v1")
        if isinstance(order.get("identity_recovery_v1"), dict)
        else {}
    )
    detail = recovery.get("detail") if isinstance(recovery.get("detail"), dict) else {}
    for value in (
        order.get("_confirmed_take_profit_price"),
        verification.get("triggerPrice"),
        detail.get("triggerPrice"),
        tp.get("target"),
        tp.get("price"),
    ):
        parsed = _f(value, 0.0)
        if parsed > 0:
            return parsed
    return 0.0


def _tp_order_payload(tp: dict[str, Any]) -> dict[str, Any]:
    value = tp.get("order")
    return value if isinstance(value, dict) else {}


def _tp_stop_order_id(tp: dict[str, Any]) -> str:
    order = _tp_order_payload(tp)
    data = order.get("data")
    data_dict = data if isinstance(data, dict) else {}
    raw = order.get("raw") if isinstance(order.get("raw"), dict) else {}
    values = (
        order.get("_confirmed_stop_plan_id"),
        order.get("stopPlanOrderId"),
        order.get("stopOrderId"),
        data_dict.get("stopPlanOrderId"),
        data_dict.get("stopOrderId"),
        data_dict.get("id"),
        raw.get("stopPlanOrderId"),
        raw.get("id"),
        data if not isinstance(data, (dict, list)) else None,
    )
    for value in values:
        text = clean_exchange_id(value)
        if text:
            return text
    return ""


def _tp_position_id(tp: dict[str, Any]) -> str:
    order = _tp_order_payload(tp)
    data = order.get("data")
    data_dict = data if isinstance(data, dict) else {}
    for value in (
        order.get("_confirmed_position_id"),
        order.get("positionId"),
        data_dict.get("positionId"),
    ):
        text = clean_exchange_id(value)
        if text:
            return text
    return ""


def _clean_identity(value: Any) -> str:
    """Normalize a durable exchange identity without accepting corrupt scalars."""

    return clean_exchange_id(value)


def _ids_from_stop_payload(value: Any) -> tuple[set[str], set[str]]:
    """Collect exact stop-plan ids from one known STOP protection branch.

    BingX ``orderId``/``placeOrderId`` can refer to a delegated regular order
    rather than the plan accepted by ``stoporder/cancel``.  They are therefore
    intentionally excluded.  Missing exact identity fails closed later instead
    of risking cancellation of an unrelated order.
    """

    order_ids: set[str] = set()
    position_ids: set[str] = set()

    def add(target: set[str], item: Any) -> None:
        cleaned = _clean_identity(item)
        if cleaned:
            target.add(cleaned)

    def walk(item: Any, *, stop_branch: bool = True) -> None:
        if not isinstance(item, dict):
            return

        raw = item.get("raw") if isinstance(item.get("raw"), dict) else {}
        data = item.get("data")
        data_dict = data if isinstance(data, dict) else {}

        for candidate in (
            item.get("_confirmed_stop_plan_id"),
            item.get("stopPlanOrderId"),
            item.get("stopOrderId"),
            item.get("stop_order_id"),
            item.get("verify_matching_stop_order_id"),
            item.get("replacement_stop_id"),
            item.get("fallback_stop_order_id"),
            data_dict.get("stopPlanOrderId"),
            data_dict.get("stopOrderId"),
            data_dict.get("stop_order_id"),
            data_dict.get("id"),
            raw.get("stopPlanOrderId"),
            raw.get("stopOrderId"),
            raw.get("id"),
        ):
            add(order_ids, candidate)

        # MARKET attached/visible STOP read-back historically stored the exact
        # old STOP only as ``candidate_ids``.  This is still a known STOP
        # protection branch, so a single exact candidate is safe to carry into
        # BE ownership.  Multiple candidates remain ambiguous and are not
        # adopted automatically.
        if stop_branch:
            candidate_ids = item.get("candidate_ids")
            if isinstance(candidate_ids, list) and len(candidate_ids) == 1:
                add(order_ids, candidate_ids[0])

            # stoporder/place currently returns the new plan id as scalar ``data``.
            # This is safe only because this walker is invoked exclusively for known
            # STOP result branches, never for an arbitrary execution payload.
        if stop_branch and not isinstance(data, (dict, list)):
            add(order_ids, data)

        for candidate in (
            item.get("positionId"),
            item.get("_confirmed_position_id"),
            data_dict.get("positionId"),
            raw.get("positionId"),
        ):
            add(position_ids, candidate)

            # Recurse only through fields known to contain STOP write/verification
            # payloads.  Do not traverse action diagnostics or recreated TP rows.
        for key in (
            "order",
            "verification",
            "stop_order",
            "replacement_stop",
            "stop_verification",
            "fallback_response_v1",
            "_attached_stop",
            "_post_fill_stop",
        ):
            child = item.get(key)
            if isinstance(child, dict):
                walk(child)

    if isinstance(value, dict):
        walk(value)
    return order_ids, position_ids


def _saved_stop_identity(payload: dict[str, Any]) -> tuple[set[str], set[str]]:
    """Return ids of the current initial/BE STOP without mixing TP/entry ids."""
    order_ids: set[str] = set()
    position_ids: set[str] = set()
    entry = payload.get("entry") if isinstance(payload, dict) else None
    be = payload.get("be") if isinstance(payload, dict) else None
    branches: list[Any] = [
        payload.get("post_fill_stop") if isinstance(payload, dict) else None,
        payload.get("market_post_fill_stop_v1") if isinstance(payload, dict) else None,
        payload.get("stop_order") if isinstance(payload, dict) else None,
        payload.get(LIMIT_ATTACHED_STOP_KEY) if isinstance(payload, dict) else None,
    ]
    if isinstance(entry, dict):
        branches.extend(
            [
                entry.get("_post_fill_stop"),
                entry.get("_attached_stop"),
                entry.get("stop_order"),
            ]
        )
    if isinstance(be, dict):
        # Do not recurse through the entire BE payload: it also contains
        # ``remaining_tp_recreated`` and action diagnostics with TP order ids.
        # Mixing those ids into STOP identity would make valid STOP history
        # impossible to match.
        for key in (
            "_confirmed_stop_plan_id",
            "stopPlanOrderId",
            "verify_matching_stop_order_id",
            "stopOrderId",
            "replacement_stop_id",
        ):
            text = _clean_identity(be.get(key))
            if text:
                order_ids.add(text)
        for key in ("positionId", "_confirmed_position_id"):
            text = _clean_identity(be.get(key))
            if text:
                position_ids.add(text)
        branches.extend(
            [
                be.get("stop_order"),
                be.get("replacement_stop"),
                be.get("stop_verification"),
            ]
        )
    for branch in branches:
        found_orders, found_positions = _ids_from_stop_payload(branch)
        order_ids.update(found_orders)
        position_ids.update(found_positions)
    return order_ids, position_ids


def _history_nested_dicts(row: dict[str, Any]) -> list[dict[str, Any]]:
    nested: list[dict[str, Any]] = [row]
    for key in ("raw", "data", "order", "trigger", "detail"):
        value = row.get(key) if isinstance(row, dict) else None
        if isinstance(value, dict):
            nested.append(value)
    return nested


def _history_value(row: dict[str, Any], *keys: str) -> Any:
    for scope in _history_nested_dicts(row):
        for key in keys:
            value = scope.get(key)
            if value not in (None, ""):
                return value
    return None


def _history_row_ids(row: dict[str, Any]) -> set[str]:
    id_keys = (
        "stopOrderId",
        "stopPlanOrderId",
        "planOrderId",
        "orderId",
        "id",
        "placeOrderId",
        "delegatedOrderId",
        "childOrderId",
        "triggerOrderId",
        "algoOrderId",
        "clientOrderId",
        "clientOrderID",
        "client_order_id",
        "origClientOrderId",
        "origClientOrderID",
        "orig_client_order_id",
    )
    values: list[Any] = []
    for scope in _history_nested_dicts(row):
        values.extend(scope.get(key) for key in id_keys)
    return {cleaned for value in values if (cleaned := clean_exchange_id(value))}


def _history_row_side(row: dict[str, Any]) -> str:
    return str(
        _history_value(row, "positionSide", "posSide", "side", "position_side") or ""
    ).lower()


def _history_row_filled_qty(row: dict[str, Any]) -> float:
    filled_qty = _f(
        _history_value(
            row,
            "filledQty",
            "executedQty",
            "cumQty",
            "dealVol",
            "executedVolume",
            "realityVol",
            "filledVolume",
            "filled_quantity",
        ),
        0.0,
    )
    status = str(_history_value(row, "status", "state_name", "stateName") or "").upper()
    state = int(_f(_history_value(row, "state"), 0.0))
    if filled_qty <= 0 and (
        row.get("fully_filled") is True
        or row.get("terminal_filled") is True
        or state == 3
        or status in {"FILLED", "FULLY_FILLED", "EXECUTED", "COMPLETED", "DONE"}
    ):
        filled_qty = _f(
            _history_value(row, "origQty", "quantity", "vol", "qty", "volume"), 0.0
        )
    return filled_qty


def _history_row_stop_price(row: dict[str, Any]) -> float:
    return _f(
        _history_value(
            row,
            "stopLossPrice",
            "triggerPrice",
            "stopPrice",
            "activationPrice",
            "price",
        ),
        0.0,
    )


def _history_row_is_terminal_stop(row: dict[str, Any]) -> bool:
    typ = str(_history_value(row, "type", "orderType", "triggerType") or "").upper()
    if "TAKE_PROFIT" in typ or "TAKEPROFIT" in typ:
        return False
    stop_like = (
        bool(_history_value(row, "stopExecuted", "stop_loss_executed"))
        or "STOP" in typ
        or typ.startswith("TRIGGER")
        or _history_row_stop_price(row) > 0
    )
    if not stop_like:
        return False
    if int(_f(_history_value(row, "state"), 0.0)) == 3:
        return True
    if bool(row.get("fully_filled")) or bool(row.get("terminal_filled")):
        return True
    status = str(_history_value(row, "status", "state_name", "stateName") or "").upper()
    return status in {"FILLED", "FULLY_FILLED", "EXECUTED", "COMPLETED", "DONE"}


def _price_reached_tp(side: str, observed: float, target: float) -> bool:
    if observed <= 0 or target <= 0:
        return False
    return observed >= target if side.lower() == "long" else observed <= target


def _mark_tp_price_seen(
    tp_rows: list[dict[str, Any]], side: str, observed_price: float
) -> set[int]:
    changed: set[int] = set()
    if observed_price <= 0:
        return changed
    seen_at = datetime.now(timezone.utc).isoformat()
    for fallback_index, tp in enumerate(tp_rows, 1):
        target = _tp_target(tp)
        if not _price_reached_tp(side, observed_price, target):
            continue
        index = int(tp.get("tp_index") or fallback_index)
        if tp.get("price_seen") is not True:
            tp["price_seen"] = True
            tp["price_seen_at"] = seen_at
            tp["price_seen_value"] = float(observed_price)
            changed.add(index)
    return changed


def _tp_exact_order_ids(tp: dict[str, Any]) -> set[str]:
    """Return every durable exact identity stored in the current TP order branch.

    BingX can expose a trigger plan through different identifiers on write,
    openOrders and allOrders (for example stopPlanOrderId, orderId, placeOrderId
    or a nested clientOrderID).  These values are safe evidence only because
    they are read from the bot-owned TP row itself.  We deliberately do not walk
    historical/previous TP fields outside the current ``order`` branch.
    """

    order = _tp_order_payload(tp)
    if not order:
        return set()
    id_keys = (
        "_confirmed_stop_plan_id",
        "_confirmed_order_id",
        "stopOrderId",
        "stopPlanOrderId",
        "planOrderId",
        "orderId",
        "orderID",
        "order_id",
        "id",
        "placeOrderId",
        "delegatedOrderId",
        "childOrderId",
        "triggerOrderId",
        "algoOrderId",
        "clientOrderId",
        "clientOrderID",
        "client_order_id",
        "externalOid",
        "origClientOrderId",
        "origClientOrderID",
        "orig_client_order_id",
    )
    child_keys = (
        "raw",
        "data",
        "order",
        "trigger",
        "detail",
        "result",
        "verification",
        "identity_recovery_v1",
        "identities",
    )
    identity_list_keys = ("identity_ids", "recovered_ids", "matched_history_ids")
    found: set[str] = set()
    visited: set[int] = set()

    def walk(value: Any, depth: int = 0) -> None:
        if depth > 5 or not isinstance(value, dict) or id(value) in visited:
            return
        visited.add(id(value))
        for key in id_keys:
            cleaned = clean_exchange_id(value.get(key))
            if cleaned:
                found.add(cleaned)
        scalar_data = value.get("data")
        if not isinstance(scalar_data, (dict, list)):
            cleaned = clean_exchange_id(scalar_data)
            if cleaned:
                found.add(cleaned)
        for key in identity_list_keys:
            items = value.get(key)
            if isinstance(items, (list, tuple, set)):
                for item in items:
                    cleaned = clean_exchange_id(item)
                    if cleaned:
                        found.add(cleaned)
        for key in child_keys:
            child = value.get(key)
            if isinstance(child, dict):
                walk(child, depth + 1)

    walk(order)
    legacy_primary = _tp_stop_order_id(tp)
    if legacy_primary:
        found.add(legacy_primary)
    return found


def _tp_plan_query_ids(tp: dict[str, Any]) -> list[str]:
    """Return bounded plan-shaped IDs from the current TP order branch.

    Older rows may retain the parent plan only inside
    ``verification.identities``/``identity_aliases``.  Search those bounded,
    known branches, while keeping regular child/order IDs as legacy fallback
    only when no explicit plan-shaped identity exists.
    """

    order = _tp_order_payload(tp)
    if not order:
        return []
    plan_keys = (
        "_confirmed_stop_plan_id",
        "stopPlanOrderId",
        "planOrderId",
        "stopOrderId",
        "triggerOrderId",
        "algoOrderId",
    )
    fallback_keys = (
        "_confirmed_order_id",
        "orderId",
        "orderID",
        "order_id",
        "id",
        "placeOrderId",
    )
    child_keys = (
        "raw",
        "data",
        "order",
        "trigger",
        "detail",
        "result",
        "verification",
        "identities",
        "identity_aliases",
    )
    explicit: list[str] = []
    fallback: list[str] = []
    visited: set[int] = set()

    def add(target: list[str], value: Any) -> None:
        cleaned = clean_exchange_id(value)
        if cleaned and cleaned not in target and len(target) < 16:
            target.append(cleaned)

    def walk(value: Any, depth: int = 0) -> None:
        if depth > 6 or id(value) in visited:
            return
        if isinstance(value, dict):
            visited.add(id(value))
            for key in plan_keys:
                item = value.get(key)
                if isinstance(item, (list, tuple, set)):
                    for nested in item:
                        add(explicit, nested)
                else:
                    add(explicit, item)
            for key in fallback_keys:
                item = value.get(key)
                if isinstance(item, (list, tuple, set)):
                    for nested in item:
                        add(fallback, nested)
                else:
                    add(fallback, item)
            for key in child_keys:
                child = value.get(key)
                if isinstance(child, (dict, list, tuple, set)):
                    walk(child, depth + 1)
        elif isinstance(value, (list, tuple, set)):
            visited.add(id(value))
            for item in list(value)[:32]:
                if isinstance(item, (dict, list, tuple, set)):
                    walk(item, depth + 1)

    walk(order)
    selected = explicit or fallback
    return selected[:8]


def _tp_history_recovery_candidate(
    tp: dict[str, Any],
    history_rows: list[dict[str, Any]],
    *,
    side: str,
) -> dict[str, Any] | None:
    """Select one explicit TP fill only to trigger an exact identity lookup.

    Price/quantity are never ownership evidence here.  They merely bound which
    child order IDs are worth querying through BingX's exact order-detail
    endpoint.  Ownership is accepted later only when that exact detail contains
    both a durable bot-owned plan ID and the history child ID.
    """

    target = _tp_confirmed_target(tp)
    planned_qty = _f(tp.get("qty") or tp.get("actual_tp_qty"), 0.0)
    if target <= 0 or planned_qty <= 0:
        return None
    wanted_side = str(side or "").lower()
    price_tolerance = max(abs(target) * 1e-9, 1e-12)
    qty_tolerance = max(abs(planned_qty) * 1e-6, 1e-10)
    candidates: list[dict[str, Any]] = []
    for row in history_rows:
        if not isinstance(row, dict):
            continue
        row_side = _history_row_side(row)
        if row_side and wanted_side and row_side != wanted_side:
            continue
        row_type = str(
            _history_value(row, "type", "orderType", "triggerType") or ""
        ).upper()
        explicit_tp_type = (
            "TAKE_PROFIT" in row_type or "TAKEPROFIT" in row_type
        )
        delegated_regular = bool(
            row_type in {"MARKET", "LIMIT"}
            and _history_row_bool(row, "reduceOnly", "reduce_only")
        )
        if not explicit_tp_type and not delegated_regular:
            continue
        if (
            not _history_row_is_terminal_filled(row)
            or _history_row_is_explicit_stop_only(row)
            or not _history_row_is_safe_tp_fill_candidate(row)
        ):
            continue
        row_price = _f(
            _history_value(
                row,
                "takeProfitPrice",
                "take_profit_price",
                "triggerPrice",
                "stopPrice",
            ),
            0.0,
        )
        row_qty = _history_row_filled_qty(row)
        if row_price > 0 and abs(row_price - target) > price_tolerance:
            continue
        if row_qty <= 0 or abs(row_qty - planned_qty) > qty_tolerance:
            continue
        if not _history_row_ids(row):
            continue
        candidates.append(row)
    if len(candidates) != 1:
        return None
    return candidates[0]


def _bounded_recovery_attempts(value: Any) -> int:
    try:
        if value in (None, "") or isinstance(value, bool):
            return 0
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return 0
    return min(max(0, parsed), 1_000_000)


def _tp_identity_recovery_retry_delay(attempts: Any) -> int:
    schedule = (60, 300, 900, 1800, 3600, 7200)
    normalized = max(1, _bounded_recovery_attempts(attempts))
    return schedule[min(normalized - 1, len(schedule) - 1)]


_RECOVERY_SECRET_RE = re.compile(
    r"(?i)([\"']?(?:signature|timestamp|recvwindow|api[-_]?(?:key|secret)|"
    r"secret[-_]?key|x-bx-apikey|authorization|passphrase|access[-_]?token|"
    r"refresh[-_]?token|bot[-_]?token)[\"']?\s*[:=]\s*)"
    r"[\"']?[^&\s,;}\]]+"
)
_RECOVERY_SIGNED_URL_RE = re.compile(r"(?i)(https?://[^\s?]+)\?[^\s]+")

_TP_IDENTITY_SAFE_KEYS = frozenset(
    {
        "stopOrderId",
        "stopPlanOrderId",
        "planOrderId",
        "orderId",
        "orderID",
        "order_id",
        "id",
        "placeOrderId",
        "delegatedOrderId",
        "childOrderId",
        "triggerOrderId",
        "algoOrderId",
        "clientOrderId",
        "clientOrderID",
        "client_order_id",
        "origClientOrderId",
        "origClientOrderID",
        "orig_client_order_id",
        "externalOid",
    }
)


def _sanitize_tp_recovery_error(value: Any) -> str:
    text = str(value or "").replace("\r", " ").replace("\n", " ")[:1600]
    text = _RECOVERY_SECRET_RE.sub(lambda match: f"{match.group(1)}<redacted>", text)
    text = _RECOVERY_SIGNED_URL_RE.sub(r"\1?<redacted>", text)
    return text[:500]


def _bounded_tp_identity_detail(detail: dict[str, Any]) -> dict[str, Any]:
    """Keep only exact identities and non-secret order facts from recovery."""

    safe: dict[str, Any] = {"version": 2}
    for key in (
        "queried_order_id",
        "symbol",
        "side",
        "positionSide",
        "type",
        "status",
        "state",
        "terminal",
        "fully_filled",
        "filledQty",
        "origQty",
        "triggerPrice",
        "reduceOnly",
        "updateTime",
    ):
        value = detail.get(key)
        if key in {"queried_order_id"}:
            value = clean_exchange_id(value)
        elif key in {"symbol", "side", "positionSide", "type", "status"}:
            value = str(value or "")[:64]
        elif key in {"filledQty", "origQty", "triggerPrice"}:
            value = _f(value, 0.0)
        elif key in {"terminal", "fully_filled", "reduceOnly"}:
            value = bool(value)
        elif key in {"state", "updateTime"}:
            if isinstance(value, bool):
                value = None
            elif isinstance(value, (int, float)):
                value = value if math.isfinite(float(value)) else None
            else:
                value = str(value or "")[:64]
        if value not in (None, "", [], {}):
            safe[key] = value

    identities: dict[str, str] = {}
    raw_identities = detail.get("identities")
    if isinstance(raw_identities, dict):
        for key, value in list(raw_identities.items())[:64]:
            if str(key) not in _TP_IDENTITY_SAFE_KEYS:
                continue
            cleaned = clean_exchange_id(value)
            if cleaned and len(identities) < 32:
                identities[str(key)[:64]] = cleaned
    safe["identities"] = identities

    aliases: dict[str, list[str]] = {}
    raw_aliases = detail.get("identity_aliases")
    if isinstance(raw_aliases, dict):
        for key, values in list(raw_aliases.items())[:64]:
            if str(key) not in _TP_IDENTITY_SAFE_KEYS:
                continue
            if not isinstance(values, (list, tuple, set)):
                continue
            bucket: list[str] = []
            for value in list(values)[:16]:
                cleaned = clean_exchange_id(value)
                if cleaned and cleaned not in bucket and len(bucket) < 8:
                    bucket.append(cleaned)
            if bucket and len(aliases) < 32:
                aliases[str(key)[:64]] = bucket
    safe["identity_aliases"] = aliases

    # An arbitrary value labelled as an ``identity_id`` must not become durable
    # evidence by itself.  Retain only IDs also exposed through a known identity
    # alias (or the exact queried ID), so malformed/private fields cannot be
    # smuggled into recovery metadata.
    allowed_identity_values = set(identities.values())
    for values in aliases.values():
        allowed_identity_values.update(values)
    queried = clean_exchange_id(safe.get("queried_order_id"))
    if queried:
        allowed_identity_values.add(queried)
    identity_ids: list[str] = []
    raw_identity_ids = detail.get("identity_ids")
    if isinstance(raw_identity_ids, (list, tuple, set)):
        for value in list(raw_identity_ids)[:64]:
            cleaned = clean_exchange_id(value)
            if (
                cleaned
                and cleaned in allowed_identity_values
                and cleaned not in identity_ids
                and len(identity_ids) < 32
            ):
                identity_ids.append(cleaned)
    for cleaned in sorted(allowed_identity_values):
        if cleaned not in identity_ids and len(identity_ids) < 32:
            identity_ids.append(cleaned)
    safe["identity_ids"] = identity_ids
    return safe


async def _recover_exact_tp_child_identities(
    *,
    adapter: Any,
    tp_rows: list[dict[str, Any]],
    history_rows: list[dict[str, Any]],
    side: str,
    symbol: str,
    max_requests: int = 4,
) -> set[int]:
    """Recover and durably persist exact plan->delegated-child TP bridges.

    The recovery is read-only and fail-closed.  A child ID is persisted only
    when one exact order-detail response contains both a current bot-owned TP
    plan ID and the terminal child ID already present in history.  No price-only
    or quantity-only adoption is possible.
    """

    fetch_detail = getattr(adapter, "fetch_protective_order_identity_detail", None)
    if not callable(fetch_detail) or not history_rows or max_requests <= 0:
        return set()

    now = datetime.now(timezone.utc)
    changed: set[int] = set()
    requests_used = 0
    candidate_owners: dict[str, set[int]] = {}
    candidates_by_row: dict[int, dict[str, Any]] = {}
    saved_ids_by_row: dict[int, set[str]] = {}
    saved_id_owners: dict[str, set[int]] = {}
    for row_number, tp in enumerate(tp_rows, 1):
        if not isinstance(tp, dict):
            continue
        row_saved_ids = _tp_exact_order_ids(tp)
        saved_ids_by_row[row_number] = row_saved_ids
        for saved_id in row_saved_ids:
            saved_id_owners.setdefault(saved_id, set()).add(row_number)

    for row_number, tp in enumerate(tp_rows, 1):
        if not isinstance(tp, dict) or tp.get("filled") is True:
            continue
        candidate = _tp_history_recovery_candidate(tp, history_rows, side=side)
        if not candidate:
            continue
        candidates_by_row[row_number] = candidate
        for child_id in _history_row_ids(candidate):
            candidate_owners.setdefault(child_id, set()).add(row_number)

    for row_number, tp in enumerate(tp_rows, 1):
        if requests_used >= max_requests:
            break
        candidate = candidates_by_row.get(row_number)
        if not candidate:
            continue
        child_ids = {
            child_id
            for child_id in _history_row_ids(candidate)
            if len(candidate_owners.get(child_id, ())) == 1
        }
        if not child_ids:
            continue
        order = _tp_order_payload(tp)
        if not order:
            continue
        state = order.get("identity_recovery_v1")
        state = state if isinstance(state, dict) else {}
        if str(state.get("status") or "") == "bridge_confirmed":
            continue
        next_attempt_at = _parse_utc_datetime(state.get("next_attempt_at"))
        if next_attempt_at is not None and next_attempt_at > now:
            continue

        saved_ids = saved_ids_by_row.get(row_number, set())
        unique_saved_ids = {
            saved_id
            for saved_id in saved_ids
            if saved_id_owners.get(saved_id) == {row_number}
        }
        foreign_saved_ids = {
            saved_id
            for saved_id, owners in saved_id_owners.items()
            if row_number not in owners
        }
        plan_ids = [
            plan_id
            for plan_id in _tp_plan_query_ids(tp)
            if plan_id in unique_saved_ids
        ]
        if not unique_saved_ids or not plan_ids:
            continue
        target = _tp_confirmed_target(tp)
        planned_qty = _f(tp.get("qty") or tp.get("actual_tp_qty"), 0.0)
        attempts = _bounded_recovery_attempts(state.get("attempts"))
        query_ids = list(sorted(child_ids)) + [
            value for value in plan_ids if value not in child_ids
        ]
        last_error = "exact detail did not expose a plan-child bridge"
        recovered = False
        for query_id in query_ids:
            if requests_used >= max_requests:
                break
            requests_used += 1
            attempts += 1
            record_counter("tp_identity_recovery_attempts")
            try:
                detail = await fetch_detail(
                    symbol=symbol,
                    order_id=query_id,
                    kind="tp",
                    side=side,
                    expected_price=target,
                    expected_qty=planned_qty,
                )
                if not isinstance(detail, dict):
                    last_error = "exact detail returned no identity snapshot"
                    continue
                detail = _bounded_tp_identity_detail(detail)
                detail_ids = _tp_exact_order_ids({"order": detail})
                matched_saved = detail_ids.intersection(unique_saved_ids)
                matched_child = detail_ids.intersection(child_ids)
                touched_foreign = detail_ids.intersection(foreign_saved_ids)
                if touched_foreign:
                    last_error = (
                        "exact detail touches durable identity owned by another TP"
                    )
                    continue
                if not matched_saved or not matched_child:
                    last_error = (
                        "exact detail lacks both unique durable plan and history child ids"
                    )
                    continue
                recovery = {
                    "version": 1,
                    "status": "bridge_confirmed",
                    "attempts": attempts,
                    "query_order_id": query_id,
                    "recovered_ids": sorted(detail_ids)[:32],
                    "matched_saved_ids": sorted(matched_saved)[:16],
                    "matched_history_ids": sorted(matched_child)[:16],
                    "detail": detail,
                    "recovered_at": now.isoformat(),
                    "next_attempt_at": None,
                    "last_error": None,
                }
                order["identity_recovery_v1"] = recovery
                changed.add(_safe_tp_index(tp.get("tp_index")) or row_number)
                record_counter("tp_identity_recovery_confirmed")
                log.info(
                    "TP_IDENTITY_BRIDGE_RECOVERED symbol=%s side=%s tp_index=%s "
                    "query_id=%s plan_ids=%s child_ids=%s",
                    str(symbol or "").upper(),
                    str(side or "").upper(),
                    _safe_tp_index(tp.get("tp_index")) or row_number,
                    query_id,
                    sorted(matched_saved),
                    sorted(matched_child),
                )
                recovered = True
                break
            except Exception as exc:
                last_error = _sanitize_tp_recovery_error(
                    f"{type(exc).__name__}: {exc}"
                )
                log.warning(
                    "TP identity bridge exact-detail read failed symbol=%s side=%s "
                    "tp_index=%s query_id=%s error=%s",
                    str(symbol or "").upper(),
                    str(side or "").upper(),
                    _safe_tp_index(tp.get("tp_index")) or row_number,
                    query_id,
                    last_error,
                )
        if recovered:
            continue
        retry_delay = _tp_identity_recovery_retry_delay(attempts)
        order["identity_recovery_v1"] = {
            "version": 1,
            "status": "not_linked",
            "attempts": attempts,
            "last_attempt_at": now.isoformat(),
            "next_attempt_at": (now + timedelta(seconds=retry_delay)).isoformat(),
            "last_error": last_error,
        }
        changed.add(_safe_tp_index(tp.get("tp_index")) or row_number)
        record_counter("tp_identity_recovery_deferred")
    return changed


def _history_row_is_terminal_filled(row: dict[str, Any]) -> bool:
    if int(_f(_history_value(row, "state"), 0.0)) == 3:
        return True
    if bool(row.get("fully_filled")) or bool(row.get("terminal_filled")):
        return True
    status = str(_history_value(row, "status", "state_name", "stateName") or "").upper()
    return status in {"FILLED", "FULLY_FILLED", "EXECUTED", "COMPLETED", "DONE"}


def _history_row_time_key(row: dict[str, Any]) -> int:
    return int(_f(_history_value(row, "updateTime", "time", "transactTime"), 0.0))


def _history_match_copy(
    row: dict[str, Any],
    *,
    mode: str,
    seed_ids: set[str],
    component_ids: set[str] | None = None,
    bridge_hops: int = 0,
    anchor_rows: int = 0,
    component_rows: int = 0,
    filled_candidates: int = 0,
    ignored_shared_ids: set[str] | None = None,
) -> dict[str, Any]:
    copied = dict(row)
    row_ids = _history_row_ids(row)
    component = set(component_ids or row_ids)
    copied["_tp_history_identity_match_v2"] = {
        "mode": str(mode),
        "seed_ids": sorted(seed_ids)[:32],
        "matched_seed_ids": sorted(seed_ids.intersection(row_ids))[:32],
        "bridge_ids": sorted((row_ids.intersection(component)) - seed_ids)[:32],
        "component_ids": sorted(component)[:64],
        "bridge_hops": max(0, int(bridge_hops)),
        "anchor_rows": max(0, int(anchor_rows)),
        "component_rows": max(0, int(component_rows)),
        "filled_candidates": max(0, int(filled_candidates)),
        "ignored_shared_ids": sorted(ignored_shared_ids or ())[:32],
    }
    return copied


def _history_exact_identity_component(
    history_rows: list[dict[str, Any]],
    *,
    seed_ids: set[str],
    side: str,
    forbidden_ids: set[str],
    ignored_ids: set[str] | None = None,
) -> tuple[list[tuple[int, dict[str, Any]]], set[str], bool]:
    """Build a bounded exact-ID component from one bot-owned TP identity.

    BingX can expose a conditional TP as two history rows: the plan row carrying
    the bot-owned plan id and a delegated/child FILLED row carrying only another
    exact id from that plan row.  The old matcher looked only for a direct
    intersection with the saved TP ids and therefore stopped on the canceled plan
    row.  This component follows only exact exchange identifiers already present
    in a directly owned history row; it never follows price, quantity or time.

    ``forbidden_ids`` contains exact identities owned by other TP rows in the
    same execution.  Any bridge touching them is ambiguous and fails closed.
    """

    compatible: list[tuple[int, dict[str, Any], set[str]]] = []
    wanted_side = str(side or "").lower()
    for index, row in enumerate(history_rows):
        if not isinstance(row, dict):
            continue
        row_side = _history_row_side(row)
        if row_side and wanted_side and row_side != wanted_side:
            continue
        row_ids = _history_row_ids(row) - set(ignored_ids or ())
        if row_ids:
            compatible.append((index, row, row_ids))

    known_ids = set(seed_ids)
    component_indexes: set[int] = set()
    row_hops: dict[int, int] = {}
    ambiguous = False

    # BingX plan -> delegated order linkage should be one or two hops. Keep a
    # small hard bound so a corrupt identity graph cannot create quadratic work
    # across a full 500-row history page. Longer chains remain manual-review.
    # Use a snapshot per round so bridge_hops reflects the actual link count.
    max_hops = min(len(compatible) + 1, 8)
    for hop in range(1, max_hops + 1):
        known_before_round = set(known_ids)
        batch: list[tuple[int, set[str]]] = []
        for index, _row, row_ids in compatible:
            if index in component_indexes or not row_ids.intersection(
                known_before_round
            ):
                continue
            if row_ids.intersection(forbidden_ids):
                ambiguous = True
                continue
            batch.append((index, row_ids))
        if not batch:
            break
        for index, row_ids in batch:
            component_indexes.add(index)
            row_hops[index] = hop
            known_ids.update(row_ids)

    component = [
        (row_hops[index], row)
        for index, row, _row_ids in compatible
        if index in component_indexes
    ]
    # A connected identity must not simultaneously describe the opposite
    # position side or a terminal STOP-only execution. Those contradictions
    # make the ownership component ambiguous even when a TP-looking child also
    # exists elsewhere in the graph.
    for row in history_rows:
        if not isinstance(row, dict):
            continue
        row_ids = _history_row_ids(row) - set(ignored_ids or ())
        if not row_ids.intersection(known_ids):
            continue
        row_side = _history_row_side(row)
        if row_side and wanted_side and row_side != wanted_side:
            ambiguous = True
        if (
            _history_row_is_terminal_filled(row)
            and _history_row_filled_qty(row) > 0
            and _history_row_is_explicit_stop_only(row)
        ):
            ambiguous = True
    return component, known_ids, ambiguous


def _history_row_is_explicit_stop_only(row: dict[str, Any]) -> bool:
    row_type = str(
        _history_value(row, "type", "orderType", "triggerType") or ""
    ).upper()
    explicit_tp = bool(
        _history_row_bool(row, "tpExecuted", "take_profit_executed")
        or "TAKE_PROFIT" in row_type
        or "TAKEPROFIT" in row_type
    )
    if explicit_tp:
        return False
    # Some BingX payload variants keep the generic TRIGGER type but expose the
    # actual terminal branch through a dedicated stop-executed flag.  Exact ID
    # equality is still not enough to reinterpret that explicit STOP evidence
    # as a TP fill.
    if _history_row_bool(row, "stopExecuted", "stop_loss_executed"):
        return True
    return bool("STOP" in row_type and not row_type.startswith("TRIGGER"))


def _history_row_bool(row: dict[str, Any], *keys: str) -> bool:
    value = _history_value(row, *keys)
    if value is True:
        return True
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value == 1
    return str(value or "").strip().lower() in {"true", "1", "yes"}


def _history_row_is_safe_tp_fill_candidate(row: dict[str, Any]) -> bool:
    """Return whether a terminal fill can safely represent a TP close.

    Exact exchange identity is necessary but not sufficient for regular
    MARKET/LIMIT rows: an entry id or a STOP id can also appear in linked
    payloads. Conditional trigger rows remain acceptable under exact TP
    identity, while regular rows must additionally prove ``reduceOnly`` and a
    concrete closing side. Explicit STOP-only rows are never TP evidence.
    """

    if _history_row_is_explicit_stop_only(row):
        return False
    row_type = str(
        _history_value(row, "type", "orderType", "triggerType") or ""
    ).upper()
    explicit_tp = (
        bool(row.get("tpExecuted"))
        or "TAKE_PROFIT" in row_type
        or "TAKEPROFIT" in row_type
    )
    if explicit_tp or row_type.startswith("TRIGGER"):
        return True
    if row_type in {"MARKET", "LIMIT"}:
        return bool(
            _history_row_bool(row, "reduceOnly", "reduce_only")
            and _history_row_side(row) in {"long", "short"}
        )
    return False


def _history_match_for_tp(
    tp: dict[str, Any],
    history_rows: list[dict[str, Any]],
    *,
    side: str,
    forbidden_order_ids: set[str] | None = None,
    shared_order_ids: set[str] | None = None,
) -> dict[str, Any] | None:
    if tp_row_has_unresolved_identity_conflict(tp):
        return None
    all_order_ids = _tp_exact_order_ids(tp)
    ignored_shared_ids = set(shared_order_ids or ()) & all_order_ids
    order_ids = all_order_ids - ignored_shared_ids
    if all_order_ids:
        # A shared client/delegated identity is not proof of ownership for one
        # TP. Keep unique plan/order ids usable, but never seed or extend the
        # graph through an id stored by multiple TP rows.
        if not order_ids:
            return None
        forbidden_ids = set(forbidden_order_ids or ()) - order_ids - ignored_shared_ids
        exact: list[dict[str, Any]] = []
        exact_ambiguous = False
        for row in history_rows:
            row_ids = _history_row_ids(row) - ignored_shared_ids
            if not order_ids.intersection(row_ids):
                continue
            row_side = _history_row_side(row)
            if row_side and row_side != side.lower():
                exact_ambiguous = True
                continue
            if row_ids.intersection(forbidden_ids):
                exact_ambiguous = True
                continue
            if (
                _history_row_is_terminal_filled(row)
                and _history_row_filled_qty(row) > 0
                and _history_row_is_explicit_stop_only(row)
            ):
                exact_ambiguous = True
                continue
            exact.append(row)

        # Prefer a directly exact terminal fill over a newer canceled plan row.
        # This alone fixes payloads where allOrders returns both states with the
        # same plan/order identity.
        exact_filled = [
            row
            for row in exact
            if _history_row_is_terminal_filled(row)
            and _history_row_filled_qty(row) > 0
            and _history_row_is_safe_tp_fill_candidate(row)
        ]
        direct_common_ids: set[str] | None = None
        for row in exact_filled:
            candidate_ids = (_history_row_ids(row) - ignored_shared_ids).intersection(
                order_ids
            )
            direct_common_ids = (
                set(candidate_ids)
                if direct_common_ids is None
                else direct_common_ids.intersection(candidate_ids)
            )
        if len(exact_filled) > 1 and not direct_common_ids:
            # Two terminal rows matching different durable ids from one TP
            # branch are not automatically the same child execution. Treat the
            # component as ambiguous instead of choosing whichever row is newer.
            exact_ambiguous = True
        if exact_filled and not exact_ambiguous:
            selected = max(exact_filled, key=_history_row_time_key)
            return _history_match_copy(
                selected,
                mode="direct_exact_terminal_fill",
                seed_ids=order_ids,
                component_ids=_history_row_ids(selected),
                bridge_hops=0,
                anchor_rows=len(exact),
                component_rows=len(exact),
                filled_candidates=len(exact_filled),
                ignored_shared_ids=ignored_shared_ids,
            )

        component, component_ids, component_ambiguous = (
            _history_exact_identity_component(
                history_rows,
                seed_ids=order_ids,
                side=side,
                forbidden_ids=forbidden_ids,
                ignored_ids=ignored_shared_ids,
            )
        )
        bridge_candidates = [
            (hop, row)
            for hop, row in component
            if _history_row_is_terminal_filled(row)
            and _history_row_filled_qty(row) > 0
            and _history_row_is_safe_tp_fill_candidate(row)
        ]
        # Direct candidates were handled above.  A bridge candidate must be
        # connected through at least one exact anchor row and must not collide
        # with another TP's durable identity.
        indirect_candidates = [
            (hop, row)
            for hop, row in bridge_candidates
            if not order_ids.intersection(_history_row_ids(row) - ignored_shared_ids)
        ]
        if indirect_candidates and not (exact_ambiguous or component_ambiguous):
            # Multiple representations of the same child order are safe only
            # when all terminal rows share at least one exact child identity.
            common_child_ids: set[str] | None = None
            for _hop, row in indirect_candidates:
                child_ids = _history_row_ids(row) - order_ids - ignored_shared_ids
                common_child_ids = (
                    set(child_ids)
                    if common_child_ids is None
                    else common_child_ids.intersection(child_ids)
                )
            if common_child_ids:
                hop, selected = max(
                    indirect_candidates,
                    key=lambda item: (
                        _history_row_time_key(item[1]),
                        _history_row_filled_qty(item[1]),
                    ),
                )
                return _history_match_copy(
                    selected,
                    mode="transitive_exact_child_fill",
                    seed_ids=order_ids,
                    component_ids=component_ids,
                    bridge_hops=hop,
                    anchor_rows=len(exact),
                    component_rows=len(component),
                    filled_candidates=len(indirect_candidates),
                    ignored_shared_ids=ignored_shared_ids,
                )

        # Keep the newest exact non-filled row for diagnostics, but never fall
        # back to a price-only match when durable TP identities exist.
        if exact:
            selected = max(exact, key=_history_row_time_key)
            return _history_match_copy(
                selected,
                mode=(
                    "ambiguous_exact_component"
                    if exact_ambiguous or component_ambiguous
                    else "direct_exact_nonfilled"
                ),
                seed_ids=order_ids,
                component_ids=component_ids or _history_row_ids(selected),
                bridge_hops=0,
                anchor_rows=len(exact),
                component_rows=len(component),
                filled_candidates=len(indirect_candidates),
                ignored_shared_ids=ignored_shared_ids,
            )
        return None

    target = _tp_target(tp)
    position_id = _tp_position_id(tp)
    if target <= 0:
        return None
    candidates: list[dict[str, Any]] = []
    for row in history_rows:
        row_side = _history_row_side(row)
        if row_side and row_side != side.lower():
            continue
        row_position_id = clean_exchange_id(_history_value(row, "positionId"))
        if position_id and row_position_id != position_id:
            # Once a durable position identity exists, a history row with a
            # missing/malformed/different positionId is not exact evidence.
            continue
        row_target = _f(_history_value(row, "takeProfitPrice"), 0.0)
        tolerance = max(abs(target) * 1e-9, 1e-12)
        if row_target > 0 and abs(row_target - target) <= tolerance:
            candidates.append(row)
    if len(candidates) == 1:
        return _history_match_copy(
            candidates[0],
            mode="legacy_explicit_tp_price",
            seed_ids=set(),
            component_ids=_history_row_ids(candidates[0]),
            component_rows=1,
            filled_candidates=int(_history_row_is_terminal_filled(candidates[0])),
        )
    return None


def _tp_history_identity_diagnostics(
    tp_rows: list[dict[str, Any]],
    history_rows: list[dict[str, Any]],
    *,
    side: str,
) -> dict[str, Any]:
    """Return a bounded, secret-free identity snapshot for unresolved closes."""

    tp_summary: list[dict[str, Any]] = []
    for fallback_index, tp in enumerate(tp_rows[:8], 1):
        if not isinstance(tp, dict):
            continue
        audit = tp.get("exchange_fill_check")
        audit_dict = audit if isinstance(audit, dict) else {}
        tp_summary.append(
            {
                "tp_index": int(tp.get("tp_index") or fallback_index),
                "filled": tp.get("filled") is True,
                "planned_qty": _f(tp.get("qty") or tp.get("actual_tp_qty"), 0.0),
                "target": _tp_target(tp),
                "price_seen": tp.get("price_seen") is True,
                "fill_source": str(tp.get("fill_source") or ""),
                "saved_ids": sorted(_tp_exact_order_ids(tp))[:32],
                "last_match_mode": str(audit_dict.get("identity_match_mode") or ""),
                "last_accepted": bool(audit_dict.get("accepted_as_owned_tp")),
            }
        )

    row_summary: list[dict[str, Any]] = []
    wanted_side = str(side or "").lower()
    for row in history_rows[:32]:
        if not isinstance(row, dict):
            continue
        row_side = _history_row_side(row)
        row_summary.append(
            {
                "ids": sorted(_history_row_ids(row))[:32],
                "side": row_side,
                "side_matches": not row_side
                or not wanted_side
                or row_side == wanted_side,
                "symbol": str(_history_value(row, "symbol") or "").upper(),
                "type": str(
                    _history_value(row, "type", "orderType", "triggerType") or ""
                ).upper(),
                "status": str(
                    _history_value(row, "status", "state_name", "stateName") or ""
                ).upper(),
                "filled_qty": _history_row_filled_qty(row),
                "terminal_filled": _history_row_is_terminal_filled(row),
                "reduce_only": _history_row_bool(row, "reduceOnly", "reduce_only"),
                "stop_executed": _history_row_bool(
                    row, "stopExecuted", "stop_loss_executed"
                ),
                "take_profit_price": _f(
                    _history_value(row, "takeProfitPrice", "take_profit_price"),
                    0.0,
                ),
                "trigger_price": _f(
                    _history_value(row, "triggerPrice", "stopPrice"), 0.0
                ),
                "update_time": _history_row_time_key(row),
            }
        )
    return {
        "version": 2,
        "tp_rows": tp_summary,
        "history_rows": row_summary,
        "history_row_count": len(history_rows),
        "truncated": len(history_rows) > len(row_summary),
    }


def _matching_terminal_stop_history_qty(
    history_rows: list[dict[str, Any]],
    *,
    side: str,
    unexplained_reduction: float,
    progress_tolerance: float,
) -> tuple[float, list[str]]:
    """Return one current terminal STOP row for diagnostics only.

    Quantity equality alone is insufficient because an old STOP fill can have
    the same size as a later TP reduction.  Require explicit STOP execution,
    exact identity, and the newest terminal row among relevant STOP/TP fills.
    The result still never claims bot ownership or changes execution state.
    """

    wanted_side = str(side or "").lower()
    tolerance = max(
        float(progress_tolerance or 0.0),
        abs(float(unexplained_reduction or 0.0)) * 1e-6,
        1e-10,
    )
    relevant_terminal_rows = [
        row
        for row in history_rows
        if isinstance(row, dict)
        and _history_row_is_terminal_filled(row)
        and (not _history_row_side(row) or _history_row_side(row) == wanted_side)
    ]
    relevant_terminal_times = [
        _history_row_time_key(row) for row in relevant_terminal_rows
    ]
    newest_terminal_time = max(relevant_terminal_times, default=0)
    if newest_terminal_time > 0:
        newest_rows = [
            row
            for row in relevant_terminal_rows
            if _history_row_time_key(row) == newest_terminal_time
        ]
        # Same-millisecond TP/STOP terminal rows are ambiguous diagnostic
        # evidence.  Prefer the unresolved marker over falsely labelling the
        # reduction as STOP-backed.
        if any(not _history_row_is_explicit_stop_only(row) for row in newest_rows):
            return 0.0, []
    matches: list[tuple[float, list[str]]] = []
    seen: set[tuple[str, float]] = set()
    for row in history_rows:
        if not isinstance(row, dict):
            continue
        row_side = _history_row_side(row)
        if row_side and wanted_side and row_side != wanted_side:
            continue
        if (
            not _history_row_is_terminal_filled(row)
            or not _history_row_is_explicit_stop_only(row)
            or not _history_row_bool(row, "stopExecuted", "stop_loss_executed")
        ):
            continue
        row_time = _history_row_time_key(row)
        if newest_terminal_time > 0 and row_time <= 0:
            continue
        if newest_terminal_time > 0 and row_time != newest_terminal_time:
            continue
        qty = _history_row_filled_qty(row)
        if qty <= 0 or abs(qty - unexplained_reduction) > tolerance:
            continue
        ids = sorted(_history_row_ids(row))[:16]
        if not ids:
            continue
        key = (ids[0], round(qty, 12))
        if key in seen:
            continue
        seen.add(key)
        matches.append((qty, ids))
    if len(matches) != 1:
        return 0.0, []
    return matches[0]


def _log_unresolved_partial_tp_history(
    *,
    execution_id: int,
    user_id: int,
    symbol: str,
    side: str,
    status: str,
    reason: str,
    original_qty: float,
    qty_now: float,
    closed_qty: float,
    confirmed_tp_qty: float,
    unexplained_reduction: float,
    progress_tolerance: float,
    observed_price: float,
    tp_rows: list[dict[str, Any]],
    history_rows: list[dict[str, Any]],
) -> None:
    """Emit a bounded identity-only snapshot for unresolved partial reductions.

    This is observability only. It never marks a TP filled, moves STOP, changes
    execution status, or persists raw BingX payloads. Repeated unchanged states
    are throttled so one delayed exchange row cannot flood Railway logs.
    """

    execution_id = int(execution_id or 0)
    unresolved = bool(
        execution_id > 0
        and history_rows
        and closed_qty > progress_tolerance
        and unexplained_reduction > progress_tolerance
    )
    if not unresolved:
        _UNRESOLVED_PARTIAL_TP_LOG_STATE.pop(execution_id, None)
        return

    stop_evidence_qty, stop_evidence_ids = _matching_terminal_stop_history_qty(
        history_rows,
        side=side,
        unexplained_reduction=unexplained_reduction,
        progress_tolerance=progress_tolerance,
    )
    if stop_evidence_qty > 0:
        signature = json.dumps(
            {
                "kind": "stop_history_evidence",
                "qty_now": round(float(qty_now), 12),
                "unexplained_reduction": round(float(unexplained_reduction), 12),
                "stop_evidence_qty": round(float(stop_evidence_qty), 12),
                "stop_evidence_ids": stop_evidence_ids,
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        now_mono = time.monotonic()
        previous = _UNRESOLVED_PARTIAL_TP_LOG_STATE.get(execution_id)
        if not (
            previous
            and previous[0] == signature
            and now_mono - previous[1] < _UNRESOLVED_PARTIAL_TP_LOG_INTERVAL_SEC
        ):
            _UNRESOLVED_PARTIAL_TP_LOG_STATE[execution_id] = (signature, now_mono)
            log.info(
                "POSITION_REDUCTION_STOP_HISTORY_EVIDENCE execution_id=%s "
                "user_id=%s symbol=%s side=%s original_qty=%.12g qty_now=%.12g "
                "unexplained_reduction=%.12g stop_history_qty=%.12g stop_ids=%s "
                "ownership_claimed=false",
                execution_id,
                int(user_id or 0),
                str(symbol or "").upper(),
                str(side or "").upper(),
                float(original_qty),
                float(qty_now),
                float(unexplained_reduction),
                float(stop_evidence_qty),
                stop_evidence_ids,
            )
        return

    diagnostics = _tp_history_identity_diagnostics(tp_rows, history_rows, side=side)
    signature_payload = {
        "qty_now": round(float(qty_now), 12),
        "closed_qty": round(float(closed_qty), 12),
        "confirmed_tp_qty": round(float(confirmed_tp_qty), 12),
        "unexplained_reduction": round(float(unexplained_reduction), 12),
        "diagnostics": diagnostics,
    }
    signature = json.dumps(
        signature_payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    now_mono = time.monotonic()
    previous = _UNRESOLVED_PARTIAL_TP_LOG_STATE.get(execution_id)
    if (
        previous
        and previous[0] == signature
        and now_mono - previous[1] < _UNRESOLVED_PARTIAL_TP_LOG_INTERVAL_SEC
    ):
        return

    if (
        execution_id not in _UNRESOLVED_PARTIAL_TP_LOG_STATE
        and len(_UNRESOLVED_PARTIAL_TP_LOG_STATE) >= _UNRESOLVED_PARTIAL_TP_LOG_MAX
    ):
        oldest = sorted(
            _UNRESOLVED_PARTIAL_TP_LOG_STATE.items(), key=lambda item: item[1][1]
        )[: max(1, _UNRESOLVED_PARTIAL_TP_LOG_MAX // 4)]
        for stale_id, _state in oldest:
            _UNRESOLVED_PARTIAL_TP_LOG_STATE.pop(stale_id, None)

    _UNRESOLVED_PARTIAL_TP_LOG_STATE[execution_id] = (signature, now_mono)
    log.info(
        "TP_HISTORY_UNRESOLVED_PARTIAL execution_id=%s user_id=%s symbol=%s side=%s "
        "status=%s reason=%s original_qty=%.12g qty_now=%.12g closed_qty=%.12g "
        "confirmed_tp_qty=%.12g unexplained_reduction=%.12g observed_price=%.12g "
        "history_rows=%s identity=%s",
        execution_id,
        int(user_id or 0),
        str(symbol or "").upper(),
        str(side or "").upper(),
        str(status or ""),
        str(reason or "").replace("\r", " ").replace("\n", " ")[:160],
        float(original_qty),
        float(qty_now),
        float(closed_qty),
        float(confirmed_tp_qty),
        float(unexplained_reduction),
        float(observed_price),
        len(history_rows),
        json.dumps(
            diagnostics,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ),
    )


def _apply_history_fills(
    tp_rows: list[dict[str, Any]],
    history_rows: list[dict[str, Any]],
    *,
    side: str,
    symbol: str = "",
) -> set[int]:
    """Apply exact BingX stop-order history evidence to saved TP rows."""
    changed: set[int] = set()
    checked_at = datetime.now(timezone.utc).isoformat()
    wanted_symbol = _symbol_key(symbol) if symbol else ""
    if wanted_symbol:
        # Adapter-side filtering is the primary barrier. Keep an independent
        # lifecycle barrier as well, so a mocked/custom adapter or a future
        # regression cannot bridge a bot-owned TP identity across symbols.
        history_rows = [
            row
            for row in history_rows
            if isinstance(row, dict)
            and (
                not _symbol_key(_history_value(row, "symbol") or "")
                or _symbol_key(_history_value(row, "symbol") or "") == wanted_symbol
            )
        ]
    tp_ids_by_row: dict[int, set[str]] = {}
    id_owners: dict[str, set[int]] = {}
    tp_index_counts: dict[int, int] = {}
    legacy_targets: dict[int, float] = {}
    for row_number, tp in enumerate(tp_rows, 1):
        if not isinstance(tp, dict):
            continue
        index = _safe_tp_index(tp.get("tp_index")) or row_number
        tp_index_counts[index] = tp_index_counts.get(index, 0) + 1
        row_ids = _tp_exact_order_ids(tp)
        tp_ids_by_row[row_number] = row_ids
        for order_id in row_ids:
            id_owners.setdefault(order_id, set()).add(row_number)
        if not row_ids:
            target = _tp_target(tp)
            if target > 0:
                legacy_targets[row_number] = target

    legacy_ambiguous_rows: set[int] = set()
    legacy_items = list(legacy_targets.items())
    for left_pos, (left_row, left_target) in enumerate(legacy_items):
        for right_row, right_target in legacy_items[left_pos + 1 :]:
            tolerance = max(
                abs(left_target) * 1e-9,
                abs(right_target) * 1e-9,
                1e-12,
            )
            if abs(left_target - right_target) <= tolerance:
                legacy_ambiguous_rows.update((left_row, right_row))

    for row_number, tp in enumerate(tp_rows, 1):
        if not isinstance(tp, dict):
            log.error(
                "Malformed non-dict TP row blocks history fill reconciliation row=%s type=%s",
                row_number,
                type(tp).__name__,
            )
            continue
        index = _safe_tp_index(tp.get("tp_index")) or row_number
        if tp_index_counts.get(index, 0) > 1:
            log.error(
                "Duplicate TP index blocks history fill reconciliation tp_index=%s rows=%s",
                index,
                tp_index_counts[index],
            )
            continue
        if tp_row_has_unresolved_qty_conflict(tp):
            log.error(
                "TP ledger quantity conflict blocks fill reconciliation tp_index=%s conflict=%s",
                index,
                tp.get("qty_conflict_v1"),
            )
            continue
        own_order_ids = tp_ids_by_row.get(row_number, set())
        if not own_order_ids and row_number in legacy_ambiguous_rows:
            target = legacy_targets.get(row_number, _tp_target(tp))
            # One legacy price-only history row must never satisfy multiple TP
            # ledger rows after exchange tick/float collapse.  Use the same
            # tolerance as the legacy matcher; exact identity rows are
            # unaffected by this guard.
            log.error(
                "Ambiguous legacy TP target blocks price-only history reconciliation tp_index=%s target=%s",
                index,
                target,
            )
            continue
        shared_order_ids = {
            order_id
            for order_id in own_order_ids
            if len(id_owners.get(order_id, ())) > 1
        }
        forbidden_order_ids = {
            order_id
            for order_id, owners in id_owners.items()
            if row_number not in owners and len(owners) == 1
        }
        match = _history_match_for_tp(
            tp,
            history_rows,
            side=side,
            forbidden_order_ids=forbidden_order_ids,
            shared_order_ids=shared_order_ids,
        )
        if not match:
            continue
        state = int(_f(_history_value(match, "state"), 0.0))
        filled_qty = _history_row_filled_qty(match)
        planned_qty = _f(tp.get("qty") or tp.get("actual_tp_qty"), 0.0)
        saved_ids = _tp_exact_order_ids(tp) - shared_order_ids
        row_ids = _history_row_ids(match)
        matched_ids = sorted(saved_ids.intersection(row_ids))
        match_meta = match.get("_tp_history_identity_match_v2")
        if not isinstance(match_meta, dict):
            match_meta = {}
        identity_match_mode = str(match_meta.get("mode") or "")
        exact_identity_bridge = identity_match_mode == "transitive_exact_child_fill"
        accepted_identity_mode = identity_match_mode in {
            "direct_exact_terminal_fill",
            "transitive_exact_child_fill",
            "legacy_explicit_tp_price",
        }
        exact_owned_identity = bool(matched_ids or exact_identity_bridge)
        terminal_filled = _history_row_is_terminal_filled(match)
        explicit_stop_only = _history_row_is_explicit_stop_only(match)
        safe_tp_fill_candidate = _history_row_is_safe_tp_fill_candidate(match)
        row_type = str(
            _history_value(match, "type", "orderType", "triggerType") or ""
        ).upper()
        explicit_tp = (
            bool(match.get("tpExecuted"))
            or "TAKE_PROFIT" in row_type
            or "TAKEPROFIT" in row_type
        )
        accepted_as_owned_tp = bool(
            terminal_filled
            and accepted_identity_mode
            and safe_tp_fill_candidate
            and not explicit_stop_only
            and (explicit_tp or exact_owned_identity)
        )
        evidence_source = (
            "exact_owned_tp_child_order_bridge"
            if accepted_as_owned_tp and exact_identity_bridge
            else "exact_owned_tp_id_generic_trigger"
            if accepted_as_owned_tp and exact_owned_identity and not explicit_tp
            else "exact_owned_tp_id"
            if accepted_as_owned_tp and exact_owned_identity
            else "explicit_tp_history"
            if accepted_as_owned_tp
            else "not_accepted"
        )
        audit = {
            "state": state,
            "status": str(
                _history_value(match, "status", "state_name", "stateName") or ""
            ),
            "type": row_type,
            "stop_order_id": clean_exchange_id(_history_value(match, "stopOrderId")),
            "delegated_order_id": clean_exchange_id(_history_value(match, "orderId")),
            "terminal_fill_order_id": clean_exchange_id(
                _history_value(
                    match,
                    "orderId",
                    "orderID",
                    "placeOrderId",
                    "delegatedOrderId",
                    "childOrderId",
                    "id",
                )
            ),
            "place_order_id": clean_exchange_id(_history_value(match, "placeOrderId")),
            "matched_exact_ids": matched_ids,
            "exact_owned_identity": exact_owned_identity,
            "exact_identity_bridge": exact_identity_bridge,
            "identity_match_mode": identity_match_mode,
            "accepted_identity_mode": accepted_identity_mode,
            "identity_bridge_ids": list(match_meta.get("bridge_ids") or [])[:32],
            "identity_component_ids": list(match_meta.get("component_ids") or [])[:64],
            "identity_bridge_hops": int(match_meta.get("bridge_hops") or 0),
            "identity_anchor_rows": int(match_meta.get("anchor_rows") or 0),
            "identity_component_rows": int(match_meta.get("component_rows") or 0),
            "identity_filled_candidates": int(match_meta.get("filled_candidates") or 0),
            "identity_ignored_shared_ids": list(
                match_meta.get("ignored_shared_ids") or []
            )[:32],
            "terminal_filled": terminal_filled,
            "explicit_tp": explicit_tp,
            "explicit_stop_only": explicit_stop_only,
            "safe_tp_fill_candidate": safe_tp_fill_candidate,
            "accepted_as_owned_tp": accepted_as_owned_tp,
            "evidence_source": evidence_source,
            "filled_qty": filled_qty,
            "planned_qty": planned_qty,
            "trigger_side": int(_f(_history_value(match, "triggerSide"), 0.0)),
            "error_code": int(_f(_history_value(match, "errorCode"), 0.0)),
        }
        existing_audit = dict(tp.get("exchange_fill_check") or {})
        existing_audit.pop("checked_at", None)
        if existing_audit != audit:
            tp["exchange_fill_check"] = {**audit, "checked_at": checked_at}
            changed.add(index)

        tolerance = max(abs(planned_qty) * 1e-6, 1e-10)
        owns_history_row = bool(
            accepted_identity_mode
            and safe_tp_fill_candidate
            and not explicit_stop_only
            and (explicit_tp or exact_owned_identity)
        )
        if owns_history_row and planned_qty > 0:
            if accepted_as_owned_tp and filled_qty + tolerance >= planned_qty:
                if tp.get("filled") is not True:
                    tp["filled"] = True
                    changed.add(index)
                # Keep the historical source value for downstream compatibility;
                # the exact v1.0.7g4 reason is stored in exchange_fill_check.
                tp["fill_source"] = "mexc_stoporder_history"
                tp["exchange_filled_qty"] = min(filled_qty, planned_qty)
                tp["filled_at"] = (
                    _history_value(match, "updateTime", "time", "transactTime")
                    or checked_at
                )
                if not tp.get("fill_detected_at"):
                    tp["fill_detected_at"] = checked_at
                    changed.add(index)
                terminal_fill_order_id = clean_exchange_id(
                    audit.get("terminal_fill_order_id")
                )
                if terminal_fill_order_id and identity_match_mode in {
                    "direct_exact_terminal_fill",
                    "transitive_exact_child_fill",
                }:
                    tp["financial_fill_identity_v1"] = {
                        "version": 1,
                        "exchange_order_id": terminal_fill_order_id,
                        "ownership_confirmed": True,
                        "identity_match_mode": identity_match_mode,
                        "confirmed_at": checked_at,
                    }
                elif tp.get("financial_fill_identity_v1") is not None:
                    # A legacy price-only TP match remains useful for lifecycle
                    # diagnostics, but it is not exact ownership evidence for
                    # fillHistory accounting. Retire any stale promotion left by
                    # g5b3g9 so it cannot enter a financial marker.
                    tp["financial_fill_identity_v1"] = None
                    changed.add(index)
                # update_execution_status_merge performs deep list merges, so an
                # omitted key would leave the old warning in durable JSON.
                tp["partial_fill_warning"] = None
            elif filled_qty > 0:
                warning = {
                    "filled_qty": filled_qty,
                    "planned_qty": planned_qty,
                    "state": state,
                    "status": str(
                        _history_value(match, "status", "state_name", "stateName") or ""
                    ),
                    "type": row_type,
                    "matched_exact_ids": matched_ids,
                }
                existing_warning = dict(tp.get("partial_fill_warning") or {})
                existing_warning.pop("checked_at", None)
                if existing_warning != warning:
                    tp["partial_fill_warning"] = {**warning, "checked_at": checked_at}
                    changed.add(index)
                log.warning(
                    "BingX TP history shows non-full fill tp_index=%s filled=%s planned=%s ids=%s type=%s status=%s",
                    index,
                    filled_qty,
                    planned_qty,
                    matched_ids,
                    row_type,
                    audit["status"],
                )
    return changed


def _execution_start_ms(value: Any) -> int:
    """Parse a DB timestamp and return a safe 90-day-bounded history start."""
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    earliest = now_ms - 90 * 24 * 60 * 60 * 1000
    parsed_ms = 0
    try:
        if isinstance(value, datetime):
            dt = value
        else:
            text = str(value or "").strip().replace("Z", "+00:00")
            dt = datetime.fromisoformat(text) if text else None
        if dt is not None:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            parsed_ms = int(dt.timestamp() * 1000) - 60 * 60 * 1000
    except Exception:
        parsed_ms = 0
    return max(earliest, min(parsed_ms or earliest, now_ms))


def _confirmed_tp_qty(tp_rows: list[dict[str, Any]]) -> float:
    return sum(
        _f(tp.get("exchange_filled_qty"), 0.0)
        or _f(tp.get("qty") or tp.get("actual_tp_qty"), 0.0)
        for tp in tp_rows
        if isinstance(tp, dict) and tp.get("filled") is True
    )


def _be_stop_probable_close_reason(
    tp_rows: list[dict[str, Any]],
    history_rows: list[dict[str, Any]],
    *,
    original_qty: float,
    payload: dict[str, Any] | None,
    side: str,
    expected_stop: float = 0.0,
    allow_timeout_fallback: bool = False,
) -> str:
    """Conservative BingX fallback for BE STOP closes whose history row is incomplete.

    This is intentionally narrower than v1.6.99.  A moved BE STOP alone is not
    enough to label a close as STOP after the history deadline: a manual close can
    also remove the position while a BE STOP existed.  The fallback therefore still
    requires a terminal STOP-like history row for the same side and rejects rows
    whose visible stop price contradicts the saved BE STOP level.
    """

    payload = payload if isinstance(payload, dict) else {}
    be = payload.get("be") if isinstance(payload.get("be"), dict) else {}
    if be.get("moved") is not True:
        return ""
    stop_order_ids, _stop_position_ids = _saved_stop_identity(payload)
    if not stop_order_ids:
        return ""

    original_qty_f = float(original_qty or 0.0)
    tolerance = max(abs(original_qty_f) * 0.002, 1e-10)
    confirmed_tp_qty = _confirmed_tp_qty(tp_rows)
    unexplained_qty = max(0.0, original_qty_f - confirmed_tp_qty)
    if original_qty_f > 0 and confirmed_tp_qty + tolerance >= original_qty_f:
        return ""

    be_stop = _f(expected_stop, 0.0) or _f(be.get("stop"), 0.0)
    stop_price_tolerance = max(abs(be_stop) * 0.001, 1e-12) if be_stop > 0 else 0.0

    for row in history_rows or []:
        if not isinstance(row, dict):
            continue
        row_side = _history_row_side(row)
        if side and row_side and row_side != side.lower():
            continue
        if not _history_row_is_terminal_stop(row):
            continue
        row_stop = _history_row_stop_price(row)
        if (
            row_stop > 0
            and be_stop > 0
            and abs(row_stop - be_stop) > stop_price_tolerance
        ):
            continue
        filled_qty = _history_row_filled_qty(row)
        if filled_qty <= tolerance:
            continue
        if unexplained_qty > tolerance and filled_qty + tolerance < min(
            unexplained_qty, original_qty_f
        ):
            continue
        return "be_stop_probable_history_gap"

    # Do not return a positive STOP classification only because the bounded
    # history wait expired.  No STOP-like terminal row means the safe result is
    # still manual review.  The allow_timeout_fallback argument is retained for
    # call-site clarity, but timeout-only classification is intentionally disabled.
    return ""


def _terminal_close_history_evidence(
    tp_rows: list[dict[str, Any]],
    history_rows: list[dict[str, Any]],
    *,
    original_qty: float,
    payload: dict[str, Any] | None = None,
    side: str = "",
    expected_stop: float = 0.0,
) -> tuple[bool, str]:
    """Return whether BingX history explains a fully closed position.

    A full TP close is accepted only when confirmed TP quantities cover the
    original position. A STOP/BE close is accepted from an exact saved STOP id
    even when BingX omits trigger/stopLossPrice in allOrders history. Price-only
    STOP attribution remains strict and requires one unambiguous terminal STOP
    row matching the expected stop. Manual closes intentionally produce no
    evidence and remain pending until the bounded deadline.
    """
    tolerance = max(abs(float(original_qty)) * 0.002, 1e-10)
    confirmed_tp_qty = _confirmed_tp_qty(tp_rows)
    if original_qty > 0 and confirmed_tp_qty + tolerance >= original_qty:
        return True, "full_tp_history"
    stop_order_ids, stop_position_ids = _saved_stop_identity(payload or {})
    stop_candidates: list[dict[str, Any]] = []
    for row in history_rows or []:
        if not isinstance(row, dict):
            continue
        filled_qty = _f(row.get("filledQty"), 0.0)
        if filled_qty <= 0:
            filled_qty = _history_row_filled_qty(row)
        if filled_qty <= 0 or not _history_row_is_terminal_stop(row):
            continue
        row_side = str(row.get("positionSide") or row.get("side") or "").lower()
        if side and row_side and row_side != side.lower():
            continue
        row_ids = _history_row_ids(row)
        if stop_order_ids:
            # A durable STOP id exists. Do not match another historical STOP
            # merely because its price is similar or because positionId is absent.
            if row_ids.intersection(stop_order_ids):
                return True, "stop_history"
            continue
        row_position_id = clean_exchange_id(row.get("positionId"))
        if stop_position_ids and row_position_id not in stop_position_ids:
            # Do not attribute a symbol/price-only STOP history row to a saved
            # exact position identity when BingX omits or corrupts positionId.
            continue
        stop_price = _history_row_stop_price(row)
        if expected_stop > 0:
            tolerance_price = max(abs(expected_stop) * 1e-7, 1e-10)
            if stop_price <= 0 or abs(stop_price - expected_stop) > tolerance_price:
                continue
        else:
            # Without exact order identity, require a visible STOP trigger price.
            # A generic filled trigger row without price could be an unrelated
            # manual/external close and must stay manual-review.
            if stop_price <= 0:
                continue
        stop_candidates.append(row)
        # Without a saved order id, accept only one unambiguous row matching the
        # current position/side/STOP. This avoids attributing an older same-symbol
        # stop to a manual close of the current trade.
    if len(stop_candidates) == 1:
        return True, "stop_history"
    probable_be_reason = _be_stop_probable_close_reason(
        tp_rows,
        history_rows,
        original_qty=original_qty,
        payload=payload,
        side=side,
        expected_stop=expected_stop,
        allow_timeout_fallback=False,
    )
    if probable_be_reason:
        return True, probable_be_reason
    return False, ""




def _financial_terminal_stop_identity(
    history_rows: list[dict[str, Any]],
    *,
    payload: dict[str, Any],
    side: str,
    expected_qty: float,
) -> tuple[str, str, float, dict[str, Any]]:
    """Return one exact terminal STOP/BE fill identity for fee reconciliation.

    Close classification can tolerate some BingX history gaps, but an accounting
    query cannot.  Require a bot-owned saved STOP identity, a terminal filled
    history row on the correct position side, and quantity parity with the
    residual position.  Multiple newest identities fail closed.
    """

    saved_order_ids, _saved_position_ids = _saved_stop_identity(payload)
    wanted_side = str(side or "").strip().lower()
    expected = _f(expected_qty, 0.0)
    tolerance = max(abs(expected) * 1e-6, 1e-10)
    if not saved_order_ids or expected <= 0:
        return "", "", 0.0, {
            "reason": "saved_stop_identity_or_expected_qty_missing",
            "saved_stop_ids": sorted(saved_order_ids)[:16],
        }

    candidates: list[tuple[int, str, str, float, list[str]]] = []
    for history_row in history_rows or []:
        if not isinstance(history_row, dict):
            continue
        if not _history_row_is_terminal_filled(history_row):
            continue
        if not _history_row_is_terminal_stop(history_row):
            continue
        row_side = _history_row_side(history_row)
        if row_side and wanted_side and row_side != wanted_side:
            continue
        row_ids = _history_row_ids(history_row)
        if not row_ids.intersection(saved_order_ids):
            continue
        row_qty = _history_row_filled_qty(history_row)
        if row_qty <= 0 or abs(row_qty - expected) > tolerance:
            continue
        order_id = clean_exchange_id(
            _history_value(
                history_row,
                "orderId",
                "orderID",
                "placeOrderId",
                "delegatedOrderId",
                "childOrderId",
                "id",
                "stopPlanOrderId",
                "stopOrderId",
            )
        )
        client_id = clean_exchange_id(
            _history_value(
                history_row,
                "clientOrderID",
                "clientOrderId",
                "client_order_id",
                "origClientOrderID",
                "origClientOrderId",
            )
        )
        if not order_id and not client_id:
            continue
        candidates.append(
            (
                _history_row_time_key(history_row),
                order_id,
                client_id,
                row_qty,
                sorted(row_ids)[:16],
            )
        )

    if not candidates:
        return "", "", 0.0, {
            "reason": "exact_terminal_stop_history_missing",
            "saved_stop_ids": sorted(saved_order_ids)[:16],
        }
    newest_time = max(item[0] for item in candidates)
    newest = [item for item in candidates if newest_time <= 0 or item[0] == newest_time]
    identities = {(item[1], item[2]) for item in newest}
    if len(identities) != 1:
        return "", "", 0.0, {
            "reason": "terminal_stop_identity_ambiguous",
            "candidate_count": len(newest),
            "candidate_ids": [item[4] for item in newest[:8]],
        }
    selected = newest[0]
    return selected[1], selected[2], selected[3], {
        "reason": "exact_terminal_stop_history",
        "history_time": selected[0],
        "history_ids": selected[4],
    }


def _financial_marker_for_closed_execution(
    *,
    execution_id: int,
    user_id: int,
    exchange: str,
    row: dict[str, Any],
    payload: dict[str, Any],
    symbol: str,
    side: str,
    original_qty: float,
    close_result: dict[str, Any] | None,
    history_rows: list[dict[str, Any]],
    terminal_history_type: str,
    now: datetime,
) -> dict[str, Any] | None:
    """Build the durable marker only for enabled, supported final closes."""

    if not get_settings().FINANCIAL_RECONCILIATION_ENABLED:
        return None
    if not isinstance(close_result, dict):
        return None
    close_type = str(close_result.get("close_type") or "").strip().lower()
    if close_type not in {"be_stop", "stop", "all_tps"}:
        return None

    terminal_order_id = ""
    terminal_client_id = ""
    terminal_expected_qty = 0.0
    terminal_diagnostics: dict[str, Any] = {}
    if close_type in {"be_stop", "stop"}:
        _tp_rows_now, confirmed_tp_qty = _confirmed_tp_qty_from_payload(payload)
        residual_qty = max(0.0, float(original_qty) - float(confirmed_tp_qty))
        if terminal_history_type == "stop_history":
            (
                terminal_order_id,
                terminal_client_id,
                terminal_expected_qty,
                terminal_diagnostics,
            ) = _financial_terminal_stop_identity(
                history_rows,
                payload=payload,
                side=side,
                expected_qty=residual_qty,
            )
        else:
            terminal_diagnostics = {
                "reason": "terminal_history_not_exact_stop",
                "terminal_history_type": terminal_history_type,
                "residual_qty": residual_qty,
            }

    marker = prepare_financial_reconciliation_marker(
        execution_id=execution_id,
        user_id=user_id,
        exchange=exchange,
        symbol=symbol,
        side=side,
        close_type=close_type,
        strategy_gross_pnl=close_result.get("realized_pnl") or 0.0,
        original_qty=original_qty,
        payload=payload,
        terminal_order_id=terminal_order_id,
        terminal_order_client_id=terminal_client_id,
        terminal_order_expected_qty=terminal_expected_qty or None,
        terminal_at=row.get("closed_at") or now,
        now=now,
    )
    marker["terminal_identity_diagnostics"] = terminal_diagnostics
    marker["terminal_history_type"] = str(terminal_history_type or "")
    return marker


async def _enqueue_financial_marker_best_effort(
    execution_id: int,
    marker: dict[str, Any] | None,
) -> None:
    if not marker or str(marker.get("state") or "") != "ready":
        if marker and str(marker.get("state") or "") == "blocked":
            log.warning(
                "FINANCIAL_RECONCILIATION_PREPARE_BLOCKED execution_id=%s blockers=%s",
                execution_id,
                marker.get("blockers"),
            )
        return
    try:
        await enqueue_prepared_financial_reconciliation(
            int(execution_id),
            expected_status="closed_on_exchange_cleanup",
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        # The durable marker is already part of the terminal execution JSON.
        # The low-priority worker will recover this narrow enqueue gap.
        log.exception(
            "FINANCIAL_RECONCILIATION_ENQUEUE_FAILED_FAIL_OPEN execution_id=%s",
            execution_id,
        )


async def _sync_tp_fill_notifications(
    *,
    notify: NotifyFn | None,
    user_id: int,
    execution_id: int,
    status: str,
    reason: str,
    symbol: str,
    side: str,
    entry: float,
    original_qty: float,
    qty_now: float,
    leverage: float,
    payload: dict[str, Any],
    infer_from_qty: bool,
    observed_price: float = 0.0,
    history_rows: list[dict[str, Any]] | None = None,
    pre_touched: set[int] | None = None,
) -> int:
    """Reconcile TP fills and deliver each notification exactly once.

    Evidence priority:
    1. exact BingX stop-order history (state/realityVol);
    2. position reduction combined with a durable TP price touch.

    A Telegram marker is persisted only after the delivery layer returns an
    explicit positive outcome. Failed deliveries remain retryable.
    """
    tp_rows, ledger_changed, repaired_indices = canonicalize_tp_ledger(payload)
    if ledger_changed:
        await db.merge_execution_metadata(
            execution_id,
            {
                "tp": tp_rows,
                "tp_ledger_v1": tp_ledger_repair_metadata(
                    repaired_indices, source="lifecycle.notification_repair"
                ),
            },
        )
        log.info(
            "TP ledger repaired before notification execution_id=%s user_id=%s symbol=%s indices=%s",
            execution_id,
            user_id,
            symbol,
            repaired_indices,
        )
    if not tp_rows:
        return 0

    ordered = sorted(
        [row for row in tp_rows if isinstance(row, dict)],
        key=lambda row: int(row.get("tp_index") or 999),
    )
    # The caller may mark a touch before invalidating the positions cache. Carry
    # those changed indexes into this transaction; otherwise the second call to
    # _mark_tp_price_seen sees price_seen=True and the touch is never persisted.
    touched = set(pre_touched or ())
    touched.update(_mark_tp_price_seen(ordered, side, observed_price))
    if history_rows:
        touched.update(
            _apply_history_fills(
                ordered,
                history_rows,
                side=side,
                symbol=symbol,
            )
        )

    closed_qty = max(0.0, float(original_qty) - float(qty_now))
    consumed = 0.0
    cumulative_pnl = 0.0
    changed = bool(touched)
    action_indexes: set[int] = set(touched)

    for fallback_index, tp in enumerate(ordered, 1):
        if tp_row_has_unresolved_qty_conflict(
            tp
        ) or tp_row_has_unresolved_identity_conflict(tp):
            log.error(
                "TP notification blocked by unresolved ledger conflict execution_id=%s symbol=%s tp_index=%s qty_conflict=%s identity_conflict=%s",
                execution_id,
                symbol,
                tp.get("tp_index") or fallback_index,
                tp.get("qty_conflict_v1"),
                tp.get("identity_conflict_v1"),
            )
            break
        tp_qty = _f(tp.get("qty") or tp.get("actual_tp_qty"), 0.0)
        tp_price = _tp_target(tp)
        if tp_qty <= 0 or tp_price <= 0:
            continue
        index = int(tp.get("tp_index") or fallback_index)
        price_evidence = tp.get("price_seen") is True
        inferred_filled = bool(
            infer_from_qty
            and price_evidence
            and closed_qty - consumed >= tp_qty - max(tp_qty * 1e-6, 1e-10)
        )
        is_filled = tp.get("filled") is True or bool(inferred_filled)
        if is_filled:
            if tp.get("filled") is not True:
                tp["filled"] = True
                tp["fill_source"] = "position_qty_plus_price_touch"
                detected_iso = datetime.now(timezone.utc).isoformat()
                # Position reduction + price evidence proves that the TP filled,
                # but it does not prove the exchange's exact fill timestamp.
                # Keep execution time unknown and persist only bot detection so
                # notifications/SLA never present a fabricated zero latency.
                tp.pop("filled_at", None)
                tp["fill_detected_at"] = detected_iso
                changed = True
                action_indexes.add(index)
            pnl = (
                (tp_price - entry) * tp_qty
                if side.lower() == "long"
                else (entry - tp_price) * tp_qty
            )
            cumulative_pnl += pnl
            consumed += tp_qty
            if tp.get("filled_notified") is True:
                # Repair stale v1.6.8 diagnostics. Deep JSON merge cannot remove
                # absent keys, so clear them explicitly.
                if tp.get("notification_pending") or tp.get("next_notify_retry_at"):
                    tp["notification_pending"] = False
                    tp["next_notify_retry_at"] = None
                    changed = True
                continue

            if tp.get("filled_notified") is not True:
                now_utc = datetime.now(timezone.utc)
                if not tp.get("fill_detected_at"):
                    # Persist the first bot-side observation separately from
                    # Telegram delivery/retry time.  Notification retries must
                    # never rewrite latency to look artificially shorter.
                    tp["fill_detected_at"] = now_utc.isoformat()
                    changed = True
                if not _tp_notification_retry_due(tp, now=now_utc):
                    # Preserve chronological delivery: TP2 must not be announced
                    # while TP1 is still pending its retry window.
                    break
                tp["notify_attempts"] = int(tp.get("notify_attempts") or 0) + 1
                tp["last_notify_attempt_at"] = now_utc.isoformat()
                changed = True
                expected_remaining = max(0.0, original_qty - consumed)
                display_remaining = min(max(0.0, qty_now), expected_remaining)
                if bool(tp.get("residual_recovery_v1")):
                    _message = card(
                        "✅ <b>ФИНАЛЬНЫЙ ОСТАТОК ЗАКРЫТ</b>",
                        symbol=symbol,
                        side=side,
                        blocks=(
                            [
                                f"🎯 Цена: <b>{tp_price}</b>",
                                f"📦 Количество: <b>{fmt_qty(tp_qty)}</b>",
                                f"Остаток позиции: <b>{fmt_qty(display_remaining)}</b>",
                            ],
                            [
                                "Финальный TP был восстановлен ботом после обнаружения одного минимального остаточного лота."
                            ],
                        ),
                    )
                else:
                    _message = tp_filled_message(
                        symbol=symbol,
                        side=side,
                        tp_index=index,
                        tp_price=tp_price,
                        qty=tp_qty,
                        entry=entry,
                        total_tps=len(ordered),
                        leverage=max(1.0, float(leverage or 1.0)),
                        remaining_qty=display_remaining,
                        cumulative_pnl=cumulative_pnl,
                        fill_source=str(
                            tp.get("fill_source") or "position_qty_reduction"
                        ),
                        filled_at=tp.get("filled_at"),
                        detected_at=tp.get("fill_detected_at") or now_utc,
                    )
                sent = await _notify(
                    notify,
                    user_id,
                    _message,
                )
                if sent:
                    tp["filled_notified"] = True
                    tp["notified_at"] = datetime.now(timezone.utc).isoformat()
                    tp["notification_pending"] = False
                    tp["next_notify_retry_at"] = None
                    action_indexes.add(index)
                else:
                    tp["notification_pending"] = True
                    retry_delay = _tp_notification_retry_delay(
                        int(tp.get("notify_attempts") or 1)
                    )
                    tp["next_notify_retry_at"] = (
                        datetime.now(timezone.utc) + timedelta(seconds=retry_delay)
                    ).isoformat()
                    # Do not send TP2 before an undelivered TP1. The next monitor
                    # pass resumes from the first pending notification.
                    changed = True
                    break
                changed = True
        else:
            # TP notifications are chronological. Do not announce TP2 while TP1
            # remains unconfirmed; history diagnostics stay persisted for review.
            break

    if changed:
        # TP fill/notification fields are metadata, not a trading state
        # transition. Never rewrite status/reason from a stale monitor snapshot.
        await db.merge_execution_metadata(execution_id, {"tp": tp_rows})
        payload["tp"] = tp_rows
    return len(action_indexes)


def _order_count(orders: Any) -> int:
    if isinstance(orders, list):
        return len(orders)
    return 0


async def _fetch_algo_orders(adapter: Any, symbol: str) -> list[dict[str, Any]]:
    if hasattr(adapter, "fetch_open_algo_orders"):
        rows = await adapter.fetch_open_algo_orders(symbol)
        return list(rows or [])
        # Some older adapters exposed conditional orders through fetch_open_orders.
    if hasattr(adapter, "fetch_open_orders"):
        rows = await adapter.fetch_open_orders(symbol)
        return list(rows or [])
    return []


async def _fetch_regular_orders(adapter: Any, symbol: str) -> list[dict[str, Any]]:
    if hasattr(adapter, "fetch_open_orders"):
        rows = await adapter.fetch_open_orders(symbol)
        return list(rows or [])
    return []


class _LifecyclePositionsContext:
    """Pass-local, account-isolated coalescing for lifecycle position pre-reads.

    The context exists only for one ``process_position_lifecycle_guard_once``
    invocation.  It never survives a pass/restart, never mixes users/accounts,
    and is invalidated before/after any exchange write.  Final zero-position and
    post-write confirmations remain direct/fresh reads outside this cache.
    """

    def __init__(self, *, max_age_seconds: float = 15.0) -> None:
        self._max_age = max(0.0, float(max_age_seconds))
        self._cache: dict[
            tuple[int, str, int], tuple[int, float, tuple[dict[str, Any], ...]]
        ] = {}
        self._generation: dict[tuple[int, str, int], int] = {}
        self._inflight: dict[
            tuple[tuple[int, str, int], int], asyncio.Task[list[dict[str, Any]]]
        ] = {}
        self._waiters: dict[tuple[tuple[int, str, int], int], int] = {}
        self._state_lock = asyncio.Lock()
        self._stats: dict[str, int] = {}

    @staticmethod
    def _account_key(user_id: int, exchange: str, adapter: Any) -> tuple[int, str, int]:
        return (int(user_id), str(exchange or "").lower(), id(adapter))

    @staticmethod
    def _copy_rows(rows: Any) -> list[dict[str, Any]]:
        return [dict(row) for row in (rows or []) if isinstance(row, dict)]

    def _inc(self, key: str, amount: int = 1) -> None:
        self._stats[key] = int(self._stats.get(key, 0)) + int(amount)

    async def _load_all(
        self,
        adapter: Any,
        key: tuple[int, str, int],
        *,
        force_refresh: bool,
    ) -> list[dict[str, Any]]:
        now = time.monotonic()
        async with self._state_lock:
            generation = int(self._generation.get(key, 0))
            if force_refresh:
                generation += 1
                self._generation[key] = generation
                self._cache.pop(key, None)
                self._inc("positions_invalidations")

            cached = self._cache.get(key)
            if cached is not None:
                cached_generation, cached_at, cached_rows = cached
                if (
                    cached_generation == generation
                    and self._max_age > 0
                    and now - cached_at <= self._max_age
                ):
                    self._inc("positions_hits")
                    return self._copy_rows(cached_rows)
                generation += 1
                self._generation[key] = generation
                self._cache.pop(key, None)
                self._inc("positions_expirations")

            inflight_key = (key, generation)
            task = self._inflight.get(inflight_key)
            if task is None:

                async def loader() -> list[dict[str, Any]]:
                    try:
                        rows = await adapter.fetch_open_positions(None)
                    except TypeError:
                        rows = await adapter.fetch_open_positions()
                    return self._copy_rows(rows)

                task = asyncio.create_task(loader())
                self._inflight[inflight_key] = task
                self._inc("positions_fetches")
            else:
                self._inc("positions_singleflight_waits")
            self._waiters[inflight_key] = int(self._waiters.get(inflight_key, 0)) + 1

        try:
            loaded = await asyncio.shield(task)
            normalized = self._copy_rows(loaded)
        except asyncio.CancelledError:
            cancel_loader = False
            async with self._state_lock:
                remaining = max(0, int(self._waiters.get(inflight_key, 1)) - 1)
                if remaining:
                    self._waiters[inflight_key] = remaining
                else:
                    self._waiters.pop(inflight_key, None)
                    if self._inflight.get(inflight_key) is task:
                        self._inflight.pop(inflight_key, None)
                    cancel_loader = not task.done()
                self._inc("positions_cancelled_waits")
            if cancel_loader:
                task.cancel()
            raise
        except Exception:
            async with self._state_lock:
                remaining = max(0, int(self._waiters.get(inflight_key, 1)) - 1)
                if remaining:
                    self._waiters[inflight_key] = remaining
                else:
                    self._waiters.pop(inflight_key, None)
                if self._inflight.get(inflight_key) is task:
                    self._inflight.pop(inflight_key, None)
                self._inc("positions_errors")
            raise

        async with self._state_lock:
            remaining = max(0, int(self._waiters.get(inflight_key, 1)) - 1)
            if remaining:
                self._waiters[inflight_key] = remaining
            else:
                self._waiters.pop(inflight_key, None)
            if self._inflight.get(inflight_key) is task:
                self._inflight.pop(inflight_key, None)
            if int(self._generation.get(key, 0)) == generation:
                self._cache[key] = (generation, time.monotonic(), tuple(normalized))
        return self._copy_rows(normalized)

    async def get_positions(
        self,
        adapter: Any,
        user_id: int,
        exchange: str,
        symbol: str | None = None,
        *,
        force_refresh: bool = False,
    ) -> list[dict[str, Any]]:
        self._inc("positions_requests")
        account = self._account_key(user_id, exchange, adapter)
        rows = await self._load_all(adapter, account, force_refresh=force_refresh)
        if not symbol:
            return rows

        wanted = str(symbol).upper().replace("-", "").replace("_", "")
        filtered: list[dict[str, Any]] = []
        for row in rows:
            raw_symbol = str(row.get("symbol") or "").strip()
            if not raw_symbol:
                # Conservative compatibility for legacy/test adapters that return
                # an already symbol-scoped row without a symbol field. Keeping the
                # row can only defer cleanup; dropping it could false-classify zero.
                filtered.append(row)
                continue
            normalized = raw_symbol.upper().replace("-", "").replace("_", "")
            if normalized == wanted:
                filtered.append(row)
        return filtered

    async def invalidate_positions(
        self, adapter: Any, user_id: int, exchange: str
    ) -> None:
        key = self._account_key(user_id, exchange, adapter)
        async with self._state_lock:
            self._generation[key] = int(self._generation.get(key, 0)) + 1
            self._cache.pop(key, None)
            self._inc("positions_invalidations")

    def stats(self) -> dict[str, int]:
        stats = dict(self._stats)
        for suffix in (
            "requests",
            "fetches",
            "hits",
            "singleflight_waits",
            "cancelled_waits",
            "invalidations",
            "expirations",
            "errors",
        ):
            stats.setdefault(f"positions_{suffix}", 0)
        return dict(sorted(stats.items()))


def _live_algo_order_ids(order: dict[str, Any]) -> set[str]:
    """Return only the exact cancellable BingX stop-plan identity."""

    raw = order.get("raw") if isinstance(order.get("raw"), dict) else {}
    for value in (
        order.get("stopPlanOrderId"),
        order.get("stopOrderId"),
        raw.get("stopPlanOrderId"),
        raw.get("stopOrderId"),
        raw.get("id"),
        order.get("id"),
    ):
        cleaned = _clean_identity(value)
        if cleaned:
            return {cleaned}
    return set()


def _live_regular_order_ids(order: dict[str, Any]) -> set[str]:
    raw = order.get("raw") if isinstance(order.get("raw"), dict) else {}
    return {
        cleaned
        for value in (
            order.get("orderId"),
            order.get("id"),
            raw.get("orderId"),
            raw.get("id"),
        )
        if (cleaned := clean_exchange_id(value))
    }


def _saved_entry_order_ids(payload: dict[str, Any]) -> set[str]:
    """Return only the regular ENTRY identity, excluding nested STOP/TP data."""

    entry = payload.get("entry") if isinstance(payload, dict) else None
    if not isinstance(entry, dict):
        return set()
    data = entry.get("data")
    data_dict = data if isinstance(data, dict) else {}
    values = (
        entry.get("_confirmed_order_id"),
        entry.get("orderId"),
        data_dict.get("orderId"),
        data if not isinstance(data, (dict, list)) else None,
    )
    return {cleaned for value in values if (cleaned := clean_exchange_id(value))}


def _saved_conditional_identity(
    payload: dict[str, Any],
) -> tuple[set[str], set[str]]:
    order_ids, position_ids = _saved_stop_identity(payload or {})
    for item in payload.get("tp") or []:
        if not isinstance(item, dict):
            continue
        if tp_row_has_unresolved_identity_conflict(item):
            # Exact cleanup must fail closed when more than one historical TP id
            # competes for the same canonical index.
            continue
        order_id = _tp_stop_order_id(item)
        if order_id:
            order_ids.add(order_id)
        position_id = _tp_position_id(item)
        if position_id:
            position_ids.add(position_id)
    return (
        {_clean_identity(value) for value in order_ids if _clean_identity(value)},
        {_clean_identity(value) for value in position_ids if _clean_identity(value)},
    )


def _call_accepts_keyword(callable_obj: Any, keyword: str) -> bool:
    try:
        signature = inspect.signature(callable_obj)
    except (TypeError, ValueError):
        return False
    return any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD or name == keyword
        for name, parameter in signature.parameters.items()
    )


async def _cancel_exact_with_symbol(
    cancel_exact: Any, order_ids: list[str] | set[str] | tuple[str, ...], symbol: str
) -> dict[str, Any]:
    """Call an exact-cancel hook with symbol when the adapter supports it.

    BingX exact cancel endpoints are symbol-scoped, while older/MEXC test
    doubles expose the legacy ``cancel_exact(ids)`` contract.  Use signature
    detection instead of a broad-cancel fallback so old adapters remain
    compatible and BingX still receives the required symbol.
    """

    cleaned = sorted(
        {clean_exchange_id(value) for value in order_ids if clean_exchange_id(value)}
    )
    if not cleaned:
        return {"success": True, "code": 0, "data": []}
    if _call_accepts_keyword(cancel_exact, "symbol"):
        return await cancel_exact(cleaned, symbol=symbol)
    return await cancel_exact(cleaned)


def _order_position_id(order: dict[str, Any]) -> str:
    raw = order.get("raw") if isinstance(order.get("raw"), dict) else {}
    return _clean_identity(order.get("positionId") or raw.get("positionId"))


async def _cleanup_stale_orders(
    adapter: Any,
    symbol: str,
    *,
    payload: dict[str, Any],
    attempts: int = 3,
    positions_context: _LifecyclePositionsContext | None = None,
    user_id: int = 0,
    exchange: str = "",
) -> dict[str, Any]:
    """Cancel only stale orders durably identified as belonging to this trade.

    Automatic symbol-wide cancel-all is intentionally forbidden.  Unrelated
    manual/external orders remain untouched and are reported separately.  If
    live orders exist but this execution has no exact durable identity, cleanup
    fails closed and the caller keeps the row in ``manual_required``.
    """

    tracked_algo_ids, tracked_position_ids = _saved_conditional_identity(payload or {})
    tracked_regular_ids = _saved_entry_order_ids(payload or {})
    entry_payload = (
        (payload or {}).get("entry")
        if isinstance((payload or {}).get("entry"), dict)
        else {}
    )
    opening_intent = (
        (payload or {}).get("entry_write_intent_v1")
        if isinstance((payload or {}).get("entry_write_intent_v1"), dict)
        else {}
    )
    opening_reconcile = (
        (payload or {}).get("opening_intent_reconciliation_v1")
        if isinstance((payload or {}).get("opening_intent_reconciliation_v1"), dict)
        else {}
    )
    expected_regular_entry = bool(
        tracked_regular_ids
        or entry_payload
        or opening_intent
        or int(_f(opening_reconcile.get("open_match_count"), 0.0)) > 0
    )
    expected_conditionals = bool(
        tracked_algo_ids
        or tracked_position_ids
        or (payload or {}).get("tp")
        or (payload or {}).get("post_fill_stop")
        or (payload or {}).get("stop_order")
        or (
            (payload or {}).get("be")
            if isinstance((payload or {}).get("be"), dict)
            else {}
        )
    )
    result: dict[str, Any] = {
        "mode": "exact_order_ids_only",
        "tracked_algo_ids": sorted(tracked_algo_ids),
        "tracked_position_ids": sorted(tracked_position_ids),
        "tracked_regular_ids": sorted(tracked_regular_ids),
        "conditional_cancelled": False,
        "regular_cancelled": False,
        "verified_clean": False,
        "identity_missing": False,
        "attempts": 0,
        "errors": [],
        "remaining_algo_orders": [],
        "remaining_open_orders": [],
        "remaining_tracked_algo_ids": [],
        "remaining_tracked_regular_ids": [],
        "unrelated_algo_orders": [],
        "unrelated_open_orders": [],
        "unidentified_relevant_algo_orders": [],
        "unidentified_relevant_regular_orders": [],
    }

    async def _invalidate_positions_snapshot() -> None:
        if positions_context is not None and user_id and exchange:
            await positions_context.invalidate_positions(adapter, user_id, exchange)

    for attempt in range(1, max(1, int(attempts)) + 1):
        result["attempts"] = attempt
        verify_algo_ok = False
        verify_open_ok = False
        try:
            algo_rows = await _fetch_algo_orders(adapter, symbol)
            verify_algo_ok = True
        except Exception as exc:
            algo_rows = []
            result["errors"].append(
                f"attempt {attempt} read_algo: {type(exc).__name__}: {exc}"
            )
        try:
            open_rows = await _fetch_regular_orders(adapter, symbol)
            verify_open_ok = True
        except Exception as exc:
            open_rows = []
            result["errors"].append(
                f"attempt {attempt} read_open: {type(exc).__name__}: {exc}"
            )

        live_algo_map: dict[str, dict[str, Any]] = {}
        for order in algo_rows:
            for order_id in _live_algo_order_ids(order):
                live_algo_map[order_id] = order
        live_open_map: dict[str, dict[str, Any]] = {}
        for order in open_rows:
            for order_id in _live_regular_order_ids(order):
                live_open_map[order_id] = order

        tracked_live_algo = sorted(tracked_algo_ids.intersection(live_algo_map))
        tracked_live_regular = sorted(tracked_regular_ids.intersection(live_open_map))
        unrelated_algo = [
            row
            for row in algo_rows
            if not _live_algo_order_ids(row).intersection(tracked_algo_ids)
        ]
        unrelated_open = [
            row
            for row in open_rows
            if not _live_regular_order_ids(row).intersection(tracked_regular_ids)
        ]
        result["unrelated_algo_orders"] = unrelated_algo
        result["unrelated_open_orders"] = unrelated_open

        unidentified_relevant_algo: list[dict[str, Any]] = []
        if verify_algo_ok:
            if (
                expected_conditionals
                and not tracked_position_ids
                and not tracked_live_algo
                and algo_rows
            ):
                # Legacy snapshots can contain a delegated ``orderId`` but no
                # exact stop-plan/position identity. If any conditional order is
                # still live, ownership cannot be proven. Preserve everything and
                # require manual review instead of declaring cleanup complete.
                unidentified_relevant_algo = list(algo_rows)
            for row in algo_rows:
                if row in unidentified_relevant_algo:
                    continue
                row_ids = _live_algo_order_ids(row)
                if row_ids.intersection(tracked_algo_ids):
                    continue
                row_position_id = _order_position_id(row)
                if tracked_position_ids and row_position_id in tracked_position_ids:
                    unidentified_relevant_algo.append(row)
                elif (
                    tracked_position_ids
                    and not tracked_algo_ids
                    and not row_position_id
                ):
                    # This execution relies on position ownership, but the live
                    # row omits/corrupts positionId. Preserve it for manual review.
                    unidentified_relevant_algo.append(row)
                elif (
                    not tracked_algo_ids
                    and expected_conditionals
                    and not tracked_position_ids
                ):
                    # A legacy row says this execution owned conditional orders,
                    # but neither an exact plan id nor a position id survived.
                    # Never guess which live order is ours.
                    unidentified_relevant_algo.append(row)
        result["unidentified_relevant_algo_orders"] = unidentified_relevant_algo
        unidentified_relevant_regular: list[dict[str, Any]] = []
        if (
            verify_open_ok
            and expected_regular_entry
            and not tracked_regular_ids
            and open_rows
        ):
            # A legacy execution says an ENTRY existed, but no exact regular
            # order identity survived. Preserve every live regular order and
            # require manual review instead of falsely declaring cleanup clean.
            unidentified_relevant_regular = list(open_rows)
        result["unidentified_relevant_regular_orders"] = unidentified_relevant_regular
        if unidentified_relevant_algo or unidentified_relevant_regular:
            result["identity_missing"] = True

        if tracked_live_algo:
            cancel_exact = getattr(adapter, "cancel_conditional_orders_exact", None)
            if not callable(cancel_exact):
                result["errors"].append(
                    "adapter has no exact conditional-order cancel method"
                )
            else:
                await _invalidate_positions_snapshot()
                try:
                    result["conditional"] = await _cancel_exact_with_symbol(
                        cancel_exact, tracked_live_algo, symbol
                    )
                    result["conditional_cancelled"] = True
                except Exception as exc:
                    result["errors"].append(
                        f"attempt {attempt} conditional_exact: {type(exc).__name__}: {exc}"
                    )
                finally:
                    await _invalidate_positions_snapshot()
        else:
            result["conditional_cancelled"] = True

        if tracked_live_regular:
            cancel_exact = getattr(adapter, "cancel_regular_orders_exact", None)
            if not callable(cancel_exact):
                result["errors"].append(
                    "adapter has no exact regular-order cancel method"
                )
            else:
                await _invalidate_positions_snapshot()
                try:
                    result["regular"] = await _cancel_exact_with_symbol(
                        cancel_exact, tracked_live_regular, symbol
                    )
                    result["regular_cancelled"] = True
                except Exception as exc:
                    result["errors"].append(
                        f"attempt {attempt} regular_exact: {type(exc).__name__}: {exc}"
                    )
                finally:
                    await _invalidate_positions_snapshot()
        else:
            result["regular_cancelled"] = True

            # Re-read after the exact writes.  An ambiguous cancel outcome is
            # resolved by observation instead of a blind duplicate write.
        if tracked_live_algo or tracked_live_regular:
            await asyncio.sleep(0.25)
        try:
            remaining_algo = await _fetch_algo_orders(adapter, symbol)
            verify_algo_ok = True
        except Exception as exc:
            remaining_algo = []
            verify_algo_ok = False
            result["errors"].append(
                f"attempt {attempt} verify_algo: {type(exc).__name__}: {exc}"
            )
        try:
            remaining_open = await _fetch_regular_orders(adapter, symbol)
            verify_open_ok = True
        except Exception as exc:
            remaining_open = []
            verify_open_ok = False
            result["errors"].append(
                f"attempt {attempt} verify_open: {type(exc).__name__}: {exc}"
            )

        remaining_algo_ids = (
            set().union(*(_live_algo_order_ids(row) for row in remaining_algo))
            if remaining_algo
            else set()
        )
        remaining_open_ids = (
            set().union(*(_live_regular_order_ids(row) for row in remaining_open))
            if remaining_open
            else set()
        )
        remaining_tracked_algo = sorted(
            tracked_algo_ids.intersection(remaining_algo_ids)
        )
        remaining_tracked_regular = sorted(
            tracked_regular_ids.intersection(remaining_open_ids)
        )
        result["remaining_algo_orders"] = remaining_algo
        result["remaining_open_orders"] = remaining_open
        result["remaining_tracked_algo_ids"] = remaining_tracked_algo
        result["remaining_tracked_regular_ids"] = remaining_tracked_regular
        result["verify_algo_ok"] = verify_algo_ok
        result["verify_open_ok"] = verify_open_ok
        if verify_algo_ok and not remaining_tracked_algo:
            # A lost cancel response is safely resolved by the authoritative
            # post-write read.  Do not repeat the write and do not force manual
            # review solely because the HTTP response was ambiguous.
            if tracked_live_algo and not result["conditional_cancelled"]:
                result["conditional_cancel_resolved_by_observation"] = True
            result["conditional_cancelled"] = True
        if verify_open_ok and not remaining_tracked_regular:
            if tracked_live_regular and not result["regular_cancelled"]:
                result["regular_cancel_resolved_by_observation"] = True
            result["regular_cancelled"] = True
        result["unrelated_algo_orders"] = [
            row
            for row in remaining_algo
            if not _live_algo_order_ids(row).intersection(tracked_algo_ids)
        ]
        result["unrelated_open_orders"] = [
            row
            for row in remaining_open
            if not _live_regular_order_ids(row).intersection(tracked_regular_ids)
        ]

        remaining_unidentified_relevant: list[dict[str, Any]] = []
        if (
            expected_conditionals
            and not tracked_position_ids
            and not remaining_tracked_algo
            and remaining_algo
        ):
            remaining_unidentified_relevant = list(remaining_algo)
        for row in remaining_algo:
            if row in remaining_unidentified_relevant:
                continue
            row_ids = _live_algo_order_ids(row)
            if row_ids.intersection(tracked_algo_ids):
                continue
            row_position_id = _order_position_id(row)
            if tracked_position_ids and row_position_id in tracked_position_ids:
                remaining_unidentified_relevant.append(row)
            elif tracked_position_ids and not tracked_algo_ids and not row_position_id:
                remaining_unidentified_relevant.append(row)
            elif (
                not tracked_algo_ids
                and expected_conditionals
                and not tracked_position_ids
            ):
                remaining_unidentified_relevant.append(row)
        result["unidentified_relevant_algo_orders"] = remaining_unidentified_relevant
        remaining_unidentified_regular: list[dict[str, Any]] = []
        if (
            verify_open_ok
            and expected_regular_entry
            and not tracked_regular_ids
            and remaining_open
        ):
            remaining_unidentified_regular = list(remaining_open)
        result["unidentified_relevant_regular_orders"] = remaining_unidentified_regular
        if remaining_unidentified_relevant or remaining_unidentified_regular:
            result["identity_missing"] = True

        no_tracked_remaining = (
            not remaining_tracked_algo and not remaining_tracked_regular
        )
        no_unidentified_hazard = (
            not remaining_unidentified_relevant and not remaining_unidentified_regular
        )
        if (
            verify_algo_ok
            and verify_open_ok
            and no_tracked_remaining
            and no_unidentified_hazard
        ):
            result["verified_clean"] = True
            return result
        if attempt < max(1, int(attempts)):
            await asyncio.sleep(0.25)

    return result


async def _reconcile_manual_zero_cleanup(
    *,
    row: dict[str, Any],
    payload: dict[str, Any],
    adapter: Any,
    positions_context: _LifecyclePositionsContext,
    exchange: str,
    write_status: Any,
) -> tuple[bool, int]:
    """Resolve old zero-position manual rows without touching unknown orders.

    The normal lifecycle path performs TP/history work before stale-order cleanup.
    Old ``manual_required`` rows from earlier releases can therefore spend every
    critical pass rebuilding the same zero candidate and never reach the exact
    cleanup stage. This focused path is intentionally narrow: it runs only for
    cleanup/replacement classifications, proves zero exposure with independent
    private reads, cancels exact bot-owned ids only, and persists every ambiguous
    order for manual review.
    """

    if str(row.get("status") or "").strip().lower() != "manual_required":
        return False, 0
    classification = db.critical_manual_backoff_classification(row)
    classification_reason = str(classification.get("reason") or "")
    if classification_reason not in {
        "cleanup_unresolved",
        "be_replacement_in_progress",
        "unknown_stop_or_be_protection",
    }:
        return False, 0
    now_utc = datetime.now(timezone.utc)
    if not db.critical_cleanup_reconcile_due(row, now=now_utc):
        return True, 0

    execution_id = int(row.get("id") or 0)
    user_id = int(row.get("user_id") or 0)
    symbol = str(row.get("symbol") or "").upper()
    side = str(row.get("side") or "").lower()
    lifecycle = (
        payload.get("lifecycle") if isinstance(payload.get("lifecycle"), dict) else {}
    )

    # The first read is always fresh. A cached zero must never authorize order
    # cleanup or the retirement of a durable BE replacement checkpoint.
    try:
        positions = await positions_context.get_positions(
            adapter, user_id, exchange, symbol, force_refresh=True
        )
    except Exception as exc:
        proof_confirmed = bool(classification.get("zero_proof_confirmed"))
        next_attempt = (
            _manual_cleanup_next_attempt(payload, now=now_utc)
            if proof_confirmed
            else None
        )
        marker = _manual_cleanup_reconcile_marker(
            payload,
            now=now_utc,
            state="pending_retry" if proof_confirmed else "api_error_no_zero_proof",
            reason=f"positions read failed: {type(exc).__name__}: {str(exc)[:300]}",
            next_attempt_at=next_attempt,
        )
        await write_status(
            "manual_required",
            str(row.get("reason") or ""),
            {
                "lifecycle": {
                    "cleanup_deferred": True,
                    "cleanup_deferred_reason": "fresh position read failed before cleanup probe",
                },
                "critical_cleanup_reconcile_v1": marker,
            },
        )
        log.warning(
            "MANUAL_CLEANUP_RECONCILE execution_id=%s user_id=%s symbol=%s side=%s state=%s next_attempt_at=%s error=%s",
            execution_id,
            user_id,
            symbol,
            side,
            marker.get("state"),
            next_attempt.isoformat() if next_attempt else None,
            f"{type(exc).__name__}: {str(exc)[:300]}",
        )
        return True, 0
    same_side = _filter_positions_by_side(positions, side)
    same_qty = _total_position_size(same_side)
    any_qty = _total_position_size(positions)
    if _position_qty_is_live(same_qty) or _position_qty_is_live(any_qty):
        invalidated = _critical_zero_invalidated_patch(
            now=now_utc,
            same_side_qty=same_qty,
            any_position_qty=any_qty,
            reason=(
                "live_position_observed"
                if _position_qty_is_live(same_qty)
                else "opposite_or_unknown_position_observed"
            ),
        )
        marker = _manual_cleanup_reconcile_marker(
            payload,
            now=now_utc,
            state="blocked_live_position",
            reason="fresh BingX position read is non-zero",
        )
        live_saved = await db.merge_execution_metadata(
            execution_id,
            {
                "lifecycle": {
                    "position_qty": same_qty,
                    "any_position_qty": any_qty,
                    "opposite_or_unknown_position_detected": bool(
                        _position_qty_is_live(any_qty)
                        and not _position_qty_is_live(same_qty)
                    ),
                    "cleanup_deferred": True,
                    "cleanup_deferred_reason": "live position observed before cleanup probe",
                },
                "critical_zero_exposure_v1": invalidated,
                "critical_cleanup_reconcile_v1": marker,
            },
            expected_status="manual_required",
        )
        if not live_saved:
            raise StaleExecutionPass(
                source="position_lifecycle_guard.manual_cleanup_live",
                execution_id=execution_id,
                expected_status="manual_required",
                attempted_status="merge_live_cleanup_block",
            )
        log.warning(
            "MANUAL_CLEANUP_RECONCILE execution_id=%s user_id=%s symbol=%s side=%s state=blocked_live_position same_qty=%.12g any_qty=%.12g",
            execution_id,
            user_id,
            symbol,
            side,
            same_qty,
            any_qty,
        )
        return True, 0

    confirm_after = max(
        3.0,
        float(getattr(get_settings(), "MONITOR_CRITICAL_INTERVAL_SEC", 5) or 5),
    )
    proof = (
        payload.get("critical_zero_exposure_v1")
        if isinstance(payload.get("critical_zero_exposure_v1"), dict)
        else {}
    )
    first_zero_at = _parse_utc_datetime(
        proof.get("first_zero_at")
    ) or _parse_utc_datetime(lifecycle.get("zero_candidate_at"))
    proof_confirmed = bool(classification.get("zero_proof_confirmed"))
    if not proof_confirmed:
        if first_zero_at is None or proof.get("invalidated_at") not in (None, ""):
            first_patch = _critical_zero_first_patch(
                payload,
                now=now_utc,
                same_side_qty=0.0,
                any_position_qty=0.0,
            )
            next_attempt = now_utc + timedelta(seconds=confirm_after)
            marker = _manual_cleanup_reconcile_marker(
                payload,
                now=now_utc,
                state="waiting_zero_confirmation",
                reason="first fresh zero-position response recorded",
                next_attempt_at=next_attempt,
            )
            await write_status(
                "manual_required",
                str(row.get("reason") or ""),
                {
                    "lifecycle": {
                        "zero_candidate_at": now_utc.isoformat(),
                        "position_qty": 0.0,
                        "any_position_qty": 0.0,
                        "opposite_or_unknown_position_detected": False,
                    },
                    "critical_zero_exposure_v1": first_patch,
                    "critical_cleanup_reconcile_v1": marker,
                },
            )
            log.info(
                "MANUAL_CLEANUP_RECONCILE execution_id=%s user_id=%s symbol=%s side=%s state=waiting_zero_confirmation next_attempt_at=%s",
                execution_id,
                user_id,
                symbol,
                side,
                next_attempt.isoformat(),
            )
            return True, 0

        zero_age = max(
            0.0, (now_utc - first_zero_at.astimezone(timezone.utc)).total_seconds()
        )
        if zero_age < confirm_after:
            return True, 0
        confirmed_patch = _critical_zero_confirmed_patch(
            payload, now=now_utc, zero_seen_at=first_zero_at
        )
        saved = await db.merge_execution_metadata(
            execution_id,
            {
                "lifecycle": {
                    "position_qty": 0.0,
                    "any_position_qty": 0.0,
                    "opposite_or_unknown_position_detected": False,
                },
                "critical_zero_exposure_v1": confirmed_patch,
            },
            expected_status="manual_required",
        )
        if not saved:
            raise StaleExecutionPass(
                source="position_lifecycle_guard.manual_cleanup_zero_proof",
                execution_id=execution_id,
                expected_status="manual_required",
                attempted_status="merge_zero_exposure_proof",
            )
        payload["critical_zero_exposure_v1"] = confirmed_patch

    async with symbol_action_lock(user_id, symbol):
        latest_row = await db.get_execution_by_id(execution_id)
        if not latest_row or str(latest_row.get("status") or "") != "manual_required":
            return True, 0
        latest_payload = db.execution_payload_dict(latest_row)
        if not latest_payload:
            latest_payload = dict(payload)

        # Final authorization read checks *all* same-symbol exposure, not only
        # the tracked side. The old path checked same-side qty here and could
        # miss an opposite position that appeared while waiting for the lock.
        await positions_context.invalidate_positions(adapter, user_id, exchange)
        try:
            final_positions_raw = list(await adapter.fetch_open_positions(symbol) or [])
        except Exception as exc:
            final_now = datetime.now(timezone.utc)
            next_attempt = _manual_cleanup_next_attempt(latest_payload, now=final_now)
            marker = _manual_cleanup_reconcile_marker(
                latest_payload,
                now=final_now,
                state="pending_retry",
                reason=f"locked positions read failed: {type(exc).__name__}: {str(exc)[:300]}",
                next_attempt_at=next_attempt,
            )
            await write_status(
                "manual_required",
                str(row.get("reason") or ""),
                {
                    "lifecycle": {
                        "cleanup_deferred": True,
                        "cleanup_deferred_reason": "locked final position read failed",
                    },
                    "critical_cleanup_reconcile_v1": marker,
                },
            )
            log.warning(
                "MANUAL_CLEANUP_RECONCILE execution_id=%s user_id=%s symbol=%s side=%s state=pending_retry next_attempt_at=%s error=%s",
                execution_id,
                user_id,
                symbol,
                side,
                next_attempt.isoformat(),
                f"{type(exc).__name__}: {str(exc)[:300]}",
            )
            return True, 0
        final_positions = _filter_positions_by_symbol(final_positions_raw, symbol)
        final_same_qty = _total_position_size(
            _filter_positions_by_side(final_positions, side)
        )
        final_any_qty = _total_position_size(final_positions)
        if _position_qty_is_live(final_same_qty) or _position_qty_is_live(
            final_any_qty
        ):
            final_now = datetime.now(timezone.utc)
            final_live_saved = await db.merge_execution_metadata(
                execution_id,
                {
                    "lifecycle": {
                        "position_qty": final_same_qty,
                        "any_position_qty": final_any_qty,
                        "opposite_or_unknown_position_detected": bool(
                            _position_qty_is_live(final_any_qty)
                            and not _position_qty_is_live(final_same_qty)
                        ),
                        "cleanup_deferred": True,
                        "cleanup_deferred_reason": "position appeared during locked cleanup authorization",
                    },
                    "critical_zero_exposure_v1": _critical_zero_invalidated_patch(
                        now=final_now,
                        same_side_qty=final_same_qty,
                        any_position_qty=final_any_qty,
                        reason=(
                            "live_position_observed"
                            if _position_qty_is_live(final_same_qty)
                            else "opposite_or_unknown_position_observed"
                        ),
                    ),
                    "critical_cleanup_reconcile_v1": _manual_cleanup_reconcile_marker(
                        latest_payload,
                        now=final_now,
                        state="blocked_live_position",
                        reason="position appeared during locked cleanup authorization",
                    ),
                },
                expected_status="manual_required",
            )
            if not final_live_saved:
                raise StaleExecutionPass(
                    source="position_lifecycle_guard.manual_cleanup_locked_live",
                    execution_id=execution_id,
                    expected_status="manual_required",
                    attempted_status="merge_locked_live_cleanup_block",
                )
            log.warning(
                "MANUAL_CLEANUP_RECONCILE execution_id=%s user_id=%s symbol=%s side=%s state=blocked_locked_position same_qty=%.12g any_qty=%.12g",
                execution_id,
                user_id,
                symbol,
                side,
                final_same_qty,
                final_any_qty,
            )
            return True, 0

        cleanup = await _cleanup_stale_orders(
            adapter,
            symbol,
            payload=latest_payload,
            attempts=1,
            positions_context=positions_context,
            user_id=user_id,
            exchange=exchange,
        )

    resolved_at = datetime.now(timezone.utc)
    be = latest_payload.get("be") if isinstance(latest_payload.get("be"), dict) else {}
    stop_diagnostic = (
        latest_payload.get("stop_diagnostic_v1")
        if isinstance(latest_payload.get("stop_diagnostic_v1"), dict)
        else {}
    )
    stale_be_replacement = bool(
        be.get("replacement_in_progress") is True
        or isinstance(be.get("replacement_write_intent_v1"), dict)
        and be.get("replacement_write_intent_v1")
    )
    stale_protection_diagnostic = bool(
        be.get("manual_required") is True
        or str(be.get("error") or "").strip()
        or stop_diagnostic.get("manual_required") is True
        or stop_diagnostic.get("active_stop_ids")
    )
    be_patch: dict[str, Any] = {}
    if stale_be_replacement or stale_protection_diagnostic:
        # With two independent zero-position reads, a final locked zero read and
        # verified-clean exact order reads there is no remaining position or
        # relevant order to protect. Retire only stale workflow/diagnostic state;
        # no exchange write is sent and unrelated/manual orders stay untouched.
        be_patch = {
            "replacement_in_progress": False,
            "replacement_write_intent_v1": None,
            "manual_required": False,
            "error": None,
            "recovery_status": "zero_position_cleanup_reconciled",
            "recovery_resolved_at": resolved_at.isoformat(),
        }

    if cleanup.get("verified_clean") is True:
        marker = _manual_cleanup_reconcile_marker(
            latest_payload,
            now=resolved_at,
            state="resolved",
            cleanup=cleanup,
            reason="zero exposure and exact stale-order cleanup verified",
        )
        patch: dict[str, Any] = {
            "lifecycle": {
                "closed_cleanup_done": True,
                "position_qty": 0.0,
                "any_position_qty": 0.0,
                "opposite_or_unknown_position_detected": False,
                "cleanup": cleanup,
                "cleanup_deferred": False,
                "cleanup_deferred_reason": None,
            },
            "critical_cleanup_reconcile_v1": marker,
        }
        if be_patch:
            patch["be"] = be_patch
        if stale_protection_diagnostic:
            patch["stop_diagnostic_v1"] = {
                "manual_required": False,
                "active_stop_count": 0,
                # ``None`` intentionally replaces a stale list under the DB
                # deep-merge rules; an empty list would be unioned with the old
                # ids and would leave the phantom protection blocker alive.
                "active_stop_ids": None,
                "resolved_at": resolved_at.isoformat(),
                "resolution": "zero_exposure_and_clean_orders_confirmed",
            }
            patch["stale_manual_protection_resolution_v1"] = {
                "version": 1,
                "resolved_at": resolved_at.isoformat(),
                "zero_exposure_confirmed": True,
                "cleanup_verified_clean": True,
                "retired_fields": [
                    name
                    for name, present in (
                        ("be.manual_required", be.get("manual_required") is True),
                        ("be.error", bool(str(be.get("error") or "").strip())),
                        (
                            "be.replacement_checkpoint",
                            stale_be_replacement,
                        ),
                        (
                            "stop_diagnostic_v1.manual_required",
                            stop_diagnostic.get("manual_required") is True,
                        ),
                        (
                            "stop_diagnostic_v1.active_stop_ids",
                            bool(stop_diagnostic.get("active_stop_ids")),
                        ),
                    )
                    if present
                ],
                "previous_be_error_code": classification.get("be_error_code"),
                "previous_be_error_fingerprint": classification.get(
                    "be_error_fingerprint"
                ),
            }
        await write_status(
            "manual_required",
            "manual review retained; zero exposure and exact order cleanup verified",
            patch,
        )
        log.info(
            "MANUAL_CLEANUP_RECONCILE execution_id=%s user_id=%s symbol=%s side=%s state=resolved tracked_algo=%s tracked_regular=%s",
            execution_id,
            user_id,
            symbol,
            side,
            len(cleanup.get("tracked_algo_ids") or []),
            len(cleanup.get("tracked_regular_ids") or []),
        )
        return True, 1

    unknown_hazard = _manual_cleanup_has_unknown_hazard(cleanup)
    next_attempt = (
        None
        if unknown_hazard
        else _manual_cleanup_next_attempt(latest_payload, now=resolved_at)
    )
    state = "blocked_unknown_orders" if unknown_hazard else "pending_retry"
    reason = (
        "unknown/manual or still-live exact orders preserved"
        if unknown_hazard
        else "authoritative cleanup read failed; retry scheduled"
    )
    marker = _manual_cleanup_reconcile_marker(
        latest_payload,
        now=resolved_at,
        state=state,
        cleanup=cleanup,
        reason=reason,
        next_attempt_at=next_attempt,
    )
    patch = {
        "lifecycle": {
            "closed_cleanup_done": False,
            "position_qty": 0.0,
            "any_position_qty": 0.0,
            "opposite_or_unknown_position_detected": False,
            "cleanup": cleanup,
            "cleanup_deferred": True,
            "cleanup_deferred_reason": reason,
        },
        "critical_cleanup_reconcile_v1": marker,
    }
    if be_patch:
        patch["be"] = be_patch
    await write_status("manual_required", str(row.get("reason") or ""), patch)
    log.warning(
        "MANUAL_CLEANUP_RECONCILE execution_id=%s user_id=%s symbol=%s side=%s state=%s next_attempt_at=%s tracked_algo_remaining=%s tracked_regular_remaining=%s unidentified_algo=%s unidentified_regular=%s errors=%s",
        execution_id,
        user_id,
        symbol,
        side,
        state,
        next_attempt.isoformat() if next_attempt else None,
        len(cleanup.get("remaining_tracked_algo_ids") or []),
        len(cleanup.get("remaining_tracked_regular_ids") or []),
        len(cleanup.get("unidentified_relevant_algo_orders") or []),
        len(cleanup.get("unidentified_relevant_regular_orders") or []),
        len(cleanup.get("errors") or []),
    )
    return True, 0


def _confirmed_manual_close_result(
    payload: dict[str, Any],
) -> dict[str, Any] | None:
    """Return the immutable result of a confirmed bot-driven manual close.

    The manual action persists both exact close evidence and the financial
    result before the lifecycle worker continues exact stale-order cleanup.
    A later ticker is not realized PnL and must never reprice that close.
    Malformed or contradictory metadata is ignored so normal fail-closed
    classification remains available.
    """
    manual_raw = payload.get("manual_position_close_v1")
    lifecycle_raw = payload.get("lifecycle")
    if not isinstance(manual_raw, dict) or manual_raw.get("confirmed") is not True:
        return None
    if (
        not isinstance(lifecycle_raw, dict)
        or lifecycle_raw.get("manual_bot_close") is not True
    ):
        return None
    close_raw = lifecycle_raw.get("close_result")
    if not isinstance(close_raw, dict):
        return None

    close_type = str(close_raw.get("close_type") or "").strip().lower()
    if close_type not in {"manual_bot_close", "manual_bot_close_price_unknown"}:
        return None
    outcome = str(close_raw.get("outcome") or "").strip().lower()
    if outcome not in {"win", "breakeven", "loss", "unknown"}:
        return None
    try:
        realized_pnl = float(close_raw.get("realized_pnl") or 0.0)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(realized_pnl):
        return None

    manual_price_confirmed = manual_raw.get("exit_price_confirmed") is True
    result_price_confirmed = close_raw.get("exit_price_confirmed") is True
    exit_price = _f(
        close_raw.get("exit_price"),
        _f(manual_raw.get("exit_price"), 0.0),
    )
    if close_type == "manual_bot_close":
        if not (manual_price_confirmed and result_price_confirmed and exit_price > 0):
            return None
    else:
        if manual_price_confirmed or result_price_confirmed:
            return None
        if outcome != "unknown" or abs(realized_pnl) > 1e-12:
            return None
        exit_price = 0.0

    return {
        "outcome": outcome,
        "realized_pnl": realized_pnl,
        "close_type": close_type,
        "tps_hit": int(close_raw.get("tps_hit") or 0),
        "total_tps": int(close_raw.get("total_tps") or 0),
        "current_price": exit_price,
        "exit_price": exit_price,
        "exit_price_source": str(
            close_raw.get("exit_price_source")
            or manual_raw.get("exit_price_source")
            or "unavailable"
        ),
        "exit_price_confirmed": manual_price_confirmed and result_price_confirmed,
        "preserved_manual_close": True,
        "message": "",
    }


async def _closed_position_result(
    *,
    adapter: Any,
    row: dict[str, Any],
    payload: dict[str, Any],
    symbol: str,
    side: str,
    original_qty: float,
    market_price: float | None = None,
    terminal_history_type: str = "",
) -> dict[str, Any]:
    """Build and classify the final result once the tracked position is gone."""
    preserved_manual = _confirmed_manual_close_result(payload)
    if preserved_manual is not None:
        return preserved_manual

    from app.services.dashboard import outcome_from_close
    from app.services.trade_notifications import (
        analyze_close_result,
        position_cleanup_message,
    )

    entry_px = _f(payload.get("actual_entry"), 0.0) or _f(row.get("entry"), 0.0)
    stop_px = _f(row.get("stop"), 0.0)
    try:
        targets_list = [float(x) for x in json.loads(row.get("targets_json") or "[]")]
    except Exception:
        targets_list = []
    be_info = payload.get("be") if isinstance(payload.get("be"), dict) else {}
    be_moved = be_info.get("moved") is True
    be_price = _f(be_info.get("stop"), 0.0)
    effective_stop = be_price if (be_moved and be_price > 0) else stop_px
    tp_payload = payload.get("tp") if isinstance(payload.get("tp"), list) else []
    # Exact terminal STOP history is stronger evidence than a later ticker
    # price. Use the saved STOP/BE price for classification and estimated PnL;
    # otherwise a fast move after the close could be mislabeled as unknown.
    current_px = (
        effective_stop
        if terminal_history_type
        in {
            "stop_history",
            "be_stop_probable_history_gap",
            "be_stop_history_timeout_fallback",
        }
        and effective_stop > 0
        else float(market_price or 0.0)
    )
    if current_px <= 0:
        try:
            current_px = float(await adapter.fetch_last_price(symbol))
        except Exception:
            current_px = 0.0

    analysis = analyze_close_result(
        side=side,
        entry=entry_px,
        stop=stop_px,
        targets=targets_list,
        original_qty=original_qty,
        qty_now=0.0,
        current_price=current_px,
        tp_orders_payload=tp_payload,
        be_moved=be_moved,
        be_stop_price=be_price,
    )
    outcome = outcome_from_close(
        close_type=str(analysis.get("close_type") or "unknown"),
        realized_pnl=float(analysis.get("total_pnl") or 0.0),
        be_was_set=be_moved,
    )
    n_targets = len(targets_list) or len([t for t in tp_payload if isinstance(t, dict)])
    message = position_cleanup_message(
        symbol=symbol,
        side=side,
        realized_pnl=float(analysis.get("total_pnl") or 0.0),
        tps_hit=int(analysis.get("tps_hit") or 0),
        total_tps=n_targets,
        be_was_set=be_moved,
        close_type=str(analysis.get("close_type") or "unknown"),
        entry=entry_px,
        exit_price=current_px,
        stop=be_price if be_moved and be_price > 0 else stop_px,
        qty=original_qty,
        created_at=row.get("created_at"),
    )
    return {
        "outcome": outcome,
        "realized_pnl": float(analysis.get("total_pnl") or 0.0),
        "close_type": str(analysis.get("close_type") or "unknown"),
        "tps_hit": int(analysis.get("tps_hit") or 0),
        "total_tps": n_targets,
        "current_price": current_px,
        "message": message,
    }


def _unique_live_position_id(positions: list[dict[str, Any]]) -> str:
    ids = {
        clean_exchange_id(pos.get("positionId"))
        for pos in positions or []
        if isinstance(pos, dict) and clean_exchange_id(pos.get("positionId"))
    }
    return next(iter(ids)) if len(ids) == 1 else ""


def _algo_position_id(order: dict[str, Any]) -> str:
    raw = order.get("raw") if isinstance(order.get("raw"), dict) else {}
    data = order.get("data") if isinstance(order.get("data"), dict) else {}
    return clean_exchange_id(
        order.get("positionId")
        or order.get("_confirmed_position_id")
        or raw.get("positionId")
        or data.get("positionId")
    )


def _algo_stop_id(order: dict[str, Any]) -> str:
    raw = order.get("raw") if isinstance(order.get("raw"), dict) else {}
    data = order.get("data") if isinstance(order.get("data"), dict) else {}
    return clean_exchange_id(
        order.get("stopPlanOrderId")
        or order.get("stopOrderId")
        or order.get("_confirmed_stop_plan_id")
        or order.get("_confirmed_order_id")
        or raw.get("stopPlanOrderId")
        or raw.get("id")
        or data.get("stopPlanOrderId")
        or data.get("id")
    )


def _algo_matches_side(order: dict[str, Any], side: str) -> bool:
    wanted = str(side or "").upper()
    if wanted not in {"LONG", "SHORT"}:
        return False
    row_side = str(order.get("positionSide") or order.get("side") or "").upper()
    if not row_side:
        return False
    accepted = {wanted, "SELL" if wanted == "LONG" else "BUY"}
    return row_side in accepted


def _algo_is_tp(order: dict[str, Any]) -> bool:
    return (
        _f(order.get("takeProfitPrice"), 0.0) > 0
        and _f(order.get("stopLossPrice"), 0.0) <= 0
    )


def _algo_is_stop(order: dict[str, Any]) -> bool:
    return (
        _f(order.get("stopLossPrice"), 0.0) > 0
        and _f(order.get("takeProfitPrice"), 0.0) <= 0
    )


def _algo_tp_price(order: dict[str, Any]) -> float:
    return _f(
        order.get("takeProfitPrice")
        or order.get("triggerPrice")
        or order.get("stopPrice"),
        0.0,
    )


def _algo_qty(order: dict[str, Any]) -> float:
    return _f(order.get("qty") or order.get("quantity"), 0.0)


def _safe_tp_index(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return 0
    return parsed if parsed > 0 else 0


def _residual_original_tps_complete(
    payload: dict[str, Any], tp_rows: list[dict[str, Any]]
) -> tuple[bool, dict[str, Any] | None]:
    snapshot = get_snapshot(payload)
    if not snapshot:
        return False, None
    items = snapshot.get("items")
    if not isinstance(items, list) or not items:
        return False, snapshot
    by_index = {
        index: row
        for row in tp_rows
        if isinstance(row, dict) and (index := _safe_tp_index(row.get("tp_index"))) > 0
    }
    for item in items:
        if not isinstance(item, dict):
            return False, snapshot
        index = _safe_tp_index(item.get("tp_index"))
        if index <= 0:
            return False, snapshot
        row = by_index.get(index)
        if (
            not row
            or row.get("filled") is not True
            or tp_row_has_unresolved_qty_conflict(row)
            or tp_row_has_unresolved_identity_conflict(row)
        ):
            return False, snapshot
    return True, snapshot


def _residual_recovery_due(state: Any, *, now: datetime) -> bool:
    if not isinstance(state, dict):
        return True
    if state.get("order_confirmed") is True or state.get("manual_required") is True:
        return False
    next_retry = _parse_utc_datetime(state.get("next_retry_at"))
    return next_retry is None or now >= next_retry


async def _reconcile_residual_and_stop_diagnostics(
    *,
    adapter: Any,
    notify: NotifyFn | None,
    execution_id: int,
    user_id: int,
    symbol: str,
    side: str,
    qty_now: float,
    positions: list[dict[str, Any]],
    observed_price: float,
    payload: dict[str, Any],
    tp_rows: list[dict[str, Any]],
    expected_status: str | None = None,
    positions_context: _LifecyclePositionsContext | None = None,
    exchange: str = "",
) -> tuple[bool, dict[str, Any]]:
    """Detect a one-lot TP remainder and diagnose duplicate STOPs safely.

    Unknown/manual orders are never cancelled.  A final residual TP is created
    only when the immutable original TP plan is fully filled, the live remainder
    is at most one executable lot, no TP (including untracked TP) is open for the
    exact position, and the final target is still ahead of market.
    """

    async def _invalidate_positions_snapshot() -> None:
        if positions_context is not None and exchange:
            await positions_context.invalidate_positions(adapter, user_id, exchange)

    now = datetime.now(timezone.utc)
    normalized_side = str(side or "").strip().lower()
    qty_now = _f(qty_now, 0.0)
    position_id = _unique_live_position_id(positions)
    if not position_id or normalized_side not in {"long", "short"} or qty_now <= 0:
        return False, {}
    try:
        algo_orders = list(await adapter.fetch_open_algo_orders(symbol) or [])
    except Exception as exc:
        return False, {
            "residual_position_v1": {
                "last_checked_at": now.isoformat(),
                "check_error": f"{type(exc).__name__}: {exc}"[:1000],
            }
        }

    relevant = [
        order
        for order in algo_orders
        if isinstance(order, dict)
        and _algo_matches_side(order, side)
        and _algo_position_id(order) == position_id
    ]
    stops = [order for order in relevant if _algo_is_stop(order)]
    tps = [order for order in relevant if _algo_is_tp(order)]
    stop_ids = sorted({_algo_stop_id(order) for order in stops if _algo_stop_id(order)})
    diagnostic: dict[str, Any] = {
        "checked_at": now.isoformat(),
        "position_id": position_id,
        "active_stop_count": len(stops),
        "active_stop_ids": stop_ids,
        "active_tp_count": len(tps),
    }
    previous_diag = payload.get("stop_diagnostic_v1")
    notify_double_stop = len(stops) > 1 and not (
        isinstance(previous_diag, dict)
        and previous_diag.get("active_stop_ids") == stop_ids
        and bool(previous_diag.get("notified"))
    )
    if notify_double_stop:
        await _notify_critical(
            notify,
            user_id,
            card(
                "⚠️ <b>ОБНАРУЖЕНО НЕСКОЛЬКО STOP</b>",
                symbol=symbol,
                side=side,
                blocks=(
                    [
                        f"🛡 Активных STOP: <b>{len(stops)}</b>",
                        f"🧾 positionId: <code>{position_id}</code>",
                    ],
                    [
                        "Бот не удаляет неизвестный STOP автоматически, чтобы не затронуть ручную защиту.",
                        "Проверьте STOP на BingX вручную.",
                    ],
                ),
            ),
            event_key=f"execution:{execution_id}:duplicate_stops:{', '.join(stop_ids) or len(stops)}",
        )
        diagnostic["notified"] = True
    elif (
        isinstance(previous_diag, dict)
        and previous_diag.get("active_stop_ids") == stop_ids
    ):
        diagnostic["notified"] = bool(previous_diag.get("notified"))

    patch: dict[str, Any] = {"stop_diagnostic_v1": diagnostic}
    complete, snapshot = _residual_original_tps_complete(payload, tp_rows)
    if not complete or not snapshot:
        return False, patch

    state = payload.get("residual_position_v1")
    state_dict = state if isinstance(state, dict) else {}
    if bool(state_dict.get("order_confirmed")):
        saved_order = (
            state_dict.get("order") if isinstance(state_dict.get("order"), dict) else {}
        )
        saved_order_id = _algo_stop_id(saved_order) or clean_exchange_id(
            state_dict.get("order_id")
        )
        saved_position_id = clean_exchange_id(state_dict.get("position_id"))
        if not saved_order_id or saved_position_id != position_id:
            patch["residual_position_v1"] = {
                **state_dict,
                "last_checked_at": now.isoformat(),
                "status": "confirmed_order_identity_corrupt",
                "manual_required": True,
            }
            return False, patch
        matching_live = [
            order for order in tps if _algo_stop_id(order) == saved_order_id
        ]
        if len(matching_live) == 1:
            patch["residual_position_v1"] = {
                **state_dict,
                "last_checked_at": now.isoformat(),
                "status": "order_confirmed_live",
                "missing_live_checks": 0,
            }
            return False, patch
        try:
            raw_missing_checks = state_dict.get("missing_live_checks")
            missing_checks = (
                int(raw_missing_checks)
                if raw_missing_checks not in (None, "")
                and not isinstance(raw_missing_checks, bool)
                else 0
            )
        except (TypeError, ValueError, OverflowError):
            missing_checks = 0
        missing_checks = max(0, missing_checks) + 1
        status_name = (
            "confirmed_order_temporarily_missing"
            if missing_checks < 2
            else "confirmed_order_missing_live_position"
        )
        updated = {
            **state_dict,
            "last_checked_at": now.isoformat(),
            "status": status_name,
            "missing_live_checks": missing_checks,
            "manual_required": missing_checks >= 2,
            "active_tp_ids": sorted(
                {_algo_stop_id(order) for order in tps if _algo_stop_id(order)}
            ),
        }
        patch["residual_position_v1"] = updated
        if missing_checks >= 2 and not bool(state_dict.get("missing_notified")):
            await _notify_critical(
                notify,
                user_id,
                card(
                    "🚨 <b>ФИНАЛЬНЫЙ TP ОСТАТКА НЕ НАЙДЕН</b>",
                    symbol=symbol,
                    side=side,
                    blocks=(
                        [f"Ожидаемый TP ID: <code>{saved_order_id}</code>"],
                        [
                            "Позиция всё ещё открыта. Повторный TP не создан, чтобы исключить дубликат."
                        ],
                    ),
                ),
                event_key=f"execution:{execution_id}:residual_confirmed_missing",
            )
            updated["missing_notified"] = True
        return False, patch
    if tps:
        expected_target = _f(state_dict.get("target"), 0.0)
        expected_qty = _f(state_dict.get("qty"), 0.0)
        price_tol = max(abs(expected_target) * 1e-9, 1e-12)
        qty_tol = max(abs(expected_qty) * 1e-9, 1e-12)
        exact_existing = [
            order
            for order in tps
            if expected_target > 0
            and expected_qty > 0
            and abs(_algo_tp_price(order) - expected_target) <= price_tol
            and abs(_algo_qty(order) - expected_qty) <= qty_tol
            and _algo_stop_id(order)
        ]
        if (
            str(state_dict.get("status") or "") == "write_intent"
            and len(exact_existing) == 1
        ):
            new_index = (
                max([_safe_tp_index(row.get("tp_index")) for row in tp_rows] or [0]) + 1
            )
            tp_rows.append(
                {
                    "tp_index": new_index,
                    "target": expected_target,
                    "qty": expected_qty,
                    "planned_qty": expected_qty,
                    "order": exact_existing[0],
                    "filled": False,
                    "filled_notified": False,
                    "notification_pending": False,
                    "notify_attempts": 0,
                    "residual_recovery_v1": True,
                    "canonical_source": "lifecycle:residual_restart_adoption",
                    "created_at": now.isoformat(),
                }
            )
            patch["residual_position_v1"] = {
                **state_dict,
                "status": "order_confirmed_after_restart",
                "order_confirmed": True,
                "confirmed_at": now.isoformat(),
                "tp_index": new_index,
                "order": exact_existing[0],
            }
            patch["tp"] = tp_rows
            return True, patch
        patch["residual_position_v1"] = {
            **state_dict,
            "last_checked_at": now.isoformat(),
            "position_id": position_id,
            "qty": qty_now,
            "status": "live_tp_already_exists_unowned",
            "manual_required": True,
            "active_tp_ids": sorted(
                {_algo_stop_id(order) for order in tps if _algo_stop_id(order)}
            ),
        }
        return False, patch

    try:
        info = await adapter.instrument_info(symbol)
        qty_step = _f(getattr(info, "qty_step", 0.0), 0.0)
    except Exception as exc:
        patch["residual_position_v1"] = {
            **(state if isinstance(state, dict) else {}),
            "last_checked_at": now.isoformat(),
            "status": "instrument_info_unavailable",
            "error": f"{type(exc).__name__}: {exc}"[:1000],
        }
        return False, patch
    tolerance = max(qty_step * 1e-6, qty_now * 1e-9, 1e-12)
    if qty_step <= 0 or qty_now > qty_step + tolerance:
        manual = {
            **(state if isinstance(state, dict) else {}),
            "last_checked_at": now.isoformat(),
            "position_id": position_id,
            "qty": qty_now,
            "qty_step": qty_step,
            "status": "residual_too_large_for_one_lot_repair",
            "manual_required": True,
        }
        patch["residual_position_v1"] = manual
        if not (isinstance(state, dict) and bool(state.get("manual_notified"))):
            await _notify_critical(
                notify,
                user_id,
                card(
                    "🚨 <b>ОСТАТОК ПОЗИЦИИ ПОСЛЕ TP</b>",
                    symbol=symbol,
                    side=side,
                    blocks=(
                        [
                            f"Остаток: <b>{fmt_qty(qty_now)}</b>",
                            f"Минимальный шаг: <b>{fmt_qty(qty_step)}</b>",
                        ],
                        [
                            "Автоматическое восстановление не выполнено: остаток больше одного минимального лота."
                        ],
                    ),
                ),
                event_key=f"execution:{execution_id}:residual_manual",
            )
            manual["manual_notified"] = True
        return False, patch

    items = [item for item in snapshot.get("items") or [] if isinstance(item, dict)]
    valid_items = [item for item in items if _safe_tp_index(item.get("tp_index")) > 0]
    if not valid_items:
        return False, patch
    final_item = max(valid_items, key=lambda item: _safe_tp_index(item.get("tp_index")))
    target = _f(final_item.get("price"), 0.0)
    if target <= 0:
        return False, patch
    current = _f(observed_price, 0.0)
    if current <= 0:
        try:
            current = _f(await adapter.fetch_last_price(symbol), 0.0)
        except Exception:
            current = 0.0
    if current <= 0:
        patch["residual_position_v1"] = {
            **(state if isinstance(state, dict) else {}),
            "last_checked_at": now.isoformat(),
            "position_id": position_id,
            "qty": qty_now,
            "qty_step": qty_step,
            "target": target,
            "status": "market_price_unavailable",
            "manual_required": False,
        }
        return False, patch
    target_passed = (normalized_side == "long" and current >= target) or (
        normalized_side == "short" and current <= target
    )
    if target_passed:
        close_fn = getattr(adapter, "emergency_close_market_confirmed", None)
        base_state = {
            **(state if isinstance(state, dict) else {}),
            "last_checked_at": now.isoformat(),
            "position_id": position_id,
            "qty": qty_now,
            "qty_step": qty_step,
            "target": target,
            "market_price": current,
        }
        if not callable(close_fn):
            manual = {
                **base_state,
                "status": "final_target_passed_close_unavailable",
                "manual_required": True,
            }
            patch["residual_position_v1"] = manual
            if not (isinstance(state, dict) and bool(state.get("manual_notified"))):
                await _notify_critical(
                    notify,
                    user_id,
                    card(
                        "🚨 <b>ОСТАТОК ПОЗИЦИИ ПОСЛЕ ВСЕХ TP</b>",
                        symbol=symbol,
                        side=side,
                        blocks=(
                            [
                                f"Остаток: <b>{fmt_qty(qty_now)}</b>",
                                f"Последняя цель: <b>{target}</b>",
                                f"Текущая цена: <b>{current}</b>",
                            ],
                            [
                                "Последняя цель уже пройдена, но adapter не поддерживает подтверждённое MARKET-закрытие остатка."
                            ],
                        ),
                    ),
                    event_key=f"execution:{execution_id}:residual_target_passed",
                )
                manual["manual_notified"] = True
            return False, patch

        if not _residual_recovery_due(state, now=now):
            return False, patch
        attempts = int(state.get("attempts") or 0) if isinstance(state, dict) else 0
        if attempts >= 1:
            unresolved = {
                **base_state,
                "status": "market_close_intent_unresolved",
                "manual_required": True,
            }
            patch["residual_position_v1"] = unresolved
            if not (isinstance(state, dict) and bool(state.get("manual_notified"))):
                await _notify_critical(
                    notify,
                    user_id,
                    card(
                        "🚨 <b>ТРЕБУЕТСЯ ПРОВЕРИТЬ ОСТАТОК ПОЗИЦИИ</b>",
                        symbol=symbol,
                        side=side,
                        blocks=(
                            [f"Остаток: <b>{fmt_qty(qty_now)}</b>"],
                            [
                                "Ранее был сохранён intent закрытия остатка, но финальное подтверждение не найдено. Повторный MARKET-close не отправлен, чтобы исключить дубль."
                            ],
                        ),
                    ),
                    event_key=f"execution:{execution_id}:residual_market_close_unresolved",
                )
                unresolved["manual_notified"] = True
            return False, patch

        intent = {
            **base_state,
            "version": 1,
            "status": "market_close_intent",
            "manual_required": False,
            "attempts": attempts + 1,
            "intent_at": now.isoformat(),
            "active_stop_ids_before_close": stop_ids,
        }
        checkpoint_saved = await db.merge_execution_metadata(
            execution_id,
            {"residual_position_v1": intent},
            expected_status=expected_status,
        )
        if not checkpoint_saved:
            patch["residual_position_v1"] = {
                **intent,
                "status": "market_close_intent_checkpoint_failed",
                "failed_at": datetime.now(timezone.utc).isoformat(),
            }
            return False, patch

        await _invalidate_positions_snapshot()
        try:
            close_result = await close_fn(
                symbol=symbol,
                side=normalized_side,
                qty=qty_now,
                client_id=f"residual-close-{execution_id}",
                position_id=position_id,
            )
        except Exception as exc:
            await _invalidate_positions_snapshot()
            failed = {
                **intent,
                "status": "market_close_failed_manual_review",
                "manual_required": True,
                "error": f"{type(exc).__name__}: {exc}"[:1000],
                "failed_at": datetime.now(timezone.utc).isoformat(),
            }
            patch["residual_position_v1"] = failed
            await _notify_critical(
                notify,
                user_id,
                card(
                    "🚨 <b>НЕ УДАЛОСЬ ЗАКРЫТЬ ОСТАТОК ПОСЛЕ ВСЕХ TP</b>",
                    symbol=symbol,
                    side=side,
                    blocks=(
                        [f"Остаток: <b>{fmt_qty(qty_now)}</b>"],
                        [details_line(failed["error"])],
                    ),
                ),
                event_key=f"execution:{execution_id}:residual_market_close_failed",
            )
            return False, patch

        await _invalidate_positions_snapshot()
        confirmed = bool((close_result or {}).get("confirmed"))
        after_qty = _f((close_result or {}).get("after_qty"), qty_now)
        close_patch = {
            **intent,
            "status": "market_close_confirmed"
            if confirmed
            else "market_close_unconfirmed_manual_review",
            "manual_required": not confirmed,
            "confirmed": confirmed,
            "confirmed_at": datetime.now(timezone.utc).isoformat()
            if confirmed
            else None,
            "close_result": close_result,
            "after_qty": after_qty,
        }
        if confirmed and stop_ids:
            cancel_exact = getattr(adapter, "cancel_conditional_orders_exact", None)
            if callable(cancel_exact):
                await _invalidate_positions_snapshot()
                try:
                    close_patch["stop_cleanup"] = await _cancel_exact_with_symbol(
                        cancel_exact, stop_ids, symbol
                    )
                except Exception as exc:
                    close_patch["stop_cleanup_error"] = f"{type(exc).__name__}: {exc}"[
                        :1000
                    ]
                finally:
                    await _invalidate_positions_snapshot()
            else:
                close_patch["stop_cleanup_skipped"] = (
                    "adapter_has_no_exact_conditional_cancel"
                )
        patch["residual_position_v1"] = close_patch
        await _notify_critical(
            notify,
            user_id,
            card(
                (
                    "✅ <b>ОСТАТОК ПОСЛЕ ВСЕХ TP ЗАКРЫТ</b>"
                    if confirmed
                    else "🚨 <b>ОСТАТОК ПОСЛЕ ВСЕХ TP ТРЕБУЕТ ПРОВЕРКИ</b>"
                ),
                symbol=symbol,
                side=side,
                blocks=(
                    [
                        f"Остаток: <b>{fmt_qty(qty_now)}</b>",
                        f"Последняя цель: <b>{target}</b>",
                        f"Текущая цена: <b>{current}</b>",
                    ],
                    [
                        "Бот закрыл один минимальный остаточный лот MARKET после подтверждения, что все исходные TP уже выполнены и последняя цель пройдена."
                        if confirmed
                        else "MARKET-close был отправлен, но уменьшение позиции не подтверждено read-back. Проверьте позицию на BingX."
                    ],
                ),
            ),
            event_key=f"execution:{execution_id}:residual_market_close",
        )
        return confirmed, patch

    if not _residual_recovery_due(state, now=now):
        return False, patch
    attempts = int(state.get("attempts") or 0) if isinstance(state, dict) else 0
    if attempts >= 1:
        unresolved = {
            **(state if isinstance(state, dict) else {}),
            "last_checked_at": now.isoformat(),
            "position_id": position_id,
            "qty": qty_now,
            "qty_step": qty_step,
            "target": target,
            "status": "write_intent_unresolved_no_live_tp",
            "manual_required": True,
        }
        patch["residual_position_v1"] = unresolved
        if not (isinstance(state, dict) and bool(state.get("manual_notified"))):
            await _notify_critical(
                notify,
                user_id,
                card(
                    "🚨 <b>ТРЕБУЕТСЯ ПРОВЕРИТЬ ФИНАЛЬНЫЙ TP</b>",
                    symbol=symbol,
                    side=side,
                    blocks=(
                        [
                            f"Остаток: <b>{fmt_qty(qty_now)}</b>",
                            f"Цель: <b>{target}</b>",
                        ],
                        [
                            "Перед перезапуском был сохранён TP write-intent, но точный ордер не найден.",
                            "Повторный TP не отправлен, чтобы исключить дубликат.",
                        ],
                    ),
                ),
                event_key=f"execution:{execution_id}:residual_intent_unresolved",
            )
            unresolved["manual_notified"] = True
        return False, patch
    intent = {
        "version": 1,
        "status": "write_intent",
        "position_id": position_id,
        "qty": qty_now,
        "qty_step": qty_step,
        "target": target,
        "attempts": attempts + 1,
        "intent_at": now.isoformat(),
    }
    checkpoint_saved = await db.merge_execution_metadata(
        execution_id,
        {"residual_position_v1": intent},
        expected_status=expected_status,
    )
    if not checkpoint_saved:
        patch["residual_position_v1"] = {
            **intent,
            "status": "intent_checkpoint_failed",
            "manual_required": False,
            "failed_at": datetime.now(timezone.utc).isoformat(),
        }
        return False, patch
    await _invalidate_positions_snapshot()
    try:
        order = await adapter.create_take_profit(
            symbol=symbol,
            side=side,
            qty=qty_now,
            price=target,
            client_id=f"residual-{execution_id}",
            position_id=position_id,
            adopt_existing=False,
        )
    except Exception as exc:
        await _invalidate_positions_snapshot()
        intent.update(
            {
                "status": "write_failed_manual_review",
                "manual_required": True,
                "error": f"{type(exc).__name__}: {exc}"[:1000],
                "failed_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        patch["residual_position_v1"] = intent
        await _notify_critical(
            notify,
            user_id,
            card(
                "🚨 <b>НЕ УДАЛОСЬ ВОССТАНОВИТЬ ФИНАЛЬНЫЙ TP</b>",
                symbol=symbol,
                side=side,
                blocks=(
                    [
                        f"Остаток: <b>{fmt_qty(qty_now)}</b>",
                        f"Цель: <b>{target}</b>",
                    ],
                    [details_line(intent["error"])],
                ),
            ),
            event_key=f"execution:{execution_id}:residual_tp_failed",
        )
        return False, patch

    await _invalidate_positions_snapshot()
    new_index = max([_safe_tp_index(row.get("tp_index")) for row in tp_rows] or [0]) + 1
    tp_rows.append(
        {
            "tp_index": new_index,
            "target": target,
            "qty": qty_now,
            "planned_qty": qty_now,
            "order": order,
            "filled": False,
            "filled_notified": False,
            "notification_pending": False,
            "notify_attempts": 0,
            "residual_recovery_v1": True,
            "canonical_source": "lifecycle:residual_final_tp",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    intent.update(
        {
            "status": "order_confirmed",
            "order_confirmed": True,
            "confirmed_at": datetime.now(timezone.utc).isoformat(),
            "tp_index": new_index,
            "order": order,
        }
    )
    patch["residual_position_v1"] = intent
    patch["tp"] = tp_rows
    await _notify_critical(
        notify,
        user_id,
        card(
            "🧹 <b>ВОССТАНОВЛЕН ФИНАЛЬНЫЙ TP ДЛЯ ОСТАТКА</b>",
            symbol=symbol,
            side=side,
            blocks=(
                [f"Остаток: <b>{fmt_qty(qty_now)}</b>", f"Цель: <b>{target}</b>"],
                [
                    "Позиция не закрывалась MARKET. Создан только reduce-only TP на последний уровень."
                ],
            ),
        ),
        event_key=f"execution:{execution_id}:residual_tp_created",
    )
    return True, patch


async def process_position_lifecycle_guard_once(
    notify: NotifyFn | None = None,
    *,
    rows_override: list[dict[str, Any]] | None = None,
    market_prices: dict[str, float] | None = None,
    shared_adapter_cache: dict[tuple[int, str], Any] | None = None,
    market_event_exchange_context: Any | None = None,
    scan_limit: int = 100,
) -> int:
    """Detect positions closed on exchange and remove orphan TP/SL orders.

    This closes the exact safety hole reported by the user: after STOP loss, TP
    orders can remain on BingX and later trigger as naked reverse positions. The
    guard scans active executions, and if the tracked position no longer exists,
    it cancels symbol-level conditional orders immediately and marks the row
    closed_on_exchange_cleanup.
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
            rows = await db.active_position_executions(
                limit=bounded_scan_limit, after_id=_SCAN_CURSOR
            )
            if not rows and _SCAN_CURSOR:
                _SCAN_CURSOR = 0
                rows = await db.active_position_executions(
                    limit=bounded_scan_limit, after_id=0
                )
            if rows:
                _SCAN_CURSOR = max(int(row.get("id") or 0) for row in rows)
                # v1.0.7g7h2f5g5b: keep the same rotating page and cursor, but
                # process one account's rows consecutively.  The pass-local
                # positions cache remains freshness-bounded at 15 seconds; this
                # merely prevents id-interleaved fan-out rows from expiring the
                # cache before the same account is visited again.
                rows = account_local_full_pass_rows(rows)
                locality_users: set[int] = set()
                for locality_row in rows:
                    try:
                        raw_locality_user_id = locality_row.get("user_id")
                        if isinstance(raw_locality_user_id, bool):
                            raise ValueError("boolean user id")
                        locality_user_id = int(raw_locality_user_id or 0)
                    except (TypeError, ValueError, OverflowError):
                        locality_user_id = 0
                    if locality_user_id > 0:
                        locality_users.add(locality_user_id)
                record_counter(
                    "full_account_locality_users", len(locality_users)
                )
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
        positions_context = _LifecyclePositionsContext(max_age_seconds=15.0)
        try:
            for original_row in rows:
                execution_id = int(original_row.get("id") or 0)
                if not execution_id:
                    continue
                set_notification_event_key(f"execution:{execution_id}")
                async with db.execution_lock(execution_id) as lock_acquired:
                    if lock_acquired is False:
                        marker = (
                            "FULL_LIFECYCLE_ADMISSION_SKIPPED"
                            if db.monitor_workload_stage() == "full"
                            else "LIFECYCLE_LOCK_DEFERRED"
                        )
                        log.warning(
                            "%s execution_id=%s stage=%s",
                            marker,
                            execution_id,
                            db.monitor_workload_stage(),
                        )
                        continue
                    row = await db.get_execution_by_id(execution_id) or original_row
                    if db.monitor_workload_stage() == "critical":
                        record_counter("critical_db_rows_loaded")
                    status = str(row.get("status") or "")
                    # Include closed_on_exchange/closed_stop_catchup rows too.
                    # Other monitors (BE/recovery/catch-up) may mark a row closed before
                    # the lifecycle guard has cancelled stale TP/SL tails. Excluding those
                    # statuses was the hole that could leave TP after STOP.
                    if status not in {
                        "opening_intent",
                        "opened",
                        "protected",
                        "partial_error",
                        "manual_required",
                        "partial_unrecoverable",
                        "closed_pending_history",
                        "closed_on_exchange",
                        "closed_stop_catchup",
                    }:
                        continue

                        # v1.6.18: track this iteration's last-known-good status so
                        # every write below refuses if a concurrent worker (or an
                        # old/new process briefly overlapping a Railway redeploy)
                        # already moved this row to a different status.
                    _known_status = status

                    async def _write_status(new_status, reason, patch=None):
                        nonlocal _known_status
                        ok = await db.update_execution_status_merge(
                            execution_id,
                            new_status,
                            reason,
                            patch,
                            expected_status=_known_status,
                            write_flow_audit_stage="position_lifecycle_guard",
                            write_flow_audit_status=new_status,
                        )
                        if ok:
                            _known_status = new_status
                        else:
                            log.info(
                                "position_lifecycle_guard: abort stale execution pass execution_id=%s "
                                "attempted=%s expected=%s",
                                execution_id,
                                new_status,
                                _known_status,
                            )
                            raise StaleExecutionPass(
                                source="position_lifecycle_guard",
                                execution_id=execution_id,
                                expected_status=_known_status,
                                attempted_status=new_status,
                            )
                        return True

                    user_id = int(row.get("user_id") or 0)
                    symbol = str(row.get("symbol") or "").upper()
                    side = str(row.get("side") or "").lower()
                    original_qty = _signed_f(row.get("qty"), 0.0)
                    invalid_fields: list[str] = []
                    if not user_id:
                        invalid_fields.append("user_id")
                    if not symbol:
                        invalid_fields.append("symbol")
                    if side not in {"long", "short"}:
                        invalid_fields.append("side")
                    if original_qty <= 0:
                        invalid_fields.append("qty")
                    if invalid_fields:
                        await _write_status(
                            "manual_required",
                            "position lifecycle guard blocked: corrupted execution fields "
                            + ", ".join(invalid_fields),
                            {
                                "lifecycle": {
                                    "manual_required": True,
                                    "reason": "invalid_execution_fields",
                                    "invalid_fields": invalid_fields,
                                }
                            },
                        )
                        processed += 1
                        continue

                    payload = db.execution_payload_dict(row)
                    _, ledger_changed, repaired_indices = canonicalize_tp_ledger(
                        payload
                    )
                    if ledger_changed:
                        await db.merge_execution_metadata(
                            execution_id,
                            {
                                "tp": payload.get("tp") or [],
                                "tp_ledger_v1": tp_ledger_repair_metadata(
                                    repaired_indices,
                                    source="lifecycle.execution_repair",
                                ),
                            },
                        )
                        log.info(
                            "TP ledger repaired execution_id=%s user_id=%s symbol=%s indices=%s",
                            execution_id,
                            user_id,
                            symbol,
                            repaired_indices,
                        )
                    lifecycle = payload.get("lifecycle") or {}
                    if (
                        status == "closed_pending_history"
                        and not db.closed_history_reconcile_due(
                            row, now=datetime.now(timezone.utc)
                        )
                    ):
                        continue
                    if (
                        lifecycle.get("closed_cleanup_done")
                        and status != "closed_pending_history"
                    ):
                        continue

                    user_settings = await get_user_settings_cache().get_or_fetch(
                        (user_id, "settings"), lambda: db.get_user_settings(user_id)
                    )
                    exchange = str(
                        payload.get("exchange")
                        or user_settings.exchange
                        or get_settings().safe_default_exchange
                    ).lower()
                    api_row = await get_api_key_cache().get_or_fetch(
                        (user_id, "api", exchange),
                        lambda: db.get_api_key(user_id, exchange),
                    )
                    if not api_row:
                        marker = (
                            payload.get("critical_manual_review_v1")
                            if isinstance(
                                payload.get("critical_manual_review_v1"), dict
                            )
                            else {}
                        )
                        if (
                            status == "manual_required"
                            and marker.get("reason") != "api_key_missing"
                        ):
                            await db.merge_execution_metadata(
                                execution_id,
                                {
                                    "critical_manual_review_v1": {
                                        "reason": "api_key_missing",
                                        "api_state": "missing",
                                        "observed_at": datetime.now(
                                            timezone.utc
                                        ).isoformat(),
                                    }
                                },
                                expected_status=_known_status,
                            )
                        continue
                    marker = (
                        payload.get("critical_manual_review_v1")
                        if isinstance(payload.get("critical_manual_review_v1"), dict)
                        else {}
                    )
                    if (
                        status == "manual_required"
                        and marker.get("reason") == "api_key_missing"
                    ):
                        resolved_marker = {
                            **marker,
                            "reason": None,
                            "api_state": "available",
                            "resolved_at": datetime.now(timezone.utc).isoformat(),
                        }
                        marker_saved = await db.merge_execution_metadata(
                            execution_id,
                            {"critical_manual_review_v1": resolved_marker},
                            expected_status=_known_status,
                        )
                        if not marker_saved:
                            raise StaleExecutionPass(
                                source="position_lifecycle_guard.api_marker_resolved",
                                execution_id=execution_id,
                                expected_status=_known_status,
                                attempted_status="merge_api_marker",
                            )
                        payload["critical_manual_review_v1"] = resolved_marker

                    cache_key = (user_id, exchange)
                    adapter = adapter_cache.get(cache_key)
                    if adapter is None:
                        adapter = build_adapter(api_row)
                        if market_event_exchange_context is not None:
                            adapter = market_event_exchange_context.wrap_adapter(
                                adapter, cache_key
                            )
                        adapter_cache[cache_key] = adapter

                    (
                        manual_cleanup_handled,
                        manual_cleanup_actions,
                    ) = await _reconcile_manual_zero_cleanup(
                        row=row,
                        payload=payload,
                        adapter=adapter,
                        positions_context=positions_context,
                        exchange=exchange,
                        write_status=_write_status,
                    )
                    if manual_cleanup_handled:
                        processed += int(manual_cleanup_actions)
                        continue

                    if status == "opening_intent":
                        intent = (
                            payload.get("entry_write_intent_v1")
                            if isinstance(payload.get("entry_write_intent_v1"), dict)
                            else {}
                        )
                        entry_payload = (
                            payload.get("entry")
                            if isinstance(payload.get("entry"), dict)
                            else {}
                        )
                        intent_client_id = clean_exchange_id(
                            intent.get("clientOrderID")
                            or intent.get("clientOrderId")
                            or entry_payload.get("clientOrderID")
                            or entry_payload.get("clientOrderId")
                        )
                        intent_order_id = clean_exchange_id(
                            entry_payload.get("orderId")
                            or entry_payload.get("orderID")
                            or intent.get("orderId")
                            or intent.get("orderID")
                        )
                        order_type = str(
                            intent.get("order_type") or entry_payload.get("type") or ""
                        ).upper()
                        status_payload: dict[str, Any] = {}
                        status_error = ""
                        if intent_client_id or intent_order_id:
                            try:
                                status_payload = (
                                    await adapter.fetch_entry_order_fill_status(
                                        symbol=symbol,
                                        order_response={
                                            "symbol": symbol,
                                            "orderId": intent_order_id,
                                            "clientOrderID": intent_client_id,
                                        },
                                    )
                                )
                            except Exception as exc:
                                status_error = f"{type(exc).__name__}: {str(exc)[:300]}"
                        open_matches: list[dict[str, Any]] = []
                        open_error = ""
                        try:
                            open_rows = await adapter.fetch_open_orders(symbol)
                            for item in open_rows or []:
                                if not isinstance(item, dict):
                                    continue
                                oid = clean_exchange_id(
                                    item.get("orderId")
                                    or item.get("orderID")
                                    or item.get("id")
                                )
                                cid = clean_exchange_id(
                                    item.get("clientOrderID")
                                    or item.get("clientOrderId")
                                    or item.get("externalOid")
                                )
                                if (intent_order_id and oid == intent_order_id) or (
                                    intent_client_id and cid == intent_client_id
                                ):
                                    open_matches.append(item)
                        except Exception as exc:
                            open_error = f"{type(exc).__name__}: {str(exc)[:300]}"

                        all_positions = await positions_context.get_positions(
                            adapter,
                            user_id,
                            exchange,
                            symbol,
                            force_refresh=True,
                        )
                        same_side_positions = _filter_positions_by_side(
                            all_positions, side
                        )
                        qty_now = _total_position_size(same_side_positions)
                        filled_or_live_position = bool(
                            _position_qty_is_live(qty_now)
                            or status_payload.get("fully_filled")
                            or status_payload.get("filled_qty", 0)
                        )
                        live_entry_order = bool(open_matches)
                        terminal_no_fill = (
                            bool(status_payload.get("terminal"))
                            and not bool(status_payload.get("filled"))
                            and not _position_qty_is_live(qty_now)
                        )
                        reconciliation = {
                            "version": 1,
                            "checked_at": datetime.now(timezone.utc).isoformat(),
                            "client_order_id": intent_client_id,
                            "order_id": intent_order_id,
                            "order_type": order_type,
                            "order_status": status_payload,
                            "order_status_error": status_error,
                            "open_match_count": len(open_matches),
                            "open_order_error": open_error,
                            "same_side_position_qty": qty_now,
                        }
                        if filled_or_live_position:
                            await _write_status(
                                "manual_required",
                                "opening_intent reconciled: entry may be live; manual STOP/TP protection check required",
                                {
                                    "opening_intent_reconciliation_v1": reconciliation,
                                    "lifecycle": {
                                        "manual_required": True,
                                        "reason": "opening_intent_live_entry_requires_manual_protection_check",
                                    },
                                },
                            )
                            await _notify_critical(
                                notify,
                                user_id,
                                ensure_visual_card(
                                    "🚨 <b>НУЖНА ПРОВЕРКА СДЕЛКИ</b>\n"
                                    f"{symbol} {side.upper()}\n"
                                    "Бот восстановил durable opening_intent после перезапуска. "
                                    "На BingX есть признаки live entry/позиции, поэтому автоматически не ставлю TP/SL вслепую. "
                                    "Проверь STOP/TP вручную."
                                ),
                                event_key=f"execution:{execution_id}:opening_intent_live",
                            )
                            processed += 1
                            continue
                        if live_entry_order and order_type == "LIMIT":
                            live_order = open_matches[0]
                            await _write_status(
                                "pending_limit",
                                "opening_intent reconciled: live pending LIMIT entry found by exact clientOrderID/orderId",
                                {
                                    "entry": {
                                        "orderId": clean_exchange_id(
                                            live_order.get("orderId")
                                            or live_order.get("id")
                                        ),
                                        "clientOrderID": intent_client_id,
                                        "clientOrderId": intent_client_id,
                                        "_exchange": "bingx",
                                    },
                                    "opening_intent_reconciliation_v1": reconciliation,
                                },
                            )
                            processed += 1
                            continue
                        if terminal_no_fill:
                            await _write_status(
                                "closed_on_exchange",
                                "opening_intent reconciled: entry terminal without fill and no position",
                                {"opening_intent_reconciliation_v1": reconciliation},
                            )
                            processed += 1
                            continue
                        await db.merge_execution_metadata(
                            execution_id,
                            {"opening_intent_reconciliation_v1": reconciliation},
                            expected_status="opening_intent",
                            write_flow_audit_stage="opening_intent_reconcile_deferred",
                            write_flow_audit_status="opening_intent",
                        )
                        processed += 1
                        continue

                    observed_price = float((market_prices or {}).get(symbol) or 0.0)
                    tp_rows = (
                        payload.get("tp") if isinstance(payload.get("tp"), list) else []
                    )
                    touched_now = _mark_tp_price_seen(
                        [item for item in tp_rows if isinstance(item, dict)],
                        side,
                        observed_price,
                    )
                    # A public TP event must not reuse a position snapshot cached just
                    # before the fill. Force one fresh private read for this account.
                    # Fetch ALL positions for the exact API account once per bounded
                    # lifecycle pass window. A public TP touch forces a fresh private
                    # snapshot because a pre-touch snapshot cannot prove post-fill qty.
                    all_positions = await positions_context.get_positions(
                        adapter,
                        user_id,
                        exchange,
                        symbol,
                        force_refresh=bool(touched_now),
                    )
                    same_side_positions = _filter_positions_by_side(all_positions, side)
                    qty_now = _total_position_size(same_side_positions)
                    qty_any = _total_position_size(all_positions)
                    progress_tolerance = max(original_qty * 0.002, 1e-10)
                    closed_qty = max(0.0, original_qty - qty_now)

                    history_rows: list[dict[str, Any]] = []
                    has_price_evidence = any(
                        isinstance(item, dict)
                        and item.get("price_seen") is True
                        and item.get("filled") is not True
                        for item in tp_rows
                    )
                    known_filled_qty = sum(
                        _f(item.get("qty") or item.get("actual_tp_qty"), 0.0)
                        for item in tp_rows
                        if isinstance(item, dict) and item.get("filled") is True
                    )
                    unexplained_reduction = max(0.0, closed_qty - known_filled_qty)
                    has_unfilled_tp = any(
                        isinstance(item, dict) and item.get("filled") is not True
                        for item in tp_rows
                    )
                    history_needed = bool(
                        tp_rows
                        and has_unfilled_tp
                        and (
                            touched_now
                            or has_price_evidence
                            or unexplained_reduction > progress_tolerance
                            or not _position_qty_is_live(qty_now)
                        )
                    )
                    if history_needed and hasattr(
                        adapter, "fetch_position_tpsl_history"
                    ):
                        try:
                            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                            history_rows = await adapter.fetch_position_tpsl_history(
                                symbol,
                                is_finished=1,
                                start_time_ms=_execution_start_ms(
                                    row.get("created_at")
                                ),
                                end_time_ms=now_ms + 60_000,
                                page_size=100,
                                max_pages=5,
                            )
                        except Exception as history_exc:
                            # Position reduction + durable price touch remains a safe
                            # fallback. A history read error must not stop STOP cleanup.
                            log.warning(
                                "TP history reconcile failed execution_id=%s user_id=%s symbol=%s error=%s: %s",
                                execution_id,
                                user_id,
                                symbol,
                                type(history_exc).__name__,
                                history_exc,
                            )

                    if (
                        history_rows
                        and unexplained_reduction > progress_tolerance
                        and tp_rows
                    ):
                        recovered_identity_indices = (
                            await _recover_exact_tp_child_identities(
                                adapter=adapter,
                                tp_rows=[
                                    item for item in tp_rows if isinstance(item, dict)
                                ],
                                history_rows=history_rows,
                                side=side,
                                symbol=symbol,
                            )
                        )
                        if recovered_identity_indices:
                            recovery_saved = await db.merge_execution_metadata(
                                execution_id,
                                {"tp": tp_rows},
                                expected_status=_known_status,
                            )
                            if not recovery_saved:
                                raise StaleExecutionPass(
                                    source="position_lifecycle_guard.tp_identity_recovery",
                                    execution_id=execution_id,
                                    expected_status=_known_status,
                                    attempted_status="merge_tp_identity_recovery",
                                )
                            payload["tp"] = tp_rows

                    entry_for_tp = _f(payload.get("actual_entry"), 0.0) or _f(
                        row.get("entry"), 0.0
                    )
                    tp_actions = await _sync_tp_fill_notifications(
                        notify=notify,
                        user_id=user_id,
                        execution_id=execution_id,
                        status=status,
                        reason=str(row.get("reason") or ""),
                        symbol=symbol,
                        side=side,
                        entry=entry_for_tp,
                        original_qty=original_qty,
                        qty_now=qty_now,
                        leverage=_f(row.get("leverage"), 1.0),
                        payload=payload,
                        infer_from_qty=_position_qty_is_live(qty_now),
                        observed_price=observed_price,
                        history_rows=history_rows,
                        pre_touched=touched_now,
                    )
                    processed += tp_actions
                    # _sync_tp_fill_notifications canonicalizes the ledger and may
                    # replace payload["tp"] with fresh row dictionaries.  Re-read
                    # the durable in-memory ledger instead of using the stale
                    # pre-sync list, otherwise a just-recovered exact child fill is
                    # falsely reported as TP_HISTORY_UNRESOLVED_PARTIAL.
                    current_tp_rows, confirmed_tp_qty_after = (
                        _confirmed_tp_qty_from_payload(payload)
                    )
                    unexplained_reduction_after = max(
                        0.0, closed_qty - confirmed_tp_qty_after
                    )
                    _log_unresolved_partial_tp_history(
                        execution_id=execution_id,
                        user_id=user_id,
                        symbol=symbol,
                        side=side,
                        status=status,
                        reason=str(row.get("reason") or ""),
                        original_qty=original_qty,
                        qty_now=qty_now,
                        closed_qty=closed_qty,
                        confirmed_tp_qty=confirmed_tp_qty_after,
                        unexplained_reduction=unexplained_reduction_after,
                        progress_tolerance=progress_tolerance,
                        observed_price=observed_price,
                        tp_rows=current_tp_rows,
                        history_rows=history_rows,
                    )
                    if history_needed or tp_actions:
                        log.info(
                            "TP reconcile execution_id=%s user_id=%s symbol=%s original_qty=%.12g qty_now=%.12g closed_qty=%.12g history_rows=%s actions=%s",
                            execution_id,
                            user_id,
                            symbol,
                            original_qty,
                            qty_now,
                            closed_qty,
                            len(history_rows),
                            tp_actions,
                        )
                    if _position_qty_is_live(qty_now):
                        if lifecycle.get("zero_candidate_at") or isinstance(
                            payload.get("critical_zero_exposure_v1"), dict
                        ):
                            if _critical_live_state_write_required(
                                payload,
                                same_side_qty=qty_now,
                                any_position_qty=qty_any,
                                reason="live_position_observed",
                            ):
                                now_live = datetime.now(timezone.utc)
                                await _write_status(
                                    status,
                                    str(row.get("reason") or ""),
                                    {
                                        "lifecycle": {
                                            "zero_candidate_at": None,
                                            "position_qty": qty_now,
                                            "any_position_qty": qty_any,
                                            "opposite_or_unknown_position_detected": False,
                                        },
                                        "critical_zero_exposure_v1": _critical_zero_invalidated_patch(
                                            now=now_live,
                                            same_side_qty=qty_now,
                                            any_position_qty=qty_any,
                                            reason="live_position_observed",
                                        ),
                                    },
                                )
                            elif db.monitor_workload_stage() == "critical":
                                record_counter("critical_db_writes_skipped")
                        _all_original_tp_done, _ = _residual_original_tps_complete(
                            payload,
                            [
                                item
                                for item in (payload.get("tp") or [])
                                if isinstance(item, dict)
                            ],
                        )
                        _be_payload = (
                            payload.get("be")
                            if isinstance(payload.get("be"), dict)
                            else {}
                        )
                        if (
                            _all_original_tp_done
                            or _be_payload.get("moved") is True
                            or isinstance(payload.get("residual_position_v1"), dict)
                            or isinstance(payload.get("stop_diagnostic_v1"), dict)
                        ):
                            (
                                _residual_created,
                                _residual_patch,
                            ) = await _reconcile_residual_and_stop_diagnostics(
                                adapter=adapter,
                                notify=notify,
                                execution_id=execution_id,
                                user_id=user_id,
                                symbol=symbol,
                                side=side,
                                qty_now=qty_now,
                                positions=same_side_positions,
                                observed_price=observed_price,
                                payload=payload,
                                tp_rows=[
                                    item
                                    for item in (payload.get("tp") or [])
                                    if isinstance(item, dict)
                                ],
                                expected_status=_known_status,
                                positions_context=positions_context,
                                exchange=exchange,
                            )
                            if _residual_patch:
                                _residual_merged = await db.merge_execution_metadata(
                                    execution_id,
                                    _residual_patch,
                                    expected_status=_known_status,
                                )
                                if not _residual_merged:
                                    raise StaleExecutionPass(
                                        source="position_lifecycle_guard.residual_reconciliation",
                                        execution_id=execution_id,
                                        expected_status=_known_status,
                                        attempted_status="merge_residual_metadata",
                                    )
                                payload.update(_residual_patch)
                            if _residual_created:
                                processed += 1
                        continue

                    # A single qty=0 response can be transient BingX propagation lag.
                    # Do not block the whole protection worker with 2s+5s sleeps. Persist
                    # a zero candidate and confirm it in a later monitor cycle after the
                    # configured critical interval. This keeps other positions protected.
                    lifecycle = payload.get("lifecycle") or {}
                    zero_seen_raw = lifecycle.get("zero_candidate_at")
                    confirm_after = max(
                        3.0,
                        float(
                            getattr(get_settings(), "MONITOR_CRITICAL_INTERVAL_SEC", 5)
                            or 5
                        ),
                    )
                    now_utc = datetime.now(timezone.utc)
                    zero_seen_at: datetime | None = None
                    if zero_seen_raw:
                        try:
                            zero_seen_at = datetime.fromisoformat(
                                str(zero_seen_raw).replace("Z", "+00:00")
                            )
                            if zero_seen_at.tzinfo is None:
                                zero_seen_at = zero_seen_at.replace(tzinfo=timezone.utc)
                        except (TypeError, ValueError):
                            zero_seen_at = None

                    if zero_seen_at is None:
                        zero_patch = (
                            _critical_zero_first_patch(
                                payload,
                                now=now_utc,
                                same_side_qty=qty_now,
                                any_position_qty=qty_any,
                            )
                            if not _position_qty_is_live(qty_any)
                            else _critical_zero_invalidated_patch(
                                now=now_utc,
                                same_side_qty=qty_now,
                                any_position_qty=qty_any,
                                reason="opposite_or_unknown_position_observed",
                            )
                        )
                        await _write_status(
                            status,
                            str(row.get("reason") or ""),
                            {
                                "lifecycle": {
                                    "zero_candidate_at": now_utc.isoformat(),
                                    "position_qty": qty_now,
                                    "any_position_qty": qty_any,
                                    "opposite_or_unknown_position_detected": bool(
                                        _position_qty_is_live(qty_any)
                                    ),
                                },
                                "critical_zero_exposure_v1": zero_patch,
                            },
                        )
                        continue

                    age = max(
                        0.0,
                        (
                            now_utc - zero_seen_at.astimezone(timezone.utc)
                        ).total_seconds(),
                    )
                    if age < confirm_after:
                        continue

                    # Second independent read after the delay. If the position came
                    # back, clear the candidate and leave every protective order intact.
                    all_positions2 = await positions_context.get_positions(
                        adapter,
                        user_id,
                        exchange,
                        symbol,
                        force_refresh=True,
                    )
                    same_side2 = _filter_positions_by_side(all_positions2, side)
                    qty_now = _total_position_size(same_side2)
                    qty_any = _total_position_size(all_positions2)
                    if _position_qty_is_live(qty_now) or _position_qty_is_live(qty_any):
                        await _write_status(
                            status,
                            str(row.get("reason") or ""),
                            {
                                "lifecycle": {
                                    "zero_candidate_at": None,
                                    "zero_candidate_cleared_at": now_utc.isoformat(),
                                    "position_qty": qty_now,
                                    "any_position_qty": qty_any,
                                    "opposite_or_unknown_position_detected": bool(
                                        _position_qty_is_live(qty_any)
                                        and not _position_qty_is_live(qty_now)
                                    ),
                                },
                                "critical_zero_exposure_v1": _critical_zero_invalidated_patch(
                                    now=now_utc,
                                    same_side_qty=qty_now,
                                    any_position_qty=qty_any,
                                    reason=(
                                        "live_position_observed"
                                        if _position_qty_is_live(qty_now)
                                        else "opposite_or_unknown_position_observed"
                                    ),
                                ),
                            },
                        )
                        log.info(
                            "lifecycle guard: %s %s zero candidate was transient; qty=%.6f any_qty=%.6f",
                            symbol,
                            side,
                            qty_now,
                            qty_any,
                        )
                        continue

                    confirmed_zero_patch = _critical_zero_confirmed_patch(
                        payload,
                        now=now_utc,
                        zero_seen_at=zero_seen_at,
                    )
                    zero_saved = await db.merge_execution_metadata(
                        execution_id,
                        {
                            "lifecycle": {
                                "position_qty": 0.0,
                                "any_position_qty": 0.0,
                                "opposite_or_unknown_position_detected": False,
                            },
                            "critical_zero_exposure_v1": confirmed_zero_patch,
                        },
                        expected_status=_known_status,
                    )
                    if not zero_saved:
                        raise StaleExecutionPass(
                            source="position_lifecycle_guard.zero_exposure_proof",
                            execution_id=execution_id,
                            expected_status=_known_status,
                            attempted_status="merge_zero_exposure_proof",
                        )
                    payload["critical_zero_exposure_v1"] = confirmed_zero_patch
                    lifecycle.update(
                        {
                            "position_qty": 0.0,
                            "any_position_qty": 0.0,
                            "opposite_or_unknown_position_detected": False,
                        }
                    )

                    # Position is confirmed absent. Re-read durable TP/SL history
                    # after the second private position check because BingX can
                    # publish qty=0 before the terminal stop-order row appears.
                    if hasattr(adapter, "fetch_position_tpsl_history"):
                        try:
                            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                            fresh_history = await adapter.fetch_position_tpsl_history(
                                symbol,
                                is_finished=1,
                                start_time_ms=_execution_start_ms(
                                    row.get("created_at")
                                ),
                                end_time_ms=now_ms + 60_000,
                                page_size=100,
                                max_pages=5,
                            )
                            if fresh_history:
                                history_rows = fresh_history
                            final_tp_rows = (
                                payload.get("tp")
                                if isinstance(payload.get("tp"), list)
                                else []
                            )
                            if history_rows and final_tp_rows:
                                recovered_identity_indices = (
                                    await _recover_exact_tp_child_identities(
                                        adapter=adapter,
                                        tp_rows=[
                                            item
                                            for item in final_tp_rows
                                            if isinstance(item, dict)
                                        ],
                                        history_rows=history_rows,
                                        side=side,
                                        symbol=symbol,
                                    )
                                )
                                if recovered_identity_indices:
                                    recovery_saved = await db.merge_execution_metadata(
                                        execution_id,
                                        {"tp": final_tp_rows},
                                        expected_status=_known_status,
                                    )
                                    if not recovery_saved:
                                        raise StaleExecutionPass(
                                            source="position_lifecycle_guard.final_tp_identity_recovery",
                                            execution_id=execution_id,
                                            expected_status=_known_status,
                                            attempted_status="merge_final_tp_identity_recovery",
                                        )
                                    payload["tp"] = final_tp_rows
                            tp_actions = await _sync_tp_fill_notifications(
                                notify=notify,
                                user_id=user_id,
                                execution_id=execution_id,
                                status=status,
                                reason=str(row.get("reason") or ""),
                                symbol=symbol,
                                side=side,
                                entry=entry_for_tp,
                                original_qty=original_qty,
                                qty_now=0.0,
                                leverage=_f(row.get("leverage"), 1.0),
                                payload=payload,
                                infer_from_qty=False,
                                observed_price=observed_price,
                                history_rows=history_rows,
                            )
                            processed += tp_actions
                        except StaleExecutionPass:
                            raise
                        except Exception as history_exc:
                            log.warning(
                                "final close history refresh failed execution_id=%s symbol=%s error=%s: %s",
                                execution_id,
                                symbol,
                                type(history_exc).__name__,
                                history_exc,
                            )

                    tp_rows = (
                        payload.get("tp") if isinstance(payload.get("tp"), list) else []
                    )
                    terminal_history, terminal_history_type = (
                        _terminal_close_history_evidence(
                            [item for item in tp_rows if isinstance(item, dict)],
                            history_rows,
                            original_qty=original_qty,
                            payload=payload,
                            side=side,
                            expected_stop=(
                                _f((payload.get("be") or {}).get("stop"), 0.0)
                                if isinstance(payload.get("be"), dict)
                                and (payload.get("be") or {}).get("moved") is True
                                else _f(row.get("stop"), 0.0)
                            ),
                        )
                    )
                    pending_tp_notifications = any(
                        isinstance(item, dict)
                        and item.get("filled") is True
                        and item.get("filled_notified") is not True
                        for item in tp_rows
                    )
                    history_reconcile = lifecycle.get("history_reconcile")
                    if not isinstance(history_reconcile, dict):
                        history_reconcile = {}
                    timeout_sec = int(
                        get_settings().CLOSED_HISTORY_RECONCILE_TIMEOUT_SEC
                    )
                    started_at = _parse_utc_datetime(
                        history_reconcile.get("started_at")
                    )
                    if started_at is None:
                        started_at = now_utc
                    history_age = max(0.0, (now_utc - started_at).total_seconds())
                    history_deadline_expired = history_age >= timeout_sec

                    # A row already in closed_pending_history has had stale-order
                    # cleanup completed. It now waits only for durable close
                    # evidence, then final classification and user notification.
                    if (
                        status == "closed_pending_history"
                        and lifecycle.get("closed_cleanup_done") is True
                    ):
                        if terminal_history and pending_tp_notifications:
                            retry_patch = _closed_history_retry_patch(
                                history_reconcile,
                                now=now_utc,
                                started_at=started_at,
                                timeout_sec=timeout_sec,
                                pending_notification=True,
                                history_rows=len(history_rows),
                                resolution=terminal_history_type,
                            )
                            await _write_status(
                                status,
                                str(row.get("reason") or ""),
                                {"lifecycle": {"history_reconcile": retry_patch}},
                            )
                            log.info(
                                "closed history retry scheduled execution_id=%s symbol=%s reason=tp_notification delay_sec=%s attempts=%s",
                                execution_id,
                                symbol,
                                retry_patch.get("retry_delay_seconds"),
                                retry_patch.get("attempts"),
                            )
                            continue
                        if not terminal_history and not history_deadline_expired:
                            retry_patch = _closed_history_retry_patch(
                                history_reconcile,
                                now=now_utc,
                                started_at=started_at,
                                timeout_sec=timeout_sec,
                                pending_notification=False,
                                history_rows=len(history_rows),
                            )
                            retry_patch["identity_diagnostics_v2"] = (
                                _tp_history_identity_diagnostics(
                                    [
                                        item
                                        for item in tp_rows
                                        if isinstance(item, dict)
                                    ],
                                    history_rows,
                                    side=side,
                                )
                            )
                            await _write_status(
                                status,
                                str(row.get("reason") or ""),
                                {"lifecycle": {"history_reconcile": retry_patch}},
                            )
                            log.info(
                                "closed history retry scheduled execution_id=%s symbol=%s reason=history_pending delay_sec=%s attempts=%s",
                                execution_id,
                                symbol,
                                retry_patch.get("retry_delay_seconds"),
                                retry_patch.get("attempts"),
                            )
                            continue
                        if not terminal_history and history_deadline_expired:
                            probable_be_reason = _be_stop_probable_close_reason(
                                [item for item in tp_rows if isinstance(item, dict)],
                                history_rows,
                                original_qty=original_qty,
                                payload=payload,
                                side=side,
                                expected_stop=(
                                    _f((payload.get("be") or {}).get("stop"), 0.0)
                                    if isinstance(payload.get("be"), dict)
                                    and (payload.get("be") or {}).get("moved") is True
                                    else _f(row.get("stop"), 0.0)
                                ),
                                allow_timeout_fallback=True,
                            )
                            if probable_be_reason:
                                close_result = await _closed_position_result(
                                    adapter=adapter,
                                    row=row,
                                    payload=payload,
                                    symbol=symbol,
                                    side=side,
                                    original_qty=original_qty,
                                    market_price=float(
                                        (market_prices or {}).get(symbol) or 0.0
                                    ),
                                    terminal_history_type=probable_be_reason,
                                )
                                await db.record_execution_outcome(
                                    execution_id,
                                    outcome=str(
                                        close_result.get("outcome") or "unknown"
                                    ),
                                    realized_pnl=float(
                                        close_result.get("realized_pnl") or 0.0
                                    ),
                                    close_type=str(
                                        close_result.get("close_type") or "unknown"
                                    ),
                                )
                                financial_marker = _financial_marker_for_closed_execution(
                                    execution_id=execution_id,
                                    user_id=user_id,
                                    exchange=exchange,
                                    row=row,
                                    payload=payload,
                                    symbol=symbol,
                                    side=side,
                                    original_qty=original_qty,
                                    close_result=close_result,
                                    history_rows=history_rows,
                                    terminal_history_type=probable_be_reason,
                                    now=now_utc,
                                )
                                probable_patch: dict[str, Any] = {
                                    "lifecycle": {
                                        "history_reconcile": {
                                            "started_at": started_at.isoformat(),
                                            "resolved_at": now_utc.isoformat(),
                                            "resolution": probable_be_reason,
                                            "attempts": int(
                                                history_reconcile.get("attempts")
                                                or 0
                                            )
                                            + 1,
                                            "last_history_rows": len(history_rows),
                                            "deadline_seconds": timeout_sec,
                                            "bingx_history_gap_fallback": True,
                                        },
                                        "close_result": {
                                            **close_result,
                                            "terminal_history_type": probable_be_reason,
                                        },
                                    }
                                }
                                if financial_marker is not None:
                                    probable_patch[
                                        FINANCIAL_RECONCILIATION_MARKER_KEY
                                    ] = financial_marker
                                await _write_status(
                                    "closed_on_exchange_cleanup",
                                    "position close classified as probable BE STOP after BingX history did not expose exact STOP fill",
                                    probable_patch,
                                )
                                await _enqueue_financial_marker_best_effort(
                                    execution_id, financial_marker
                                )
                                if close_result.get("message"):
                                    await _notify_critical(
                                        notify,
                                        user_id,
                                        str(close_result["message"]),
                                        event_key=f"execution:{execution_id}:probable_be_stop_close",
                                    )
                                processed += 1
                                continue
                            stale_decision = _stale_closed_history_timeout(
                                row,
                                history_reconcile,
                                now=now_utc,
                                timeout_sec=timeout_sec,
                                stale_silent_after_sec=int(
                                    getattr(
                                        get_settings(),
                                        "CLOSED_HISTORY_STALE_SILENT_AFTER_SEC",
                                        3600,
                                    )
                                    or 0
                                ),
                            )
                            close_type = (
                                "stale_history_timeout_manual_or_external_close_silent"
                                if stale_decision["should_silence"]
                                else "history_timeout_manual_or_external_close"
                            )
                            await db.record_execution_outcome(
                                execution_id,
                                outcome="unknown",
                                realized_pnl=0.0,
                                close_type=close_type,
                            )
                            history_patch = {
                                "started_at": started_at.isoformat(),
                                "timed_out_at": now_utc.isoformat(),
                                "identity_diagnostics_v2": (
                                    _tp_history_identity_diagnostics(
                                        [
                                            item
                                            for item in tp_rows
                                            if isinstance(item, dict)
                                        ],
                                        history_rows,
                                        side=side,
                                    )
                                ),
                                "attempts": int(history_reconcile.get("attempts") or 0)
                                + 1,
                                "last_history_rows": len(history_rows),
                                "deadline_seconds": timeout_sec,
                                "stale_silence": stale_decision,
                            }
                            if stale_decision["should_silence"]:
                                history_patch.update(
                                    {
                                        "stale_silenced": True,
                                        "stale_silenced_at": now_utc.isoformat(),
                                    }
                                )
                            else:
                                history_patch["timeout_notified_at"] = (
                                    now_utc.isoformat()
                                )
                            await _write_status(
                                "closed_on_exchange_cleanup",
                                (
                                    "stale old closed execution archived silently; BingX TP/SL history did not identify the close before deadline"
                                    if stale_decision["should_silence"]
                                    else "position and stale orders are confirmed closed; BingX TP/SL history did not identify the close before deadline"
                                ),
                                {
                                    "lifecycle": {
                                        "history_reconcile": history_patch,
                                        "close_result": {
                                            "outcome": "unknown",
                                            "close_type": close_type,
                                        },
                                    }
                                },
                            )
                            if not stale_decision["should_silence"]:
                                created_at = _parse_utc_datetime(row.get("created_at"))
                                created_text = (
                                    created_at.strftime("%d.%m.%Y %H:%M UTC")
                                    if created_at is not None
                                    else "неизвестно"
                                )
                                await _notify_critical(
                                    notify,
                                    user_id,
                                    card(
                                        "🧾 <b>ПОЗИЦИЯ ЗАКРЫТА ВНЕ TP/STOP БОТА</b>",
                                        symbol=symbol,
                                        side=side,
                                        blocks=(
                                            [
                                                "✅ Позиция на BingX закрыта",
                                                "ℹ️ Закрытие не подтверждено bot-owned TP/STOP",
                                                "🧾 Возможная причина: ручное или внешнее закрытие",
                                            ],
                                            [
                                                f"🧷 <b>Execution ID:</b> {execution_id}",
                                                f"🕓 <b>Создана:</b> {created_text}",
                                            ],
                                            [
                                                "🔎 Проверьте историю сделок и фактический PnL на BingX"
                                            ],
                                        ),
                                    ),
                                    event_key=f"execution:{execution_id}:history_timeout_manual_review",
                                )
                            else:
                                log.info(
                                    "stale closed history archived silently execution_id=%s user_id=%s symbol=%s side=%s age=%.1f threshold=%s anchor=%s",
                                    execution_id,
                                    user_id,
                                    symbol,
                                    side,
                                    float(stale_decision.get("age_seconds") or 0.0),
                                    stale_decision.get("threshold_seconds"),
                                    stale_decision.get("anchor_source"),
                                )
                            processed += 1
                            continue
                        # History has now settled after cleanup. Finalize without
                        # repeating broad symbol cancellation.
                        close_result = await _closed_position_result(
                            adapter=adapter,
                            row=row,
                            payload=payload,
                            symbol=symbol,
                            side=side,
                            original_qty=original_qty,
                            market_price=float(
                                (market_prices or {}).get(symbol) or 0.0
                            ),
                            terminal_history_type=terminal_history_type,
                        )
                        await db.record_execution_outcome(
                            execution_id,
                            outcome=str(close_result.get("outcome") or "unknown"),
                            realized_pnl=float(close_result.get("realized_pnl") or 0.0),
                            close_type=str(close_result.get("close_type") or "unknown"),
                        )
                        financial_marker = _financial_marker_for_closed_execution(
                            execution_id=execution_id,
                            user_id=user_id,
                            exchange=exchange,
                            row=row,
                            payload=payload,
                            symbol=symbol,
                            side=side,
                            original_qty=original_qty,
                            close_result=close_result,
                            history_rows=history_rows,
                            terminal_history_type=terminal_history_type,
                            now=now_utc,
                        )
                        terminal_patch: dict[str, Any] = {
                            "lifecycle": {
                                "history_reconcile": {
                                    "started_at": started_at.isoformat(),
                                    "resolved_at": now_utc.isoformat(),
                                    "resolution": terminal_history_type,
                                    "attempts": int(
                                        history_reconcile.get("attempts") or 0
                                    )
                                    + 1,
                                    "last_history_rows": len(history_rows),
                                    "deadline_seconds": timeout_sec,
                                },
                                "close_result": close_result,
                            }
                        }
                        if financial_marker is not None:
                            terminal_patch[FINANCIAL_RECONCILIATION_MARKER_KEY] = (
                                financial_marker
                            )
                        await _write_status(
                            "closed_on_exchange_cleanup",
                            "position close confirmed by delayed BingX TP/SL history; stale orders already cleaned",
                            terminal_patch,
                        )
                        await _enqueue_financial_marker_best_effort(
                            execution_id, financial_marker
                        )
                        if close_result.get("message"):
                            await _notify_critical(
                                notify, user_id, str(close_result["message"])
                            )
                        processed += 1
                        continue

                    known_closed_status = status in {
                        "closed_on_exchange",
                        "closed_stop_catchup",
                    }
                    defer_final_history = (
                        not terminal_history and not known_closed_status
                    ) or pending_tp_notifications
                    close_result: dict[str, Any] | None = None
                    if not _position_qty_is_live(qty_any) and not defer_final_history:
                        try:
                            close_result = await _closed_position_result(
                                adapter=adapter,
                                row=row,
                                payload=payload,
                                symbol=symbol,
                                side=side,
                                original_qty=original_qty,
                                market_price=float(
                                    (market_prices or {}).get(symbol) or 0.0
                                ),
                                terminal_history_type=terminal_history_type,
                            )
                            await db.record_execution_outcome(
                                execution_id,
                                outcome=str(close_result.get("outcome") or "unknown"),
                                realized_pnl=float(
                                    close_result.get("realized_pnl") or 0.0
                                ),
                                close_type=str(
                                    close_result.get("close_type") or "unknown"
                                ),
                            )
                        except Exception as result_exc:
                            log.warning(
                                "failed to classify closed execution %s: %s",
                                execution_id,
                                result_exc,
                            )

                    if _position_qty_is_live(qty_any):
                        # The tracked side is gone, but an opposite/unknown position exists.
                        # Persist an unknown result so this closed execution no longer
                        # inflates the active-position count or winrate denominator.
                        await db.record_execution_outcome(
                            execution_id,
                            outcome="unknown",
                            realized_pnl=0.0,
                            close_type="reverse_position_detected",
                        )
                        patch = {
                            "lifecycle": {
                                "closed_cleanup_done": True,
                                "previous_status": status,
                                "position_qty": qty_now,
                                "any_position_qty": qty_any,
                                "opposite_or_unknown_position_detected": True,
                                "positions": all_positions2,
                                "close_result": {
                                    "outcome": "unknown",
                                    "close_type": "reverse_position_detected",
                                },
                            }
                        }
                        await _write_status(
                            "manual_required",
                            "original position side is gone, but another same-symbol position exists on BingX",
                            patch,
                        )
                        await _notify_critical(
                            notify,
                            user_id,
                            card(
                                "🚨 <b>ОБНАРУЖЕНА ОБРАТНАЯ ПОЗИЦИЯ</b>",
                                symbol=symbol,
                                side=side,
                                blocks=(
                                    [
                                        f"📦 <b>Объём другой позиции:</b> {fmt_qty(qty_any)}",
                                        "❌ Исходная сторона позиции больше не найдена",
                                        "⚠️ Возможен старый TP, открывший обратную позицию",
                                    ],
                                    [
                                        "🔴 <b>Проверьте на BingX:</b>",
                                        *tree_lines(
                                            [
                                                "Направление и объём позиции",
                                                "STOP",
                                                "TP",
                                                "Conditional orders",
                                            ]
                                        ),
                                    ],
                                    [
                                        "🚫 Новые автоматические действия по этой записи остановлены"
                                    ],
                                ),
                            ),
                        )
                        processed += 1
                        continue

                        # Exact order-id cleanup is safe even when another same-symbol
                        # execution exists.  Record the overlap for diagnostics, but
                        # never use it as a reason to fall back to symbol-wide cancel.
                    other_rows = await db.other_active_symbol_executions(
                        user_id, symbol, execution_id, limit=5
                    )
                    if other_rows:
                        log.info(
                            "lifecycle_guard: %s %s exact cleanup with other execution ids=%s",
                            symbol,
                            side,
                            [int(r.get("id") or 0) for r in other_rows],
                        )

                    async with symbol_action_lock(user_id, symbol):
                        # Re-read the execution payload inside the lock so cleanup
                        # uses the latest post-BE TP/STOP ids.  New same-symbol
                        # orders may coexist; only ids durably owned by this row
                        # are eligible for cancellation.
                        latest_row = await db.get_execution_by_id(execution_id)
                        if latest_row:
                            latest_status = str(latest_row.get("status") or "")
                            if latest_status and latest_status != status:
                                await db.merge_execution_metadata(
                                    execution_id,
                                    {
                                        "lifecycle": {
                                            "cleanup_deferred": True,
                                            "cleanup_deferred_reason": (
                                                "execution status changed while waiting for symbol lock: "
                                                f"{status}->{latest_status}"
                                            ),
                                        }
                                    },
                                )
                                continue
                            latest_payload = db.execution_payload_dict(latest_row)
                            if not latest_payload:
                                latest_payload = payload
                        else:
                            latest_payload = payload

                            # The earlier zero-position read may have been stale.  A
                            # delayed BingX position can appear while this task waits
                            # for the user+symbol lock.  Re-read inside the critical
                            # section before cancelling even exact old protection.
                        await positions_context.invalidate_positions(
                            adapter, user_id, exchange
                        )
                        # Deliberately direct/fresh: this read authorizes exact stale
                        # order cleanup and must never be served from the pass cache.
                        fresh_all_positions = _filter_positions_by_symbol(
                            list(await adapter.fetch_open_positions(symbol) or []),
                            symbol,
                        )
                        fresh_positions = _filter_positions_by_side(
                            fresh_all_positions, side
                        )
                        fresh_qty = _total_position_size(fresh_positions)
                        fresh_any_qty = _total_position_size(fresh_all_positions)
                        if _position_qty_is_live(fresh_qty) or _position_qty_is_live(
                            fresh_any_qty
                        ):
                            await db.merge_execution_metadata(
                                execution_id,
                                {
                                    "lifecycle": {
                                        "cleanup_deferred": True,
                                        "cleanup_deferred_reason": (
                                            "position reappeared during locked final check"
                                        ),
                                        "locked_recheck_position_qty": fresh_qty,
                                        "locked_recheck_any_position_qty": fresh_any_qty,
                                        "opposite_or_unknown_position_detected": bool(
                                            _position_qty_is_live(fresh_any_qty)
                                            and not _position_qty_is_live(fresh_qty)
                                        ),
                                    },
                                    "critical_zero_exposure_v1": _critical_zero_invalidated_patch(
                                        now=datetime.now(timezone.utc),
                                        same_side_qty=fresh_qty,
                                        any_position_qty=fresh_any_qty,
                                        reason=(
                                            "live_position_observed"
                                            if _position_qty_is_live(fresh_qty)
                                            else "opposite_or_unknown_position_observed"
                                        ),
                                    ),
                                },
                            )
                            continue
                        cleanup = await _cleanup_stale_orders(
                            adapter,
                            symbol,
                            payload=latest_payload,
                            attempts=3,
                            positions_context=positions_context,
                            user_id=user_id,
                            exchange=exchange,
                        )
                    patch = {
                        "lifecycle": {
                            "closed_cleanup_done": cleanup.get("verified_clean") is True,
                            "previous_status": status,
                            "position_qty": qty_now,
                            "cleanup": cleanup,
                            "cleanup_deferred": False,
                            "close_result": close_result
                            or {"outcome": "unknown", "close_type": "unknown"},
                        }
                    }
                    critical_cancel_failed = bool(
                        cleanup.get("errors")
                    ) and not cleanup.get("conditional_cancelled")
                    if (
                        defer_final_history
                        and cleanup.get("verified_clean") is True
                        and not critical_cancel_failed
                    ):
                        patch["lifecycle"]["history_reconcile"] = {
                            **_closed_history_retry_patch(
                                history_reconcile,
                                now=now_utc,
                                started_at=started_at,
                                timeout_sec=timeout_sec,
                                pending_notification=bool(pending_tp_notifications),
                                history_rows=len(history_rows),
                                resolution=terminal_history_type or None,
                            ),
                            "fresh_close_observed": True,
                        }
                        if not terminal_history:
                            patch["lifecycle"]["history_reconcile"][
                                "identity_diagnostics_v2"
                            ] = _tp_history_identity_diagnostics(
                                [item for item in tp_rows if isinstance(item, dict)],
                                history_rows,
                                side=side,
                            )
                        pending_close_type = (
                            "waiting_for_tp_notification"
                            if pending_tp_notifications and terminal_history
                            else "waiting_for_mexc_history"
                        )
                        patch["lifecycle"]["close_result"] = {
                            "outcome": "pending",
                            "close_type": pending_close_type,
                        }
                        pending_reason = (
                            "position closed and stale orders cleaned; waiting for confirmed TP notification delivery"
                            if pending_tp_notifications and terminal_history
                            else "position closed and stale orders cleaned; waiting for delayed BingX TP/SL history"
                        )
                        await _write_status(
                            "closed_pending_history",
                            pending_reason,
                            patch,
                        )
                        log.info(
                            "close history pending execution_id=%s user_id=%s symbol=%s deadline_sec=%s",
                            execution_id,
                            user_id,
                            symbol,
                            timeout_sec,
                        )
                        processed += 1
                        continue

                    cleanup_verified = cleanup.get("verified_clean") is True
                    if cleanup_verified:
                        new_status = "closed_on_exchange_cleanup"
                        reason = (
                            "position closed on exchange; stale TP/SL orders verified absent after a transient cancel error"
                            if critical_cancel_failed
                            else "position closed on exchange; stale TP/SL orders cancelled and verified clean"
                        )
                    else:
                        # Position exposure is already confirmed zero. Keep only a
                        # cleanup-pending terminal family status so portfolio/day
                        # risk and slots are released while the lifecycle worker
                        # continues exact-id cleanup. Same-symbol admission remains
                        # blocked until cleanup is verified.
                        new_status = "closed_on_exchange"
                        reason = "position closed on exchange; position risk released, stale TP/SL/order cleanup still needs exact verification"
                    financial_marker: dict[str, Any] | None = None
                    if new_status == "closed_on_exchange_cleanup":
                        financial_marker = _financial_marker_for_closed_execution(
                            execution_id=execution_id,
                            user_id=user_id,
                            exchange=exchange,
                            row=row,
                            payload=latest_payload,
                            symbol=symbol,
                            side=side,
                            original_qty=original_qty,
                            close_result=close_result,
                            history_rows=history_rows,
                            terminal_history_type=terminal_history_type,
                            now=now_utc,
                        )
                        if financial_marker is not None:
                            patch[FINANCIAL_RECONCILIATION_MARKER_KEY] = (
                                financial_marker
                            )
                    try:
                        patch["write_flow_audit_v1"] = build_write_flow_audit(
                            latest_payload,
                            status=new_status,
                            stage="lifecycle_cleanup",
                        )
                    except Exception as audit_exc:
                        patch["write_flow_audit_v1"] = {
                            "version": 1,
                            "stage": "lifecycle_cleanup",
                            "status": new_status,
                            "audit_error": f"{type(audit_exc).__name__}: {audit_exc}"[
                                :500
                            ],
                        }
                    await _write_status(new_status, reason, patch)
                    if new_status == "closed_on_exchange_cleanup":
                        await _enqueue_financial_marker_best_effort(
                            execution_id, financial_marker
                        )
                    if new_status == "closed_on_exchange":
                        await _notify_critical(
                            notify,
                            user_id,
                            card(
                                "🚨 <b>ОСТАТОЧНЫЕ ОРДЕРА НЕ УДАЛЕНЫ</b>",
                                symbol=symbol,
                                side=side,
                                blocks=(
                                    [
                                        "✅ Позиция на BingX уже закрыта",
                                        "❌ Точная очистка ордеров этой сделки не подтверждена",
                                        f"🎯 <b>Неудалённые tracked TP/STOP:</b> {len(cleanup.get('remaining_tracked_algo_ids') or [])}",
                                        f"🧾 <b>Неудалённые tracked ENTRY:</b> {len(cleanup.get('remaining_tracked_regular_ids') or [])}",
                                        f"❓ <b>Неопознанные ордера той же позиции:</b> {len(cleanup.get('unidentified_relevant_algo_orders') or [])}",
                                    ],
                                    [
                                        "🔴 <b>Сделайте прямо сейчас:</b>",
                                        *tree_lines(
                                            [
                                                "Откройте BingX",
                                                "Сверьте symbol, сторону, positionId и цены",
                                                "Удалите только старые ордера именно этой сделки",
                                                "Не удаляйте вручную чужие или новые ордера по символу",
                                            ]
                                        ),
                                    ],
                                    [
                                        f"🔒 Несвязанные algo-ордера сохранены: {len(cleanup.get('unrelated_algo_orders') or [])}",
                                        f"🔒 Несвязанные regular-ордера сохранены: {len(cleanup.get('unrelated_open_orders') or [])}",
                                    ],
                                    [details_line(cleanup.get("errors"))],
                                ),
                            ),
                        )
                    else:
                        if close_result and close_result.get("message"):
                            await _notify_critical(
                                notify, user_id, str(close_result["message"])
                            )
                        else:
                            await _notify_critical(
                                notify,
                                user_id,
                                card(
                                    "✅ <b>ПОЗИЦИЯ ЗАКРЫТА</b>",
                                    symbol=symbol,
                                    side=side,
                                    blocks=(
                                        [
                                            "🧹 Ордеры этой сделки удалены точно по ID и проверены"
                                        ],
                                        [
                                            "⚠️ Итог сделки не удалось классифицировать автоматически"
                                        ],
                                    ),
                                ),
                            )
                    processed += 1
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
            try:
                stats = positions_context.stats()
                context_mode = "critical" if rows_override is not None else "full"
                log.info(
                    "LIFECYCLE_POSITIONS_CONTEXT mode=%s rows=%s %s",
                    context_mode,
                    len(rows),
                    " ".join(f"{key}={value}" for key, value in stats.items()),
                )
            except Exception:
                pass
            set_notification_event_key("")
            if owns_adapter_cache:
                for adapter in adapter_cache.values():
                    try:
                        await adapter.close()
                    except Exception:
                        pass


async def position_lifecycle_guard_loop(notify: NotifyFn | None = None) -> None:
    settings = get_settings()
    interval = max(5, int(getattr(settings, "MONITOR_ACTIVE_INTERVAL_SEC", 15) or 15))
    while True:
        try:
            await process_position_lifecycle_guard_once(notify=notify)
        except Exception:
            log.exception("position_lifecycle_guard_loop iteration failed")
        await asyncio.sleep(interval)
