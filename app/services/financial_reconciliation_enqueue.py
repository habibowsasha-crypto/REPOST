"""Prepare and durably enqueue terminal financial reconciliation jobs.

This module is intentionally exchange-read free.  It snapshots only exact
identities that are already durable in the execution payload after the position
has been proven closed and cleanup has completed.  The low-priority worker then
performs all BingX ``fillHistory`` reads outside the trading/lifecycle hot path.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping

from app.config import get_settings
from app.database import db
from app.services.exchange_identity import clean_exchange_id
from app.services.financial_reconciliation_models import (
    FINANCIAL_FINAL_CLOSE_TYPES,
    ORDER_ROLE_BE_STOP,
    ORDER_ROLE_ENTRY,
    ORDER_ROLE_FINAL_TP,
    ORDER_ROLE_STOP,
    ORDER_ROLE_TP,
    FinancialOrderExpectation,
    normalize_order_expectations,
)

log = logging.getLogger(__name__)

MARKER_KEY = "financial_reconciliation_enqueue_v1"
_MARKER_VERSION = 1
_RETRY_DELAYS_SEC = (5, 30, 120, 300, 900)

_REGULAR_ID_KEYS = (
    "_confirmed_order_id",
    "orderId",
    "orderID",
    "order_id",
    "placeOrderId",
    "delegatedOrderId",
    "childOrderId",
    "id",
)
_PLAN_ID_KEYS = (
    "_confirmed_stop_plan_id",
    "stopPlanOrderId",
    "stopOrderId",
    "planOrderId",
    "triggerOrderId",
    "algoOrderId",
)
_CLIENT_ID_KEYS = (
    "clientOrderID",
    "clientOrderId",
    "client_order_id",
    "externalOid",
    "origClientOrderID",
    "origClientOrderId",
    "orig_client_order_id",
)
_CHILD_KEYS = (
    "data",
    "raw",
    "order",
    "trigger",
    "detail",
    "result",
    "verification",
    "identity_recovery_v1",
    "identities",
    "identity_aliases",
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value).strip().replace("Z", "+00:00")
        if " " in raw and "T" not in raw:
            raw = raw.replace(" ", "T", 1)
        try:
            parsed = datetime.fromisoformat(raw)
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _decimal(value: Any, *, positive: bool = False) -> Decimal | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not parsed.is_finite():
        return None
    if positive and parsed <= 0:
        return None
    return parsed


def _decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in {"", "-0"} else text


def _walk_scopes(value: Any, *, max_depth: int = 6) -> list[Mapping[str, Any]]:
    scopes: list[Mapping[str, Any]] = []
    seen: set[int] = set()

    def walk(item: Any, depth: int) -> None:
        if depth > max_depth or not isinstance(item, Mapping) or id(item) in seen:
            return
        seen.add(id(item))
        scopes.append(item)
        for key in _CHILD_KEYS:
            child = item.get(key)
            if isinstance(child, Mapping):
                walk(child, depth + 1)

    walk(value, 0)
    return scopes


def _first_identity(value: Any, keys: Iterable[str]) -> str:
    for scope in _walk_scopes(value):
        for key in keys:
            cleaned = clean_exchange_id(scope.get(key))
            if cleaned:
                return cleaned
        scalar_data = scope.get("data")
        if not isinstance(scalar_data, (Mapping, list, tuple, set)):
            cleaned = clean_exchange_id(scalar_data)
            if cleaned:
                return cleaned
    return ""


def _all_identities(value: Any, keys: Iterable[str]) -> set[str]:
    identities: set[str] = set()
    for scope in _walk_scopes(value):
        for key in keys:
            raw = scope.get(key)
            candidates = raw if isinstance(raw, (list, tuple, set)) else (raw,)
            for candidate in candidates:
                cleaned = clean_exchange_id(candidate)
                if cleaned:
                    identities.add(cleaned)
    return identities


def _order_identity(value: Any, *, allow_plan_fallback: bool) -> tuple[str, str]:
    regular = _first_identity(value, _REGULAR_ID_KEYS)
    if regular:
        return regular, "regular"
    if allow_plan_fallback:
        plan = _first_identity(value, _PLAN_ID_KEYS)
        if plan:
            return plan, "plan_fallback"
    return "", "missing"


def _client_identity(value: Any) -> str:
    return _first_identity(value, _CLIENT_ID_KEYS)


def _tp_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = payload.get("tp")
    rows = [dict(item) for item in raw if isinstance(item, Mapping)] if isinstance(raw, list) else []
    rows.sort(key=lambda item: int(item.get("tp_index") or item.get("index") or 999999))
    return rows


def _tp_qty(row: Mapping[str, Any]) -> Decimal | None:
    for key in ("exchange_filled_qty", "actual_filled_qty", "filled_qty", "qty", "actual_tp_qty"):
        parsed = _decimal(row.get(key), positive=True)
        if parsed is not None:
            return parsed
    order = row.get("order") if isinstance(row.get("order"), Mapping) else row
    for scope in _walk_scopes(order):
        for key in ("executedQty", "filledQty", "realityVol", "_submitted_quantity", "origQty", "quantity", "qty"):
            parsed = _decimal(scope.get(key), positive=True)
            if parsed is not None:
                return parsed
    return None


def _tp_fill_order_identity(row: Mapping[str, Any]) -> tuple[str, str]:
    """Return the exact terminal fill order id persisted by lifecycle.

    BingX can expose a conditional TP through a bot-owned plan id and execute it
    through a different delegated child order.  ``fillHistory`` is keyed by the
    child fill order, not necessarily by the parent plan.  Lifecycle persists
    that exact terminal identity only after the existing plan -> child ownership
    bridge has succeeded.  Older rows may contain the same bounded evidence in
    ``exchange_fill_check``; accept it only when that check explicitly proved an
    owned terminal fill.
    """

    snapshot = row.get("financial_fill_identity_v1")
    if isinstance(snapshot, Mapping) and int(snapshot.get("version") or 0) == 1:
        order_id = clean_exchange_id(snapshot.get("exchange_order_id"))
        mode = str(snapshot.get("identity_match_mode") or "")
        if (
            order_id
            and snapshot.get("ownership_confirmed") is True
            and mode
            in {"direct_exact_terminal_fill", "transitive_exact_child_fill"}
        ):
            return order_id, "terminal_fill_snapshot"

    audit = row.get("exchange_fill_check")
    if not isinstance(audit, Mapping):
        return "", "missing"
    if (
        audit.get("accepted_as_owned_tp") is not True
        or audit.get("terminal_filled") is not True
    ):
        return "", "missing"
    mode = str(audit.get("identity_match_mode") or "")
    if mode not in {"direct_exact_terminal_fill", "transitive_exact_child_fill"}:
        return "", "missing"
    order_id = clean_exchange_id(audit.get("terminal_fill_order_id"))
    if not order_id:
        # g5b3g8 stored the terminal history row's orderId under this legacy
        # diagnostic name.  It is safe only behind the exact ownership gates
        # above; arbitrary delegated/plan ids are never accepted here.
        order_id = clean_exchange_id(audit.get("delegated_order_id"))
    return (order_id, "terminal_fill_audit") if order_id else ("", "missing")


def _regular_tp_fill_order_identity(value: Any) -> tuple[str, str]:
    """Return a regular child order id that is provably not a TP plan id.

    Older payloads often store the parent trigger-plan id in both
    ``_confirmed_order_id`` and ``stopPlanOrderId``. Treating the former as a
    regular order id recreates the partial-accounting defect that the terminal
    child snapshot is meant to solve. A regular fallback is accepted only when
    its identity is distinct from every plan identity in the same payload.
    """

    plan_ids = _all_identities(value, _PLAN_ID_KEYS)
    for scope in _walk_scopes(value):
        for key in _REGULAR_ID_KEYS:
            candidate = clean_exchange_id(scope.get(key))
            if candidate and candidate not in plan_ids:
                return candidate, "regular"
        scalar_data = scope.get("data")
        if not isinstance(scalar_data, (Mapping, list, tuple, set)):
            candidate = clean_exchange_id(scalar_data)
            if candidate and candidate not in plan_ids:
                return candidate, "regular"
    return "", "missing"


def _entry_expectation(payload: Mapping[str, Any], original_qty: Decimal) -> tuple[dict[str, Any] | None, str | None]:
    entry = payload.get("entry") if isinstance(payload.get("entry"), Mapping) else {}
    intent = (
        payload.get("entry_write_intent_v1")
        if isinstance(payload.get("entry_write_intent_v1"), Mapping)
        else {}
    )
    exchange_order_id, identity_kind = _order_identity(entry, allow_plan_fallback=False)
    client_order_id = _client_identity(entry) or _client_identity(intent)
    # The financial worker intentionally queries fillHistory only by exact exchange
    # orderId.  A client id is useful diagnostic metadata but is not sufficient
    # to create a runnable accounting job.
    if not exchange_order_id:
        return None, "entry_exact_exchange_order_id_missing"
    return (
        {
            "exchange_order_id": exchange_order_id or None,
            "client_order_id": client_order_id or None,
            "role": ORDER_ROLE_ENTRY,
            "tp_index": 0,
            "required": True,
            "expected_qty": _decimal_text(original_qty),
            "metadata": {
                "source": "terminal_execution_payload",
                "identity_kind": identity_kind,
                "order_type": str(intent.get("order_type") or entry.get("type") or "").upper(),
            },
        },
        None,
    )


def prepare_financial_reconciliation_marker(
    *,
    execution_id: int,
    user_id: int,
    exchange: str,
    symbol: str,
    side: str,
    close_type: str,
    strategy_gross_pnl: Any,
    original_qty: Any,
    payload: Mapping[str, Any],
    terminal_order_id: Any = None,
    terminal_order_client_id: Any = None,
    terminal_order_expected_qty: Any = None,
    terminal_at: Any = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Create a self-contained durable enqueue marker.

    The marker is safe to persist in ``exchange_order_ids_json`` before the
    actual queue insert.  If the process stops between those writes, the
    financial worker can recover the enqueue later without inspecting exchange
    state or reclassifying the close.
    """

    current = now or _utcnow()
    normalized_close = str(close_type or "").strip().lower()
    blockers: list[str] = []
    if normalized_close not in FINANCIAL_FINAL_CLOSE_TYPES:
        blockers.append("unsupported_close_type")
    normalized_side = str(side or "").strip().lower()
    if normalized_side not in {"long", "short"}:
        blockers.append("invalid_side")
    qty = _decimal(original_qty, positive=True)
    if qty is None:
        blockers.append("original_qty_missing")
    gross = _decimal(strategy_gross_pnl)
    if gross is None:
        blockers.append("strategy_gross_pnl_invalid")

    expectations: list[dict[str, Any]] = []
    if qty is not None:
        entry, entry_error = _entry_expectation(payload, qty)
        if entry is not None:
            expectations.append(entry)
        elif entry_error:
            blockers.append(entry_error)

    filled_tp_rows = [
        row for row in _tp_rows(payload) if row.get("filled") is True
    ]
    filled_total = Decimal("0")
    tp_expectations: list[dict[str, Any]] = []
    for fallback_index, row in enumerate(filled_tp_rows, 1):
        try:
            tp_index = int(row.get("tp_index") or row.get("index") or fallback_index)
        except (TypeError, ValueError, OverflowError):
            tp_index = 0
        if tp_index <= 0:
            blockers.append("tp_index_invalid")
            continue
        tp_qty = _tp_qty(row)
        if tp_qty is None:
            blockers.append(f"tp{tp_index}_expected_qty_missing")
            continue
        order_payload = row.get("order") if isinstance(row.get("order"), Mapping) else row
        order_id, identity_kind = _tp_fill_order_identity(row)
        if not order_id:
            order_id, identity_kind = _regular_tp_fill_order_identity(order_payload)
        client_id = _client_identity(order_payload)
        # A trigger/plan id is not the exact child fill-order id accepted by
        # fillHistory.  Keep the marker blocked instead of creating a job that
        # can only expire as unavailable.
        if not order_id or identity_kind not in {
            "regular",
            "terminal_fill_snapshot",
            "terminal_fill_audit",
        }:
            blockers.append(f"tp{tp_index}_exact_exchange_order_id_missing")
            continue
        filled_total += tp_qty
        tp_expectations.append(
            {
                "exchange_order_id": order_id or None,
                "client_order_id": client_id or None,
                "role": ORDER_ROLE_TP,
                "tp_index": tp_index,
                "required": True,
                "expected_qty": _decimal_text(tp_qty),
                "metadata": {
                    "source": "terminal_tp_ledger",
                    "identity_kind": identity_kind,
                    "target": row.get("target") or row.get("price"),
                    "filled_at": row.get("filled_at") or row.get("filled_detected_at"),
                },
            }
        )

    if qty is not None:
        tolerance = max(Decimal("1e-12"), abs(qty) * Decimal("1e-9"))
        if filled_total > qty + tolerance:
            blockers.append("filled_tp_qty_exceeds_original_qty")

    if normalized_close == "all_tps":
        if not tp_expectations:
            blockers.append("all_tps_without_confirmed_tp_orders")
        elif qty is not None and abs(filled_total - qty) > max(Decimal("1e-12"), abs(qty) * Decimal("1e-6")):
            blockers.append("all_tps_qty_not_equal_original_qty")
        if tp_expectations:
            final_index = max(int(item["tp_index"]) for item in tp_expectations)
            for item in tp_expectations:
                if int(item["tp_index"]) == final_index:
                    item["role"] = ORDER_ROLE_FINAL_TP
        expectations.extend(tp_expectations)
    elif normalized_close in {"stop", "be_stop"}:
        expectations.extend(tp_expectations)
        residual = _decimal(terminal_order_expected_qty, positive=True)
        if residual is None and qty is not None:
            residual = qty - filled_total
            if residual <= 0:
                residual = None
        terminal_id = clean_exchange_id(terminal_order_id)
        terminal_client = clean_exchange_id(terminal_order_client_id)
        if residual is None:
            blockers.append("terminal_close_expected_qty_missing")
        if not terminal_id:
            blockers.append("terminal_close_exact_exchange_order_id_missing")
        if residual is not None and terminal_id:
            expectations.append(
                {
                    "exchange_order_id": terminal_id or None,
                    "client_order_id": terminal_client or None,
                    "role": ORDER_ROLE_BE_STOP if normalized_close == "be_stop" else ORDER_ROLE_STOP,
                    "tp_index": 0,
                    "required": True,
                    "expected_qty": _decimal_text(residual),
                    "metadata": {
                        "source": "terminal_close_history",
                        "identity_kind": "regular",
                    },
                }
            )

    normalized_expectations: list[FinancialOrderExpectation] = []
    if not blockers:
        try:
            normalized_expectations = normalize_order_expectations(expectations)
        except (TypeError, ValueError) as exc:
            blockers.append(f"expectation_validation_failed:{type(exc).__name__}:{str(exc)[:200]}")

    state = "ready" if not blockers and normalized_expectations else "blocked"
    terminal_dt = _parse_datetime(terminal_at) or current
    return {
        "version": _MARKER_VERSION,
        "state": state,
        "execution_id": int(execution_id),
        "user_id": int(user_id),
        "exchange": str(exchange or "bingx").strip().lower(),
        "symbol": str(symbol or "").strip().upper(),
        "side": normalized_side,
        "close_type": normalized_close,
        "strategy_gross_pnl": _decimal_text(gross or Decimal("0")),
        "terminal_at": terminal_dt.isoformat(),
        "prepared_at": current.isoformat(),
        "next_attempt_at": current.isoformat(),
        "attempts": 0,
        "job_id": None,
        "blockers": blockers,
        "order_expectations": [
            {
                "exchange_order_id": item.exchange_order_id,
                "client_order_id": item.client_order_id,
                "role": item.role,
                "tp_index": item.tp_index,
                "required": item.required,
                "expected_qty": item.expected_qty,
                "metadata_json": item.metadata_json,
            }
            for item in normalized_expectations
        ],
    }


def _marker_from_row(row: Mapping[str, Any]) -> dict[str, Any] | None:
    raw = row.get("exchange_order_ids_json")
    if isinstance(raw, str):
        try:
            payload = json.loads(raw or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
    elif isinstance(raw, Mapping):
        payload = dict(raw)
    else:
        return None
    marker = payload.get(MARKER_KEY)
    return dict(marker) if isinstance(marker, Mapping) else None


def _retry_marker(marker: Mapping[str, Any], *, error: str, now: datetime) -> dict[str, Any]:
    try:
        attempts = max(0, int(marker.get("attempts") or 0)) + 1
    except (TypeError, ValueError, OverflowError):
        attempts = 1
    delay = _RETRY_DELAYS_SEC[min(attempts - 1, len(_RETRY_DELAYS_SEC) - 1)]
    return {
        **dict(marker),
        "state": "retry",
        "attempts": attempts,
        "last_error": str(error or "enqueue_failed")[:500],
        "last_attempt_at": now.isoformat(),
        "next_attempt_at": (now + timedelta(seconds=delay)).isoformat(),
    }


async def enqueue_prepared_financial_reconciliation(
    execution_id: int,
    *,
    expected_status: str = "closed_on_exchange_cleanup",
    now: datetime | None = None,
) -> dict[str, Any] | None:
    """Idempotently enqueue one prepared terminal marker.

    Failures are recorded back into the marker for bounded recovery by the
    low-priority worker.  No exchange request is performed here.
    """

    if not get_settings().FINANCIAL_RECONCILIATION_ENABLED:
        return None
    current = now or _utcnow()
    row = await db.get_execution_by_id(int(execution_id))
    if not row or str(row.get("status") or "") != str(expected_status):
        return None
    marker = _marker_from_row(row)
    if not marker or int(marker.get("version") or 0) != _MARKER_VERSION:
        return None
    if str(marker.get("state") or "") in {"queued", "blocked"}:
        return marker
    due = _parse_datetime(marker.get("next_attempt_at"))
    if due is not None and due > current:
        return marker
    if str(marker.get("state") or "") not in {"ready", "retry"}:
        return marker

    try:
        job = await db.enqueue_financial_reconciliation_job(
            execution_id=int(marker["execution_id"]),
            user_id=int(marker["user_id"]),
            exchange=str(marker.get("exchange") or "bingx"),
            symbol=str(marker["symbol"]),
            side=str(marker["side"]),
            close_type=str(marker["close_type"]),
            strategy_gross_pnl=marker.get("strategy_gross_pnl") or "0",
            order_expectations=list(marker.get("order_expectations") or []),
            terminal_at=marker.get("terminal_at"),
            deadline_after_sec=float(get_settings().FINANCIAL_RECONCILIATION_DEADLINE_SEC),
        )
        queued = {
            **marker,
            "state": "queued",
            "job_id": int(job.get("id") or 0),
            "queued_at": current.isoformat(),
            "last_error": None,
        }
        saved = await db.merge_execution_metadata(
            int(execution_id),
            {MARKER_KEY: queued},
            expected_status=expected_status,
        )
        if not saved:
            log.warning(
                "FINANCIAL_RECONCILIATION_MARKER_ACK_MISSED execution_id=%s job_id=%s",
                execution_id,
                queued.get("job_id"),
            )
        return queued
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        retry = _retry_marker(
            marker,
            error=f"{type(exc).__name__}:{exc}",
            now=current,
        )
        try:
            await db.merge_execution_metadata(
                int(execution_id),
                {MARKER_KEY: retry},
                expected_status=expected_status,
            )
        except Exception:
            log.exception(
                "FINANCIAL_RECONCILIATION_ENQUEUE_RETRY_MARKER_FAILED execution_id=%s",
                execution_id,
            )
        log.warning(
            "FINANCIAL_RECONCILIATION_ENQUEUE_RETRY execution_id=%s attempts=%s error=%s",
            execution_id,
            retry.get("attempts"),
            retry.get("last_error"),
        )
        return retry


async def recover_pending_financial_reconciliation_enqueues(*, limit: int = 20) -> int:
    """Recover durable markers left between terminal status and queue insert."""

    if not get_settings().FINANCIAL_RECONCILIATION_ENABLED:
        return 0
    rows = await db.pending_financial_reconciliation_enqueue_rows(limit=limit)
    recovered = 0
    for row in rows:
        try:
            result = await enqueue_prepared_financial_reconciliation(
                int(row.get("id") or 0),
                expected_status="closed_on_exchange_cleanup",
            )
            if result and str(result.get("state") or "") == "queued":
                recovered += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception(
                "FINANCIAL_RECONCILIATION_ENQUEUE_RECOVERY_FAILED execution_id=%s",
                row.get("id"),
            )
    return recovered


__all__ = [
    "MARKER_KEY",
    "prepare_financial_reconciliation_marker",
    "enqueue_prepared_financial_reconciliation",
    "recover_pending_financial_reconciliation_enqueues",
]
