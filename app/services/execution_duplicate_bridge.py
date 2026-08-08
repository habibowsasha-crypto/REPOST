"""Exact G59 repair for the reviewed duplicate execution pair 1581/1582.

The duplicate can arise when a concurrent lifecycle worker advances the durable
``opening_intent`` row before the original signal executor performs its final
CAS. G58 then inserted a fallback row. G59 fixes the future race in
``signal_executor`` and this bridge repairs only the already-reviewed pair.

No exchange call/write is performed. The stale row is superseded only when both
runtime rows still match the exact reviewed identity and share an exact entry
order/client identity. Otherwise the bridge fails closed and changes nothing.
"""

from __future__ import annotations

import asyncio
import json
import logging
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from app.config import get_settings
from app.database import db
from app.services.exchange_identity import clean_exchange_id
from app.services.stop_ownership import entry_order_ids

log = logging.getLogger(__name__)

G59_DUPLICATE_STATUS = "superseded_duplicate"
G59_DUPLICATE_KEY = "g59_duplicate_execution_bridge_v1"
G59_CANONICAL_KEY = "g59_duplicate_execution_canonical_v1"

_STALE_ID = 1581
_CANONICAL_ID = 1582
_TARGET_USER_ID = 6835564228
_TARGET_GROUP_ID = 2256
_TARGET_SYMBOL = "AAVEUSDT"
_TARGET_SIDE = "short"
_TARGET_QTY = Decimal("0.7")
_STALE_REASON = (
    "LIMIT TP coverage conflict on TP3: BingX TP qty after rounding is zero "
    "for AAVEUSDT short; TP write aborted"
)


def _payload(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _decimal(value: Any) -> Decimal | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _same_qty(value: Any) -> bool:
    parsed = _decimal(value)
    return parsed is not None and abs(parsed - _TARGET_QTY) <= Decimal("1e-9")


def _client_entry_ids(payload: Mapping[str, Any]) -> set[str]:
    out: set[str] = set()

    def add(value: Any) -> None:
        cleaned = clean_exchange_id(value)
        if cleaned:
            out.add(cleaned)

    intent = payload.get("entry_write_intent_v1")
    if isinstance(intent, Mapping):
        add(intent.get("clientOrderID"))
        add(intent.get("clientOrderId"))
    entry = payload.get("entry")
    if isinstance(entry, Mapping):
        add(entry.get("clientOrderID"))
        add(entry.get("clientOrderId"))
        for nested_key in ("data", "raw"):
            nested = entry.get(nested_key)
            if isinstance(nested, Mapping):
                add(nested.get("clientOrderID"))
                add(nested.get("clientOrderId"))
    return out


def _identity_ok(row: Mapping[str, Any], execution_id: int) -> bool:
    return bool(
        int(row.get("id") or 0) == execution_id
        and int(row.get("user_id") or 0) == _TARGET_USER_ID
        and int(row.get("trade_group_id") or 0) == _TARGET_GROUP_ID
        and str(row.get("symbol") or "").strip().upper() == _TARGET_SYMBOL
        and str(row.get("side") or "").strip().lower() == _TARGET_SIDE
        and _same_qty(row.get("qty"))
    )


def _shared_entry_identity(
    stale_payload: Mapping[str, Any], canonical_payload: Mapping[str, Any]
) -> tuple[str, list[str]]:
    shared_order_ids = sorted(
        entry_order_ids(dict(stale_payload)) & entry_order_ids(dict(canonical_payload))
    )
    if shared_order_ids:
        return "exchange_order_id", shared_order_ids
    shared_client_ids = sorted(
        _client_entry_ids(stale_payload) & _client_entry_ids(canonical_payload)
    )
    if shared_client_ids:
        return "client_order_id", shared_client_ids
    return "", []


async def _merge_canonical_marker(
    row: Mapping[str, Any], marker: Mapping[str, Any]
) -> bool:
    execution_id = int(row["id"])
    for _ in range(3):
        latest = await db.get_execution_by_id(execution_id)
        if not latest:
            return False
        status = str(latest.get("status") or "")
        if status == G59_DUPLICATE_STATUS:
            return False
        if await db.merge_execution_metadata(
            execution_id,
            {G59_CANONICAL_KEY: dict(marker)},
            expected_status=status,
            write_flow_audit_stage="g59_duplicate_canonical_link",
            write_flow_audit_status=status,
        ):
            return True
    return False


async def recover_exact_duplicate_execution_once() -> dict[str, int]:
    counters = {
        "scanned": 0,
        "superseded": 0,
        "already_completed": 0,
        "blocked_identity": 0,
        "blocked_status": 0,
        "blocked_financial_job": 0,
        "blocked_entry_identity": 0,
        "canonical_marker_conflict": 0,
        "write_conflict": 0,
        "missing": 0,
        "errors": 0,
    }
    if not bool(get_settings().FINANCIAL_RECONCILIATION_ENABLED):
        return counters

    try:
        stale = await db.get_execution_by_id(_STALE_ID)
        canonical = await db.get_execution_by_id(_CANONICAL_ID)
        if not stale or not canonical:
            counters["missing"] = 1
            return counters
        counters["scanned"] = 1
        stale_payload = _payload(stale.get("exchange_order_ids_json"))
        previous = stale_payload.get(G59_DUPLICATE_KEY)
        if str(stale.get("status") or "") == G59_DUPLICATE_STATUS or (
            isinstance(previous, Mapping) and previous.get("completed") is True
        ):
            counters["already_completed"] = 1
            return counters

        if not _identity_ok(stale, _STALE_ID) or not _identity_ok(
            canonical, _CANONICAL_ID
        ):
            counters["blocked_identity"] = 1
            return counters
        if str(stale.get("status") or "") != "manual_required" or str(
            stale.get("reason") or ""
        ) != _STALE_REASON:
            counters["blocked_status"] = 1
            return counters

        stale_job = await db.get_financial_reconciliation_job(execution_id=_STALE_ID)
        if stale_job:
            counters["blocked_financial_job"] = 1
            return counters

        canonical_payload = _payload(canonical.get("exchange_order_ids_json"))
        identity_kind, shared_ids = _shared_entry_identity(
            stale_payload, canonical_payload
        )
        if not identity_kind or not shared_ids:
            counters["blocked_entry_identity"] = 1
            log.warning(
                "G59_DUPLICATE_EXECUTION_IDENTITY_BLOCKED stale_id=%s canonical_id=%s",
                _STALE_ID,
                _CANONICAL_ID,
            )
            return counters

        marker = {
            "version": 1,
            "completed": True,
            "stale_execution_id": _STALE_ID,
            "canonical_execution_id": _CANONICAL_ID,
            "user_id": _TARGET_USER_ID,
            "trade_group_id": _TARGET_GROUP_ID,
            "symbol": _TARGET_SYMBOL,
            "side": _TARGET_SIDE,
            "qty": str(_TARGET_QTY),
            "shared_entry_identity_kind": identity_kind,
            "shared_entry_identity": shared_ids[:5],
            "original_stale_status": str(stale.get("status") or ""),
            "original_stale_reason": str(stale.get("reason") or "")[:500],
            "exchange_reads_performed": 0,
            "exchange_writes_performed": 0,
        }
        if not await _merge_canonical_marker(canonical, marker):
            counters["canonical_marker_conflict"] = 1
            return counters

        written = await db.update_execution_status_merge(
            _STALE_ID,
            G59_DUPLICATE_STATUS,
            f"G59 exact duplicate superseded by execution {_CANONICAL_ID}; shared {identity_kind}",
            {G59_DUPLICATE_KEY: marker},
            expected_status="manual_required",
            write_flow_audit_stage="g59_duplicate_superseded",
            write_flow_audit_status=G59_DUPLICATE_STATUS,
        )
        if not written:
            counters["write_conflict"] = 1
            return counters
        counters["superseded"] = 1
        log.info(
            "G59_DUPLICATE_EXECUTION_SUPERSEDED stale_id=%s canonical_id=%s identity_kind=%s shared_ids=%s",
            _STALE_ID,
            _CANONICAL_ID,
            identity_kind,
            shared_ids[:5],
        )
        return counters
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        counters["errors"] += 1
        log.exception(
            "G59_DUPLICATE_EXECUTION_BRIDGE_FAILED stale_id=%s canonical_id=%s error_type=%s error=%s",
            _STALE_ID,
            _CANONICAL_ID,
            type(exc).__name__,
            str(exc)[:300],
        )
        return counters
