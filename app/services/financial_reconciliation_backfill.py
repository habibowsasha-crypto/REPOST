"""Bounded exact-identity recovery for terminal executions missing a financial job.

G54 fixes a narrow statistics gap: a ``trade_executions`` row can already be
terminal and linked to statistics while the durable financial marker/job was
never created (for example, a deploy between terminal persistence and enqueue,
or a row closed before the financial feature was enabled).

The recovery is deliberately fail-closed:
- only ``closed_on_exchange_cleanup`` executions are considered;
- signal lifecycle is never used to infer execution closure;
- only the execution's own durable ``close_type`` is accepted;
- only exact exchange order identities already persisted in the execution JSON
  are used;
- no exchange write is performed and no PnL is invented.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from app.config import get_settings
from app.database import db
from app.services.exchange_identity import clean_exchange_id
from app.services.financial_reconciliation_enqueue import (
    MARKER_KEY,
    enqueue_prepared_financial_reconciliation,
    prepare_financial_reconciliation_marker,
)
from app.services.financial_reconciliation_models import (
    FINANCIAL_FINAL_CLOSE_TYPES,
    ORDER_ROLE_BE_STOP,
    ORDER_ROLE_STOP,
)

log = logging.getLogger(__name__)

RECOVERY_KEY = "g54_terminal_orphan_backfill_v1"
_ALLOWED_IDENTITY_MODES = {
    "direct_exact_terminal_fill",
    "transitive_exact_child_fill",
    "terminal_fill_snapshot",
    "terminal_fill_audit",
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _positive_decimal(value: Any) -> Decimal | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() and parsed > 0 else None


def _exact_terminal_expectation_from_marker(
    marker: Mapping[str, Any], close_type: str
) -> tuple[str, str, Decimal | None]:
    wanted_role = ORDER_ROLE_BE_STOP if close_type == "be_stop" else ORDER_ROLE_STOP
    for item in marker.get("order_expectations") or []:
        if not isinstance(item, Mapping):
            continue
        if str(item.get("role") or "").strip().lower() != wanted_role:
            continue
        order_id = clean_exchange_id(item.get("exchange_order_id"))
        qty = _positive_decimal(item.get("expected_qty"))
        if order_id and qty is not None:
            return order_id, clean_exchange_id(item.get("client_order_id")), qty
    return "", "", None


def _walk(value: Any, path: tuple[str, ...] = (), depth: int = 0):
    if depth > 8:
        return
    if isinstance(value, Mapping):
        yield path, value
        for key, child in value.items():
            if isinstance(child, (Mapping, list, tuple)):
                yield from _walk(child, (*path, str(key)), depth + 1)
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            if isinstance(child, (Mapping, list, tuple)):
                yield from _walk(child, (*path, str(index)), depth + 1)


def _exact_terminal_expectation_from_payload(
    payload: Mapping[str, Any], close_type: str
) -> tuple[str, str, Decimal | None, str]:
    """Accept only an explicitly owned terminal STOP/BE fill snapshot."""

    wanted_role = ORDER_ROLE_BE_STOP if close_type == "be_stop" else ORDER_ROLE_STOP
    candidates: list[tuple[str, str, Decimal, str]] = []
    for path, scope in _walk(payload):
        path_text = ".".join(path).lower()
        role = str(scope.get("role") or scope.get("close_type") or "").strip().lower()
        ownership = scope.get("ownership_confirmed") is True
        mode = str(scope.get("identity_match_mode") or scope.get("identity_kind") or "").strip()
        terminal_context = any(token in path_text for token in ("terminal", "close", "stop", "be"))
        if role not in {wanted_role, close_type}:
            continue
        if not terminal_context:
            continue
        if not ownership or mode not in _ALLOWED_IDENTITY_MODES:
            continue
        order_id = clean_exchange_id(
            scope.get("exchange_order_id")
            or scope.get("terminal_fill_order_id")
            or scope.get("orderId")
            or scope.get("order_id")
        )
        qty = _positive_decimal(
            scope.get("expected_qty")
            or scope.get("qty")
            or scope.get("filled_qty")
            or scope.get("executedQty")
        )
        if not order_id or qty is None:
            continue
        client_id = clean_exchange_id(
            scope.get("client_order_id")
            or scope.get("clientOrderId")
            or scope.get("clientOrderID")
        )
        candidates.append((order_id, client_id, qty, path_text or "root"))
    unique = {(item[0], item[1], str(item[2])) for item in candidates}
    if len(unique) != 1:
        return "", "", None, (
            "exact_terminal_snapshot_missing" if not candidates else "exact_terminal_snapshot_ambiguous"
        )
    order_id, client_id, qty, source = candidates[0]
    return order_id, client_id, qty, source


def _close_type(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in FINANCIAL_FINAL_CLOSE_TYPES else ""


async def _candidate_rows(limit: int) -> list[dict[str, Any]]:
    bounded = max(1, min(int(limit or 20), 100))
    if db.is_postgres():
        async with db.connect() as conn:
            rows = await conn.fetch(
                """
                SELECT e.*,r.financial_state,r.projection_status,
                       j.id AS financial_job_id,j.status AS financial_job_status
                FROM trade_executions e
                JOIN analytics_execution_results r ON r.execution_id=e.id
                LEFT JOIN financial_reconciliation_jobs j ON j.execution_id=e.id
                WHERE e.status='closed_on_exchange_cleanup'
                  AND UPPER(COALESCE(r.financial_state,'PENDING'))<>'FINAL'
                  AND j.id IS NULL
                  AND COALESCE(e.exchange_order_ids_json,'') NOT LIKE $1
                ORDER BY e.id ASC
                LIMIT $2
                """,
                f'%"{RECOVERY_KEY}"%',
                bounded,
            )
            return [dict(row) for row in rows]
    async with db.connect() as conn:
        cursor = await conn.execute(
            """
            SELECT e.*,r.financial_state,r.projection_status,
                   j.id AS financial_job_id,j.status AS financial_job_status
            FROM trade_executions e
            JOIN analytics_execution_results r ON r.execution_id=e.id
            LEFT JOIN financial_reconciliation_jobs j ON j.execution_id=e.id
            WHERE e.status='closed_on_exchange_cleanup'
              AND UPPER(COALESCE(r.financial_state,'PENDING'))<>'FINAL'
              AND j.id IS NULL
              AND COALESCE(e.exchange_order_ids_json,'') NOT LIKE ?
            ORDER BY e.id ASC
            LIMIT ?
            """,
            (f'%"{RECOVERY_KEY}"%', bounded),
        )
        return [dict(row) for row in await cursor.fetchall()]


async def recover_terminal_financial_backfill_once(*, limit: int = 20) -> dict[str, int]:
    """Rebuild/enqueue exact terminal jobs missing from the durable queue."""

    counters = {
        "scanned": 0,
        "queued": 0,
        "ready": 0,
        "blocked": 0,
        "skipped_already_audited": 0,
        "skipped_close_type": 0,
        "write_conflict": 0,
        "errors": 0,
    }
    if not bool(get_settings().FINANCIAL_RECONCILIATION_ENABLED):
        return counters
    rows = await _candidate_rows(limit)
    now = _utcnow()
    for row in rows:
        counters["scanned"] += 1
        execution_id = int(row.get("id") or 0)
        try:
            payload = _json_mapping(row.get("exchange_order_ids_json"))
            previous_audit = payload.get(RECOVERY_KEY)
            existing_marker = payload.get(MARKER_KEY)
            if (
                isinstance(previous_audit, Mapping)
                and previous_audit.get("completed") is True
            ):
                counters["skipped_already_audited"] += 1
                continue

            close_type = _close_type(row.get("close_type"))
            if not close_type:
                audit = {
                    "version": 1,
                    "completed": True,
                    "result": "blocked",
                    "reason": "execution_close_type_not_exact_terminal",
                    "runtime_close_type": str(row.get("close_type") or ""),
                    "checked_at": now.isoformat(),
                }
                await db.merge_execution_metadata(
                    execution_id,
                    {RECOVERY_KEY: audit},
                    expected_status="closed_on_exchange_cleanup",
                )
                counters["skipped_close_type"] += 1
                continue

            original_qty = _positive_decimal(row.get("qty") or row.get("planned_entry_qty"))
            marker_before = dict(existing_marker) if isinstance(existing_marker, Mapping) else {}
            terminal_order_id = ""
            terminal_client_id = ""
            terminal_expected_qty: Decimal | None = None
            terminal_source = "not_required"
            if close_type in {"stop", "be_stop"}:
                terminal_order_id, terminal_client_id, terminal_expected_qty = (
                    _exact_terminal_expectation_from_marker(marker_before, close_type)
                )
                terminal_source = "existing_marker"
                if not terminal_order_id:
                    (
                        terminal_order_id,
                        terminal_client_id,
                        terminal_expected_qty,
                        terminal_source,
                    ) = _exact_terminal_expectation_from_payload(payload, close_type)

            marker = prepare_financial_reconciliation_marker(
                execution_id=execution_id,
                user_id=int(row.get("user_id") or 0),
                exchange="bingx",
                symbol=str(row.get("symbol") or ""),
                side=str(row.get("side") or ""),
                close_type=close_type,
                strategy_gross_pnl=row.get("realized_pnl") or 0,
                original_qty=original_qty,
                payload=payload,
                terminal_order_id=terminal_order_id,
                terminal_order_client_id=terminal_client_id,
                terminal_order_expected_qty=terminal_expected_qty,
                terminal_at=row.get("closed_at") or row.get("updated_at") or now,
                now=now,
            )
            marker["recovery_source"] = RECOVERY_KEY
            marker["terminal_identity_source"] = terminal_source
            audit = {
                "version": 1,
                "completed": True,
                "result": str(marker.get("state") or "blocked"),
                "close_type": close_type,
                "marker_blockers": list(marker.get("blockers") or []),
                "terminal_identity_source": terminal_source,
                "checked_at": now.isoformat(),
            }
            written = await db.merge_execution_metadata(
                execution_id,
                {MARKER_KEY: marker, RECOVERY_KEY: audit},
                expected_status="closed_on_exchange_cleanup",
            )
            if not written:
                counters["write_conflict"] += 1
                continue
            if str(marker.get("state") or "") != "ready":
                counters["blocked"] += 1
                log.warning(
                    "G54_TERMINAL_FINANCIAL_BACKFILL_BLOCKED execution_id=%s close_type=%s blockers=%s terminal_source=%s",
                    execution_id,
                    close_type,
                    marker.get("blockers"),
                    terminal_source,
                )
                continue
            counters["ready"] += 1
            queued = await enqueue_prepared_financial_reconciliation(
                execution_id,
                expected_status="closed_on_exchange_cleanup",
                now=now,
            )
            if queued and str(queued.get("state") or "") == "queued":
                counters["queued"] += 1
                log.info(
                    "G54_TERMINAL_FINANCIAL_BACKFILL_QUEUED execution_id=%s close_type=%s job_id=%s expectations=%s",
                    execution_id,
                    close_type,
                    queued.get("job_id"),
                    len(queued.get("order_expectations") or []),
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            counters["errors"] += 1
            log.exception(
                "G54_TERMINAL_FINANCIAL_BACKFILL_FAILED execution_id=%s",
                execution_id,
            )
    return counters


__all__ = ["RECOVERY_KEY", "recover_terminal_financial_backfill_once"]
