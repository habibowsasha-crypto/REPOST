"""Exact one-time bridge for execution 1478 into closed-history recovery.

Production evidence showed that execution 1478 (AAVEUSDT LONG, user 6835564228)
was a real live 0.7-contract position before the position later became zero and
exact stale-order cleanup was verified.  The generic manual-required cleanup
path intentionally retained the row in ``manual_required`` and therefore the
normal delayed BingX TP/SL-history reconciliation never ran.

G55 is deliberately narrow and fail-closed:
- only the exact reviewed execution identity is eligible;
- durable two-read zero exposure and verified-clean order cleanup are required;
- an existing financial job or a changed runtime identity blocks the bridge;
- the write only changes local lifecycle state to ``closed_pending_history``;
- no exchange request and no exchange write is performed here;
- existing lifecycle/financial workers remain responsible for exact BingX
  history, fills, fees, funding and FINAL projection.
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
from app.services.execution_exposure import (
    manual_required_zero_exposure_release_state,
)

log = logging.getLogger(__name__)

RECOVERY_KEY = "g55_exact_manual_history_bridge_v1"
PRIOR_LIVE_KEY = "g55_prior_live_position_evidence_v1"

_TARGET = {
    "execution_id": 1478,
    "trade_group_id": 2106,
    "user_id": 6835564228,
    "symbol": "AAVEUSDT",
    "side": "long",
    "qty": Decimal("0.7"),
    "entry": Decimal("92.29"),
    "stop": Decimal("87.67"),
    "created_at_prefix": "2026-08-04T11:48",
}

_PRODUCTION_EVIDENCE = {
    "source": "reviewed_production_log",
    "log_sha256": "541aedb4caea092824aee74f2b226dfd24733609790691cfccd8a08f6cbe29d0",
    "first_live_seen_at": "2026-08-06T08:19:52.547451+00:00",
    "last_live_seen_at": "2026-08-06T08:55:03.958131+00:00",
    "live_qty": "0.7",
    "reviewed_version": "1.0.7g7h2f5g5b3g53",
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


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


def _int_value(value: Any) -> int | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError, OverflowError):
        return None


def _same_decimal(value: Any, expected: Decimal) -> bool:
    parsed = _decimal(value)
    if parsed is None:
        return False
    tolerance = max(Decimal("1e-12"), abs(expected) * Decimal("1e-9"))
    return abs(parsed - expected) <= tolerance


def _created_at_matches(value: Any) -> bool:
    raw = str(value or "").strip().replace(" ", "T", 1)
    return raw.startswith(str(_TARGET["created_at_prefix"]))


def _identity_blockers(row: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    checks = (
        (_int_value(row.get("id")) == int(_TARGET["execution_id"]), "execution_id"),
        (
            _int_value(row.get("trade_group_id")) == int(_TARGET["trade_group_id"]),
            "trade_group_id",
        ),
        (_int_value(row.get("user_id")) == int(_TARGET["user_id"]), "user_id"),
        (
            str(row.get("symbol") or "").strip().upper() == _TARGET["symbol"],
            "symbol",
        ),
        (
            str(row.get("side") or "").strip().lower() == _TARGET["side"],
            "side",
        ),
        (_same_decimal(row.get("qty"), _TARGET["qty"]), "qty"),
        (_same_decimal(row.get("entry"), _TARGET["entry"]), "entry"),
        (_same_decimal(row.get("stop"), _TARGET["stop"]), "stop"),
        (_created_at_matches(row.get("created_at")), "created_at"),
    )
    blockers.extend(name for passed, name in checks if not passed)
    return blockers


async def _candidate_row() -> dict[str, Any] | None:
    execution_id = int(_TARGET["execution_id"])
    if db.is_postgres():
        async with db.connect() as conn:
            row = await conn.fetchrow(
                """
                SELECT e.*,j.id AS financial_job_id,j.status AS financial_job_status
                FROM trade_executions e
                LEFT JOIN financial_reconciliation_jobs j ON j.execution_id=e.id
                WHERE e.id=$1
                """,
                execution_id,
            )
            return dict(row) if row else None
    async with db.connect() as conn:
        cursor = await conn.execute(
            """
            SELECT e.*,j.id AS financial_job_id,j.status AS financial_job_status
            FROM trade_executions e
            LEFT JOIN financial_reconciliation_jobs j ON j.execution_id=e.id
            WHERE e.id=?
            """,
            (execution_id,),
        )
        row = await cursor.fetchone()
        return dict(row) if row else None


async def recover_exact_manual_history_bridge_once() -> dict[str, int]:
    """Bridge the exact reviewed AAVE execution into existing history recovery.

    The function is idempotent. It performs no exchange call and creates no
    financial job itself. The normal lifecycle worker will do the exact read-only
    BingX history probe after the status transition.
    """

    counters = {
        "scanned": 0,
        "bridged": 0,
        "already_completed": 0,
        "blocked_status": 0,
        "blocked_identity": 0,
        "blocked_financial_job": 0,
        "blocked_zero_cleanup": 0,
        "write_conflict": 0,
        "errors": 0,
    }
    if not bool(get_settings().FINANCIAL_RECONCILIATION_ENABLED):
        return counters

    row = await _candidate_row()
    if not row:
        return counters
    counters["scanned"] = 1
    execution_id = _int_value(row.get("id")) or 0
    try:
        payload = _payload(row.get("exchange_order_ids_json"))
        previous = payload.get(RECOVERY_KEY)
        if isinstance(previous, Mapping) and previous.get("completed") is True:
            counters["already_completed"] = 1
            return counters

        status = str(row.get("status") or "").strip().lower()
        if status != "manual_required":
            counters["blocked_status"] = 1
            return counters

        identity_blockers = _identity_blockers(row)
        if identity_blockers:
            counters["blocked_identity"] = 1
            log.error(
                "G55_EXACT_MANUAL_HISTORY_BRIDGE_IDENTITY_BLOCKED execution_id=%s blockers=%s",
                execution_id,
                identity_blockers,
            )
            return counters

        if row.get("financial_job_id") not in (None, "", 0, "0"):
            counters["blocked_financial_job"] = 1
            return counters

        release = manual_required_zero_exposure_release_state(row)
        cleanup_marker = (
            payload.get("critical_cleanup_reconcile_v1")
            if isinstance(payload.get("critical_cleanup_reconcile_v1"), Mapping)
            else {}
        )
        cleanup_ready = bool(
            release.get("eligible") is True
            and str(cleanup_marker.get("state") or "").strip().lower() == "resolved"
            and cleanup_marker.get("verified_clean") is True
        )
        if not cleanup_ready:
            counters["blocked_zero_cleanup"] = 1
            log.warning(
                "G55_EXACT_MANUAL_HISTORY_BRIDGE_ZERO_CLEANUP_BLOCKED execution_id=%s release_reason=%s cleanup_state=%s verified_clean=%s",
                execution_id,
                release.get("reason"),
                cleanup_marker.get("state"),
                cleanup_marker.get("verified_clean"),
            )
            return counters

        now = _utcnow()
        zero_proof = (
            payload.get("critical_zero_exposure_v1")
            if isinstance(payload.get("critical_zero_exposure_v1"), Mapping)
            else {}
        )
        audit = {
            "version": 1,
            "completed": True,
            "result": "bridged_to_closed_pending_history",
            "reason": "reviewed_prior_live_position_then_exact_zero_cleanup",
            "checked_at": now.isoformat(),
            "target_identity": {
                "execution_id": execution_id,
                "trade_group_id": _int_value(row.get("trade_group_id")) or 0,
                "user_id": _int_value(row.get("user_id")) or 0,
                "symbol": str(row.get("symbol") or "").upper(),
                "side": str(row.get("side") or "").lower(),
                "qty": str(_TARGET["qty"]),
            },
            "zero_proof": {
                "confirmed": bool(zero_proof.get("confirmed") is True),
                "confirmations": int(zero_proof.get("confirmations") or 0),
                "first_zero_at": zero_proof.get("first_zero_at"),
                "second_zero_at": zero_proof.get("second_zero_at"),
            },
            "cleanup": {
                "state": cleanup_marker.get("state"),
                "verified_clean": cleanup_marker.get("verified_clean"),
                "resolved_at": cleanup_marker.get("resolved_at")
                or cleanup_marker.get("checked_at"),
            },
            "production_evidence": dict(_PRODUCTION_EVIDENCE),
            "exchange_reads_performed": 0,
            "exchange_writes_performed": 0,
        }
        prior_live = {
            "version": 1,
            "confirmed": True,
            "source": "g55_reviewed_production_evidence",
            "execution_id": execution_id,
            "user_id": _int_value(row.get("user_id")) or 0,
            "symbol": str(row.get("symbol") or "").upper(),
            "side": str(row.get("side") or "").lower(),
            "qty": str(_TARGET["qty"]),
            "first_seen_at": _PRODUCTION_EVIDENCE["first_live_seen_at"],
            "last_seen_at": _PRODUCTION_EVIDENCE["last_live_seen_at"],
            "log_sha256": _PRODUCTION_EVIDENCE["log_sha256"],
            "recorded_at": now.isoformat(),
        }
        history = {
            "started_at": now.isoformat(),
            "attempts": 0,
            "last_attempt_at": None,
            "last_history_rows": 0,
            "next_attempt_at": now.isoformat(),
            "tp_notification_pending": False,
            "fresh_close_observed": False,
            "historical_recovery": True,
            "source": RECOVERY_KEY,
        }
        patch = {
            RECOVERY_KEY: audit,
            PRIOR_LIVE_KEY: prior_live,
            "lifecycle": {
                "closed_cleanup_done": True,
                "previous_status": "manual_required",
                "position_qty": 0.0,
                "any_position_qty": 0.0,
                "opposite_or_unknown_position_detected": False,
                "cleanup_deferred": False,
                "cleanup_deferred_reason": None,
                "history_reconcile": history,
                "close_result": {
                    "outcome": "pending",
                    "close_type": "waiting_for_bingx_history",
                },
            },
        }
        written = await db.update_execution_status_merge(
            execution_id,
            "closed_pending_history",
            "G55 exact bridge: reviewed live position became zero; cleanup verified; awaiting exact BingX close history",
            patch,
            expected_status="manual_required",
            write_flow_audit_stage="g55_exact_manual_history_bridge",
            write_flow_audit_status="closed_pending_history",
        )
        if not written:
            counters["write_conflict"] = 1
            return counters
        counters["bridged"] = 1
        log.warning(
            "G55_EXACT_MANUAL_HISTORY_BRIDGE_APPLIED execution_id=%s user_id=%s symbol=%s side=%s qty=%s next_status=closed_pending_history",
            execution_id,
            row.get("user_id"),
            row.get("symbol"),
            row.get("side"),
            row.get("qty"),
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        counters["errors"] = 1
        log.exception(
            "G55_EXACT_MANUAL_HISTORY_BRIDGE_FAILED execution_id=%s",
            execution_id,
        )
    return counters


__all__ = [
    "RECOVERY_KEY",
    "PRIOR_LIVE_KEY",
    "recover_exact_manual_history_bridge_once",
]
