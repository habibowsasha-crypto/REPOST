"""One-shot G59 rearm for exact G57 funding-quorum contamination failures.

G57 correctly failed closed when BingX returned rows outside the requested
income scope. Production then showed that all four recovery query shapes could
be contaminated by unrelated rows, leaving otherwise fully reconciled terminal
executions in AMBIGUOUS/manual_review.

G59 does not invent funding and performs no exchange request here. It only
re-arms the three reviewed account-scoped executions whose durable state exactly
matches the G57 failure signature. The existing financial projection worker then
runs the read-only multi-scope quorum with the G59 recovery parser.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from app.config import get_settings
from app.database import db

log = logging.getLogger(__name__)

G59_REARM_MARKER = "g59_contaminated_income_local_filter_rearmed_once"
G59_FUNDING_PENDING = "g59_local_filter_pending"
G59_FUNDING_PROCESSING = "g59_local_filter_processing"
G59_G57_FAILURE_PREFIX = (
    "statistics_funding_g57_quorum_ambiguous:g48_recovery_no_success:"
)
G59_TARGET_EXECUTION_IDS = frozenset({1520, 1533, 1534})
G59_TARGET_USER_ID = 6835564228
G59_SOURCE_ATTEMPTS = 2

_REQUIRED_REASON_FRAGMENTS = (
    '"variant":"exact"',
    "incomeType is outside requested recovery scope",
    '"variant":"symbol_all_types"',
    "asset is invalid",
    '"variant":"all_symbols_funding"',
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _row_matches(row: Mapping[str, Any]) -> bool:
    try:
        execution_id = int(row.get("execution_id") or 0)
        user_id = int(row.get("user_id") or 0)
        attempts = int(row.get("funding_recovery_attempts") or 0)
        event_count = int(row.get("funding_event_count") or 0)
        zero_count = int(row.get("funding_zero_observations") or 0)
    except (TypeError, ValueError, OverflowError):
        return False
    reason = str(row.get("ambiguity_reason") or "")
    recovery_reason = str(row.get("funding_recovery_reason") or "")
    return bool(
        execution_id in G59_TARGET_EXECUTION_IDS
        and user_id == G59_TARGET_USER_ID
        and str(row.get("projection_status") or "") == "unavailable"
        and str(row.get("funding_state") or "") == "manual_review"
        and str(row.get("financial_state") or "") == "AMBIGUOUS"
        and str(row.get("volume_parity_status") or "")
        in {"exact", "within_tolerance"}
        and event_count == 0
        and zero_count == 0
        and attempts == G59_SOURCE_ATTEMPTS
        and reason.startswith(G59_G57_FAILURE_PREFIX)
        and recovery_reason.startswith(G59_G57_FAILURE_PREFIX)
        and all(fragment in reason for fragment in _REQUIRED_REASON_FRAGMENTS)
    )


async def _candidate(execution_id: int) -> dict[str, Any] | None:
    if db.is_postgres():
        async with db.connect() as conn:
            row = await conn.fetchrow(
                """
                SELECT r.*,j.id AS financial_job_id,j.status AS financial_job_status
                FROM analytics_execution_results r
                LEFT JOIN financial_reconciliation_jobs j
                  ON j.execution_id=r.execution_id
                WHERE r.execution_id=$1
                """,
                int(execution_id),
            )
            return dict(row) if row else None
    async with db.connect() as conn:
        cursor = await conn.execute(
            """
            SELECT r.*,j.id AS financial_job_id,j.status AS financial_job_status
            FROM analytics_execution_results r
            LEFT JOIN financial_reconciliation_jobs j
              ON j.execution_id=r.execution_id
            WHERE r.execution_id=?
            """,
            (int(execution_id),),
        )
        row = await cursor.fetchone()
        return dict(row) if row else None


async def _rearm(row: Mapping[str, Any], *, now: datetime) -> bool:
    execution_id = int(row["execution_id"])
    deadline_sec = max(
        3600,
        int(getattr(get_settings(), "STATISTICS_FUNDING_RECOVERY_DEADLINE_SEC", 21600)),
    )
    deadline = now + timedelta(seconds=deadline_sec)
    if db.is_postgres():
        async with db.connect() as conn:
            tag = await conn.execute(
                """
                UPDATE analytics_execution_results
                SET projection_status='retry',
                    projection_next_attempt_at=$1,
                    projection_deadline_at=$2,
                    projection_processing_started_at=NULL,
                    projection_lease_token=NULL,
                    projection_last_error=NULL,
                    funding_state='pending',
                    funding_recovery_status=$3,
                    funding_recovery_reason=$4,
                    financial_state='PROVISIONAL',
                    data_quality_status='partial',
                    ambiguity_reason=NULL,
                    funding_finalized_at=NULL,
                    finalized_at=NULL,
                    final_eligible=0,
                    quality_gate_version=0,
                    quality_evaluated_at=NULL,
                    updated_at=NOW(),
                    result_version=result_version+1
                WHERE execution_id=$5
                  AND user_id=$6
                  AND projection_status='unavailable'
                  AND funding_state='manual_review'
                  AND financial_state='AMBIGUOUS'
                  AND volume_parity_status IN ('exact','within_tolerance')
                  AND COALESCE(funding_event_count,0)=0
                  AND COALESCE(funding_zero_observations,0)=0
                  AND COALESCE(funding_recovery_attempts,0)=$7
                  AND ambiguity_reason=$8
                  AND funding_recovery_reason=$9
                """,
                now,
                deadline,
                G59_FUNDING_PENDING,
                G59_REARM_MARKER,
                execution_id,
                G59_TARGET_USER_ID,
                G59_SOURCE_ATTEMPTS,
                str(row.get("ambiguity_reason") or ""),
                str(row.get("funding_recovery_reason") or ""),
            )
            return str(tag).endswith(" 1")
    async with db.connect() as conn:
        cursor = await conn.execute(
            """
            UPDATE analytics_execution_results
            SET projection_status='retry',
                projection_next_attempt_at=?,
                projection_deadline_at=?,
                projection_processing_started_at=NULL,
                projection_lease_token=NULL,
                projection_last_error=NULL,
                funding_state='pending',
                funding_recovery_status=?,
                funding_recovery_reason=?,
                financial_state='PROVISIONAL',
                data_quality_status='partial',
                ambiguity_reason=NULL,
                funding_finalized_at=NULL,
                finalized_at=NULL,
                final_eligible=0,
                quality_gate_version=0,
                quality_evaluated_at=NULL,
                updated_at=CURRENT_TIMESTAMP,
                result_version=result_version+1
            WHERE execution_id=?
              AND user_id=?
              AND projection_status='unavailable'
              AND funding_state='manual_review'
              AND financial_state='AMBIGUOUS'
              AND volume_parity_status IN ('exact','within_tolerance')
              AND COALESCE(funding_event_count,0)=0
              AND COALESCE(funding_zero_observations,0)=0
              AND COALESCE(funding_recovery_attempts,0)=?
              AND ambiguity_reason=?
              AND funding_recovery_reason=?
            """,
            (
                now.isoformat(),
                deadline.isoformat(),
                G59_FUNDING_PENDING,
                G59_REARM_MARKER,
                execution_id,
                G59_TARGET_USER_ID,
                G59_SOURCE_ATTEMPTS,
                str(row.get("ambiguity_reason") or ""),
                str(row.get("funding_recovery_reason") or ""),
            ),
        )
        await conn.commit()
        return int(getattr(cursor, "rowcount", 0) or 0) == 1


async def recover_g59_funding_rearm_once() -> dict[str, int]:
    counters = {
        "scanned": 0,
        "rearmed": 0,
        "already_rearmed": 0,
        "blocked_state": 0,
        "blocked_signature": 0,
        "blocked_job": 0,
        "write_conflict": 0,
        "missing": 0,
        "errors": 0,
    }
    settings = get_settings()
    if not (
        bool(settings.FINANCIAL_RECONCILIATION_ENABLED)
        and bool(settings.STATISTICS_FUNDING_ENABLED)
    ):
        return counters

    now = _utcnow()
    for execution_id in sorted(G59_TARGET_EXECUTION_IDS):
        try:
            row = await _candidate(execution_id)
            if not row:
                counters["missing"] += 1
                continue
            counters["scanned"] += 1
            recovery_status = str(row.get("funding_recovery_status") or "")
            recovery_reason = str(row.get("funding_recovery_reason") or "")
            if recovery_status in {G59_FUNDING_PENDING, G59_FUNDING_PROCESSING} or recovery_reason.startswith(
                "statistics_funding_g59_quorum_ambiguous:"
            ) or recovery_reason == G59_REARM_MARKER:
                counters["already_rearmed"] += 1
                continue
            if str(row.get("financial_job_status") or "") != "confirmed":
                counters["blocked_job"] += 1
                continue
            state_fields_ok = (
                str(row.get("projection_status") or "") == "unavailable"
                and str(row.get("funding_state") or "") == "manual_review"
                and str(row.get("financial_state") or "") == "AMBIGUOUS"
                and str(row.get("volume_parity_status") or "")
                in {"exact", "within_tolerance"}
                and int(row.get("funding_event_count") or 0) == 0
                and int(row.get("funding_zero_observations") or 0) == 0
                and int(row.get("funding_recovery_attempts") or 0)
                == G59_SOURCE_ATTEMPTS
            )
            if not state_fields_ok:
                counters["blocked_state"] += 1
                continue
            if not _row_matches(row):
                counters["blocked_signature"] += 1
                continue
            if await _rearm(row, now=now):
                counters["rearmed"] += 1
                log.info(
                    "G59_FUNDING_LOCAL_FILTER_REARM_APPLIED execution_id=%s user_id=%s",
                    execution_id,
                    G59_TARGET_USER_ID,
                )
            else:
                counters["write_conflict"] += 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            counters["errors"] += 1
            log.exception(
                "G59_FUNDING_LOCAL_FILTER_REARM_FAILED execution_id=%s error_type=%s error=%s",
                execution_id,
                type(exc).__name__,
                str(exc)[:300],
            )
    return counters
