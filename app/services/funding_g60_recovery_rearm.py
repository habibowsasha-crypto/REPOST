"""Generic one-shot G60 rearm for legacy G57 funding-quorum contamination.

G59 proved that BingX recovery responses may contain unrelated income rows and
added a safe local filter to the read-only recovery adapter.  Older rows that
already failed under G57 can still remain AMBIGUOUS/manual_review, however.

G60 removes the execution-ID allowlist.  It scans only configured administrator
user IDs and only the exact durable G57 failure signature: confirmed financial
job, exact/within-tolerance volume parity, no funding evidence, exactly two
funding attempts, and the known multi-scope contamination diagnostics.  Matching
rows are re-armed once into the existing G48 four-view read-only quorum using
G59's strict target-row parser.

This service performs no exchange request and no trading-state write.  A failed
G60 quorum becomes a G60 ambiguity and therefore cannot loop through this
selector again.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from app.config import get_settings
from app.database import db

log = logging.getLogger(__name__)

G60_REARM_MARKER = "g60_generic_g57_contamination_rearmed_once"
G60_FUNDING_PENDING = "g60_local_filter_pending"
G60_FUNDING_PROCESSING = "g60_local_filter_processing"
G60_G57_FAILURE_PREFIX = (
    "statistics_funding_g57_quorum_ambiguous:g48_recovery_no_success:"
)
G60_SOURCE_ATTEMPTS = 2
G60_DEFAULT_SCAN_LIMIT = 20

# Keep the selector as narrow as the already-proven G59 production signature.
# The fourth diagnostic can be truncated by the durable reason length limit, so
# only the stable first three evidence fragments are mandatory.
_REQUIRED_REASON_FRAGMENTS = (
    '"variant":"exact"',
    "incomeType is outside requested recovery scope",
    '"variant":"symbol_all_types"',
    "asset is invalid",
    '"variant":"all_symbols_funding"',
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _admin_user_ids(settings: Any | None = None) -> tuple[int, ...]:
    source = settings if settings is not None else get_settings()
    values: set[int] = set()
    for raw in getattr(source, "admin_ids", []) or []:
        try:
            value = int(raw)
        except (TypeError, ValueError, OverflowError):
            continue
        if value > 0:
            values.add(value)
    return tuple(sorted(values))


def _row_matches(row: Mapping[str, Any], *, allowed_user_ids: Sequence[int]) -> bool:
    allowed = {int(value) for value in allowed_user_ids if int(value) > 0}
    try:
        user_id = int(row.get("user_id") or 0)
        attempts = int(row.get("funding_recovery_attempts") or 0)
        event_count = int(row.get("funding_event_count") or 0)
        zero_count = int(row.get("funding_zero_observations") or 0)
    except (TypeError, ValueError, OverflowError):
        return False
    reason = str(row.get("ambiguity_reason") or "")
    recovery_reason = str(row.get("funding_recovery_reason") or "")
    return bool(
        allowed
        and user_id in allowed
        and str(row.get("projection_status") or "") == "unavailable"
        and str(row.get("funding_state") or "") == "manual_review"
        and str(row.get("financial_state") or "") == "AMBIGUOUS"
        and str(row.get("volume_parity_status") or "")
        in {"exact", "within_tolerance"}
        and event_count == 0
        and zero_count == 0
        and attempts == G60_SOURCE_ATTEMPTS
        and reason.startswith(G60_G57_FAILURE_PREFIX)
        and recovery_reason.startswith(G60_G57_FAILURE_PREFIX)
        and all(fragment in reason for fragment in _REQUIRED_REASON_FRAGMENTS)
        and all(fragment in recovery_reason for fragment in _REQUIRED_REASON_FRAGMENTS)
    )


async def _candidates(
    *, allowed_user_ids: Sequence[int], limit: int = G60_DEFAULT_SCAN_LIMIT
) -> list[dict[str, Any]]:
    user_ids = tuple(sorted({int(value) for value in allowed_user_ids if int(value) > 0}))
    if not user_ids:
        return []
    bounded_limit = max(1, min(100, int(limit)))
    prefix_like = G60_G57_FAILURE_PREFIX + "%"
    if db.is_postgres():
        async with db.connect() as conn:
            rows = await conn.fetch(
                """
                SELECT r.*
                FROM analytics_execution_results r
                WHERE r.user_id=ANY($1::bigint[])
                  AND r.projection_status='unavailable'
                  AND r.funding_state='manual_review'
                  AND r.financial_state='AMBIGUOUS'
                  AND r.volume_parity_status IN ('exact','within_tolerance')
                  AND COALESCE(r.funding_event_count,0)=0
                  AND COALESCE(r.funding_zero_observations,0)=0
                  AND COALESCE(r.funding_recovery_attempts,0)=$2
                  AND r.ambiguity_reason LIKE $3
                  AND r.funding_recovery_reason LIKE $3
                  AND POSITION($4 IN r.ambiguity_reason)>0
                  AND POSITION($5 IN r.ambiguity_reason)>0
                  AND POSITION($6 IN r.ambiguity_reason)>0
                  AND POSITION($7 IN r.ambiguity_reason)>0
                  AND POSITION($8 IN r.ambiguity_reason)>0
                  AND POSITION($4 IN r.funding_recovery_reason)>0
                  AND POSITION($5 IN r.funding_recovery_reason)>0
                  AND POSITION($6 IN r.funding_recovery_reason)>0
                  AND POSITION($7 IN r.funding_recovery_reason)>0
                  AND POSITION($8 IN r.funding_recovery_reason)>0
                  AND EXISTS (
                    SELECT 1
                    FROM financial_reconciliation_jobs j
                    WHERE j.execution_id=r.execution_id
                      AND j.status='confirmed'
                  )
                ORDER BY r.updated_at,r.execution_id
                LIMIT $9
                """,
                list(user_ids),
                G60_SOURCE_ATTEMPTS,
                prefix_like,
                _REQUIRED_REASON_FRAGMENTS[0],
                _REQUIRED_REASON_FRAGMENTS[1],
                _REQUIRED_REASON_FRAGMENTS[2],
                _REQUIRED_REASON_FRAGMENTS[3],
                _REQUIRED_REASON_FRAGMENTS[4],
                bounded_limit,
            )
            return [dict(row) for row in rows]

    placeholders = ",".join("?" for _ in user_ids)
    async with db.connect() as conn:
        cursor = await conn.execute(
            f"""
            SELECT r.*
            FROM analytics_execution_results r
            WHERE r.user_id IN ({placeholders})
              AND r.projection_status='unavailable'
              AND r.funding_state='manual_review'
              AND r.financial_state='AMBIGUOUS'
              AND r.volume_parity_status IN ('exact','within_tolerance')
              AND COALESCE(r.funding_event_count,0)=0
              AND COALESCE(r.funding_zero_observations,0)=0
              AND COALESCE(r.funding_recovery_attempts,0)=?
              AND r.ambiguity_reason LIKE ?
              AND r.funding_recovery_reason LIKE ?
              AND instr(r.ambiguity_reason,?)>0
              AND instr(r.ambiguity_reason,?)>0
              AND instr(r.ambiguity_reason,?)>0
              AND instr(r.ambiguity_reason,?)>0
              AND instr(r.ambiguity_reason,?)>0
              AND instr(r.funding_recovery_reason,?)>0
              AND instr(r.funding_recovery_reason,?)>0
              AND instr(r.funding_recovery_reason,?)>0
              AND instr(r.funding_recovery_reason,?)>0
              AND instr(r.funding_recovery_reason,?)>0
              AND EXISTS (
                SELECT 1
                FROM financial_reconciliation_jobs j
                WHERE j.execution_id=r.execution_id
                  AND j.status='confirmed'
              )
            ORDER BY r.updated_at,r.execution_id
            LIMIT ?
            """,
            (
                *user_ids,
                G60_SOURCE_ATTEMPTS,
                prefix_like,
                prefix_like,
                _REQUIRED_REASON_FRAGMENTS[0],
                _REQUIRED_REASON_FRAGMENTS[1],
                _REQUIRED_REASON_FRAGMENTS[2],
                _REQUIRED_REASON_FRAGMENTS[3],
                _REQUIRED_REASON_FRAGMENTS[4],
                _REQUIRED_REASON_FRAGMENTS[0],
                _REQUIRED_REASON_FRAGMENTS[1],
                _REQUIRED_REASON_FRAGMENTS[2],
                _REQUIRED_REASON_FRAGMENTS[3],
                _REQUIRED_REASON_FRAGMENTS[4],
                bounded_limit,
            ),
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def _rearm(row: Mapping[str, Any], *, now: datetime) -> bool:
    execution_id = int(row["execution_id"])
    user_id = int(row["user_id"])
    deadline_sec = max(
        3600,
        int(getattr(get_settings(), "STATISTICS_FUNDING_RECOVERY_DEADLINE_SEC", 21600)),
    )
    deadline = now + timedelta(seconds=deadline_sec)
    ambiguity_reason = str(row.get("ambiguity_reason") or "")
    recovery_reason = str(row.get("funding_recovery_reason") or "")
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
                  AND EXISTS (
                    SELECT 1
                    FROM financial_reconciliation_jobs j
                    WHERE j.execution_id=analytics_execution_results.execution_id
                      AND j.status='confirmed'
                  )
                """,
                now,
                deadline,
                G60_FUNDING_PENDING,
                G60_REARM_MARKER,
                execution_id,
                user_id,
                G60_SOURCE_ATTEMPTS,
                ambiguity_reason,
                recovery_reason,
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
              AND EXISTS (
                SELECT 1
                FROM financial_reconciliation_jobs j
                WHERE j.execution_id=analytics_execution_results.execution_id
                  AND j.status='confirmed'
              )
            """,
            (
                now.isoformat(),
                deadline.isoformat(),
                G60_FUNDING_PENDING,
                G60_REARM_MARKER,
                execution_id,
                user_id,
                G60_SOURCE_ATTEMPTS,
                ambiguity_reason,
                recovery_reason,
            ),
        )
        await conn.commit()
        return int(getattr(cursor, "rowcount", 0) or 0) == 1


async def recover_g60_funding_rearm_once(*, limit: int = G60_DEFAULT_SCAN_LIMIT) -> dict[str, int]:
    counters = {
        "scanned": 0,
        "rearmed": 0,
        "blocked_signature": 0,
        "write_conflict": 0,
        "errors": 0,
    }
    settings = get_settings()
    if not (
        bool(settings.FINANCIAL_RECONCILIATION_ENABLED)
        and bool(settings.STATISTICS_FUNDING_ENABLED)
    ):
        return counters

    allowed_user_ids = _admin_user_ids(settings)
    if not allowed_user_ids:
        return counters

    try:
        rows = await _candidates(allowed_user_ids=allowed_user_ids, limit=limit)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        counters["errors"] += 1
        log.exception(
            "G60_FUNDING_GENERIC_REARM_SCAN_FAILED error_type=%s error=%s",
            type(exc).__name__,
            str(exc)[:300],
        )
        return counters

    now = _utcnow()
    for row in rows:
        counters["scanned"] += 1
        try:
            if not _row_matches(row, allowed_user_ids=allowed_user_ids):
                counters["blocked_signature"] += 1
                continue
            if await _rearm(row, now=now):
                counters["rearmed"] += 1
                log.info(
                    "G60_FUNDING_GENERIC_REARM_APPLIED execution_id=%s user_id=%s",
                    int(row["execution_id"]),
                    int(row["user_id"]),
                )
            else:
                counters["write_conflict"] += 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            counters["errors"] += 1
            log.exception(
                "G60_FUNDING_GENERIC_REARM_FAILED execution_id=%s error_type=%s error=%s",
                row.get("execution_id"),
                type(exc).__name__,
                str(exc)[:300],
            )
    return counters
