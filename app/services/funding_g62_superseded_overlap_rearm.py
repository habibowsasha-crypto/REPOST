"""G62 repair for funding ambiguity caused only by proven superseded executions.

G59 introduced the durable ``superseded_duplicate`` status for an execution row
that is proven to be a duplicate of a canonical execution.  Statistics reports
already exclude such rows, but the funding overlap query in the financial
projection still treated them as possible owners of a funding event.  That can
leave the canonical execution permanently AMBIGUOUS/manual_review.

G62 fixes the forward path in ``statistics_financial_projection`` and provides
this narrow DB-only one-shot rearm for already-stuck administrator projections.
A row is rearmed only when:
- it is an exact/within-tolerance, confirmed financial projection;
- its durable ambiguity is exactly ``funding_attribution_overlap``;
- every blocker named by that ambiguity is still ``superseded_duplicate``;
- retained ambiguous funding evidence exists for the same user/symbol/reason;
- recomputing the original event-time overlap finds no non-superseded execution.

No exchange request is made here and no trading state is changed.  The normal
financial projection performs the read-only BingX funding request later and can
promote matching retained ambiguous funding evidence only after unique ownership
has been re-established.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from app.config import get_settings
from app.database import db

log = logging.getLogger(__name__)

G62_REARM_MARKER = "g62_superseded_overlap_rearmed_once"
G62_FUNDING_PENDING = "g62_superseded_overlap_pending"
G62_AMBIGUITY_PREFIX = "statistics_funding_attribution_ambiguous:funding_attribution_overlap:"
G62_INNER_REASON_PREFIX = "funding_attribution_overlap:"
G62_DEFAULT_SCAN_LIMIT = 20
_SUPERSEDED_STATUS = "superseded_duplicate"
_ID_LIST_RE = re.compile(r"^[0-9]+(?:,[0-9]+){0,19}$")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _canonical_symbol(value: Any) -> str:
    return str(value or "").upper().replace("-", "").replace("_", "").strip()


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


def _parse_blocker_ids(reason: Any) -> tuple[int, ...]:
    text = str(reason or "")
    if not text.startswith(G62_AMBIGUITY_PREFIX):
        return ()
    suffix = text[len(G62_AMBIGUITY_PREFIX) :].strip()
    if not _ID_LIST_RE.fullmatch(suffix):
        return ()
    values = tuple(dict.fromkeys(int(part) for part in suffix.split(",")))
    return values if values and all(value > 0 for value in values) else ()


def _inner_reason(reason: Any) -> str:
    text = str(reason or "")
    if not text.startswith(G62_AMBIGUITY_PREFIX):
        return ""
    return text[len("statistics_funding_attribution_ambiguous:") :]


def _row_matches(row: Mapping[str, Any], *, allowed_user_ids: Sequence[int]) -> bool:
    allowed = {int(value) for value in allowed_user_ids if int(value) > 0}
    try:
        user_id = int(row.get("user_id") or 0)
        event_count = int(row.get("funding_event_count") or 0)
        zero_count = int(row.get("funding_zero_observations") or 0)
    except (TypeError, ValueError, OverflowError):
        return False
    reason = str(row.get("ambiguity_reason") or "")
    recovery_reason = str(row.get("funding_recovery_reason") or "")
    blockers = _parse_blocker_ids(reason)
    return bool(
        allowed
        and user_id in allowed
        and blockers
        and str(row.get("projection_status") or "") == "unavailable"
        and str(row.get("funding_state") or "") == "manual_review"
        and str(row.get("financial_state") or "") == "AMBIGUOUS"
        and str(row.get("volume_parity_status") or "")
        in {"exact", "within_tolerance"}
        and event_count == 0
        and zero_count == 0
        and recovery_reason == reason
    )


async def _candidates(
    *, allowed_user_ids: Sequence[int], limit: int = G62_DEFAULT_SCAN_LIMIT
) -> list[dict[str, Any]]:
    user_ids = tuple(sorted({int(value) for value in allowed_user_ids if int(value) > 0}))
    if not user_ids:
        return []
    bounded_limit = max(1, min(100, int(limit)))
    prefix_like = G62_AMBIGUITY_PREFIX + "%"
    if db.is_postgres():
        async with db.connect() as conn:
            rows = await conn.fetch(
                """
                SELECT r.*
                FROM analytics_execution_results r
                JOIN trade_executions e ON e.id=r.execution_id
                WHERE r.user_id=ANY($1::bigint[])
                  AND COALESCE(e.status,'') <> 'superseded_duplicate'
                  AND r.projection_status='unavailable'
                  AND r.funding_state='manual_review'
                  AND r.financial_state='AMBIGUOUS'
                  AND r.volume_parity_status IN ('exact','within_tolerance')
                  AND COALESCE(r.funding_event_count,0)=0
                  AND COALESCE(r.funding_zero_observations,0)=0
                  AND r.ambiguity_reason LIKE $2
                  AND r.funding_recovery_reason=r.ambiguity_reason
                  AND EXISTS (
                    SELECT 1 FROM financial_reconciliation_jobs j
                    WHERE j.execution_id=r.execution_id AND j.status='confirmed'
                  )
                ORDER BY r.updated_at,r.execution_id
                LIMIT $3
                """,
                list(user_ids),
                prefix_like,
                bounded_limit,
            )
            return [dict(row) for row in rows]

    placeholders = ",".join("?" for _ in user_ids)
    async with db.connect() as conn:
        cursor = await conn.execute(
            f"""
            SELECT r.*
            FROM analytics_execution_results r
            JOIN trade_executions e ON e.id=r.execution_id
            WHERE r.user_id IN ({placeholders})
              AND COALESCE(e.status,'') <> 'superseded_duplicate'
              AND r.projection_status='unavailable'
              AND r.funding_state='manual_review'
              AND r.financial_state='AMBIGUOUS'
              AND r.volume_parity_status IN ('exact','within_tolerance')
              AND COALESCE(r.funding_event_count,0)=0
              AND COALESCE(r.funding_zero_observations,0)=0
              AND r.ambiguity_reason LIKE ?
              AND r.funding_recovery_reason=r.ambiguity_reason
              AND EXISTS (
                SELECT 1 FROM financial_reconciliation_jobs j
                WHERE j.execution_id=r.execution_id AND j.status='confirmed'
              )
            ORDER BY r.updated_at,r.execution_id
            LIMIT ?
            """,
            (*user_ids, prefix_like, bounded_limit),
        )
        return [dict(row) for row in await cursor.fetchall()]


async def _blockers_are_still_superseded(
    *, row: Mapping[str, Any], blocker_ids: Sequence[int]
) -> bool:
    user_id = int(row["user_id"])
    symbol = _canonical_symbol(row.get("symbol"))
    if not blocker_ids or not symbol:
        return False
    for blocker_id in blocker_ids:
        blocker = await db.get_execution_by_id(int(blocker_id))
        if not blocker:
            return False
        if str(blocker.get("status") or "") != _SUPERSEDED_STATUS:
            return False
        if int(blocker.get("user_id") or 0) != user_id:
            return False
        if _canonical_symbol(blocker.get("symbol")) != symbol:
            return False
    return True


async def _ambiguous_event_times(
    *, user_id: int, symbol: str, reason: str
) -> tuple[datetime, ...]:
    canonical = _canonical_symbol(symbol)
    if not canonical or not reason:
        return ()
    if db.is_postgres():
        async with db.connect() as conn:
            rows = await conn.fetch(
                """
                SELECT event_time
                FROM financial_funding_events
                WHERE exchange='bingx' AND user_id=$1
                  AND UPPER(REPLACE(REPLACE(symbol,'-',''),'_',''))=$2
                  AND execution_id IS NULL
                  AND attribution_status='ambiguous'
                  AND attribution_reason=$3
                ORDER BY event_time
                """,
                user_id,
                canonical,
                reason,
            )
            return tuple(row["event_time"] for row in rows if row["event_time"] is not None)

    async with db.connect() as conn:
        cursor = await conn.execute(
            """
            SELECT event_time
            FROM financial_funding_events
            WHERE exchange='bingx' AND user_id=?
              AND UPPER(REPLACE(REPLACE(symbol,'-',''),'_',''))=?
              AND execution_id IS NULL
              AND attribution_status='ambiguous'
              AND attribution_reason=?
            ORDER BY event_time
            """,
            (user_id, canonical, reason),
        )
        rows = await cursor.fetchall()
        result: list[datetime] = []
        for item in rows:
            value = item[0]
            if isinstance(value, datetime):
                dt = value
            else:
                try:
                    dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                except (TypeError, ValueError):
                    return ()
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            result.append(dt.astimezone(timezone.utc))
        return tuple(result)


async def _has_non_superseded_overlap(
    *, execution_id: int, user_id: int, symbol: str, event_time: datetime
) -> bool:
    if db.is_postgres():
        async with db.connect() as conn:
            value = await conn.fetchval(
                """
                SELECT EXISTS(
                  SELECT 1 FROM trade_executions
                  WHERE id<>$1 AND user_id=$2 AND UPPER(symbol)=UPPER($3)
                    AND COALESCE(status,'') <> 'superseded_duplicate'
                    AND created_at <= $4
                    AND COALESCE(closed_at,updated_at,NOW()) >= $4
                )
                """,
                execution_id,
                user_id,
                symbol,
                event_time,
            )
            return bool(value)

    async with db.connect() as conn:
        cursor = await conn.execute(
            """
            SELECT 1 FROM trade_executions
            WHERE id<>? AND user_id=? AND UPPER(symbol)=UPPER(?)
              AND COALESCE(status,'') <> 'superseded_duplicate'
              AND julianday(created_at) <= julianday(?)
              AND julianday(COALESCE(closed_at,updated_at,CURRENT_TIMESTAMP)) >= julianday(?)
            LIMIT 1
            """,
            (
                execution_id,
                user_id,
                symbol,
                event_time.isoformat(),
                event_time.isoformat(),
            ),
        )
        return await cursor.fetchone() is not None


async def _has_superseded_only_proof(row: Mapping[str, Any]) -> bool:
    blocker_ids = _parse_blocker_ids(row.get("ambiguity_reason"))
    if not blocker_ids:
        return False
    if not await _blockers_are_still_superseded(row=row, blocker_ids=blocker_ids):
        return False
    inner_reason = _inner_reason(row.get("ambiguity_reason"))
    event_times = await _ambiguous_event_times(
        user_id=int(row["user_id"]),
        symbol=str(row.get("symbol") or ""),
        reason=inner_reason,
    )
    if not event_times:
        return False
    for event_time in event_times:
        if await _has_non_superseded_overlap(
            execution_id=int(row["execution_id"]),
            user_id=int(row["user_id"]),
            symbol=str(row.get("symbol") or ""),
            event_time=event_time,
        ):
            return False
    return True


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
                WHERE execution_id=$5 AND user_id=$6
                  AND projection_status='unavailable'
                  AND funding_state='manual_review'
                  AND financial_state='AMBIGUOUS'
                  AND volume_parity_status IN ('exact','within_tolerance')
                  AND COALESCE(funding_event_count,0)=0
                  AND COALESCE(funding_zero_observations,0)=0
                  AND ambiguity_reason=$7
                  AND funding_recovery_reason=$8
                  AND EXISTS (
                    SELECT 1 FROM financial_reconciliation_jobs j
                    WHERE j.execution_id=analytics_execution_results.execution_id
                      AND j.status='confirmed'
                  )
                """,
                now,
                deadline,
                G62_FUNDING_PENDING,
                G62_REARM_MARKER,
                execution_id,
                user_id,
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
            WHERE execution_id=? AND user_id=?
              AND projection_status='unavailable'
              AND funding_state='manual_review'
              AND financial_state='AMBIGUOUS'
              AND volume_parity_status IN ('exact','within_tolerance')
              AND COALESCE(funding_event_count,0)=0
              AND COALESCE(funding_zero_observations,0)=0
              AND ambiguity_reason=?
              AND funding_recovery_reason=?
              AND EXISTS (
                SELECT 1 FROM financial_reconciliation_jobs j
                WHERE j.execution_id=analytics_execution_results.execution_id
                  AND j.status='confirmed'
              )
            """,
            (
                now.isoformat(),
                deadline.isoformat(),
                G62_FUNDING_PENDING,
                G62_REARM_MARKER,
                execution_id,
                user_id,
                ambiguity_reason,
                recovery_reason,
            ),
        )
        await conn.commit()
        return int(getattr(cursor, "rowcount", 0) or 0) == 1


async def recover_g62_superseded_overlap_once(
    *, limit: int = G62_DEFAULT_SCAN_LIMIT
) -> dict[str, int]:
    counters = {
        "scanned": 0,
        "rearmed": 0,
        "blocked_signature": 0,
        "blocked_proof": 0,
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
            "G62_SUPERSEDED_OVERLAP_REARM_SCAN_FAILED error_type=%s error=%s",
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
            if not await _has_superseded_only_proof(row):
                counters["blocked_proof"] += 1
                continue
            if await _rearm(row, now=now):
                counters["rearmed"] += 1
                log.info(
                    "G62_SUPERSEDED_OVERLAP_REARM_APPLIED execution_id=%s user_id=%s blockers=%s",
                    int(row["execution_id"]),
                    int(row["user_id"]),
                    list(_parse_blocker_ids(row.get("ambiguity_reason"))),
                )
            else:
                counters["write_conflict"] += 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            counters["errors"] += 1
            log.exception(
                "G62_SUPERSEDED_OVERLAP_REARM_FAILED execution_id=%s error_type=%s error=%s",
                row.get("execution_id"),
                type(exc).__name__,
                str(exc)[:300],
            )
    return counters
