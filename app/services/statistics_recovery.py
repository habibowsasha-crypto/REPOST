"""Explicit, audit-only recovery review interface for statistics step 8.

The module exposes recoverable quality findings to an administrator and can
append an idempotent ``recovery_requested`` audit event.  It deliberately does
not repair or mutate signals, executions, fills, orders, positions or exchange
state.  Any future evidence-based repair remains a separate reviewed change.
"""

from __future__ import annotations

import html
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from app.config import get_settings
from app.database.db import connect, is_postgres, monitor_db_workload
from app.services.statistics_quality_store import record_statistics_recovery_audit

_MAX_RECOVERY_ROWS = 100


@dataclass(frozen=True, slots=True)
class StatisticsRecoveryCandidate:
    audit_id: int
    period_id: int | None
    entity_type: str
    entity_id: str
    issue_code: str
    severity: str
    reason: str
    created_at: datetime | None


@dataclass(frozen=True, slots=True)
class StatisticsRecoveryRequestResult:
    status: str
    audit_id: int
    recovery_audit_key: str = ""


def _as_dict(row: Any) -> dict[str, Any]:
    try:
        return dict(row)
    except Exception:
        return {}


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError, OverflowError):
        return 0


def _dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip().replace("Z", "+00:00")
        if " " in text and "T" not in text:
            text = text.replace(" ", "T", 1)
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _candidate(row: Any) -> StatisticsRecoveryCandidate | None:
    data = _as_dict(row)
    audit_id = _int(data.get("id"))
    if audit_id <= 0:
        return None
    raw_period = data.get("period_id")
    return StatisticsRecoveryCandidate(
        audit_id=audit_id,
        period_id=_int(raw_period) if raw_period not in (None, "") else None,
        entity_type=str(data.get("entity_type") or "unknown"),
        entity_id=str(data.get("entity_id") or "unknown"),
        issue_code=str(data.get("issue_code") or "unknown"),
        severity=str(data.get("severity") or "unknown"),
        reason=str(data.get("reason") or ""),
        created_at=_dt(data.get("created_at")),
    )


async def _candidate_belongs_to_user(
    candidate: StatisticsRecoveryCandidate, user_id: int
) -> bool:
    actor = int(user_id)
    if actor <= 0:
        return False
    entity_type = str(candidate.entity_type or "").strip().lower()
    try:
        entity_id = int(candidate.entity_id)
    except (TypeError, ValueError):
        return False
    if entity_type == "execution":
        query = (
            "SELECT 1 FROM analytics_execution_results WHERE execution_id=$1 AND user_id=$2 LIMIT 1"
            if is_postgres()
            else "SELECT 1 FROM analytics_execution_results WHERE execution_id=? AND user_id=? LIMIT 1"
        )
    elif entity_type == "signal":
        query = (
            "SELECT 1 FROM analytics_execution_results WHERE analytics_signal_id=$1 AND user_id=$2 LIMIT 1"
            if is_postgres()
            else "SELECT 1 FROM analytics_execution_results WHERE analytics_signal_id=? AND user_id=? LIMIT 1"
        )
    else:
        return False
    async with connect() as conn:
        if is_postgres():
            row = await conn.fetchrow(query, entity_id, actor)
        else:
            cursor = await conn.execute(query, (entity_id, actor))
            row = await cursor.fetchone()
    return row is not None


async def get_statistics_recovery_candidate(
    audit_id: int, *, user_id: int | None = None
) -> StatisticsRecoveryCandidate | None:
    normalized = int(audit_id)
    if normalized <= 0:
        return None
    query = (
        "SELECT id,period_id,entity_type,entity_id,issue_code,severity,reason,created_at "
        "FROM statistics_quality_audit "
        "WHERE id=$1 AND action='detected' AND recoverable=1"
        if is_postgres()
        else
        "SELECT id,period_id,entity_type,entity_id,issue_code,severity,reason,created_at "
        "FROM statistics_quality_audit "
        "WHERE id=? AND action='detected' AND recoverable=1"
    )
    async with monitor_db_workload(stage="statistics_recovery"):
        async with connect() as conn:
            if is_postgres():
                row = await conn.fetchrow(query, normalized)
            else:
                cursor = await conn.execute(query, (normalized,))
                row = await cursor.fetchone()
    candidate = _candidate(row)
    if candidate is None or user_id is None:
        return candidate
    return candidate if await _candidate_belongs_to_user(candidate, int(user_id)) else None


async def list_statistics_recovery_candidates(
    *, period_id: int | None = None, limit: int = 20, user_id: int | None = None
) -> tuple[StatisticsRecoveryCandidate, ...]:
    requested_limit = max(1, min(_MAX_RECOVERY_ROWS, int(limit)))
    bounded = _MAX_RECOVERY_ROWS if user_id is not None else requested_limit
    normalized_period = int(period_id) if period_id is not None else None
    if normalized_period is not None and normalized_period <= 0:
        raise ValueError("period_id must be positive")
    if is_postgres():
        if normalized_period is None:
            query = (
                "SELECT id,period_id,entity_type,entity_id,issue_code,severity,reason,created_at "
                "FROM statistics_quality_audit "
                "WHERE action='detected' AND recoverable=1 "
                "ORDER BY created_at DESC,id DESC LIMIT $1"
            )
            args = (bounded,)
        else:
            query = (
                "SELECT id,period_id,entity_type,entity_id,issue_code,severity,reason,created_at "
                "FROM statistics_quality_audit "
                "WHERE action='detected' AND recoverable=1 AND period_id=$1 "
                "ORDER BY created_at DESC,id DESC LIMIT $2"
            )
            args = (normalized_period, bounded)
    else:
        if normalized_period is None:
            query = (
                "SELECT id,period_id,entity_type,entity_id,issue_code,severity,reason,created_at "
                "FROM statistics_quality_audit "
                "WHERE action='detected' AND recoverable=1 "
                "ORDER BY created_at DESC,id DESC LIMIT ?"
            )
            args = (bounded,)
        else:
            query = (
                "SELECT id,period_id,entity_type,entity_id,issue_code,severity,reason,created_at "
                "FROM statistics_quality_audit "
                "WHERE action='detected' AND recoverable=1 AND period_id=? "
                "ORDER BY created_at DESC,id DESC LIMIT ?"
            )
            args = (normalized_period, bounded)
    async with monitor_db_workload(stage="statistics_recovery"):
        async with connect() as conn:
            if is_postgres():
                rows = await conn.fetch(query, *args)
            else:
                cursor = await conn.execute(query, args)
                rows = await cursor.fetchall()
    candidates = tuple(item for row in rows if (item := _candidate(row)) is not None)
    if user_id is None:
        return candidates[:requested_limit]
    scoped: list[StatisticsRecoveryCandidate] = []
    for item in candidates:
        if await _candidate_belongs_to_user(item, int(user_id)):
            scoped.append(item)
            if len(scoped) >= requested_limit:
                break
    return tuple(scoped)


async def format_statistics_recovery_report(
    period_id: int | None = None, *, user_id: int | None = None
) -> str:
    rows = await list_statistics_recovery_candidates(
        period_id=period_id, limit=20, user_id=user_id
    )
    scope = f"период #{int(period_id)}" if period_id is not None else "все периоды"
    lines = [
        "<b>🧯 RECOVERY REVIEW</b>",
        "",
        f"Scope: <b>{html.escape(scope)}</b>",
        (f"Account: <code>{int(user_id)}</code>" if user_id is not None else "Account: all (technical)"),
        f"Recoverable findings: <b>{len(rows)}</b> (показано до 20)",
        "",
        "⚠️ Этот раздел только создаёт audit-запрос на проверку. Он не исправляет историю и не меняет торговлю.",
    ]
    if not rows:
        lines.extend(["", "Подтверждённых recoverable findings нет."])
        return "\n".join(lines)
    lines.append("")
    for item in rows:
        period = f"p{item.period_id}" if item.period_id is not None else "p—"
        lines.append(
            f"<b>#{item.audit_id}</b> · {period} · {html.escape(item.entity_type)} "
            f"<code>{html.escape(item.entity_id)}</code> · "
            f"<code>{html.escape(item.issue_code)}</code>"
        )
        lines.append(f"Причина: {html.escape(item.reason[:300] or '—')}")
    lines.extend(
        [
            "",
            "Запросить ручную проверку:",
            "<code>/stats_recovery_request AUDIT_ID</code>",
        ]
    )
    return "\n".join(lines)


async def request_statistics_recovery(
    *, audit_id: int, actor_user_id: int, scope_user_id: int | None = None
) -> StatisticsRecoveryRequestResult:
    normalized_audit = int(audit_id)
    actor = int(actor_user_id)
    if normalized_audit <= 0 or actor <= 0:
        return StatisticsRecoveryRequestResult("invalid", normalized_audit)
    if not bool(get_settings().STATISTICS_QUALITY_ENABLED):
        return StatisticsRecoveryRequestResult("disabled", normalized_audit)
    candidate = await get_statistics_recovery_candidate(
        normalized_audit, user_id=scope_user_id
    )
    if candidate is None:
        return StatisticsRecoveryRequestResult("not_found", normalized_audit)
    audit_key = f"recovery-request:{normalized_audit}:{actor}"
    inserted = await record_statistics_recovery_audit(
        audit_key=audit_key,
        period_id=candidate.period_id,
        entity_type=candidate.entity_type,
        entity_id=candidate.entity_id,
        issue_code=candidate.issue_code,
        action="recovery_requested",
        reason="Administrator requested evidence-based recovery review",
        actor_user_id=actor,
        metadata={
            "source_quality_audit_id": candidate.audit_id,
            "source_reason": candidate.reason,
            "automatic_repair": False,
        },
    )
    return StatisticsRecoveryRequestResult(
        "requested" if inserted else "already_requested",
        normalized_audit,
        audit_key,
    )
