"""Optional durable audit/quarantine writer for statistics plan step 7.

No worker or command is wired here.  Writes happen only through an explicit
caller and only while ``STATISTICS_QUALITY_ENABLED`` is true.  The module never
touches trade_groups, trade_executions, orders or exchange adapters.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from app.config import get_settings
from app.database.db import connect, is_postgres
from app.services.statistics_quality import StatisticsQualityReport


@dataclass(frozen=True, slots=True)
class StatisticsQualityPersistResult:
    status: str
    scan_id: str
    audit_rows: int = 0
    signal_rows_quarantined: int = 0
    execution_rows_quarantined: int = 0


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _safe(value: Any, limit: int = 800) -> str:
    return " ".join(str(value or "").replace("\x00", " ").split())[:limit]


def _audit_key(scan_id: str, entity_type: str, entity_id: str, issue_code: str) -> str:
    raw = f"{scan_id}|{entity_type}|{entity_id}|{issue_code}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


async def persist_statistics_quality_report(
    report: StatisticsQualityReport,
    *,
    period_id: int | None,
    actor_user_id: int | None = None,
    scan_id: str | None = None,
    apply_quarantine: bool = True,
) -> StatisticsQualityPersistResult:
    """Append audit evidence and optionally mark analytics projections quarantined.

    Existing ``ambiguous`` and ``unavailable`` states are never downgraded.
    Existing reasons are preserved; a quality scan only fills an empty reason.
    """

    settings = get_settings()
    normalized_scan = _safe(scan_id or uuid.uuid4().hex, 100)
    if not bool(settings.STATISTICS_QUALITY_ENABLED):
        return StatisticsQualityPersistResult(status="disabled", scan_id=normalized_scan)
    if period_id is not None and int(period_id) <= 0:
        raise ValueError("period_id must be positive")

    detected_at = _now()
    audit_rows = 0
    signal_quarantine = 0
    execution_quarantine = 0
    quarantine_by_entity: dict[tuple[str, str], tuple[str, str]] = {}
    for issue in report.quarantine_rows:
        quarantine_by_entity.setdefault(
            (issue.entity_type, issue.entity_id),
            (issue.issue_code, issue.reason),
        )

    async with connect() as conn:
        if is_postgres():
            async with conn.transaction():
                for issue in report.issues:
                    result = await conn.execute(
                        """
                        INSERT INTO statistics_quality_audit(
                          audit_key,scan_id,period_id,entity_type,entity_id,action,
                          issue_code,severity,recoverable,reason,actor_user_id,
                          metadata_json,created_at
                        ) VALUES($1,$2,$3,$4,$5,'detected',$6,$7,$8,$9,$10,$11,$12)
                        ON CONFLICT(audit_key) DO NOTHING
                        """,
                        _audit_key(normalized_scan, issue.entity_type, issue.entity_id, issue.issue_code),
                        normalized_scan,
                        period_id,
                        issue.entity_type,
                        issue.entity_id,
                        issue.issue_code,
                        issue.severity,
                        int(issue.recoverable),
                        _safe(issue.reason),
                        actor_user_id,
                        json.dumps({"related_ids": list(issue.related_ids)}, sort_keys=True),
                        detected_at,
                    )
                    audit_rows += int(str(result).endswith(" 1"))

                if apply_quarantine:
                    for (entity_type, entity_id), (code, reason) in quarantine_by_entity.items():
                        try:
                            numeric_id = int(entity_id)
                        except (TypeError, ValueError, OverflowError):
                            continue
                        bounded_reason = _safe(f"{code}: {reason}")
                        if entity_type == "signal":
                            result = await conn.execute(
                                """
                                UPDATE signal_analytics_signals
                                SET data_quality_status=CASE
                                      WHEN data_quality_status IN ('ambiguous','unavailable')
                                        THEN data_quality_status
                                      ELSE 'quarantined' END,
                                    data_quality_reason=COALESCE(NULLIF(data_quality_reason,''),$1),
                                    updated_at=NOW()
                                WHERE id=$2
                                """,
                                bounded_reason,
                                numeric_id,
                            )
                            signal_quarantine += int(str(result).endswith(" 1"))
                        elif entity_type == "execution":
                            result = await conn.execute(
                                """
                                UPDATE analytics_execution_results
                                SET data_quality_status=CASE
                                      WHEN data_quality_status IN ('ambiguous','unavailable')
                                        THEN data_quality_status
                                      ELSE 'quarantined' END,
                                    ambiguity_reason=COALESCE(NULLIF(ambiguity_reason,''),$1),
                                    updated_at=NOW()
                                WHERE execution_id=$2
                                """,
                                bounded_reason,
                                numeric_id,
                            )
                            execution_quarantine += int(str(result).endswith(" 1"))
            return StatisticsQualityPersistResult(
                status="persisted",
                scan_id=normalized_scan,
                audit_rows=audit_rows,
                signal_rows_quarantined=signal_quarantine,
                execution_rows_quarantined=execution_quarantine,
            )

        await conn.execute("BEGIN IMMEDIATE")
        try:
            for issue in report.issues:
                cursor = await conn.execute(
                    """
                    INSERT OR IGNORE INTO statistics_quality_audit(
                      audit_key,scan_id,period_id,entity_type,entity_id,action,
                      issue_code,severity,recoverable,reason,actor_user_id,
                      metadata_json,created_at
                    ) VALUES(?,?,?,?,?,'detected',?,?,?,?,?,?,?)
                    """,
                    (
                        _audit_key(normalized_scan, issue.entity_type, issue.entity_id, issue.issue_code),
                        normalized_scan,
                        period_id,
                        issue.entity_type,
                        issue.entity_id,
                        issue.issue_code,
                        issue.severity,
                        int(issue.recoverable),
                        _safe(issue.reason),
                        actor_user_id,
                        json.dumps({"related_ids": list(issue.related_ids)}, sort_keys=True),
                        detected_at.isoformat(),
                    ),
                )
                audit_rows += max(0, int(getattr(cursor, "rowcount", 0) or 0))

            if apply_quarantine:
                for (entity_type, entity_id), (code, reason) in quarantine_by_entity.items():
                    try:
                        numeric_id = int(entity_id)
                    except (TypeError, ValueError, OverflowError):
                        continue
                    bounded_reason = _safe(f"{code}: {reason}")
                    if entity_type == "signal":
                        cursor = await conn.execute(
                            """
                            UPDATE signal_analytics_signals
                            SET data_quality_status=CASE
                                  WHEN data_quality_status IN ('ambiguous','unavailable')
                                    THEN data_quality_status
                                  ELSE 'quarantined' END,
                                data_quality_reason=COALESCE(NULLIF(data_quality_reason,''),?),
                                updated_at=CURRENT_TIMESTAMP
                            WHERE id=?
                            """,
                            (bounded_reason, numeric_id),
                        )
                        signal_quarantine += max(0, int(getattr(cursor, "rowcount", 0) or 0))
                    elif entity_type == "execution":
                        cursor = await conn.execute(
                            """
                            UPDATE analytics_execution_results
                            SET data_quality_status=CASE
                                  WHEN data_quality_status IN ('ambiguous','unavailable')
                                    THEN data_quality_status
                                  ELSE 'quarantined' END,
                                ambiguity_reason=COALESCE(NULLIF(ambiguity_reason,''),?),
                                updated_at=CURRENT_TIMESTAMP
                            WHERE execution_id=?
                            """,
                            (bounded_reason, numeric_id),
                        )
                        execution_quarantine += max(0, int(getattr(cursor, "rowcount", 0) or 0))
            await conn.commit()
        except BaseException:
            await conn.rollback()
            raise

    return StatisticsQualityPersistResult(
        status="persisted",
        scan_id=normalized_scan,
        audit_rows=audit_rows,
        signal_rows_quarantined=signal_quarantine,
        execution_rows_quarantined=execution_quarantine,
    )


async def record_statistics_recovery_audit(
    *,
    audit_key: str,
    period_id: int | None,
    entity_type: str,
    entity_id: str,
    issue_code: str,
    action: str,
    reason: str,
    actor_user_id: int,
    before_status: str | None = None,
    after_status: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> bool:
    """Append one immutable recovery audit event for the future admin command.

    This function records evidence only.  It deliberately does not change a
    signal/execution, preventing an unaudited helper from becoming a hidden
    repair path.
    """

    if action not in {"recovery_requested", "recovery_approved", "recovery_applied", "recovery_rejected", "resolved"}:
        raise ValueError("unsupported recovery audit action")
    if entity_type not in {"signal", "execution", "fill", "dataset"}:
        raise ValueError("unsupported recovery entity_type")
    if not bool(get_settings().STATISTICS_QUALITY_ENABLED):
        return False
    payload = json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True, default=str)
    async with connect() as conn:
        if is_postgres():
            result = await conn.execute(
                """
                INSERT INTO statistics_quality_audit(
                  audit_key,scan_id,period_id,entity_type,entity_id,action,
                  issue_code,severity,recoverable,reason,actor_user_id,
                  before_status,after_status,metadata_json,created_at
                ) VALUES($1,'recovery',$2,$3,$4,$5,$6,'info',1,$7,$8,$9,$10,$11,NOW())
                ON CONFLICT(audit_key) DO NOTHING
                """,
                _safe(audit_key, 200),
                period_id,
                entity_type,
                _safe(entity_id, 100),
                action,
                _safe(issue_code, 160),
                _safe(reason),
                actor_user_id,
                _safe(before_status, 100) or None,
                _safe(after_status, 100) or None,
                payload,
            )
            return str(result).endswith(" 1")
        cursor = await conn.execute(
            """
            INSERT OR IGNORE INTO statistics_quality_audit(
              audit_key,scan_id,period_id,entity_type,entity_id,action,
              issue_code,severity,recoverable,reason,actor_user_id,
              before_status,after_status,metadata_json,created_at
            ) VALUES(?,'recovery',?,?,?,?,?,'info',1,?,?,?,?,?,CURRENT_TIMESTAMP)
            """,
            (
                _safe(audit_key, 200),
                period_id,
                entity_type,
                _safe(entity_id, 100),
                action,
                _safe(issue_code, 160),
                _safe(reason),
                actor_user_id,
                _safe(before_status, 100) or None,
                _safe(after_status, 100) or None,
                payload,
            ),
        )
        await conn.commit()
        return max(0, int(getattr(cursor, "rowcount", 0) or 0)) == 1
