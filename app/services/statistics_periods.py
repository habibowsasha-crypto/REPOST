"""Safe statistics-period management for plan step 8.

The module is isolated from trading.  A reset never deletes historical rows; it
atomically closes the active statistics period and creates exactly one new
active period.  Confirmation requests are durable, expiring and bound to the
admin, token and active period that were previewed.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from app import __version__
from app.config import get_settings
from app.database.db import connect, is_postgres, monitor_db_workload

_RESET_ADVISORY_LOCK_KEY = 2147483543
_RESET_TOKEN_TTL_SECONDS = 600
_MAX_PERIODS = 200


@dataclass(frozen=True, slots=True)
class StatisticsPeriod:
    id: int
    name: str
    status: str
    period_kind: str
    started_at: datetime | None
    closed_at: datetime | None
    created_by: int | None
    reset_reason: str | None
    source_version: str
    settings_snapshot_json: str
    signal_count: int = 0
    execution_count: int = 0
    final_execution_count: int = 0
    quality_issue_count: int = 0


@dataclass(frozen=True, slots=True)
class StatisticsResetRequest:
    request_id: int
    token: str
    actor_user_id: int
    reason: str
    active_period_id: int
    active_period_name: str
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class StatisticsResetResult:
    status: str
    request_id: int
    old_period_id: int | None = None
    new_period_id: int | None = None
    new_period_name: str | None = None
    reason: str = ""


def _as_dict(row: Any) -> dict[str, Any]:
    try:
        return dict(row)
    except Exception:
        return {}


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


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError, OverflowError):
        return 0


def _safe_reason(value: str | None) -> str:
    reason = " ".join(str(value or "manual_admin_reset").strip().split())
    return (reason or "manual_admin_reset")[:500]


def _token_hash(token: str) -> str:
    return hashlib.sha256(str(token).encode("utf-8")).hexdigest()


def _settings_snapshot() -> str:
    """Return a deliberately allow-listed, secret-free runtime snapshot."""

    settings = get_settings()
    keys: Iterable[str] = (
        "SIGNAL_ANALYTICS_ENABLED",
        "SIGNAL_ANALYTICS_INGRESS_ENABLED",
        "SIGNAL_ANALYTICS_TRACKING_ENABLED",
        "SIGNAL_ANALYTICS_RECOVERY_ENABLED",
        "SIGNAL_ANALYTICS_REPORTS_ENABLED",
        "SIGNAL_ANALYTICS_SIMULATION_ENABLED",
        "STATISTICS_PERIODS_ENABLED",
        "STATISTICS_EXECUTION_RESULTS_ENABLED",
        "STATISTICS_FUNDING_ENABLED",
        "STATISTICS_FUNDING_ZERO_CONFIRMATIONS",
        "STATISTICS_FUNDING_ZERO_GRACE_SEC",
        "STATISTICS_FUNDING_MAX_RECOVERY_ATTEMPTS",
        "STATISTICS_FUNDING_RECOVERY_DEADLINE_SEC",
        "STATISTICS_LINKAGE_ENABLED",
        "STATISTICS_RECOVERY_ENABLED",
        "STATISTICS_QUALITY_ENABLED",
        "STATS_RESET_ENABLED",
        "STATS_V2_REPORTS_ENABLED",
        "STATISTICS_SHADOW_EXPECTED_STAGE",
        "STATISTICS_SHADOW_REQUIRE_POSTGRES",
        "STATISTICS_SHADOW_DB_DIAGNOSTICS_ENABLED",
        "STATISTICS_SHADOW_STRICT_STARTUP",
        "FINANCIAL_RECONCILIATION_ENABLED",
        "FINANCIAL_RECONCILIATION_WORKERS",
        "FINANCIAL_RECONCILIATION_REQUESTS_PER_SECOND",
    )
    payload = {key: getattr(settings, key, None) for key in keys}
    payload["app_version"] = __version__
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)


def _period_from_row(row: Any) -> StatisticsPeriod | None:
    data = _as_dict(row)
    period_id = _int(data.get("id"))
    if period_id <= 0:
        return None
    return StatisticsPeriod(
        id=period_id,
        name=str(data.get("name") or f"period_{period_id}"),
        status=str(data.get("status") or "unknown"),
        period_kind=str(data.get("period_kind") or "legacy"),
        started_at=_dt(data.get("started_at")),
        closed_at=_dt(data.get("closed_at")),
        created_by=(
            _int(data.get("created_by"))
            if data.get("created_by") not in (None, "")
            else None
        ),
        reset_reason=(
            str(data.get("reset_reason"))
            if data.get("reset_reason") not in (None, "")
            else None
        ),
        source_version=str(data.get("source_version") or "unknown"),
        settings_snapshot_json=str(data.get("settings_snapshot_json") or "{}"),
        signal_count=_int(data.get("signal_count")),
        execution_count=_int(data.get("execution_count")),
        final_execution_count=_int(data.get("final_execution_count")),
        quality_issue_count=_int(data.get("quality_issue_count")),
    )


_PERIOD_SELECT = """
SELECT p.*,
  (SELECT COUNT(*) FROM signal_analytics_signals s WHERE s.period_id=p.id) AS signal_count,
  (SELECT COUNT(*) FROM analytics_execution_results r WHERE r.period_id=p.id) AS execution_count,
  (SELECT COUNT(*) FROM analytics_execution_results r WHERE r.period_id=p.id AND UPPER(r.financial_state)='FINAL') AS final_execution_count,
  (SELECT COUNT(*) FROM statistics_quality_audit q WHERE q.period_id=p.id AND q.action='detected') AS quality_issue_count
FROM statistics_periods p
"""


async def get_active_statistics_period() -> StatisticsPeriod | None:
    async with monitor_db_workload(stage="statistics_periods"):
        async with connect() as conn:
            query = _PERIOD_SELECT + " WHERE p.status='active' ORDER BY p.id DESC LIMIT 1"
            if is_postgres():
                row = await conn.fetchrow(query)
            else:
                cursor = await conn.execute(query)
                row = await cursor.fetchone()
    return _period_from_row(row)


async def get_statistics_period(period_id: int) -> StatisticsPeriod | None:
    normalized = int(period_id)
    if normalized <= 0:
        return None
    async with monitor_db_workload(stage="statistics_periods"):
        async with connect() as conn:
            query = _PERIOD_SELECT + (" WHERE p.id=$1" if is_postgres() else " WHERE p.id=?")
            if is_postgres():
                row = await conn.fetchrow(query, normalized)
            else:
                cursor = await conn.execute(query, (normalized,))
                row = await cursor.fetchone()
    return _period_from_row(row)


async def list_statistics_periods(*, limit: int = 50) -> tuple[StatisticsPeriod, ...]:
    bounded = max(1, min(_MAX_PERIODS, int(limit)))
    async with monitor_db_workload(stage="statistics_periods"):
        async with connect() as conn:
            query = _PERIOD_SELECT + (
                " ORDER BY p.started_at DESC,p.id DESC LIMIT $1"
                if is_postgres()
                else " ORDER BY p.started_at DESC,p.id DESC LIMIT ?"
            )
            if is_postgres():
                rows = await conn.fetch(query, bounded)
            else:
                cursor = await conn.execute(query, (bounded,))
                rows = await cursor.fetchall()
    return tuple(period for row in rows if (period := _period_from_row(row)) is not None)


async def create_statistics_reset_request(
    *,
    actor_user_id: int,
    reason: str | None = None,
    ttl_seconds: int = _RESET_TOKEN_TTL_SECONDS,
) -> StatisticsResetRequest:
    """Create a durable reset preview bound to the current active period."""

    actor = int(actor_user_id)
    if actor <= 0:
        raise ValueError("actor_user_id must be positive")
    if not bool(get_settings().STATS_RESET_ENABLED):
        raise PermissionError("STATS_RESET_ENABLED is false")
    token = secrets.token_hex(8)
    digest = _token_hash(token)
    normalized_reason = _safe_reason(reason)
    now = datetime.now(timezone.utc)
    expires = now + timedelta(seconds=max(60, min(3600, int(ttl_seconds))))
    snapshot = _settings_snapshot()

    async with monitor_db_workload(stage="statistics_reset"):
        async with connect() as conn:
            if is_postgres():
                async with conn.transaction():
                    active = await conn.fetchrow(
                        "SELECT id,name FROM statistics_periods WHERE status='active' ORDER BY id DESC LIMIT 1 FOR SHARE"
                    )
                    if not active:
                        raise RuntimeError("active statistics period is missing")
                    row = await conn.fetchrow(
                        """
                        INSERT INTO statistics_reset_requests(
                          token_hash,actor_user_id,reason,status,expires_at,
                          old_period_id,source_version,settings_snapshot_json,
                          created_at,updated_at
                        ) VALUES($1,$2,$3,'pending',$4,$5,$6,$7,NOW(),NOW())
                        RETURNING id
                        """,
                        digest,
                        actor,
                        normalized_reason,
                        expires,
                        int(active["id"]),
                        __version__,
                        snapshot,
                    )
                    request_id = int(row["id"])
                    active_id = int(active["id"])
                    active_name = str(active["name"])
            else:
                await conn.execute("BEGIN IMMEDIATE")
                try:
                    cursor = await conn.execute(
                        "SELECT id,name FROM statistics_periods WHERE status='active' ORDER BY id DESC LIMIT 1"
                    )
                    active = await cursor.fetchone()
                    if not active:
                        raise RuntimeError("active statistics period is missing")
                    cursor = await conn.execute(
                        """
                        INSERT INTO statistics_reset_requests(
                          token_hash,actor_user_id,reason,status,expires_at,
                          old_period_id,source_version,settings_snapshot_json,
                          created_at,updated_at
                        ) VALUES(?,?,?,'pending',?,?,?,?,?,?)
                        """,
                        (
                            digest,
                            actor,
                            normalized_reason,
                            expires.isoformat(),
                            int(active[0]),
                            __version__,
                            snapshot,
                            now.isoformat(),
                            now.isoformat(),
                        ),
                    )
                    request_id = int(cursor.lastrowid)
                    active_id = int(active[0])
                    active_name = str(active[1])
                    await conn.commit()
                except BaseException:
                    await conn.rollback()
                    raise

    return StatisticsResetRequest(
        request_id=request_id,
        token=token,
        actor_user_id=actor,
        reason=normalized_reason,
        active_period_id=active_id,
        active_period_name=active_name,
        expires_at=expires,
    )


async def confirm_statistics_reset(
    *,
    request_id: int,
    token: str,
    actor_user_id: int,
) -> StatisticsResetResult:
    """Atomically close the previewed period and create one new active period.

    The same confirmed request is idempotent.  A different request previewed
    against an older active period is rejected as stale instead of immediately
    closing the newly-created period.
    """

    request_id = int(request_id)
    actor = int(actor_user_id)
    if request_id <= 0 or actor <= 0 or not token:
        return StatisticsResetResult("invalid", request_id, reason="invalid_request")
    if not bool(get_settings().STATS_RESET_ENABLED):
        return StatisticsResetResult("disabled", request_id, reason="reset_flag_off")
    digest = _token_hash(token)
    now = datetime.now(timezone.utc)

    async with monitor_db_workload(stage="statistics_reset"):
        async with connect() as conn:
            if is_postgres():
                async with conn.transaction():
                    await conn.execute(
                        "SELECT pg_advisory_xact_lock($1)", _RESET_ADVISORY_LOCK_KEY
                    )
                    row = await conn.fetchrow(
                        "SELECT * FROM statistics_reset_requests WHERE id=$1 FOR UPDATE",
                        request_id,
                    )
                    if not row:
                        return StatisticsResetResult("not_found", request_id)
                    data = _as_dict(row)
                    if int(data.get("actor_user_id") or 0) != actor or not hmac.compare_digest(
                        str(data.get("token_hash") or ""), digest
                    ):
                        return StatisticsResetResult("forbidden", request_id)
                    if str(data.get("status") or "") == "applied":
                        new_id = _int(data.get("new_period_id")) or None
                        new_name = None
                        if new_id:
                            new_name = await conn.fetchval(
                                "SELECT name FROM statistics_periods WHERE id=$1", new_id
                            )
                        return StatisticsResetResult(
                            "already_applied",
                            request_id,
                            _int(data.get("old_period_id")) or None,
                            new_id,
                            str(new_name) if new_name else None,
                        )
                    if str(data.get("status") or "") != "pending":
                        return StatisticsResetResult(
                            str(data.get("status") or "invalid"), request_id
                        )
                    expires = _dt(data.get("expires_at"))
                    if expires is None or expires < now:
                        await conn.execute(
                            "UPDATE statistics_reset_requests SET status='expired',updated_at=NOW() WHERE id=$1",
                            request_id,
                        )
                        return StatisticsResetResult("expired", request_id)
                    active = await conn.fetchrow(
                        "SELECT id FROM statistics_periods WHERE status='active' ORDER BY id DESC LIMIT 1 FOR UPDATE"
                    )
                    old_period_id = _int(data.get("old_period_id"))
                    if not active or int(active["id"]) != old_period_id:
                        await conn.execute(
                            "UPDATE statistics_reset_requests SET status='stale',updated_at=NOW() WHERE id=$1",
                            request_id,
                        )
                        return StatisticsResetResult(
                            "stale", request_id, old_period_id or None, reason="active_period_changed"
                        )
                    await conn.execute(
                        "UPDATE statistics_periods SET status='closed',closed_at=NOW(),updated_at=NOW() WHERE id=$1 AND status='active'",
                        old_period_id,
                    )
                    period_name = f"production_{now:%Y%m%d_%H%M%S}_{request_id}"
                    new_row = await conn.fetchrow(
                        """
                        INSERT INTO statistics_periods(
                          name,status,period_kind,started_at,created_by,reset_reason,
                          source_version,settings_snapshot_json,created_at,updated_at
                        ) VALUES($1,'active','production',NOW(),$2,$3,$4,$5,NOW(),NOW())
                        RETURNING id,name
                        """,
                        period_name,
                        actor,
                        str(data.get("reason") or "manual_admin_reset"),
                        str(data.get("source_version") or __version__),
                        str(data.get("settings_snapshot_json") or "{}"),
                    )
                    new_id = int(new_row["id"])
                    await conn.execute(
                        """
                        UPDATE statistics_reset_requests
                        SET status='applied',new_period_id=$1,applied_at=NOW(),updated_at=NOW()
                        WHERE id=$2
                        """,
                        new_id,
                        request_id,
                    )
                    return StatisticsResetResult(
                        "applied", request_id, old_period_id, new_id, str(new_row["name"])
                    )

            await conn.execute("BEGIN IMMEDIATE")
            try:
                cursor = await conn.execute(
                    "SELECT * FROM statistics_reset_requests WHERE id=?", (request_id,)
                )
                row = await cursor.fetchone()
                if not row:
                    await conn.rollback()
                    return StatisticsResetResult("not_found", request_id)
                data = _as_dict(row)
                if int(data.get("actor_user_id") or 0) != actor or not hmac.compare_digest(
                    str(data.get("token_hash") or ""), digest
                ):
                    await conn.rollback()
                    return StatisticsResetResult("forbidden", request_id)
                if str(data.get("status") or "") == "applied":
                    new_id = _int(data.get("new_period_id")) or None
                    new_name = None
                    if new_id:
                        cursor = await conn.execute(
                            "SELECT name FROM statistics_periods WHERE id=?", (new_id,)
                        )
                        name_row = await cursor.fetchone()
                        new_name = str(name_row[0]) if name_row else None
                    await conn.commit()
                    return StatisticsResetResult(
                        "already_applied",
                        request_id,
                        _int(data.get("old_period_id")) or None,
                        new_id,
                        new_name,
                    )
                if str(data.get("status") or "") != "pending":
                    await conn.commit()
                    return StatisticsResetResult(
                        str(data.get("status") or "invalid"), request_id
                    )
                expires = _dt(data.get("expires_at"))
                if expires is None or expires < now:
                    await conn.execute(
                        "UPDATE statistics_reset_requests SET status='expired',updated_at=? WHERE id=?",
                        (now.isoformat(), request_id),
                    )
                    await conn.commit()
                    return StatisticsResetResult("expired", request_id)
                cursor = await conn.execute(
                    "SELECT id FROM statistics_periods WHERE status='active' ORDER BY id DESC LIMIT 1"
                )
                active = await cursor.fetchone()
                old_period_id = _int(data.get("old_period_id"))
                if not active or int(active[0]) != old_period_id:
                    await conn.execute(
                        "UPDATE statistics_reset_requests SET status='stale',updated_at=? WHERE id=?",
                        (now.isoformat(), request_id),
                    )
                    await conn.commit()
                    return StatisticsResetResult(
                        "stale", request_id, old_period_id or None, reason="active_period_changed"
                    )
                await conn.execute(
                    "UPDATE statistics_periods SET status='closed',closed_at=?,updated_at=? WHERE id=? AND status='active'",
                    (now.isoformat(), now.isoformat(), old_period_id),
                )
                period_name = f"production_{now:%Y%m%d_%H%M%S}_{request_id}"
                cursor = await conn.execute(
                    """
                    INSERT INTO statistics_periods(
                      name,status,period_kind,started_at,created_by,reset_reason,
                      source_version,settings_snapshot_json,created_at,updated_at
                    ) VALUES(?,'active','production',?,?,?,?,?,?,?)
                    """,
                    (
                        period_name,
                        now.isoformat(),
                        actor,
                        str(data.get("reason") or "manual_admin_reset"),
                        str(data.get("source_version") or __version__),
                        str(data.get("settings_snapshot_json") or "{}"),
                        now.isoformat(),
                        now.isoformat(),
                    ),
                )
                new_id = int(cursor.lastrowid)
                await conn.execute(
                    """
                    UPDATE statistics_reset_requests
                    SET status='applied',new_period_id=?,applied_at=?,updated_at=?
                    WHERE id=?
                    """,
                    (new_id, now.isoformat(), now.isoformat(), request_id),
                )
                await conn.commit()
                return StatisticsResetResult(
                    "applied", request_id, old_period_id, new_id, period_name
                )
            except BaseException:
                await conn.rollback()
                raise


async def cancel_statistics_reset(
    *, request_id: int, token: str, actor_user_id: int
) -> bool:
    request_id = int(request_id)
    actor = int(actor_user_id)
    digest = _token_hash(token)
    async with connect() as conn:
        if is_postgres():
            result = await conn.execute(
                """
                UPDATE statistics_reset_requests
                SET status='cancelled',updated_at=NOW()
                WHERE id=$1 AND actor_user_id=$2 AND token_hash=$3 AND status='pending'
                """,
                request_id,
                actor,
                digest,
            )
            return str(result).endswith(" 1")
        cursor = await conn.execute(
            """
            UPDATE statistics_reset_requests
            SET status='cancelled',updated_at=CURRENT_TIMESTAMP
            WHERE id=? AND actor_user_id=? AND token_hash=? AND status='pending'
            """,
            (request_id, actor, digest),
        )
        await conn.commit()
        return max(0, int(getattr(cursor, "rowcount", 0) or 0)) == 1
