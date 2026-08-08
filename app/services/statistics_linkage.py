"""Fail-open exact statistics linkage for SIGNAL -> GROUP -> EXECUTION.

Step g5b3g14 deliberately reuses the existing single analytics DB writer.  The
trading handler only enqueues primitive IDs with ``put_nowait``; all SQL runs in
this low-priority module.  Durable trade_groups/trade_executions are also scanned
periodically, so a full queue or Railway restart cannot permanently lose a link.

The linker is fail-closed:
- a signal is matched only by exact source chat/message + signal fingerprint;
- symbol, side and entry type must also agree;
- one trade group cannot be silently assigned to two analytics signals;
- one execution projection cannot be silently moved between signals.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from app.config import get_settings
from app.database.db import connect, is_postgres

log = logging.getLogger(__name__)

LINKAGE_STATUS_LINKED = "linked_exact"
LINKAGE_STATUS_CONFLICT = "conflict"
LINKAGE_STATUS_UNLINKED = "unlinked"

_PG_GROUP_LOCK_BASE = 8_104_000_000_000_000
_PG_EXECUTION_LOCK_BASE = 8_104_100_000_000_000
_PG_DEADLOCK_RETRIES = 3


@dataclass(frozen=True, slots=True)
class StatisticsLinkageBatchResult:
    periods_assigned: int = 0
    groups_linked: int = 0
    groups_conflicted: int = 0
    executions_projected: int = 0
    executions_conflicted: int = 0
    missing: int = 0

    def merge(
        self, other: "StatisticsLinkageBatchResult"
    ) -> "StatisticsLinkageBatchResult":
        return StatisticsLinkageBatchResult(
            periods_assigned=self.periods_assigned + other.periods_assigned,
            groups_linked=self.groups_linked + other.groups_linked,
            groups_conflicted=self.groups_conflicted + other.groups_conflicted,
            executions_projected=(
                self.executions_projected + other.executions_projected
            ),
            executions_conflicted=(
                self.executions_conflicted + other.executions_conflicted
            ),
            missing=self.missing + other.missing,
        )


def _row_dict(row: Any) -> dict[str, Any]:
    return dict(row) if row is not None else {}


def _positive_ids(values: Iterable[int]) -> tuple[int, ...]:
    result: set[int] = set()
    for value in values:
        try:
            parsed = int(value)
        except (TypeError, ValueError, OverflowError):
            continue
        if parsed > 0:
            result.add(parsed)
    return tuple(sorted(result))


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    try:
        parsed = json.loads(str(value or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return list(parsed) if isinstance(parsed, list) else []


def _canonical_tp_distribution(value: Any, *, target_count: int) -> str | None:
    if target_count <= 0 or target_count > 20:
        return None
    raw = _json_list(value)
    if len(raw) != target_count:
        return None
    values: list[Decimal] = []
    for item in raw:
        parsed = _decimal_or_none(item)
        if parsed is None or parsed <= 0:
            return None
        values.append(parsed)
    if abs(sum(values, Decimal()) - Decimal("100")) > Decimal("0.02"):
        return None
    normalized = [
        int(item) if item == item.to_integral_value() else float(item)
        for item in values
    ]
    return json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))


def _canonical_target_levels(value: Any) -> tuple[Decimal, ...] | None:
    raw = _json_list(value)
    if not raw or len(raw) > 20:
        return None
    result: list[Decimal] = []
    for item in raw:
        parsed = _decimal_or_none(item)
        if parsed is None or parsed <= 0:
            return None
        result.append(parsed)
    return tuple(result)


async def _promote_signal_tp_consensus(
    conn: Any, *, signal_id: int, group_id: int
) -> str:
    """Promote an allocation only when all exact locked executions agree."""
    if is_postgres():
        signal = _row_dict(await conn.fetchrow(
            "SELECT targets_json,target_percents_json,target_percents_source "
            "FROM signal_analytics_signals WHERE id=$1", signal_id
        ))
        rows = await conn.fetch(
            "SELECT targets_json,tp_distribution_json,tp_distribution_locked "
            "FROM trade_executions WHERE trade_group_id=$1 ORDER BY id", group_id
        )
    else:
        cur = await conn.execute(
            "SELECT targets_json,target_percents_json,target_percents_source "
            "FROM signal_analytics_signals WHERE id=?", (signal_id,)
        )
        signal = _row_dict(await cur.fetchone())
        cur = await conn.execute(
            "SELECT targets_json,tp_distribution_json,tp_distribution_locked "
            "FROM trade_executions WHERE trade_group_id=? ORDER BY id", (group_id,)
        )
        rows = await cur.fetchall()
    targets = _canonical_target_levels(signal.get("targets_json")) or ()
    explicit = _canonical_tp_distribution(
        signal.get("target_percents_json"), target_count=len(targets)
    )
    current_source = str(signal.get("target_percents_source") or "").lower()

    # Once at least one execution exists, simulation must use the exact rounded
    # allocations that were really sent for execution. Configured/source
    # percentages are only a provisional fallback before any execution exists.
    if rows:
        distributions: list[str] = []
        incomplete = False
        variant = False
        for raw_row in rows:
            row = _row_dict(raw_row)
            execution_targets = _canonical_target_levels(row.get("targets_json"))
            locked = int(row.get("tp_distribution_locked") or 0) == 1
            if execution_targets is None or not locked:
                incomplete = True
                continue
            distribution = _canonical_tp_distribution(
                row.get("tp_distribution_json"), target_count=len(execution_targets)
            )
            if distribution is None:
                incomplete = True
                continue
            if execution_targets != targets:
                variant = True
                continue
            distributions.append(distribution)
        unique = sorted(set(distributions))
        if variant:
            source, value = "execution_variant", "[]"
        elif len(unique) > 1:
            source, value = "execution_conflict", "[]"
        elif unique and not incomplete:
            source, value = "execution_consensus", unique[0]
        elif incomplete:
            source, value = "execution_incomplete", "[]"
        else:
            source, value = "empty", "[]"
    elif explicit is not None and current_source in {
        "source_signal", "source_message", "source_or_empty"
    }:
        source, value = "source_signal", explicit
    else:
        source, value = "empty", "[]"
    if is_postgres():
        await conn.execute(
            "UPDATE signal_analytics_signals SET target_percents_json=$1,"
            "target_percents_source=$2,updated_at=NOW() WHERE id=$3",
            value, source, signal_id
        )
    else:
        await conn.execute(
            "UPDATE signal_analytics_signals SET target_percents_json=?,"
            "target_percents_source=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (value, source, signal_id)
        )
    return source


async def _active_period_id(conn: Any) -> int | None:
    if is_postgres():
        value = await conn.fetchval(
            "SELECT id FROM statistics_periods "
            "WHERE status='active' ORDER BY id LIMIT 1"
        )
        return int(value) if value is not None else None
    cursor = await conn.execute(
        "SELECT id FROM statistics_periods "
        "WHERE status='active' ORDER BY id LIMIT 1"
    )
    row = await cursor.fetchone()
    return int(row[0]) if row else None


async def assign_active_period_to_ingest_events(
    ingest_event_ids: Iterable[str],
) -> int:
    """Assign the current period only to newly persisted process observations.

    This is intentionally separate from the core analytics INSERT.  If the
    additive statistics schema is unavailable, core signal analytics keeps
    working and the caller records a fail-open error.
    """

    settings = get_settings()
    if not bool(settings.STATISTICS_PERIODS_ENABLED):
        return 0
    ids = tuple(
        sorted(
            {
                str(value or "").strip()
                for value in ingest_event_ids
                if str(value or "").strip()
            }
        )
    )
    if not ids:
        return 0
    async with connect() as conn:
        period_id = await _active_period_id(conn)
        if period_id is None:
            return 0
        if is_postgres():
            rows = await conn.fetch(
                """
                UPDATE signal_analytics_signals
                SET period_id=$1,
                    legacy_data=0,
                    linkage_status=CASE
                      WHEN trade_group_id IS NULL THEN 'unlinked'
                      ELSE linkage_status END,
                    data_quality_status=CASE
                      WHEN COALESCE(needs_recovery,0)=1 THEN 'recovery_required'
                      ELSE 'partial' END,
                    data_quality_reason=CASE
                      WHEN COALESCE(needs_recovery,0)=1
                        THEN COALESCE(data_quality_reason,'restart_gap_forward_only')
                      ELSE 'new_signal_waiting_linkage' END,
                    updated_at=NOW()
                WHERE period_id IS NULL
                  AND ingest_event_id = ANY($2::text[])
                RETURNING id
                """,
                period_id,
                list(ids),
            )
            return len(rows)
        placeholders = ",".join("?" for _ in ids)
        cursor = await conn.execute(
            f"""
            UPDATE signal_analytics_signals
            SET period_id=?,
                legacy_data=0,
                linkage_status=CASE
                  WHEN trade_group_id IS NULL THEN 'unlinked'
                  ELSE linkage_status END,
                data_quality_status=CASE
                  WHEN COALESCE(needs_recovery,0)=1 THEN 'recovery_required'
                  ELSE 'partial' END,
                data_quality_reason=CASE
                  WHEN COALESCE(needs_recovery,0)=1
                    THEN COALESCE(data_quality_reason,'restart_gap_forward_only')
                  ELSE 'new_signal_waiting_linkage' END,
                updated_at=CURRENT_TIMESTAMP
            WHERE period_id IS NULL
              AND ingest_event_id IN ({placeholders})
            """,
            (period_id, *ids),
        )
        count = max(0, int(getattr(cursor, "rowcount", 0) or 0))
        await conn.commit()
        return count


async def _fetch_group(conn: Any, group_id: int) -> dict[str, Any]:
    if is_postgres():
        return _row_dict(
            await conn.fetchrow("SELECT * FROM trade_groups WHERE id=$1", group_id)
        )
    cursor = await conn.execute("SELECT * FROM trade_groups WHERE id=?", (group_id,))
    return _row_dict(await cursor.fetchone())


def _is_postgres_deadlock(exc: BaseException) -> bool:
    return (
        str(getattr(exc, "sqlstate", "") or "") == "40P01"
        or type(exc).__name__ == "DeadlockDetectedError"
    )


async def _run_postgres_entity_transaction(
    conn: Any,
    *,
    lock_key: int,
    entity_id: int,
    operation: Any,
) -> StatisticsLinkageBatchResult | None:
    """Run one linkage entity in a short, retryable PostgreSQL transaction.

    The previous implementation held one transaction across the whole backlog
    and locked trade_groups with FOR UPDATE.  Railway logs showed a live 40P01
    deadlock during startup.  A transaction-scoped advisory lock serializes the
    same entity, while short transactions prevent thousands of unrelated rows
    from staying locked together.  Busy entities are skipped fail-open and are
    picked up by the next periodic backlog cycle.
    """

    for attempt in range(_PG_DEADLOCK_RETRIES):
        try:
            async with conn.transaction():
                acquired = await conn.fetchval(
                    "SELECT pg_try_advisory_xact_lock($1::bigint)",
                    int(lock_key + entity_id),
                )
                if not bool(acquired):
                    return None
                return await operation(conn, entity_id)
        except Exception as exc:
            if not _is_postgres_deadlock(exc) or attempt + 1 >= _PG_DEADLOCK_RETRIES:
                raise
            # Deterministic bounded jitter avoids two workers retrying in lockstep.
            delay = 0.04 * (attempt + 1) + (entity_id % 17) / 1000
            await asyncio.sleep(delay)
    return None


async def _fetch_exact_signal_candidates(
    conn: Any, group: dict[str, Any]
) -> list[dict[str, Any]]:
    source_chat_id = group.get("source_chat_id")
    source_message_id = group.get("source_message_id")
    fingerprint = str(group.get("signal_hash") or "").strip().lower()
    symbol = str(group.get("symbol") or "").strip().upper()
    side = str(group.get("side") or "").strip().lower()
    entry_type = str(group.get("entry_type") or "").strip().upper()
    if (
        source_chat_id is None
        or source_message_id is None
        or not fingerprint
        or not symbol
        or side not in {"long", "short"}
        or entry_type not in {"LIMIT", "MARKET"}
    ):
        return []
    if is_postgres():
        rows = await conn.fetch(
            """
            SELECT *
            FROM signal_analytics_signals
            WHERE source_chat_id=$1
              AND content_fingerprint=$2
              AND (first_source_message_id=$3 OR last_source_message_id=$3)
              AND UPPER(symbol)=$4
              AND LOWER(side)=$5
              AND UPPER(order_type)=$6
            ORDER BY id ASC
            """,
            int(source_chat_id),
            fingerprint,
            int(source_message_id),
            symbol,
            side,
            entry_type,
        )
        return [_row_dict(row) for row in rows]
    cursor = await conn.execute(
        """
        SELECT *
        FROM signal_analytics_signals
        WHERE source_chat_id=?
          AND content_fingerprint=?
          AND (first_source_message_id=? OR last_source_message_id=?)
          AND UPPER(symbol)=?
          AND LOWER(side)=?
          AND UPPER(order_type)=?
        ORDER BY id ASC
        """,
        (
            int(source_chat_id),
            fingerprint,
            int(source_message_id),
            int(source_message_id),
            symbol,
            side,
            entry_type,
        ),
    )
    return [_row_dict(row) for row in await cursor.fetchall()]


async def _mark_signal_conflict(
    conn: Any, signal_ids: Iterable[int], *, reason: str
) -> int:
    ids = _positive_ids(signal_ids)
    if not ids:
        return 0
    bounded_reason = str(reason or "linkage_conflict")[:160]
    if is_postgres():
        rows = await conn.fetch(
            """
            UPDATE signal_analytics_signals
            SET linkage_status='conflict',
                data_quality_status='ambiguous',
                data_quality_reason=$1,
                updated_at=NOW()
            WHERE id = ANY($2::bigint[])
            RETURNING id
            """,
            bounded_reason,
            list(ids),
        )
        return len(rows)
    placeholders = ",".join("?" for _ in ids)
    cursor = await conn.execute(
        f"""
        UPDATE signal_analytics_signals
        SET linkage_status='conflict',
            data_quality_status='ambiguous',
            data_quality_reason=?,
            updated_at=CURRENT_TIMESTAMP
        WHERE id IN ({placeholders})
        """,
        (bounded_reason, *ids),
    )
    return max(0, int(getattr(cursor, "rowcount", 0) or 0))


async def _existing_group_owner_ids(
    conn: Any, group_id: int, *, exclude_signal_id: int
) -> tuple[int, ...]:
    if is_postgres():
        rows = await conn.fetch(
            "SELECT id FROM signal_analytics_signals "
            "WHERE trade_group_id=$1 AND id<>$2 ORDER BY id",
            group_id,
            exclude_signal_id,
        )
        return tuple(int(row["id"]) for row in rows)
    cursor = await conn.execute(
        "SELECT id FROM signal_analytics_signals "
        "WHERE trade_group_id=? AND id<>? ORDER BY id",
        (group_id, exclude_signal_id),
    )
    return tuple(int(row[0]) for row in await cursor.fetchall())


async def _link_one_group(conn: Any, group_id: int) -> StatisticsLinkageBatchResult:
    group = await _fetch_group(conn, group_id)
    if not group:
        return StatisticsLinkageBatchResult(missing=1)
    candidates = await _fetch_exact_signal_candidates(conn, group)
    if not candidates:
        return StatisticsLinkageBatchResult(missing=1)
    if len(candidates) != 1:
        await _mark_signal_conflict(
            conn,
            (int(row.get("id") or 0) for row in candidates),
            reason="multiple_exact_signal_candidates",
        )
        return StatisticsLinkageBatchResult(groups_conflicted=1)

    signal = candidates[0]
    signal_id = int(signal.get("id") or 0)
    existing_group = int(signal.get("trade_group_id") or 0)
    if existing_group not in {0, group_id}:
        await _mark_signal_conflict(
            conn, (signal_id,), reason="signal_already_owned_by_other_group"
        )
        return StatisticsLinkageBatchResult(groups_conflicted=1)
    owners = await _existing_group_owner_ids(
        conn, group_id, exclude_signal_id=signal_id
    )
    if owners:
        await _mark_signal_conflict(
            conn,
            (signal_id, *owners),
            reason="trade_group_claimed_by_multiple_signals",
        )
        return StatisticsLinkageBatchResult(groups_conflicted=1)

    period_id = signal.get("period_id")
    if period_id is None:
        period_id = await _active_period_id(conn)
    if is_postgres():
        row = await conn.fetchrow(
            """
            UPDATE signal_analytics_signals
            SET trade_group_id=$1,
                legacy_data=CASE
                  WHEN period_id IS NULL AND CAST($2 AS BIGINT) IS NOT NULL THEN 0
                  ELSE legacy_data END,
                period_id=COALESCE(period_id,CAST($2 AS BIGINT)),
                linkage_status='linked_exact',
                linked_at=COALESCE(linked_at,NOW()),
                data_quality_status=CASE
                  WHEN COALESCE(needs_recovery,0)=1 THEN 'recovery_required'
                  WHEN period_id IS NULL AND CAST($2 AS BIGINT) IS NOT NULL THEN 'partial'
                  WHEN COALESCE(legacy_data,1)=1 THEN 'legacy'
                  ELSE 'partial' END,
                data_quality_reason=CASE
                  WHEN COALESCE(needs_recovery,0)=1
                    THEN COALESCE(data_quality_reason,'restart_gap_forward_only')
                  ELSE 'exact_trade_group_link' END,
                updated_at=NOW()
            WHERE id=$3 AND (trade_group_id IS NULL OR trade_group_id=$1)
            RETURNING id
            """,
            group_id,
            int(period_id) if period_id is not None else None,
            signal_id,
        )
        if row is None:
            await _mark_signal_conflict(
                conn, (signal_id,), reason="trade_group_compare_and_set_failed"
            )
            return StatisticsLinkageBatchResult(groups_conflicted=1)
    else:
        cursor = await conn.execute(
            """
            UPDATE signal_analytics_signals
            SET trade_group_id=?,
                legacy_data=CASE
                  WHEN period_id IS NULL AND ? IS NOT NULL THEN 0
                  ELSE legacy_data END,
                period_id=COALESCE(period_id,?),
                linkage_status='linked_exact',
                linked_at=COALESCE(linked_at,CURRENT_TIMESTAMP),
                data_quality_status=CASE
                  WHEN COALESCE(needs_recovery,0)=1 THEN 'recovery_required'
                  WHEN period_id IS NULL AND ? IS NOT NULL THEN 'partial'
                  WHEN COALESCE(legacy_data,1)=1 THEN 'legacy'
                  ELSE 'partial' END,
                data_quality_reason=CASE
                  WHEN COALESCE(needs_recovery,0)=1
                    THEN COALESCE(data_quality_reason,'restart_gap_forward_only')
                  ELSE 'exact_trade_group_link' END,
                updated_at=CURRENT_TIMESTAMP
            WHERE id=? AND (trade_group_id IS NULL OR trade_group_id=?)
            """,
            (
                group_id,
                int(period_id) if period_id is not None else None,
                int(period_id) if period_id is not None else None,
                int(period_id) if period_id is not None else None,
                signal_id,
                group_id,
            ),
        )
        if int(getattr(cursor, "rowcount", 0) or 0) != 1:
            await _mark_signal_conflict(
                conn, (signal_id,), reason="trade_group_compare_and_set_failed"
            )
            return StatisticsLinkageBatchResult(groups_conflicted=1)
    return StatisticsLinkageBatchResult(groups_linked=1)


async def _fetch_execution_link_rows(
    conn: Any, execution_id: int
) -> list[dict[str, Any]]:
    sql = """
        SELECT
          e.id AS execution_id,e.trade_group_id,e.user_id,e.symbol AS execution_symbol,
          e.side AS execution_side,e.entry AS execution_entry,e.stop AS execution_stop,
          e.risk_percent,e.realized_pnl,e.status AS execution_status,
          e.equity_snapshot_usd,e.planned_risk_usd,e.initial_price_risk_usd,
          e.initial_risk_percent_of_equity,e.estimated_fee_risk_usd,
          e.expected_loss_at_stop_usd,e.planned_entry_qty,e.stop_distance,
          e.risk_snapshot_at,e.risk_snapshot_source,e.risk_snapshot_status,
          e.risk_snapshot_reason,e.targets_json AS execution_targets_json,
          e.tp_distribution_json,e.tp_distribution_source,e.tp_distribution_locked,
          e.tp_distribution_version,
          g.entry_type,g.planned_entry,g.symbol AS group_symbol,g.side AS group_side,
          g.source_chat_id,g.source_message_id,g.signal_hash,
          s.id AS analytics_signal_id,s.period_id,s.legacy_data,
          s.symbol AS signal_symbol,s.side AS signal_side,s.order_type AS signal_order_type,
          s.linkage_status AS signal_linkage_status
        FROM trade_executions e
        JOIN trade_groups g ON g.id=e.trade_group_id
        JOIN signal_analytics_signals s ON s.trade_group_id=g.id
        WHERE e.id={placeholder}
        ORDER BY s.id ASC
    """
    if is_postgres():
        rows = await conn.fetch(sql.format(placeholder="$1"), execution_id)
        return [_row_dict(row) for row in rows]
    cursor = await conn.execute(sql.format(placeholder="?"), (execution_id,))
    return [_row_dict(row) for row in await cursor.fetchall()]


async def _mark_projection_conflict(
    conn: Any,
    *,
    execution_id: int,
    user_id: int,
    symbol: str,
    side: str,
    reason: str,
) -> None:
    bounded_reason = str(reason or "execution_linkage_conflict")[:160]
    if is_postgres():
        await conn.execute(
            """
            INSERT INTO analytics_execution_results(
              execution_id,user_id,exchange,symbol,side,linkage_status,
              financial_state,data_quality_status,ambiguity_reason,legacy_data
            ) VALUES($1,$2,'bingx',$3,$4,'conflict','AMBIGUOUS','ambiguous',$5,1)
            ON CONFLICT(execution_id) DO UPDATE SET
              linkage_status='conflict',financial_state='AMBIGUOUS',
              data_quality_status='ambiguous',ambiguity_reason=EXCLUDED.ambiguity_reason,
              updated_at=NOW()
            """,
            execution_id,
            user_id,
            symbol,
            side,
            bounded_reason,
        )
        return
    await conn.execute(
        """
        INSERT INTO analytics_execution_results(
          execution_id,user_id,exchange,symbol,side,linkage_status,
          financial_state,data_quality_status,ambiguity_reason,legacy_data
        ) VALUES(?,?,'bingx',?,?,'conflict','AMBIGUOUS','ambiguous',?,1)
        ON CONFLICT(execution_id) DO UPDATE SET
          linkage_status='conflict',financial_state='AMBIGUOUS',
          data_quality_status='ambiguous',ambiguity_reason=excluded.ambiguity_reason,
          updated_at=CURRENT_TIMESTAMP
        """,
        (execution_id, user_id, symbol, side, bounded_reason),
    )


def _entity_link_fingerprint(row: dict[str, Any]) -> str:
    payload = {
        "signal_id": int(row.get("analytics_signal_id") or 0),
        "trade_group_id": int(row.get("trade_group_id") or 0),
        "execution_id": int(row.get("execution_id") or 0),
        "user_id": int(row.get("user_id") or 0),
        "symbol": str(row.get("execution_symbol") or "").strip().upper(),
        "side": str(row.get("execution_side") or "").strip().lower(),
        "source_chat_id": (
            int(row["source_chat_id"]) if row.get("source_chat_id") is not None else None
        ),
        "source_message_id": (
            int(row["source_message_id"])
            if row.get("source_message_id") is not None
            else None
        ),
        "signal_hash": str(row.get("signal_hash") or "").strip().lower(),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


async def _persist_immutable_entity_link(conn: Any, row: dict[str, Any]) -> bool:
    """Persist exact SIGNAL -> GROUP -> EXECUTION ownership without reassignment.

    The unique execution/fingerprint constraints are the durable source of truth.
    Replays are idempotent.  Any mismatch is marked as a conflict and the caller
    keeps the execution outside FINAL/simulation metrics.
    """

    signal_id = int(row.get("analytics_signal_id") or 0)
    group_id = int(row.get("trade_group_id") or 0)
    execution_id = int(row.get("execution_id") or 0)
    user_id = int(row.get("user_id") or 0)
    symbol = str(row.get("execution_symbol") or "").strip().upper()
    side = str(row.get("execution_side") or "").strip().lower()
    source_chat_id = row.get("source_chat_id")
    source_message_id = row.get("source_message_id")
    fingerprint = _entity_link_fingerprint(row)
    expected = {
        "signal_id": signal_id,
        "trade_group_id": group_id,
        "execution_id": execution_id,
        "user_id": user_id,
        "symbol": symbol,
        "side": side,
        "source_chat_id": int(source_chat_id) if source_chat_id is not None else None,
        "source_message_id": (
            int(source_message_id) if source_message_id is not None else None
        ),
        "identity_fingerprint": fingerprint,
    }
    if is_postgres():
        await conn.execute(
            """
            INSERT INTO statistics_entity_links(
              signal_id,trade_group_id,execution_id,user_id,symbol,side,
              source_chat_id,source_message_id,identity_fingerprint,linkage_status
            ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'linked_exact')
            ON CONFLICT DO NOTHING
            """,
            signal_id,
            group_id,
            execution_id,
            user_id,
            symbol,
            side,
            int(source_chat_id) if source_chat_id is not None else None,
            int(source_message_id) if source_message_id is not None else None,
            fingerprint,
        )
        existing_rows = await conn.fetch(
            """
            SELECT * FROM statistics_entity_links
            WHERE execution_id=$1 OR identity_fingerprint=$2
            ORDER BY id FOR UPDATE
            """,
            execution_id,
            fingerprint,
        )
    else:
        await conn.execute(
            """
            INSERT OR IGNORE INTO statistics_entity_links(
              signal_id,trade_group_id,execution_id,user_id,symbol,side,
              source_chat_id,source_message_id,identity_fingerprint,linkage_status
            ) VALUES(?,?,?,?,?,?,?,?,?,'linked_exact')
            """,
            (
                signal_id,
                group_id,
                execution_id,
                user_id,
                symbol,
                side,
                int(source_chat_id) if source_chat_id is not None else None,
                int(source_message_id) if source_message_id is not None else None,
                fingerprint,
            ),
        )
        cursor = await conn.execute(
            """
            SELECT * FROM statistics_entity_links
            WHERE execution_id=? OR identity_fingerprint=? ORDER BY id
            """,
            (execution_id, fingerprint),
        )
        existing_rows = await cursor.fetchall()
    rows = [_row_dict(item) for item in existing_rows]
    exact = [
        item
        for item in rows
        if all(item.get(key) == value for key, value in expected.items())
    ]
    if len(rows) == 1 and len(exact) == 1:
        return True
    reason = "immutable_entity_link_conflict"
    if is_postgres():
        await conn.execute(
            """
            UPDATE statistics_entity_links SET linkage_status='conflict',
              conflict_reason=$1,updated_at=NOW()
            WHERE execution_id=$2 OR identity_fingerprint=$3
            """,
            reason,
            execution_id,
            fingerprint,
        )
    else:
        await conn.execute(
            """
            UPDATE statistics_entity_links SET linkage_status='conflict',
              conflict_reason=?,updated_at=CURRENT_TIMESTAMP
            WHERE execution_id=? OR identity_fingerprint=?
            """,
            (reason, execution_id, fingerprint),
        )
    return False


async def _project_one_execution(
    conn: Any, execution_id: int
) -> StatisticsLinkageBatchResult:
    rows = await _fetch_execution_link_rows(conn, execution_id)
    if not rows:
        return StatisticsLinkageBatchResult(missing=1)
    row = rows[0]
    user_id = int(row.get("user_id") or 0)
    symbol = str(row.get("execution_symbol") or "").strip().upper()
    side = str(row.get("execution_side") or "").strip().lower()
    if len(rows) != 1:
        await _mark_projection_conflict(
            conn, execution_id=execution_id, user_id=user_id, symbol=symbol,
            side=side, reason="multiple_signal_owners_for_execution_group"
        )
        return StatisticsLinkageBatchResult(executions_conflicted=1)
    signal_id = int(row.get("analytics_signal_id") or 0)
    group_id = int(row.get("trade_group_id") or 0)
    entry_type = str(row.get("entry_type") or "").strip().upper()
    valid = (
        signal_id > 0 and group_id > 0 and user_id > 0 and symbol
        and symbol == str(row.get("group_symbol") or "").strip().upper()
        == str(row.get("signal_symbol") or "").strip().upper()
        and side in {"long", "short"}
        and side == str(row.get("group_side") or "").strip().lower()
        == str(row.get("signal_side") or "").strip().lower()
        and entry_type in {"LIMIT", "MARKET"}
        and entry_type == str(row.get("signal_order_type") or "").strip().upper()
        and str(row.get("signal_linkage_status") or "").strip() == LINKAGE_STATUS_LINKED
    )
    if not valid:
        await _mark_projection_conflict(
            conn, execution_id=execution_id, user_id=user_id, symbol=symbol,
            side=side, reason="execution_group_signal_identity_mismatch"
        )
        return StatisticsLinkageBatchResult(executions_conflicted=1)
    existing = {}
    if is_postgres():
        existing = _row_dict(await conn.fetchrow(
            "SELECT analytics_signal_id,trade_group_id FROM analytics_execution_results WHERE execution_id=$1",
            execution_id
        ))
    else:
        cur = await conn.execute(
            "SELECT analytics_signal_id,trade_group_id FROM analytics_execution_results WHERE execution_id=?",
            (execution_id,)
        )
        existing = _row_dict(await cur.fetchone())
    if existing and (
        int(existing.get("analytics_signal_id") or 0) not in {0, signal_id}
        or int(existing.get("trade_group_id") or 0) not in {0, group_id}
    ):
        await _mark_projection_conflict(
            conn, execution_id=execution_id, user_id=user_id, symbol=symbol,
            side=side, reason="execution_projection_already_owned"
        )
        return StatisticsLinkageBatchResult(executions_conflicted=1)

    period_id = row.get("period_id")
    legacy_data = 1 if int(row.get("legacy_data") or 0) else 0
    decimal_fields = {
        "planned_entry_reference": _decimal_or_none(row.get("planned_entry")),
        "execution_reference_price": _decimal_or_none(row.get("execution_entry")),
        "initial_stop_price": _decimal_or_none(row.get("execution_stop")),
        "planned_risk_percent": _decimal_or_none(row.get("risk_percent")),
        "equity_snapshot_usd": _decimal_or_none(row.get("equity_snapshot_usd")),
        "planned_risk_usd": _decimal_or_none(row.get("planned_risk_usd")),
        "initial_price_risk_usd": _decimal_or_none(row.get("initial_price_risk_usd")),
        "initial_risk_percent_of_equity": _decimal_or_none(row.get("initial_risk_percent_of_equity")),
        "estimated_fee_risk_usd": _decimal_or_none(row.get("estimated_fee_risk_usd")),
        "expected_loss_at_stop_usd": _decimal_or_none(row.get("expected_loss_at_stop_usd")),
        "planned_entry_qty": _decimal_or_none(row.get("planned_entry_qty")),
        "stop_distance": _decimal_or_none(row.get("stop_distance")),
    }
    target_count = len(_json_list(row.get("execution_targets_json")))
    tp_distribution = _canonical_tp_distribution(
        row.get("tp_distribution_json"), target_count=target_count
    )
    tp_locked = 1 if int(row.get("tp_distribution_locked") or 0) == 1 else 0
    if not tp_locked:
        tp_distribution = None
    tp_source = (
        str(row.get("tp_distribution_source") or "").strip()
        if tp_distribution is not None else "missing_or_unlocked"
    )
    quality = "legacy" if legacy_data else "partial"
    columns = [
        "execution_id","analytics_signal_id","trade_group_id","period_id","user_id",
        "exchange","symbol","side","entry_order_type","linkage_status",
        *decimal_fields.keys(),"risk_snapshot_at","risk_snapshot_source",
        "risk_snapshot_status","risk_snapshot_reason","tp_distribution_json",
        "tp_distribution_source","tp_distribution_locked","tp_distribution_version",
        "completeness_mask","data_quality_status","ambiguity_reason","legacy_data"
    ]
    values = [
        execution_id,signal_id,group_id,int(period_id) if period_id is not None else None,user_id,
        "bingx",symbol,side,entry_type,"linked_exact",*decimal_fields.values(),
        row.get("risk_snapshot_at"),row.get("risk_snapshot_source"),
        row.get("risk_snapshot_status"),row.get("risk_snapshot_reason"),
        tp_distribution,tp_source,tp_locked,int(row.get("tp_distribution_version") or 1),
        1,quality,None,legacy_data
    ]
    update_columns = [c for c in columns if c not in {"execution_id","ambiguity_reason"}]
    if is_postgres():
        placeholders = ",".join(f"${i}" for i in range(1, len(values)+1))
        assignments = ",".join(
            f"{c}=EXCLUDED.{c}" if c in {
                "analytics_signal_id","trade_group_id","period_id","user_id","exchange",
                "symbol","side","entry_order_type","linkage_status","data_quality_status",
                "legacy_data","tp_distribution_locked","tp_distribution_version"
            } else f"{c}=COALESCE(EXCLUDED.{c},analytics_execution_results.{c})"
            for c in update_columns
        )
        await conn.execute(
            f"INSERT INTO analytics_execution_results({','.join(columns)}) VALUES({placeholders}) "
            f"ON CONFLICT(execution_id) DO UPDATE SET {assignments},updated_at=NOW() "
            "WHERE (analytics_execution_results.analytics_signal_id IS NULL OR "
            "analytics_execution_results.analytics_signal_id=EXCLUDED.analytics_signal_id) "
            "AND (analytics_execution_results.trade_group_id IS NULL OR "
            "analytics_execution_results.trade_group_id=EXCLUDED.trade_group_id)",
            *values
        )
    else:
        sqlite_values = [str(v) if isinstance(v, Decimal) else v for v in values]
        placeholders = ",".join("?" for _ in sqlite_values)
        assignments = ",".join(
            f"{c}=excluded.{c}" if c in {
                "analytics_signal_id","trade_group_id","period_id","user_id","exchange",
                "symbol","side","entry_order_type","linkage_status","data_quality_status",
                "legacy_data","tp_distribution_locked","tp_distribution_version"
            } else f"{c}=COALESCE(excluded.{c},analytics_execution_results.{c})"
            for c in update_columns
        )
        await conn.execute(
            f"INSERT INTO analytics_execution_results({','.join(columns)}) VALUES({placeholders}) "
            f"ON CONFLICT(execution_id) DO UPDATE SET {assignments},updated_at=CURRENT_TIMESTAMP "
            "WHERE (analytics_execution_results.analytics_signal_id IS NULL OR "
            "analytics_execution_results.analytics_signal_id=excluded.analytics_signal_id) "
            "AND (analytics_execution_results.trade_group_id IS NULL OR "
            "analytics_execution_results.trade_group_id=excluded.trade_group_id)",
            tuple(sqlite_values)
        )
    if is_postgres():
        persisted = _row_dict(await conn.fetchrow(
            "SELECT analytics_signal_id,trade_group_id FROM analytics_execution_results WHERE execution_id=$1",
            execution_id
        ))
    else:
        cur = await conn.execute(
            "SELECT analytics_signal_id,trade_group_id FROM analytics_execution_results WHERE execution_id=?",
            (execution_id,)
        )
        persisted = _row_dict(await cur.fetchone())
    if int(persisted.get("analytics_signal_id") or 0) != signal_id or int(persisted.get("trade_group_id") or 0) != group_id:
        await _mark_projection_conflict(
            conn, execution_id=execution_id, user_id=user_id, symbol=symbol, side=side,
            reason="execution_projection_compare_and_set_failed"
        )
        return StatisticsLinkageBatchResult(executions_conflicted=1)
    if not await _persist_immutable_entity_link(conn, row):
        await _mark_projection_conflict(
            conn, execution_id=execution_id, user_id=user_id, symbol=symbol, side=side,
            reason="immutable_entity_link_conflict"
        )
        await _mark_signal_conflict(
            conn, (signal_id,), reason="immutable_entity_link_conflict"
        )
        return StatisticsLinkageBatchResult(executions_conflicted=1)
    await _promote_signal_tp_consensus(conn, signal_id=signal_id, group_id=group_id)
    return StatisticsLinkageBatchResult(executions_projected=1)


async def _execution_group_id(conn: Any, execution_id: int) -> int | None:
    if is_postgres():
        value = await conn.fetchval(
            "SELECT trade_group_id FROM trade_executions WHERE id=$1", execution_id
        )
        return int(value) if value is not None else None
    cursor = await conn.execute(
        "SELECT trade_group_id FROM trade_executions WHERE id=?", (execution_id,)
    )
    row = await cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else None


async def _group_execution_ids(conn: Any, group_id: int) -> tuple[int, ...]:
    if is_postgres():
        rows = await conn.fetch(
            "SELECT id FROM trade_executions WHERE trade_group_id=$1 ORDER BY id",
            group_id,
        )
        return tuple(int(row["id"]) for row in rows)
    cursor = await conn.execute(
        "SELECT id FROM trade_executions WHERE trade_group_id=? ORDER BY id",
        (group_id,),
    )
    return tuple(int(row[0]) for row in await cursor.fetchall())


async def _backlog_group_ids(conn: Any, limit: int) -> tuple[int, ...]:
    sql = """
        SELECT DISTINCT g.id
        FROM trade_groups g
        JOIN signal_analytics_signals s
          ON s.content_fingerprint=g.signal_hash
         AND s.source_chat_id=g.source_chat_id
         AND (s.first_source_message_id=g.source_message_id
              OR s.last_source_message_id=g.source_message_id)
         AND UPPER(s.symbol)=UPPER(g.symbol)
         AND LOWER(s.side)=LOWER(g.side)
         AND UPPER(s.order_type)=UPPER(g.entry_type)
        WHERE s.trade_group_id IS NULL
          AND COALESCE(s.linkage_status,'unlinked_legacy') IN (
            'unlinked','unlinked_legacy','pending'
          )
        ORDER BY g.id DESC
        LIMIT {placeholder}
    """
    if is_postgres():
        rows = await conn.fetch(sql.format(placeholder="$1"), limit)
        return tuple(int(row["id"]) for row in rows)
    cursor = await conn.execute(sql.format(placeholder="?"), (limit,))
    return tuple(int(row[0]) for row in await cursor.fetchall())


async def _backlog_execution_ids(conn: Any, limit: int) -> tuple[int, ...]:
    """Return missing, conflicted, or source-newer execution projections.

    Package g36 stores immutable risk/TP evidence in ``trade_executions`` after
    the initial execution row can already have been projected.  The old backlog
    only repaired absent linkage rows, so a missed in-memory refresh left
    ``analytics_execution_results`` permanently stale.  This selector compares
    durable source evidence with its projection and replays only bounded,
    reconstructable drift.  It never guesses historical values.
    """

    if is_postgres():
        rows = await conn.fetch(
            """
            SELECT e.id
            FROM trade_executions e
            JOIN signal_analytics_signals s
              ON s.trade_group_id=e.trade_group_id
             AND s.linkage_status='linked_exact'
            LEFT JOIN analytics_execution_results r ON r.execution_id=e.id
            LEFT JOIN statistics_entity_links l ON l.execution_id=e.id
            WHERE r.id IS NULL
               OR COALESCE(r.linkage_status,'pending') IN ('pending','unlinked')
               OR l.id IS NULL
               OR COALESCE(l.linkage_status,'linked_exact') <> 'linked_exact'
               OR (
                    e.risk_snapshot_at IS NOT NULL
                    AND (r.risk_snapshot_at IS NULL OR e.risk_snapshot_at > r.risk_snapshot_at)
                  )
               OR (
                    LOWER(COALESCE(e.risk_snapshot_status,'missing'))='complete'
                    AND (
                      LOWER(COALESCE(r.risk_snapshot_status,'missing'))<>'complete'
                      OR r.equity_snapshot_usd IS NULL
                      OR r.planned_risk_usd IS NULL
                      OR r.expected_loss_at_stop_usd IS NULL
                      OR r.planned_entry_qty IS NULL
                      OR r.stop_distance IS NULL
                    )
                  )
               OR (
                    COALESCE(e.tp_distribution_locked,0)=1
                    AND (
                      COALESCE(r.tp_distribution_locked,0)<>1
                      OR COALESCE(r.tp_distribution_json,'')<>COALESCE(e.tp_distribution_json,'')
                      OR COALESCE(r.tp_distribution_source,'')<>COALESCE(e.tp_distribution_source,'')
                      OR COALESCE(r.tp_distribution_version,0)<>COALESCE(e.tp_distribution_version,0)
                    )
                  )
            ORDER BY e.id DESC
            LIMIT $1
            """,
            limit,
        )
        return tuple(int(row["id"]) for row in rows)

    cursor = await conn.execute(
        """
        SELECT e.id
        FROM trade_executions e
        JOIN signal_analytics_signals s
          ON s.trade_group_id=e.trade_group_id
         AND s.linkage_status='linked_exact'
        LEFT JOIN analytics_execution_results r ON r.execution_id=e.id
        LEFT JOIN statistics_entity_links l ON l.execution_id=e.id
        WHERE r.id IS NULL
           OR COALESCE(r.linkage_status,'pending') IN ('pending','unlinked')
           OR l.id IS NULL
           OR COALESCE(l.linkage_status,'linked_exact') <> 'linked_exact'
           OR (
                e.risk_snapshot_at IS NOT NULL
                AND (
                  r.risk_snapshot_at IS NULL
                  OR julianday(e.risk_snapshot_at)>julianday(r.risk_snapshot_at)
                )
              )
           OR (
                LOWER(COALESCE(e.risk_snapshot_status,'missing'))='complete'
                AND (
                  LOWER(COALESCE(r.risk_snapshot_status,'missing'))<>'complete'
                  OR r.equity_snapshot_usd IS NULL
                  OR r.planned_risk_usd IS NULL
                  OR r.expected_loss_at_stop_usd IS NULL
                  OR r.planned_entry_qty IS NULL
                  OR r.stop_distance IS NULL
                )
              )
           OR (
                COALESCE(e.tp_distribution_locked,0)=1
                AND (
                  COALESCE(r.tp_distribution_locked,0)<>1
                  OR COALESCE(r.tp_distribution_json,'')<>COALESCE(e.tp_distribution_json,'')
                  OR COALESCE(r.tp_distribution_source,'')<>COALESCE(e.tp_distribution_source,'')
                  OR COALESCE(r.tp_distribution_version,0)<>COALESCE(e.tp_distribution_version,0)
                )
              )
        ORDER BY e.id DESC
        LIMIT ?
        """,
        (limit,),
    )
    return tuple(int(row[0]) for row in await cursor.fetchall())


async def reconcile_statistics_linkage(
    *,
    group_ids: Iterable[int] = (),
    execution_ids: Iterable[int] = (),
    include_backlog: bool = False,
    backlog_limit: int = 5000,
) -> StatisticsLinkageBatchResult:
    """Reconcile exact group/signal links and one-to-one execution projections."""

    settings = get_settings()
    if not bool(settings.STATISTICS_LINKAGE_ENABLED):
        return StatisticsLinkageBatchResult()
    projection_enabled = bool(settings.STATISTICS_EXECUTION_RESULTS_ENABLED)
    requested_groups = set(_positive_ids(group_ids))
    explicit_groups = set(requested_groups)
    explicit_executions = set(_positive_ids(execution_ids))
    bounded_limit = max(1, min(int(backlog_limit or 1), 10_000))

    async with connect() as conn:
        if is_postgres():
            # Discover work without holding row locks. Each group/execution is
            # then committed independently below, which keeps lock duration
            # bounded and prevents one backlog failure from rolling back all
            # previously completed linkage work.
            if include_backlog:
                explicit_groups.update(await _backlog_group_ids(conn, bounded_limit))
            for execution_id in tuple(explicit_executions):
                group_id = await _execution_group_id(conn, execution_id)
                if group_id:
                    explicit_groups.add(group_id)

            result = StatisticsLinkageBatchResult()
            for group_id in sorted(explicit_groups):
                partial = await _run_postgres_entity_transaction(
                    conn,
                    lock_key=_PG_GROUP_LOCK_BASE,
                    entity_id=group_id,
                    operation=_link_one_group,
                )
                if partial is not None:
                    result = result.merge(partial)

            if projection_enabled:
                if include_backlog:
                    explicit_executions.update(
                        await _backlog_execution_ids(conn, bounded_limit)
                    )
                for group_id in sorted(requested_groups):
                    explicit_executions.update(
                        await _group_execution_ids(conn, group_id)
                    )
                for execution_id in sorted(explicit_executions):
                    partial = await _run_postgres_entity_transaction(
                        conn,
                        lock_key=_PG_EXECUTION_LOCK_BASE,
                        entity_id=execution_id,
                        operation=_project_one_execution,
                    )
                    if partial is not None:
                        result = result.merge(partial)
            return result

        await conn.execute("BEGIN IMMEDIATE")
        try:
            if include_backlog:
                explicit_groups.update(await _backlog_group_ids(conn, bounded_limit))
            for execution_id in tuple(explicit_executions):
                group_id = await _execution_group_id(conn, execution_id)
                if group_id:
                    explicit_groups.add(group_id)
            result = StatisticsLinkageBatchResult()
            for group_id in sorted(explicit_groups):
                result = result.merge(await _link_one_group(conn, group_id))
            if projection_enabled:
                if include_backlog:
                    explicit_executions.update(
                        await _backlog_execution_ids(conn, bounded_limit)
                    )
                for group_id in sorted(requested_groups):
                    explicit_executions.update(
                        await _group_execution_ids(conn, group_id)
                    )
                for execution_id in sorted(explicit_executions):
                    result = result.merge(
                        await _project_one_execution(conn, execution_id)
                    )
            await conn.commit()
            return result
        except BaseException:
            await conn.rollback()
            raise
