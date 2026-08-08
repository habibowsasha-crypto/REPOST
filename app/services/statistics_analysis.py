"""Read-only dataset loader and analysis facade for statistics plan steps 6-7.

The facade is deliberately not wired into Telegram commands in these steps.
Step 8 can call it behind STATS_V2_REPORTS_ENABLED.  Loading performs bounded
SELECT queries only and never touches exchange adapters or trading workers.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

from app.database.db import connect, is_postgres, monitor_db_workload
from app.services.statistics_metrics import StatisticsMetricsReport, calculate_statistics_metrics
from app.services.statistics_quality import StatisticsQualityReport, calculate_statistics_quality


DEFAULT_ANALYSIS_ROW_LIMIT = 100_000
MAX_ANALYSIS_ROW_LIMIT = 250_000
MAX_ANALYSIS_EVENT_ROWS = 1_000_000
MAX_ANALYSIS_FILL_ROWS = 1_000_000

_SIGNAL_COLUMNS = """
id,period_id,published_at,symbol,side,order_type,timeframe,strategy,
entry_reference,activated_price,stop_price,targets_json,target_percents_json,
target_percents_source,status,duplicate_count,trade_group_id,linkage_status,needs_recovery,activated_at,
max_tp_index,be_trigger_tp_index,be_armed_at,completed_at,terminal_reason,
ambiguous_reason,recovery_status,recovery_method,recovery_completed_at,
recovery_confidence,data_quality_status,data_quality_reason,legacy_data,
quality_reasons_json,final_eligible,simulation_eligible,risk_analysis_eligible,
quality_gate_version,quality_evaluated_at,created_at,updated_at
""".replace("\n", "")

_EVENT_COLUMNS = "id,signal_id,event_type,level_index,observed_at,observed_price"

_EXECUTION_COLUMNS = """
id,execution_id,analytics_signal_id,trade_group_id,period_id,user_id,exchange,
symbol,side,entry_order_type,linkage_status,first_entry_fill_at,last_entry_fill_at,
actual_entry_qty,actual_entry_avg_price,planned_entry_reference,
execution_reference_price,initial_stop_price,equity_snapshot_usd,
planned_risk_percent,planned_risk_usd,initial_price_risk_usd,
initial_risk_percent_of_equity,estimated_fee_risk_usd,expected_loss_at_stop_usd,
planned_entry_qty,stop_distance,risk_snapshot_at,risk_snapshot_source,risk_snapshot_status,
risk_snapshot_reason,tp_distribution_json,tp_distribution_source,tp_distribution_locked,
tp_distribution_version,first_exit_fill_at,last_exit_fill_at,
actual_exit_qty,actual_exit_avg_price,signal_max_tp_index,execution_max_tp_index,
canonical_terminal_reason,terminal_detail,strategy_gross_pnl,exchange_gross_pnl,
gross_pnl_source,trading_fee_signed,trading_fee_cost,funding_signed,settlement_asset,
net_pnl,provisional_net_pnl,result_r,provisional_result_r,entry_slippage_bps,
limit_price_slippage_bps,execution_duration_seconds,trading_reconciliation_state,
financial_state,funding_state,volume_parity_status,completeness_mask,
completeness_percent,data_quality_status,ambiguity_reason,projection_status,
projection_attempts,projection_next_attempt_at,projection_deadline_at,
projection_processing_started_at,projection_last_error,funding_query_start_at,
funding_query_end_at,funding_event_count,funding_recovery_attempts,
funding_zero_observations,funding_first_empty_at,funding_last_checked_at,
funding_recovery_status,funding_recovery_reason,funding_finalized_at,
quality_reasons_json,final_eligible,simulation_eligible,risk_analysis_eligible,
quality_gate_version,quality_evaluated_at,market_event_review_status,
market_event_exclusion_reason,market_event_reviewed_at,legacy_data,result_version,
created_at,updated_at,finalized_at
""".replace("\n", "")
_FILL_COLUMNS = """
id,job_id,execution_id,user_id,exchange,trade_id,order_id,order_key,role,tp_index,
symbol,side,price,qty,realized_pnl,fee,fee_asset,fill_time,fingerprint,metadata_json,
position_side,liquidity_role,source_endpoint,ingested_at,created_at
""".replace("\n", "")



@dataclass(frozen=True, slots=True)
class StatisticsAnalysisDataset:
    period_id: int | None
    signal_rows: tuple[dict[str, Any], ...]
    event_rows: tuple[dict[str, Any], ...]
    execution_rows: tuple[dict[str, Any], ...]
    total_signals: int
    total_executions: int
    signal_rows_truncated: bool
    event_rows_truncated: bool
    execution_rows_truncated: bool
    fill_rows: tuple[dict[str, Any], ...] = ()
    total_fills: int = 0
    fill_rows_truncated: bool = False


def _as_dict(row: Any) -> dict[str, Any]:
    try:
        return dict(row)
    except Exception:
        return {}


def _bounded_limit(value: int) -> int:
    return max(1, min(MAX_ANALYSIS_ROW_LIMIT, int(value)))


async def _count(conn: Any, table: str, period_id: int | None) -> int:
    where = "" if period_id is None else " WHERE period_id=$1" if is_postgres() else " WHERE period_id=?"
    query = f"SELECT COUNT(*) FROM {table}{where}"
    if is_postgres():
        value = await conn.fetchval(query, *(() if period_id is None else (period_id,)))
        return int(value or 0)
    cursor = await conn.execute(query, () if period_id is None else (period_id,))
    row = await cursor.fetchone()
    return int(row[0] if row else 0)


async def _fetch_rows(
    conn: Any,
    *,
    table: str,
    columns: str,
    order_by: str,
    period_id: int | None,
    limit: int,
) -> list[dict[str, Any]]:
    where = "" if period_id is None else " WHERE period_id=$1" if is_postgres() else " WHERE period_id=?"
    if is_postgres():
        limit_placeholder = "$1" if period_id is None else "$2"
        args = (limit,) if period_id is None else (period_id, limit)
        rows = await conn.fetch(
            f"SELECT {columns} FROM {table}{where} ORDER BY {order_by} LIMIT {limit_placeholder}",
            *args,
        )
        return [_as_dict(row) for row in rows]
    args = (limit,) if period_id is None else (period_id, limit)
    cursor = await conn.execute(
        f"SELECT {columns} FROM {table}{where} ORDER BY {order_by} LIMIT ?",
        args,
    )
    return [_as_dict(row) for row in await cursor.fetchall()]



def _runtime_marker_fields(raw_payload: Any) -> dict[str, Any]:
    try:
        payload = json.loads(raw_payload or "{}") if isinstance(raw_payload, str) else dict(raw_payload or {})
    except (TypeError, ValueError, json.JSONDecodeError):
        payload = {}
    marker = payload.get("financial_reconciliation_enqueue_v1")
    marker = dict(marker) if isinstance(marker, dict) else {}
    audit = payload.get("g54_terminal_orphan_backfill_v1")
    audit = dict(audit) if isinstance(audit, dict) else {}
    g55 = payload.get("g55_exact_manual_history_bridge_v1")
    g55 = dict(g55) if isinstance(g55, dict) else {}
    prior_live = payload.get("g55_prior_live_position_evidence_v1")
    prior_live = dict(prior_live) if isinstance(prior_live, dict) else {}
    return {
        "financial_marker_state": str(marker.get("state") or "missing"),
        "financial_marker_blockers_json": json.dumps(
            list(marker.get("blockers") or []), ensure_ascii=False, sort_keys=True
        ),
        "financial_marker_recovery_source": str(marker.get("recovery_source") or ""),
        "g54_backfill_result": str(audit.get("result") or ""),
        "g54_backfill_reason": str(audit.get("reason") or ""),
        "g55_history_bridge_result": str(g55.get("result") or ""),
        "g55_history_bridge_reason": str(g55.get("reason") or ""),
        "g55_history_bridge_checked_at": str(g55.get("checked_at") or ""),
        "g55_prior_live_position_confirmed": int(prior_live.get("confirmed") is True),
        "g55_prior_live_position_qty": str(prior_live.get("qty") or ""),
    }


async def _fetch_execution_rows(
    conn: Any, *, period_id: int | None, limit: int, user_id: int | None = None
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[int] = []
    if period_id is not None:
        params.append(int(period_id))
        clauses.append(f"r.period_id=${len(params)}" if is_postgres() else "r.period_id=?")
    if user_id is not None:
        params.append(int(user_id))
        clauses.append(f"r.user_id=${len(params)}" if is_postgres() else "r.user_id=?")
        clauses.append("COALESCE(e.status,'') <> 'superseded_duplicate'")
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    result_columns = ",".join(f"r.{column.strip()}" for column in _EXECUTION_COLUMNS.split(","))
    runtime_columns = """
      e.status AS runtime_execution_status,
      e.reason AS runtime_execution_reason,
      e.close_type AS runtime_close_type,
      e.closed_at AS runtime_closed_at,
      e.realized_pnl AS runtime_realized_pnl,
      e.qty AS runtime_qty,
      e.updated_at AS runtime_updated_at,
      j.id AS financial_job_id,
      j.status AS financial_job_status,
      j.attempts AS financial_job_attempts,
      j.last_error AS financial_job_last_error,
      j.expected_order_count AS financial_expected_order_count,
      j.confirmed_order_count AS financial_confirmed_order_count,
      j.fill_count AS financial_job_fill_count,
      j.terminal_at AS financial_job_terminal_at,
      j.deadline_at AS financial_job_deadline_at,
      e.exchange_order_ids_json AS _runtime_execution_payload
    """
    if is_postgres():
        limit_placeholder = f"${len(params) + 1}"
        args = (*params, int(limit))
        rows = await conn.fetch(
            f"SELECT {result_columns},{runtime_columns} "
            "FROM analytics_execution_results r "
            "LEFT JOIN trade_executions e ON e.id=r.execution_id "
            "LEFT JOIN financial_reconciliation_jobs j ON j.execution_id=r.execution_id "
            f"{where} ORDER BY COALESCE(r.last_exit_fill_at,r.first_entry_fill_at,r.created_at),r.execution_id "
            f"LIMIT {limit_placeholder}",
            *args,
        )
        result = [_as_dict(row) for row in rows]
    else:
        args = (*params, int(limit))
        cursor = await conn.execute(
            f"SELECT {result_columns},{runtime_columns} "
            "FROM analytics_execution_results r "
            "LEFT JOIN trade_executions e ON e.id=r.execution_id "
            "LEFT JOIN financial_reconciliation_jobs j ON j.execution_id=r.execution_id "
            f"{where} ORDER BY COALESCE(r.last_exit_fill_at,r.first_entry_fill_at,r.created_at),r.execution_id LIMIT ?",
            args,
        )
        result = [_as_dict(row) for row in await cursor.fetchall()]
    for row in result:
        row.update(_runtime_marker_fields(row.pop("_runtime_execution_payload", None)))
    return result


async def _fetch_events(
    conn: Any,
    signal_ids: list[int],
    *,
    limit: int,
) -> tuple[list[dict[str, Any]], bool]:
    if not signal_ids:
        return [], False
    bounded = max(1, min(MAX_ANALYSIS_EVENT_ROWS, int(limit)))
    fetch_limit = bounded + 1
    if is_postgres():
        rows = await conn.fetch(
            f"SELECT {_EVENT_COLUMNS} FROM signal_analytics_level_events "
            "WHERE signal_id = ANY($1::bigint[]) "
            "ORDER BY signal_id,observed_at,id LIMIT $2",
            signal_ids,
            fetch_limit,
        )
        result = [_as_dict(row) for row in rows]
        return result[:bounded], len(result) > bounded

    result: list[dict[str, Any]] = []
    for offset in range(0, len(signal_ids), 500):
        remaining = fetch_limit - len(result)
        if remaining <= 0:
            break
        chunk = signal_ids[offset : offset + 500]
        placeholders = ",".join("?" for _ in chunk)
        cursor = await conn.execute(
            f"SELECT {_EVENT_COLUMNS} FROM signal_analytics_level_events "
            f"WHERE signal_id IN ({placeholders}) "
            "ORDER BY signal_id,observed_at,id LIMIT ?",
            (*chunk, remaining),
        )
        result.extend(_as_dict(row) for row in await cursor.fetchall())
    result.sort(
        key=lambda row: (
            int(row.get("signal_id") or 0),
            str(row.get("observed_at") or ""),
            int(row.get("id") or 0),
        )
    )
    return result[:bounded], len(result) > bounded


async def _count_fills(
    conn: Any, period_id: int | None, user_id: int | None = None
) -> int:
    clauses: list[str] = []
    params: list[int] = []
    if period_id is not None:
        params.append(int(period_id))
        clauses.append(f"r.period_id=${len(params)}" if is_postgres() else "r.period_id=?")
    if user_id is not None:
        params.append(int(user_id))
        clauses.append(f"r.user_id=${len(params)}" if is_postgres() else "r.user_id=?")
        clauses.append("COALESCE(e.status,'') <> 'superseded_duplicate'")
    if not clauses:
        query = "SELECT COUNT(*) FROM financial_reconciliation_fills"
        if is_postgres():
            return int(await conn.fetchval(query) or 0)
        cursor = await conn.execute(query)
        row = await cursor.fetchone()
        return int(row[0] if row else 0)
    query = (
        "SELECT COUNT(*) FROM financial_reconciliation_fills f "
        "JOIN analytics_execution_results r ON r.execution_id=f.execution_id "
        "LEFT JOIN trade_executions e ON e.id=r.execution_id "
        "WHERE " + " AND ".join(clauses)
    )
    if is_postgres():
        return int(await conn.fetchval(query, *params) or 0)
    cursor = await conn.execute(query, tuple(params))
    row = await cursor.fetchone()
    return int(row[0] if row else 0)


async def _fetch_fills(
    conn: Any,
    *,
    period_id: int | None,
    limit: int,
    user_id: int | None = None,
) -> list[dict[str, Any]]:
    bounded = max(1, min(MAX_ANALYSIS_FILL_ROWS, int(limit)))
    clauses: list[str] = []
    params: list[int] = []
    if period_id is not None:
        params.append(int(period_id))
        clauses.append(f"r.period_id=${len(params)}" if is_postgres() else "r.period_id=?")
    if user_id is not None:
        params.append(int(user_id))
        clauses.append(f"r.user_id=${len(params)}" if is_postgres() else "r.user_id=?")
        clauses.append("COALESCE(e.status,'') <> 'superseded_duplicate'")
    prefixed = ",".join("f." + column.strip() for column in _FILL_COLUMNS.split(","))
    if clauses:
        query = (
            f"SELECT {prefixed} FROM financial_reconciliation_fills f "
            "JOIN analytics_execution_results r ON r.execution_id=f.execution_id "
            "LEFT JOIN trade_executions e ON e.id=r.execution_id "
            "WHERE " + " AND ".join(clauses) + " ORDER BY f.fill_time,f.id LIMIT "
        )
    else:
        query = f"SELECT {_FILL_COLUMNS} FROM financial_reconciliation_fills ORDER BY fill_time,id LIMIT "
    if is_postgres():
        rows = await conn.fetch(query + f"${len(params) + 1}", *params, bounded)
        return [_as_dict(row) for row in rows]
    cursor = await conn.execute(query + "?", (*params, bounded))
    return [_as_dict(row) for row in await cursor.fetchall()]


async def _count_account_signals(
    conn: Any, *, period_id: int | None, user_id: int
) -> int:
    clauses: list[str] = []
    params: list[int] = []
    if period_id is not None:
        params.append(int(period_id))
        clauses.append(f"s.period_id=${len(params)}" if is_postgres() else "s.period_id=?")
    params.append(int(user_id))
    user_placeholder = f"${len(params)}" if is_postgres() else "?"
    clauses.append(
        "EXISTS (SELECT 1 FROM analytics_execution_results r "
        "JOIN trade_executions e ON e.id=r.execution_id "
        f"WHERE r.analytics_signal_id=s.id AND r.user_id={user_placeholder} "
        "AND r.period_id=s.period_id AND COALESCE(e.status,'') <> 'superseded_duplicate')"
    )
    query = "SELECT COUNT(*) FROM signal_analytics_signals s WHERE " + " AND ".join(clauses)
    if is_postgres():
        return int(await conn.fetchval(query, *params) or 0)
    cursor = await conn.execute(query, tuple(params))
    row = await cursor.fetchone()
    return int(row[0] if row else 0)


async def _fetch_account_signal_rows(
    conn: Any, *, period_id: int | None, user_id: int, limit: int
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[int] = []
    if period_id is not None:
        params.append(int(period_id))
        clauses.append(f"s.period_id=${len(params)}" if is_postgres() else "s.period_id=?")
    params.append(int(user_id))
    user_placeholder = f"${len(params)}" if is_postgres() else "?"
    clauses.append(
        "EXISTS (SELECT 1 FROM analytics_execution_results r "
        "JOIN trade_executions e ON e.id=r.execution_id "
        f"WHERE r.analytics_signal_id=s.id AND r.user_id={user_placeholder} "
        "AND r.period_id=s.period_id AND COALESCE(e.status,'') <> 'superseded_duplicate')"
    )
    query = (
        f"SELECT {_SIGNAL_COLUMNS} FROM signal_analytics_signals s WHERE "
        + " AND ".join(clauses)
        + " ORDER BY s.published_at,s.id LIMIT "
    )
    if is_postgres():
        rows = await conn.fetch(query + f"${len(params) + 1}", *params, int(limit))
        return [_as_dict(row) for row in rows]
    cursor = await conn.execute(query + "?", (*params, int(limit)))
    return [_as_dict(row) for row in await cursor.fetchall()]


async def load_statistics_analysis_dataset(
    *,
    period_id: int | None = None,
    row_limit: int = DEFAULT_ANALYSIS_ROW_LIMIT,
    user_id: int | None = None,
) -> StatisticsAnalysisDataset:
    bounded = _bounded_limit(row_limit)
    normalized_period = int(period_id) if period_id is not None else None
    normalized_user = int(user_id) if user_id is not None else None
    if normalized_period is not None and normalized_period <= 0:
        raise ValueError("period_id must be positive")
    if normalized_user is not None and normalized_user <= 0:
        raise ValueError("user_id must be positive")
    async with monitor_db_workload(stage="statistics_analysis"):
        async with connect() as conn:
            if normalized_user is None:
                total_signals = await _count(conn, "signal_analytics_signals", normalized_period)
                signal_rows = await _fetch_rows(
                    conn,
                    table="signal_analytics_signals",
                    columns=_SIGNAL_COLUMNS,
                    order_by="published_at,id",
                    period_id=normalized_period,
                    limit=bounded,
                )
            else:
                total_signals = await _count_account_signals(
                    conn, period_id=normalized_period, user_id=normalized_user
                )
                signal_rows = await _fetch_account_signal_rows(
                    conn, period_id=normalized_period, user_id=normalized_user, limit=bounded
                )
            if normalized_user is None:
                total_executions = await _count(conn, "analytics_execution_results", normalized_period)
            else:
                if is_postgres():
                    if normalized_period is None:
                        total_executions = int(await conn.fetchval(
                            "SELECT COUNT(*) FROM analytics_execution_results r JOIN trade_executions e ON e.id=r.execution_id WHERE r.user_id=$1 AND COALESCE(e.status,'') <> 'superseded_duplicate'",
                            normalized_user,
                        ) or 0)
                    else:
                        total_executions = int(await conn.fetchval(
                            "SELECT COUNT(*) FROM analytics_execution_results r JOIN trade_executions e ON e.id=r.execution_id WHERE r.period_id=$1 AND r.user_id=$2 AND COALESCE(e.status,'') <> 'superseded_duplicate'",
                            normalized_period, normalized_user,
                        ) or 0)
                else:
                    if normalized_period is None:
                        cur = await conn.execute(
                            "SELECT COUNT(*) FROM analytics_execution_results r JOIN trade_executions e ON e.id=r.execution_id WHERE r.user_id=? AND COALESCE(e.status,'') <> 'superseded_duplicate'",
                            (normalized_user,),
                        )
                    else:
                        cur = await conn.execute(
                            "SELECT COUNT(*) FROM analytics_execution_results r JOIN trade_executions e ON e.id=r.execution_id WHERE r.period_id=? AND r.user_id=? AND COALESCE(e.status,'') <> 'superseded_duplicate'",
                            (normalized_period, normalized_user),
                        )
                    row = await cur.fetchone()
                    total_executions = int(row[0] if row else 0)
            total_fills = await _count_fills(conn, normalized_period, normalized_user)
            execution_rows = await _fetch_execution_rows(
                conn,
                period_id=normalized_period,
                limit=bounded,
                user_id=normalized_user,
            )
            fill_limit = min(MAX_ANALYSIS_FILL_ROWS, bounded * 16)
            fill_rows = await _fetch_fills(
                conn, period_id=normalized_period, limit=fill_limit, user_id=normalized_user
            )
            signal_ids = sorted(
                {
                    int(row.get("id") or 0)
                    for row in signal_rows
                    if int(row.get("id") or 0) > 0
                }
            )
            event_limit = min(MAX_ANALYSIS_EVENT_ROWS, bounded * 8)
            event_rows, event_rows_truncated = await _fetch_events(
                conn,
                signal_ids,
                limit=event_limit,
            )
    return StatisticsAnalysisDataset(
        period_id=normalized_period,
        signal_rows=tuple(signal_rows),
        event_rows=tuple(event_rows),
        execution_rows=tuple(execution_rows),
        total_signals=total_signals,
        total_executions=total_executions,
        signal_rows_truncated=total_signals > len(signal_rows),
        event_rows_truncated=event_rows_truncated,
        execution_rows_truncated=total_executions > len(execution_rows),
        fill_rows=tuple(fill_rows),
        total_fills=total_fills,
        fill_rows_truncated=total_fills > len(fill_rows),
    )


def calculate_dataset_metrics(
    dataset: StatisticsAnalysisDataset,
    *,
    minimum_symbol_sample: int = 20,
    include_user_breakdown: bool = False,
    allow_truncated: bool = False,
) -> StatisticsMetricsReport:
    if not allow_truncated and (
        dataset.signal_rows_truncated
        or dataset.event_rows_truncated
        or dataset.execution_rows_truncated
    ):
        raise ValueError("statistics analysis dataset is truncated")
    return calculate_statistics_metrics(
        dataset.signal_rows,
        dataset.event_rows,
        dataset.execution_rows,
        minimum_symbol_sample=minimum_symbol_sample,
        include_user_breakdown=include_user_breakdown,
    )


def calculate_dataset_quality(
    dataset: StatisticsAnalysisDataset,
    *,
    allow_truncated: bool = False,
) -> StatisticsQualityReport:
    if not allow_truncated and (
        dataset.signal_rows_truncated
        or dataset.event_rows_truncated
        or dataset.execution_rows_truncated
        or dataset.fill_rows_truncated
    ):
        raise ValueError("statistics quality dataset is truncated")
    return calculate_statistics_quality(
        dataset.signal_rows,
        dataset.event_rows,
        dataset.execution_rows,
        dataset.fill_rows,
    )
