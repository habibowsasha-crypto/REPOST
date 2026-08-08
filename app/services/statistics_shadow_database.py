"""Read-only database diagnostics for statistics shadow rollout step 10.

The probe executes only bounded COUNT/metadata queries after the normal schema
initialization.  It never creates periods, never changes status, never calls
BingX and never touches trading orders.  Startup execution is opt-in through
``STATISTICS_SHADOW_DB_DIAGNOSTICS_ENABLED``.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from app import __version__
from app.database.db import connect, is_postgres, monitor_db_workload


_REQUIRED_TABLES = (
    "statistics_periods",
    "signal_analytics_signals",
    "signal_analytics_level_events",
    "trade_groups",
    "trade_executions",
    "financial_reconciliation_jobs",
    "financial_reconciliation_fills",
    "analytics_execution_results",
    "financial_funding_events",
    "statistics_entity_links",
    "statistics_quality_audit",
    "statistics_reset_requests",
)


@dataclass(frozen=True, slots=True)
class ShadowDatabaseItem:
    severity: str
    code: str
    message: str


@dataclass(frozen=True, slots=True)
class ShadowDatabaseReport:
    app_version: str
    generated_at_utc: str
    backend: str
    expected_stage: str
    ready: bool
    required_table_count: int
    missing_tables: tuple[str, ...]
    active_period_count: int
    active_period_id: int | None
    active_period_name: str | None
    active_period_kind: str | None
    active_period_source_version: str | None
    counters: Mapping[str, int]
    items: tuple[ShadowDatabaseItem, ...]

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["counters"] = dict(self.counters)
        payload["items"] = [asdict(item) for item in self.items]
        return payload


async def _fetchval(conn: Any, pg_sql: str, sqlite_sql: str, *args: Any) -> Any:
    if is_postgres():
        return await conn.fetchval(pg_sql, *args)
    cursor = await conn.execute(sqlite_sql, tuple(args))
    row = await cursor.fetchone()
    return row[0] if row else None


async def _fetchrow(conn: Any, pg_sql: str, sqlite_sql: str, *args: Any) -> dict[str, Any]:
    if is_postgres():
        row = await conn.fetchrow(pg_sql, *args)
    else:
        cursor = await conn.execute(sqlite_sql, tuple(args))
        row = await cursor.fetchone()
    try:
        return dict(row) if row else {}
    except Exception:
        return {}


async def _table_exists(conn: Any, table: str) -> bool:
    if is_postgres():
        value = await conn.fetchval("SELECT to_regclass($1)", f"public.{table}")
        return value is not None
    cursor = await conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    )
    return (await cursor.fetchone()) is not None


async def build_statistics_shadow_database_report(
    *,
    expected_stage: str,
    stale_processing_seconds: int = 120,
) -> ShadowDatabaseReport:
    """Return a bounded secret-free snapshot of shadow database health."""

    backend = "postgres" if is_postgres() else "sqlite"
    items: list[ShadowDatabaseItem] = []

    def add(severity: str, code: str, message: str) -> None:
        items.append(ShadowDatabaseItem(severity, code, message))

    counters: dict[str, int] = {}
    missing: list[str] = []
    active_row: dict[str, Any] = {}
    active_count = 0

    async with monitor_db_workload(stage="statistics_shadow_diagnostics"):
        async with connect() as conn:
            for table in _REQUIRED_TABLES:
                if not await _table_exists(conn, table):
                    missing.append(table)

            if missing:
                add(
                    "CRITICAL",
                    "shadow_schema_tables_missing",
                    "Не найдены обязательные таблицы: " + ", ".join(missing),
                )
            else:
                active_count = int(
                    await _fetchval(
                        conn,
                        "SELECT COUNT(*) FROM statistics_periods WHERE status='active'",
                        "SELECT COUNT(*) FROM statistics_periods WHERE status='active'",
                    )
                    or 0
                )
                active_row = await _fetchrow(
                    conn,
                    "SELECT id,name,period_kind,source_version FROM statistics_periods "
                    "WHERE status='active' ORDER BY id DESC LIMIT 1",
                    "SELECT id,name,period_kind,source_version FROM statistics_periods "
                    "WHERE status='active' ORDER BY id DESC LIMIT 1",
                )

                count_queries = {
                    "signals_total": (
                        "SELECT COUNT(*) FROM signal_analytics_signals",
                        "SELECT COUNT(*) FROM signal_analytics_signals",
                    ),
                    "signals_completed": (
                        "SELECT COUNT(*) FROM signal_analytics_signals WHERE completed_at IS NOT NULL",
                        "SELECT COUNT(*) FROM signal_analytics_signals WHERE completed_at IS NOT NULL",
                    ),
                    "signals_without_period": (
                        "SELECT COUNT(*) FROM signal_analytics_signals WHERE period_id IS NULL",
                        "SELECT COUNT(*) FROM signal_analytics_signals WHERE period_id IS NULL",
                    ),
                    "signals_recovery_pending": (
                        "SELECT COUNT(*) FROM signal_analytics_signals "
                        "WHERE recovery_status IN ('pending','processing','retry')",
                        "SELECT COUNT(*) FROM signal_analytics_signals "
                        "WHERE recovery_status IN ('pending','processing','retry')",
                    ),
                    "signals_recovery_unresolved": (
                        "SELECT COUNT(*) FROM signal_analytics_signals "
                        "WHERE needs_recovery=1 AND COALESCE(recovery_status,'') NOT IN ('pending','processing','retry')",
                        "SELECT COUNT(*) FROM signal_analytics_signals "
                        "WHERE needs_recovery=1 AND COALESCE(recovery_status,'') NOT IN ('pending','processing','retry')",
                    ),
                    "signals_recovery_legacy_forward_resumed": (
                        "SELECT COUNT(*) FROM signal_analytics_signals WHERE recovery_status='forward_resumed'",
                        "SELECT COUNT(*) FROM signal_analytics_signals WHERE recovery_status='forward_resumed'",
                    ),
                    "signals_recovered_exact": (
                        "SELECT COUNT(*) FROM signal_analytics_signals WHERE recovery_status='recovered_exact'",
                        "SELECT COUNT(*) FROM signal_analytics_signals WHERE recovery_status='recovered_exact'",
                    ),
                    "signals_recovery_unavailable": (
                        "SELECT COUNT(*) FROM signal_analytics_signals WHERE recovery_status='unavailable'",
                        "SELECT COUNT(*) FROM signal_analytics_signals WHERE recovery_status='unavailable'",
                    ),
                    "executions_total": (
                        "SELECT COUNT(*) FROM analytics_execution_results",
                        "SELECT COUNT(*) FROM analytics_execution_results",
                    ),
                    "executions_final": (
                        "SELECT COUNT(*) FROM analytics_execution_results WHERE UPPER(financial_state)='FINAL'",
                        "SELECT COUNT(*) FROM analytics_execution_results WHERE UPPER(financial_state)='FINAL'",
                    ),
                    "executions_provisional": (
                        "SELECT COUNT(*) FROM analytics_execution_results WHERE UPPER(financial_state)='PROVISIONAL'",
                        "SELECT COUNT(*) FROM analytics_execution_results WHERE UPPER(financial_state)='PROVISIONAL'",
                    ),
                    "executions_ambiguous": (
                        "SELECT COUNT(*) FROM analytics_execution_results WHERE UPPER(financial_state)='AMBIGUOUS'",
                        "SELECT COUNT(*) FROM analytics_execution_results WHERE UPPER(financial_state)='AMBIGUOUS'",
                    ),
                    "executions_linked_exact": (
                        "SELECT COUNT(*) FROM analytics_execution_results WHERE linkage_status='linked_exact'",
                        "SELECT COUNT(*) FROM analytics_execution_results WHERE linkage_status='linked_exact'",
                    ),
                    "executions_without_period": (
                        "SELECT COUNT(*) FROM analytics_execution_results WHERE period_id IS NULL",
                        "SELECT COUNT(*) FROM analytics_execution_results WHERE period_id IS NULL",
                    ),
                    "funding_events": (
                        "SELECT COUNT(*) FROM financial_funding_events",
                        "SELECT COUNT(*) FROM financial_funding_events",
                    ),
                    "entity_links_exact": (
                        "SELECT COUNT(*) FROM statistics_entity_links WHERE linkage_status='linked_exact'",
                        "SELECT COUNT(*) FROM statistics_entity_links WHERE linkage_status='linked_exact'",
                    ),
                    "entity_links_conflict": (
                        "SELECT COUNT(*) FROM statistics_entity_links WHERE linkage_status='conflict'",
                        "SELECT COUNT(*) FROM statistics_entity_links WHERE linkage_status='conflict'",
                    ),
                    "executions_final_eligible": (
                        "SELECT COUNT(*) FROM analytics_execution_results WHERE final_eligible=1",
                        "SELECT COUNT(*) FROM analytics_execution_results WHERE final_eligible=1",
                    ),
                    "executions_simulation_eligible": (
                        "SELECT COUNT(*) FROM analytics_execution_results WHERE simulation_eligible=1",
                        "SELECT COUNT(*) FROM analytics_execution_results WHERE simulation_eligible=1",
                    ),
                    "executions_risk_analysis_eligible": (
                        "SELECT COUNT(*) FROM analytics_execution_results WHERE risk_analysis_eligible=1",
                        "SELECT COUNT(*) FROM analytics_execution_results WHERE risk_analysis_eligible=1",
                    ),
                    "funding_manual_review": (
                        "SELECT COUNT(*) FROM analytics_execution_results WHERE funding_state='manual_review' OR funding_recovery_status='manual_review'",
                        "SELECT COUNT(*) FROM analytics_execution_results WHERE funding_state='manual_review' OR funding_recovery_status='manual_review'",
                    ),
                    "quality_detected": (
                        "SELECT COUNT(*) FROM statistics_quality_audit WHERE action='detected'",
                        "SELECT COUNT(*) FROM statistics_quality_audit WHERE action='detected'",
                    ),
                    "reset_requests_applied": (
                        "SELECT COUNT(*) FROM statistics_reset_requests WHERE status='applied'",
                        "SELECT COUNT(*) FROM statistics_reset_requests WHERE status='applied'",
                    ),
                }
                for key, (pg_sql, sqlite_sql) in count_queries.items():
                    counters[key] = int(
                        await _fetchval(conn, pg_sql, sqlite_sql) or 0
                    )

                stale_sec = max(30, int(stale_processing_seconds))
                if is_postgres():
                    stale = await conn.fetchval(
                        "SELECT COUNT(*) FROM analytics_execution_results "
                        "WHERE projection_status='processing' "
                        "AND projection_processing_started_at < NOW() - ($1 * INTERVAL '1 second')",
                        stale_sec,
                    )
                else:
                    cursor = await conn.execute(
                        "SELECT COUNT(*) FROM analytics_execution_results "
                        "WHERE projection_status='processing' "
                        "AND projection_processing_started_at IS NOT NULL "
                        "AND datetime(projection_processing_started_at) < datetime('now', ?)",
                        (f"-{stale_sec} seconds",),
                    )
                    row = await cursor.fetchone()
                    stale = row[0] if row else 0
                counters["stale_projection_leases"] = int(stale or 0)

    if not missing:
        if active_count != 1:
            add(
                "CRITICAL",
                "shadow_active_period_count_invalid",
                f"Ожидался ровно один active statistics period, найдено: {active_count}.",
            )
        if counters.get("stale_projection_leases", 0) > 0:
            add(
                "WARNING",
                "shadow_stale_projection_leases",
                "Есть зависшие projection lease; перед финансовой сверкой нужен restart/reclaim контроль.",
            )
        if counters.get("signals_without_period", 0) > 0:
            add(
                "WARNING",
                "shadow_signals_without_period",
                "Есть analytics signals без period_id; их нельзя смешивать с чистым production period.",
            )
        if counters.get("executions_without_period", 0) > 0:
            add(
                "WARNING",
                "shadow_executions_without_period",
                "Есть execution projections без period_id; financial denominator должен исключать их.",
            )
        if counters.get("executions_ambiguous", 0) > 0:
            add(
                "WARNING",
                "shadow_ambiguous_executions_present",
                "Найдены AMBIGUOUS executions; они должны оставаться вне FINAL метрик.",
            )

        if counters.get("signals_recovery_pending", 0) > 0:
            add(
                "WARNING",
                "shadow_signal_recovery_pending",
                "Есть restart-gap сигналы в recovery; они остаются вне итоговых метрик до доказанного восстановления.",
            )
        if counters.get("signals_recovery_unavailable", 0) > 0:
            add(
                "WARNING",
                "shadow_signal_recovery_unavailable",
                "Есть сигналы с недоступной для доказательства историей; они остаются fail-closed.",
            )
        if counters.get("signals_recovery_unresolved", 0) > 0:
            add(
                "WARNING",
                "shadow_signal_recovery_unresolved",
                "Есть старые или неоднозначные restart-gap строки вне active recovery queue.",
            )
        if counters.get("signals_recovery_legacy_forward_resumed", 0) > 0:
            add(
                "WARNING",
                "shadow_signal_recovery_legacy_forward_resumed",
                "Обнаружены строки старого forward-only режима; они намеренно не загружаются в live tracker.",
            )

        active_kind = str(active_row.get("period_kind") or "")
        if str(expected_stage) == "reset_test" and active_kind not in {"test", "shadow"}:
            add(
                "CRITICAL",
                "reset_test_requires_nonproduction_period",
                "reset_test допускается только при active period_kind=test или shadow.",
            )

    if not any(item.severity == "CRITICAL" for item in items):
        add(
            "INFO",
            "shadow_database_ready",
            "Additive statistics schema и active period доступны для контролируемой проверки.",
        )

    return ShadowDatabaseReport(
        app_version=__version__,
        generated_at_utc=datetime.now(timezone.utc).isoformat(),
        backend=backend,
        expected_stage=str(expected_stage),
        ready=not any(item.severity == "CRITICAL" for item in items),
        required_table_count=len(_REQUIRED_TABLES),
        missing_tables=tuple(missing),
        active_period_count=active_count,
        active_period_id=(int(active_row["id"]) if active_row.get("id") is not None else None),
        active_period_name=(str(active_row.get("name")) if active_row.get("name") is not None else None),
        active_period_kind=(str(active_row.get("period_kind")) if active_row.get("period_kind") is not None else None),
        active_period_source_version=(str(active_row.get("source_version")) if active_row.get("source_version") is not None else None),
        counters=counters,
        items=tuple(items),
    )


def format_statistics_shadow_database_report(report: Mapping[str, Any]) -> str:
    counters = report.get("counters") or {}
    head = (
        "STATISTICS_SHADOW_DB_SUMMARY "
        f"version={report.get('app_version')} "
        f"stage={report.get('expected_stage')} "
        f"backend={report.get('backend')} "
        f"ready={report.get('ready')} "
        f"active_periods={report.get('active_period_count')} "
        f"active_period_id={report.get('active_period_id') or '-'} "
        f"period_kind={report.get('active_period_kind') or '-'} "
        f"signals={counters.get('signals_total', 0)} "
        f"executions={counters.get('executions_total', 0)} "
        f"final={counters.get('executions_final', 0)} "
        f"ambiguous={counters.get('executions_ambiguous', 0)} "
        f"recovery_pending={counters.get('signals_recovery_pending', 0)} "
        f"recovery_unresolved={counters.get('signals_recovery_unresolved', 0)} "
        f"recovered_exact={counters.get('signals_recovered_exact', 0)} "
        f"stale_leases={counters.get('stale_projection_leases', 0)}"
    )
    lines = [head]
    for item in report.get("items") or []:
        lines.append(
            "STATISTICS_SHADOW_DB_ITEM "
            f"severity={item.get('severity')} code={item.get('code')} "
            f"message={str(item.get('message') or '')}"
        )
    return "\n".join(lines)


async def log_statistics_shadow_database_report(
    *,
    settings: Any,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    logger = logger or logging.getLogger(__name__)
    report_obj = await build_statistics_shadow_database_report(
        expected_stage=str(
            getattr(settings, "STATISTICS_SHADOW_EXPECTED_STAGE", "off") or "off"
        ).strip().lower(),
        stale_processing_seconds=int(
            getattr(settings, "FINANCIAL_RECONCILIATION_STALE_PROCESSING_SEC", 120)
            or 120
        ),
    )
    report = report_obj.as_dict()
    text = format_statistics_shadow_database_report(report)
    if not report_obj.ready:
        logger.error(text)
    elif any(item.severity == "WARNING" for item in report_obj.items):
        logger.warning(text)
    else:
        logger.info(text)
    if bool(getattr(settings, "STATISTICS_SHADOW_STRICT_STARTUP", False)) and not report_obj.ready:
        codes = ", ".join(
            item.code for item in report_obj.items if item.severity == "CRITICAL"
        )
        raise RuntimeError(f"Statistics shadow database diagnostics failed: {codes}")
    return report
