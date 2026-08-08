"""Period-aware reports and safe ZIP/CSV export for statistics plan step 8.

All work is explicit, administrator-triggered and read-only.  The module never
queries BingX, never changes trading state and never runs in price/STOP/TP/BE
hot paths.  Bounded exports are chronological, UTF-8 BOM encoded and protected
against spreadsheet formula injection.
"""

from __future__ import annotations

import csv
import html
import io
import json
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping, Sequence

from app import __version__
from app.database.db import connect, is_postgres, monitor_db_workload
from app.services.statistics_analysis import (
    DEFAULT_ANALYSIS_ROW_LIMIT,
    StatisticsAnalysisDataset,
    calculate_dataset_metrics,
    calculate_dataset_quality,
    load_statistics_analysis_dataset,
)
from app.services.statistics_periods import (
    StatisticsPeriod,
    get_active_statistics_period,
    get_statistics_period,
    list_statistics_periods,
)
from app.services.statistics_quality_gate import QUALITY_GATE_VERSION
from app.services.statistics_simulation import (
    DEFAULT_BE_POLICIES,
    DEFAULT_TP_SCHEMES,
    simulate_take_profit_scheme,
)

EXPORT_SCHEMA_VERSION = "statistics-v2-package5-g45"
DEFAULT_EXPORT_ROW_LIMIT = 50_000
MAX_EXPORT_ROW_LIMIT = 100_000


@dataclass(frozen=True, slots=True)
class StatisticsZipExport:
    payload: bytes
    filename: str
    period_id: int
    period_name: str
    file_rows: dict[str, int]
    truncated: bool


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


def _decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        result = Decimal(str(value))
    except Exception:
        return None
    return result if result.is_finite() else None


def _fmt_decimal(value: Any, places: int = 2) -> str:
    parsed = _decimal(value)
    if parsed is None:
        return "—"
    quantum = Decimal("1").scaleb(-max(0, places))
    return f"{parsed.quantize(quantum):f}"


def _fmt_signed(value: Any, places: int = 2) -> str:
    parsed = _decimal(value)
    if parsed is None:
        return "—"
    rendered = _fmt_decimal(parsed, places)
    return f"+{rendered}" if parsed > 0 else rendered


def _fmt_drawdown(value: Any, places: int = 2) -> str:
    parsed = _decimal(value)
    if parsed is None:
        return "—"
    if parsed == 0:
        return _fmt_decimal(parsed, places)
    return f"-{_fmt_decimal(abs(parsed), places)}"


def _fmt_percent(value: Any, places: int = 1) -> str:
    parsed = _decimal(value)
    if parsed is None:
        return "—"
    return f"{_fmt_decimal(parsed, places)}%"


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


def _fmt_dt(value: Any) -> str:
    parsed = _dt(value)
    if parsed is None:
        return "—"
    msk = parsed.astimezone(timezone(timedelta(hours=3)))
    return f"{msk:%d.%m.%Y %H:%M} МСК"


def _escape(value: Any) -> str:
    return html.escape(str(value or "—"))


def _truncated(dataset: StatisticsAnalysisDataset) -> bool:
    return any(
        (
            dataset.signal_rows_truncated,
            dataset.event_rows_truncated,
            dataset.execution_rows_truncated,
            dataset.fill_rows_truncated,
        )
    )


@dataclass(frozen=True, slots=True)
class FriendlySignalLifecycleSummary:
    """Observed lifecycle counters for the human-facing dashboard.

    Unlike strict research metrics, these counters intentionally include every
    durably observed signal row, including rows marked ``needs_recovery``.  The
    dashboard therefore shows what the tracker actually recorded, while a
    separate completeness line states how many terminal trajectories are fit
    for strict win-rate research.
    """

    entered: int
    tp_hits: dict[int, int]
    stop_before_tp1: int
    be_armed: int
    be_closed_after_tp: dict[int, int]
    be_closed_total: int


@dataclass(frozen=True, slots=True)
class StatisticsQualityGateSummary:
    signal_final: int
    signal_simulation: int
    signal_risk: int
    execution_final: int
    execution_simulation: int
    execution_risk: int
    execution_final_net_pnl: Decimal
    exact_linked_executions: int
    funding_confirmed_rows: int
    funding_confirmed_zero: int
    funding_pending: int
    funding_zero_pending: int
    funding_manual_review: int
    stale_gate_rows: int


def _quality_gate_summary(
    signal_rows: Sequence[Mapping[str, Any]],
    execution_rows: Sequence[Mapping[str, Any]],
) -> StatisticsQualityGateSummary:
    signals = [dict(row) for row in signal_rows]
    executions = [dict(row) for row in execution_rows]
    final_rows = [row for row in executions if _int(row.get("final_eligible")) == 1]
    return StatisticsQualityGateSummary(
        signal_final=sum(_int(row.get("final_eligible")) == 1 for row in signals),
        signal_simulation=sum(_int(row.get("simulation_eligible")) == 1 for row in signals),
        signal_risk=sum(_int(row.get("risk_analysis_eligible")) == 1 for row in signals),
        execution_final=len(final_rows),
        execution_simulation=sum(_int(row.get("simulation_eligible")) == 1 for row in executions),
        execution_risk=sum(_int(row.get("risk_analysis_eligible")) == 1 for row in executions),
        execution_final_net_pnl=sum(
            (_decimal(row.get("net_pnl")) or Decimal()) for row in final_rows
        ),
        exact_linked_executions=sum(
            str(row.get("linkage_status") or "").strip().lower() in {"linked_exact", "exact"}
            for row in executions
        ),
        funding_confirmed_rows=sum(
            str(row.get("funding_state") or "").strip().lower() == "confirmed"
            for row in executions
        ),
        funding_confirmed_zero=sum(
            str(row.get("funding_state") or "").strip().lower() == "confirmed_zero"
            for row in executions
        ),
        funding_pending=sum(
            str(row.get("funding_state") or "").strip().lower() == "pending"
            for row in executions
        ),
        funding_zero_pending=sum(
            str(row.get("funding_recovery_status") or "").strip().lower()
            == "pending_zero_confirmation"
            for row in executions
        ),
        funding_manual_review=sum(
            str(row.get("funding_state") or "").strip().lower() == "manual_review"
            or str(row.get("funding_recovery_status") or "").strip().lower() == "manual_review"
            for row in executions
        ),
        stale_gate_rows=sum(_quality_gate_is_stale(row) for row in signals)
        + sum(_quality_gate_is_stale(row) for row in executions),
    )


def _friendly_signal_lifecycle_summary(
    rows: Sequence[Mapping[str, Any]],
) -> FriendlySignalLifecycleSummary:
    normalized = tuple(dict(row) for row in rows)
    entered = sum(_dt(row.get("activated_at")) is not None for row in normalized)
    tp_hits = {
        level: sum(_int(row.get("max_tp_index")) >= level for row in normalized)
        for level in range(1, 5)
    }
    stop_before_tp1 = sum(
        str(row.get("status") or "").strip().lower() == "completed_stop"
        and _int(row.get("max_tp_index")) <= 0
        for row in normalized
    )
    be_armed = sum(_dt(row.get("be_armed_at")) is not None for row in normalized)
    be_closed_after_tp = {
        level: sum(
            str(row.get("status") or "").strip().lower() == "completed_be"
            and _int(row.get("max_tp_index")) == level
            for row in normalized
        )
        for level in range(1, 4)
    }
    be_closed_total = sum(be_closed_after_tp.values())
    return FriendlySignalLifecycleSummary(
        entered=entered,
        tp_hits=tp_hits,
        stop_before_tp1=stop_before_tp1,
        be_armed=be_armed,
        be_closed_after_tp=be_closed_after_tp,
        be_closed_total=be_closed_total,
    )


async def _resolve_period(period_id: int | None) -> StatisticsPeriod:
    period = (
        await get_active_statistics_period()
        if period_id is None
        else await get_statistics_period(int(period_id))
    )
    if period is None:
        raise LookupError("statistics period not found")
    return period


async def _load_period_dataset(
    period_id: int,
    *,
    row_limit: int = DEFAULT_ANALYSIS_ROW_LIMIT,
    user_id: int | None = None,
) -> StatisticsAnalysisDataset:
    return await load_statistics_analysis_dataset(
        period_id=period_id,
        row_limit=max(1, min(MAX_EXPORT_ROW_LIMIT, int(row_limit))),
        user_id=user_id,
    )


def _period_header(period: StatisticsPeriod) -> list[str]:
    status_icon = "🟢" if period.status == "active" else "⚪"
    return [
        "<b>📊 СТАТИСТИКА ПЕРИОДА</b>",
        "",
        f"{status_icon} Период: <b>#{period.id}</b> — <b>{_escape(period.name)}</b>",
        f"Статус: <b>{_escape(period.status)}</b> | тип: <b>{_escape(period.period_kind)}</b>",
        f"Начало: {_escape(_fmt_dt(period.started_at))}",
        f"Завершение: {_escape(_fmt_dt(period.closed_at))}",
        f"Версия создания: <code>{_escape(period.source_version)}</code>",
    ]


def _runtime_execution_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    normalized = [dict(row) for row in rows]
    terminal = 0
    active = 0
    waiting_financial = 0
    missing_runtime = 0
    for row in normalized:
        status = str(row.get("runtime_execution_status") or "").strip().lower()
        is_terminal = status.startswith("closed_")
        if not status:
            missing_runtime += 1
        elif is_terminal:
            terminal += 1
        else:
            active += 1
        if is_terminal and int(row.get("final_eligible") or 0) != 1:
            waiting_financial += 1
    return {
        "total": len(normalized),
        "terminal": terminal,
        "active": active,
        "waiting_financial": waiting_financial,
        "missing_runtime": missing_runtime,
    }


async def format_statistics_period_report(
    period_id: int | None = None, *, user_id: int | None = None
) -> str:
    """Render the friendly administrator dashboard.

    The main Telegram card deliberately avoids internal statistics-v2 states.
    Exact technical status, linkage, quality and quarantine remain available in
    :func:`format_statistics_technical_report` and in the downloadable export.
    """
    period = await _resolve_period(period_id)
    dataset = (
        await _load_period_dataset(period.id, user_id=user_id)
        if user_id is not None
        else await _load_period_dataset(period.id)
    )
    lines = [
        "<b>📊 СТАТИСТИКА</b>",
        "━━━━━━━━━━━━━━━━━━",
        "",
        "👤 <b>Область статистики</b>",
        (
            f"Только мой BingX аккаунт · user_id <code>{int(user_id)}</code>"
            if user_id is not None
            else "Все аккаунты (технический режим)"
        ),
        "",
        "🗓 <b>Период</b>",
        (
            f"с <b>{_escape(_fmt_dt(period.started_at))}</b>"
            if period.status == "active"
            else f"<b>{_escape(_fmt_dt(period.started_at))}</b> → <b>{_escape(_fmt_dt(period.closed_at))}</b>"
        ),
    ]
    if _truncated(dataset):
        return "\n".join(
            lines
            + [
                "",
                "⚠️ <b>Период слишком большой для быстрого экрана.</b>",
                "",
                f"Сигналов: <b>{dataset.total_signals}</b>",
                f"Исполнений: <b>{dataset.total_executions}</b>",
                "",
                "📥 Скачайте полную статистику — в ZIP сохраняются все доступные строки и технические сведения.",
            ]
        )

    metrics = calculate_dataset_metrics(dataset)
    s = metrics.signals
    e = metrics.executions

    if s.total_unique_signals == 0 and e.total_executions == 0:
        return "\n".join(
            lines
            + [
                "",
                "🌱 <b>Новый период начат</b>",
                "",
                "Пока новых сигналов нет.",
                "Все следующие сигналы и сделки будут учитываться здесь.",
                "",
                "📥 Подробные данные можно скачать кнопкой ниже.",
            ]
        )

    lifecycle = _friendly_signal_lifecycle_summary(dataset.signal_rows)
    runtime = _runtime_execution_summary(dataset.execution_rows)
    tp1_rate = (
        Decimal(lifecycle.tp_hits.get(1, 0)) * Decimal("100") / Decimal(lifecycle.entered)
        if lifecycle.entered > 0
        else None
    )
    strict_completed = s.scope.eligible_rows
    incomplete_completed = max(0, s.completed - strict_completed)

    lines.extend(
        [
            "",
            (
                "📌 <b>Сигналы, связанные с моим BingX аккаунтом</b>"
                if user_id is not None
                else "📌 <b>Рыночные сигналы (движение цены)</b>"
            ),
            f"Всего: <b>{s.total_unique_signals}</b>",
            f"Активных: <b>{s.active}</b>  ·  Завершено: <b>{s.completed}</b>",
            f"Ждут входа: <b>{s.waiting_entry}</b>  ·  Цена достигла входа: <b>{lifecycle.entered}</b>",
            "",
            "🎯 <b>Уровни, достигнутые ценой по сигналам</b>",
            f"TP1: <b>{lifecycle.tp_hits.get(1, 0)}</b>  ·  TP2: <b>{lifecycle.tp_hits.get(2, 0)}</b>",
            f"TP3: <b>{lifecycle.tp_hits.get(3, 0)}</b>  ·  TP4: <b>{lifecycle.tp_hits.get(4, 0)}</b>",
            f"Стоп до TP1: <b>{lifecycle.stop_before_tp1}</b>  ·  Б/У активирован: <b>{lifecycle.be_armed}</b>",
            (
                f"Дошли до TP1: <b>{lifecycle.tp_hits.get(1, 0)} из {lifecycle.entered}</b> "
                f"(<b>{_fmt_percent(tp1_rate)}</b>)"
                if lifecycle.entered > 0
                else "Дошли до TP1: <b>0 из 0</b>"
            ),
            "",
            "🛡 <b>ЗАКРЫТИЕ В БЕЗУБЫТОК</b>",
            "",
            f"После TP1  •  <b>{lifecycle.be_closed_after_tp.get(1, 0)}</b>",
            f"После TP2  •  <b>{lifecycle.be_closed_after_tp.get(2, 0)}</b>",
            f"После TP3  •  <b>{lifecycle.be_closed_after_tp.get(3, 0)}</b>",
            "",
            f"Всего закрыто в Б/У: <b>{lifecycle.be_closed_total}</b>",
            "",
            (
                "🏦 <b>Исполнения на моём BingX аккаунте</b>"
                if user_id is not None
                else "🏦 <b>Исполнения на аккаунтах BingX</b>"
            ),
            f"Создано executions: <b>{runtime['total']}</b>",
            f"Сейчас в работе по runtime-статусу: <b>{runtime['active']}</b>",
            f"Закрыто по runtime-статусу: <b>{runtime['terminal']}</b>",
            f"Закрыто, но ждёт финансовой сверки: <b>{runtime['waiting_financial']}</b>",
            *(
                [f"Нет runtime-связи: <b>{runtime['missing_runtime']}</b>"]
                if runtime["missing_runtime"] > 0
                else []
            ),
            f"Финансово FINAL: <b>{e.scope.eligible_rows}</b>",
            "",
            "🧪 <b>Полнота рыночных историй сигналов</b>",
            f"С полной историей: <b>{strict_completed} из {s.completed}</b>",
            f"Неполная или спорная история: <b>{incomplete_completed}</b>",
            "",
            "💰 <b>Финансы</b>",
        ]
    )

    if e.scope.eligible_rows <= 0:
        lines.extend(
            [
                "Подтверждённых закрытых исполнений пока нет.",
                "Финансовый итог появится после точной сверки с биржей.",
            ]
        )
    else:
        lines.extend(
            [
                f"Подтверждённый результат FINAL: <b>{_fmt_signed(e.net_pnl)} USDT</b>",
                f"Средний результат: <b>{_fmt_signed(e.expectancy_r, 3)}R</b>",
                f"Комиссии: <b>{_fmt_signed(e.trading_fees_signed)} USDT</b>",
                f"Фандинг: <b>{_fmt_signed(e.funding_signed)} USDT</b>",
                "",
                "📉 <b>Максимальная просадка</b>",
                f"<b>{_fmt_drawdown(e.maximum_drawdown_r, 3)}R</b>  ·  <b>{_fmt_drawdown(e.maximum_drawdown_usd)} USDT</b>",
            ]
        )

    confirmation_line = (
        "Финансовых исполнений пока нет."
        if e.total_executions <= 0
        else (
            f"Финансово FINAL: <b>{e.scope.eligible_rows}</b> из <b>{e.total_executions}</b> executions; "
            f"закрытых runtime ждут сверки: <b>{runtime['waiting_financial']}</b>"
        )
    )
    lines.extend(
        [
            "",
            "✅ <b>Подтверждение данных</b>",
            confirmation_line,
            "",
            "📥 Полный отчёт со всеми техническими данными доступен по кнопке «Скачать статистику».",
        ]
    )
    return "\n".join(lines)


async def format_statistics_technical_report(period_id: int | None = None, *, user_id: int | None = None) -> str:
    """Render the full diagnostic view previously shown on the main card."""
    period = await _resolve_period(period_id)
    dataset = (
        await _load_period_dataset(period.id, user_id=user_id)
        if user_id is not None
        else await _load_period_dataset(period.id)
    )
    lines = _period_header(period)
    if user_id is not None:
        lines.extend(["", f"👤 Только мой BingX аккаунт · <code>{int(user_id)}</code>"])
    if _truncated(dataset):
        lines.extend(
            [
                "",
                "⚠️ <b>Период превышает безопасный лимит отчёта.</b>",
                "Метрики не рассчитываются по обрезанной выборке. Используйте ZIP-экспорт.",
                f"Сигналов: <b>{dataset.total_signals}</b>",
                f"Исполнений: <b>{dataset.total_executions}</b>",
                f"Fills: <b>{dataset.total_fills}</b>",
            ]
        )
        return "\n".join(lines)

    metrics = calculate_dataset_metrics(dataset)
    quality = calculate_dataset_quality(dataset)
    gate = _quality_gate_summary(dataset.signal_rows, dataset.execution_rows)
    s = metrics.signals
    e = metrics.executions
    lines.extend(
        [
            "",
            "<b>📌 Уникальные сигналы</b>",
            f"Всего: <b>{s.total_unique_signals}</b>",
            f"Завершено и пригодно для метрик: <b>{s.scope.eligible_rows}</b>",
            f"Активных: <b>{s.active}</b> | ждут входа: <b>{s.waiting_entry}</b>",
            f"Требуют recovery: <b>{s.needs_recovery}</b> | ambiguous: <b>{s.ambiguous}</b>",
            f"TP1: <b>{s.tp_hit_counts.get(1, 0)}</b> | TP2: <b>{s.tp_hit_counts.get(2, 0)}</b> | TP3: <b>{s.tp_hit_counts.get(3, 0)}</b> | TP4: <b>{s.tp_hit_counts.get(4, 0)}</b>",
            f"STOP до TP1: <b>{s.stop_before_tp1}</b>",
            f"Полные траектории: <b>{_fmt_decimal(s.complete_trajectory_share_percent, 1)}%</b>",
            "",
            "<b>💰 Фактические исполнения</b>",
            f"Всего: <b>{e.total_executions}</b> | FINAL: <b>{e.final}</b>",
            f"PROVISIONAL: <b>{e.provisional}</b> | AMBIGUOUS: <b>{e.ambiguous}</b> | UNAVAILABLE: <b>{e.unavailable}</b>",
            f"Net PnL FINAL: <b>{_fmt_decimal(e.net_pnl)} USDT</b>",
            f"Expectancy: <b>{_fmt_decimal(e.expectancy_r, 3)}R</b>",
            f"Profit factor: <b>{_fmt_decimal(e.profit_factor, 3)}</b>",
            f"Max drawdown: <b>{_fmt_decimal(e.maximum_drawdown_r, 3)}R</b> / <b>{_fmt_decimal(e.maximum_drawdown_usd)} USDT</b>",
            f"Комиссии: <b>{_fmt_decimal(e.trading_fees_signed)} USDT</b> | funding: <b>{_fmt_decimal(e.funding_signed)} USDT</b>",
            "",
            "<b>🩺 Полнота и доверие</b>",
            f"Связано сигналов: <b>{_fmt_decimal(quality.linked_signal_coverage_percent, 1)}%</b>",
            f"FINAL financial coverage: <b>{_fmt_decimal(quality.final_financial_coverage_percent, 1)}%</b>",
            f"Статус доверия: <b>{_escape(quality.trust_status)}</b>",
            f"Проблем quality: <b>{len(quality.issues)}</b> | quarantine: <b>{len(quality.quarantine_rows)}</b>",
            "",
            "<b>🛡️ Quality Gate v2</b>",
            f"Executions: FINAL PnL <b>{gate.execution_final}</b> | simulations <b>{gate.execution_simulation}</b> | risk audit <b>{gate.execution_risk}</b>",
            f"Signals: FINAL <b>{gate.signal_final}</b> | simulations <b>{gate.signal_simulation}</b> | risk audit <b>{gate.signal_risk}</b>",
            f"Net PnL по FINAL-gate: <b>{_fmt_decimal(gate.execution_final_net_pnl)} USDT</b>",
            f"Funding: rows <b>{gate.funding_confirmed_rows}</b> | zero <b>{gate.funding_confirmed_zero}</b> | pending <b>{gate.funding_pending}</b> | manual review <b>{gate.funding_manual_review}</b>",
            f"Gate rows ожидают пересчёт: <b>{gate.stale_gate_rows}</b>",
            "",
            "ℹ️ FINAL PnL, симуляции и анализ риска допускаются независимо. Неполные данные не подмешиваются в неподходящую метрику.",
        ]
    )
    return "\n".join(lines)


async def format_statistics_financial_report(period_id: int | None = None, *, user_id: int | None = None) -> str:
    period = await _resolve_period(period_id)
    dataset = (
        await _load_period_dataset(period.id, user_id=user_id)
        if user_id is not None
        else await _load_period_dataset(period.id)
    )
    if _truncated(dataset):
        return (
            f"⚠️ <b>Финансовый отчёт периода #{period.id} не рассчитан:</b> "
            "bounded dataset truncated."
        )
    report = calculate_dataset_metrics(dataset).executions
    reasons = ", ".join(
        f"{html.escape(key)}={value}"
        for key, value in sorted(report.scope.exclusion_reasons.items())
    ) or "нет"
    return "\n".join(
        [
            "<b>💰 ФИНАНСОВАЯ СТАТИСТИКА</b>",
            "",
            f"Период: <b>#{period.id}</b> — <b>{_escape(period.name)}</b>",
            f"Аккаунт: <code>{int(user_id)}</code>" if user_id is not None else "Аккаунт: все (technical)",
            f"Всего executions: <b>{report.total_executions}</b>",
            f"FINAL: <b>{report.final}</b> | полнота: <b>{_fmt_decimal(report.scope.completeness_percent, 1)}%</b>",
            f"PROVISIONAL: <b>{report.provisional}</b>",
            f"AMBIGUOUS: <b>{report.ambiguous}</b> | UNAVAILABLE: <b>{report.unavailable}</b> | PENDING: <b>{report.pending}</b>",
            "",
            f"Gross PnL: <b>{_fmt_decimal(report.gross_pnl)} USDT</b>",
            f"Trading fees signed: <b>{_fmt_decimal(report.trading_fees_signed)} USDT</b>",
            f"Funding signed: <b>{_fmt_decimal(report.funding_signed)} USDT</b>",
            f"Net PnL: <b>{_fmt_decimal(report.net_pnl)} USDT</b>",
            f"Expectancy: <b>{_fmt_decimal(report.expectancy_r, 3)}R</b>",
            f"Средний R: <b>{_fmt_decimal(report.average_r, 3)}R</b> | медиана: <b>{_fmt_decimal(report.median_r, 3)}R</b>",
            f"Profit factor: <b>{_fmt_decimal(report.profit_factor, 3)}</b>",
            f"Max drawdown: <b>{_fmt_decimal(report.maximum_drawdown_r, 3)}R</b> / <b>{_fmt_decimal(report.maximum_drawdown_usd)} USDT</b>",
            f"Худшая серия убытков: <b>{report.worst_losing_streak}</b>",
            "",
            f"Исключения: <code>{reasons}</code>",
            "ℹ️ Комиссия и funding сохраняют знак биржи; нулевые значения не выдумываются при недоступной истории.",
        ]
    )


async def format_statistics_quality_report(period_id: int | None = None, *, user_id: int | None = None) -> str:
    period = await _resolve_period(period_id)
    dataset = (
        await _load_period_dataset(period.id, user_id=user_id)
        if user_id is not None
        else await _load_period_dataset(period.id)
    )
    if _truncated(dataset):
        return (
            f"⚠️ <b>Quality-отчёт периода #{period.id} заблокирован:</b> "
            "bounded dataset truncated."
        )
    report = calculate_dataset_quality(dataset)
    gate = _quality_gate_summary(dataset.signal_rows, dataset.execution_rows)
    issue_lines = [
        f"- {html.escape(code)}: <b>{count}</b>"
        for code, count in list(sorted(report.issue_counts.items(), key=lambda x: (-x[1], x[0])))[:12]
    ]
    lines = [
        "<b>🩺 DATA QUALITY</b>",
        "",
        f"Период: <b>#{period.id}</b> — <b>{_escape(period.name)}</b>",
            f"Аккаунт: <code>{int(user_id)}</code>" if user_id is not None else "Аккаунт: все (technical)",
        f"Trust status: <b>{_escape(report.trust_status)}</b>",
        f"Связано сигналов: <b>{_fmt_decimal(report.linked_signal_coverage_percent, 1)}%</b>",
        f"FINAL financial coverage: <b>{_fmt_decimal(report.final_financial_coverage_percent, 1)}%</b>",
        f"Сигналы: <b>{report.signal_summary.total_rows}</b> | quarantined: <b>{report.signal_summary.quarantined_rows}</b>",
        f"Executions: <b>{report.execution_summary.total_rows}</b> | FINAL: <b>{report.execution_summary.final_rows}</b> | quarantined: <b>{report.execution_summary.quarantined_rows}</b>",
        f"Fills: <b>{report.fill_summary.total_rows}</b> | duplicate observations: <b>{report.fill_summary.duplicate_observations}</b>",
        f"Gate v2 executions: FINAL <b>{gate.execution_final}</b> | simulation <b>{gate.execution_simulation}</b> | risk <b>{gate.execution_risk}</b>",
        f"Funding pending zero: <b>{gate.funding_zero_pending}</b> | manual review: <b>{gate.funding_manual_review}</b>",
        f"Gate rows ожидают пересчёт: <b>{gate.stale_gate_rows}</b>",
        f"Всего выявленных проблем: <b>{len(report.issues)}</b>",
    ]
    if report.trust_reasons:
        lines.append("Причины: " + html.escape(", ".join(report.trust_reasons)))
    if issue_lines:
        lines.extend(["", "<b>Основные issue codes</b>", *issue_lines])
    lines.extend(
        [
            "",
            "ℹ️ Команда формирует read-only проверку. Она не исправляет историю и не отправляет запросы на биржу.",
        ]
    )
    return "\n".join(lines)


async def format_statistics_periods_report(
    *, limit: int = 30, user_id: int | None = None
) -> str:
    periods = await list_statistics_periods(limit=limit)
    lines = ["<b>🗂 ПЕРИОДЫ СТАТИСТИКИ</b>", ""]
    if user_id is not None:
        lines.extend([f"👤 Только мой BingX аккаунт · <code>{int(user_id)}</code>", ""])
    if not periods:
        return "\n".join(lines + ["Периоды не найдены."])
    for period in periods:
        icon = "🟢" if period.status == "active" else "⚪"
        if user_id is None:
            signal_count = period.signal_count
            execution_count = period.execution_count
            final_count = period.final_execution_count
            quality_count = period.quality_issue_count
        else:
            dataset = await _load_period_dataset(period.id, user_id=user_id)
            metrics = calculate_dataset_metrics(dataset) if not _truncated(dataset) else None
            signal_count = dataset.total_signals
            execution_count = dataset.total_executions
            final_count = metrics.executions.scope.eligible_rows if metrics is not None else 0
            quality_count = len(calculate_dataset_quality(dataset).issues) if metrics is not None else 0
        lines.extend(
            [
                f"{icon} <b>#{period.id}</b> — {_escape(period.name)}",
                f"　{_escape(_fmt_dt(period.started_at))} → {_escape(_fmt_dt(period.closed_at))}",
                f"　signals <b>{signal_count}</b> | executions <b>{execution_count}</b> | FINAL <b>{final_count}</b> | quality <b>{quality_count}</b>",
            ]
        )
    lines.extend(["", "Открыть: <code>/stats_period ID</code>", "Экспорт: <code>/stats_export ID</code>"])
    return "\n".join(lines)


async def format_statistics_all_report(*, user_id: int | None = None) -> str:
    periods = await list_statistics_periods(limit=MAX_EXPORT_ROW_LIMIT)
    active = next((item for item in periods if item.status == "active"), None)
    if user_id is None:
        total_signals = sum(item.signal_count for item in periods)
        total_executions = sum(item.execution_count for item in periods)
        total_final = sum(item.final_execution_count for item in periods)
        total_quality = sum(item.quality_issue_count for item in periods)
    else:
        total_signals = total_executions = total_final = total_quality = 0
        for period in periods:
            dataset = await _load_period_dataset(period.id, user_id=user_id)
            total_signals += dataset.total_signals
            total_executions += dataset.total_executions
            if not _truncated(dataset):
                total_final += calculate_dataset_metrics(dataset).executions.scope.eligible_rows
                total_quality += len(calculate_dataset_quality(dataset).issues)
    return "\n".join(
        [
            "<b>📚 СВОДКА ВСЕХ ПЕРИОДОВ</b>",
            "",
            f"Аккаунт: <code>{int(user_id)}</code>" if user_id is not None else "Аккаунт: все (technical)",
            f"Периодов: <b>{len(periods)}</b>",
            f"Активный: <b>{('#' + str(active.id) + ' — ' + active.name) if active else 'нет'}</b>",
            f"Уникальных сигналов по периодам: <b>{total_signals}</b>",
            f"Executions: <b>{total_executions}</b> | FINAL: <b>{total_final}</b>",
            f"Quality issues: <b>{total_quality}</b>",
            "",
            "ℹ️ Сводка изолирована по аккаунту и не смешивает executions других пользователей.",
        ]
    )


_SENSITIVE_EXPORT_KEY_FRAGMENTS = (
    "api_key",
    "api_secret",
    "secret",
    "passphrase",
    "authorization",
    "access_token",
    "refresh_token",
    "bot_token",
    "cookie",
    "signature",
)


def _is_sensitive_export_key(value: Any) -> bool:
    normalized = str(value or "").strip().lower().replace("-", "_")
    return any(fragment in normalized for fragment in _SENSITIVE_EXPORT_KEY_FRAGMENTS)


def _redact_export_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): ("[REDACTED]" if _is_sensitive_export_key(key) else _redact_export_value(item))
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_redact_export_value(item) for item in value]
    return value


def _csv_value(value: Any, *, column: str = "") -> Any:
    if _is_sensitive_export_key(column):
        return "[REDACTED]"
    if value is None:
        return ""
    if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        return value
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    safe_value = _redact_export_value(value)
    if isinstance(safe_value, (dict, list, tuple)):
        text = json.dumps(safe_value, ensure_ascii=False, sort_keys=True, default=str)
    else:
        text = str(safe_value)
        if str(column).lower().endswith("_json") and text.strip().startswith(("{", "[")):
            try:
                parsed = json.loads(text)
            except (TypeError, ValueError, json.JSONDecodeError):
                parsed = None
            if isinstance(parsed, (dict, list)):
                text = json.dumps(
                    _redact_export_value(parsed),
                    ensure_ascii=False,
                    sort_keys=True,
                    default=str,
                )
    if text.startswith(("=", "+", "-", "@", "\t", "\r", "\n")):
        return "'" + text
    return text


def _csv_bytes(rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=list(columns), extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(
            {column: _csv_value(row.get(column), column=column) for column in columns}
        )
    return stream.getvalue().encode("utf-8-sig")


def _columns(rows: Sequence[Mapping[str, Any]], preferred: Sequence[str] = ()) -> tuple[str, ...]:
    ordered: list[str] = []
    seen: set[str] = set()
    for key in preferred:
        if key not in seen:
            ordered.append(key)
            seen.add(key)
    for row in rows:
        for key in row.keys():
            if key not in seen:
                ordered.append(str(key))
                seen.add(str(key))
    return tuple(ordered or preferred or ("empty",))


async def _fetch_period_funding(
    period_id: int, limit: int, *, user_id: int | None = None
) -> tuple[list[dict[str, Any]], int]:
    if is_postgres():
        where = "r.period_id=$1"
        args: list[int] = [int(period_id)]
        if user_id is not None:
            args.append(int(user_id))
            where += f" AND r.user_id=${len(args)}"
            where += " AND COALESCE(e.status,'') <> 'superseded_duplicate'"
        limit_placeholder = f"${len(args) + 1}"
        query = (
            "SELECT f.* FROM financial_funding_events f "
            "JOIN analytics_execution_results r ON r.execution_id=f.execution_id "
            "LEFT JOIN trade_executions e ON e.id=r.execution_id "
            f"WHERE {where} ORDER BY f.event_time,f.id LIMIT {limit_placeholder}"
        )
        count_query = (
            "SELECT COUNT(*) FROM financial_funding_events f "
            "JOIN analytics_execution_results r ON r.execution_id=f.execution_id "
            "LEFT JOIN trade_executions e ON e.id=r.execution_id "
            f"WHERE {where}"
        )
    else:
        where = "r.period_id=?"
        args = [int(period_id)]
        if user_id is not None:
            args.append(int(user_id))
            where += " AND r.user_id=?"
            where += " AND COALESCE(e.status,'') <> 'superseded_duplicate'"
        query = (
            "SELECT f.* FROM financial_funding_events f "
            "JOIN analytics_execution_results r ON r.execution_id=f.execution_id "
            "LEFT JOIN trade_executions e ON e.id=r.execution_id "
            f"WHERE {where} ORDER BY f.event_time,f.id LIMIT ?"
        )
        count_query = (
            "SELECT COUNT(*) FROM financial_funding_events f "
            "JOIN analytics_execution_results r ON r.execution_id=f.execution_id "
            "LEFT JOIN trade_executions e ON e.id=r.execution_id "
            f"WHERE {where}"
        )
    async with connect() as conn:
        if is_postgres():
            total = int(await conn.fetchval(count_query, *args) or 0)
            rows = await conn.fetch(query, *args, int(limit))
        else:
            cursor = await conn.execute(count_query, tuple(args))
            row = await cursor.fetchone()
            total = int(row[0] if row else 0)
            cursor = await conn.execute(query, (*args, int(limit)))
            rows = await cursor.fetchall()
    return [_as_dict(row) for row in rows], total


async def _fetch_period_quality_audit(period_id: int, limit: int) -> tuple[list[dict[str, Any]], int]:
    query = (
        "SELECT * FROM statistics_quality_audit WHERE period_id=$1 ORDER BY created_at,id LIMIT $2"
        if is_postgres()
        else "SELECT * FROM statistics_quality_audit WHERE period_id=? ORDER BY created_at,id LIMIT ?"
    )
    count_query = (
        "SELECT COUNT(*) FROM statistics_quality_audit WHERE period_id=$1"
        if is_postgres()
        else "SELECT COUNT(*) FROM statistics_quality_audit WHERE period_id=?"
    )
    async with connect() as conn:
        if is_postgres():
            total = int(await conn.fetchval(count_query, period_id) or 0)
            rows = await conn.fetch(query, period_id, limit)
        else:
            cursor = await conn.execute(count_query, (period_id,))
            row = await cursor.fetchone()
            total = int(row[0] if row else 0)
            cursor = await conn.execute(query, (period_id, limit))
            rows = await cursor.fetchall()
    return [_as_dict(row) for row in rows], total


def _calculated_quality_rows(dataset: StatisticsAnalysisDataset) -> list[dict[str, Any]]:
    if _truncated(dataset):
        return []
    report = calculate_dataset_quality(dataset)
    return [
        {
            "source": "calculated_read_only",
            "entity_type": issue.entity_type,
            "entity_id": issue.entity_id,
            "issue_code": issue.issue_code,
            "severity": issue.severity,
            "recoverable": int(issue.recoverable),
            "reason": issue.reason,
            "related_ids": json.dumps(list(issue.related_ids), ensure_ascii=False),
        }
        for issue in report.issues
    ]


def _simulation_rows(dataset: StatisticsAnalysisDataset) -> list[dict[str, Any]]:
    if _truncated(dataset):
        return []
    rows: list[dict[str, Any]] = []
    for scheme in DEFAULT_TP_SCHEMES:
        for policy in DEFAULT_BE_POLICIES:
            result = simulate_take_profit_scheme(
                dataset.signal_rows,
                dataset.event_rows,
                dataset.execution_rows,
                scheme=scheme,
                be_policy=policy,
                require_costs=True,
            )
            eligible = int(result.scope.eligible_rows)
            total = int(result.scope.total_rows)
            scope_status = (
                "no_eligible_rows"
                if eligible <= 0
                else "full_scope"
                if total > 0 and eligible == total
                else "partial_scope"
            )
            rows.append(
                {
                    "scheme": result.scheme_name,
                    "be_policy": result.be_policy_name,
                    "scope_status": scope_status,
                    "total_rows": total,
                    "eligible_rows": eligible,
                    "excluded_rows": result.scope.excluded_rows,
                    "completeness_percent": result.scope.completeness_percent,
                    "total_r": result.total_r,
                    "average_r": result.average_r,
                    "median_r": result.median_r,
                    "win_rate_percent": result.win_rate_percent,
                    "profit_factor": result.profit_factor,
                    "maximum_drawdown_r": result.maximum_drawdown_r,
                    "worst_losing_streak": result.worst_losing_streak,
                    "cost_coverage_percent": result.cost_coverage_percent,
                    "exclusion_reasons_json": json.dumps(result.scope.exclusion_reasons, sort_keys=True),
                }
            )
    return rows


def _collection_integrity_counts(
    signal_rows: Sequence[Mapping[str, Any]],
    execution_rows: Sequence[Mapping[str, Any]],
) -> dict[str, int]:
    """Return truthful export counters from fields present in the dataset.

    Package g44 omitted quality-gate/freshness columns from the analysis SELECT,
    which made every exported gate counter look like zero and every row look
    stale.  G45 keeps the legacy counters for compatibility and adds explicit
    terminal/non-terminal, projection and simulation-readiness splits.  All
    counters remain fail-closed and never infer missing exchange evidence.
    """

    terminal_statuses = {"completed_stop", "completed_be", "completed_tp"}
    counters = {
        "signals_recovery_pending": 0,
        "signals_recovery_unresolved": 0,
        "signals_recovery_legacy_forward_resumed": 0,
        "signals_recovered_exact": 0,
        "signals_recovery_unavailable": 0,
        "signals_recovery_ambiguous": 0,
        "signals_tp_distribution_missing": 0,
        "signals_tp_distribution_missing_terminal": 0,
        "signals_tp_distribution_missing_nonterminal": 0,
        "signals_tp_distribution_consensus": 0,
        "signals_tp_distribution_conflict": 0,
        "signals_tp_distribution_variant": 0,
        "executions_risk_snapshot_missing": 0,
        "executions_risk_snapshot_complete": 0,
        "executions_risk_snapshot_missing_final": 0,
        "executions_risk_snapshot_missing_nonfinal": 0,
        "executions_tp_distribution_missing": 0,
        "executions_tp_distribution_unlocked": 0,
        "executions_tp_distribution_locked": 0,
        "executions_tp_distribution_missing_final": 0,
        "executions_tp_distribution_missing_pending": 0,
        "executions_projection_complete": 0,
        "executions_projection_pending": 0,
        "executions_projection_processing": 0,
        "executions_projection_retry": 0,
        "executions_projection_ambiguous": 0,
        "executions_projection_unavailable": 0,
        "executions_projection_other": 0,
        "executions_financial_pending_on_terminal_signal": 0,
        "executions_financial_pending_on_nonterminal_signal": 0,
        "executions_runtime_linked": 0,
        "executions_runtime_missing": 0,
        "executions_runtime_active": 0,
        "executions_runtime_closed": 0,
        "executions_runtime_closed_waiting_financial": 0,
        "executions_financial_job_missing": 0,
        "executions_financial_job_present": 0,
        "executions_g54_backfill_ready": 0,
        "executions_g54_backfill_blocked": 0,
        "signals_final_eligible": 0,
        "signals_simulation_eligible": 0,
        "signals_risk_analysis_eligible": 0,
        "executions_final_eligible": 0,
        "executions_simulation_eligible": 0,
        "executions_risk_analysis_eligible": 0,
        "executions_funding_pending": 0,
        "executions_funding_zero_pending": 0,
        "executions_funding_manual_review": 0,
        "quality_gate_rows_current": 0,
        "quality_gate_rows_stale": 0,
    }
    signal_status_by_id: dict[int, str] = {}
    for row in signal_rows:
        signal_id = _int(row.get("id"))
        recovery = str(row.get("recovery_status") or "").strip().lower()
        status = str(row.get("status") or "").strip().lower()
        if signal_id > 0:
            signal_status_by_id[signal_id] = status
        needs_recovery = bool(row.get("needs_recovery"))
        if recovery in {"pending", "processing", "retry"}:
            counters["signals_recovery_pending"] += 1
        if needs_recovery and recovery not in {"pending", "processing", "retry"}:
            counters["signals_recovery_unresolved"] += 1
        if recovery == "forward_resumed":
            counters["signals_recovery_legacy_forward_resumed"] += 1
        if recovery == "recovered_exact":
            counters["signals_recovered_exact"] += 1
        if recovery == "unavailable":
            counters["signals_recovery_unavailable"] += 1
        if recovery == "ambiguous" or (status == "ambiguous" and recovery not in {"", "not_required"}):
            counters["signals_recovery_ambiguous"] += 1
        tp_source = str(row.get("target_percents_source") or "").strip().lower()
        tp_value = str(row.get("target_percents_json") or "[]").strip()
        missing_tp = tp_value in {"", "[]", "null", "None"} or tp_source in {
            "", "empty", "source_or_empty", "execution_incomplete"
        }
        if tp_source == "execution_conflict":
            counters["signals_tp_distribution_conflict"] += 1
        elif tp_source == "execution_variant":
            counters["signals_tp_distribution_variant"] += 1
        elif missing_tp:
            counters["signals_tp_distribution_missing"] += 1
            split = (
                "signals_tp_distribution_missing_terminal"
                if status in terminal_statuses
                else "signals_tp_distribution_missing_nonterminal"
            )
            counters[split] += 1
        else:
            counters["signals_tp_distribution_consensus"] += 1
        counters["signals_final_eligible"] += int(_int(row.get("final_eligible")) == 1)
        counters["signals_simulation_eligible"] += int(_int(row.get("simulation_eligible")) == 1)
        counters["signals_risk_analysis_eligible"] += int(_int(row.get("risk_analysis_eligible")) == 1)
        stale = _quality_gate_is_stale(row)
        counters["quality_gate_rows_stale"] += int(stale)
        counters["quality_gate_rows_current"] += int(not stale)

    known_projection_states = {
        "complete", "pending", "processing", "retry", "ambiguous", "unavailable"
    }
    for row in execution_rows:
        financial_state = str(row.get("financial_state") or "").strip().lower()
        required_risk = (
            row.get("equity_snapshot_usd"), row.get("planned_risk_usd"),
            row.get("expected_loss_at_stop_usd"), row.get("planned_entry_qty"),
            row.get("stop_distance"),
        )
        missing_risk = any(value in (None, "") for value in required_risk)
        if missing_risk:
            counters["executions_risk_snapshot_missing"] += 1
            counters[
                "executions_risk_snapshot_missing_final"
                if financial_state == "final"
                else "executions_risk_snapshot_missing_nonfinal"
            ] += 1
        else:
            counters["executions_risk_snapshot_complete"] += 1

        distribution = str(row.get("tp_distribution_json") or "[]").strip()
        distribution_missing = distribution in {"", "[]", "null", "None"}
        distribution_locked = _int(row.get("tp_distribution_locked")) == 1
        if distribution_missing:
            counters["executions_tp_distribution_missing"] += 1
            if financial_state == "final":
                counters["executions_tp_distribution_missing_final"] += 1
            if financial_state == "pending":
                counters["executions_tp_distribution_missing_pending"] += 1
        if distribution_locked:
            counters["executions_tp_distribution_locked"] += 1
        else:
            counters["executions_tp_distribution_unlocked"] += 1

        projection = str(row.get("projection_status") or "").strip().lower()
        projection_key = (
            f"executions_projection_{projection}"
            if projection in known_projection_states
            else "executions_projection_other"
        )
        counters[projection_key] += 1

        if financial_state == "pending":
            signal_status = signal_status_by_id.get(_int(row.get("analytics_signal_id")), "")
            counters[
                "executions_financial_pending_on_terminal_signal"
                if signal_status in terminal_statuses
                else "executions_financial_pending_on_nonterminal_signal"
            ] += 1

        runtime_status = str(row.get("runtime_execution_status") or "").strip().lower()
        runtime_closed = runtime_status.startswith("closed_")
        counters[
            "executions_runtime_linked" if runtime_status else "executions_runtime_missing"
        ] += 1
        if runtime_status:
            counters[
                "executions_runtime_closed" if runtime_closed else "executions_runtime_active"
            ] += 1
        if runtime_closed and financial_state != "final":
            counters["executions_runtime_closed_waiting_financial"] += 1
        has_job = row.get("financial_job_id") not in (None, "", 0, "0")
        counters[
            "executions_financial_job_present" if has_job else "executions_financial_job_missing"
        ] += 1
        backfill_result = str(row.get("g54_backfill_result") or "").strip().lower()
        if backfill_result == "ready":
            counters["executions_g54_backfill_ready"] += 1
        elif backfill_result == "blocked":
            counters["executions_g54_backfill_blocked"] += 1

        counters["executions_final_eligible"] += int(_int(row.get("final_eligible")) == 1)
        counters["executions_simulation_eligible"] += int(_int(row.get("simulation_eligible")) == 1)
        counters["executions_risk_analysis_eligible"] += int(_int(row.get("risk_analysis_eligible")) == 1)
        funding_state = str(row.get("funding_state") or "").strip().lower()
        funding_recovery = str(row.get("funding_recovery_status") or "").strip().lower()
        counters["executions_funding_pending"] += int(funding_state == "pending")
        counters["executions_funding_zero_pending"] += int(funding_recovery == "pending_zero_confirmation")
        counters["executions_funding_manual_review"] += int(
            funding_state == "manual_review" or funding_recovery == "manual_review"
        )
        stale = _quality_gate_is_stale(row)
        counters["quality_gate_rows_stale"] += int(stale)
        counters["quality_gate_rows_current"] += int(not stale)
    return counters


async def export_statistics_period_zip(
    *,
    period_id: int | None = None,
    row_limit: int = DEFAULT_EXPORT_ROW_LIMIT,
    admin_export: bool = True,
    user_id: int | None = None,
) -> StatisticsZipExport:
    period = await _resolve_period(period_id)
    bounded = max(1, min(MAX_EXPORT_ROW_LIMIT, int(row_limit)))
    async with monitor_db_workload(stage="statistics_export"):
        dataset = await _load_period_dataset(period.id, row_limit=bounded, user_id=user_id)
        funding_rows, funding_total = await _fetch_period_funding(period.id, bounded, user_id=user_id)
        audit_rows, audit_total = await _fetch_period_quality_audit(period.id, bounded)

    signal_rows = [dict(row) for row in dataset.signal_rows]
    event_rows = [dict(row) for row in dataset.event_rows]
    execution_rows = [dict(row) for row in dataset.execution_rows]
    fill_rows = [dict(row) for row in dataset.fill_rows]
    if user_id is not None:
        scoped_signal_ids = {str(_int(row.get("id"))) for row in signal_rows}
        scoped_execution_ids = {str(_int(row.get("execution_id") or row.get("id"))) for row in execution_rows}
        scoped_audit_rows: list[dict[str, Any]] = []
        for row in audit_rows:
            entity_type = str(row.get("entity_type") or "").strip().lower()
            entity_id = str(row.get("entity_id") or "").strip()
            if (entity_type == "signal" and entity_id in scoped_signal_ids) or (
                entity_type == "execution" and entity_id in scoped_execution_ids
            ):
                scoped_audit_rows.append(row)
        audit_rows = scoped_audit_rows
        audit_total = len(audit_rows)
    if not admin_export:
        for rows in (execution_rows, fill_rows, funding_rows, audit_rows):
            for row in rows:
                row.pop("user_id", None)
                row.pop("actor_user_id", None)
    quality_rows = _calculated_quality_rows(dataset)
    for row in audit_rows:
        row["source"] = "durable_audit"
    quality_rows.extend(audit_rows)
    simulation_rows = _simulation_rows(dataset)
    collection_counts = _collection_integrity_counts(signal_rows, execution_rows)

    files: dict[str, bytes] = {
        "signals.csv": _csv_bytes(signal_rows, _columns(signal_rows, ("id", "period_id", "published_at"))),
        "signal_events.csv": _csv_bytes(event_rows, _columns(event_rows, ("id", "signal_id", "observed_at"))),
        "executions.csv": _csv_bytes(execution_rows, _columns(execution_rows, ("id", "execution_id", "period_id", "analytics_signal_id"))),
        "fills.csv": _csv_bytes(fill_rows, _columns(fill_rows, ("id", "execution_id", "fill_time"))),
        "funding.csv": _csv_bytes(funding_rows, _columns(funding_rows, ("id", "execution_id", "event_time"))),
        "quality.csv": _csv_bytes(quality_rows, _columns(quality_rows, ("source", "entity_type", "entity_id", "issue_code"))),
        "simulations.csv": _csv_bytes(simulation_rows, _columns(simulation_rows, ("scheme", "be_policy"))),
    }
    truncated = any(
        (
            _truncated(dataset),
            funding_total > len(funding_rows),
            audit_total > len(audit_rows),
        )
    )
    generated = datetime.now(timezone.utc)
    metadata_lines = [
        "ANTILUD BINGX STATISTICS EXPORT",
        f"schema_version={EXPORT_SCHEMA_VERSION}",
        f"app_version={__version__}",
        f"generated_at_utc={generated.isoformat()}",
        f"period_id={period.id}",
        f"period_name={period.name}",
        f"period_status={period.status}",
        f"period_kind={period.period_kind}",
        f"period_started_at={period.started_at.isoformat() if period.started_at else ''}",
        f"period_closed_at={period.closed_at.isoformat() if period.closed_at else ''}",
        f"period_source_version={period.source_version}",
        f"admin_export={str(bool(admin_export)).lower()}",
        f"account_scope_user_id={int(user_id) if user_id is not None else 'all'}",
        f"account_scope_policy={'requesting_admin_only' if user_id is not None else 'technical_all_accounts'}",
        f"row_limit={bounded}",
        f"truncated={str(truncated).lower()}",
        f"signals_total={dataset.total_signals}",
        f"signals_exported={len(signal_rows)}",
        f"events_exported={len(event_rows)}",
        f"executions_total={dataset.total_executions}",
        f"executions_exported={len(execution_rows)}",
        f"fills_total={dataset.total_fills}",
        f"fills_exported={len(fill_rows)}",
        f"funding_total={funding_total}",
        f"funding_exported={len(funding_rows)}",
        f"quality_audit_total={audit_total}",
        f"quality_rows_exported={len(quality_rows)}",
        f"simulations_exported={len(simulation_rows)}",
        f"simulations_with_eligible_rows={sum(_int(row.get('eligible_rows')) > 0 for row in simulation_rows)}",
        f"simulations_without_eligible_rows={sum(_int(row.get('eligible_rows')) <= 0 for row in simulation_rows)}",
        f"simulation_max_eligible_rows={max((_int(row.get('eligible_rows')) for row in simulation_rows), default=0)}",
        f"simulation_weighted_current_max_eligible_rows={max((_int(row.get('eligible_rows')) for row in simulation_rows if str(row.get('scheme') or '') == 'weighted_current'), default=0)}",
        *[f"{key}={value}" for key, value in collection_counts.items()],
        "collection_quality_policy=exact_fields_only_fail_closed",
        "money_policy=exchange_signed_fees_and_funding",
        "final_metrics_policy=FINAL_only_fail_closed",
        "simulation_scope_policy=per_scheme_fail_closed",
        "csv_encoding=UTF-8-BOM",
        "csv_formula_injection_protection=enabled",
        "secrets_included=false",
    ]
    files["metadata.txt"] = ("\n".join(metadata_lines) + "\n").encode("utf-8")

    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for filename in (
            "signals.csv",
            "signal_events.csv",
            "executions.csv",
            "fills.csv",
            "funding.csv",
            "quality.csv",
            "simulations.csv",
            "metadata.txt",
        ):
            archive.writestr(filename, files[filename])
    safe_name = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in period.name)[:50]
    filename = (
        f"antilud_statistics_user_{int(user_id)}_period_{period.id}_{safe_name}_{generated:%Y%m%d_%H%M%S}_utc.zip"
        if user_id is not None
        else f"antilud_statistics_period_{period.id}_{safe_name}_{generated:%Y%m%d_%H%M%S}_utc.zip"
    )
    file_rows = {
        "signals.csv": len(signal_rows),
        "signal_events.csv": len(event_rows),
        "executions.csv": len(execution_rows),
        "fills.csv": len(fill_rows),
        "funding.csv": len(funding_rows),
        "quality.csv": len(quality_rows),
        "simulations.csv": len(simulation_rows),
        "metadata.txt": len(metadata_lines),
    }
    return StatisticsZipExport(
        payload=output.getvalue(),
        filename=filename,
        period_id=period.id,
        period_name=period.name,
        file_rows=file_rows,
        truncated=truncated,
    )

def _quality_gate_is_stale(row: Mapping[str, Any]) -> bool:
    if _int(row.get("quality_gate_version")) < QUALITY_GATE_VERSION:
        return True
    evaluated = _dt(row.get("quality_evaluated_at"))
    updated = _dt(row.get("updated_at"))
    return evaluated is None or (updated is not None and evaluated < updated)


