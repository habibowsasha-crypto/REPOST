"""Read-only admin reports and CSV export for source-signal analytics.

Reports run only on explicit administrator requests. They never execute inside
signal parsing, public-price evaluation, trade execution or protection loops.
"""

from __future__ import annotations

import csv
import html
import io
import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from app.database.db import connect, is_postgres, monitor_db_workload
from app.services.signal_analytics_ingress import signal_analytics_dispatcher_stats

_MAX_EXPORT_ROWS = 50_000

_REPORT_COLUMNS = """
id,published_at,last_seen_at,source_chat_id,source_title,signal_id_text,
symbol,side,order_type,source_format,timeframe,strategy,source_leverage,
entry_low,entry_high,entry_reference,stop_price,targets_json,
target_percents_json,target_percents_source,status,duplicate_count,needs_recovery,recovery_status,
recovery_method,recovery_attempts,recovery_next_attempt_at,recovery_completed_at,
recovery_confidence,recovery_last_error,recovery_cursor_at,
data_quality_status,data_quality_reason,
tracking_started_at,expiry_at,zone_touched_at,activated_at,activated_price,max_tp_index,
be_trigger_tp_index,be_armed_at,completed_at,terminal_reason,
ambiguous_reason,last_observed_at,last_observed_price,state_version
""".replace("\n", "")


@dataclass(frozen=True, slots=True)
class SignalAnalyticsCsvExport:
    payload: bytes
    rows: int
    total: int
    truncated: bool


@dataclass(frozen=True, slots=True)
class SignalAnalyticsSummary:
    unique_signals: int
    total_messages: int
    duplicates: int
    waiting: int
    active: int
    completed: int
    expired_not_entered: int
    ambiguous: int
    untracked: int
    recovery_pending: int
    activated_total: int
    tp1: int
    tp2: int
    tp3: int
    tp4: int
    completed_be: int
    be_after_tp1: int
    be_after_tp2: int
    be_after_tp3_plus: int
    stop_no_tp: int
    stop_after_tp: int
    all_targets: int
    other_completed: int
    long_count: int
    short_count: int
    ema_cross: int
    macd_cross: int
    active_no_tp: int
    active_tp1: int
    active_tp2: int
    active_tp3_plus: int
    first_signal_at: datetime | None
    last_signal_at: datetime | None
    status_counts: dict[str, int]


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
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


async def _count_signal_rows() -> int:
    query = "SELECT COUNT(*) AS total FROM signal_analytics_signals"
    async with connect() as conn:
        if is_postgres():
            value = await conn.fetchval(query)
        else:
            cursor = await conn.execute(query)
            row = await cursor.fetchone()
            value = row[0] if row else 0
    return max(0, _int(value))


async def _fetch_signal_rows(
    *, limit: int | None = None, latest: bool = False
) -> list[dict[str, Any]]:
    order = "DESC" if latest else "ASC"
    query = (
        f"SELECT {_REPORT_COLUMNS} FROM signal_analytics_signals "
        f"ORDER BY published_at {order},id {order}"
    )
    args: tuple[Any, ...] = ()
    if limit is not None:
        bounded = max(1, min(_MAX_EXPORT_ROWS, int(limit)))
        query += " LIMIT $1" if is_postgres() else " LIMIT ?"
        args = (bounded,)
    async with connect() as conn:
        if is_postgres():
            rows = await conn.fetch(query, *args)
        else:
            cursor = await conn.execute(query, args)
            rows = await cursor.fetchall()
    result = [_as_dict(row) for row in rows]
    # Export chooses the newest bounded slice but presents it chronologically.
    if latest:
        result.reverse()
    return result


async def _fetch_level_event_rows(
    signal_ids: Iterable[int],
) -> list[dict[str, Any]]:
    """Fetch only events that can appear in the current bounded export."""

    ids = sorted({int(value) for value in signal_ids if int(value) > 0})
    if not ids:
        return []
    columns = (
        "id,signal_id,event_key,event_type,level_index,observed_at,"
        "observed_price"
    )
    materialized: list[dict[str, Any]] = []
    async with connect() as conn:
        if is_postgres():
            rows = await conn.fetch(
                f"SELECT {columns} FROM signal_analytics_level_events "
                "WHERE signal_id = ANY($1::bigint[]) "
                "ORDER BY signal_id ASC,observed_at ASC,id ASC",
                ids,
            )
            return [_as_dict(row) for row in rows]

        # Stay below SQLite's common 999-bind-variable limit.
        for offset in range(0, len(ids), 500):
            chunk = ids[offset : offset + 500]
            placeholders = ",".join("?" for _ in chunk)
            cursor = await conn.execute(
                f"SELECT {columns} FROM signal_analytics_level_events "
                f"WHERE signal_id IN ({placeholders}) "
                "ORDER BY signal_id ASC,observed_at ASC,id ASC",
                tuple(chunk),
            )
            materialized.extend(_as_dict(row) for row in await cursor.fetchall())
    materialized.sort(
        key=lambda row: (
            _int(row.get("signal_id")),
            str(row.get("observed_at") or ""),
            _int(row.get("id")),
        )
    )
    return materialized


def summarize_signal_analytics_rows(
    rows: Iterable[dict[str, Any]],
) -> SignalAnalyticsSummary:
    materialized = list(rows)
    statuses: Counter[str] = Counter()
    total_messages = 0
    tp_counts = [0, 0, 0, 0]
    long_count = 0
    short_count = 0
    ema_cross = 0
    macd_cross = 0
    completed_be = 0
    stop_no_tp = 0
    all_targets = 0
    active_no_tp = 0
    active_tp1 = 0
    active_tp2 = 0
    active_tp3_plus = 0
    recovery_pending = 0
    activated_total = 0
    be_after_tp1 = 0
    be_after_tp2 = 0
    be_after_tp3_plus = 0
    stop_after_tp = 0
    dates: list[datetime] = []

    for row in materialized:
        status = str(row.get("status") or "unknown").strip().lower()
        needs_recovery = bool(_int(row.get("needs_recovery")))
        if needs_recovery and status in {"waiting_entry", "active"}:
            statuses["recovery_pending"] += 1
            recovery_pending += 1
        else:
            statuses[status] += 1
        duplicate_count = max(1, _int(row.get("duplicate_count")))
        total_messages += duplicate_count
        max_tp = max(0, _int(row.get("max_tp_index")))
        for index in range(1, 5):
            if max_tp >= index:
                tp_counts[index - 1] += 1

        side = str(row.get("side") or "").strip().lower()
        long_count += int(side == "long")
        short_count += int(side == "short")
        strategy = str(row.get("strategy") or "").strip().lower()
        ema_cross += int("ema" in strategy and "cross" in strategy)
        macd_cross += int("macd" in strategy and "cross" in strategy)

        completed_be += int(status == "completed_be")
        terminal_reason = str(row.get("terminal_reason") or "").strip().lower()
        if status == "completed_be":
            if terminal_reason == "be_after_tp1":
                be_after_tp1 += 1
            elif terminal_reason == "be_after_tp2":
                be_after_tp2 += 1
            elif terminal_reason.startswith("be_after_tp"):
                be_after_tp3_plus += 1
            elif max_tp == 1:
                be_after_tp1 += 1
            elif max_tp == 2:
                be_after_tp2 += 1
            else:
                be_after_tp3_plus += 1
        stop_no_tp += int(
            status == "completed_stop"
            and (max_tp == 0 or terminal_reason == "stop_no_tp")
        )
        stop_after_tp += int(status == "completed_stop" and max_tp > 0)
        all_targets += int(status == "completed_tp")
        if row.get("activated_at") not in (None, "") or status in {
            "active",
            "completed_tp",
            "completed_be",
            "completed_stop",
        }:
            activated_total += 1

        if status == "active" and not needs_recovery:
            if max_tp <= 0:
                active_no_tp += 1
            elif max_tp == 1:
                active_tp1 += 1
            elif max_tp == 2:
                active_tp2 += 1
            else:
                active_tp3_plus += 1

        published = _dt(row.get("published_at"))
        if published is not None:
            dates.append(published)

    unique = len(materialized)
    completed = sum(
        statuses[name]
        for name in ("completed_tp", "completed_be", "completed_stop")
    )
    other_completed = max(
        0,
        completed
        - completed_be
        - stop_no_tp
        - stop_after_tp
        - all_targets,
    )
    untracked = statuses["shadow_received"] + statuses["unknown"]
    return SignalAnalyticsSummary(
        unique_signals=unique,
        total_messages=total_messages,
        duplicates=max(0, total_messages - unique),
        waiting=statuses["waiting_entry"],
        active=statuses["active"],
        completed=completed,
        expired_not_entered=statuses["expired_not_entered"],
        ambiguous=statuses["ambiguous"],
        untracked=untracked,
        recovery_pending=recovery_pending,
        activated_total=activated_total,
        tp1=tp_counts[0],
        tp2=tp_counts[1],
        tp3=tp_counts[2],
        tp4=tp_counts[3],
        completed_be=completed_be,
        be_after_tp1=be_after_tp1,
        be_after_tp2=be_after_tp2,
        be_after_tp3_plus=be_after_tp3_plus,
        stop_no_tp=stop_no_tp,
        stop_after_tp=stop_after_tp,
        all_targets=all_targets,
        other_completed=other_completed,
        long_count=long_count,
        short_count=short_count,
        ema_cross=ema_cross,
        macd_cross=macd_cross,
        active_no_tp=active_no_tp,
        active_tp1=active_tp1,
        active_tp2=active_tp2,
        active_tp3_plus=active_tp3_plus,
        first_signal_at=min(dates) if dates else None,
        last_signal_at=max(dates) if dates else None,
        status_counts=dict(statuses),
    )


async def get_signal_analytics_summary() -> SignalAnalyticsSummary:
    async with monitor_db_workload(stage="analytics_report"):
        return summarize_signal_analytics_rows(await _fetch_signal_rows())


def _fmt_dt(value: datetime | None) -> str:
    if value is None:
        return "—"
    # Project/user-facing reports use UTC+3 without an external tz database.
    local = value.astimezone(timezone(timedelta(hours=3)))
    return f"{local:%d.%m.%Y %H:%M} МСК"


def _count_percent(count: int, total: int) -> str:
    if total <= 0:
        return f"<b>{count}</b>"
    return f"<b>{count}</b> из {total} — <b>{(count / total) * 100:.1f}%</b>"


def format_signal_analytics_summary(
    summary: SignalAnalyticsSummary,
    *, health: dict[str, int] | None = None,
) -> str:
    runtime = dict(health or signal_analytics_dispatcher_stats())
    tracked_total = (
        summary.waiting
        + summary.active
        + summary.completed
        + summary.expired_not_entered
        + summary.ambiguous
        + summary.untracked
        + summary.recovery_pending
    )
    mismatch = summary.unique_signals - tracked_total
    enabled = bool(runtime.get("enabled"))
    ingress_enabled = bool(runtime.get("ingress_enabled"))
    tracking_enabled = bool(runtime.get("tracking_enabled"))
    price_loop_enabled = bool(runtime.get("price_loop_enabled", 1))
    worker_ok = bool(runtime.get("worker"))
    collection_ok = (
        enabled
        and ingress_enabled
        and worker_ok
        and not runtime.get("db_errors")
        and not runtime.get("dropped")
    )
    tracking_ok = (
        enabled
        and tracking_enabled
        and price_loop_enabled
        and bool(runtime.get("tracker_started"))
        and not runtime.get("tracking_dropped")
    )
    if not enabled or not ingress_enabled:
        collection_label = "выключен"
        health_icon = "⏸"
    elif collection_ok:
        collection_label = "работает"
        health_icon = "✅"
    else:
        collection_label = "требует проверки"
        health_icon = "⚠️"
    worker_icon = "✅" if worker_ok else "⏸"
    if not enabled or not tracking_enabled:
        tracking_icon = "⏸"
        tracking_label = "выключено"
    elif not price_loop_enabled:
        tracking_icon = "⚠️"
        tracking_label = "нет публичного price-loop"
    elif tracking_ok:
        tracking_icon = "✅"
        tracking_label = "активно"
    else:
        tracking_icon = "⚠️"
        tracking_label = "требует проверки"

    lines = [
        "<b>📊 АНАЛИТИКА СИГНАЛОВ</b>",
        "",
        "<b>📥 Входящие данные</b>",
        f"Уникальных сигналов: <b>{summary.unique_signals}</b>",
        f"Всего сообщений: <b>{summary.total_messages}</b>",
        f"Дубликатов: <b>{summary.duplicates}</b>",
        "",
        "<b>📌 Состояние сигналов</b>",
        f"⏳ Ожидают входа: <b>{summary.waiting}</b>",
        f"🟢 Активных: <b>{summary.active}</b>",
        f"✅ Завершённых: <b>{summary.completed}</b>",
        f"⚪ Не активировались: <b>{summary.expired_not_entered}</b>",
        f"⚠️ Неоднозначных: <b>{summary.ambiguous}</b>",
    ]
    if summary.recovery_pending:
        lines.append(
            f"🧩 Требуют восстановления/проверки: "
            f"<b>{summary.recovery_pending}</b>"
        )
        lines.append(
            "⚠️ TP/SL/Б/У ниже показаны только по подтверждённым наблюдениям; "
            "сигналы на восстановлении в итог не угадываются."
        )
    if summary.untracked:
        lines.append(f"🗂 Только сохранены до tracking: <b>{summary.untracked}</b>")
    if mismatch:
        lines.append(f"🚨 Расхождение состояний: <b>{mismatch:+d}</b>")

    lines.extend(
        [
            "",
            "<b>🎯 Достижение целей</b>",
            f"Активировались: <b>{summary.activated_total}</b>",
            f"TP1 достигли: {_count_percent(summary.tp1, summary.activated_total)}",
            f"TP2 достигли: {_count_percent(summary.tp2, summary.activated_total)}",
            f"TP3 достигли: {_count_percent(summary.tp3, summary.activated_total)}",
            f"TP4 достигли: {_count_percent(summary.tp4, summary.activated_total)}",
            "",
            "<b>🏁 Итог завершённых</b>",
            f"❌ STOP без TP: <b>{summary.stop_no_tp}</b>",
            f"🛡 Выбило в Б/У по цене: <b>{summary.completed_be}</b>",
            f"　├ после TP1: <b>{summary.be_after_tp1}</b>",
            f"　├ после TP2: <b>{summary.be_after_tp2}</b>",
            f"　└ после TP3+: <b>{summary.be_after_tp3_plus}</b>",
            f"↩️ STOP после частичных TP: <b>{summary.stop_after_tp}</b>",
            f"✅ Все цели выполнены: <b>{summary.all_targets}</b>",
        ]
    )
    if summary.other_completed:
        lines.append(f"🧩 Другой завершённый исход: <b>{summary.other_completed}</b>")

    if summary.active:
        lines.extend(
            [
                "",
                "<b>🟢 Активные сейчас</b>",
                f"Без TP: <b>{summary.active_no_tp}</b>",
                f"После TP1: <b>{summary.active_tp1}</b>",
                f"После TP2: <b>{summary.active_tp2}</b>",
                f"После TP3+: <b>{summary.active_tp3_plus}</b>",
            ]
        )

    lines.extend(
        [
            "",
            "<b>↕️ Направление</b>",
            f"LONG: <b>{summary.long_count}</b> | SHORT: <b>{summary.short_count}</b>",
            "",
            "<b>⚙️ Стратегии</b>",
            f"EMA Cross: <b>{summary.ema_cross}</b>",
            f"MACD Cross: <b>{summary.macd_cross}</b>",
            "",
            f"Первый сигнал: {html.escape(_fmt_dt(summary.first_signal_at))}",
            f"Последний сигнал: {html.escape(_fmt_dt(summary.last_signal_at))}",
            "",
            "<b>🩺 Сбор данных</b>",
            f"{health_icon} Сбор входящих: {collection_label}",
            f"{worker_icon} DB-worker: {'активен' if worker_ok else 'не запущен'}",
            f"{tracking_icon} Отслеживание цены: {tracking_label}",
            f"Очередь: <b>{_int(runtime.get('queued'))}/{_int(runtime.get('queue_max'))}</b>",
            f"Ошибок БД: <b>{_int(runtime.get('db_errors'))}</b>",
            f"Пропущено: <b>{_int(runtime.get('dropped')) + _int(runtime.get('tracking_dropped'))}</b>",
            "",
            "ℹ️ Уровни фиксируются по выборочным публичным last-price снимкам. "
            "Интервалы после перезапуска не угадываются и помечаются для восстановления.",
        ]
    )
    if _int(runtime.get("tracking_out_of_order_snapshots")):
        lines.append(
            "Старых снимков цены пропущено: "
            f"<b>{_int(runtime.get('tracking_out_of_order_snapshots'))}</b>"
        )
    if _int(runtime.get("tracking_capacity_overflow")):
        lines.append(
            "🚨 Превышен лимит активного реестра: "
            f"<b>{_int(runtime.get('tracking_capacity_overflow'))}</b>"
        )
    return "\n".join(lines)


def _csv_safe_text(value: Any) -> str:
    """Prevent spreadsheet formula execution from untrusted text fields."""

    text = str(value or "")
    if text.startswith(("=", "+", "-", "@", "\t", "\r", "\n")):
        return "'" + text
    return text


def _targets_for_export(value: Any) -> list[str]:
    try:
        parsed = json.loads(str(value or "[]"))
    except (TypeError, ValueError):
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed]


async def export_signal_analytics_csv_bundle(
    *, limit: int = _MAX_EXPORT_ROWS
) -> SignalAnalyticsCsvExport:
    bounded = max(1, min(_MAX_EXPORT_ROWS, int(limit)))
    async with monitor_db_workload(stage="analytics_report"):
        total = await _count_signal_rows()
        rows = await _fetch_signal_rows(limit=bounded, latest=True)
        exported_ids = {_int(row.get("id")) for row in rows if _int(row.get("id")) > 0}
        event_map: dict[int, dict[str, dict[str, Any]]] = {}
        if exported_ids:
            for event in await _fetch_level_event_rows(exported_ids):
                signal_id = _int(event.get("signal_id"))
                if signal_id not in exported_ids:
                    continue
                key = str(event.get("event_key") or "").strip().upper()
                if not key:
                    continue
                # Event keys are unique per signal in the schema. setdefault keeps
                # the earliest row if legacy/manual data contains a duplicate.
                event_map.setdefault(signal_id, {}).setdefault(key, event)
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer)
    writer.writerow(
        [
            "analytics_id",
            "published_at_utc",
            "last_seen_at_utc",
            "source_chat_id",
            "source_title",
            "signal_id",
            "symbol",
            "side",
            "order_type",
            "timeframe",
            "strategy",
            "source_leverage",
            "entry_low",
            "entry_high",
            "entry_reference",
            "stop_price",
            "tp1",
            "tp2",
            "tp3",
            "tp4",
            "status",
            "max_tp_index",
            "be_trigger_tp_index",
            "terminal_reason",
            "entry_zone_at",
            "entry_at",
            "tp1_reached_at",
            "tp2_reached_at",
            "tp3_reached_at",
            "tp4_reached_at",
            "be_armed_event_at",
            "breakeven_at",
            "stop_at",
            "expired_at",
            "ambiguous_event_at",
            "duplicate_count",
            "needs_recovery",
            "zone_touched_at",
            "activated_at",
            "activated_price",
            "be_armed_at",
            "completed_at",
            "ambiguous_reason",
            "last_observed_at",
            "last_observed_price",
        ]
    )
    for row in rows:
        signal_id = _int(row.get("id"))
        events = event_map.get(signal_id, {})

        def event_at(key: str) -> Any:
            return events.get(key, {}).get("observed_at", "")

        targets = _targets_for_export(row.get("targets_json"))
        targets += [""] * max(0, 4 - len(targets))
        writer.writerow(
            [
                row.get("id"),
                row.get("published_at"),
                row.get("last_seen_at"),
                row.get("source_chat_id"),
                _csv_safe_text(row.get("source_title")),
                _csv_safe_text(row.get("signal_id_text")),
                _csv_safe_text(row.get("symbol")),
                _csv_safe_text(row.get("side")),
                _csv_safe_text(row.get("order_type")),
                _csv_safe_text(row.get("timeframe")),
                _csv_safe_text(row.get("strategy")),
                row.get("source_leverage"),
                row.get("entry_low"),
                row.get("entry_high"),
                row.get("entry_reference"),
                row.get("stop_price"),
                *targets[:4],
                _csv_safe_text(row.get("status")),
                row.get("max_tp_index"),
                row.get("be_trigger_tp_index"),
                _csv_safe_text(row.get("terminal_reason")),
                event_at("ENTRY_ZONE"),
                event_at("ENTRY"),
                event_at("TP1"),
                event_at("TP2"),
                event_at("TP3"),
                event_at("TP4"),
                event_at("BE_ARMED"),
                event_at("BREAKEVEN"),
                event_at("STOP"),
                event_at("EXPIRED"),
                event_at("AMBIGUOUS"),
                row.get("duplicate_count"),
                row.get("needs_recovery"),
                row.get("zone_touched_at"),
                row.get("activated_at"),
                row.get("activated_price"),
                row.get("be_armed_at"),
                row.get("completed_at"),
                _csv_safe_text(row.get("ambiguous_reason")),
                row.get("last_observed_at"),
                row.get("last_observed_price"),
            ]
        )
    # UTF-8 BOM lets Excel open Russian column/data text without manual import.
    payload = ("\ufeff" + buffer.getvalue()).encode("utf-8")
    return SignalAnalyticsCsvExport(
        payload=payload,
        rows=len(rows),
        total=total,
        truncated=total > len(rows),
    )


async def export_signal_analytics_csv(*, limit: int = _MAX_EXPORT_ROWS) -> bytes:
    """Backward-compatible bytes-only export helper."""

    return (await export_signal_analytics_csv_bundle(limit=limit)).payload
