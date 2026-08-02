from __future__ import annotations

import csv
import html
import io
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Mapping


@dataclass(frozen=True, slots=True)
class AnalyticsPeriod:
    key: str
    label: str
    delta: timedelta | None


ANALYTICS_PERIODS: dict[str, AnalyticsPeriod] = {
    "day": AnalyticsPeriod("day", "24 часа", timedelta(days=1)),
    "week": AnalyticsPeriod("week", "7 дней", timedelta(days=7)),
    "month": AnalyticsPeriod("month", "30 дней", timedelta(days=30)),
    "all": AnalyticsPeriod("all", "всё время", None),
}

JOB_LABELS = {
    "join": "Подписки/выходы",
    "reaction": "Реакции",
    "view": "Просмотры",
}


def resolve_analytics_period(
    key: str,
    *,
    now: datetime,
) -> tuple[AnalyticsPeriod, datetime | None]:
    """Resolve a strict period key and its inclusive UTC cutoff.

    Unknown values are rejected rather than silently widening the report to all
    history. This keeps stale or forged callback data fail-closed.
    """

    period = ANALYTICS_PERIODS.get(str(key).strip().lower())
    if period is None:
        raise ValueError("Неизвестный период аналитики")
    cutoff = now - period.delta if period.delta is not None else None
    return period, cutoff


def parse_analytics_period_callback(data: str | None, prefix: str) -> str:
    raw = str(data or "")
    if not raw.startswith(prefix):
        raise ValueError("Некорректная кнопка аналитики")
    period = raw[len(prefix) :].strip().lower()
    if period not in ANALYTICS_PERIODS:
        raise ValueError("Неизвестный период аналитики")
    return period


def success_rate(success: int, failed: int) -> float | None:
    total = max(0, int(success)) + max(0, int(failed))
    if total <= 0:
        return None
    return max(0, int(success)) * 100.0 / total


def format_rate(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:.1f}%"


def format_duration(seconds: float | int | None) -> str:
    if seconds is None:
        return "—"
    value = max(0, int(round(float(seconds))))
    if value < 60:
        return f"{value} сек."
    if value < 3600:
        minutes, remainder = divmod(value, 60)
        return f"{minutes} мин. {remainder:02d} сек."
    hours, remainder = divmod(value, 3600)
    minutes = remainder // 60
    return f"{hours} ч. {minutes:02d} мин."


def _safe_int(value: object) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _csv_safe_text(value: object) -> str:
    """Normalize one text cell and neutralize spreadsheet formula injection."""

    text = " ".join(str(value or "").split())
    if text.lstrip().startswith(("=", "+", "-", "@")):
        return "'" + text
    return text


def _rank_lines(rows: list[Mapping[str, object]], *, empty: str) -> str:
    if not rows:
        return empty
    rendered: list[str] = []
    for index, row in enumerate(rows, 1):
        raw_name = " ".join(str(row.get("name") or "Без названия").split())
        if len(raw_name) > 72:
            raw_name = raw_name[:71] + "…"
        name = html.escape(raw_name)
        success = _safe_int(row.get("success"))
        failed = _safe_int(row.get("failed"))
        rate = success_rate(success, failed)
        pending = _safe_int(row.get("pending"))
        running = _safe_int(row.get("running"))
        load_text = f" · очередь {pending}/{running}" if pending or running else ""
        rendered.append(
            f"{index}. <b>{name}</b> — ✅ {success} · ❌ {failed} · {format_rate(rate)}{load_text}"
        )
    return "\n".join(rendered)


def render_analytics_overview(snapshot: Mapping[str, object]) -> str:
    period_label = html.escape(str(snapshot.get("period_label") or "—"))
    generated_at = snapshot.get("generated_at")
    generated_text = (
        generated_at.strftime("%d.%m.%Y %H:%M")
        if isinstance(generated_at, datetime)
        else "—"
    )
    current = snapshot.get("current") or {}
    totals = snapshot.get("totals") or {}
    jobs = snapshot.get("jobs") or {}
    top_accounts = list(snapshot.get("top_accounts") or [])[:3]
    top_targets = list(snapshot.get("top_targets") or [])[:3]

    job_lines: list[str] = []
    for kind in ("join", "reaction", "view"):
        item = jobs.get(kind, {}) if isinstance(jobs, Mapping) else {}
        success = _safe_int(item.get("success"))
        failed = _safe_int(item.get("failed"))
        cancelled = _safe_int(item.get("cancelled"))
        job_lines.append(
            f"{JOB_LABELS[kind]}: ✅ <b>{success}</b> · ❌ {failed} · отменено {cancelled} · "
            f"успех {format_rate(success_rate(success, failed))}"
        )

    timing = totals.get("timing", {}) if isinstance(totals, Mapping) else {}
    daily = list(snapshot.get("daily") or [])
    trend_lines: list[str] = []
    for row in daily[-7:]:
        date_value = row.get("date")
        if hasattr(date_value, "strftime"):
            date_text = date_value.strftime("%d.%m")
        else:
            raw_date = str(date_value or "")
            if len(raw_date) >= 10 and raw_date[4:5] == "-" and raw_date[7:8] == "-":
                date_text = f"{raw_date[8:10]}.{raw_date[5:7]}"
            else:
                date_text = raw_date
        trend_lines.append(
            f"{date_text}: ✅ {_safe_int(row.get('success'))} · ❌ {_safe_int(row.get('failed'))}"
        )

    coverage_note = str(snapshot.get("coverage_note") or "").strip()
    coverage_block = f"\n\nℹ️ <i>{html.escape(coverage_note)}</i>" if coverage_note else ""
    trend_block = (
        "\n\n📅 <b>Последние 7 дней с данными</b>\n" + "\n".join(trend_lines)
        if trend_lines
        else ""
    )

    return (
        "📊 <b>Расширенная аналитика LikeBot</b>\n"
        f"<i>Период: {period_label} · обновлено {generated_text} UTC</i>\n\n"
        "━━━━━━━━━━━━━━\n"
        "📌 <b>Итог</b>\n"
        f"Терминальных задач: <b>{_safe_int(totals.get('terminal'))}</b>\n"
        f"Успешно: <b>{_safe_int(totals.get('success'))}</b> · ошибок: <b>{_safe_int(totals.get('failed'))}</b> · "
        f"отменено: <b>{_safe_int(totals.get('cancelled'))}</b>\n"
        f"Общий успех: <b>{format_rate(_safe_float(totals.get('success_rate')))}</b>\n"
        f"Сейчас в очереди: <b>{_safe_int(current.get('pending'))}</b> · выполняются: <b>{_safe_int(current.get('running'))}</b>\n\n"
        "⚙️ <b>По типам</b>\n"
        + "\n".join(job_lines)
        + "\n\n⏱ <b>Скорость обработки</b>\n"
        f"Среднее опоздание запуска: <b>{format_duration(_safe_float(timing.get('queue_lag_seconds')))}</b>\n"
        f"Среднее выполнение задачи: <b>{format_duration(_safe_float(timing.get('execution_seconds')))}</b>\n\n"
        "👥 <b>Аккаунты</b>\n"
        f"Активны: <b>{_safe_int(current.get('active_accounts'))}</b> / {_safe_int(current.get('accounts'))} · "
        f"проблемные: {_safe_int(current.get('problem_accounts'))}\n"
        + _rank_lines(top_accounts, empty="Данных за период пока нет.")
        + "\n\n📢 <b>Каналы и группы</b>\n"
        f"Активные цели: <b>{_safe_int(current.get('active_targets'))}</b> / {_safe_int(current.get('targets'))}\n"
        + _rank_lines(top_targets, empty="Данных за период пока нет.")
        + trend_block
        + coverage_block
        + "\n━━━━━━━━━━━━━━"
    )


def render_analytics_ranking(
    snapshot: Mapping[str, object],
    *,
    ranking: str,
) -> str:
    if ranking not in {"accounts", "targets"}:
        raise ValueError("Неизвестный рейтинг аналитики")
    title = "👥 Аккаунты" if ranking == "accounts" else "📢 Каналы и группы"
    rows = list(snapshot.get(f"top_{ranking}") or [])
    problem_rows = list(snapshot.get(f"problem_{ranking}") or [])[:5]
    period_label = html.escape(str(snapshot.get("period_label") or "—"))
    lines = _rank_lines(rows, empty="Данных за выбранный период пока нет.")
    problems = _rank_lines(problem_rows, empty="Ошибок за выбранный период нет.")
    return (
        f"{title} — <b>рейтинг</b>\n"
        f"<i>Период: {period_label}</i>\n\n"
        "🏆 <b>По успешным действиям</b>\n"
        f"{lines}\n\n"
        "⚠️ <b>Больше всего ошибок</b>\n"
        f"{problems}\n\n"
        "ℹ️ Успех считается только по завершённым и ошибочным задачам; отменённые не влияют на процент. "
        "Очередь показана как pending/running."
    )


def build_analytics_csv(snapshot: Mapping[str, object]) -> bytes:
    """Build an Excel-friendly UTF-8 CSV without account credentials or links."""

    stream = io.StringIO(newline="")
    writer = csv.writer(stream, delimiter=";")
    writer.writerow(["LikeBot analytics"])
    writer.writerow(["Период", str(snapshot.get("period_label") or "")])
    generated_at = snapshot.get("generated_at")
    writer.writerow(
        [
            "Сформировано UTC",
            generated_at.isoformat(timespec="seconds") if isinstance(generated_at, datetime) else "",
        ]
    )
    writer.writerow([])

    totals = snapshot.get("totals") or {}
    current = snapshot.get("current") or {}
    writer.writerow(["Сводка", "Значение"])
    writer.writerow(["Успешно", _safe_int(totals.get("success"))])
    writer.writerow(["Ошибки", _safe_int(totals.get("failed"))])
    writer.writerow(["Отменено", _safe_int(totals.get("cancelled"))])
    writer.writerow(["Всего терминальных", _safe_int(totals.get("terminal"))])
    writer.writerow(["Успех, %", format_rate(_safe_float(totals.get("success_rate")))])
    writer.writerow(["Ожидают сейчас", _safe_int(current.get("pending"))])
    writer.writerow(["Выполняются сейчас", _safe_int(current.get("running"))])
    writer.writerow([])

    writer.writerow(
        [
            "Тип",
            "Успешно",
            "Ошибки",
            "Отменено",
            "Успех, %",
            "Среднее опоздание, сек.",
            "Среднее выполнение, сек.",
            "Выборка времени",
        ]
    )
    jobs = snapshot.get("jobs") or {}
    for kind in ("join", "reaction", "view"):
        item = jobs.get(kind, {}) if isinstance(jobs, Mapping) else {}
        writer.writerow(
            [
                JOB_LABELS[kind],
                _safe_int(item.get("success")),
                _safe_int(item.get("failed")),
                _safe_int(item.get("cancelled")),
                format_rate(success_rate(_safe_int(item.get("success")), _safe_int(item.get("failed")))),
                round(_safe_float(item.get("avg_queue_lag_seconds")) or 0.0, 3),
                round(_safe_float(item.get("avg_execution_seconds")) or 0.0, 3),
                _safe_int(item.get("timing_samples")),
            ]
        )
    writer.writerow([])

    writer.writerow(["Рейтинг аккаунтов"])
    writer.writerow(["Место", "Аккаунт", "Успешно", "Ошибки", "Отменено", "Pending", "Running", "Успех, %"])
    for index, row in enumerate(list(snapshot.get("top_accounts") or []), 1):
        success = _safe_int(row.get("success"))
        failed = _safe_int(row.get("failed"))
        writer.writerow(
            [
                index,
                _csv_safe_text(row.get("name") or "Без названия"),
                success,
                failed,
                _safe_int(row.get("cancelled")),
                _safe_int(row.get("pending")),
                _safe_int(row.get("running")),
                format_rate(success_rate(success, failed)),
            ]
        )
    writer.writerow([])

    writer.writerow(["Рейтинг каналов и групп"])
    writer.writerow(["Место", "Название", "Тип", "Успешно", "Ошибки", "Отменено", "Pending", "Running", "Успех, %"])
    for index, row in enumerate(list(snapshot.get("top_targets") or []), 1):
        success = _safe_int(row.get("success"))
        failed = _safe_int(row.get("failed"))
        writer.writerow(
            [
                index,
                _csv_safe_text(row.get("name") or "Без названия"),
                "Группа" if row.get("kind") == "group" else "Канал",
                success,
                failed,
                _safe_int(row.get("cancelled")),
                _safe_int(row.get("pending")),
                _safe_int(row.get("running")),
                format_rate(success_rate(success, failed)),
            ]
        )
    writer.writerow([])

    writer.writerow(["Динамика по дням"])
    writer.writerow(["Дата UTC", "Успешно", "Ошибки", "Отменено", "Всего"])
    for row in list(snapshot.get("daily") or []):
        writer.writerow(
            [
                str(row.get("date") or ""),
                _safe_int(row.get("success")),
                _safe_int(row.get("failed")),
                _safe_int(row.get("cancelled")),
                _safe_int(row.get("terminal")),
            ]
        )

    coverage_note = str(snapshot.get("coverage_note") or "").strip()
    if coverage_note:
        writer.writerow([])
        writer.writerow(["Примечание", coverage_note])
    return ("\ufeff" + stream.getvalue()).encode("utf-8")
