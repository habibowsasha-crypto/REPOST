"""Fail-closed statistics data-quality validation for plan step 7.

The module is intentionally pure and read-only.  It validates already-durable
SIGNAL, level-event, EXECUTION and FILL rows, produces explicit denominators and
quarantine rows, and never performs exchange requests or mutates trading state.

A later command layer can persist the generated audit/quarantine rows through
``statistics_quality_store``.  Keeping validation pure makes the same input
produce the same issues and prevents a report from silently repairing history.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping, Sequence


_ZERO = Decimal("0")
_HUNDRED = Decimal("100")
_TOLERANCE = Decimal("0.00000001")
_VOLUME_REL_TOLERANCE = Decimal("0.000000000001")
_SIGNAL_TERMINAL = frozenset({"completed_stop", "completed_be", "completed_tp"})
_BAD_QUALITY = frozenset(
    {"ambiguous", "unavailable", "needs_recovery", "quarantined", "recovery_required"}
)
_FINAL_VOLUME = frozenset({"exact", "within_tolerance"})
_CLOSE_ROLES = frozenset({"TP", "FINAL_TP", "STOP", "BE_STOP"})


@dataclass(frozen=True, slots=True)
class QualityIssue:
    entity_type: str
    entity_id: str
    issue_code: str
    severity: str
    reason: str
    recoverable: bool = False
    related_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class QualityEntitySummary:
    total_rows: int
    complete_rows: int = 0
    provisional_rows: int = 0
    needs_recovery_rows: int = 0
    ambiguous_rows: int = 0
    unavailable_rows: int = 0
    legacy_rows: int = 0
    quarantined_rows: int = 0
    linked_rows: int = 0
    unlinked_rows: int = 0
    final_rows: int = 0
    recovered_after_restart_rows: int = 0
    recovery_unresolved_rows: int = 0
    duplicate_observations: int = 0


@dataclass(frozen=True, slots=True)
class QuarantineRow:
    entity_type: str
    entity_id: str
    issue_code: str
    severity: str
    recoverable: bool
    reason: str
    related_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class StatisticsQualityReport:
    signal_summary: QualityEntitySummary
    execution_summary: QualityEntitySummary
    fill_summary: QualityEntitySummary
    issues: tuple[QualityIssue, ...]
    quarantine_rows: tuple[QuarantineRow, ...]
    issue_counts: dict[str, int] = field(default_factory=dict)
    linked_signal_coverage_percent: Decimal | None = None
    final_financial_coverage_percent: Decimal | None = None
    trust_status: str = "empty"
    trust_reasons: tuple[str, ...] = ()


def _text(value: Any) -> str:
    return str(value or "").strip()


def _lower(value: Any) -> str:
    return _text(value).lower()


def _upper(value: Any) -> str:
    return _text(value).upper()


def _int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def _decimal(value: Any) -> Decimal | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        if " " in text and "T" not in text:
            text = text.replace(" ", "T", 1)
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _canonical_symbol(value: Any) -> str:
    return _upper(value).replace("-", "").replace("_", "").replace("/", "")


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if value in (None, ""):
        return []
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _percent(numerator: int, denominator: int) -> Decimal | None:
    if denominator <= 0:
        return None
    return Decimal(numerator) * _HUNDRED / Decimal(denominator)


def _within(left: Decimal | None, right: Decimal | None, tolerance: Decimal = _TOLERANCE) -> bool:
    if left is None or right is None:
        return False
    return (left - right).copy_abs() <= tolerance


def _fill_gross_total(rows: Sequence[Mapping[str, Any]]) -> tuple[Decimal, str]:
    """Return the same gross-PnL aggregate used by financial reconciliation.

    BingX live fillHistory may omit a normalized realizedPnl field even though
    exact price, quantity and role are present.  Such rows are durably marked
    ``derived_from_fill_prices``.  Summing their stored zero realized_pnl values
    creates a false quality quarantine, so mirror the production linear-contract
    notional calculation for those rows.
    """

    exchange_total = _ZERO
    derive_from_prices = False
    for row in rows:
        realized = _decimal(row.get("realized_pnl"))
        if realized is not None:
            exchange_total += realized
        metadata = row.get("metadata_json")
        if isinstance(metadata, Mapping):
            parsed_metadata = dict(metadata)
        else:
            try:
                parsed_metadata = json.loads(str(metadata or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                parsed_metadata = {}
        if (
            isinstance(parsed_metadata, Mapping)
            and _text(parsed_metadata.get("realized_pnl_source"))
            == "derived_from_fill_prices"
        ):
            derive_from_prices = True

    if not derive_from_prices:
        return exchange_total, "exchange_realized_pnl"

    derived_total = _ZERO
    for row in rows:
        price = _decimal(row.get("price"))
        qty = _decimal(row.get("qty"))
        role = _upper(row.get("role"))
        side = _lower(row.get("side"))
        if price is None or qty is None or price <= 0 or qty <= 0:
            return exchange_total, "exchange_realized_pnl_fallback"
        if side not in {"long", "short"}:
            return exchange_total, "exchange_realized_pnl_fallback"
        notional = price * qty
        if role == "ENTRY":
            derived_total += -notional if side == "long" else notional
        elif role in _CLOSE_ROLES:
            derived_total += notional if side == "long" else -notional
        else:
            return exchange_total, "exchange_realized_pnl_fallback"
    return derived_total, "derived_from_fill_prices"


def _issue_sort_key(issue: QualityIssue) -> tuple[str, str, str, str, str]:
    return (
        issue.entity_type,
        issue.entity_id,
        issue.severity,
        issue.issue_code,
        issue.reason,
    )


def _add_issue(
    target: list[QualityIssue],
    seen: set[tuple[str, str, str, str]],
    *,
    entity_type: str,
    entity_id: Any,
    issue_code: str,
    severity: str,
    reason: str,
    recoverable: bool = False,
    related_ids: Iterable[Any] = (),
) -> None:
    normalized_entity = str(entity_id)
    normalized_reason = " ".join(str(reason or issue_code).split())[:800]
    key = (entity_type, normalized_entity, issue_code, normalized_reason)
    if key in seen:
        return
    seen.add(key)
    target.append(
        QualityIssue(
            entity_type=entity_type,
            entity_id=normalized_entity,
            issue_code=issue_code,
            severity=severity,
            reason=normalized_reason,
            recoverable=bool(recoverable),
            related_ids=tuple(sorted({str(value) for value in related_ids if str(value)})),
        )
    )


def _status_issues(
    *,
    entity_type: str,
    entity_id: Any,
    quality: str,
    legacy: bool,
    needs_recovery: bool,
    recovery_status: str,
    ambiguity_reason: str,
    issues: list[QualityIssue],
    seen: set[tuple[str, str, str, str]],
) -> None:
    if legacy:
        _add_issue(
            issues,
            seen,
            entity_type=entity_type,
            entity_id=entity_id,
            issue_code=f"{entity_type}_legacy_data",
            severity="warning",
            reason="legacy row is retained for audit but is not a clean modern sample",
        )
    if needs_recovery or quality in {"needs_recovery", "recovery_required"}:
        _add_issue(
            issues,
            seen,
            entity_type=entity_type,
            entity_id=entity_id,
            issue_code=f"{entity_type}_recovery_unresolved",
            severity="quarantine",
            reason=f"restart/recovery gap remains unresolved ({recovery_status or 'unknown'})",
            recoverable=True,
        )
    if quality == "ambiguous":
        _add_issue(
            issues,
            seen,
            entity_type=entity_type,
            entity_id=entity_id,
            issue_code=f"{entity_type}_existing_ambiguous",
            severity="quarantine",
            reason=ambiguity_reason or "row is already marked ambiguous",
        )
    elif quality == "unavailable":
        _add_issue(
            issues,
            seen,
            entity_type=entity_type,
            entity_id=entity_id,
            issue_code=f"{entity_type}_existing_unavailable",
            severity="quarantine",
            reason=ambiguity_reason or "required evidence is unavailable",
            recoverable=True,
        )
    elif quality == "quarantined":
        _add_issue(
            issues,
            seen,
            entity_type=entity_type,
            entity_id=entity_id,
            issue_code=f"{entity_type}_existing_quarantine",
            severity="quarantine",
            reason=ambiguity_reason or "row is already quarantined",
            recoverable=True,
        )
    elif quality in {"partial", "provisional", "pending"}:
        _add_issue(
            issues,
            seen,
            entity_type=entity_type,
            entity_id=entity_id,
            issue_code=f"{entity_type}_incomplete",
            severity="warning",
            reason=f"row quality is {quality}",
            recoverable=True,
        )


def _summarize_signals(rows: Sequence[Mapping[str, Any]]) -> QualityEntitySummary:
    counters: Counter[str] = Counter()
    duplicate_observations = 0
    for row in rows:
        quality = _lower(row.get("data_quality_status")) or "pending"
        linkage = _lower(row.get("linkage_status")) or "unlinked"
        recovery = _lower(row.get("recovery_status")) or "not_required"
        needs_recovery = bool(_int(row.get("needs_recovery")))
        legacy = bool(_int(row.get("legacy_data")))
        duplicate_observations += max(0, _int(row.get("duplicate_count")))
        counters[quality] += 1
        counters["legacy"] += int(legacy)
        counters["needs_recovery"] += int(needs_recovery)
        counters["linked"] += int(linkage == "linked_exact")
        counters["unlinked"] += int(linkage != "linked_exact")
        counters["recovered"] += int(recovery in {"forward_resumed", "recovered"})
        counters["recovery_unresolved"] += int(
            needs_recovery or recovery in {"pending", "forward_resumed", "failed", "unavailable"}
        )
    return QualityEntitySummary(
        total_rows=len(rows),
        complete_rows=counters["complete"],
        provisional_rows=counters["partial"] + counters["provisional"] + counters["pending"],
        needs_recovery_rows=counters["needs_recovery"],
        ambiguous_rows=counters["ambiguous"],
        unavailable_rows=counters["unavailable"],
        legacy_rows=counters["legacy"],
        quarantined_rows=counters["quarantined"],
        linked_rows=counters["linked"],
        unlinked_rows=counters["unlinked"],
        recovered_after_restart_rows=counters["recovered"],
        recovery_unresolved_rows=counters["recovery_unresolved"],
        duplicate_observations=duplicate_observations,
    )


def _summarize_executions(rows: Sequence[Mapping[str, Any]]) -> QualityEntitySummary:
    counters: Counter[str] = Counter()
    for row in rows:
        quality = _lower(row.get("data_quality_status")) or "pending"
        linkage = _lower(row.get("linkage_status")) or "pending"
        state = _upper(row.get("financial_state")) or "PENDING"
        legacy = bool(_int(row.get("legacy_data")))
        counters[quality] += 1
        counters["legacy"] += int(legacy)
        counters["linked"] += int(linkage == "linked_exact")
        counters["unlinked"] += int(linkage != "linked_exact")
        counters["final"] += int(state == "FINAL")
    return QualityEntitySummary(
        total_rows=len(rows),
        complete_rows=counters["complete"],
        provisional_rows=counters["partial"] + counters["provisional"] + counters["pending"],
        ambiguous_rows=counters["ambiguous"],
        unavailable_rows=counters["unavailable"],
        legacy_rows=counters["legacy"],
        quarantined_rows=counters["quarantined"],
        linked_rows=counters["linked"],
        unlinked_rows=counters["unlinked"],
        final_rows=counters["final"],
    )


def _summarize_fills(rows: Sequence[Mapping[str, Any]]) -> QualityEntitySummary:
    complete = 0
    provisional = 0
    for row in rows:
        if (
            _int(row.get("execution_id")) > 0
            and _decimal(row.get("qty")) is not None
            and _decimal(row.get("price")) is not None
            and _datetime(row.get("fill_time")) is not None
            and _text(row.get("trade_id"))
        ):
            complete += 1
        else:
            provisional += 1
    return QualityEntitySummary(
        total_rows=len(rows),
        complete_rows=complete,
        provisional_rows=provisional,
    )


def _validate_signals(
    signal_rows: Sequence[Mapping[str, Any]],
    event_rows: Sequence[Mapping[str, Any]],
    issues: list[QualityIssue],
    seen: set[tuple[str, str, str, str]],
) -> None:
    events_by_signal: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
    for event in event_rows:
        signal_id = _int(event.get("signal_id"))
        if signal_id > 0:
            events_by_signal[signal_id].append(event)

    for row in signal_rows:
        signal_id = _int(row.get("id"))
        entity_id = signal_id if signal_id > 0 else _text(row.get("id")) or "unknown"
        quality = _lower(row.get("data_quality_status")) or "pending"
        needs_recovery = bool(_int(row.get("needs_recovery")))
        recovery_status = _lower(row.get("recovery_status")) or "not_required"
        existing_reason = _text(row.get("data_quality_reason") or row.get("ambiguous_reason"))
        _status_issues(
            entity_type="signal",
            entity_id=entity_id,
            quality=quality,
            legacy=bool(_int(row.get("legacy_data"))),
            needs_recovery=needs_recovery,
            recovery_status=recovery_status,
            ambiguity_reason=existing_reason,
            issues=issues,
            seen=seen,
        )

        duplicate_count = _int(row.get("duplicate_count"))
        if duplicate_count < 0:
            _add_issue(
                issues,
                seen,
                entity_type="signal",
                entity_id=entity_id,
                issue_code="signal_duplicate_count_invalid",
                severity="quarantine",
                reason="duplicate_count cannot be negative",
                recoverable=True,
            )

        activated_at = _datetime(row.get("activated_at"))
        completed_at = _datetime(row.get("completed_at"))
        status = _lower(row.get("status"))
        if activated_at and completed_at and completed_at < activated_at:
            _add_issue(
                issues,
                seen,
                entity_type="signal",
                entity_id=entity_id,
                issue_code="signal_completion_before_activation",
                severity="quarantine",
                reason="completed_at is earlier than activated_at",
            )
        if status in _SIGNAL_TERMINAL and activated_at is None:
            _add_issue(
                issues,
                seen,
                entity_type="signal",
                entity_id=entity_id,
                issue_code="signal_terminal_missing_activation",
                severity="quarantine",
                reason=f"terminal status {status} has no activated_at",
                recoverable=True,
            )
        if status in _SIGNAL_TERMINAL and completed_at is None:
            _add_issue(
                issues,
                seen,
                entity_type="signal",
                entity_id=entity_id,
                issue_code="signal_terminal_missing_completion",
                severity="quarantine",
                reason=f"terminal status {status} has no completed_at",
                recoverable=True,
            )

        signal_events = sorted(
            events_by_signal.get(signal_id, []),
            key=lambda event: (
                _datetime(event.get("observed_at")) or datetime.min.replace(tzinfo=timezone.utc),
                _int(event.get("id")),
            ),
        )
        tp_times: dict[int, datetime] = {}
        stop_times: list[datetime] = []
        for event in signal_events:
            event_type = _upper(event.get("event_type"))
            event_time = _datetime(event.get("observed_at"))
            level = _int(event.get("level_index"))
            if event_type == "TP" and 1 <= level <= 4 and event_time is not None:
                previous = tp_times.get(level)
                if previous is None or event_time < previous:
                    tp_times[level] = event_time
            elif event_type == "STOP" and event_time is not None:
                stop_times.append(event_time)

        for level in range(2, 5):
            current = tp_times.get(level)
            prior = tp_times.get(level - 1)
            if current is not None and prior is None:
                _add_issue(
                    issues,
                    seen,
                    entity_type="signal",
                    entity_id=entity_id,
                    issue_code="signal_tp_sequence_gap",
                    severity="quarantine",
                    reason=f"TP{level} exists without TP{level - 1}",
                    recoverable=True,
                )
            elif current is not None and prior is not None and current < prior:
                _add_issue(
                    issues,
                    seen,
                    entity_type="signal",
                    entity_id=entity_id,
                    issue_code="signal_tp_chronology_invalid",
                    severity="quarantine",
                    reason=f"TP{level} is earlier than TP{level - 1}",
                )

        event_max_tp = max(tp_times, default=0)
        stored_max_tp = max(0, _int(row.get("max_tp_index")))
        if stored_max_tp != event_max_tp:
            _add_issue(
                issues,
                seen,
                entity_type="signal",
                entity_id=entity_id,
                issue_code="signal_max_tp_event_mismatch",
                severity="quarantine",
                reason=f"max_tp_index={stored_max_tp} but durable TP events prove {event_max_tp}",
                recoverable=True,
            )

        be_armed_at = _datetime(row.get("be_armed_at"))
        trigger_level = max(0, _int(row.get("be_trigger_tp_index")))
        if be_armed_at is not None and trigger_level > 0:
            trigger_time = tp_times.get(trigger_level)
            if trigger_time is None:
                _add_issue(
                    issues,
                    seen,
                    entity_type="signal",
                    entity_id=entity_id,
                    issue_code="signal_be_trigger_event_missing",
                    severity="quarantine",
                    reason=f"BE is armed but TP{trigger_level} trigger event is missing",
                    recoverable=True,
                )
            elif be_armed_at < trigger_time:
                _add_issue(
                    issues,
                    seen,
                    entity_type="signal",
                    entity_id=entity_id,
                    issue_code="signal_be_before_trigger_tp",
                    severity="quarantine",
                    reason=f"BE armed before TP{trigger_level}",
                )
        if status == "completed_be" and be_armed_at is None:
            _add_issue(
                issues,
                seen,
                entity_type="signal",
                entity_id=entity_id,
                issue_code="signal_be_terminal_without_arm",
                severity="quarantine",
                reason="completed_be has no durable be_armed_at",
                recoverable=True,
            )

        targets_count = min(4, len(_json_list(row.get("targets_json"))))
        terminal_reason = _lower(row.get("terminal_reason"))
        all_targets = (
            status == "completed_tp"
            or terminal_reason in {"all_targets", "all_tps", "tp_all"}
            or (targets_count > 0 and stored_max_tp >= targets_count)
        )
        if stop_times and all_targets:
            _add_issue(
                issues,
                seen,
                entity_type="signal",
                entity_id=entity_id,
                issue_code="signal_stop_all_tps_terminal_conflict",
                severity="quarantine",
                reason="durable STOP evidence conflicts with ALL_TPS terminal outcome",
            )
        if status == "completed_stop" and targets_count > 0 and stored_max_tp >= targets_count:
            _add_issue(
                issues,
                seen,
                entity_type="signal",
                entity_id=entity_id,
                issue_code="signal_stop_after_all_targets_conflict",
                severity="quarantine",
                reason="completed_stop cannot coexist with every configured TP reached",
            )


def _validate_fill_identity(
    fill_rows: Sequence[Mapping[str, Any]],
    issues: list[QualityIssue],
    seen: set[tuple[str, str, str, str]],
) -> None:
    by_identity: dict[tuple[str, int, str], set[int]] = defaultdict(set)
    by_fingerprint: dict[tuple[str, int, str], set[int]] = defaultdict(set)
    for row in fill_rows:
        fill_id = _int(row.get("id"))
        entity_id = fill_id if fill_id > 0 else _text(row.get("trade_id")) or "unknown"
        execution_id = _int(row.get("execution_id"))
        user_id = _int(row.get("user_id"))
        exchange = _lower(row.get("exchange")) or "bingx"
        trade_id = _text(row.get("trade_id"))
        fingerprint = _text(row.get("fingerprint"))
        if trade_id:
            by_identity[(exchange, user_id, trade_id)].add(execution_id)
        if fingerprint:
            by_fingerprint[(exchange, user_id, fingerprint)].add(execution_id)

        qty = _decimal(row.get("qty"))
        price = _decimal(row.get("price"))
        fill_time = _datetime(row.get("fill_time"))
        if execution_id <= 0:
            _add_issue(
                issues,
                seen,
                entity_type="fill",
                entity_id=entity_id,
                issue_code="fill_execution_missing",
                severity="quarantine",
                reason="fill has no positive execution_id",
                recoverable=True,
            )
        if not trade_id:
            _add_issue(
                issues,
                seen,
                entity_type="fill",
                entity_id=entity_id,
                issue_code="fill_trade_identity_missing",
                severity="quarantine",
                reason="fill has no stable trade_id",
                recoverable=True,
            )
        if qty is None or qty <= 0:
            _add_issue(
                issues,
                seen,
                entity_type="fill",
                entity_id=entity_id,
                issue_code="fill_qty_invalid",
                severity="quarantine",
                reason="fill quantity must be positive",
            )
        if price is None or price <= 0:
            _add_issue(
                issues,
                seen,
                entity_type="fill",
                entity_id=entity_id,
                issue_code="fill_price_invalid",
                severity="quarantine",
                reason="fill price must be positive",
            )
        if fill_time is None:
            _add_issue(
                issues,
                seen,
                entity_type="fill",
                entity_id=entity_id,
                issue_code="fill_time_invalid",
                severity="quarantine",
                reason="fill time is missing or invalid",
                recoverable=True,
            )
        if _decimal(row.get("fee")) is None:
            _add_issue(
                issues,
                seen,
                entity_type="fill",
                entity_id=entity_id,
                issue_code="fill_fee_invalid",
                severity="quarantine",
                reason="fill fee is not a finite Decimal",
                recoverable=True,
            )

    for identity, owners in by_identity.items():
        concrete = {owner for owner in owners if owner > 0}
        if len(concrete) > 1:
            exchange, user_id, trade_id = identity
            for execution_id in sorted(concrete):
                _add_issue(
                    issues,
                    seen,
                    entity_type="execution",
                    entity_id=execution_id,
                    issue_code="execution_fill_owned_by_multiple_executions",
                    severity="quarantine",
                    reason=f"{exchange}/{user_id}/{trade_id} belongs to multiple executions",
                    related_ids=concrete,
                )
    for identity, owners in by_fingerprint.items():
        concrete = {owner for owner in owners if owner > 0}
        if len(concrete) > 1:
            exchange, user_id, fingerprint = identity
            for execution_id in sorted(concrete):
                _add_issue(
                    issues,
                    seen,
                    entity_type="execution",
                    entity_id=execution_id,
                    issue_code="execution_fill_fingerprint_duplicate",
                    severity="quarantine",
                    reason=f"fill fingerprint {exchange}/{user_id}/{fingerprint} is reused across executions",
                    related_ids=concrete,
                )


def _validate_executions(
    signal_rows: Sequence[Mapping[str, Any]],
    execution_rows: Sequence[Mapping[str, Any]],
    fill_rows: Sequence[Mapping[str, Any]],
    issues: list[QualityIssue],
    seen: set[tuple[str, str, str, str]],
) -> None:
    signals = {_int(row.get("id")): row for row in signal_rows if _int(row.get("id")) > 0}
    fills_by_execution: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
    for fill in fill_rows:
        execution_id = _int(fill.get("execution_id"))
        if execution_id > 0:
            fills_by_execution[execution_id].append(fill)

    for row in execution_rows:
        execution_id = _int(row.get("execution_id") or row.get("id"))
        entity_id = execution_id if execution_id > 0 else _text(row.get("execution_id")) or "unknown"
        quality = _lower(row.get("data_quality_status")) or "pending"
        existing_reason = _text(row.get("ambiguity_reason"))
        if _lower(row.get("market_event_review_status")) == "manual_review":
            _add_issue(
                issues,
                seen,
                entity_type="execution",
                entity_id=entity_id,
                issue_code="execution_market_event_manual_review",
                severity="quarantine",
                reason=_text(row.get("market_event_exclusion_reason")) or "market_event_manual_review",
                recoverable=True,
            )
        _status_issues(
            entity_type="execution",
            entity_id=entity_id,
            quality=quality,
            legacy=bool(_int(row.get("legacy_data"))),
            needs_recovery=False,
            recovery_status="not_required",
            ambiguity_reason=existing_reason,
            issues=issues,
            seen=seen,
        )

        linkage = _lower(row.get("linkage_status")) or "pending"
        signal_id = _int(row.get("analytics_signal_id"))
        if linkage != "linked_exact" or signal_id <= 0:
            _add_issue(
                issues,
                seen,
                entity_type="execution",
                entity_id=entity_id,
                issue_code="execution_not_exact_linked",
                severity="quarantine" if _upper(row.get("financial_state")) == "FINAL" else "warning",
                reason=f"linkage_status={linkage or 'missing'} analytics_signal_id={signal_id}",
                recoverable=True,
            )

        entry_qty = _decimal(row.get("actual_entry_qty"))
        exit_qty = _decimal(row.get("actual_exit_qty"))
        if entry_qty is not None and entry_qty <= 0:
            _add_issue(
                issues,
                seen,
                entity_type="execution",
                entity_id=entity_id,
                issue_code="execution_entry_qty_invalid",
                severity="quarantine",
                reason="actual_entry_qty must be positive",
            )
        if exit_qty is not None and exit_qty < 0:
            _add_issue(
                issues,
                seen,
                entity_type="execution",
                entity_id=entity_id,
                issue_code="execution_exit_qty_invalid",
                severity="quarantine",
                reason="actual_exit_qty cannot be negative",
            )
        if entry_qty is not None and entry_qty > 0 and exit_qty is not None:
            tolerance = max(_VOLUME_REL_TOLERANCE, entry_qty.copy_abs() * _VOLUME_REL_TOLERANCE)
            if exit_qty - entry_qty > tolerance:
                _add_issue(
                    issues,
                    seen,
                    entity_type="execution",
                    entity_id=entity_id,
                    issue_code="execution_exit_exceeds_entry",
                    severity="quarantine",
                    reason=f"exit qty {exit_qty} exceeds entry qty {entry_qty}",
                )

        financial_state = _upper(row.get("financial_state")) or "PENDING"
        volume_state = _lower(row.get("volume_parity_status")) or "pending"
        completeness = _decimal(row.get("completeness_percent"))
        funding_state = _lower(row.get("funding_state")) or "not_checked"
        funding_recovery_status = _lower(row.get("funding_recovery_status"))
        if funding_state == "manual_review" or funding_recovery_status == "manual_review":
            _add_issue(
                issues,
                seen,
                entity_type="execution",
                entity_id=entity_id,
                issue_code="execution_funding_manual_review",
                severity="warning",
                reason=_text(row.get("funding_recovery_reason")) or "funding recovery requires manual review",
                recoverable=True,
            )
        elif funding_recovery_status == "pending_zero_confirmation":
            _add_issue(
                issues,
                seen,
                entity_type="execution",
                entity_id=entity_id,
                issue_code="execution_funding_zero_confirmation_pending",
                severity="warning",
                reason=(
                    f"empty funding observations={_int(row.get('funding_zero_observations'))}; "
                    "grace/recheck confirmation is not complete"
                ),
                recoverable=True,
            )
        if financial_state == "FINAL" and funding_state not in {
            "confirmed", "confirmed_zero", "not_applicable"
        }:
            _add_issue(
                issues,
                seen,
                entity_type="execution",
                entity_id=entity_id,
                issue_code="execution_final_funding_not_confirmed",
                severity="quarantine",
                reason=f"FINAL cannot use funding_state={funding_state}",
                recoverable=True,
            )

        gate_version = _int(row.get("quality_gate_version"))
        if gate_version >= 2:
            gate_final = bool(_int(row.get("final_eligible")))
            gate_sim = bool(_int(row.get("simulation_eligible")))
            gate_risk = bool(_int(row.get("risk_analysis_eligible")))
            if gate_final and (
                financial_state != "FINAL"
                or funding_state not in {"confirmed", "confirmed_zero", "not_applicable"}
                or linkage != "linked_exact"
                or _decimal(row.get("net_pnl")) is None
            ):
                _add_issue(
                    issues,
                    seen,
                    entity_type="execution",
                    entity_id=entity_id,
                    issue_code="execution_quality_gate_final_contradiction",
                    severity="quarantine",
                    reason="final_eligible=1 contradicts durable financial/linkage evidence",
                    recoverable=True,
                )
            if gate_risk and any(
                _decimal(row.get(column)) is None
                for column in (
                    "equity_snapshot_usd", "planned_risk_usd",
                    "initial_price_risk_usd", "planned_entry_qty", "stop_distance"
                )
            ):
                _add_issue(
                    issues,
                    seen,
                    entity_type="execution",
                    entity_id=entity_id,
                    issue_code="execution_quality_gate_risk_contradiction",
                    severity="quarantine",
                    reason="risk_analysis_eligible=1 but immutable risk evidence is incomplete",
                    recoverable=True,
                )
            if gate_sim and (
                _int(row.get("tp_distribution_locked")) != 1
                or not _json_list(row.get("tp_distribution_json"))
            ):
                _add_issue(
                    issues,
                    seen,
                    entity_type="execution",
                    entity_id=entity_id,
                    issue_code="execution_quality_gate_simulation_contradiction",
                    severity="quarantine",
                    reason="simulation_eligible=1 but exact locked TP allocation is missing",
                    recoverable=True,
                )
        if financial_state == "FINAL":
            if entry_qty is None or entry_qty <= 0 or exit_qty is None:
                _add_issue(
                    issues,
                    seen,
                    entity_type="execution",
                    entity_id=entity_id,
                    issue_code="execution_final_missing_volume",
                    severity="quarantine",
                    reason="FINAL requires complete positive ENTRY and EXIT quantities",
                    recoverable=True,
                )
            if volume_state not in _FINAL_VOLUME:
                _add_issue(
                    issues,
                    seen,
                    entity_type="execution",
                    entity_id=entity_id,
                    issue_code="execution_final_volume_parity_invalid",
                    severity="quarantine",
                    reason=f"FINAL cannot use volume_parity_status={volume_state}",
                    recoverable=True,
                )
            if completeness is None or completeness < _HUNDRED:
                _add_issue(
                    issues,
                    seen,
                    entity_type="execution",
                    entity_id=entity_id,
                    issue_code="execution_final_incomplete",
                    severity="quarantine",
                    reason=f"FINAL requires 100% completeness, got {completeness}",
                    recoverable=True,
                )

        signal = signals.get(signal_id)
        if signal is not None:
            if _canonical_symbol(signal.get("symbol")) != _canonical_symbol(row.get("symbol")):
                _add_issue(
                    issues,
                    seen,
                    entity_type="execution",
                    entity_id=entity_id,
                    issue_code="execution_signal_symbol_mismatch",
                    severity="quarantine",
                    reason="execution symbol differs from linked signal symbol",
                    related_ids=(signal_id,),
                )
            if _lower(signal.get("side")) != _lower(row.get("side")):
                _add_issue(
                    issues,
                    seen,
                    entity_type="execution",
                    entity_id=entity_id,
                    issue_code="execution_signal_side_mismatch",
                    severity="quarantine",
                    reason="execution side differs from linked signal side",
                    related_ids=(signal_id,),
                )

        first_entry = _datetime(row.get("first_entry_fill_at"))
        last_exit = _datetime(row.get("last_exit_fill_at"))
        duration = _int(row.get("execution_duration_seconds"), default=-1)
        if first_entry and last_exit and last_exit < first_entry:
            _add_issue(
                issues,
                seen,
                entity_type="execution",
                entity_id=entity_id,
                issue_code="execution_duration_negative",
                severity="quarantine",
                reason="last_exit_fill_at is earlier than first_entry_fill_at",
            )
        if row.get("execution_duration_seconds") not in (None, "") and duration < 0:
            _add_issue(
                issues,
                seen,
                entity_type="execution",
                entity_id=entity_id,
                issue_code="execution_duration_value_negative",
                severity="quarantine",
                reason="execution_duration_seconds cannot be negative",
            )
        if first_entry and last_exit and duration >= 0:
            expected_duration = Decimal(str(int((last_exit - first_entry).total_seconds())))
            if not _within(Decimal(duration), expected_duration, Decimal("1")):
                _add_issue(
                    issues,
                    seen,
                    entity_type="execution",
                    entity_id=entity_id,
                    issue_code="execution_duration_mismatch",
                    severity="quarantine",
                    reason=f"stored duration {duration}s differs from fill chronology {expected_duration}s",
                    recoverable=True,
                )

        canonical_terminal = _upper(row.get("canonical_terminal_reason"))
        terminal_detail = _lower(row.get("terminal_detail"))
        if canonical_terminal in {"STOP", "BE", "ALL_TPS", "TP"} and any(
            marker in terminal_detail for marker in ("manual", "external", "outside_system", "operator")
        ):
            _add_issue(
                issues,
                seen,
                entity_type="execution",
                entity_id=entity_id,
                issue_code="execution_manual_external_masked",
                severity="quarantine",
                reason=f"manual/external terminal detail is masked as {canonical_terminal}",
            )

        execution_fills = fills_by_execution.get(execution_id, [])
        fee_total = _ZERO
        valid_fee_count = 0
        for fill in execution_fills:
            fill_id = _int(fill.get("id"))
            if _int(fill.get("user_id")) != _int(row.get("user_id")):
                _add_issue(
                    issues,
                    seen,
                    entity_type="fill",
                    entity_id=fill_id or _text(fill.get("trade_id")) or "unknown",
                    issue_code="fill_user_owner_mismatch",
                    severity="quarantine",
                    reason="fill user_id differs from execution user_id",
                    related_ids=(execution_id,),
                )
            if _canonical_symbol(fill.get("symbol")) != _canonical_symbol(row.get("symbol")):
                _add_issue(
                    issues,
                    seen,
                    entity_type="fill",
                    entity_id=fill_id or _text(fill.get("trade_id")) or "unknown",
                    issue_code="fill_symbol_owner_mismatch",
                    severity="quarantine",
                    reason="fill symbol differs from execution symbol",
                    related_ids=(execution_id,),
                )
            if _lower(fill.get("side")) != _lower(row.get("side")):
                _add_issue(
                    issues,
                    seen,
                    entity_type="fill",
                    entity_id=fill_id or _text(fill.get("trade_id")) or "unknown",
                    issue_code="fill_side_owner_mismatch",
                    severity="quarantine",
                    reason="fill side differs from execution side",
                    related_ids=(execution_id,),
                )
            fee = _decimal(fill.get("fee"))
            if fee is not None:
                fee_total += fee
                valid_fee_count += 1

        gross_total, gross_source = _fill_gross_total(execution_fills)

        stored_fee = _decimal(row.get("trading_fee_signed"))
        if execution_fills and valid_fee_count == len(execution_fills):
            if stored_fee is None or not _within(stored_fee, fee_total):
                _add_issue(
                    issues,
                    seen,
                    entity_type="execution",
                    entity_id=entity_id,
                    issue_code="execution_fee_aggregate_mismatch",
                    severity="quarantine",
                    reason=f"stored trading_fee_signed={stored_fee} but fills sum to {fee_total}",
                    recoverable=True,
                )
        stored_gross = _decimal(row.get("exchange_gross_pnl"))
        if execution_fills and stored_gross is not None and not _within(stored_gross, gross_total):
            _add_issue(
                issues,
                seen,
                entity_type="execution",
                entity_id=entity_id,
                issue_code="execution_gross_pnl_aggregate_mismatch",
                severity="quarantine",
                reason=(
                    f"stored exchange_gross_pnl={stored_gross} but fills "
                    f"aggregate to {gross_total} via {gross_source}"
                ),
                recoverable=True,
            )

        gross = _decimal(row.get("exchange_gross_pnl"))
        fee = _decimal(row.get("trading_fee_signed"))
        funding = _decimal(row.get("funding_signed"))
        net = _decimal(row.get("net_pnl"))
        if financial_state == "FINAL":
            if None in {gross, fee, funding, net}:
                _add_issue(
                    issues,
                    seen,
                    entity_type="execution",
                    entity_id=entity_id,
                    issue_code="execution_final_financial_component_missing",
                    severity="quarantine",
                    reason="FINAL requires gross PnL, signed fee, signed funding and net PnL",
                    recoverable=True,
                )
            else:
                assert gross is not None and fee is not None and funding is not None and net is not None
                expected_net = gross + fee + funding
                if not _within(net, expected_net):
                    _add_issue(
                        issues,
                        seen,
                        entity_type="execution",
                        entity_id=entity_id,
                        issue_code="execution_net_pnl_formula_mismatch",
                        severity="quarantine",
                        reason=f"net_pnl={net} but gross+fee+funding={expected_net}",
                        recoverable=True,
                    )

            risk = _decimal(row.get("initial_price_risk_usd"))
            result_r = _decimal(row.get("result_r"))
            if risk is None or risk <= 0:
                _add_issue(
                    issues,
                    seen,
                    entity_type="execution",
                    entity_id=entity_id,
                    issue_code="execution_r_analysis_initial_risk_missing",
                    severity="warning",
                    reason=(
                        "financial FINAL PnL is retained, but R/risk analysis requires "
                        "positive initial_price_risk_usd"
                    ),
                    recoverable=True,
                )
            elif result_r is None:
                _add_issue(
                    issues,
                    seen,
                    entity_type="execution",
                    entity_id=entity_id,
                    issue_code="execution_r_analysis_result_missing",
                    severity="warning",
                    reason="financial FINAL PnL is retained, but result_r is unavailable",
                    recoverable=True,
                )
            elif net is None or not _within(result_r, net / risk):
                expected = None if net is None else net / risk
                _add_issue(
                    issues,
                    seen,
                    entity_type="execution",
                    entity_id=entity_id,
                    issue_code="execution_result_r_formula_mismatch",
                    severity="quarantine",
                    reason=f"result_r={result_r} but net_pnl/initial_risk={expected}",
                    recoverable=True,
                )

        # A negative net result for canonical BE is valid because commission and
        # funding are real signed costs.  It is intentionally not an issue.


def calculate_statistics_quality(
    signal_rows: Iterable[Mapping[str, Any]],
    event_rows: Iterable[Mapping[str, Any]],
    execution_rows: Iterable[Mapping[str, Any]],
    fill_rows: Iterable[Mapping[str, Any]] = (),
) -> StatisticsQualityReport:
    """Validate one bounded, non-truncated statistics dataset.

    The caller is responsible for rejecting truncated datasets.  This function
    never infers omitted rows and never mutates the supplied mappings.
    """

    signals = [dict(row) for row in signal_rows]
    events = [dict(row) for row in event_rows]
    executions = [dict(row) for row in execution_rows]
    fills = [dict(row) for row in fill_rows]

    issues: list[QualityIssue] = []
    seen: set[tuple[str, str, str, str]] = set()
    _validate_signals(signals, events, issues, seen)
    _validate_fill_identity(fills, issues, seen)
    _validate_executions(signals, executions, fills, issues, seen)

    issues.sort(key=_issue_sort_key)
    quarantine_rows = tuple(
        QuarantineRow(
            entity_type=issue.entity_type,
            entity_id=issue.entity_id,
            issue_code=issue.issue_code,
            severity=issue.severity,
            recoverable=issue.recoverable,
            reason=issue.reason,
            related_ids=issue.related_ids,
        )
        for issue in issues
        if issue.severity == "quarantine"
    )
    issue_counts = dict(sorted(Counter(issue.issue_code for issue in issues).items()))

    signal_summary = _summarize_signals(signals)
    execution_summary = _summarize_executions(executions)
    fill_summary = _summarize_fills(fills)
    linked_coverage = _percent(signal_summary.linked_rows, signal_summary.total_rows)
    final_coverage = _percent(execution_summary.final_rows, execution_summary.total_rows)

    trust_reasons: list[str] = []
    if quarantine_rows:
        trust_status = "blocked"
        trust_reasons.append(f"{len(quarantine_rows)} quarantine issue(s) require review")
    elif issues:
        trust_status = "limited"
        trust_reasons.append(f"{len(issues)} warning issue(s) reduce completeness")
    elif not signals and not executions and not fills:
        trust_status = "empty"
        trust_reasons.append("dataset contains no rows")
    elif (
        signal_summary.provisional_rows
        or signal_summary.legacy_rows
        or signal_summary.unlinked_rows
        or execution_summary.provisional_rows
        or execution_summary.legacy_rows
        or execution_summary.unlinked_rows
    ):
        trust_status = "limited"
        trust_reasons.append("dataset contains incomplete, legacy or unlinked rows")
    else:
        trust_status = "verified"
        trust_reasons.append("no integrity contradiction was found in the supplied complete dataset")

    if signal_summary.recovery_unresolved_rows:
        trust_reasons.append(
            f"{signal_summary.recovery_unresolved_rows} signal recovery gap(s) remain unresolved"
        )
    if execution_summary.total_rows and execution_summary.final_rows < execution_summary.total_rows:
        trust_reasons.append(
            f"FINAL financial coverage is {final_coverage}%"
        )

    return StatisticsQualityReport(
        signal_summary=signal_summary,
        execution_summary=execution_summary,
        fill_summary=fill_summary,
        issues=tuple(issues),
        quarantine_rows=quarantine_rows,
        issue_counts=issue_counts,
        linked_signal_coverage_percent=linked_coverage,
        final_financial_coverage_percent=final_coverage,
        trust_status=trust_status,
        trust_reasons=tuple(trust_reasons),
    )


def quality_quarantine_export_rows(
    report: StatisticsQualityReport,
) -> tuple[dict[str, Any], ...]:
    """Return stable row dictionaries for the step-8 quarantine CSV exporter."""

    return tuple(
        {
            "entity_type": row.entity_type,
            "entity_id": row.entity_id,
            "issue_code": row.issue_code,
            "severity": row.severity,
            "recoverable": int(row.recoverable),
            "reason": row.reason,
            "related_ids": ",".join(row.related_ids),
        }
        for row in report.quarantine_rows
    )
