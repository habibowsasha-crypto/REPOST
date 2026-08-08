"""Durable, fail-closed quality gates for statistics Package 4.

The gate answers three independent questions:

* ``final_eligible`` -- exchange-backed financial result is exact and complete;
* ``simulation_eligible`` -- the public trajectory and the exact executed TP
  allocation are complete enough for strategy simulations;
* ``risk_analysis_eligible`` -- immutable sizing/risk evidence is present.

Package 4 hardens Package 3 in four ways:

* the immutable ``statistics_entity_links`` row is verified instead of trusting
  only a cached linkage status;
* FINAL is independently checked against fills, volume, fees, funding,
  chronology, completeness bits, and the PnL identity;
* signal-level simulation requires at least one verified execution and all
  linked executions must be simulation-eligible;
* refreshes use CAS and ``quality_evaluated_at`` so a failed or concurrent
  refresh is recovered by the bounded backlog instead of becoming permanently
  stale.

The module is metadata-only. It never calls BingX and never changes ENTRY,
STOP, TP, BE, sizing, risk slots, or order state.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping, Sequence

from app.config import get_settings
from app.database import db

QUALITY_GATE_VERSION = 3
_SIGNAL_TERMINAL = {
    "completed_tp",
    "completed_stop",
    "completed_be",
    "completed_expired",
    "completed_cancelled",
}
_FUNDING_CONFIRMED = {"confirmed", "confirmed_zero", "not_applicable"}
_EXACT_LINKAGE = {"linked_exact", "exact"}
_RISK_OK = {"complete", "captured", "exact", "confirmed"}
_VOLUME_OK = {"exact", "within_tolerance"}

# Keep the independent FINAL check aligned with the durable projection bits.
_BIT_LINKAGE = 1
_BIT_TERMINAL = 2
_BIT_ENTRY_FILLS = 4
_BIT_EXIT_FILLS = 8
_BIT_VOLUME_PARITY = 16
_BIT_FEES = 32
_BIT_FUNDING = 64
_BIT_CHRONOLOGY = 256
_REQUIRED_FINAL_MASK = (
    _BIT_LINKAGE
    | _BIT_TERMINAL
    | _BIT_ENTRY_FILLS
    | _BIT_EXIT_FILLS
    | _BIT_VOLUME_PARITY
    | _BIT_FEES
    | _BIT_FUNDING
    | _BIT_CHRONOLOGY
)
_PNL_TOLERANCE = Decimal("0.00000001")


@dataclass(frozen=True, slots=True)
class QualityGateDecision:
    final_eligible: bool
    simulation_eligible: bool
    risk_analysis_eligible: bool
    reasons: tuple[str, ...]

    def reasons_json(self) -> str:
        return json.dumps(list(self.reasons), ensure_ascii=False, separators=(",", ":"))


@dataclass(frozen=True, slots=True)
class QualityGateRefreshResult:
    executions_refreshed: int = 0
    signals_refreshed: int = 0
    final_eligible_executions: int = 0
    simulation_eligible_executions: int = 0
    risk_eligible_executions: int = 0
    execution_cas_conflicts: int = 0
    signal_cas_conflicts: int = 0


def _row(row: Any) -> dict[str, Any]:
    return dict(row) if row is not None else {}


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError, OverflowError):
        return 0


def _decimal(value: Any) -> Decimal | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


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


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    try:
        parsed = json.loads(str(value or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return list(parsed) if isinstance(parsed, list) else []


def _decimal_sequence(value: Any) -> tuple[Decimal, ...] | None:
    raw = _json_list(value)
    if not raw:
        return None
    result: list[Decimal] = []
    for item in raw:
        parsed = _decimal(item)
        if parsed is None or parsed <= 0:
            return None
        result.append(parsed)
    return tuple(result)


def _valid_tp_distribution(value: Any, *, target_count: int) -> bool:
    raw = _json_list(value)
    if target_count <= 0 or len(raw) != target_count:
        return False
    total = Decimal()
    for item in raw:
        parsed = _decimal(item)
        if parsed is None or parsed <= 0:
            return False
        total += parsed
    return abs(total - Decimal("100")) <= Decimal("0.02")


def _expected_settlement_asset(symbol: Any) -> str | None:
    canonical = str(symbol or "").strip().upper().replace("-", "").replace("_", "")
    if canonical.endswith("USDT"):
        return "USDT"
    if canonical.endswith("USDC"):
        return "USDC"
    return None


def evaluate_execution_quality_gate(row: Mapping[str, Any]) -> QualityGateDecision:
    """Evaluate one execution without mutating or guessing missing evidence."""

    reasons: list[str] = []
    market_event_review = str(row.get("market_event_review_status") or "clear").strip().lower()
    if market_event_review == "manual_review":
        reason = str(row.get("market_event_exclusion_reason") or "market_event_manual_review").strip()
        return QualityGateDecision(
            final_eligible=False,
            simulation_eligible=False,
            risk_analysis_eligible=False,
            reasons=(reason or "market_event_manual_review",),
        )
    linkage = str(row.get("linkage_status") or "").strip().lower()
    immutable_link_verified = _int(row.get("immutable_link_verified")) == 1
    exact_link = (
        linkage in _EXACT_LINKAGE
        and immutable_link_verified
        and _int(row.get("analytics_signal_id")) > 0
        and _int(row.get("trade_group_id")) > 0
        and _int(row.get("execution_id") or row.get("id")) > 0
    )
    if linkage not in _EXACT_LINKAGE:
        reasons.append("missing_exact_linkage")
    elif not immutable_link_verified:
        reasons.append("immutable_link_not_verified")

    financial_state = str(row.get("financial_state") or "PENDING").strip().upper()
    if financial_state != "FINAL":
        reasons.append(f"financial_not_final:{financial_state.lower()}")

    funding_state = str(row.get("funding_state") or "not_checked").strip().lower()
    if funding_state not in _FUNDING_CONFIRMED:
        reasons.append(f"funding_not_confirmed:{funding_state or 'missing'}")

    entry_qty = _decimal(row.get("actual_entry_qty"))
    exit_qty = _decimal(row.get("actual_exit_qty"))
    entry_avg = _decimal(row.get("actual_entry_avg_price"))
    exit_avg = _decimal(row.get("actual_exit_avg_price"))
    gross = _decimal(row.get("exchange_gross_pnl"))
    fee = _decimal(row.get("trading_fee_signed"))
    funding = _decimal(row.get("funding_signed"))
    net = _decimal(row.get("net_pnl"))
    volume_status = str(row.get("volume_parity_status") or "").strip().lower()
    mask = _int(row.get("completeness_mask"))

    first_entry = _datetime(row.get("first_entry_fill_at"))
    last_entry = _datetime(row.get("last_entry_fill_at"))
    first_exit = _datetime(row.get("first_exit_fill_at"))
    last_exit = _datetime(row.get("last_exit_fill_at"))
    chronology_ok = (
        first_entry is not None
        and last_entry is not None
        and first_exit is not None
        and last_exit is not None
        and first_entry <= last_entry <= last_exit
        and first_entry <= first_exit <= last_exit
    )
    completeness_ok = (mask & _REQUIRED_FINAL_MASK) == _REQUIRED_FINAL_MASK
    asset = str(row.get("settlement_asset") or "").strip().upper()
    expected_asset = _expected_settlement_asset(row.get("symbol"))
    asset_ok = expected_asset is not None and asset == expected_asset
    pnl_identity_ok = (
        gross is not None
        and fee is not None
        and funding is not None
        and net is not None
        and abs(net - (gross + fee + funding)) <= _PNL_TOLERANCE
    )

    if entry_qty is None or entry_qty <= 0:
        reasons.append("missing_actual_entry_qty")
    if exit_qty is None or exit_qty <= 0:
        reasons.append("missing_actual_exit_qty")
    if entry_avg is None or entry_avg <= 0:
        reasons.append("missing_actual_entry_price")
    if exit_avg is None or exit_avg <= 0:
        reasons.append("missing_actual_exit_price")
    if volume_status not in _VOLUME_OK:
        reasons.append(f"volume_not_exact:{volume_status or 'missing'}")
    if gross is None:
        reasons.append("missing_exchange_gross_pnl")
    if fee is None:
        reasons.append("missing_trading_fee")
    if funding is None:
        reasons.append("missing_funding_value")
    if net is None:
        reasons.append("missing_net_pnl")
    if not pnl_identity_ok:
        reasons.append("net_pnl_identity_mismatch")
    if not asset_ok:
        reasons.append("settlement_asset_mismatch")
    if not chronology_ok:
        reasons.append("fill_chronology_incomplete")
    if not completeness_ok:
        reasons.append("financial_completeness_mask_incomplete")

    final_ok = (
        exact_link
        and financial_state == "FINAL"
        and funding_state in _FUNDING_CONFIRMED
        and entry_qty is not None
        and entry_qty > 0
        and exit_qty is not None
        and exit_qty > 0
        and entry_avg is not None
        and entry_avg > 0
        and exit_avg is not None
        and exit_avg > 0
        and volume_status in _VOLUME_OK
        and pnl_identity_ok
        and asset_ok
        and chronology_ok
        and completeness_ok
    )

    signal_targets = _decimal_sequence(
        row.get("signal_targets_json") or row.get("targets_json")
    )
    execution_targets = _decimal_sequence(row.get("execution_targets_json"))
    targets_match = (
        signal_targets is not None
        and execution_targets is not None
        and signal_targets == execution_targets
    )
    distribution_ok = (
        signal_targets is not None
        and _int(row.get("tp_distribution_locked")) == 1
        and _valid_tp_distribution(
            row.get("tp_distribution_json"), target_count=len(signal_targets)
        )
    )
    signal_distribution_ok = (
        signal_targets is not None
        and str(row.get("signal_target_percents_source") or "").strip().lower()
        == "execution_consensus"
        and _valid_tp_distribution(
            row.get("signal_target_percents_json"), target_count=len(signal_targets)
        )
        and _decimal_sequence(row.get("signal_target_percents_json"))
        == _decimal_sequence(row.get("tp_distribution_json"))
    )
    signal_status = str(row.get("signal_status") or "").strip().lower()
    signal_recovery = _int(row.get("signal_needs_recovery")) > 0
    signal_ambiguous = bool(str(row.get("signal_ambiguous_reason") or "").strip())
    if signal_targets is None:
        reasons.append("missing_signal_targets")
    if execution_targets is None:
        reasons.append("missing_execution_targets")
    if not targets_match:
        reasons.append("execution_signal_targets_mismatch")
    if not distribution_ok:
        reasons.append("missing_exact_tp_allocation")
    if not signal_distribution_ok:
        reasons.append("signal_tp_allocation_not_execution_consensus")
    if signal_status and signal_status not in _SIGNAL_TERMINAL:
        reasons.append(f"signal_not_terminal:{signal_status}")
    if signal_recovery:
        reasons.append("signal_recovery_unresolved")
    if signal_ambiguous:
        reasons.append("signal_ambiguous")

    simulation_ok = (
        exact_link
        and signal_status in _SIGNAL_TERMINAL
        and not signal_recovery
        and not signal_ambiguous
        and targets_match
        and distribution_ok
        and signal_distribution_ok
    )

    equity = _decimal(row.get("equity_snapshot_usd"))
    planned_risk_percent = _decimal(row.get("planned_risk_percent"))
    planned_risk = _decimal(row.get("planned_risk_usd"))
    price_risk = _decimal(row.get("initial_price_risk_usd"))
    expected_loss = _decimal(row.get("expected_loss_at_stop_usd"))
    planned_qty = _decimal(row.get("planned_entry_qty"))
    stop_distance = _decimal(row.get("stop_distance"))
    risk_snapshot_at = _datetime(row.get("risk_snapshot_at"))
    risk_snapshot_source = str(row.get("risk_snapshot_source") or "").strip()
    risk_status = str(row.get("risk_snapshot_status") or "").strip().lower()
    risk_ok = (
        exact_link
        and risk_status in _RISK_OK
        and risk_snapshot_at is not None
        and bool(risk_snapshot_source)
        and equity is not None
        and equity > 0
        and planned_risk_percent is not None
        and planned_risk_percent > 0
        and planned_risk is not None
        and planned_risk > 0
        and price_risk is not None
        and price_risk > 0
        and expected_loss is not None
        and expected_loss > 0
        and planned_qty is not None
        and planned_qty > 0
        and stop_distance is not None
        and stop_distance > 0
    )
    if risk_status not in _RISK_OK:
        reasons.append(f"risk_snapshot_not_complete:{risk_status or 'missing'}")
    if risk_snapshot_at is None:
        reasons.append("missing_risk_snapshot_time")
    if not risk_snapshot_source:
        reasons.append("missing_risk_snapshot_source")
    if equity is None or equity <= 0:
        reasons.append("missing_equity_snapshot")
    if planned_risk_percent is None or planned_risk_percent <= 0:
        reasons.append("missing_planned_risk_percent")
    if planned_risk is None or planned_risk <= 0:
        reasons.append("missing_planned_risk")
    if price_risk is None or price_risk <= 0:
        reasons.append("missing_initial_price_risk")
    if expected_loss is None or expected_loss <= 0:
        reasons.append("missing_expected_loss_at_stop")
    if planned_qty is None or planned_qty <= 0:
        reasons.append("missing_planned_entry_qty")
    if stop_distance is None or stop_distance <= 0:
        reasons.append("missing_stop_distance")

    return QualityGateDecision(
        final_eligible=final_ok,
        simulation_eligible=simulation_ok,
        risk_analysis_eligible=risk_ok,
        reasons=tuple(dict.fromkeys(reasons)),
    )


def evaluate_signal_quality_gate(
    signal: Mapping[str, Any],
    execution_decisions: Sequence[QualityGateDecision],
) -> QualityGateDecision:
    reasons: list[str] = []
    status = str(signal.get("status") or "").strip().lower()
    linkage = str(signal.get("linkage_status") or "").strip().lower()
    recovery = _int(signal.get("needs_recovery")) > 0 or str(
        signal.get("recovery_status") or ""
    ).strip().lower() in {"pending", "required", "unresolved", "ambiguous"}
    ambiguous = bool(str(signal.get("ambiguous_reason") or "").strip())
    targets = _json_list(signal.get("targets_json"))
    allocation_ok = (
        str(signal.get("target_percents_source") or "").strip().lower()
        == "execution_consensus"
        and _valid_tp_distribution(
            signal.get("target_percents_json"), target_count=len(targets)
        )
    )

    if status not in _SIGNAL_TERMINAL:
        reasons.append(f"signal_not_terminal:{status or 'missing'}")
    if linkage not in _EXACT_LINKAGE or _int(signal.get("trade_group_id")) <= 0:
        reasons.append("missing_exact_linkage")
    if recovery:
        reasons.append("signal_recovery_unresolved")
    if ambiguous:
        reasons.append("signal_ambiguous")
    if not execution_decisions:
        reasons.append("missing_execution_link")
    if not targets:
        reasons.append("missing_signal_targets")
    if not allocation_ok:
        reasons.append("missing_signal_tp_execution_consensus")

    base = (
        status in _SIGNAL_TERMINAL
        and linkage in _EXACT_LINKAGE
        and _int(signal.get("trade_group_id")) > 0
        and not recovery
        and not ambiguous
        and bool(execution_decisions)
    )
    final_ok = base and all(item.final_eligible for item in execution_decisions)
    simulation_ok = (
        base
        and bool(targets)
        and allocation_ok
        and all(item.simulation_eligible for item in execution_decisions)
    )
    risk_ok = base and all(
        item.risk_analysis_eligible for item in execution_decisions
    )
    if execution_decisions and not final_ok:
        reasons.append("one_or_more_executions_not_final")
    if execution_decisions and not simulation_ok:
        reasons.append("one_or_more_executions_not_simulation_eligible")
    if execution_decisions and not risk_ok:
        reasons.append("one_or_more_executions_without_risk_snapshot")

    return QualityGateDecision(
        final_eligible=final_ok,
        simulation_eligible=simulation_ok,
        risk_analysis_eligible=risk_ok,
        reasons=tuple(dict.fromkeys(reasons)),
    )


def _positive_ids(values: Iterable[int]) -> tuple[int, ...]:
    result: set[int] = set()
    for value in values:
        parsed = _int(value)
        if parsed > 0:
            result.add(parsed)
    return tuple(sorted(result))


def _execution_select() -> str:
    return """
      SELECT r.*,
             s.status AS signal_status,
             s.needs_recovery AS signal_needs_recovery,
             s.ambiguous_reason AS signal_ambiguous_reason,
             s.targets_json AS signal_targets_json,
             s.target_percents_json AS signal_target_percents_json,
             s.target_percents_source AS signal_target_percents_source,
             e.targets_json AS execution_targets_json,
             CASE WHEN l.id IS NOT NULL
                       AND l.linkage_status='linked_exact'
                       AND l.signal_id=r.analytics_signal_id
                       AND l.trade_group_id=r.trade_group_id
                       AND l.execution_id=r.execution_id
                       AND l.user_id=r.user_id
                       AND UPPER(l.symbol)=UPPER(r.symbol)
                       AND LOWER(l.side)=LOWER(r.side)
                  THEN 1 ELSE 0 END AS immutable_link_verified
      FROM analytics_execution_results r
      LEFT JOIN signal_analytics_signals s ON s.id=r.analytics_signal_id
      LEFT JOIN trade_executions e ON e.id=r.execution_id
      LEFT JOIN statistics_entity_links l ON l.execution_id=r.execution_id
    """


async def _fetch_execution_rows(
    conn: Any, *, execution_ids: tuple[int, ...], include_backlog: bool, limit: int
) -> list[dict[str, Any]]:
    select = _execution_select()
    if db.is_postgres():
        if execution_ids:
            rows = await conn.fetch(
                select + " WHERE r.execution_id=ANY($1::bigint[]) ORDER BY r.execution_id",
                list(execution_ids),
            )
        elif include_backlog:
            rows = await conn.fetch(
                select
                + " WHERE COALESCE(r.quality_gate_version,0)<$1 "
                  "OR r.quality_evaluated_at IS NULL "
                  "OR r.quality_evaluated_at<r.updated_at "
                  "ORDER BY r.execution_id LIMIT $2",
                QUALITY_GATE_VERSION,
                limit,
            )
        else:
            return []
        return [_row(item) for item in rows]
    if execution_ids:
        placeholders = ",".join("?" for _ in execution_ids)
        cursor = await conn.execute(
            select + f" WHERE r.execution_id IN ({placeholders}) ORDER BY r.execution_id",
            execution_ids,
        )
    elif include_backlog:
        cursor = await conn.execute(
            select
            + " WHERE COALESCE(r.quality_gate_version,0)<? "
              "OR r.quality_evaluated_at IS NULL "
              "OR julianday(r.quality_evaluated_at)<julianday(r.updated_at) "
              "ORDER BY r.execution_id LIMIT ?",
            (QUALITY_GATE_VERSION, limit),
        )
    else:
        return []
    return [_row(item) for item in await cursor.fetchall()]


async def evaluate_statistics_final_candidate(
    *, execution_id: int, candidate_values: Mapping[str, Any]
) -> QualityGateDecision:
    """Evaluate an unsaved financial FINAL candidate without mutating state.

    The financial projection worker must not write ``financial_state=FINAL``
    first and only then discover that the independent quality gate rejects the
    row.  This helper loads the same immutable-link/signal context used by the
    durable gate, overlays only the proposed projection values in memory, and
    returns the normal fail-closed decision.

    No BingX call and no database write is performed here.
    """

    parsed_execution_id = _int(execution_id)
    if parsed_execution_id <= 0:
        return QualityGateDecision(
            final_eligible=False,
            simulation_eligible=False,
            risk_analysis_eligible=False,
            reasons=("invalid_execution_id",),
        )
    async with db.connect() as conn:
        rows = await _fetch_execution_rows(
            conn,
            execution_ids=(parsed_execution_id,),
            include_backlog=False,
            limit=1,
        )
    if len(rows) != 1:
        return QualityGateDecision(
            final_eligible=False,
            simulation_eligible=False,
            risk_analysis_eligible=False,
            reasons=("final_candidate_context_missing",),
        )
    candidate = dict(rows[0])
    candidate.update(dict(candidate_values))
    return evaluate_execution_quality_gate(candidate)


async def _update_execution_gate(
    conn: Any,
    *,
    execution_id: int,
    expected_result_version: int,
    expected_updated_at: Any,
    decision: QualityGateDecision,
) -> bool:
    if db.is_postgres():
        result = await conn.execute(
            """
            UPDATE analytics_execution_results SET
              quality_reasons_json=$1,final_eligible=$2,simulation_eligible=$3,
              risk_analysis_eligible=$4,quality_gate_version=$5,
              quality_evaluated_at=NOW()
            WHERE execution_id=$6 AND result_version=$7 AND updated_at=$8
            """,
            decision.reasons_json(),
            int(decision.final_eligible),
            int(decision.simulation_eligible),
            int(decision.risk_analysis_eligible),
            QUALITY_GATE_VERSION,
            execution_id,
            expected_result_version,
            expected_updated_at,
        )
        return str(result).endswith(" 1")
    cursor = await conn.execute(
        """
        UPDATE analytics_execution_results SET
          quality_reasons_json=?,final_eligible=?,simulation_eligible=?,
          risk_analysis_eligible=?,quality_gate_version=?,
          quality_evaluated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
        WHERE execution_id=? AND result_version=? AND updated_at=?
        """,
        (
            decision.reasons_json(),
            int(decision.final_eligible),
            int(decision.simulation_eligible),
            int(decision.risk_analysis_eligible),
            QUALITY_GATE_VERSION,
            execution_id,
            expected_result_version,
            expected_updated_at,
        ),
    )
    return int(getattr(cursor, "rowcount", 0) or 0) == 1


async def _fetch_stale_signal_ids(conn: Any, *, limit: int) -> tuple[int, ...]:
    if db.is_postgres():
        rows = await conn.fetch(
            """
            SELECT id FROM signal_analytics_signals
            WHERE COALESCE(quality_gate_version,0)<$1
               OR quality_evaluated_at IS NULL
               OR quality_evaluated_at<updated_at
            ORDER BY id LIMIT $2
            """,
            QUALITY_GATE_VERSION,
            limit,
        )
        return tuple(int(item["id"]) for item in rows)
    cursor = await conn.execute(
        """
        SELECT id FROM signal_analytics_signals
        WHERE COALESCE(quality_gate_version,0)<?
           OR quality_evaluated_at IS NULL
           OR julianday(quality_evaluated_at)<julianday(updated_at)
        ORDER BY id LIMIT ?
        """,
        (QUALITY_GATE_VERSION, limit),
    )
    return tuple(int(item[0]) for item in await cursor.fetchall())


async def _fetch_signals(conn: Any, signal_ids: tuple[int, ...]) -> list[dict[str, Any]]:
    if not signal_ids:
        return []
    if db.is_postgres():
        rows = await conn.fetch(
            "SELECT * FROM signal_analytics_signals WHERE id=ANY($1::bigint[]) ORDER BY id",
            list(signal_ids),
        )
        return [_row(item) for item in rows]
    placeholders = ",".join("?" for _ in signal_ids)
    cursor = await conn.execute(
        f"SELECT * FROM signal_analytics_signals WHERE id IN ({placeholders}) ORDER BY id",
        signal_ids,
    )
    return [_row(item) for item in await cursor.fetchall()]


async def _fetch_signal_execution_rows(
    conn: Any, signal_id: int
) -> list[dict[str, Any]]:
    select = _execution_select()
    if db.is_postgres():
        rows = await conn.fetch(
            select + " WHERE r.analytics_signal_id=$1 ORDER BY r.execution_id",
            signal_id,
        )
        return [_row(item) for item in rows]
    cursor = await conn.execute(
        select + " WHERE r.analytics_signal_id=? ORDER BY r.execution_id",
        (signal_id,),
    )
    return [_row(item) for item in await cursor.fetchall()]


async def _update_signal_gate(
    conn: Any,
    *,
    signal_id: int,
    expected_state_version: int,
    expected_updated_at: Any,
    decision: QualityGateDecision,
) -> bool:
    if db.is_postgres():
        result = await conn.execute(
            """
            UPDATE signal_analytics_signals SET
              quality_reasons_json=$1,final_eligible=$2,simulation_eligible=$3,
              risk_analysis_eligible=$4,quality_gate_version=$5,
              quality_evaluated_at=NOW()
            WHERE id=$6 AND state_version=$7 AND updated_at=$8
            """,
            decision.reasons_json(),
            int(decision.final_eligible),
            int(decision.simulation_eligible),
            int(decision.risk_analysis_eligible),
            QUALITY_GATE_VERSION,
            signal_id,
            expected_state_version,
            expected_updated_at,
        )
        return str(result).endswith(" 1")
    cursor = await conn.execute(
        """
        UPDATE signal_analytics_signals SET
          quality_reasons_json=?,final_eligible=?,simulation_eligible=?,
          risk_analysis_eligible=?,quality_gate_version=?,
          quality_evaluated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
        WHERE id=? AND state_version=? AND updated_at=?
        """,
        (
            decision.reasons_json(),
            int(decision.final_eligible),
            int(decision.simulation_eligible),
            int(decision.risk_analysis_eligible),
            QUALITY_GATE_VERSION,
            signal_id,
            expected_state_version,
            expected_updated_at,
        ),
    )
    return int(getattr(cursor, "rowcount", 0) or 0) == 1


async def refresh_statistics_quality_gates(
    *,
    execution_ids: Iterable[int] = (),
    signal_ids: Iterable[int] = (),
    include_backlog: bool = False,
    limit: int = 1000,
) -> QualityGateRefreshResult:
    """Refresh bounded durable gate decisions after linkage/financial updates."""

    if not bool(get_settings().STATISTICS_QUALITY_ENABLED):
        return QualityGateRefreshResult()
    execution_ids_t = _positive_ids(execution_ids)
    explicit_signal_ids = set(_positive_ids(signal_ids))
    bounded = max(1, min(10_000, int(limit or 1)))

    async with db.connect() as conn:
        execution_rows = await _fetch_execution_rows(
            conn,
            execution_ids=execution_ids_t,
            include_backlog=include_backlog,
            limit=bounded,
        )
        signal_ids_to_refresh = set(explicit_signal_ids)
        if include_backlog:
            signal_ids_to_refresh.update(
                await _fetch_stale_signal_ids(conn, limit=bounded)
            )

        execution_refreshed = 0
        execution_cas_conflicts = 0
        final_count = simulation_count = risk_count = 0
        for row in execution_rows:
            execution_id = _int(row.get("execution_id"))
            if execution_id <= 0:
                continue
            decision = evaluate_execution_quality_gate(row)
            saved = await _update_execution_gate(
                conn,
                execution_id=execution_id,
                expected_result_version=_int(row.get("result_version")),
                expected_updated_at=row.get("updated_at"),
                decision=decision,
            )
            if not saved:
                execution_cas_conflicts += 1
                continue
            execution_refreshed += 1
            signal_id = _int(row.get("analytics_signal_id"))
            if signal_id > 0:
                signal_ids_to_refresh.add(signal_id)
            final_count += int(decision.final_eligible)
            simulation_count += int(decision.simulation_eligible)
            risk_count += int(decision.risk_analysis_eligible)

        signals = await _fetch_signals(conn, tuple(sorted(signal_ids_to_refresh)))
        signal_refreshed = 0
        signal_cas_conflicts = 0
        for signal in signals:
            signal_id = _int(signal.get("id"))
            linked_rows = await _fetch_signal_execution_rows(conn, signal_id)
            decisions = [evaluate_execution_quality_gate(item) for item in linked_rows]
            decision = evaluate_signal_quality_gate(signal, decisions)
            saved = await _update_signal_gate(
                conn,
                signal_id=signal_id,
                expected_state_version=_int(signal.get("state_version")),
                expected_updated_at=signal.get("updated_at"),
                decision=decision,
            )
            if saved:
                signal_refreshed += 1
            else:
                signal_cas_conflicts += 1

        if not db.is_postgres():
            await conn.commit()
        return QualityGateRefreshResult(
            executions_refreshed=execution_refreshed,
            signals_refreshed=signal_refreshed,
            final_eligible_executions=final_count,
            simulation_eligible_executions=simulation_count,
            risk_eligible_executions=risk_count,
            execution_cas_conflicts=execution_cas_conflicts,
            signal_cas_conflicts=signal_cas_conflicts,
        )
