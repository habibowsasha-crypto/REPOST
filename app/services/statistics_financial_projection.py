"""Durable statistics-v2 financial projection for exact BingX executions.

Step g5b3g15 extends the existing single low-priority financial reconciliation
worker.  It does not create a second worker and never participates in ENTRY,
STOP, TP, BE, sizing, risk-slot, or public-price hot paths.

The module projects already-durable exact fills into ``analytics_execution_results``
and, under a separate disabled-by-default flag, reads BingX ``FUNDING_FEE``
income records.  All joins and calculations are fail-closed: ambiguous identity,
volume, asset, chronology or funding attribution never becomes FINAL.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

from app.config import get_settings
from app.database import db
from app.exchanges.bingx.adapter import BingxResponseIntegrityError
from app.services.financial_reconciliation_models import (
    FINANCIAL_STATUS_AMBIGUOUS,
    FINANCIAL_STATUS_CONFIRMED,
    FINANCIAL_STATUS_PARTIAL,
    FINANCIAL_STATUS_UNAVAILABLE,
    ORDER_ROLE_BE_STOP,
    ORDER_ROLE_ENTRY,
    ORDER_ROLE_FINAL_TP,
    ORDER_ROLE_STOP,
    ORDER_ROLE_TP,
    FinancialFillRecord,
    aggregate_fill_records,
    normalize_fill_records,
)
from app.services.funding_g59_recovery_rearm import (
    G59_FUNDING_PENDING,
    G59_FUNDING_PROCESSING,
    G59_REARM_MARKER,
)
from app.services.funding_g60_recovery_rearm import (
    G60_FUNDING_PENDING,
    G60_FUNDING_PROCESSING,
    G60_REARM_MARKER,
)
from app.services.statistics_quality_gate import (
    QUALITY_GATE_VERSION,
    evaluate_statistics_final_candidate,
)

log = logging.getLogger(__name__)

PROJECTION_PENDING = "pending"
PROJECTION_PROCESSING = "processing"
PROJECTION_RETRY = "retry"
PROJECTION_COMPLETE = "complete"
PROJECTION_AMBIGUOUS = "ambiguous"
PROJECTION_UNAVAILABLE = "unavailable"

FUNDING_NOT_CHECKED = "not_checked"
FUNDING_PENDING = "pending"
FUNDING_CONFIRMED = "confirmed"
FUNDING_CONFIRMED_ZERO = "confirmed_zero"
FUNDING_AMBIGUOUS = "ambiguous"
FUNDING_UNAVAILABLE = "unavailable"
FUNDING_MANUAL_REVIEW = "manual_review"
FUNDING_NOT_APPLICABLE = "not_applicable"

FINANCIAL_PENDING = "PENDING"
FINANCIAL_PROVISIONAL = "PROVISIONAL"
FINANCIAL_FINAL = "FINAL"
FINANCIAL_AMBIGUOUS = "AMBIGUOUS"
FINANCIAL_UNAVAILABLE = "UNAVAILABLE"

_BINGX_FUNDING_ENDPOINT = "/openApi/swap/v2/user/income"
_RETRYABLE_NONE_LIST_REASON = "expected list payload, got NoneType"
_G45_NONE_INTEGRITY_AMBIGUITY = (
    "statistics_funding_integrity_ambiguous:"
    "BingX response integrity check failed for "
    f"{_BINGX_FUNDING_ENDPOINT}: {_RETRYABLE_NONE_LIST_REASON}"
)
_G47_EXHAUSTED_NONE_NEEDLE = (
    "statistics_funding_integrity_retryable:"
    "BingX response integrity check failed for "
    f"{_BINGX_FUNDING_ENDPOINT}: {_RETRYABLE_NONE_LIST_REASON}"
)
# Production evidence from Period 3 identified exactly two exhausted rows.  G47
# may re-arm these IDs once, only from the exact seven-attempt fail-closed state.
# This is intentionally not a generic manual-review retry switch.
_G47_TARGETED_EXECUTION_IDS = frozenset({1483, 1485})
_G47_TARGETED_SOURCE_ATTEMPTS = 7
_G47_RECOVERY_VARIANTS = (
    "exact",
    "symbol_all_types",
    "all_symbols_funding",
)
_G47_RECOVERY_MIN_QUORUM = 2
_G47_REARM_MARKER = "g47_targeted_exhausted_none_rearmed_once"
# G48 adds one independent unfiltered income view and may re-arm exactly one
# attempt for any exact-volume exhausted NoneType row at the configured maximum.
# The attempt counter is preserved, so a failed G48 attempt cannot loop.
_G48_RECOVERY_VARIANTS = (
    "exact",
    "symbol_all_types",
    "all_symbols_funding",
    "all_income_unfiltered",
)
_G48_RECOVERY_MIN_QUORUM = 2
_G48_REARM_MARKER = "g48_unfiltered_income_rearmed_once"
_G49_REARM_MARKER = "g49_g47_quorum_selector_rearmed_once"
_G49_G47_QUORUM_PREFIX = (
    "funding_recovery_exhausted:attempts=8:deadline_expired=0:"
    "statistics_funding_request_failed:g47_recovery_quorum_not_met:matching=1/2:"
)
# G51 repairs one exact production row that exhausted on the deadline before
# reaching the configured maximum.  It remains reserved for execution 1504 so
# the already verified production path and its regression contract stay intact.
_G51_TARGETED_EXECUTION_IDS = frozenset({1504})
_G51_TARGETED_SOURCE_ATTEMPTS = 7
_G51_REARM_MARKER = "g51_deadline_expired_none_rearmed_once"
_G51_DEADLINE_NONE_REASON = (
    "funding_recovery_exhausted:attempts=7:deadline_expired=1:"
    + _G47_EXHAUSTED_NONE_NEEDLE
)
# G52 generalizes the same exact fail-closed recovery signature for future rows.
# It deliberately excludes the G51 target and accepts no broad manual-review
# state: every evidence field and the complete strict reason must match.  The
# preserved counter advances 7 -> 8 through the existing G48 read-only quorum,
# so a failed G52 attempt cannot loop through this selector again.
_G52_DEADLINE_SOURCE_ATTEMPTS = 7
_G52_REARM_MARKER = "g52_generic_deadline_expired_none_rearmed_once"
_G52_DEADLINE_NONE_REASON = _G51_DEADLINE_NONE_REASON
_G52_RESERVED_EXECUTION_IDS = frozenset(
    _G47_TARGETED_EXECUTION_IDS | _G51_TARGETED_EXECUTION_IDS
)
# G57 handles one exact BingX integrity failure shape observed in production:
# the endpoint accepted an incomeType=FUNDING_FEE request but returned at least
# one row with a different incomeType.  The ordinary exact reader must keep
# rejecting that response.  G57 only re-arms rows that stopped on this precise
# internally generated reason, then routes them through the existing G48
# multi-scope read-only quorum.
_G57_INCOME_TYPE_SCOPE_PREFIX = (
    "statistics_funding_integrity_ambiguous:"
    "BingX response integrity check failed for "
    f"{_BINGX_FUNDING_ENDPOINT}: row["
)
_G57_INCOME_TYPE_SCOPE_SUFFIX = "].incomeType is outside exact request scope"
_G57_INCOME_TYPE_SCOPE_SQL_LIKE = (
    _G57_INCOME_TYPE_SCOPE_PREFIX + "%" + _G57_INCOME_TYPE_SCOPE_SUFFIX
)
_G57_SOURCE_ATTEMPTS = 1
_G57_REARM_MARKER = "g57_income_type_scope_rearmed_once"
_G57_FALLBACK_PENDING = "g57_scope_fallback_pending"
_G57_FALLBACK_PROCESSING = "g57_scope_fallback_processing"

_CLOSE_ROLES = frozenset(
    {ORDER_ROLE_TP, ORDER_ROLE_FINAL_TP, ORDER_ROLE_STOP, ORDER_ROLE_BE_STOP}
)

BIT_LINKAGE = 1
BIT_TERMINAL = 2
BIT_ENTRY_FILLS = 4
BIT_EXIT_FILLS = 8
BIT_VOLUME_PARITY = 16
BIT_FEES = 32
BIT_FUNDING = 64
BIT_INITIAL_RISK = 128
BIT_CHRONOLOGY = 256
_ALL_COMPLETENESS_BITS = (
    BIT_LINKAGE,
    BIT_TERMINAL,
    BIT_ENTRY_FILLS,
    BIT_EXIT_FILLS,
    BIT_VOLUME_PARITY,
    BIT_FEES,
    BIT_FUNDING,
    BIT_INITIAL_RISK,
    BIT_CHRONOLOGY,
)


class _FundingAdapter(Protocol):
    async def fetch_funding_income(
        self,
        *,
        symbol: str,
        start_time_ms: int,
        end_time_ms: int,
        limit: int = 1000,
    ) -> list[dict[str, Any]] | None: ...

    async def fetch_funding_income_recovery_variant(
        self,
        *,
        symbol: str,
        start_time_ms: int,
        end_time_ms: int,
        limit: int,
        variant: str,
    ) -> Mapping[str, Any]: ...


class _RateLimiter(Protocol):
    async def wait(self) -> None: ...


@dataclass(frozen=True, slots=True)
class StatisticsFinancialOutcome:
    execution_id: int
    action: str
    projection_status: str
    financial_state: str
    funding_state: str
    attempts: int
    error: str = ""


class FundingRequestFailed(RuntimeError):
    """Funding endpoint did not return a trustworthy list response."""


@dataclass(frozen=True, slots=True)
class FundingRecord:
    exchange_event_id: str
    symbol: str
    amount_signed: Decimal
    asset: str
    event_time: datetime
    metadata_json: str


@dataclass(frozen=True, slots=True)
class FundingRecoveryQuorum:
    rows: tuple[dict[str, Any], ...]
    confirmations: int
    fingerprint: str
    variants: tuple[str, ...]
    diagnostics_json: str


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_text(value: Any, *, limit: int = 800) -> str:
    text = str(value or "").replace("\x00", "").replace("\r", " ").replace("\n", " ")
    return " ".join(text.split())[:limit]


def _is_retryable_funding_integrity_error(exc: BingxResponseIntegrityError) -> bool:
    """Allow retry only for the exact transient funding collection envelope.

    ``data: null`` is not an empty collection and never means zero funding.  It
    is, however, a transport/envelope integrity failure that can be retried by
    the existing bounded durable schedule.  Every other integrity failure
    remains fail-closed manual review.
    """

    return (
        str(getattr(exc, "endpoint", "")) == _BINGX_FUNDING_ENDPOINT
        and str(getattr(exc, "reason", "")) == _RETRYABLE_NONE_LIST_REASON
    )


def _decimal(value: Any, *, allow_none: bool = False) -> Decimal | None:
    if value in (None, ""):
        if allow_none:
            return None
        raise ValueError("numeric value is required")
    if isinstance(value, bool):
        raise ValueError("boolean is not numeric")
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("numeric value is invalid") from exc
    if not parsed.is_finite():
        raise ValueError("numeric value is not finite")
    return parsed


def _plain(value: Decimal | None) -> str | None:
    if value is None:
        return None
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in {"", "-0"} else text


def _datetime(value: Any, *, allow_none: bool = True) -> datetime | None:
    if value in (None, ""):
        if allow_none:
            return None
        raise ValueError("datetime is required")
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
        except ValueError as exc:
            raise ValueError("datetime is invalid") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(timezone.utc).isoformat() if value is not None else None


def _canonical_symbol(value: Any) -> str:
    text = str(value or "").strip().upper().replace("-", "").replace("_", "").replace("/", "")
    if not text or not text.isascii() or not text.isalnum() or len(text) > 40:
        raise ValueError("invalid canonical symbol")
    if not (text.endswith("USDT") or text.endswith("USDC")):
        raise ValueError("unsupported settlement symbol")
    return text


def _settlement_asset(symbol: str) -> str:
    return "USDT" if symbol.endswith("USDT") else "USDC"


def _weighted_average(fills: Sequence[FinancialFillRecord]) -> tuple[Decimal, Decimal]:
    qty = sum((_decimal(item.qty) or Decimal()) for item in fills)
    if qty <= 0:
        raise ValueError("weighted average quantity must be positive")
    notional = sum(
        ((_decimal(item.price) or Decimal()) * (_decimal(item.qty) or Decimal()))
        for item in fills
    )
    return qty, notional / qty


def _fill_times(fills: Sequence[FinancialFillRecord]) -> tuple[datetime, datetime]:
    times = [_datetime(item.fill_time, allow_none=False) for item in fills]
    assert all(item is not None for item in times)
    concrete = [item for item in times if item is not None]
    return min(concrete), max(concrete)


def _bps_slippage(*, side: str, actual: Decimal, reference: Decimal | None) -> Decimal | None:
    if reference is None or reference <= 0:
        return None
    if side == "long":
        return (actual - reference) / reference * Decimal("10000")
    if side == "short":
        return (reference - actual) / reference * Decimal("10000")
    raise ValueError("side must be long or short")


def _canonical_terminal(close_type: Any, max_tp: int) -> tuple[str, str]:
    value = str(close_type or "").strip().lower()
    if value == "all_tps":
        return "ALL_TPS", "all_targets"
    suffix = "no_tp" if max_tp <= 0 else f"after_tp{max_tp}"
    if value == "be_stop":
        return "BE", f"be_{suffix}"
    if value == "stop":
        return "STOP", f"stop_{suffix}"
    return "UNKNOWN", "unresolved_history"


def _completeness(mask: int) -> Decimal:
    passed = sum(1 for bit in _ALL_COMPLETENESS_BITS if mask & bit)
    return Decimal(passed) * Decimal("100") / Decimal(len(_ALL_COMPLETENESS_BITS))


def _as_fill_records(rows: Sequence[Mapping[str, Any]]) -> list[FinancialFillRecord]:
    normalized: list[FinancialFillRecord] = []
    for row in rows:
        mapping = dict(row)
        mapping.update(
            {
                "trade_id": row.get("trade_id"),
                "order_id": row.get("order_id"),
                "role": row.get("role"),
                "tp_index": row.get("tp_index"),
                "symbol": row.get("symbol"),
                "side": row.get("side"),
                "price": row.get("price"),
                "qty": row.get("qty"),
                "realized_pnl": row.get("realized_pnl"),
                "fee": row.get("fee"),
                "fee_asset": row.get("fee_asset"),
                "fill_time": row.get("fill_time"),
                "metadata_json": row.get("metadata_json"),
            }
        )
        normalized.append(FinancialFillRecord.from_mapping(mapping))
    return normalize_fill_records(normalized)


def build_trading_projection(
    *,
    projection: Mapping[str, Any],
    execution: Mapping[str, Any],
    job: Mapping[str, Any],
    fill_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Build the deterministic trading/fee part of one execution projection."""

    status = str(job.get("status") or "").strip().lower()
    side = str(execution.get("side") or projection.get("side") or "").strip().lower()
    symbol = _canonical_symbol(execution.get("symbol") or projection.get("symbol"))
    if side not in {"long", "short"}:
        raise ValueError("execution side is invalid")

    base: dict[str, Any] = {
        "trading_reconciliation_state": status or "pending",
        "financial_state": FINANCIAL_PENDING,
        "funding_state": str(projection.get("funding_state") or FUNDING_NOT_CHECKED),
        "data_quality_status": "pending",
        "ambiguity_reason": None,
        "settlement_asset": None,
        "funding_signed": projection.get("funding_signed"),
        "net_pnl": None,
        "result_r": None,
    }
    if status == FINANCIAL_STATUS_AMBIGUOUS:
        base.update(
            financial_state=FINANCIAL_AMBIGUOUS,
            data_quality_status="ambiguous",
            ambiguity_reason=_safe_text(job.get("last_error") or "trading_reconciliation_ambiguous"),
        )
        return base
    if status == FINANCIAL_STATUS_UNAVAILABLE:
        base.update(
            financial_state=FINANCIAL_UNAVAILABLE,
            data_quality_status="unavailable",
            ambiguity_reason=_safe_text(job.get("last_error") or "trading_history_unavailable"),
        )
        return base
    if status not in {FINANCIAL_STATUS_CONFIRMED, FINANCIAL_STATUS_PARTIAL}:
        return base

    fills = _as_fill_records(fill_rows)
    entry_fills = [item for item in fills if item.role == ORDER_ROLE_ENTRY]
    exit_fills = [item for item in fills if item.role in _CLOSE_ROLES]
    if not entry_fills or not exit_fills:
        state = FINANCIAL_PROVISIONAL if fills else FINANCIAL_UNAVAILABLE
        base.update(
            financial_state=state,
            data_quality_status="partial" if fills else "unavailable",
            ambiguity_reason="required_entry_or_exit_fills_missing",
        )
        return base

    entry_qty, entry_avg = _weighted_average(entry_fills)
    exit_qty, exit_avg = _weighted_average(exit_fills)
    entry_first, entry_last = _fill_times(entry_fills)
    exit_first, exit_last = _fill_times(exit_fills)
    chronology_valid = entry_first <= entry_last <= exit_last and entry_first <= exit_first

    tolerance = max(Decimal("0.000000000001"), entry_qty.copy_abs() * Decimal("0.000000000001"))
    qty_diff = exit_qty - entry_qty
    if qty_diff == 0:
        volume_status = "exact"
    elif qty_diff.copy_abs() <= tolerance:
        volume_status = "within_tolerance"
    else:
        volume_status = "mismatch"

    aggregate = aggregate_fill_records(fills)
    gross = _decimal(aggregate["exchange_gross_pnl"])
    fees = _decimal(aggregate["total_trading_fee"])
    provisional_net = _decimal(aggregate["net_pnl_after_trading_fee"])
    fee_asset = str(aggregate.get("fee_asset") or "").strip().upper() or None
    expected_asset = _settlement_asset(symbol)
    mixed_assets = bool(aggregate.get("mixed_fee_assets"))
    if mixed_assets or fee_asset != expected_asset:
        base.update(
            financial_state=FINANCIAL_AMBIGUOUS,
            data_quality_status="ambiguous",
            ambiguity_reason="mixed_or_wrong_fee_asset",
        )
        return base

    persisted_gross = _decimal(job.get("exchange_gross_pnl"), allow_none=True)
    persisted_fee = _decimal(job.get("total_trading_fee"), allow_none=True)
    persisted_net = _decimal(job.get("net_pnl_after_trading_fee"), allow_none=True)
    compare_tolerance = Decimal("0.00000001")
    if (
        persisted_gross is None
        or persisted_fee is None
        or persisted_net is None
        or (persisted_gross - gross).copy_abs() > compare_tolerance
        or (persisted_fee - fees).copy_abs() > compare_tolerance
        or (persisted_net - provisional_net).copy_abs() > compare_tolerance
    ):
        base.update(
            financial_state=FINANCIAL_AMBIGUOUS,
            data_quality_status="ambiguous",
            ambiguity_reason="financial_job_fill_aggregate_mismatch",
        )
        return base

    stop = _decimal(
        projection.get("initial_stop_price")
        if projection.get("initial_stop_price") not in (None, "")
        else execution.get("stop"),
        allow_none=True,
    )
    initial_risk = (
        (entry_avg - stop).copy_abs() * entry_qty
        if stop is not None and stop > 0 and stop != entry_avg
        else None
    )
    planned_reference = _decimal(projection.get("planned_entry_reference"), allow_none=True)
    execution_reference = _decimal(
        projection.get("execution_reference_price")
        if projection.get("execution_reference_price") not in (None, "")
        else execution.get("entry"),
        allow_none=True,
    )
    entry_slippage = _bps_slippage(side=side, actual=entry_avg, reference=planned_reference)
    limit_slippage = None
    if str(projection.get("entry_order_type") or "").strip().upper() == "LIMIT":
        limit_slippage = _bps_slippage(
            side=side, actual=entry_avg, reference=execution_reference
        )

    max_tp = max(
        [int(item.tp_index or 0) for item in exit_fills if item.role in {ORDER_ROLE_TP, ORDER_ROLE_FINAL_TP}]
        or [0]
    )
    terminal_reason, terminal_detail = _canonical_terminal(job.get("close_type"), max_tp)
    source = "exchange_realized_pnl"
    for item in fills:
        try:
            metadata = json.loads(item.metadata_json or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            metadata = {}
        if str(metadata.get("realized_pnl_source") or "") == "derived_from_fill_prices":
            source = "derived_from_fill_prices"
            break

    mask = 0
    if str(projection.get("linkage_status") or "") == "linked_exact":
        mask |= BIT_LINKAGE
    mask |= BIT_TERMINAL | BIT_ENTRY_FILLS | BIT_EXIT_FILLS | BIT_FEES
    if volume_status in {"exact", "within_tolerance"}:
        mask |= BIT_VOLUME_PARITY
    if initial_risk is not None and initial_risk > 0:
        mask |= BIT_INITIAL_RISK
    if chronology_valid:
        mask |= BIT_CHRONOLOGY

    ambiguity = None
    financial_state = FINANCIAL_PROVISIONAL
    quality = "partial"
    if not chronology_valid:
        ambiguity = "fill_chronology_invalid"
        financial_state = FINANCIAL_AMBIGUOUS
        quality = "ambiguous"
    elif qty_diff > tolerance:
        ambiguity = "exit_qty_exceeds_entry_qty"
        financial_state = FINANCIAL_AMBIGUOUS
        quality = "ambiguous"
    elif volume_status == "mismatch":
        ambiguity = "entry_exit_volume_mismatch"
        quality = "partial"
    elif status == FINANCIAL_STATUS_PARTIAL:
        ambiguity = "trading_reconciliation_partial"
        quality = "partial"

    provisional_r = (
        provisional_net / initial_risk
        if initial_risk is not None and initial_risk > 0
        else None
    )
    fee_cost = max(Decimal(), -fees)

    base.update(
        first_entry_fill_at=_iso(entry_first),
        last_entry_fill_at=_iso(entry_last),
        actual_entry_qty=_plain(entry_qty),
        actual_entry_avg_price=_plain(entry_avg),
        first_exit_fill_at=_iso(exit_first),
        last_exit_fill_at=_iso(exit_last),
        actual_exit_qty=_plain(exit_qty),
        actual_exit_avg_price=_plain(exit_avg),
        execution_max_tp_index=max_tp,
        canonical_terminal_reason=terminal_reason,
        terminal_detail=terminal_detail,
        strategy_gross_pnl=_plain(_decimal(job.get("strategy_gross_pnl"), allow_none=True)),
        exchange_gross_pnl=_plain(gross),
        gross_pnl_source=source,
        trading_fee_signed=_plain(fees),
        trading_fee_cost=_plain(fee_cost),
        settlement_asset=fee_asset,
        provisional_net_pnl=_plain(provisional_net),
        provisional_result_r=_plain(provisional_r),
        initial_price_risk_usd=_plain(initial_risk),
        entry_slippage_bps=_plain(entry_slippage),
        limit_price_slippage_bps=_plain(limit_slippage),
        execution_duration_seconds=max(0, int((exit_last - entry_first).total_seconds())),
        volume_parity_status=volume_status,
        completeness_mask=mask,
        completeness_percent=_plain(_completeness(mask)),
        financial_state=financial_state,
        data_quality_status=quality,
        ambiguity_reason=ambiguity,
    )
    return base


def normalize_funding_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    symbol: str,
    start_time: datetime,
    end_time: datetime,
) -> list[FundingRecord]:
    canonical = _canonical_symbol(symbol)
    asset = _settlement_asset(canonical)
    result: dict[str, FundingRecord] = {}
    for index, raw in enumerate(rows):
        row_symbol = _canonical_symbol(raw.get("symbol"))
        if row_symbol != canonical:
            raise ValueError(f"funding row[{index}] cross-symbol")
        if str(raw.get("incomeType") or "").strip().upper() != "FUNDING_FEE":
            raise ValueError(f"funding row[{index}] wrong incomeType")
        row_asset = str(raw.get("asset") or "").strip().upper()
        if row_asset != asset:
            raise ValueError(f"funding row[{index}] wrong settlement asset")
        amount = _decimal(raw.get("income"))
        raw_time = raw.get("time")
        if isinstance(raw_time, bool):
            raise ValueError(f"funding row[{index}] invalid time")
        try:
            event_millis = int(str(raw_time).strip())
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"funding row[{index}] invalid time") from exc
        if event_millis <= 0:
            raise ValueError(f"funding row[{index}] invalid time")
        event_time = datetime.fromtimestamp(event_millis / 1000, tz=timezone.utc)
        if event_time < start_time or event_time > end_time:
            # Query includes a transport safety margin; only events during the
            # exact open-position interval are eligible for attribution.
            continue
        event_id = str(
            raw.get("exchangeEventId")
            or raw.get("tranId")
            or raw.get("tradeId")
            or ""
        ).strip()
        identity_source = str(raw.get("identitySource") or "exchange").strip()
        if not event_id:
            identity_payload = {
                "asset": row_asset,
                "income": _plain(amount or Decimal()),
                "incomeType": "FUNDING_FEE",
                "symbol": canonical,
                "time": event_millis,
            }
            event_id = "derived-funding:" + hashlib.sha256(
                json.dumps(
                    identity_payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
            identity_source = "derived_canonical_fields"
        if len(event_id) > 200 or any(ch in event_id for ch in "\x00\r\n"):
            raise ValueError(f"funding row[{index}] invalid stable event identity")
        metadata = (
            dict(raw.get("raw"))
            if isinstance(raw.get("raw"), Mapping)
            else dict(raw)
        )
        metadata["statistics_identity_source"] = identity_source
        record = FundingRecord(
            exchange_event_id=event_id,
            symbol=canonical,
            amount_signed=amount or Decimal(),
            asset=row_asset,
            event_time=event_time,
            metadata_json=json.dumps(
                dict(metadata), ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
            ),
        )
        previous = result.get(event_id)
        if previous is not None and previous != record:
            raise ValueError(f"conflicting duplicate funding event {event_id}")
        result[event_id] = record
    return sorted(result.values(), key=lambda item: (item.event_time, item.exchange_event_id))



def _funding_records_fingerprint(records: Sequence[FundingRecord]) -> str:
    payload = [
        {
            "asset": item.asset,
            "event_id": item.exchange_event_id,
            "income": _plain(item.amount_signed),
            "symbol": item.symbol,
            "time": _iso(item.event_time),
        }
        for item in records
    ]
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _is_g47_targeted_exhausted_row(row: Mapping[str, Any]) -> bool:
    execution_id = int(row.get("execution_id") or 0)
    reason = str(row.get("ambiguity_reason") or "")
    return (
        execution_id in _G47_TARGETED_EXECUTION_IDS
        and str(row.get("projection_status") or "") == PROJECTION_UNAVAILABLE
        and str(row.get("funding_state") or "") == FUNDING_MANUAL_REVIEW
        and str(row.get("financial_state") or "") == FINANCIAL_PROVISIONAL
        and str(row.get("volume_parity_status") or "")
        in {"exact", "within_tolerance"}
        and int(row.get("funding_event_count") or 0) == 0
        and int(row.get("funding_zero_observations") or 0) == 0
        and int(row.get("funding_recovery_attempts") or 0)
        == _G47_TARGETED_SOURCE_ATTEMPTS
        and reason.startswith("funding_recovery_exhausted:")
        and _G47_EXHAUSTED_NONE_NEEDLE in reason
    )


def _is_g51_deadline_exhausted_none_row(row: Mapping[str, Any]) -> bool:
    """Allow one exact G51 recovery for production execution 1504.

    The row stopped at attempt 7 only because its durable deadline expired.
    Every state/evidence field and the complete strict NoneType reason must
    match exactly.  The attempt counter is preserved, so the G48 quorum runs
    as attempt 8 and cannot loop through this selector again.
    """

    return (
        int(row.get("execution_id") or 0) in _G51_TARGETED_EXECUTION_IDS
        and str(row.get("projection_status") or "") == PROJECTION_UNAVAILABLE
        and str(row.get("funding_state") or "") == FUNDING_MANUAL_REVIEW
        and str(row.get("financial_state") or "") == FINANCIAL_PROVISIONAL
        and str(row.get("volume_parity_status") or "")
        in {"exact", "within_tolerance"}
        and int(row.get("funding_event_count") or 0) == 0
        and int(row.get("funding_zero_observations") or 0) == 0
        and int(row.get("funding_recovery_attempts") or 0)
        == _G51_TARGETED_SOURCE_ATTEMPTS
        and str(row.get("ambiguity_reason") or "")
        == _G51_DEADLINE_NONE_REASON
    )


def _is_g52_deadline_exhausted_none_row(row: Mapping[str, Any]) -> bool:
    """Allow one generic G52 recovery for the exact deadline-expired signature.

    G52 excludes the already verified G51 execution 1504, requires the same
    complete production state/evidence and preserves attempt 7.  The G48
    read-only quorum therefore runs once as attempt 8; any success must still
    pass the independent quality gate and any failure remains fail-closed.
    """

    return (
        int(row.get("execution_id") or 0) not in _G52_RESERVED_EXECUTION_IDS
        and str(row.get("projection_status") or "") == PROJECTION_UNAVAILABLE
        and str(row.get("funding_state") or "") == FUNDING_MANUAL_REVIEW
        and str(row.get("financial_state") or "") == FINANCIAL_PROVISIONAL
        and str(row.get("volume_parity_status") or "")
        in {"exact", "within_tolerance"}
        and int(row.get("funding_event_count") or 0) == 0
        and int(row.get("funding_zero_observations") or 0) == 0
        and int(row.get("funding_recovery_attempts") or 0)
        == _G52_DEADLINE_SOURCE_ATTEMPTS
        and str(row.get("ambiguity_reason") or "")
        == _G52_DEADLINE_NONE_REASON
    )


def _is_g57_income_type_scope_row(row: Mapping[str, Any]) -> bool:
    """Return whether the exact G57 integrity-recovery signature matches.

    This is intentionally narrower than a generic manual-review retry.  The
    strict exact funding reader must have failed on an internally generated
    ``row[N].incomeType is outside exact request scope`` reason, with no
    persisted funding evidence and exactly one prior funding attempt.  G57
    then gets one durable re-arm state and uses only the existing read-only G48
    multi-scope quorum.
    """

    reason = str(row.get("ambiguity_reason") or "")
    if not (
        reason.startswith(_G57_INCOME_TYPE_SCOPE_PREFIX)
        and reason.endswith(_G57_INCOME_TYPE_SCOPE_SUFFIX)
    ):
        return False
    row_index = reason[
        len(_G57_INCOME_TYPE_SCOPE_PREFIX) : -len(_G57_INCOME_TYPE_SCOPE_SUFFIX)
    ]
    return (
        row_index.isdigit()
        and str(row.get("projection_status") or "") == PROJECTION_UNAVAILABLE
        and str(row.get("funding_state") or "") == FUNDING_MANUAL_REVIEW
        and str(row.get("financial_state") or "") == FINANCIAL_AMBIGUOUS
        and str(row.get("volume_parity_status") or "")
        in {"exact", "within_tolerance"}
        and int(row.get("funding_event_count") or 0) == 0
        and int(row.get("funding_zero_observations") or 0) == 0
        and int(row.get("funding_recovery_attempts") or 0)
        == _G57_SOURCE_ATTEMPTS
    )


def _is_g48_exhausted_none_row(
    row: Mapping[str, Any], *, max_attempts: int
) -> bool:
    """Return whether one bounded G48 read-only recovery is allowed.

    The predicate is generic only for the exact production failure signature:
    a linked exact-volume result exhausted at the configured maximum because
    the BingX income collection contained the strict NoneType integrity error.
    FINAL rows, rows with any funding evidence, and attempt counters above or
    below the maximum are excluded.  The preserved counter makes this one-shot.
    """

    reason = str(row.get("ambiguity_reason") or "")
    exact_none_exhaustion = (
        reason.startswith("funding_recovery_exhausted:")
        and _G47_EXHAUSTED_NONE_NEEDLE in reason
    )
    # G49 production hotfix: G47 rewrote the exhausted reason after its quorum
    # attempt, so the old retryable marker is no longer present.  Accept only
    # the exact fail-closed 1/2 quorum shape observed in Period 3, with the same
    # BingX endpoint/NoneType integrity evidence.  This does not broaden the
    # selector to arbitrary manual-review or generic request failures.
    g47_quorum_exhaustion = (
        reason.startswith(_G49_G47_QUORUM_PREFIX)
        and '"variant":"symbol_all_types","status":"ok"' in reason
        and '"exact_rows":0' in reason
        and _BINGX_FUNDING_ENDPOINT in reason
        and _RETRYABLE_NONE_LIST_REASON in reason
    )
    return (
        str(row.get("projection_status") or "") == PROJECTION_UNAVAILABLE
        and str(row.get("funding_state") or "") == FUNDING_MANUAL_REVIEW
        and str(row.get("financial_state") or "") == FINANCIAL_PROVISIONAL
        and str(row.get("volume_parity_status") or "")
        in {"exact", "within_tolerance"}
        and int(row.get("funding_event_count") or 0) == 0
        and int(row.get("funding_zero_observations") or 0) == 0
        and int(row.get("funding_recovery_attempts") or 0) == int(max_attempts)
        and (exact_none_exhaustion or g47_quorum_exhaustion)
    )


async def _fetch_g47_targeted_funding_quorum(
    *,
    adapter: _FundingAdapter,
    rate_limiter: _RateLimiter | None,
    execution_id: int,
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    start_time_ms: int,
    end_time_ms: int,
    limit: int,
) -> FundingRecoveryQuorum:
    method = getattr(adapter, "fetch_funding_income_recovery_variant", None)
    if not callable(method):
        raise FundingRequestFailed("g47_recovery_variant_not_supported")

    successes: list[tuple[str, tuple[dict[str, Any], ...], str, int]] = []
    diagnostics: list[dict[str, Any]] = []
    for variant in _G47_RECOVERY_VARIANTS:
        if rate_limiter is not None:
            await rate_limiter.wait()
        try:
            response = await method(
                symbol=symbol,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
                limit=limit,
                variant=variant,
            )
            if not isinstance(response, Mapping):
                raise FundingRequestFailed(
                    f"g47_variant_invalid_envelope:{variant}:{type(response).__name__}"
                )
            raw_rows = response.get("rows")
            if not isinstance(raw_rows, list):
                raise FundingRequestFailed(
                    f"g47_variant_invalid_rows:{variant}:{type(raw_rows).__name__}"
                )
            records = normalize_funding_rows(
                raw_rows,
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
            )
            fingerprint = _funding_records_fingerprint(records)
            scope_count = int(response.get("scope_row_count") or 0)
            ignored_non_target_rows = int(response.get("ignored_non_target_rows") or 0)
            row_tuple = tuple(dict(row) for row in raw_rows)
            successes.append((variant, row_tuple, fingerprint, scope_count))
            diagnostic = {
                "variant": variant,
                "status": "ok",
                "scope_rows": scope_count,
                "ignored_non_target_rows": ignored_non_target_rows,
                "exact_rows": len(records),
                "fingerprint": fingerprint[:16],
            }
            diagnostics.append(diagnostic)
            log.info(
                "STATISTICS_FUNDING_G47_DIAGNOSTIC "
                "execution_id=%s variant=%s status=ok scope_rows=%s "
                "exact_rows=%s fingerprint=%s",
                execution_id,
                variant,
                scope_count,
                len(records),
                fingerprint[:16],
            )
        except Exception as exc:
            detail = _safe_text(exc, limit=240)
            diagnostics.append(
                {
                    "variant": variant,
                    "status": "error",
                    "error_type": type(exc).__name__,
                    "reason": detail,
                }
            )
            log.warning(
                "STATISTICS_FUNDING_G47_DIAGNOSTIC "
                "execution_id=%s variant=%s status=error error_type=%s reason=%s",
                execution_id,
                variant,
                type(exc).__name__,
                detail,
            )

    if not successes:
        compact = _safe_text(
            json.dumps(diagnostics, ensure_ascii=False, separators=(",", ":")),
            limit=700,
        )
        raise FundingRequestFailed(f"g47_recovery_no_success:{compact}")

    grouped: dict[str, list[tuple[str, tuple[dict[str, Any], ...], int]]] = {}
    for variant, rows, fingerprint, scope_count in successes:
        grouped.setdefault(fingerprint, []).append((variant, rows, scope_count))
    if len(grouped) != 1:
        summary = ",".join(
            f"{fingerprint[:12]}:{len(items)}"
            for fingerprint, items in sorted(grouped.items())
        )
        raise ValueError(f"g47_recovery_cross_scope_conflict:{summary}")

    fingerprint, matching = next(iter(grouped.items()))
    if len(matching) < _G47_RECOVERY_MIN_QUORUM:
        compact = _safe_text(
            json.dumps(diagnostics, ensure_ascii=False, separators=(",", ":")),
            limit=700,
        )
        raise FundingRequestFailed(
            "g47_recovery_quorum_not_met:"
            f"matching={len(matching)}/{_G47_RECOVERY_MIN_QUORUM}:{compact}"
        )

    variants = tuple(item[0] for item in matching)
    diagnostics_json = _safe_text(
        json.dumps(diagnostics, ensure_ascii=False, separators=(",", ":")),
        limit=700,
    )
    log.info(
        "STATISTICS_FUNDING_G47_QUORUM execution_id=%s confirmations=%s "
        "variants=%s exact_rows=%s fingerprint=%s",
        execution_id,
        len(matching),
        ",".join(variants),
        len(matching[0][1]),
        fingerprint[:16],
    )
    return FundingRecoveryQuorum(
        rows=matching[0][1],
        confirmations=len(matching),
        fingerprint=fingerprint,
        variants=variants,
        diagnostics_json=diagnostics_json,
    )


async def _fetch_g48_funding_quorum(
    *,
    adapter: _FundingAdapter,
    rate_limiter: _RateLimiter | None,
    execution_id: int,
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    start_time_ms: int,
    end_time_ms: int,
    limit: int,
) -> FundingRecoveryQuorum:
    """Use four strict read-only income views and require a matching quorum."""

    method = getattr(adapter, "fetch_funding_income_recovery_variant", None)
    if not callable(method):
        raise FundingRequestFailed("g48_recovery_variant_not_supported")

    successes: list[tuple[str, tuple[dict[str, Any], ...], str, int]] = []
    diagnostics: list[dict[str, Any]] = []
    for variant in _G48_RECOVERY_VARIANTS:
        if rate_limiter is not None:
            await rate_limiter.wait()
        try:
            response = await method(
                symbol=symbol,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
                limit=limit,
                variant=variant,
            )
            if not isinstance(response, Mapping):
                raise FundingRequestFailed(
                    f"g48_variant_invalid_envelope:{variant}:{type(response).__name__}"
                )
            raw_rows = response.get("rows")
            if not isinstance(raw_rows, list):
                raise FundingRequestFailed(
                    f"g48_variant_invalid_rows:{variant}:{type(raw_rows).__name__}"
                )
            records = normalize_funding_rows(
                raw_rows,
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
            )
            fingerprint = _funding_records_fingerprint(records)
            scope_count = int(response.get("scope_row_count") or 0)
            row_tuple = tuple(dict(row) for row in raw_rows)
            successes.append((variant, row_tuple, fingerprint, scope_count))
            diagnostic = {
                "variant": variant,
                "status": "ok",
                "scope_rows": scope_count,
                "exact_rows": len(records),
                "fingerprint": fingerprint[:16],
            }
            diagnostics.append(diagnostic)
            log.info(
                "STATISTICS_FUNDING_G48_DIAGNOSTIC "
                "execution_id=%s variant=%s status=ok scope_rows=%s "
                "exact_rows=%s fingerprint=%s",
                execution_id,
                variant,
                scope_count,
                len(records),
                fingerprint[:16],
            )
        except Exception as exc:
            detail = _safe_text(exc, limit=240)
            diagnostics.append(
                {
                    "variant": variant,
                    "status": "error",
                    "error_type": type(exc).__name__,
                    "reason": detail,
                }
            )
            log.warning(
                "STATISTICS_FUNDING_G48_DIAGNOSTIC "
                "execution_id=%s variant=%s status=error error_type=%s reason=%s",
                execution_id,
                variant,
                type(exc).__name__,
                detail,
            )

    if not successes:
        compact = _safe_text(
            json.dumps(diagnostics, ensure_ascii=False, separators=(",", ":")),
            limit=900,
        )
        raise FundingRequestFailed(f"g48_recovery_no_success:{compact}")

    grouped: dict[str, list[tuple[str, tuple[dict[str, Any], ...], int]]] = {}
    for variant, rows, fingerprint, scope_count in successes:
        grouped.setdefault(fingerprint, []).append((variant, rows, scope_count))
    if len(grouped) != 1:
        summary = ",".join(
            f"{fingerprint[:12]}:{len(items)}"
            for fingerprint, items in sorted(grouped.items())
        )
        raise ValueError(f"g48_recovery_cross_scope_conflict:{summary}")

    fingerprint, matching = next(iter(grouped.items()))
    if len(matching) < _G48_RECOVERY_MIN_QUORUM:
        compact = _safe_text(
            json.dumps(diagnostics, ensure_ascii=False, separators=(",", ":")),
            limit=900,
        )
        raise FundingRequestFailed(
            "g48_recovery_quorum_not_met:"
            f"matching={len(matching)}/{_G48_RECOVERY_MIN_QUORUM}:{compact}"
        )

    variants = tuple(item[0] for item in matching)
    diagnostics_json = _safe_text(
        json.dumps(diagnostics, ensure_ascii=False, separators=(",", ":")),
        limit=900,
    )
    log.info(
        "STATISTICS_FUNDING_G48_QUORUM execution_id=%s confirmations=%s "
        "variants=%s exact_rows=%s fingerprint=%s",
        execution_id,
        len(matching),
        ",".join(variants),
        len(matching[0][1]),
        fingerprint[:16],
    )
    return FundingRecoveryQuorum(
        rows=matching[0][1],
        confirmations=len(matching),
        fingerprint=fingerprint,
        variants=variants,
        diagnostics_json=diagnostics_json,
    )


def _row_dict(row: Any) -> dict[str, Any]:
    return dict(row) if row is not None else {}


async def _claim_due_projection(*, now: datetime) -> dict[str, Any] | None:
    settings = get_settings()
    if not (
        bool(settings.STATISTICS_EXECUTION_RESULTS_ENABLED)
        and bool(settings.FINANCIAL_RECONCILIATION_ENABLED)
    ):
        return None
    funding_enabled = bool(settings.STATISTICS_FUNDING_ENABLED)
    stale_sec = int(settings.FINANCIAL_RECONCILIATION_STALE_PROCESSING_SEC)
    deadline_sec = int(settings.FINANCIAL_RECONCILIATION_DEADLINE_SEC)
    max_funding_attempts = int(
        getattr(settings, "STATISTICS_FUNDING_MAX_RECOVERY_ATTEMPTS", 8)
    )
    # getattr keeps test doubles and staged deployments compatible while the
    # additive Package 3 settings roll out. Production Settings always expose
    # these fields; the defaults mirror app.config.
    funding_deadline_sec = max(
        int(getattr(settings, "STATISTICS_FUNDING_RECOVERY_DEADLINE_SEC", 21600)),
        int(getattr(settings, "STATISTICS_FUNDING_ZERO_GRACE_SEC", 900)) + 3600,
    )
    lease = uuid.uuid4().hex
    stale_before = now - timedelta(seconds=stale_sec)
    deadline = now + timedelta(seconds=deadline_sec)
    funding_deadline = now + timedelta(seconds=funding_deadline_sec)

    async with db.connect() as conn:
        if db.is_postgres():
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT r.execution_id,r.projection_status,r.funding_state,
                           r.financial_state,r.volume_parity_status,
                           r.funding_event_count,r.funding_zero_observations,
                           r.funding_recovery_attempts,r.ambiguity_reason
                    FROM analytics_execution_results r
                    JOIN financial_reconciliation_jobs j
                      ON j.execution_id=r.execution_id
                    WHERE r.linkage_status='linked_exact'
                      AND j.status=ANY($1::text[])
                      AND (
                        (r.projection_status IN ('pending','retry')
                         AND COALESCE(r.projection_next_attempt_at,NOW()) <= $2)
                        OR (r.projection_status='processing'
                            AND r.projection_processing_started_at < $3)
                        OR ($4::boolean AND j.status='confirmed'
                            AND r.projection_status='complete'
                            AND r.funding_state='not_checked'
                            AND r.volume_parity_status IN ('exact','within_tolerance')
                            AND r.financial_state='PROVISIONAL'
                            AND r.ambiguity_reason IS NULL)
                        OR ($4::boolean AND j.status='confirmed'
                            AND r.projection_status='ambiguous'
                            AND r.funding_state='ambiguous'
                            AND r.volume_parity_status IN ('exact','within_tolerance')
                            AND r.ambiguity_reason LIKE
                                '%no stable funding event identity%')
                        OR ($4::boolean AND j.status='confirmed'
                            AND r.projection_status='unavailable'
                            AND r.funding_state='manual_review'
                            AND r.financial_state='AMBIGUOUS'
                            AND r.volume_parity_status IN ('exact','within_tolerance')
                            AND COALESCE(r.funding_event_count,0)=0
                            AND COALESCE(r.funding_zero_observations,0)=0
                            AND COALESCE(r.funding_recovery_attempts,0)>0
                            AND COALESCE(r.funding_recovery_attempts,0)<$5
                            AND r.ambiguity_reason=$6)
                        OR ($4::boolean AND j.status='confirmed'
                            AND r.execution_id=ANY($7::bigint[])
                            AND r.projection_status='unavailable'
                            AND r.funding_state='manual_review'
                            AND r.financial_state='PROVISIONAL'
                            AND r.volume_parity_status IN ('exact','within_tolerance')
                            AND COALESCE(r.funding_event_count,0)=0
                            AND COALESCE(r.funding_zero_observations,0)=0
                            AND COALESCE(r.funding_recovery_attempts,0)=$8
                            AND r.ambiguity_reason LIKE 'funding_recovery_exhausted:%'
                            AND POSITION($9 IN r.ambiguity_reason)>0)
                        OR ($4::boolean AND j.status='confirmed'
                            AND r.projection_status='unavailable'
                            AND r.funding_state='manual_review'
                            AND r.financial_state='PROVISIONAL'
                            AND r.volume_parity_status IN ('exact','within_tolerance')
                            AND COALESCE(r.funding_event_count,0)=0
                            AND COALESCE(r.funding_zero_observations,0)=0
                            AND COALESCE(r.funding_recovery_attempts,0)=$5
                            AND r.ambiguity_reason LIKE 'funding_recovery_exhausted:%'
                            AND (
                              POSITION($9 IN r.ambiguity_reason)>0
                              OR (r.ambiguity_reason LIKE $10
                                  AND POSITION($11 IN r.ambiguity_reason)>0)
                            ))
                        OR ($4::boolean AND j.status='confirmed'
                            AND r.execution_id=ANY($12::bigint[])
                            AND r.projection_status='unavailable'
                            AND r.funding_state='manual_review'
                            AND r.financial_state='PROVISIONAL'
                            AND r.volume_parity_status IN ('exact','within_tolerance')
                            AND COALESCE(r.funding_event_count,0)=0
                            AND COALESCE(r.funding_zero_observations,0)=0
                            AND COALESCE(r.funding_recovery_attempts,0)=$13
                            AND r.ambiguity_reason=$14)
                        OR ($4::boolean AND j.status='confirmed'
                            AND r.execution_id NOT IN (1483,1485,1504)
                            AND r.projection_status='unavailable'
                            AND r.funding_state='manual_review'
                            AND r.financial_state='PROVISIONAL'
                            AND r.volume_parity_status IN ('exact','within_tolerance')
                            AND COALESCE(r.funding_event_count,0)=0
                            AND COALESCE(r.funding_zero_observations,0)=0
                            AND COALESCE(r.funding_recovery_attempts,0)=$13
                            AND r.ambiguity_reason=$14)
                        OR ($4::boolean AND j.status='confirmed'
                            AND r.projection_status='unavailable'
                            AND r.funding_state='manual_review'
                            AND r.financial_state='AMBIGUOUS'
                            AND r.volume_parity_status IN ('exact','within_tolerance')
                            AND COALESCE(r.funding_event_count,0)=0
                            AND COALESCE(r.funding_zero_observations,0)=0
                            AND COALESCE(r.funding_recovery_attempts,0)=$15
                            AND r.ambiguity_reason LIKE $16)
                      )
                    ORDER BY COALESCE(r.projection_next_attempt_at,r.updated_at),r.execution_id
                    FOR UPDATE OF r SKIP LOCKED
                    LIMIT 1
                    """,
                    [
                        FINANCIAL_STATUS_CONFIRMED,
                        FINANCIAL_STATUS_PARTIAL,
                        FINANCIAL_STATUS_AMBIGUOUS,
                        FINANCIAL_STATUS_UNAVAILABLE,
                    ],
                    now,
                    stale_before,
                    funding_enabled,
                    max_funding_attempts,
                    _G45_NONE_INTEGRITY_AMBIGUITY,
                    sorted(_G47_TARGETED_EXECUTION_IDS),
                    _G47_TARGETED_SOURCE_ATTEMPTS,
                    _G47_EXHAUSTED_NONE_NEEDLE,
                    _G49_G47_QUORUM_PREFIX + "%",
                    _RETRYABLE_NONE_LIST_REASON,
                    sorted(_G51_TARGETED_EXECUTION_IDS),
                    _G51_TARGETED_SOURCE_ATTEMPTS,
                    _G51_DEADLINE_NONE_REASON,
                    _G57_SOURCE_ATTEMPTS,
                    _G57_INCOME_TYPE_SCOPE_SQL_LIKE,
                )
                if not row:
                    return None
                row_map = _row_dict(row)
                execution_id = int(row_map["execution_id"])
                legacy_identity_retry = (
                    str(row_map.get("projection_status")) == PROJECTION_AMBIGUOUS
                    and str(row_map.get("funding_state")) == FUNDING_AMBIGUOUS
                    and "no stable funding event identity"
                    in str(row_map.get("ambiguity_reason") or "")
                )
                restarting_funding = funding_enabled and (
                    str(row_map.get("funding_state")) == FUNDING_NOT_CHECKED
                    or legacy_identity_retry
                )
                rearming_g45_none = funding_enabled and (
                    str(row_map.get("projection_status")) == PROJECTION_UNAVAILABLE
                    and str(row_map.get("funding_state")) == FUNDING_MANUAL_REVIEW
                    and str(row_map.get("financial_state")) == FINANCIAL_AMBIGUOUS
                    and str(row_map.get("volume_parity_status"))
                    in {"exact", "within_tolerance"}
                    and int(row_map.get("funding_event_count") or 0) == 0
                    and int(row_map.get("funding_zero_observations") or 0) == 0
                    and 0
                    < int(row_map.get("funding_recovery_attempts") or 0)
                    < max_funding_attempts
                    and str(row_map.get("ambiguity_reason") or "")
                    == _G45_NONE_INTEGRITY_AMBIGUITY
                )
                rearming_g47_none = funding_enabled and _is_g47_targeted_exhausted_row(
                    row_map
                )
                rearming_g48_none = funding_enabled and _is_g48_exhausted_none_row(
                    row_map, max_attempts=max_funding_attempts
                )
                rearming_g51_deadline = (
                    funding_enabled and _is_g51_deadline_exhausted_none_row(row_map)
                )
                rearming_g52_deadline = (
                    funding_enabled and _is_g52_deadline_exhausted_none_row(row_map)
                )
                rearming_g57_scope = (
                    funding_enabled and _is_g57_income_type_scope_row(row_map)
                )
                await conn.execute(
                    """
                    UPDATE analytics_execution_results
                    SET projection_status='processing',
                        projection_attempts=CASE WHEN $4 THEN 1 ELSE projection_attempts+1 END,
                        projection_next_attempt_at=$1,
                        projection_deadline_at=CASE
                          WHEN $4 OR $7 OR $8 OR $9 OR $10 OR $11 OR $12 THEN $6
                          ELSE COALESCE(projection_deadline_at,$2) END,
                        projection_processing_started_at=$1,
                        projection_lease_token=$3,
                        projection_last_error=NULL,
                        funding_state=CASE WHEN $4 OR $7 OR $8 OR $9 OR $10 OR $11 OR $12 THEN 'pending' ELSE funding_state END,
                        funding_recovery_status=CASE
                          WHEN $12 THEN 'g57_scope_fallback_pending'
                          WHEN $9 OR $10 OR $11 THEN 'g48_fallback_pending'
                          WHEN $8 THEN 'g47_fallback_pending'
                          WHEN $4 OR $7 THEN 'pending'
                          ELSE funding_recovery_status END,
                        funding_recovery_reason=CASE
                          WHEN $4 THEN NULL
                          WHEN $7 THEN 'g46_retryable_none_integrity_rearmed'
                          WHEN $8 THEN 'g47_targeted_exhausted_none_rearmed_once'
                          WHEN $9 THEN 'g49_g47_quorum_selector_rearmed_once'
                          WHEN $10 THEN 'g51_deadline_expired_none_rearmed_once'
                          WHEN $11 THEN 'g52_generic_deadline_expired_none_rearmed_once'
                          WHEN $12 THEN 'g57_income_type_scope_rearmed_once'
                          ELSE funding_recovery_reason END,
                        funding_recovery_attempts=CASE WHEN $4 THEN 0 ELSE funding_recovery_attempts END,
                        funding_zero_observations=CASE WHEN $4 THEN 0 ELSE funding_zero_observations END,
                        funding_first_empty_at=CASE WHEN $4 THEN NULL ELSE funding_first_empty_at END,
                        financial_state=CASE WHEN $7 OR $8 OR $9 OR $10 OR $11 OR $12 THEN 'PROVISIONAL' ELSE financial_state END,
                        data_quality_status=CASE WHEN $7 OR $8 OR $9 OR $10 OR $11 OR $12 THEN 'partial' ELSE data_quality_status END,
                        ambiguity_reason=CASE WHEN $7 OR $8 OR $9 OR $10 OR $11 OR $12 THEN NULL ELSE ambiguity_reason END,
                        funding_finalized_at=CASE WHEN $7 OR $8 OR $9 OR $10 OR $11 OR $12 THEN NULL ELSE funding_finalized_at END,
                        finalized_at=CASE WHEN $7 OR $8 OR $9 OR $10 OR $11 OR $12 THEN NULL ELSE finalized_at END,
                        final_eligible=CASE WHEN $7 OR $8 OR $9 OR $10 OR $11 OR $12 THEN 0 ELSE final_eligible END,
                        updated_at=NOW()
                    WHERE execution_id=$5
                    """,
                    now,
                    deadline,
                    lease,
                    restarting_funding,
                    execution_id,
                    funding_deadline,
                    rearming_g45_none,
                    rearming_g47_none,
                    rearming_g48_none,
                    rearming_g51_deadline,
                    rearming_g52_deadline,
                    rearming_g57_scope,
                )
                return _row_dict(
                    await conn.fetchrow(
                        "SELECT * FROM analytics_execution_results WHERE execution_id=$1",
                        execution_id,
                    )
                )

        await conn.execute("BEGIN IMMEDIATE")
        try:
            cursor = await conn.execute(
                """
                SELECT r.execution_id,r.projection_status,r.funding_state,
                       r.financial_state,r.volume_parity_status,
                       r.funding_event_count,r.funding_zero_observations,
                       r.funding_recovery_attempts,r.ambiguity_reason
                FROM analytics_execution_results r
                JOIN financial_reconciliation_jobs j
                  ON j.execution_id=r.execution_id
                WHERE r.linkage_status='linked_exact'
                  AND j.status IN ('confirmed','partial','ambiguous','unavailable')
                  AND (
                    (r.projection_status IN ('pending','retry')
                     AND julianday(COALESCE(r.projection_next_attempt_at,CURRENT_TIMESTAMP)) <= julianday(?))
                    OR (r.projection_status='processing'
                        AND julianday(r.projection_processing_started_at) < julianday(?))
                    OR (?=1 AND j.status='confirmed'
                        AND r.projection_status='complete'
                        AND r.funding_state='not_checked'
                        AND r.volume_parity_status IN ('exact','within_tolerance')
                        AND r.financial_state='PROVISIONAL'
                        AND r.ambiguity_reason IS NULL)
                    OR (?=1 AND j.status='confirmed'
                        AND r.projection_status='ambiguous'
                        AND r.funding_state='ambiguous'
                        AND r.volume_parity_status IN ('exact','within_tolerance')
                        AND r.ambiguity_reason LIKE
                            '%no stable funding event identity%')
                    OR (?=1 AND j.status='confirmed'
                        AND r.projection_status='unavailable'
                        AND r.funding_state='manual_review'
                        AND r.financial_state='AMBIGUOUS'
                        AND r.volume_parity_status IN ('exact','within_tolerance')
                        AND COALESCE(r.funding_event_count,0)=0
                        AND COALESCE(r.funding_zero_observations,0)=0
                        AND COALESCE(r.funding_recovery_attempts,0)>0
                        AND COALESCE(r.funding_recovery_attempts,0)<?
                        AND r.ambiguity_reason=?)
                    OR (?=1 AND j.status='confirmed'
                        AND r.execution_id IN (1483,1485)
                        AND r.projection_status='unavailable'
                        AND r.funding_state='manual_review'
                        AND r.financial_state='PROVISIONAL'
                        AND r.volume_parity_status IN ('exact','within_tolerance')
                        AND COALESCE(r.funding_event_count,0)=0
                        AND COALESCE(r.funding_zero_observations,0)=0
                        AND COALESCE(r.funding_recovery_attempts,0)=?
                        AND r.ambiguity_reason LIKE 'funding_recovery_exhausted:%'
                        AND instr(r.ambiguity_reason,?)>0)
                    OR (?=1 AND j.status='confirmed'
                        AND r.projection_status='unavailable'
                        AND r.funding_state='manual_review'
                        AND r.financial_state='PROVISIONAL'
                        AND r.volume_parity_status IN ('exact','within_tolerance')
                        AND COALESCE(r.funding_event_count,0)=0
                        AND COALESCE(r.funding_zero_observations,0)=0
                        AND COALESCE(r.funding_recovery_attempts,0)=?
                        AND r.ambiguity_reason LIKE 'funding_recovery_exhausted:%'
                        AND (
                          instr(r.ambiguity_reason,?)>0
                          OR (r.ambiguity_reason LIKE ?
                              AND instr(r.ambiguity_reason,?)>0)
                        ))
                    OR (?=1 AND j.status='confirmed'
                        AND r.execution_id IN (1504)
                        AND r.projection_status='unavailable'
                        AND r.funding_state='manual_review'
                        AND r.financial_state='PROVISIONAL'
                        AND r.volume_parity_status IN ('exact','within_tolerance')
                        AND COALESCE(r.funding_event_count,0)=0
                        AND COALESCE(r.funding_zero_observations,0)=0
                        AND COALESCE(r.funding_recovery_attempts,0)=?
                        AND r.ambiguity_reason=?)
                    OR (?=1 AND j.status='confirmed'
                        AND r.execution_id NOT IN (1483,1485,1504)
                        AND r.projection_status='unavailable'
                        AND r.funding_state='manual_review'
                        AND r.financial_state='PROVISIONAL'
                        AND r.volume_parity_status IN ('exact','within_tolerance')
                        AND COALESCE(r.funding_event_count,0)=0
                        AND COALESCE(r.funding_zero_observations,0)=0
                        AND COALESCE(r.funding_recovery_attempts,0)=?
                        AND r.ambiguity_reason=?)
                    OR (?=1 AND j.status='confirmed'
                        AND r.projection_status='unavailable'
                        AND r.funding_state='manual_review'
                        AND r.financial_state='AMBIGUOUS'
                        AND r.volume_parity_status IN ('exact','within_tolerance')
                        AND COALESCE(r.funding_event_count,0)=0
                        AND COALESCE(r.funding_zero_observations,0)=0
                        AND COALESCE(r.funding_recovery_attempts,0)=?
                        AND r.ambiguity_reason LIKE ?)
                  )
                ORDER BY COALESCE(r.projection_next_attempt_at,r.updated_at),r.execution_id
                LIMIT 1
                """,
                (
                    _iso(now),
                    _iso(stale_before),
                    int(funding_enabled),
                    int(funding_enabled),
                    int(funding_enabled),
                    max_funding_attempts,
                    _G45_NONE_INTEGRITY_AMBIGUITY,
                    int(funding_enabled),
                    _G47_TARGETED_SOURCE_ATTEMPTS,
                    _G47_EXHAUSTED_NONE_NEEDLE,
                    int(funding_enabled),
                    max_funding_attempts,
                    _G47_EXHAUSTED_NONE_NEEDLE,
                    _G49_G47_QUORUM_PREFIX + "%",
                    _RETRYABLE_NONE_LIST_REASON,
                    int(funding_enabled),
                    _G51_TARGETED_SOURCE_ATTEMPTS,
                    _G51_DEADLINE_NONE_REASON,
                    int(funding_enabled),
                    _G52_DEADLINE_SOURCE_ATTEMPTS,
                    _G52_DEADLINE_NONE_REASON,
                    int(funding_enabled),
                    _G57_SOURCE_ATTEMPTS,
                    _G57_INCOME_TYPE_SCOPE_SQL_LIKE,
                ),
            )
            row = await cursor.fetchone()
            if not row:
                await conn.commit()
                return None
            row_map = _row_dict(row)
            execution_id = int(row_map["execution_id"])
            legacy_identity_retry = (
                str(row_map.get("projection_status")) == PROJECTION_AMBIGUOUS
                and str(row_map.get("funding_state")) == FUNDING_AMBIGUOUS
                and "no stable funding event identity"
                in str(row_map.get("ambiguity_reason") or "")
            )
            restarting_funding = funding_enabled and (
                str(row_map.get("funding_state")) == FUNDING_NOT_CHECKED
                or legacy_identity_retry
            )
            rearming_g45_none = funding_enabled and (
                str(row_map.get("projection_status")) == PROJECTION_UNAVAILABLE
                and str(row_map.get("funding_state")) == FUNDING_MANUAL_REVIEW
                and str(row_map.get("financial_state")) == FINANCIAL_AMBIGUOUS
                and str(row_map.get("volume_parity_status"))
                in {"exact", "within_tolerance"}
                and int(row_map.get("funding_event_count") or 0) == 0
                and int(row_map.get("funding_zero_observations") or 0) == 0
                and 0
                < int(row_map.get("funding_recovery_attempts") or 0)
                < max_funding_attempts
                and str(row_map.get("ambiguity_reason") or "")
                == _G45_NONE_INTEGRITY_AMBIGUITY
            )
            rearming_g47_none = funding_enabled and _is_g47_targeted_exhausted_row(
                row_map
            )
            rearming_g48_none = funding_enabled and _is_g48_exhausted_none_row(
                row_map, max_attempts=max_funding_attempts
            )
            rearming_g51_deadline = (
                funding_enabled and _is_g51_deadline_exhausted_none_row(row_map)
            )
            rearming_g52_deadline = (
                funding_enabled and _is_g52_deadline_exhausted_none_row(row_map)
            )
            rearming_g57_scope = (
                funding_enabled and _is_g57_income_type_scope_row(row_map)
            )
            await conn.execute(
                """
                UPDATE analytics_execution_results
                SET projection_status='processing',
                    projection_attempts=CASE WHEN :restart=1 THEN 1 ELSE projection_attempts+1 END,
                    projection_next_attempt_at=:now,
                    projection_deadline_at=CASE
                      WHEN :restart=1 OR :g45=1 OR :g47=1 OR :g48=1 OR :g51=1 OR :g52=1 OR :g57=1
                      THEN :funding_deadline
                      ELSE COALESCE(projection_deadline_at,:deadline) END,
                    projection_processing_started_at=:now,
                    projection_lease_token=:lease,
                    projection_last_error=NULL,
                    funding_state=CASE
                      WHEN :restart=1 OR :g45=1 OR :g47=1 OR :g48=1 OR :g51=1 OR :g52=1 OR :g57=1
                      THEN 'pending' ELSE funding_state END,
                    funding_recovery_status=CASE
                      WHEN :g57=1 THEN 'g57_scope_fallback_pending'
                      WHEN :g48=1 OR :g51=1 OR :g52=1 THEN 'g48_fallback_pending'
                      WHEN :g47=1 THEN 'g47_fallback_pending'
                      WHEN :restart=1 OR :g45=1 THEN 'pending'
                      ELSE funding_recovery_status END,
                    funding_recovery_reason=CASE
                      WHEN :restart=1 THEN NULL
                      WHEN :g45=1 THEN 'g46_retryable_none_integrity_rearmed'
                      WHEN :g47=1 THEN 'g47_targeted_exhausted_none_rearmed_once'
                      WHEN :g48=1 THEN 'g49_g47_quorum_selector_rearmed_once'
                      WHEN :g51=1 THEN 'g51_deadline_expired_none_rearmed_once'
                      WHEN :g52=1 THEN 'g52_generic_deadline_expired_none_rearmed_once'
                      WHEN :g57=1 THEN 'g57_income_type_scope_rearmed_once'
                      ELSE funding_recovery_reason END,
                    funding_recovery_attempts=CASE
                      WHEN :restart=1 THEN 0 ELSE funding_recovery_attempts END,
                    funding_zero_observations=CASE
                      WHEN :restart=1 THEN 0 ELSE funding_zero_observations END,
                    funding_first_empty_at=CASE
                      WHEN :restart=1 THEN NULL ELSE funding_first_empty_at END,
                    financial_state=CASE
                      WHEN :g45=1 OR :g47=1 OR :g48=1 OR :g51=1 OR :g52=1 OR :g57=1
                      THEN 'PROVISIONAL' ELSE financial_state END,
                    data_quality_status=CASE
                      WHEN :g45=1 OR :g47=1 OR :g48=1 OR :g51=1 OR :g52=1 OR :g57=1
                      THEN 'partial' ELSE data_quality_status END,
                    ambiguity_reason=CASE
                      WHEN :g45=1 OR :g47=1 OR :g48=1 OR :g51=1 OR :g52=1 OR :g57=1
                      THEN NULL ELSE ambiguity_reason END,
                    funding_finalized_at=CASE
                      WHEN :g45=1 OR :g47=1 OR :g48=1 OR :g51=1 OR :g52=1 OR :g57=1
                      THEN NULL ELSE funding_finalized_at END,
                    finalized_at=CASE
                      WHEN :g45=1 OR :g47=1 OR :g48=1 OR :g51=1 OR :g52=1 OR :g57=1
                      THEN NULL ELSE finalized_at END,
                    final_eligible=CASE
                      WHEN :g45=1 OR :g47=1 OR :g48=1 OR :g51=1 OR :g52=1 OR :g57=1
                      THEN 0 ELSE final_eligible END,
                    updated_at=CURRENT_TIMESTAMP
                WHERE execution_id=:execution_id
                """,
                {
                    "restart": int(restarting_funding),
                    "g45": int(rearming_g45_none),
                    "g47": int(rearming_g47_none),
                    "g48": int(rearming_g48_none),
                    "g51": int(rearming_g51_deadline),
                    "g52": int(rearming_g52_deadline),
                    "g57": int(rearming_g57_scope),
                    "now": _iso(now),
                    "funding_deadline": _iso(funding_deadline),
                    "deadline": _iso(deadline),
                    "lease": lease,
                    "execution_id": execution_id,
                },
            )
            cursor = await conn.execute(
                "SELECT * FROM analytics_execution_results WHERE execution_id=?",
                (execution_id,),
            )
            projection = _row_dict(await cursor.fetchone())
            await conn.commit()
            return projection
        except Exception:
            await conn.rollback()
            raise


async def _load_context(execution_id: int) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    execution = await db.get_execution_by_id(execution_id)
    job = await db.get_financial_reconciliation_job(execution_id=execution_id)
    if not execution or not job:
        raise RuntimeError("projection execution or financial job is missing")
    fills = await db.list_financial_reconciliation_fills(int(job["id"]))
    return dict(execution), dict(job), [dict(row) for row in fills]


async def _overlapping_execution_ids(
    *,
    execution_id: int,
    user_id: int,
    symbol: str,
    event_time: datetime,
) -> tuple[int, ...]:
    """Return other execution rows that could own the same funding event.

    The query intentionally uses broad trade-execution lifecycle boundaries.
    A pending LIMIT can therefore cause an ambiguity instead of an unsafe exact
    attribution.  This is fail-closed and does not affect trading.
    """

    async with db.connect() as conn:
        if db.is_postgres():
            rows = await conn.fetch(
                """
                SELECT id FROM trade_executions
                WHERE id<>$1 AND user_id=$2 AND UPPER(symbol)=UPPER($3)
                  AND COALESCE(status,'') <> 'superseded_duplicate'
                  AND created_at <= $4
                  AND COALESCE(closed_at,updated_at,NOW()) >= $4
                ORDER BY id
                """,
                execution_id,
                user_id,
                symbol,
                event_time,
            )
            return tuple(int(row["id"]) for row in rows)
        cursor = await conn.execute(
            """
            SELECT id FROM trade_executions
            WHERE id<>? AND user_id=? AND UPPER(symbol)=UPPER(?)
              AND COALESCE(status,'') <> 'superseded_duplicate'
              AND julianday(created_at) <= julianday(?)
              AND julianday(COALESCE(closed_at,updated_at,CURRENT_TIMESTAMP)) >= julianday(?)
            ORDER BY id
            """,
            (execution_id, user_id, symbol, _iso(event_time), _iso(event_time)),
        )
        return tuple(int(row[0]) for row in await cursor.fetchall())


async def _persist_funding_events(
    *,
    execution_id: int,
    user_id: int,
    records: Sequence[FundingRecord],
) -> Decimal:
    total = Decimal()
    async with db.connect() as conn:
        if db.is_postgres():
            async with conn.transaction():
                for item in records:
                    existing = await conn.fetchrow(
                        """
                        SELECT execution_id,symbol,amount_signed,asset,event_time,
                               attribution_status,attribution_reason
                        FROM financial_funding_events
                        WHERE exchange='bingx' AND user_id=$1 AND exchange_event_id=$2
                        FOR UPDATE
                        """,
                        user_id,
                        item.exchange_event_id,
                    )
                    if existing:
                        if (
                            _canonical_symbol(existing["symbol"]) != item.symbol
                            or _decimal(existing["amount_signed"]) != item.amount_signed
                            or str(existing["asset"]).upper() != item.asset
                            or _datetime(existing["event_time"], allow_none=False) != item.event_time
                        ):
                            raise ValueError(
                                f"funding event payload conflict {item.exchange_event_id}"
                            )
                        existing_execution_id = existing["execution_id"]
                        if existing_execution_id is None:
                            if str(existing["attribution_status"] or "") != "ambiguous":
                                raise ValueError(
                                    f"funding event ownership conflict {item.exchange_event_id}"
                                )
                            tag = await conn.execute(
                                """
                                UPDATE financial_funding_events
                                SET execution_id=$3,
                                    attribution_status='assigned_exact',
                                    attribution_reason='single_execution_open_interval',
                                    updated_at=NOW()
                                WHERE exchange='bingx' AND user_id=$1
                                  AND exchange_event_id=$2
                                  AND execution_id IS NULL
                                  AND attribution_status='ambiguous'
                                """,
                                user_id,
                                item.exchange_event_id,
                                execution_id,
                            )
                            if not str(tag).endswith(" 1"):
                                raise ValueError(
                                    f"funding event ambiguous promotion conflict {item.exchange_event_id}"
                                )
                        elif int(existing_execution_id) != execution_id:
                            raise ValueError(
                                f"funding event ownership conflict {item.exchange_event_id}"
                            )
                    else:
                        await conn.execute(
                            """
                            INSERT INTO financial_funding_events(
                              execution_id,user_id,exchange,symbol,position_side,
                              exchange_event_id,amount_signed,asset,event_time,
                              attribution_status,attribution_reason,source_endpoint,
                              metadata_json,created_at,updated_at
                            ) VALUES(
                              $1,$2,'bingx',$3,NULL,$4,$5,$6,$7,
                              'assigned_exact','single_execution_open_interval',
                              'swap_v2_user_income',$8,NOW(),NOW()
                            )
                            """,
                            execution_id,
                            user_id,
                            item.symbol,
                            item.exchange_event_id,
                            item.amount_signed,
                            item.asset,
                            item.event_time,
                            item.metadata_json,
                        )
                    total += item.amount_signed
                return total

        await conn.execute("BEGIN IMMEDIATE")
        try:
            for item in records:
                cursor = await conn.execute(
                    """
                    SELECT execution_id,symbol,amount_signed,asset,event_time,
                           attribution_status,attribution_reason
                    FROM financial_funding_events
                    WHERE exchange='bingx' AND user_id=? AND exchange_event_id=?
                    """,
                    (user_id, item.exchange_event_id),
                )
                existing = _row_dict(await cursor.fetchone())
                if existing:
                    if (
                        _canonical_symbol(existing.get("symbol")) != item.symbol
                        or _decimal(existing.get("amount_signed")) != item.amount_signed
                        or str(existing.get("asset") or "").upper() != item.asset
                        or _datetime(existing.get("event_time"), allow_none=False) != item.event_time
                    ):
                        raise ValueError(
                            f"funding event payload conflict {item.exchange_event_id}"
                        )
                    existing_execution_id = existing.get("execution_id")
                    if existing_execution_id is None:
                        if str(existing.get("attribution_status") or "") != "ambiguous":
                            raise ValueError(
                                f"funding event ownership conflict {item.exchange_event_id}"
                            )
                        update_cursor = await conn.execute(
                            """
                            UPDATE financial_funding_events
                            SET execution_id=?,
                                attribution_status='assigned_exact',
                                attribution_reason='single_execution_open_interval',
                                updated_at=CURRENT_TIMESTAMP
                            WHERE exchange='bingx' AND user_id=?
                              AND exchange_event_id=?
                              AND execution_id IS NULL
                              AND attribution_status='ambiguous'
                            """,
                            (execution_id, user_id, item.exchange_event_id),
                        )
                        if int(getattr(update_cursor, "rowcount", 0) or 0) != 1:
                            raise ValueError(
                                f"funding event ambiguous promotion conflict {item.exchange_event_id}"
                            )
                    elif int(existing_execution_id) != execution_id:
                        raise ValueError(
                            f"funding event ownership conflict {item.exchange_event_id}"
                        )
                else:
                    await conn.execute(
                        """
                        INSERT INTO financial_funding_events(
                          execution_id,user_id,exchange,symbol,position_side,
                          exchange_event_id,amount_signed,asset,event_time,
                          attribution_status,attribution_reason,source_endpoint,
                          metadata_json,created_at,updated_at
                        ) VALUES(
                          ?,?,'bingx',?,NULL,?,?,?,?,
                          'assigned_exact','single_execution_open_interval',
                          'swap_v2_user_income',?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP
                        )
                        """,
                        (
                            execution_id,
                            user_id,
                            item.symbol,
                            item.exchange_event_id,
                            _plain(item.amount_signed),
                            item.asset,
                            _iso(item.event_time),
                            item.metadata_json,
                        ),
                    )
                total += item.amount_signed
            await conn.commit()
            return total
        except Exception:
            await conn.rollback()
            raise


async def _persist_ambiguous_funding_events(
    *,
    user_id: int,
    records: Sequence[FundingRecord],
    reason: str,
) -> None:
    """Durably retain funding evidence that cannot be assigned to one execution."""

    bounded_reason = _safe_text(reason, limit=500)
    async with db.connect() as conn:
        if db.is_postgres():
            async with conn.transaction():
                for item in records:
                    existing = await conn.fetchrow(
                        """
                        SELECT execution_id,symbol,amount_signed,asset,event_time,
                               attribution_status
                        FROM financial_funding_events
                        WHERE exchange='bingx' AND user_id=$1 AND exchange_event_id=$2
                        FOR UPDATE
                        """,
                        user_id,
                        item.exchange_event_id,
                    )
                    if existing:
                        if (
                            _canonical_symbol(existing["symbol"]) != item.symbol
                            or _decimal(existing["amount_signed"]) != item.amount_signed
                            or str(existing["asset"]).upper() != item.asset
                            or _datetime(existing["event_time"], allow_none=False) != item.event_time
                        ):
                            raise ValueError(
                                f"funding event payload conflict {item.exchange_event_id}"
                            )
                        if existing["execution_id"] is not None:
                            raise ValueError(
                                f"funding event already assigned {item.exchange_event_id}"
                            )
                        await conn.execute(
                            """
                            UPDATE financial_funding_events
                            SET attribution_status='ambiguous',attribution_reason=$3,
                                updated_at=NOW()
                            WHERE exchange='bingx' AND user_id=$1 AND exchange_event_id=$2
                            """,
                            user_id,
                            item.exchange_event_id,
                            bounded_reason,
                        )
                    else:
                        await conn.execute(
                            """
                            INSERT INTO financial_funding_events(
                              execution_id,user_id,exchange,symbol,position_side,
                              exchange_event_id,amount_signed,asset,event_time,
                              attribution_status,attribution_reason,source_endpoint,
                              metadata_json,created_at,updated_at
                            ) VALUES(
                              NULL,$1,'bingx',$2,NULL,$3,$4,$5,$6,
                              'ambiguous',$7,'swap_v2_user_income',$8,NOW(),NOW()
                            )
                            """,
                            user_id,
                            item.symbol,
                            item.exchange_event_id,
                            item.amount_signed,
                            item.asset,
                            item.event_time,
                            bounded_reason,
                            item.metadata_json,
                        )
                return

        await conn.execute("BEGIN IMMEDIATE")
        try:
            for item in records:
                cursor = await conn.execute(
                    """
                    SELECT execution_id,symbol,amount_signed,asset,event_time,
                           attribution_status
                    FROM financial_funding_events
                    WHERE exchange='bingx' AND user_id=? AND exchange_event_id=?
                    """,
                    (user_id, item.exchange_event_id),
                )
                existing = _row_dict(await cursor.fetchone())
                if existing:
                    if (
                        _canonical_symbol(existing.get("symbol")) != item.symbol
                        or _decimal(existing.get("amount_signed")) != item.amount_signed
                        or str(existing.get("asset") or "").upper() != item.asset
                        or _datetime(existing.get("event_time"), allow_none=False) != item.event_time
                    ):
                        raise ValueError(
                            f"funding event payload conflict {item.exchange_event_id}"
                        )
                    if existing.get("execution_id") is not None:
                        raise ValueError(
                            f"funding event already assigned {item.exchange_event_id}"
                        )
                    await conn.execute(
                        """
                        UPDATE financial_funding_events
                        SET attribution_status='ambiguous',attribution_reason=?,
                            updated_at=CURRENT_TIMESTAMP
                        WHERE exchange='bingx' AND user_id=? AND exchange_event_id=?
                        """,
                        (bounded_reason, user_id, item.exchange_event_id),
                    )
                else:
                    await conn.execute(
                        """
                        INSERT INTO financial_funding_events(
                          execution_id,user_id,exchange,symbol,position_side,
                          exchange_event_id,amount_signed,asset,event_time,
                          attribution_status,attribution_reason,source_endpoint,
                          metadata_json,created_at,updated_at
                        ) VALUES(
                          NULL,?,'bingx',?,NULL,?,?,?,?,
                          'ambiguous',?,'swap_v2_user_income',?,
                          CURRENT_TIMESTAMP,CURRENT_TIMESTAMP
                        )
                        """,
                        (
                            user_id,
                            item.symbol,
                            item.exchange_event_id,
                            _plain(item.amount_signed),
                            item.asset,
                            _iso(item.event_time),
                            bounded_reason,
                            item.metadata_json,
                        ),
                    )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise


def _projection_update_columns(values: Mapping[str, Any]) -> tuple[str, ...]:
    allowed = {
        "first_entry_fill_at",
        "last_entry_fill_at",
        "actual_entry_qty",
        "actual_entry_avg_price",
        "planned_risk_usd",
        "initial_price_risk_usd",
        "initial_risk_percent_of_equity",
        "first_exit_fill_at",
        "last_exit_fill_at",
        "actual_exit_qty",
        "actual_exit_avg_price",
        "execution_max_tp_index",
        "canonical_terminal_reason",
        "terminal_detail",
        "strategy_gross_pnl",
        "exchange_gross_pnl",
        "gross_pnl_source",
        "trading_fee_signed",
        "trading_fee_cost",
        "funding_signed",
        "settlement_asset",
        "net_pnl",
        "provisional_net_pnl",
        "result_r",
        "provisional_result_r",
        "entry_slippage_bps",
        "limit_price_slippage_bps",
        "execution_duration_seconds",
        "trading_reconciliation_state",
        "funding_state",
        "financial_state",
        "volume_parity_status",
        "completeness_mask",
        "completeness_percent",
        "data_quality_status",
        "ambiguity_reason",
        "projection_status",
        "projection_next_attempt_at",
        "projection_deadline_at",
        "projection_processing_started_at",
        "projection_lease_token",
        "projection_last_error",
        "funding_query_start_at",
        "funding_query_end_at",
        "funding_event_count",
        "funding_recovery_attempts",
        "funding_zero_observations",
        "funding_first_empty_at",
        "funding_last_checked_at",
        "funding_recovery_status",
        "funding_recovery_reason",
        "funding_finalized_at",
        "quality_reasons_json",
        "final_eligible",
        "simulation_eligible",
        "risk_analysis_eligible",
        "quality_gate_version",
        "finalized_at",
    }
    return tuple(key for key in values if key in allowed)


_PROJECTION_TIMESTAMP_COLUMNS = frozenset({
    "first_entry_fill_at",
    "last_entry_fill_at",
    "first_exit_fill_at",
    "last_exit_fill_at",
    "projection_next_attempt_at",
    "projection_deadline_at",
    "projection_processing_started_at",
    "funding_query_start_at",
    "funding_query_end_at",
    "funding_first_empty_at",
    "funding_last_checked_at",
    "funding_finalized_at",
    "finalized_at",
})
_PROJECTION_NUMERIC_COLUMNS = frozenset({
    "actual_entry_qty",
    "actual_entry_avg_price",
    "planned_risk_usd",
    "initial_price_risk_usd",
    "initial_risk_percent_of_equity",
    "actual_exit_qty",
    "actual_exit_avg_price",
    "strategy_gross_pnl",
    "exchange_gross_pnl",
    "trading_fee_signed",
    "trading_fee_cost",
    "funding_signed",
    "net_pnl",
    "provisional_net_pnl",
    "result_r",
    "provisional_result_r",
    "entry_slippage_bps",
    "limit_price_slippage_bps",
    "completeness_percent",
})
_PROJECTION_INTEGER_COLUMNS = frozenset({
    "execution_max_tp_index",
    "execution_duration_seconds",
    "completeness_mask",
    "funding_event_count",
    "funding_recovery_attempts",
    "funding_zero_observations",
    "final_eligible",
    "simulation_eligible",
    "risk_analysis_eligible",
    "quality_gate_version",
})


def _pg_projection_value(column: str, value: Any) -> Any:
    if value is None:
        return None
    if column in _PROJECTION_TIMESTAMP_COLUMNS:
        return _datetime(value, allow_none=True)
    if column in _PROJECTION_NUMERIC_COLUMNS:
        return _decimal(value, allow_none=True)
    if column in _PROJECTION_INTEGER_COLUMNS:
        return int(value)
    return value


async def _save_projection(
    *,
    execution_id: int,
    lease_token: str,
    values: Mapping[str, Any],
) -> bool:
    columns = _projection_update_columns(values)
    if not columns:
        return False
    async with db.connect() as conn:
        if db.is_postgres():
            assignments = ",".join(
                f"{column}=${index + 1}" for index, column in enumerate(columns)
            )
            args = [_pg_projection_value(column, values[column]) for column in columns]
            args.extend([execution_id, lease_token])
            result = await conn.execute(
                f"""
                UPDATE analytics_execution_results
                SET {assignments},updated_at=NOW(),result_version=result_version+1
                WHERE execution_id=${len(columns)+1}
                  AND projection_lease_token=${len(columns)+2}
                """,
                *args,
            )
            return str(result).endswith(" 1")
        assignments = ",".join(f"{column}=?" for column in columns)
        cursor = await conn.execute(
            f"""
            UPDATE analytics_execution_results
            SET {assignments},updated_at=CURRENT_TIMESTAMP,
                result_version=result_version+1
            WHERE execution_id=? AND projection_lease_token=?
            """,
            tuple(values[column] for column in columns)
            + (execution_id, lease_token),
        )
        await conn.commit()
        return int(getattr(cursor, "rowcount", 0) or 0) == 1


def _retry_delay(attempts: int, *, execution_id: int = 0) -> float:
    """Bounded funding backoff with deterministic jitter.

    The schedule is intentionally minutes/hours, not seconds, so a broken
    funding endpoint cannot load the existing financial worker or BingX.
    """

    schedule = (60.0, 300.0, 900.0, 3600.0, 10800.0, 21600.0)
    index = max(0, min(int(attempts or 1) - 1, len(schedule) - 1))
    base = schedule[index]
    digest = hashlib.sha256(f"{int(execution_id)}:{int(attempts)}".encode()).digest()
    # 0.90 .. 1.10 deterministic multiplier avoids synchronized replicas.
    multiplier = 0.90 + (int.from_bytes(digest[:2], "big") / 65535.0) * 0.20
    return max(30.0, base * multiplier)


async def _manual_review_projection(
    projection: Mapping[str, Any],
    *,
    reason: str,
    now: datetime,
    ambiguous: bool,
    extra_values: Mapping[str, Any] | None = None,
) -> StatisticsFinancialOutcome:
    values: dict[str, Any] = {
        "projection_status": PROJECTION_UNAVAILABLE,
        "projection_next_attempt_at": _iso(now),
        "projection_processing_started_at": None,
        "projection_lease_token": None,
        "projection_last_error": reason,
        "funding_state": FUNDING_MANUAL_REVIEW,
        "funding_recovery_status": "manual_review",
        "funding_recovery_reason": reason,
        "funding_last_checked_at": _iso(now),
        "financial_state": FINANCIAL_AMBIGUOUS if ambiguous else FINANCIAL_PROVISIONAL,
        "data_quality_status": "ambiguous" if ambiguous else "partial",
        "ambiguity_reason": reason,
        "finalized_at": None,
    }
    if extra_values:
        values.update(dict(extra_values))
    saved = await _save_projection(
        execution_id=int(projection["execution_id"]),
        lease_token=str(projection["projection_lease_token"]),
        values=values,
    )
    if not saved:
        raise RuntimeError(
            "statistics projection lease changed before manual-review persistence"
        )
    return StatisticsFinancialOutcome(
        execution_id=int(projection["execution_id"]),
        action="funding_manual_review",
        projection_status=PROJECTION_UNAVAILABLE,
        financial_state=str(values["financial_state"]),
        funding_state=FUNDING_MANUAL_REVIEW,
        attempts=int(projection.get("funding_recovery_attempts") or 0),
        error=reason,
    )


async def _reschedule_projection(
    projection: Mapping[str, Any],
    *,
    reason: str,
    now: datetime,
    extra_values: Mapping[str, Any] | None = None,
    minimum_delay: float | None = None,
) -> StatisticsFinancialOutcome:
    attempts = int(projection.get("funding_recovery_attempts") or 0)
    deadline = _datetime(projection.get("projection_deadline_at"))
    settings = get_settings()
    max_attempts = int(settings.STATISTICS_FUNDING_MAX_RECOVERY_ATTEMPTS)
    expired = deadline is not None and now >= deadline
    if attempts >= max_attempts or expired:
        exhausted_reason = (
            f"funding_recovery_exhausted:attempts={attempts}:"
            f"deadline_expired={int(expired)}:{reason}"
        )
        return await _manual_review_projection(
            projection,
            reason=exhausted_reason,
            now=now,
            ambiguous=False,
            extra_values=extra_values,
        )
    delay = _retry_delay(
        attempts, execution_id=int(projection.get("execution_id") or 0)
    )
    if minimum_delay is not None:
        delay = max(delay, float(minimum_delay))
    if deadline is not None:
        delay = min(delay, max(1.0, (deadline - now).total_seconds()))
    values: dict[str, Any] = {
        "projection_status": PROJECTION_RETRY,
        "projection_next_attempt_at": _iso(now + timedelta(seconds=delay)),
        "projection_processing_started_at": None,
        "projection_lease_token": None,
        "projection_last_error": reason,
        "funding_state": FUNDING_PENDING,
        "funding_recovery_status": "pending",
        "funding_recovery_reason": reason,
        "funding_last_checked_at": _iso(now),
        "financial_state": FINANCIAL_PROVISIONAL,
        "data_quality_status": "partial",
        "ambiguity_reason": None,
        "finalized_at": None,
    }
    if extra_values:
        values.update(dict(extra_values))
    saved = await _save_projection(
        execution_id=int(projection["execution_id"]),
        lease_token=str(projection["projection_lease_token"]),
        values=values,
    )
    if not saved:
        raise RuntimeError(
            "statistics projection lease changed before reschedule persistence"
        )
    return StatisticsFinancialOutcome(
        execution_id=int(projection["execution_id"]),
        action="funding_rescheduled",
        projection_status=PROJECTION_RETRY,
        financial_state=FINANCIAL_PROVISIONAL,
        funding_state=FUNDING_PENDING,
        attempts=attempts,
        error=reason,
    )


async def process_statistics_financial_projection_once(
    *,
    adapter_loader: Callable[[int], Awaitable[_FundingAdapter]],
    rate_limiter: _RateLimiter | None,
    now: datetime | None = None,
) -> StatisticsFinancialOutcome | None:
    """Claim and project at most one terminal execution using the same worker."""

    current = now or _utc_now()
    projection = await _claim_due_projection(now=current)
    if not projection:
        return None
    execution_id = int(projection["execution_id"])
    lease = str(projection.get("projection_lease_token") or "")
    if not lease:
        raise RuntimeError("statistics projection claim has no lease")

    g57_scope_recovery = False
    g59_scope_recovery = False
    g60_scope_recovery = False
    try:
        execution, job, fill_rows = await _load_context(execution_id)
        values = build_trading_projection(
            projection=projection,
            execution=execution,
            job=job,
            fill_rows=fill_rows,
        )
        job_status = str(job.get("status") or "").lower()
        if job_status != FINANCIAL_STATUS_CONFIRMED:
            terminal_projection_status = (
                PROJECTION_AMBIGUOUS
                if values.get("financial_state") == FINANCIAL_AMBIGUOUS
                else PROJECTION_UNAVAILABLE
                if values.get("financial_state") == FINANCIAL_UNAVAILABLE
                else PROJECTION_COMPLETE
            )
            values.update(
                projection_status=terminal_projection_status,
                projection_next_attempt_at=_iso(current),
                projection_processing_started_at=None,
                projection_lease_token=None,
                projection_last_error=values.get("ambiguity_reason"),
                finalized_at=_iso(current),
            )
            saved = await _save_projection(
                execution_id=execution_id, lease_token=lease, values=values
            )
            if not saved:
                raise RuntimeError(
                    "statistics projection lease changed before terminal persistence"
                )
            return StatisticsFinancialOutcome(
                execution_id=execution_id,
                action="trading_projection_terminal",
                projection_status=terminal_projection_status,
                financial_state=str(values.get("financial_state")),
                funding_state=str(values.get("funding_state")),
                attempts=int(projection.get("projection_attempts") or 0),
                error=_safe_text(values.get("ambiguity_reason")),
            )

        if values.get("financial_state") == FINANCIAL_AMBIGUOUS:
            values.update(
                projection_status=PROJECTION_AMBIGUOUS,
                funding_state=FUNDING_AMBIGUOUS,
                projection_processing_started_at=None,
                projection_lease_token=None,
                projection_last_error=values.get("ambiguity_reason"),
                finalized_at=_iso(current),
            )
            saved = await _save_projection(
                execution_id=execution_id, lease_token=lease, values=values
            )
            if not saved:
                raise RuntimeError(
                    "statistics projection lease changed before ambiguous persistence"
                )
            return StatisticsFinancialOutcome(
                execution_id=execution_id,
                action="trading_projection_ambiguous",
                projection_status=PROJECTION_AMBIGUOUS,
                financial_state=FINANCIAL_AMBIGUOUS,
                funding_state=FUNDING_AMBIGUOUS,
                attempts=int(projection.get("projection_attempts") or 0),
                error=_safe_text(values.get("ambiguity_reason")),
            )

        # Funding cannot repair an incomplete trading volume.  Keep the
        # already-proven fills/fees as PROVISIONAL and do not spend an API call
        # on a result that can never become FINAL without a trading-history
        # correction.  The due-claim query also excludes this row, preventing a
        # restart/retry loop when funding is enabled later.
        if str(values.get("volume_parity_status") or "") not in {
            "exact",
            "within_tolerance",
        }:
            values.update(
                funding_state=FUNDING_NOT_CHECKED,
                financial_state=FINANCIAL_PROVISIONAL,
                net_pnl=None,
                result_r=None,
                projection_status=PROJECTION_COMPLETE,
                projection_next_attempt_at=_iso(current),
                projection_processing_started_at=None,
                projection_lease_token=None,
                projection_last_error=values.get("ambiguity_reason"),
                finalized_at=None,
            )
            saved = await _save_projection(
                execution_id=execution_id, lease_token=lease, values=values
            )
            if not saved:
                raise RuntimeError(
                    "statistics projection lease changed before incomplete-volume persistence"
                )
            return StatisticsFinancialOutcome(
                execution_id=execution_id,
                action="trading_projection_incomplete_volume",
                projection_status=PROJECTION_COMPLETE,
                financial_state=FINANCIAL_PROVISIONAL,
                funding_state=FUNDING_NOT_CHECKED,
                attempts=int(projection.get("projection_attempts") or 0),
                error=_safe_text(values.get("ambiguity_reason")),
            )

        funding_enabled = bool(get_settings().STATISTICS_FUNDING_ENABLED)
        if not funding_enabled:
            values.update(
                funding_state=FUNDING_NOT_CHECKED,
                financial_state=FINANCIAL_PROVISIONAL,
                net_pnl=None,
                result_r=None,
                projection_status=PROJECTION_COMPLETE,
                projection_next_attempt_at=_iso(current),
                projection_processing_started_at=None,
                projection_lease_token=None,
                projection_last_error=None,
                finalized_at=None,
            )
            saved = await _save_projection(
                execution_id=execution_id, lease_token=lease, values=values
            )
            if not saved:
                raise RuntimeError(
                    "statistics projection lease changed before provisional persistence"
                )
            return StatisticsFinancialOutcome(
                execution_id=execution_id,
                action="trading_projection_provisional",
                projection_status=PROJECTION_COMPLETE,
                financial_state=FINANCIAL_PROVISIONAL,
                funding_state=FUNDING_NOT_CHECKED,
                attempts=int(projection.get("projection_attempts") or 0),
            )

        g47_targeted_recovery = (
            execution_id in _G47_TARGETED_EXECUTION_IDS
            and str(projection.get("funding_recovery_status") or "")
            in {"g47_fallback_pending", "g47_fallback_processing"}
            and str(projection.get("funding_recovery_reason") or "")
            == _G47_REARM_MARKER
        )
        g57_scope_recovery = (
            str(projection.get("funding_recovery_status") or "")
            in {_G57_FALLBACK_PENDING, _G57_FALLBACK_PROCESSING}
            and str(projection.get("funding_recovery_reason") or "")
            == _G57_REARM_MARKER
        )
        g59_scope_recovery = (
            str(projection.get("funding_recovery_status") or "")
            in {G59_FUNDING_PENDING, G59_FUNDING_PROCESSING}
            and str(projection.get("funding_recovery_reason") or "")
            == G59_REARM_MARKER
        )
        g60_scope_recovery = (
            str(projection.get("funding_recovery_status") or "")
            in {G60_FUNDING_PENDING, G60_FUNDING_PROCESSING}
            and str(projection.get("funding_recovery_reason") or "")
            == G60_REARM_MARKER
        )
        g48_unfiltered_recovery = g60_scope_recovery or g59_scope_recovery or g57_scope_recovery or (
            str(projection.get("funding_recovery_status") or "")
            in {"g48_fallback_pending", "g48_fallback_processing"}
            and str(projection.get("funding_recovery_reason") or "")
            in {
                _G48_REARM_MARKER,
                _G49_REARM_MARKER,
                _G51_REARM_MARKER,
                _G52_REARM_MARKER,
            }
        )
        funding_attempt = int(projection.get("funding_recovery_attempts") or 0) + 1
        recovery_processing_status = "processing"
        if g60_scope_recovery:
            recovery_processing_status = G60_FUNDING_PROCESSING
        elif g59_scope_recovery:
            recovery_processing_status = G59_FUNDING_PROCESSING
        elif g57_scope_recovery:
            recovery_processing_status = _G57_FALLBACK_PROCESSING
        elif g48_unfiltered_recovery:
            recovery_processing_status = "g48_fallback_processing"
        elif g47_targeted_recovery:
            recovery_processing_status = "g47_fallback_processing"
        values.update(
            funding_state=FUNDING_PENDING,
            funding_recovery_attempts=funding_attempt,
            funding_recovery_status=recovery_processing_status,
            funding_last_checked_at=_iso(current),
            financial_state=FINANCIAL_PROVISIONAL,
            projection_status=PROJECTION_PROCESSING,
            projection_last_error=None,
        )
        # Keep the in-memory claim aligned with the durable attempt counter so
        # retries and terminal manual-review decisions use funding attempts, not
        # unrelated trading-projection attempts.
        projection = dict(projection)
        projection["funding_recovery_attempts"] = funding_attempt
        provisional_saved = await _save_projection(
            execution_id=execution_id, lease_token=lease, values=values
        )
        if not provisional_saved:
            raise RuntimeError("statistics projection lease changed before funding query")

        first_entry = _datetime(values.get("first_entry_fill_at"), allow_none=False)
        last_exit = _datetime(values.get("last_exit_fill_at"), allow_none=False)
        assert first_entry is not None and last_exit is not None
        if last_exit < first_entry:
            raise ValueError("funding query interval chronology invalid")
        if last_exit - first_entry > timedelta(days=90):
            return await _manual_review_projection(
                projection,
                reason="funding_interval_exceeds_bingx_retention",
                now=current,
                ambiguous=False,
            )
        margin = timedelta(seconds=300)
        query_start = first_entry - margin
        query_end = last_exit + margin
        adapter = await adapter_loader(int(execution["user_id"]))
        query_start_ms = max(1, int(query_start.timestamp() * 1000))
        query_end_ms = int(query_end.timestamp() * 1000)
        recovery_quorum: FundingRecoveryQuorum | None = None
        recovery_version = ""
        if g48_unfiltered_recovery:
            if g60_scope_recovery:
                log.warning(
                    "STATISTICS_FUNDING_G60_GENERIC_QUORUM_START "
                    "execution_id=%s symbol=%s source_attempts=%s",
                    execution_id,
                    str(execution["symbol"]),
                    int(projection.get("funding_recovery_attempts") or 0),
                )
            elif g59_scope_recovery:
                log.warning(
                    "STATISTICS_FUNDING_G59_LOCAL_FILTER_QUORUM_START "
                    "execution_id=%s symbol=%s source_attempts=%s",
                    execution_id,
                    str(execution["symbol"]),
                    int(projection.get("funding_recovery_attempts") or 0),
                )
            elif g57_scope_recovery:
                log.warning(
                    "STATISTICS_FUNDING_G57_SCOPE_QUORUM_START "
                    "execution_id=%s symbol=%s source_attempts=%s",
                    execution_id,
                    str(execution["symbol"]),
                    int(projection.get("funding_recovery_attempts") or 0),
                )
            recovery_quorum = await _fetch_g48_funding_quorum(
                adapter=adapter,
                rate_limiter=rate_limiter,
                execution_id=execution_id,
                symbol=str(execution["symbol"]),
                start_time=first_entry,
                end_time=last_exit,
                start_time_ms=query_start_ms,
                end_time_ms=query_end_ms,
                limit=1000,
            )
            recovery_version = (
                "g60" if g60_scope_recovery
                else "g59" if g59_scope_recovery
                else "g57" if g57_scope_recovery
                else "g48"
            )
            raw_rows = [dict(row) for row in recovery_quorum.rows]
        elif g47_targeted_recovery:
            recovery_quorum = await _fetch_g47_targeted_funding_quorum(
                adapter=adapter,
                rate_limiter=rate_limiter,
                execution_id=execution_id,
                symbol=str(execution["symbol"]),
                start_time=first_entry,
                end_time=last_exit,
                start_time_ms=query_start_ms,
                end_time_ms=query_end_ms,
                limit=1000,
            )
            recovery_version = "g47"
            raw_rows = [dict(row) for row in recovery_quorum.rows]
        else:
            if rate_limiter is not None:
                await rate_limiter.wait()
            raw_rows = await adapter.fetch_funding_income(
                symbol=str(execution["symbol"]),
                start_time_ms=query_start_ms,
                end_time_ms=query_end_ms,
                limit=1000,
            )
        if raw_rows is None:
            raise FundingRequestFailed("funding_endpoint_returned_none")
        if not isinstance(raw_rows, list):
            raise FundingRequestFailed(
                f"funding_endpoint_invalid_type:{type(raw_rows).__name__}"
            )
        records = normalize_funding_rows(
            raw_rows,
            symbol=str(execution["symbol"]),
            start_time=first_entry,
            end_time=last_exit,
        )
        zero_observations = 0
        if not records:
            settings = get_settings()
            observations = int(projection.get("funding_zero_observations") or 0) + 1
            if recovery_quorum is not None:
                observations = max(observations, recovery_quorum.confirmations)
            zero_observations = observations
            first_empty = _datetime(projection.get("funding_first_empty_at")) or current
            grace_end = last_exit + timedelta(
                seconds=int(settings.STATISTICS_FUNDING_ZERO_GRACE_SEC)
            )
            confirmations = int(settings.STATISTICS_FUNDING_ZERO_CONFIRMATIONS)
            if current < grace_end or observations < confirmations:
                pending_reason = (
                    "funding_zero_waiting_confirmation:"
                    f"observations={observations}/{confirmations}:"
                    f"grace_complete={int(current >= grace_end)}"
                )
                minimum_delay = max(
                    60.0,
                    (grace_end - current).total_seconds()
                    if current < grace_end
                    else 0.0,
                )
                zero_retry_values = {
                    "funding_zero_observations": observations,
                    "funding_first_empty_at": _iso(first_empty),
                    "funding_last_checked_at": _iso(current),
                    "funding_query_start_at": _iso(query_start),
                    "funding_query_end_at": _iso(query_end),
                    "funding_event_count": 0,
                    "funding_recovery_status": "pending_zero_confirmation",
                    "funding_recovery_reason": pending_reason,
                }
                if recovery_version == "g60":
                    zero_retry_values.update(
                        funding_recovery_status=G60_FUNDING_PENDING,
                        funding_recovery_reason=G60_REARM_MARKER,
                    )
                elif recovery_version == "g59":
                    zero_retry_values.update(
                        funding_recovery_status=G59_FUNDING_PENDING,
                        funding_recovery_reason=G59_REARM_MARKER,
                    )
                elif recovery_version == "g57":
                    zero_retry_values.update(
                        funding_recovery_status=_G57_FALLBACK_PENDING,
                        funding_recovery_reason=_G57_REARM_MARKER,
                    )
                return await _reschedule_projection(
                    projection,
                    reason=pending_reason,
                    now=current,
                    minimum_delay=minimum_delay,
                    extra_values=zero_retry_values,
                )
        for record in records:
            overlaps = await _overlapping_execution_ids(
                execution_id=execution_id,
                user_id=int(execution["user_id"]),
                symbol=str(execution["symbol"]),
                event_time=record.event_time,
            )
            if overlaps:
                reason = "funding_attribution_overlap:" + ",".join(
                    str(item) for item in overlaps[:20]
                )
                await _persist_ambiguous_funding_events(
                    user_id=int(execution["user_id"]),
                    records=[record],
                    reason=reason,
                )
                raise ValueError(reason)
        funding_total = await _persist_funding_events(
            execution_id=execution_id,
            user_id=int(execution["user_id"]),
            records=records,
        )
        provisional_net = _decimal(values.get("provisional_net_pnl")) or Decimal()
        net = provisional_net + funding_total
        risk = _decimal(values.get("initial_price_risk_usd"), allow_none=True)
        result_r = net / risk if risk is not None and risk > 0 else None
        mask = int(values.get("completeness_mask") or 0) | BIT_FUNDING
        final_quality = "complete" if (mask & BIT_INITIAL_RISK) else "partial"
        if recovery_quorum is not None:
            recovery_reason = (
                f"{recovery_version}_cross_scope_confirmed_rows"
                if records
                else f"{recovery_version}_cross_scope_confirmed_zero"
            ) + (
                f":quorum={recovery_quorum.confirmations}:"
                f"variants={','.join(recovery_quorum.variants)}:"
                f"fingerprint={recovery_quorum.fingerprint[:16]}"
            )
        else:
            recovery_reason = (
                "confirmed_rows" if records
                else "confirmed_zero_after_grace_and_rechecks"
            )
        values.update(
            funding_signed=_plain(funding_total),
            funding_state=(FUNDING_CONFIRMED if records else FUNDING_CONFIRMED_ZERO),
            net_pnl=_plain(net),
            result_r=_plain(result_r),
            financial_state=FINANCIAL_FINAL,
            completeness_mask=mask,
            completeness_percent=_plain(_completeness(mask)),
            data_quality_status=final_quality,
            ambiguity_reason=None,
            projection_status=PROJECTION_COMPLETE,
            projection_next_attempt_at=_iso(current),
            projection_processing_started_at=None,
            projection_lease_token=None,
            projection_last_error=None,
            funding_query_start_at=_iso(query_start),
            funding_query_end_at=_iso(query_end),
            funding_event_count=len(records),
            funding_zero_observations=(zero_observations if not records else 0),
            funding_first_empty_at=(
                _iso(_datetime(projection.get("funding_first_empty_at")) or current)
                if not records else None
            ),
            funding_last_checked_at=_iso(current),
            funding_recovery_status="complete",
            funding_recovery_reason=recovery_reason,
            funding_finalized_at=_iso(current),
            finalized_at=_iso(current),
        )
        gate_decision = await evaluate_statistics_final_candidate(
            execution_id=execution_id,
            candidate_values=values,
        )
        values.update(
            quality_reasons_json=gate_decision.reasons_json(),
            final_eligible=int(gate_decision.final_eligible),
            simulation_eligible=int(gate_decision.simulation_eligible),
            risk_analysis_eligible=int(gate_decision.risk_analysis_eligible),
            quality_gate_version=QUALITY_GATE_VERSION,
        )
        if not gate_decision.final_eligible:
            gate_reason = _safe_text(
                "statistics_final_quality_gate_failed:"
                + ",".join(gate_decision.reasons),
                limit=800,
            )
            values.update(
                financial_state=FINANCIAL_PROVISIONAL,
                projection_status=PROJECTION_UNAVAILABLE,
                projection_last_error=gate_reason,
                data_quality_status="partial",
                ambiguity_reason=gate_reason,
                finalized_at=None,
                final_eligible=0,
            )
            saved = await _save_projection(
                execution_id=execution_id, lease_token=lease, values=values
            )
            if not saved:
                raise RuntimeError(
                    "statistics projection lease changed before quality-gate block"
                )
            return StatisticsFinancialOutcome(
                execution_id=execution_id,
                action="financial_projection_quality_gate_blocked",
                projection_status=PROJECTION_UNAVAILABLE,
                financial_state=FINANCIAL_PROVISIONAL,
                funding_state=str(values["funding_state"]),
                attempts=int(projection.get("funding_recovery_attempts") or 0),
                error=gate_reason,
            )
        saved = await _save_projection(
            execution_id=execution_id, lease_token=lease, values=values
        )
        if not saved:
            raise RuntimeError("statistics projection lease changed before finalize")
        return StatisticsFinancialOutcome(
            execution_id=execution_id,
            action="financial_projection_final",
            projection_status=PROJECTION_COMPLETE,
            financial_state=FINANCIAL_FINAL,
            funding_state=str(values["funding_state"]),
            attempts=int(projection.get("funding_recovery_attempts") or 0),
        )
    except FundingRequestFailed as exc:
        if g60_scope_recovery:
            reason = (
                "statistics_funding_g60_quorum_ambiguous:" + _safe_text(exc)
            )
            log.warning(
                "STATISTICS_FUNDING_G60_GENERIC_QUORUM_BLOCKED "
                "execution_id=%s reason=%s",
                execution_id,
                _safe_text(exc, limit=500),
            )
            return await _manual_review_projection(
                projection,
                reason=reason,
                now=current,
                ambiguous=True,
            )
        if g59_scope_recovery:
            reason = (
                "statistics_funding_g59_quorum_ambiguous:" + _safe_text(exc)
            )
            log.warning(
                "STATISTICS_FUNDING_G59_LOCAL_FILTER_QUORUM_BLOCKED "
                "execution_id=%s reason=%s",
                execution_id,
                _safe_text(exc, limit=500),
            )
            return await _manual_review_projection(
                projection,
                reason=reason,
                now=current,
                ambiguous=True,
            )
        if g57_scope_recovery:
            reason = (
                "statistics_funding_g57_quorum_ambiguous:" + _safe_text(exc)
            )
            log.warning(
                "STATISTICS_FUNDING_G57_SCOPE_QUORUM_BLOCKED "
                "execution_id=%s reason=%s",
                execution_id,
                _safe_text(exc, limit=500),
            )
            return await _manual_review_projection(
                projection,
                reason=reason,
                now=current,
                ambiguous=True,
            )
        return await _reschedule_projection(
            projection,
            reason=f"statistics_funding_request_failed:{_safe_text(exc)}",
            now=current,
        )
    except BingxResponseIntegrityError as exc:
        detail = _safe_text(exc)
        if _is_retryable_funding_integrity_error(exc):
            return await _reschedule_projection(
                projection,
                reason=f"statistics_funding_integrity_retryable:{detail}",
                now=current,
            )
        if "truncated" in detail.lower():
            return await _reschedule_projection(
                projection,
                reason=f"statistics_funding_incomplete:{detail}",
                now=current,
            )
        return await _manual_review_projection(
            projection,
            reason=f"statistics_funding_integrity_ambiguous:{detail}",
            now=current,
            ambiguous=True,
        )
    except (ValueError, InvalidOperation) as exc:
        if g60_scope_recovery:
            reason = "statistics_funding_g60_quorum_ambiguous:" + _safe_text(exc)
            log.warning(
                "STATISTICS_FUNDING_G60_GENERIC_QUORUM_CONFLICT "
                "execution_id=%s reason=%s",
                execution_id,
                _safe_text(exc, limit=500),
            )
            return await _manual_review_projection(
                projection, reason=reason, now=current, ambiguous=True
            )
        if g59_scope_recovery:
            reason = "statistics_funding_g59_quorum_ambiguous:" + _safe_text(exc)
            log.warning(
                "STATISTICS_FUNDING_G59_LOCAL_FILTER_QUORUM_CONFLICT "
                "execution_id=%s reason=%s",
                execution_id,
                _safe_text(exc, limit=500),
            )
            return await _manual_review_projection(
                projection, reason=reason, now=current, ambiguous=True
            )
        return await _manual_review_projection(
            projection,
            reason=f"statistics_funding_attribution_ambiguous:{_safe_text(exc)}",
            now=current,
            ambiguous=True,
        )
    except Exception as exc:
        # Exchange/network errors are retried by the projection's own durable
        # schedule.  No write to trading state is performed.
        log.warning(
            "STATISTICS_FINANCIAL_PROJECTION_RETRY execution_id=%s error=%s",
            execution_id,
            type(exc).__name__,
        )
        return await _reschedule_projection(
            projection,
            reason=f"statistics_financial_retry:{type(exc).__name__}:{_safe_text(exc)}",
            now=current,
        )
