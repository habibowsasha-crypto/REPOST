"""Exact BingX fill binding for deferred financial reconciliation.

The module is intentionally runtime-neutral: it does not start workers, touch
Telegram, or alter trading lifecycle.  It binds already fetched BingX
``fillHistory`` rows to one durable order expectation only after exact order
identity, symbol, direction, time window, and quantity checks succeed.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Protocol, Sequence

from app.services.exchange_identity import clean_exchange_id
from app.services.financial_reconciliation_models import (
    ORDER_ROLE_BE_STOP,
    ORDER_ROLE_ENTRY,
    ORDER_ROLE_FINAL_TP,
    ORDER_ROLE_STOP,
    ORDER_ROLE_TP,
    FinancialFillRecord,
    FinancialOrderExpectation,
    normalize_fill_records,
)


class BingxFinancialFillBindingError(ValueError):
    """BingX fill history cannot be attributed safely to one expected order."""


class _FillHistoryAdapter(Protocol):
    async def fetch_trade_fills(
        self,
        *,
        symbol: str,
        order_id: str,
        start_time_ms: int,
        end_time_ms: int,
        trading_unit: str = "CONT",
        currency: str | None = None,
    ) -> list[dict[str, Any]]: ...


_CLOSE_ROLES = frozenset(
    {ORDER_ROLE_TP, ORDER_ROLE_FINAL_TP, ORDER_ROLE_STOP, ORDER_ROLE_BE_STOP}
)
_EXACT_ROLES = frozenset({ORDER_ROLE_ENTRY, *_CLOSE_ROLES})


def _canonical_symbol(value: Any) -> str:
    text = str(value or "").strip().upper().replace("/", "").replace("_", "").replace("-", "")
    if not text or not text.isascii() or not text.isalnum() or len(text) > 40:
        raise BingxFinancialFillBindingError("invalid BingX fill symbol")
    if not (text.endswith("USDT") or text.endswith("USDC")):
        raise BingxFinancialFillBindingError("unsupported BingX settlement currency")
    return text


def _plain_decimal(value: Any, *, field: str, positive: bool = False) -> str:
    if value in (None, "") or isinstance(value, bool):
        raise BingxFinancialFillBindingError(f"{field} is missing or invalid")
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise BingxFinancialFillBindingError(f"{field} is not numeric") from exc
    if not parsed.is_finite():
        raise BingxFinancialFillBindingError(f"{field} is not finite")
    if positive and parsed <= 0:
        raise BingxFinancialFillBindingError(f"{field} must be positive")
    text = format(parsed, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in {"", "-0"} else text


def _millisecond_time(value: Any, *, start_time_ms: int, end_time_ms: int) -> tuple[int, str]:
    if isinstance(value, bool):
        raise BingxFinancialFillBindingError("fill time is invalid")
    try:
        millis = int(str(value).strip())
    except (TypeError, ValueError, OverflowError) as exc:
        raise BingxFinancialFillBindingError("fill time is invalid") from exc
    if millis <= 0 or millis < start_time_ms or millis > end_time_ms:
        raise BingxFinancialFillBindingError("fill time is outside requested window")
    iso = datetime.fromtimestamp(millis / 1000, tz=timezone.utc).isoformat()
    return millis, iso


def expected_bingx_trade_side(*, execution_side: Any, role: str) -> str:
    side = str(execution_side or "").strip().lower()
    if side not in {"long", "short"}:
        raise BingxFinancialFillBindingError("execution side must be long or short")
    normalized_role = str(role or "").strip().lower()
    if normalized_role not in _EXACT_ROLES:
        raise BingxFinancialFillBindingError(
            "exact fill binding supports entry/tp/final_tp/stop/be_stop only"
        )
    if normalized_role == ORDER_ROLE_ENTRY:
        return "BUY" if side == "long" else "SELL"
    return "SELL" if side == "long" else "BUY"


def bind_bingx_fill_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    expectation: FinancialOrderExpectation | Mapping[str, Any],
    symbol: Any,
    execution_side: Any,
    start_time_ms: int,
    end_time_ms: int,
    currency: str | None = None,
    trading_unit: str = "CONT",
) -> list[FinancialFillRecord]:
    """Bind normalized ``fillHistory`` rows to one exact durable expectation.

    Empty history is valid and means the worker should retry later.  Any row
    outside the exact order/symbol/direction/time scope fails closed rather than
    being ignored, because an ignored cross-scope row would hide an exchange
    filter or response-integrity failure.
    """

    item = (
        expectation
        if isinstance(expectation, FinancialOrderExpectation)
        else FinancialOrderExpectation.from_mapping(expectation)
    )
    order_id = clean_exchange_id(item.exchange_order_id)
    if not order_id:
        raise BingxFinancialFillBindingError(
            "exact exchange_order_id is required before querying fill history"
        )
    if item.role not in _EXACT_ROLES:
        raise BingxFinancialFillBindingError(
            "exact fill binding supports entry/tp/final_tp/stop/be_stop only"
        )
    if isinstance(start_time_ms, bool) or isinstance(end_time_ms, bool):
        raise BingxFinancialFillBindingError("fill history time window is invalid")
    try:
        start_ms = int(start_time_ms)
        end_ms = int(end_time_ms)
    except (TypeError, ValueError, OverflowError) as exc:
        raise BingxFinancialFillBindingError("fill history time window is invalid") from exc
    if start_ms <= 0 or end_ms <= 0 or start_ms >= end_ms:
        raise BingxFinancialFillBindingError("fill history time window is invalid")

    wanted_symbol = _canonical_symbol(symbol)
    wanted_side = expected_bingx_trade_side(execution_side=execution_side, role=item.role)
    position_side = str(execution_side).strip().lower()
    unit = str(trading_unit or "CONT").strip().upper()
    if unit != "CONT":
        raise BingxFinancialFillBindingError(
            "financial reconciliation requires CONT trading unit for exact quantity parity"
        )
    quote = wanted_symbol[-4:]
    requested_currency = str(currency or quote).strip().upper()
    if requested_currency not in {"USDT", "USDC"} or requested_currency != quote:
        raise BingxFinancialFillBindingError("currency does not match requested symbol")

    mapped: list[FinancialFillRecord] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise BingxFinancialFillBindingError(f"fill row[{index}] is not an object")
        row_order_id = clean_exchange_id(row.get("orderId", row.get("order_id")))
        if not row_order_id or row_order_id != order_id:
            raise BingxFinancialFillBindingError(
                f"fill row[{index}] does not match expected orderId"
            )
        row_symbol = _canonical_symbol(row.get("symbol"))
        if row_symbol != wanted_symbol:
            raise BingxFinancialFillBindingError(
                f"fill row[{index}] does not match expected symbol"
            )
        raw_trade_side = str(row.get("side") or "").strip().upper()
        if raw_trade_side != wanted_side:
            raise BingxFinancialFillBindingError(
                f"fill row[{index}] has unexpected BUY/SELL direction"
            )
        raw_position_side = str(row.get("positionSide") or "").strip().upper()
        if raw_position_side and raw_position_side != position_side.upper():
            raise BingxFinancialFillBindingError(
                f"fill row[{index}] has unexpected positionSide"
            )
        trade_id = clean_exchange_id(row.get("tradeId", row.get("trade_id")))
        if not trade_id:
            raise BingxFinancialFillBindingError(f"fill row[{index}] has no tradeId")
        _millis, fill_time = _millisecond_time(
            row.get("time", row.get("fill_time")),
            start_time_ms=start_ms,
            end_time_ms=end_ms,
        )
        fee_asset = str(row.get("feeAsset") or row.get("fee_asset") or requested_currency).strip().upper()
        if fee_asset != requested_currency:
            raise BingxFinancialFillBindingError(
                f"fill row[{index}] fee asset does not match settlement currency"
            )
        metadata = {
            "source": "bingx_fill_history",
            "raw_trade_side": raw_trade_side,
            "raw_position_side": raw_position_side,
            "trading_unit": unit,
            "query_start_ts": start_ms,
            "query_end_ts": end_ms,
            "realized_pnl_source": str(
                row.get("realizedPnlSource") or "exchange"
            ),
        }
        raw_metadata = row.get("raw")
        if isinstance(raw_metadata, Mapping):
            metadata["raw"] = dict(raw_metadata)
        mapped.append(
            FinancialFillRecord.from_mapping(
                {
                    "trade_id": trade_id,
                    "order_id": order_id,
                    "role": item.role,
                    "tp_index": item.tp_index,
                    "symbol": wanted_symbol,
                    "side": position_side,
                    "price": _plain_decimal(row.get("price"), field="price", positive=True),
                    "qty": _plain_decimal(row.get("qty"), field="qty", positive=True),
                    "realized_pnl": _plain_decimal(
                        row.get("realizedPnl", row.get("realized_pnl")),
                        field="realizedPnl",
                    ),
                    "fee": _plain_decimal(row.get("fee"), field="fee"),
                    "fee_asset": requested_currency,
                    "fill_time": fill_time,
                    "metadata": metadata,
                }
            )
        )

    normalized = normalize_fill_records(mapped)
    if item.expected_qty is not None:
        confirmed_qty = sum((Decimal(record.qty) for record in normalized), Decimal("0"))
        expected_qty = Decimal(item.expected_qty)
        tolerance = max(Decimal("1e-12"), abs(expected_qty) * Decimal("1e-12"))
        if confirmed_qty > expected_qty + tolerance:
            raise BingxFinancialFillBindingError(
                "fill quantity exceeds durable expected order quantity"
            )
    return sorted(normalized, key=lambda record: (record.fill_time, record.trade_id))


async def fetch_and_bind_bingx_order_fills(
    adapter: _FillHistoryAdapter,
    *,
    expectation: FinancialOrderExpectation | Mapping[str, Any],
    symbol: Any,
    execution_side: Any,
    start_time_ms: int,
    end_time_ms: int,
    currency: str | None = None,
) -> list[FinancialFillRecord]:
    """Read and bind one expected order without any lifecycle side effects."""

    item = (
        expectation
        if isinstance(expectation, FinancialOrderExpectation)
        else FinancialOrderExpectation.from_mapping(expectation)
    )
    order_id = clean_exchange_id(item.exchange_order_id)
    if not order_id:
        raise BingxFinancialFillBindingError(
            "exact exchange_order_id is required before querying fill history"
        )
    rows = await adapter.fetch_trade_fills(
        symbol=str(symbol),
        order_id=order_id,
        start_time_ms=int(start_time_ms),
        end_time_ms=int(end_time_ms),
        trading_unit="CONT",
        currency=currency,
    )
    return bind_bingx_fill_rows(
        rows,
        expectation=item,
        symbol=symbol,
        execution_side=execution_side,
        start_time_ms=int(start_time_ms),
        end_time_ms=int(end_time_ms),
        currency=currency,
        trading_unit="CONT",
    )
