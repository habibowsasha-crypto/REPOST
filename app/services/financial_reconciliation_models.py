"""Pure domain models for deferred BingX financial reconciliation.

This module deliberately contains no database, Telegram or exchange calls.  It
normalizes exact order expectations and fill records so later workers can remain
idempotent and fail closed when BingX history is incomplete or ambiguous.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping, Sequence


FINANCIAL_STATUS_PENDING = "pending"
FINANCIAL_STATUS_PROCESSING = "processing"
FINANCIAL_STATUS_CONFIRMED = "confirmed"
FINANCIAL_STATUS_PARTIAL = "partial"
FINANCIAL_STATUS_AMBIGUOUS = "ambiguous"
FINANCIAL_STATUS_UNAVAILABLE = "unavailable"

FINANCIAL_ACTIVE_STATUSES = frozenset(
    {FINANCIAL_STATUS_PENDING, FINANCIAL_STATUS_PROCESSING}
)
FINANCIAL_TERMINAL_STATUSES = frozenset(
    {
        FINANCIAL_STATUS_CONFIRMED,
        FINANCIAL_STATUS_PARTIAL,
        FINANCIAL_STATUS_AMBIGUOUS,
        FINANCIAL_STATUS_UNAVAILABLE,
    }
)
FINANCIAL_ALL_STATUSES = FINANCIAL_ACTIVE_STATUSES | FINANCIAL_TERMINAL_STATUSES

FINANCIAL_FINAL_CLOSE_TYPES = frozenset({"be_stop", "stop", "all_tps"})

ORDER_ROLE_ENTRY = "entry"
ORDER_ROLE_TP = "tp"
ORDER_ROLE_STOP = "stop"
ORDER_ROLE_BE_STOP = "be_stop"
ORDER_ROLE_FINAL_TP = "final_tp"
ORDER_ROLE_UNKNOWN = "unknown"
FINANCIAL_ORDER_ROLES = frozenset(
    {
        ORDER_ROLE_ENTRY,
        ORDER_ROLE_TP,
        ORDER_ROLE_STOP,
        ORDER_ROLE_BE_STOP,
        ORDER_ROLE_FINAL_TP,
        ORDER_ROLE_UNKNOWN,
    }
)

ORDER_STATUS_EXPECTED = "expected"
ORDER_STATUS_CONFIRMED = "confirmed"
ORDER_STATUS_MISSING = "missing"
ORDER_STATUS_AMBIGUOUS = "ambiguous"
ORDER_STATUS_UNAVAILABLE = "unavailable"
FINANCIAL_ORDER_STATUSES = frozenset(
    {
        ORDER_STATUS_EXPECTED,
        ORDER_STATUS_CONFIRMED,
        ORDER_STATUS_MISSING,
        ORDER_STATUS_AMBIGUOUS,
        ORDER_STATUS_UNAVAILABLE,
    }
)

_NOTIFICATION_STATUSES = frozenset({"pending", "queued", "delivered", "skipped"})
_IDENTIFIER_RE = re.compile(r"^[^\x00\r\n]{1,160}$")


def _clean_text(value: Any, *, field: str, max_length: int = 160) -> str:
    text = str(value or "").strip().replace("\x00", "")
    if not text:
        raise ValueError(f"{field} is required")
    if len(text) > max_length:
        raise ValueError(f"{field} is too long")
    if not _IDENTIFIER_RE.fullmatch(text):
        raise ValueError(f"{field} contains unsupported control characters")
    return text


def optional_text(value: Any, *, max_length: int = 1000) -> str | None:
    if value is None:
        return None
    text = str(value).strip().replace("\x00", "")
    if not text:
        return None
    return text[:max_length]


def decimal_text(
    value: Any,
    *,
    field: str,
    allow_none: bool = False,
    nonnegative: bool = False,
    positive: bool = False,
) -> str | None:
    if value in (None, ""):
        if allow_none:
            return None
        raise ValueError(f"{field} is required")
    if isinstance(value, bool):
        raise ValueError(f"{field} must be numeric")
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be numeric") from exc
    if not parsed.is_finite():
        raise ValueError(f"{field} must be finite")
    if nonnegative and parsed < 0:
        raise ValueError(f"{field} must be nonnegative")
    if positive and parsed <= 0:
        raise ValueError(f"{field} must be positive")
    # Normalize exponent notation while preserving exact decimal semantics.
    normalized = format(parsed, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    if normalized in {"", "-0"}:
        normalized = "0"
    return normalized


def normalize_close_type(value: Any) -> str:
    close_type = str(value or "").strip().lower()
    if close_type not in FINANCIAL_FINAL_CLOSE_TYPES:
        raise ValueError("financial reconciliation supports only be_stop/stop/all_tps")
    return close_type


def normalize_side(value: Any) -> str:
    side = str(value or "").strip().lower()
    if side not in {"long", "short"}:
        raise ValueError("side must be long or short")
    return side


def normalize_status(value: Any, *, terminal_only: bool = False) -> str:
    status = str(value or "").strip().lower()
    allowed = FINANCIAL_TERMINAL_STATUSES if terminal_only else FINANCIAL_ALL_STATUSES
    if status not in allowed:
        raise ValueError(f"unsupported financial status: {status or '<empty>'}")
    return status


def normalize_notification_status(value: Any) -> str:
    status = str(value or "pending").strip().lower()
    if status not in _NOTIFICATION_STATUSES:
        raise ValueError(f"unsupported financial notification status: {status}")
    return status


def normalize_datetime_text(value: Any, *, field: str, allow_none: bool = True) -> str | None:
    if value in (None, ""):
        if allow_none:
            return None
        raise ValueError(f"{field} is required")
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value).strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        if " " in raw and "T" not in raw:
            raw = raw.replace(" ", "T", 1)
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(f"{field} must be an ISO datetime") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed = parsed.astimezone(timezone.utc)
    return parsed.isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def order_key(*, exchange_order_id: Any = None, client_order_id: Any = None) -> str:
    exchange_id = optional_text(exchange_order_id, max_length=128)
    client_id = optional_text(client_order_id, max_length=128)
    if exchange_id:
        return f"order:{exchange_id}"
    if client_id:
        return f"client:{client_id}"
    raise ValueError("financial order expectation requires orderId or clientOrderID")


@dataclass(frozen=True)
class FinancialOrderExpectation:
    order_key: str
    exchange_order_id: str | None
    client_order_id: str | None
    role: str
    tp_index: int
    required: bool
    expected_qty: str | None
    metadata_json: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "FinancialOrderExpectation":
        exchange_order_id = optional_text(
            value.get("exchange_order_id", value.get("order_id")), max_length=128
        )
        client_order_id = optional_text(
            value.get("client_order_id", value.get("clientOrderID")), max_length=128
        )
        key = order_key(
            exchange_order_id=exchange_order_id,
            client_order_id=client_order_id,
        )
        role = str(value.get("role") or ORDER_ROLE_UNKNOWN).strip().lower()
        if role not in FINANCIAL_ORDER_ROLES:
            raise ValueError(f"unsupported financial order role: {role}")
        tp_index = int(value.get("tp_index") or 0)
        if tp_index < 0 or tp_index > 100:
            raise ValueError("tp_index must be between 0 and 100")
        if role in {ORDER_ROLE_TP, ORDER_ROLE_FINAL_TP} and tp_index <= 0:
            raise ValueError("TP order expectation requires tp_index > 0")
        expected_qty = decimal_text(
            value.get("expected_qty"),
            field="expected_qty",
            allow_none=True,
            positive=True,
        )
        metadata = value.get("metadata")
        if metadata is None:
            metadata = value.get("metadata_json")
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except (TypeError, ValueError, json.JSONDecodeError):
                    metadata = {"raw": metadata[:1000]}
        if not isinstance(metadata, Mapping):
            metadata = {}
        return cls(
            order_key=key,
            exchange_order_id=exchange_order_id,
            client_order_id=client_order_id,
            role=role,
            tp_index=tp_index,
            required=bool(value.get("required", True)),
            expected_qty=expected_qty,
            metadata_json=canonical_json(dict(metadata)),
        )


def normalize_order_expectations(
    values: Iterable[Mapping[str, Any] | FinancialOrderExpectation],
) -> list[FinancialOrderExpectation]:
    by_key: dict[str, FinancialOrderExpectation] = {}
    for raw in values:
        item = raw if isinstance(raw, FinancialOrderExpectation) else FinancialOrderExpectation.from_mapping(raw)
        previous = by_key.get(item.order_key)
        if previous is None:
            by_key[item.order_key] = item
            continue
        # Duplicate identities must describe the same logical order.  A mismatch
        # is safer to reject than to merge into an ambiguous ownership record.
        immutable = (item.role, item.tp_index)
        previous_immutable = (previous.role, previous.tp_index)
        if immutable != previous_immutable:
            raise ValueError(f"conflicting financial order expectation: {item.order_key}")
        if (
            item.exchange_order_id
            and previous.exchange_order_id
            and item.exchange_order_id != previous.exchange_order_id
        ):
            raise ValueError(f"conflicting exchange order identity: {item.order_key}")
        if (
            item.client_order_id
            and previous.client_order_id
            and item.client_order_id != previous.client_order_id
        ):
            raise ValueError(f"conflicting client order identity: {item.order_key}")
        if (
            item.expected_qty is not None
            and previous.expected_qty is not None
            and Decimal(item.expected_qty) != Decimal(previous.expected_qty)
        ):
            raise ValueError(f"conflicting expected quantity: {item.order_key}")
        by_key[item.order_key] = FinancialOrderExpectation(
            order_key=item.order_key,
            exchange_order_id=item.exchange_order_id or previous.exchange_order_id,
            client_order_id=item.client_order_id or previous.client_order_id,
            role=item.role,
            tp_index=item.tp_index,
            required=bool(item.required or previous.required),
            expected_qty=item.expected_qty or previous.expected_qty,
            metadata_json=item.metadata_json if item.metadata_json != "{}" else previous.metadata_json,
        )
    return [by_key[key] for key in sorted(by_key)]


@dataclass(frozen=True)
class FinancialFillRecord:
    trade_id: str
    order_id: str
    order_key: str
    role: str
    tp_index: int
    symbol: str
    side: str
    price: str
    qty: str
    realized_pnl: str
    fee: str
    fee_asset: str | None
    fill_time: str
    fingerprint: str
    metadata_json: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "FinancialFillRecord":
        trade_id = _clean_text(
            value.get("trade_id", value.get("tradeId")), field="trade_id", max_length=128
        )
        order_id = _clean_text(
            value.get("order_id", value.get("orderId")), field="order_id", max_length=128
        )
        role = str(value.get("role") or ORDER_ROLE_UNKNOWN).strip().lower()
        if role not in FINANCIAL_ORDER_ROLES:
            raise ValueError(f"unsupported financial fill role: {role}")
        tp_index = int(value.get("tp_index") or 0)
        if tp_index < 0 or tp_index > 100:
            raise ValueError("tp_index must be between 0 and 100")
        if role in {ORDER_ROLE_TP, ORDER_ROLE_FINAL_TP} and tp_index <= 0:
            raise ValueError("TP fill requires tp_index > 0")
        if role not in {ORDER_ROLE_TP, ORDER_ROLE_FINAL_TP} and tp_index != 0:
            raise ValueError("non-TP fill requires tp_index = 0")
        symbol = _clean_text(value.get("symbol"), field="symbol", max_length=64).upper()
        side = normalize_side(value.get("side"))
        price = decimal_text(value.get("price"), field="price", positive=True)
        qty = decimal_text(
            value.get("qty", value.get("quantity")), field="qty", positive=True
        )
        realized_pnl = decimal_text(
            value.get("realized_pnl", value.get("realizedPnl", 0)),
            field="realized_pnl",
        )
        fee = decimal_text(value.get("fee", 0), field="fee")
        fee_asset = optional_text(
            value.get("fee_asset", value.get("feeAsset")), max_length=32
        )
        fill_time = normalize_datetime_text(
            value.get("fill_time", value.get("time")),
            field="fill_time",
            allow_none=False,
        )
        metadata = value.get("metadata")
        if metadata is None:
            metadata = value.get("metadata_json")
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except (TypeError, ValueError, json.JSONDecodeError):
                    metadata = {"raw": metadata[:1000]}
        if not isinstance(metadata, Mapping):
            metadata = {}
        payload = {
            "trade_id": trade_id,
            "order_id": order_id,
            "role": role,
            "tp_index": tp_index,
            "symbol": symbol,
            "side": side,
            "price": price,
            "qty": qty,
            "realized_pnl": realized_pnl,
            "fee": fee,
            "fee_asset": fee_asset,
            "fill_time": fill_time,
        }
        fingerprint = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
        return cls(
            trade_id=trade_id,
            order_id=order_id,
            order_key=f"order:{order_id}",
            role=role,
            tp_index=tp_index,
            symbol=symbol,
            side=side,
            price=str(price),
            qty=str(qty),
            realized_pnl=str(realized_pnl),
            fee=str(fee),
            fee_asset=fee_asset.upper() if fee_asset else None,
            fill_time=str(fill_time),
            fingerprint=fingerprint,
            metadata_json=canonical_json(dict(metadata)),
        )


def normalize_fill_records(
    values: Iterable[Mapping[str, Any] | FinancialFillRecord],
) -> list[FinancialFillRecord]:
    by_trade_id: dict[str, FinancialFillRecord] = {}
    for raw in values:
        item = raw if isinstance(raw, FinancialFillRecord) else FinancialFillRecord.from_mapping(raw)
        previous = by_trade_id.get(item.trade_id)
        if previous is not None and previous.fingerprint != item.fingerprint:
            raise ValueError(f"conflicting duplicate trade_id: {item.trade_id}")
        by_trade_id[item.trade_id] = item
    return [by_trade_id[key] for key in sorted(by_trade_id)]


def fill_matches_expectation(
    fill: FinancialFillRecord, expectation: FinancialOrderExpectation
) -> bool:
    """Return whether a normalized fill matches its durable expected role.

    ``tp`` and ``final_tp`` are the same exchange-side order family, but the
    durable expectation keeps the final TP distinction for reporting. All other
    roles must match exactly.
    """

    tp_roles = {ORDER_ROLE_TP, ORDER_ROLE_FINAL_TP}
    if fill.role in tp_roles or expectation.role in tp_roles:
        return (
            fill.role in tp_roles
            and expectation.role in tp_roles
            and fill.tp_index == expectation.tp_index
        )
    return fill.role == expectation.role and fill.tp_index == expectation.tp_index


def aggregate_fill_records(
    values: Sequence[FinancialFillRecord],
) -> dict[str, Any]:
    gross = Decimal("0")
    fee = Decimal("0")
    order_ids: set[str] = set()
    fee_assets: set[str] = set()
    total_qty = Decimal("0")
    derive_gross_from_prices = False
    for item in values:
        try:
            metadata = json.loads(item.metadata_json or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            metadata = {}
        if str(metadata.get("realized_pnl_source") or "exchange") == (
            "derived_from_fill_prices"
        ):
            derive_gross_from_prices = True
        gross += Decimal(item.realized_pnl)
        fee += Decimal(item.fee)
        total_qty += Decimal(item.qty)
        order_ids.add(item.order_id)
        if item.fee_asset:
            fee_assets.add(item.fee_asset)

    if derive_gross_from_prices:
        # The current U-M fillHistory contract exposes exact fill prices,
        # quantities and commissions but no realizedPnl field.  For a linear
        # USDT/USDC contract, gross PnL is the signed entry/close notional
        # difference.  Every order quantity is independently reconciled before
        # a job can become confirmed, so this cannot hide a missing partial fill.
        gross = Decimal()
        close_roles = {
            ORDER_ROLE_TP,
            ORDER_ROLE_FINAL_TP,
            ORDER_ROLE_STOP,
            ORDER_ROLE_BE_STOP,
        }
        for item in values:
            notional = Decimal(item.price) * Decimal(item.qty)
            if item.role == ORDER_ROLE_ENTRY:
                gross += -notional if item.side == "long" else notional
            elif item.role in close_roles:
                gross += notional if item.side == "long" else -notional
            else:
                raise ValueError(
                    f"cannot derive gross PnL for unsupported fill role: {item.role}"
                )
    return {
        "exchange_gross_pnl": decimal_text(gross, field="exchange_gross_pnl"),
        "total_trading_fee": decimal_text(fee, field="total_trading_fee"),
        "net_pnl_after_trading_fee": decimal_text(
            gross + fee, field="net_pnl_after_trading_fee"
        ),
        "fill_count": len(values),
        "confirmed_order_count": len(order_ids),
        "confirmed_qty": decimal_text(total_qty, field="confirmed_qty", nonnegative=True),
        "fee_asset": next(iter(fee_assets)) if len(fee_assets) == 1 else None,
        "mixed_fee_assets": len(fee_assets) > 1,
    }


def finite_float(value: Any) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("value must be finite")
    return parsed
