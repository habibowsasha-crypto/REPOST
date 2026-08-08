from __future__ import annotations

import math
import asyncio
import json
import hashlib
import logging
import re
import time
from dataclasses import replace
from collections import OrderedDict
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN
from datetime import datetime, timezone
from typing import Any, Callable, Optional
import threading as _threading

from app.config import get_settings
from app.services.admin_only_mode import admin_only_trade_user_allowed
from app.services.monitor_diagnostics import record_stage_rows
from app.database import db
from app.services.exchange_factory import (
    NetworkAmbiguousErrors,
    SymbolNotSupportedErrors,
    build_adapter,
    entry_order_type,
    exchange_title,
)
from app.services.models import ExecutionResult, Signal, TpMode, UserMode
from app.services.trade_notification_policy import (
    mandatory_trade_warning_payload,
    optional_trade_skip_payload,
)
from app.services.exchange_identity import clean_exchange_id
from app.services.notification_style import ensure_visual_card, card, details_line
from app.exchanges.bingx import BingxMarketProtectionError as BingxMarketProtectionError
from app.exchanges.bingx.adapter import (
    BingxExchangeRejected,
    BingxOrderCancelRejected,
)
from app.exchanges.bingx.symbols import (
    bingx_tradfi_exchange_symbol,
    canonical_bingx_tradfi_symbol,
)
from app.services.risk_engine import calculate_position_size
from app.services.signal_parser import signal_hash
from app.services.signal_analytics_ingress import submit_statistics_execution_linkage
from app.services.bingx_contract_aliases import (
    canonicalize_bingx_1000_signal,
    scale_bingx_1000_market_hint,
)
from app.services.tp_distribution import (
    acceleration_distribution,
    bell_distribution,
    early_fixation_distribution,
    equal_distribution,
    limit_targets_for_mode,
    manual_distribution,
    smart_scale_out_distribution,
)
from app.services.tp_qty import (
    build_tp_plan,
    order_normalized_qty,
    order_required_qty_step,
)
from app.services.limit_policy import (
    POLICY_KEY as LIMIT_POLICY_KEY,
    build_policy as build_limit_policy,
)
from app.services.tp_plan_snapshot import (
    POLICY_KEY,
    SNAPSHOT_KEY,
    build_policy,
    build_snapshot_from_plan,
    snapshot_items,
)
from app.services.ttl_cache import (
    get_api_key_cache,
    get_instrument_info_cache,
    get_market_price_cache,
    get_user_settings_cache,
)
from app.services.positions_cache import get_global_positions_cache
from app.services.price_anomaly import detect_signal_price_anomaly
from app.services.signal_decimal_normalizer import decimal_normalization_preview_payload
from app.services.write_flow_audit import build_write_flow_audit

NotifyFn = Callable[[int, str], object]

log = logging.getLogger(__name__)

def _bingx_contract_symbol_for_display(symbol: str) -> str:
    tradfi = bingx_tradfi_exchange_symbol(symbol)
    if tradfi:
        return tradfi
    raw = str(symbol or "").strip().upper().replace("_", "-")
    if "-" in raw:
        return raw
    if raw.endswith("USDT") and len(raw) > 4:
        return f"{raw[:-4]}-USDT"
    return raw


def _is_bingx_unsupported_symbol_error(exc: Exception) -> bool:
    code = str(getattr(exc, "error_code", "") or "").strip()
    message = str(getattr(exc, "error_message", "") or exc or "").lower()
    text = str(exc or "").lower()
    if code == "109425":
        return True
    if "not exist" in message and "contract" in message:
        return True
    if "not exist" in text and "/openapi/swap/v2/quote/contracts" in text:
        return True
    if "не найдена на bingx futures" in text or "не поддерживается на bingx" in text:
        return True
    return False


def _unsupported_symbol_result_payload(
    signal: Signal,
    exc: Exception,
    *,
    exchange: str,
    trade_group_id: int | None = None,
) -> dict[str, Any]:
    raw_symbol = str(signal.symbol or "").upper()
    normalized_symbol = _bingx_contract_symbol_for_display(raw_symbol)
    return mandatory_trade_warning_payload(
        "unsupported_symbol",
        {
            "exchange": exchange,
            "trade_group_id": trade_group_id,
            "symbol_unavailable": True,
            "error_kind": "unsupported_symbol_on_bingx",
            "user_notification_kind": "unsupported_symbol_on_bingx",
            "raw_symbol": raw_symbol,
            "normalized_symbol": normalized_symbol,
            "exchange_error_code": str(getattr(exc, "error_code", "") or ""),
            "exchange_error_message": str(
                getattr(exc, "error_message", "") or exc or ""
            )[:500],
            "exchange_endpoint": "/openApi/swap/v2/quote/contracts",
        },
    )


def _unsupported_symbol_reason(signal: Signal) -> str:
    return f"Пара {str(signal.symbol or '').upper()} не поддерживается на BingX Futures"


def _exact_bingx_api_permission_error(
    exc: BaseException,
) -> BaseException | None:
    """Return the exact exception carrying BingX business code 100004.

    Matching remains fail-closed and never parses message text.  A bounded
    explicit ``__cause__`` walk is allowed so a deliberate contextual wrapper
    cannot erase a deterministic exchange rejection.  Implicit ``__context__``
    is intentionally ignored: an unrelated exception raised while handling a
    permission error must not create quarantine.  Normal BingX paths now
    re-raise the original rejection.
    """

    current: BaseException | None = exc
    seen: set[int] = set()
    accepted_types = (BingxExchangeRejected, BingxOrderCancelRejected)
    for _ in range(8):
        if current is None or id(current) in seen:
            break
        seen.add(id(current))
        # Do not duck-type on ``error_code`` alone.  Other exchanges can reuse
        # the same numeric code, and arbitrary wrappers may expose an
        # ``error_code`` attribute.  Only structured BingX rejections are
        # eligible to create a durable BingX quarantine marker.
        if isinstance(current, accepted_types):
            code = str(getattr(current, "error_code", "") or "").strip()
            if code == "100004":
                return current
        cause = getattr(current, "__cause__", None)
        current = cause if isinstance(cause, BaseException) else None
    return None


def _is_bingx_api_permission_error(exc: BaseException) -> bool:
    """Match only deterministic BingX account-permission rejection code 100004."""

    return _exact_bingx_api_permission_error(exc) is not None


def _api_quarantine_payload(
    marker: dict[str, Any] | None,
    *,
    exchange: str,
    trade_group_id: int | None,
) -> dict[str, Any]:
    row = dict(marker or {})
    try:
        hit_count = max(1, int(row.get("hit_count") or 1))
    except (TypeError, ValueError, OverflowError):
        hit_count = 1
    return mandatory_trade_warning_payload(
        "api_permission_quarantine",
        {
            "exchange": exchange,
            "trade_group_id": trade_group_id,
            "api_permission_quarantine": True,
            "api_quarantine_active": True,
            "api_quarantine_incident_token": str(row.get("incident_token") or "")[:128],
            "api_quarantine_new": bool(row.get("newly_quarantined")),
            "api_quarantine_error_code": str(row.get("error_code") or "100004"),
            "api_quarantine_endpoint": str(row.get("endpoint") or "")[:300],
            "api_quarantine_hit_count": hit_count,
            "api_quarantine_user_notification_pending": not bool(
                row.get("user_notified_at")
            ),
            "api_quarantine_admin_notification_pending": not bool(
                row.get("admin_notified_at")
            ),
        },
    )


def _api_quarantine_reason() -> str:
    return (
        "BingX API автоматически приостановлен: биржа вернула code=100004 "
        "(нет обязательного разрешения API). Включите Read и Futures Trading, "
        "затем заново подключите ключ в боте"
    )


async def _quarantine_permission_result(
    *,
    user_id: int,
    exchange: str,
    trade_group_id: int | None,
    exc: BaseException,
    credential_fingerprint: str = "",
) -> ExecutionResult | None:
    normalized_exchange = str(exchange or "").strip().lower()
    if normalized_exchange != "bingx":
        return None
    permission_exc = _exact_bingx_api_permission_error(exc)
    if permission_exc is None:
        return None
    audit = dict(getattr(permission_exc, "response_audit", {}) or {})
    endpoint = str(audit.get("path") or "")[:300]
    try:
        marker = await db.quarantine_api_key_permission(
            int(user_id),
            exchange=exchange,
            error_code=str(
                getattr(permission_exc, "error_code", "100004") or "100004"
            ),
            error_message=str(
                getattr(permission_exc, "error_message", "") or permission_exc
            ),
            endpoint=endpoint,
            credential_fingerprint=credential_fingerprint,
        )
    except Exception as quarantine_exc:
        log.exception(
            "API_PERMISSION_QUARANTINE_PERSIST_FAILED user_id=%s exchange=%s endpoint=%s",
            int(user_id),
            exchange,
            endpoint or "-",
        )
        return ExecutionResult(
            int(user_id),
            "error",
            (
                "BingX отклонила API по правам (code=100004), но бот не смог "
                f"сохранить безопасную блокировку: {type(quarantine_exc).__name__}"
            ),
            mandatory_trade_warning_payload(
                "api_permission_quarantine_persist_failed",
                {
                    "exchange": exchange,
                    "trade_group_id": trade_group_id,
                    "api_permission_quarantine": True,
                    "api_quarantine_persist_failed": True,
                    "api_quarantine_active": False,
                    "api_quarantine_error_code": "100004",
                },
            ),
        )
    if not isinstance(marker, dict):
        log.error(
            "API_PERMISSION_QUARANTINE_INVALID_MARKER user_id=%s exchange=%s marker_type=%s",
            int(user_id),
            exchange,
            type(marker).__name__,
        )
        return ExecutionResult(
            int(user_id),
            "error",
            (
                "BingX отклонила API по правам (code=100004), но хранилище "
                "не подтвердило активную безопасную блокировку"
            ),
            mandatory_trade_warning_payload(
                "api_permission_quarantine_invalid_marker",
                {
                    "exchange": exchange,
                    "trade_group_id": trade_group_id,
                    "api_permission_quarantine": True,
                    "api_quarantine_persist_failed": True,
                    "api_quarantine_active": False,
                    "api_quarantine_invalid_marker": True,
                    "api_quarantine_error_code": "100004",
                },
            ),
        )

    if marker.get("credential_identity_missing") is True:
        log.error(
            "API_PERMISSION_QUARANTINE_IDENTITY_MISSING user_id=%s exchange=%s endpoint=%s",
            int(user_id),
            exchange,
            endpoint or "-",
        )
        return ExecutionResult(
            int(user_id),
            "error",
            (
                "BingX отклонила API по правам (code=100004), но бот не смог "
                "безопасно связать ошибку с конкретной версией API-ключа"
            ),
            mandatory_trade_warning_payload(
                "api_permission_quarantine_identity_missing",
                {
                    "exchange": exchange,
                    "trade_group_id": trade_group_id,
                    "api_permission_quarantine": True,
                    "api_quarantine_persist_failed": True,
                    "api_quarantine_active": False,
                    "api_quarantine_identity_missing": True,
                    "api_quarantine_error_code": "100004",
                },
            ),
        )

    if marker.get("stale_credential") is True:
        log.info(
            "API_PERMISSION_STALE_CREDENTIAL_IGNORED user_id=%s exchange=%s code=%s endpoint=%s",
            int(user_id),
            exchange,
            str(marker.get("error_code") or "100004"),
            str(marker.get("endpoint") or "-")[:120],
        )
        return ExecutionResult(
            int(user_id),
            "skipped",
            (
                "API-ключ был заменён во время обработки сигнала. Ошибка старого "
                "ключа не применилась к текущему ключу; сделка не открывалась"
            ),
            mandatory_trade_warning_payload(
                "api_credential_changed_during_execution",
                {
                    "exchange": exchange,
                    "trade_group_id": trade_group_id,
                    "api_quarantine_stale_credential": True,
                    "api_quarantine_error_code": "100004",
                },
            ),
        )

    marker_code = str(marker.get("error_code") or "").strip()
    try:
        marker_active = int(marker.get("active") or 0) == 1
    except (TypeError, ValueError, OverflowError):
        marker_active = False
    marker_token = str(marker.get("incident_token") or "").strip()
    if not marker_active or marker_code != "100004" or not marker_token:
        log.error(
            "API_PERMISSION_QUARANTINE_INVALID_MARKER user_id=%s exchange=%s active=%s code=%s",
            int(user_id),
            exchange,
            marker.get("active"),
            marker_code or "-",
        )
        return ExecutionResult(
            int(user_id),
            "error",
            (
                "BingX отклонила API по правам (code=100004), но хранилище "
                "не подтвердило активную безопасную блокировку"
            ),
            mandatory_trade_warning_payload(
                "api_permission_quarantine_invalid_marker",
                {
                    "exchange": exchange,
                    "trade_group_id": trade_group_id,
                    "api_permission_quarantine": True,
                    "api_quarantine_persist_failed": True,
                    "api_quarantine_active": False,
                    "api_quarantine_invalid_marker": True,
                    "api_quarantine_error_code": "100004",
                },
            ),
        )

    log.warning(
        "API_PERMISSION_QUARANTINED user_id=%s exchange=%s code=%s endpoint=%s new=%s hits=%s",
        int(user_id),
        exchange,
        str(marker.get("error_code") or "100004"),
        str(marker.get("endpoint") or "-")[:120],
        bool(marker.get("newly_quarantined")),
        int(marker.get("hit_count") or 1),
    )
    return ExecutionResult(
        int(user_id),
        "skipped",
        _api_quarantine_reason(),
        _api_quarantine_payload(
            marker, exchange=exchange, trade_group_id=trade_group_id
        ),
    )


def _attach_write_flow_audit(payload: dict[str, Any], *, status: str, stage: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return payload
    try:
        payload["write_flow_audit_v1"] = build_write_flow_audit(payload, status=status, stage=stage)
    except Exception as exc:
        payload["write_flow_audit_v1"] = {
            "version": 1,
            "status": status,
            "stage": stage,
            "audit_error": f"{type(exc).__name__}: {exc}"[:500],
        }
    return payload


_OPENING_REGISTRY: set[tuple[int, str]] = set()
_OPENING_REGISTRY_LOCK = _threading.Lock()

# Serialize trade creation per user. Different users still execute in parallel,
# but two simultaneous signals for one account cannot both pass the same risk
# snapshot and exceed slots/portfolio limits.
_USER_TRADE_LOCKS: OrderedDict[int, asyncio.Lock] = OrderedDict()
_MAX_USER_TRADE_LOCKS = 5000

_BACKGROUND_TP_TASKS: set[asyncio.Task[None]] = set()
_BACKGROUND_TP_EXECUTION_IDS: set[int] = set()
_BACKGROUND_TP_LOCK = _threading.Lock()
_BACKGROUND_TP_STATS: dict[str, int] = {
    "scheduled": 0,
    "recovered": 0,
    "active": 0,
    "completed": 0,
    "partial_error": 0,
    "error": 0,
    "cancelled": 0,
    "skipped": 0,
}


def _user_trade_lock(user_id: int) -> asyncio.Lock:
    uid = int(user_id)
    lock = _USER_TRADE_LOCKS.get(uid)
    if lock is not None:
        _USER_TRADE_LOCKS.move_to_end(uid)
        return lock
    lock = asyncio.Lock()
    _USER_TRADE_LOCKS[uid] = lock
    if len(_USER_TRADE_LOCKS) > _MAX_USER_TRADE_LOCKS:
        for old_uid, old_lock in list(_USER_TRADE_LOCKS.items()):
            if old_uid != uid and not old_lock.locked():
                _USER_TRADE_LOCKS.pop(old_uid, None)
                break
    return lock


def register_opening(user_id: int, symbol: str) -> None:
    with _OPENING_REGISTRY_LOCK:
        _OPENING_REGISTRY.add((int(user_id), str(symbol).upper()))


def unregister_opening(user_id: int, symbol: str) -> None:
    with _OPENING_REGISTRY_LOCK:
        _OPENING_REGISTRY.discard((int(user_id), str(symbol).upper()))


def _exchange_order_id_from_payload(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    def _pick(row: Any) -> str:
        if not isinstance(row, dict):
            return ""
        return clean_exchange_id(
            row.get("_confirmed_order_id")
            or row.get("orderId")
            or row.get("orderID")
            or row.get("order_id")
            or row.get("stopPlanOrderId")
            or row.get("id")
        )
    direct = _pick(payload)
    if direct:
        return direct
    data = payload.get("data")
    if isinstance(data, dict):
        direct = _pick(data)
        if direct:
            return direct
        nested = data.get("order")
        direct = _pick(nested)
        if direct:
            return direct
    nested = payload.get("order")
    direct = _pick(nested)
    if direct:
        return direct
    return ""


def is_symbol_opening(user_id: int, symbol: str) -> bool:
    """True while signal_executor is opening a position for this user+symbol."""
    with _OPENING_REGISTRY_LOCK:
        return (int(user_id), str(symbol).upper()) in _OPENING_REGISTRY


def choose_distribution(mode: TpMode, count: int, manual: list[float]) -> list[float]:
    if mode == TpMode.MANUAL:
        return manual_distribution(manual, count)
    if mode == TpMode.EQUAL:
        return equal_distribution(count)
    if mode == TpMode.BELL:
        return bell_distribution(count)
    if mode == TpMode.ACCELERATION:
        return acceleration_distribution(count)
    if mode == TpMode.EARLY_FIXATION:
        return early_fixation_distribution(count)
    return smart_scale_out_distribution(count)


def _market_fill_risk_snapshot(
    *,
    sizing,
    signal: Signal,
    actual_entry: float,
    taker_fee_rate: float,
) -> dict[str, float]:
    """Build the durable, post-fill MARKET risk audit snapshot."""
    actual = float(actual_entry)
    fee_rate = max(0.0, float(taker_fee_rate))
    effective_stop_distance = (
        abs(actual - float(signal.stop)) + (actual + float(signal.stop)) * fee_rate
    )
    realized_risk_usdt = float(sizing.qty) * effective_stop_distance
    realized_risk_percent = (
        realized_risk_usdt / float(sizing.balance_usdt) * 100.0
        if float(sizing.balance_usdt) > 0
        else 0.0
    )
    signal_entry = float(signal.entry)
    slippage_percent = (
        abs(actual - signal_entry) / signal_entry * 100.0 if signal_entry > 0 else 0.0
    )
    return {
        "actual_entry": actual,
        "signal_entry": signal_entry,
        "stop": float(signal.stop),
        "qty": float(sizing.qty),
        "target_risk_usdt": float(getattr(sizing, "target_risk_usdt", 0.0) or 0.0),
        "pretrade_executable_risk_usdt": float(sizing.risk_usdt),
        "realized_risk_usdt": round(realized_risk_usdt, 8),
        "realized_risk_percent": round(realized_risk_percent, 4),
        "market_slippage_percent": round(slippage_percent, 6),
        "taker_fee_rate": fee_rate,
    }


def _statistics_risk_snapshot(
    *,
    sizing: Any,
    signal: Signal,
    entry_price: float,
    taker_fee_rate: float,
    source: str,
) -> dict[str, Any]:
    """Build immutable, finite risk evidence for statistics.

    This mirrors the already-approved sizing result; it does not participate in
    order admission or change any trading formula.
    """

    equity = _f(getattr(sizing, "balance_usdt", 0.0), 0.0)
    qty = _f(getattr(sizing, "qty", 0.0), 0.0)
    entry = _f(entry_price, 0.0)
    stop = _f(signal.stop, 0.0)
    fee_rate = max(0.0, _f(taker_fee_rate, 0.0))
    stop_distance = abs(entry - stop)
    price_risk = qty * stop_distance
    fee_risk = qty * (entry + stop) * fee_rate
    expected_loss = price_risk + fee_risk
    risk_percent = price_risk / equity * 100.0 if equity > 0 else 0.0
    complete = equity > 0 and qty > 0 and entry > 0 and stop > 0 and stop_distance > 0
    return {
        "equity_snapshot_usd": round(equity, 8) if equity > 0 else None,
        "planned_risk_usd": round(
            _f(getattr(sizing, "target_risk_usdt", 0.0), 0.0), 8
        ),
        "initial_price_risk_usd": round(price_risk, 8),
        "initial_risk_percent_of_equity": round(risk_percent, 8),
        "estimated_fee_risk_usd": round(fee_risk, 8),
        "expected_loss_at_stop_usd": round(expected_loss, 8),
        "planned_entry_qty": round(qty, 12),
        "stop_distance": round(stop_distance, 12),
        "risk_snapshot_at": datetime.now(timezone.utc),
        "risk_snapshot_source": str(source or "unknown")[:80],
        "risk_snapshot_status": "complete" if complete else "partial",
        "risk_snapshot_reason": None if complete else "missing_finite_sizing_component",
    }


def _configured_tp_distribution_source(
    *, signal_percentages: list[Any], tp_mode: Any
) -> str:
    if signal_percentages:
        return "source_signal"
    mode = str(getattr(tp_mode, "value", tp_mode) or "unknown").strip().lower()
    return f"user_settings:{mode}"[:80]


def _balance_preflight_values(details: dict[str, Any] | None) -> tuple[float, float]:
    """Return ``(equity, freely available margin)`` from BingX balance data.

    Equity is the correct capital base for percentage risk sizing.  Admission
    of a new order must instead use free/available balance, because existing
    positions and pending orders may already consume most of the account equity.
    The available value deliberately fails closed to zero when the API omits it.
    """

    payload = details or {}
    if "total_equity" in payload:
        # Explicit zero equity is a real risk state (for example a wallet whose
        # unrealised loss consumed its capital). Never replace it with wallet
        # balance merely because zero is falsey.
        equity = float(payload.get("total_equity") or 0.0)
    else:
        equity = float(payload.get("total_wallet_balance") or 0.0)
    if not math.isfinite(equity) or equity < 0:
        equity = 0.0
    if "available_balance" in payload:
        # An explicit zero is meaningful: all account equity may already be
        # committed to positions or pending orders.  Never replace it with a
        # truthy legacy ``USDT`` balance.
        available = float(payload.get("available_balance") or 0.0)
    else:
        available = float(payload.get("USDT") or 0.0)
    if not math.isfinite(available) or available < 0:
        available = 0.0
    return equity, available


def _floor_to_step(value: float, step: float) -> float:
    if step <= 0:
        return float(value)
    d = Decimal(str(value))
    q = Decimal(str(step))
    return float((d / q).to_integral_value(rounding=ROUND_DOWN) * q)


def _ceil_to_step(value: float, step: float) -> float:
    if step <= 0:
        return float(value)
    d = Decimal(str(value))
    q = Decimal(str(step))
    return float((d / q).to_integral_value(rounding=ROUND_CEILING) * q)


def _safe_isolated_leverage(
    signal: Signal,
    exchange_max_leverage: int,
    buffer_percent: float,
) -> int:
    """Conservatively cap isolated leverage so liquidation stays beyond STOP.

    Exact liquidation depends on BingX maintenance-margin tiers and fees, which
    are not available before opening. The cap therefore uses the inverse-leverage
    distance and adds a configurable safety buffer. This is intentionally more
    conservative than blindly selecting the contract maximum.
    """
    entry = Decimal(str(signal.entry))
    stop = Decimal(str(signal.stop))
    if entry <= 0:
        raise ValueError("entry должен быть положительным для isolated leverage")
    stop_fraction = abs(entry - stop) / entry
    buffer_fraction = Decimal(str(max(0.5, float(buffer_percent)))) / Decimal("100")
    required_distance = stop_fraction + buffer_fraction
    if required_distance <= 0:
        return 1
    safe = int(
        (Decimal("1") / required_distance).to_integral_value(rounding=ROUND_DOWN)
    )
    return max(1, min(int(exchange_max_leverage or 1), safe))




async def _resolve_exchange_max_leverage(adapter: Any, info: Any, signal: Signal) -> tuple[int, str]:
    """Resolve the side-specific exchange maximum without silent 1x fallback.

    BingX public contract metadata can omit max leverage.  If the adapter
    exposes a private max-leverage reader, use it as the authoritative value
    before risk sizing and before the entry write.
    """

    fetcher = getattr(adapter, "fetch_max_leverage", None)
    if callable(fetcher):
        max_lev = int(await fetcher(info.symbol, signal.side.value))
        if max_lev <= 0:
            raise ValueError("exchange returned empty max leverage")
        return max_lev, "private_trade_leverage"

    max_lev = int(getattr(info, "max_leverage", 0) or 0)
    if max_lev <= 0:
        raise ValueError("instrument metadata does not contain max leverage")
    return max_lev, "instrument_info"


def _normalize_signal_prices_to_tick(
    signal: Signal, price_tick: float
) -> tuple[Signal, dict[str, Any]]:
    """Convert entry/stop/TP prices to valid exchange price ticks.

    Direction is conservative and side-aware so the normalized plan does not
    increase real risk beyond the configured percent after sizing is recalculated:
    LIMIT LONG: entry ceil, stop floor, TP floor.
    LIMIT SHORT: entry floor, stop ceil, TP ceil.

    MARKET signals use a fetched current price as risk entry. That fetched price is
    not sent as a limit price, so we do NOT tick-normalize entry for MARKET; only
    STOP/TP trigger prices are normalized for the selected exchange.

    Two distinct signal targets can collapse to the same exchange tick. BingX treats
    same-price position TP updates asynchronously, so submitting them as separate
    targets can create a quantity conflict. Such targets are merged here before any
    immutable policy or order is created. Complete signal percentages are merged as
    well; incomplete percentages are discarded and the configured TP scheme is used.
    """
    if not price_tick or price_tick <= 0:
        return signal, {}
    side = str(signal.side.value).lower()
    is_market = str(getattr(signal, "order_type", "LIMIT")).upper() == "MARKET"
    if side == "long":
        entry = (
            float(signal.entry)
            if is_market
            else _ceil_to_step(signal.entry, price_tick)
        )
        stop = _floor_to_step(signal.stop, price_tick)
        normalized_targets = [_floor_to_step(tp, price_tick) for tp in signal.targets]
    else:
        entry = (
            float(signal.entry)
            if is_market
            else _floor_to_step(signal.entry, price_tick)
        )
        stop = _ceil_to_step(signal.stop, price_tick)
        normalized_targets = [_ceil_to_step(tp, price_tick) for tp in signal.targets]

    complete_signal_pcts = (
        len(signal.target_percents) == len(signal.targets)
        and all(float(value) > 0 for value in signal.target_percents)
        and abs(sum(float(value) for value in signal.target_percents) - 100.0) <= 0.01
    )
    unique_targets: list[float] = []
    unique_pcts: list[float] = []
    collapsed_groups: list[dict[str, Any]] = []
    tolerance = max(float(price_tick) * 1e-9, 1e-12)
    for index, price in enumerate(normalized_targets):
        existing = next(
            (
                i
                for i, current in enumerate(unique_targets)
                if abs(float(current) - float(price)) <= tolerance
            ),
            None,
        )
        pct = float(signal.target_percents[index]) if complete_signal_pcts else 0.0
        if existing is None:
            unique_targets.append(float(price))
            if complete_signal_pcts:
                unique_pcts.append(pct)
            collapsed_groups.append(
                {
                    "price": float(price),
                    "original_tp_indices": [index + 1],
                }
            )
        else:
            collapsed_groups[existing]["original_tp_indices"].append(index + 1)
            if complete_signal_pcts:
                unique_pcts[existing] += pct

    collapsed = len(unique_targets) != len(normalized_targets)
    if not complete_signal_pcts:
        unique_pcts = []

    changed = (
        (abs(entry - signal.entry) > 1e-15)
        or (abs(stop - signal.stop) > 1e-15)
        or any(abs(a - b) > 1e-15 for a, b in zip(normalized_targets, signal.targets))
        or collapsed
    )
    if not changed:
        return signal, {}
    normalized = replace(
        signal,
        entry=float(entry),
        stop=float(stop),
        targets=unique_targets,
        target_percents=unique_pcts,
    )
    normalized.validate()
    return normalized, {
        "price_tick": price_tick,
        "original_entry": signal.entry,
        "original_stop": signal.stop,
        "original_targets": list(signal.targets),
        "entry": normalized.entry,
        "stop": normalized.stop,
        "targets": list(normalized.targets),
        "market_entry_not_tick_normalized": bool(is_market),
        "collapsed_same_tick_targets": [
            group for group in collapsed_groups if len(group["original_tp_indices"]) > 1
        ],
    }


def _signal_distribution(signal: Signal, selected_targets: list[float]) -> list[float]:
    """Use explicit TP percentages from signal only when they are complete and safe.

    We intentionally require the selected TP list to match the full signal target
    list. If the user limits TP count, configured TP scheme remains the source of
    truth because the signal percentages no longer sum to 100 for the selected
    subset.
    """
    vals = [float(x) for x in (getattr(signal, "target_percents", []) or [])]
    if not vals:
        return []
    if len(vals) != len(signal.targets) or len(selected_targets) != len(signal.targets):
        return []
    if any(v <= 0 for v in vals):
        return []
    if abs(sum(vals) - 100.0) > 0.01:
        return []
    return vals


def _stable_client_id(
    prefix: str, sig_hash: str, user_id: int, suffix: str = ""
) -> str:
    raw = hashlib.sha256(f"{sig_hash}:{user_id}:{suffix}".encode()).hexdigest()[:20]
    return f"{prefix}-{raw}"[:36]


def _risk_limit_error(user_settings, state: dict, new_risk: float) -> str | None:
    active_count = int(state.get("active_count") or 0)
    active_risk = float(state.get("active_risk_percent") or 0.0)
    daily_risk = float(state.get("daily_risk_percent") or 0.0)
    max_open = int(user_settings.max_open_trades or 0)
    max_portfolio = float(user_settings.max_portfolio_risk_percent or 0.0)
    max_daily = float(user_settings.daily_risk_limit_percent or 0.0)

    if max_open > 0 and active_count >= max_open:
        return (
            f"лимит открытых сделок: {active_count}/{max_open}. Новая сделка запрещена"
        )
    if max_portfolio > 0 and active_risk + new_risk > max_portfolio + 1e-9:
        return f"портфельный риск будет превышен: {active_risk:.2f}% + {new_risk:.2f}% > {max_portfolio:.2f}%"
    if max_daily > 0 and daily_risk + new_risk > max_daily + 1e-9:
        return f"дневной риск будет превышен: {daily_risk:.2f}% + {new_risk:.2f}% > {max_daily:.2f}%"
    return None


def _f(value: Any, default: float = 0.0) -> float:
    """Parse a finite non-negative exchange scalar without repairing corruption."""
    try:
        if value in (None, "") or isinstance(value, bool):
            return default
        parsed = float(value)
        return parsed if math.isfinite(parsed) and parsed >= 0 else default
    except (TypeError, ValueError, OverflowError):
        return default


def _position_size(pos: dict[str, Any]) -> float:
    for key in ("size", "availableSize", "positionAmt", "qty", "total"):
        val = _f(pos.get(key), 0.0)
        if val > 0:
            return val
    return 0.0


def _positive_price(value: Any) -> float:
    try:
        if value in (None, "") or isinstance(value, bool):
            return 0.0
        val = float(value)
        return val if math.isfinite(val) and val > 0 else 0.0
    except (TypeError, ValueError, OverflowError):
        return 0.0


def _position_entry_price(pos: dict[str, Any]) -> float:
    for key in (
        "avgFillPrice",
        "entryPrice",
        "avgEntryPrice",
        "positionAvgPrice",
        "avgEntry",
        "openPrice",
        "avgPrice",
    ):
        val = _positive_price(pos.get(key))
        if val > 0:
            return val
    return 0.0


def _order_entry_price(order: Any) -> float:
    if not isinstance(order, dict):
        return 0.0
    # Check top-level and common nested payloads first.
    payloads = [order]
    for key in ("result", "data", "order", "info"):
        if isinstance(order.get(key), dict):
            payloads.append(order[key])
    for payload in payloads:
        for key in (
            "avgFillPrice",
            "filledAvgPrice",
            "average",
            "entryPrice",
            "execPrice",
            "price",
            "avgPrice",
        ):
            val = _positive_price(payload.get(key))
            if val > 0:
                return val
    return 0.0


async def _try_actual_entry(
    adapter: Any,
    symbol: str,
    side: str,
    fallback: float,
    entry_order: Any | None = None,
) -> float:
    from_order = _order_entry_price(entry_order)
    if from_order > 0:
        return from_order

    # Exchanges may need a moment after MARKET fill before position avgPrice appears.
    for attempt in range(3):
        try:
            positions = await adapter.fetch_open_positions(symbol, side.upper())
            if positions:
                val = _position_entry_price(positions[0])
                if val > 0:
                    return val
        except Exception:
            pass
        if attempt < 2:
            await asyncio.sleep(0.5)
    return fallback



def _normalize_trade_symbol(value: Any) -> str:
    tradfi = canonical_bingx_tradfi_symbol(value)
    if tradfi:
        return tradfi
    return str(value or "").upper().replace("/", "").replace("-", "").replace("_", "")


_CONDITIONAL_ENTRY_GUARD_MARKERS = (
    "STOP",
    "TAKE_PROFIT",
    "TRIGGER",
    "TAKE_STOP",
    "TRAILING",
)


def _is_live_regular_entry_order(order: Any, symbol: str) -> bool:
    """Return True only for a live regular entry order for this exact symbol.

    This guard protects new signals from old pending LIMIT entries, but it must
    not block XLM/ZEC because BingX returned another symbol's order, or because
    a protective STOP/TP row was returned without reduceOnly.  Defensive local
    filtering is intentional: exchange reads are treated as untrusted until
    symbol and order type are checked by the bot.
    """

    if not isinstance(order, dict):
        return False
    if _normalize_trade_symbol(order.get("symbol")) != _normalize_trade_symbol(symbol):
        return False

    typ = str(order.get("type") or "").upper()
    if typ and any(marker in typ for marker in _CONDITIONAL_ENTRY_GUARD_MARKERS):
        return False
    if bool(order.get("reduceOnly")):
        return False

    state_raw = order.get("state")
    try:
        state = int(float(state_raw)) if state_raw not in (None, "") else 0
    except Exception:
        state = 0
    status = str(order.get("status") or order.get("state_name") or "").upper()
    if state in {3, 4, 5} or status in {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED", "FAILED"}:
        return False

    return True

def _entry_order_guard_diagnostics(orders: Any, symbol: str) -> dict[str, Any]:
    """Explain exactly why live openOrders did or did not block a signal."""

    rows = [o for o in (orders or []) if isinstance(o, dict)]
    target = _normalize_trade_symbol(symbol)
    audit: dict[str, Any] = {
        "symbol": target,
        "rows_total": len(rows),
        "same_symbol_rows": 0,
        "regular_entry_rows": 0,
        "ignored_cross_symbol": 0,
        "ignored_protective": 0,
        "ignored_reduce_only": 0,
        "ignored_terminal": 0,
        "blocking_rows": [],
    }
    for order in rows:
        row_symbol = _normalize_trade_symbol(order.get("symbol"))
        if row_symbol != target:
            audit["ignored_cross_symbol"] += 1
            continue
        audit["same_symbol_rows"] += 1
        typ = str(order.get("type") or "").upper()
        status = str(order.get("status") or order.get("state_name") or "").upper()
        state_raw = order.get("state")
        try:
            state = int(float(state_raw)) if state_raw not in (None, "") else 0
        except Exception:
            state = 0
        if typ and any(marker in typ for marker in _CONDITIONAL_ENTRY_GUARD_MARKERS):
            audit["ignored_protective"] += 1
            continue
        if bool(order.get("reduceOnly")):
            audit["ignored_reduce_only"] += 1
            continue
        if state in {3, 4, 5} or status in {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED", "FAILED"}:
            audit["ignored_terminal"] += 1
            continue
        audit["regular_entry_rows"] += 1
        audit["blocking_rows"].append({
            "symbol": row_symbol,
            "orderId": clean_exchange_id(order.get("orderId") or order.get("orderID") or order.get("id")),
            "clientOrderID": clean_exchange_id(order.get("clientOrderID") or order.get("clientOrderId")),
            "type": typ,
            "side": str(order.get("side") or "").upper(),
            "status": status or str(state_raw or ""),
            "reduceOnly": bool(order.get("reduceOnly")),
        })
    return audit


def _extract_position_id_from_positions(positions: list[dict[str, Any]]) -> str:
    for pos in positions or []:
        pid = clean_exchange_id(pos.get("positionId"))
        if pid:
            return pid
    return ""


def _protective_stop_order_id(row: dict[str, Any]) -> str:
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    for value in (
        row.get("stopPlanOrderId"),
        row.get("stopOrderId"),
        row.get("orderId"),
        row.get("orderID"),
        row.get("id"),
        raw.get("stopPlanOrderId"),
        raw.get("stopOrderId"),
        raw.get("orderId"),
        raw.get("orderID"),
        raw.get("id"),
    ):
        cleaned = clean_exchange_id(value)
        if cleaned:
            return cleaned
    return ""


def _protective_stop_price(row: dict[str, Any]) -> float:
    return _positive_price(
        row.get("stopLossPrice")
        or row.get("triggerPrice")
        or row.get("stopPrice")
        or row.get("price")
    )


def _protective_qty(row: dict[str, Any]) -> float:
    return _f(row.get("qty") or row.get("quantity") or row.get("origQty") or row.get("vol") or row.get("size"), 0.0)


def _is_stop_like_row(row: dict[str, Any]) -> bool:
    typ = str(row.get("type") or "").upper()
    if "TAKE_PROFIT" in typ or "TAKE_STOP" in typ:
        return False
    if "STOP" in typ or "TRIGGER" in typ or "TRAILING" in typ:
        return _protective_stop_price(row) > 0
    return _positive_price(row.get("stopLossPrice")) > 0


async def _rollback_unprotected_market_position(
    adapter: Any,
    *,
    symbol: str,
    side: str,
    stop: float,
    requested_qty: float,
    sig_hash: str,
    user_id: int,
    trade_group_id: int | None,
    position_id: str = "",
) -> dict[str, Any]:
    """Emergency-close only the quantity opened by the current MARKET attempt.

    The pre-entry guard requires no same-symbol position before MARKET entry, so
    the safe rollback quantity is the positive live position observed after the
    failed STOP proof, capped by the submitted entry qty.  This mirrors the MEXC
    safety pattern: an unprotected MARKET position must not be left for TP flow.
    """

    side_l = str(side or "").lower()
    audit: dict[str, Any] = {
        "attempted": True,
        "symbol": _normalize_trade_symbol(symbol),
        "side": side_l,
        "requested_qty": float(requested_qty or 0.0),
        "positionId": clean_exchange_id(position_id),
        "stop": float(stop or 0.0),
    }
    positions: list[dict[str, Any]] = []
    live_qty = 0.0
    for attempt in range(5):
        try:
            positions = list(await adapter.fetch_open_positions(symbol, side_l.upper()) or [])
            live_qty = sum(_position_size(p) for p in positions)
            if live_qty > 0:
                break
        except Exception as exc:
            audit["position_read_error"] = f"{type(exc).__name__}: {exc}"[:300]
        if attempt < 4:
            await asyncio.sleep(0.25 * (attempt + 1))

    audit["observed_live_qty"] = live_qty
    qty_cap = float(requested_qty or 0.0)
    close_qty = min(live_qty, qty_cap) if qty_cap > 0 else live_qty
    audit["close_qty"] = close_qty
    if close_qty <= 0:
        audit["confirmed"] = False
        audit["status"] = "no_live_position_to_close"
        return audit

    close_cid = _stable_client_id(
        "avc-rb",
        sig_hash,
        int(user_id),
        f"market-stop-rollback:{trade_group_id or int(time.time() * 1000)}",
    )
    try:
        close_result = await adapter.emergency_close_market_confirmed(
            symbol=symbol,
            side=side_l,
            qty=close_qty,
            client_id=close_cid,
            position_id=position_id or None,
        )
        audit["close_result"] = close_result
        audit["confirmed"] = bool((close_result or {}).get("confirmed"))
        audit["status"] = "rollback_confirmed" if audit["confirmed"] else "rollback_unconfirmed"
    except Exception as exc:
        audit["confirmed"] = False
        audit["status"] = "rollback_error"
        audit["close_error"] = f"{type(exc).__name__}: {exc}"[:500]

    # If the delayed STOP later appears and rollback closed the position, cancel
    # only exact rows matching the attempted STOP.  This avoids leaving an orphan
    # close-trigger order after the position is gone, without any broad cancel.
    if audit.get("confirmed"):
        try:
            rows = list(await adapter.fetch_open_algo_orders(symbol) or [])
            stop_ids: list[str] = []
            info = await adapter.instrument_info(symbol)
            price_tol = max(_f(getattr(info, "price_tick", 0.0), 0.0) * 0.51, abs(float(stop)) * 1e-9, 1e-12)
            for row in rows:
                if not isinstance(row, dict):
                    continue
                if _normalize_trade_symbol(row.get("symbol")) != _normalize_trade_symbol(symbol):
                    continue
                row_side = str(row.get("side") or row.get("positionSide") or "").lower()
                if row_side and row_side != side_l:
                    continue
                if not _is_stop_like_row(row):
                    continue
                if abs(_protective_stop_price(row) - float(stop)) > price_tol:
                    continue
                sid = _protective_stop_order_id(row)
                if sid:
                    stop_ids.append(sid)
            if stop_ids:
                audit["orphan_stop_cancel_ids"] = sorted(set(stop_ids))
                audit["orphan_stop_cancel"] = await adapter.cancel_conditional_orders_exact(
                    sorted(set(stop_ids)), symbol=symbol
                )
        except Exception as exc:
            audit["orphan_stop_cancel_error"] = f"{type(exc).__name__}: {exc}"[:500]
    return audit


async def _ensure_market_stop_before_tp(
    adapter: Any,
    *,
    symbol: str,
    side: str,
    stop: float,
    qty: float,
    entry_order: dict[str, Any] | None,
    sig_hash: str,
    user_id: int,
    trade_group_id: int | None,
    result_payload: dict[str, Any],
) -> dict[str, Any]:
    """Verify or create a MARKET protective STOP before TP placement.

    BingX accepts MARKET orders with an attached stopLoss JSON, but if the
    attached STOP is not visible after the fill (or the response omitted exact
    STOP identity) the bot must not proceed to partial TP writes.  This helper
    performs only read-only detection first; it creates one explicit fallback
    STOP_MARKET only when no matching protective STOP is visible.
    """

    side_l = str(side or "").lower()
    target_side = side_l
    info = await adapter.instrument_info(symbol)
    price_tol = max(_f(getattr(info, "price_tick", 0.0), 0.0) * 0.51, abs(float(stop)) * 1e-9, 1e-12)
    qty_step = _f(getattr(info, "qty_step", 0.0), 0.0)
    qty_tol = max(qty_step * 0.51, 1e-12)
    wanted_symbol = _normalize_trade_symbol(symbol)
    wanted_stop = float(stop)
    requested_qty = max(0.0, float(qty or 0.0))

    positions: list[dict[str, Any]] = []
    live_qty = 0.0
    for attempt in range(4):
        try:
            positions = list(await adapter.fetch_open_positions(symbol, side_l.upper()) or [])
            live_qty = sum(_position_size(p) for p in positions)
            if live_qty > 0:
                break
        except Exception as exc:
            result_payload["market_stop_position_read_error_v1"] = f"{type(exc).__name__}: {exc}"[:300]
        if attempt < 3:
            await asyncio.sleep(0.25 * (attempt + 1))

    position_id = _extract_position_id_from_positions(positions)
    min_qty = min(requested_qty, live_qty) if live_qty > 0 else requested_qty
    min_qty = max(0.0, min_qty)

    async def _read_matching_stops() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        rows = list(await adapter.fetch_open_algo_orders(symbol) or [])
        matched: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            if _normalize_trade_symbol(row.get("symbol")) != wanted_symbol:
                continue
            row_side = str(row.get("side") or row.get("positionSide") or "").lower()
            if row_side and row_side != target_side:
                continue
            row_pos = clean_exchange_id(row.get("positionId"))
            if row_pos and position_id and row_pos != position_id:
                continue
            if not _is_stop_like_row(row):
                continue
            row_stop = _protective_stop_price(row)
            if row_stop <= 0 or abs(row_stop - wanted_stop) > price_tol:
                continue
            row_qty = _protective_qty(row)
            if min_qty > 0 and row_qty > 0 and row_qty + qty_tol < min_qty:
                continue
            matched.append(row)
        return rows, matched

    # BingX may expose the attached MARKET stop with a short delay.  A single
    # immediate openOrders miss followed by an explicit fallback write can
    # create two identical protective STOPs.  Give the attached order a bounded
    # visibility window before writing anything new.
    open_algo: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    visibility_attempts = 0
    for visibility_attempt in range(6):
        visibility_attempts = visibility_attempt + 1
        open_algo, candidates = await _read_matching_stops()
        if candidates:
            break
        if visibility_attempt < 5:
            await asyncio.sleep(min(0.75, 0.15 * (visibility_attempt + 1)))

    candidate_ids = sorted({_protective_stop_order_id(row) for row in candidates if _protective_stop_order_id(row)})
    audit = {
        "symbol": wanted_symbol,
        "side": target_side,
        "positionId": position_id,
        "requested_qty": requested_qty,
        "live_qty": live_qty,
        "minimum_protected_qty": min_qty,
        "stop": wanted_stop,
        "candidate_ids": candidate_ids,
        "visibility_attempts": visibility_attempts,
    }
    if candidates:
        audit["status"] = "existing_stop_visible"
        audit["matched_count"] = len(candidates)
        if len(candidates) > 1:
            # Do not continue into TP placement with an ambiguous duplicate
            # protective topology.  No order is cancelled here because none of
            # the duplicate ids is yet proven to be the fallback created by
            # this exact function call.
            audit["status"] = "duplicate_existing_stops_visible"
            audit["manual_cleanup_required"] = True
            result_payload["market_post_fill_stop_v1"] = audit
            raise BingxMarketProtectionError(
                "MARKET position is protected by multiple matching STOP orders; "
                "TP placement was blocked until exact duplicate cleanup",
                emergency_close_confirmed=False,
                entry_order=entry_order,
                protection_order=audit,
                position_id=position_id,
                opened_qty=live_qty,
            )
        if len(candidate_ids) == 1:
            # Durable exact identity for the initial BingX protective STOP.
            # BE replacement can then accept this old STOP even if BingX later
            # omits positionId in openOrders.
            audit["stop_order_id"] = candidate_ids[0]
            audit["stopPlanOrderId"] = candidate_ids[0]
        elif candidate_ids:
            audit["candidate_id_count"] = len(candidate_ids)
        result_payload["market_post_fill_stop_v1"] = audit
        return audit

    if live_qty <= 0:
        rollback_audit = await _rollback_unprotected_market_position(
            adapter,
            symbol=symbol,
            side=side_l,
            stop=wanted_stop,
            requested_qty=requested_qty,
            sig_hash=sig_hash,
            user_id=user_id,
            trade_group_id=trade_group_id,
            position_id=position_id,
        )
        audit["status"] = "position_not_visible_no_stop_created"
        audit["emergency_rollback_v1"] = rollback_audit
        result_payload["market_post_fill_stop_v1"] = audit
        raise BingxMarketProtectionError(
            "MARKET entry submitted but BingX position/STOP was not visible after fill; "
            "TP placement blocked and emergency rollback could not be proven automatically",
            emergency_close_confirmed=bool(rollback_audit.get("confirmed")),
            entry_order=entry_order,
            protection_order=audit,
            emergency_close_order=rollback_audit,
            position_id=position_id,
            opened_qty=float(rollback_audit.get("close_qty") or 0.0),
        )

    fallback_qty = min_qty if min_qty > 0 else live_qty
    stop_cid = _stable_client_id(
        "avc-mstp",
        sig_hash,
        int(user_id),
        f"market-stop:{trade_group_id or int(time.time() * 1000)}",
    )
    try:
        fallback_stop = await adapter.set_position_stop_loss(
            symbol=symbol,
            side=side_l,
            qty=fallback_qty,
            stop=wanted_stop,
            client_id=stop_cid,
            position_id=position_id or None,
        )
    except Exception as stop_exc:
        rollback_audit = await _rollback_unprotected_market_position(
            adapter,
            symbol=symbol,
            side=side_l,
            stop=wanted_stop,
            requested_qty=requested_qty,
            sig_hash=sig_hash,
            user_id=user_id,
            trade_group_id=trade_group_id,
            position_id=position_id,
        )
        audit.update(
            {
                "status": "fallback_stop_failed_rollback_attempted",
                "fallback_qty": fallback_qty,
                "fallback_error": f"{type(stop_exc).__name__}: {stop_exc}",
                "emergency_rollback_v1": rollback_audit,
            }
        )
        result_payload["market_post_fill_stop_v1"] = audit
        raise BingxMarketProtectionError(
            "MARKET entry opened a live position but protective STOP was not confirmed/created; "
            "TP placement blocked and MEXC-style emergency rollback was attempted",
            emergency_close_confirmed=bool(rollback_audit.get("confirmed")),
            entry_order=entry_order,
            protection_order=audit,
            emergency_close_order=rollback_audit,
            position_id=position_id,
            opened_qty=float(rollback_audit.get("close_qty") or live_qty or 0.0),
        ) from stop_exc

    stop_id = clean_exchange_id(
        (fallback_stop or {}).get("_confirmed_order_id")
        or (fallback_stop or {}).get("stopPlanOrderId")
        or (fallback_stop or {}).get("orderId")
        or (fallback_stop or {}).get("orderID")
    )
    if not bool((fallback_stop or {}).get("_stop_open_confirmed")) or not stop_id:
        rollback_audit = await _rollback_unprotected_market_position(
            adapter,
            symbol=symbol,
            side=side_l,
            stop=wanted_stop,
            requested_qty=requested_qty,
            sig_hash=sig_hash,
            user_id=user_id,
            trade_group_id=trade_group_id,
            position_id=position_id,
        )
        audit.update(
            {
                "status": "fallback_stop_unconfirmed_rollback_attempted",
                "fallback_qty": fallback_qty,
                "fallback_response_v1": fallback_stop,
                "emergency_rollback_v1": rollback_audit,
            }
        )
        result_payload["market_post_fill_stop_v1"] = audit
        raise BingxMarketProtectionError(
            "MARKET fallback STOP response was not read-back confirmed; TP placement blocked and emergency rollback was attempted",
            emergency_close_confirmed=bool(rollback_audit.get("confirmed")),
            entry_order=entry_order,
            protection_order=audit,
            emergency_close_order=rollback_audit,
            position_id=position_id,
            opened_qty=float(rollback_audit.get("close_qty") or live_qty or 0.0),
        )

    # A late attached STOP can become visible after the explicit fallback was
    # confirmed.  If that happens, cancel only the exact fallback id created by
    # this call, never the pre-existing/late order.  This removes the duplicate
    # without creating a protection gap.
    post_write_rows: list[dict[str, Any]] = []
    post_write_candidates: list[dict[str, Any]] = []
    post_write_ids: list[str] = []
    post_write_visibility_attempts = 0
    for post_write_attempt in range(6):
        post_write_visibility_attempts = post_write_attempt + 1
        post_write_rows, post_write_candidates = await _read_matching_stops()
        post_write_ids = sorted(
            {
                _protective_stop_order_id(row)
                for row in post_write_candidates
                if _protective_stop_order_id(row)
            }
        )
        if len(post_write_candidates) > 1 or (post_write_ids and stop_id not in post_write_ids):
            break
        if post_write_attempt < 5:
            await asyncio.sleep(min(0.75, 0.15 * (post_write_attempt + 1)))
    if len(post_write_ids) == 1 and stop_id not in post_write_ids:
        retained_id = post_write_ids[0]
        audit.update(
            {
                "status": "late_attached_stop_retained_fallback_not_live",
                "fallback_qty": fallback_qty,
                "fallback_stop_order_id": stop_id,
                "fallback_response_v1": fallback_stop,
                "stop_order_id": retained_id,
                "stopPlanOrderId": retained_id,
                "candidate_ids": post_write_ids,
                "matched_count": 1,
                "post_write_visibility_attempts": post_write_visibility_attempts,
            }
        )
        result_payload["market_post_fill_stop_v1"] = audit
        return audit

    if len(post_write_candidates) > 1 and stop_id in post_write_ids:
        retained_before_cancel = [value for value in post_write_ids if value != stop_id]
        rollback_audit: dict[str, Any] = {
            "detected_ids": post_write_ids,
            "new_fallback_id": stop_id,
            "retained_ids_before_cancel": retained_before_cancel,
        }
        try:
            rollback_audit["cancel_result"] = await adapter.cancel_conditional_orders_exact(
                [stop_id], symbol=symbol
            )
            _, remaining_candidates = await _read_matching_stops()
            remaining_ids = sorted(
                {
                    _protective_stop_order_id(row)
                    for row in remaining_candidates
                    if _protective_stop_order_id(row)
                }
            )
            rollback_audit["remaining_ids"] = remaining_ids
            rollback_audit["fallback_removed"] = stop_id not in remaining_ids
            rollback_audit["protection_retained"] = bool(remaining_ids)
            if stop_id in remaining_ids or not remaining_ids or len(remaining_ids) > 1:
                raise RuntimeError(
                    "exact fallback rollback did not leave one confirmed protective STOP"
                )
            retained_id = remaining_ids[0]
            audit.update(
                {
                    "status": "late_attached_stop_retained_fallback_rolled_back",
                    "fallback_qty": fallback_qty,
                    "fallback_stop_order_id": stop_id,
                    "fallback_response_v1": fallback_stop,
                    "duplicate_race_rollback_v1": rollback_audit,
                    "stop_order_id": retained_id,
                    "stopPlanOrderId": retained_id,
                    "candidate_ids": remaining_ids,
                    "matched_count": 1,
                }
            )
            result_payload["market_post_fill_stop_v1"] = audit
            return audit
        except Exception as cleanup_exc:
            rollback_audit["error"] = f"{type(cleanup_exc).__name__}: {cleanup_exc}"
            audit.update(
                {
                    "status": "duplicate_stop_race_cleanup_unconfirmed",
                    "fallback_qty": fallback_qty,
                    "fallback_stop_order_id": stop_id,
                    "fallback_response_v1": fallback_stop,
                    "duplicate_race_rollback_v1": rollback_audit,
                    "manual_cleanup_required": True,
                }
            )
            result_payload["market_post_fill_stop_v1"] = audit
            raise BingxMarketProtectionError(
                "A late attached STOP appeared after fallback creation and exact duplicate cleanup was not proven",
                emergency_close_confirmed=False,
                entry_order=entry_order,
                protection_order=audit,
                position_id=position_id,
                opened_qty=live_qty,
            ) from cleanup_exc

    audit.update(
        {
            "status": "fallback_stop_created_and_confirmed",
            "fallback_qty": fallback_qty,
            "fallback_stop_order_id": stop_id,
            "fallback_response_v1": fallback_stop,
            "post_write_candidate_ids": post_write_ids,
            "post_write_visibility_attempts": post_write_visibility_attempts,
        }
    )
    result_payload["market_post_fill_stop_v1"] = audit
    return audit


def background_tp_stats() -> dict[str, int]:
    """Best-effort process-local stats for v1.0.6a/6b background TP tasks."""
    with _BACKGROUND_TP_LOCK:
        data = dict(_BACKGROUND_TP_STATS)
        data["tracked"] = len(_BACKGROUND_TP_TASKS)
        data["tracked_executions"] = len(_BACKGROUND_TP_EXECUTION_IDS)
        return data


def _background_tp_stat(name: str, delta: int = 1) -> None:
    try:
        with _BACKGROUND_TP_LOCK:
            _BACKGROUND_TP_STATS[name] = int(_BACKGROUND_TP_STATS.get(name, 0)) + int(delta)
            if _BACKGROUND_TP_STATS.get(name, 0) < 0:
                _BACKGROUND_TP_STATS[name] = 0
    except Exception:
        return


async def stop_background_tp_tasks(timeout: float | None = None) -> None:
    """Wait briefly for in-memory TP tasks before DB/HTTP shutdown.

    On a clean shutdown we still give already-started TP placement tasks a
    bounded grace period before closing DB/HTTP resources. v1.0.6b also stores
    enough job metadata in trade_executions so a later process can re-schedule
    unfinished protected rows if this grace period is not enough.
    """
    with _BACKGROUND_TP_LOCK:
        tasks = [task for task in _BACKGROUND_TP_TASKS if not task.done()]
    if not tasks:
        return
    wait_timeout = max(1.0, float(timeout if timeout is not None else get_settings().TRADE_DISPATCHER_SHUTDOWN_TIMEOUT_SECONDS))
    done, pending = await asyncio.wait(tasks, timeout=wait_timeout)
    if pending:
        log.error(
            "background TP shutdown grace expired pending=%s timeout=%.1fs",
            len(pending),
            wait_timeout,
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
    if done:
        await asyncio.gather(*done, return_exceptions=True)


def _tp_order_entry_from_result_v1_0_6b(
    idx: int,
    target: float,
    tp_qty: float,
    order: Any,
) -> dict[str, Any]:
    actual_tp_qty = order_normalized_qty(order, tp_qty)
    if isinstance(order, dict) and order.get("_reduce_only_fallback"):
        log.warning(
            "TP%d placed WITHOUT reduceOnly (exchange fallback). Monitor position closely.",
            idx,
        )
    return {
        "tp_index": int(idx),
        "target": float(target),
        "qty": actual_tp_qty,
        "planned_qty": float(tp_qty),
        "order": order,
        "reduce_only_fallback": bool(
            isinstance(order, dict) and order.get("_reduce_only_fallback")
        ),
    }


async def _place_market_tp_orders_v1_0_6a(
    adapter: Any,
    *,
    exchange: str,
    symbol: str,
    side: str,
    sig_hash: str,
    user_id: int,
    market_position_id: str,
    market_plan_items: list[dict[str, Any]],
    tp_parallel_limit: int,
    initial_owned_order_ids: set[str] | None = None,
    skip_tp_indexes: set[int] | None = None,
    on_tp_confirmed: Callable[[dict[str, Any], int], Any] | None = None,
) -> tuple[list[dict[str, Any]], list[tuple[int, float, float, Any, Exception | None]], dict[str, Any]]:
    """Place TP rows using the existing sequential BingX fail-fast semantics.

    v1.0.6b keeps the fast v1.0.6a worker-release behavior, but persists each
    confirmed TP row immediately. If Railway redeploys after TP1 but before TP3,
    the next process can recover from durable metadata instead of starting from
    an empty in-memory task.
    """
    tp_sem = asyncio.Semaphore(max(1, int(tp_parallel_limit or 1)))
    tp_orders: list[dict[str, Any]] = []
    skipped = {int(x) for x in (skip_tp_indexes or set())}
    bingx_confirmed_tp_order_ids: set[str] = {
        clean_exchange_id(value)
        for value in (initial_owned_order_ids or set())
        if clean_exchange_id(value)
    }

    async def _place_one_tp(sequence: int, idx: int, tp_target: float, tp_quantity: float):
        if exchange == "bingx":
            # Preserve the existing v1.6.67+ BingX pacing/fail-fast behaviour.
            await asyncio.sleep(((sequence - 1) // 4) * 2.10)
        async with tp_sem:
            try:
                order_resp = await adapter.create_take_profit(
                    symbol=symbol,
                    side=side,
                    qty=tp_quantity,
                    price=tp_target,
                    client_id=_stable_client_id("avc-tp", sig_hash, user_id, f"tp{idx}"),
                    position_id=market_position_id or None,
                    owned_order_ids=(
                        sorted(bingx_confirmed_tp_order_ids)
                        if exchange == "bingx"
                        else None
                    ),
                )
                return (idx, tp_target, tp_quantity, order_resp, None)
            except Exception as exc:
                return (idx, tp_target, tp_quantity, None, exc)

    items_to_place = [
        item for item in market_plan_items
        if int(item.get("tp_index") or 0) not in skipped
    ]
    if exchange == "bingx":
        tp_results = []
        for sequence, item in enumerate(items_to_place, start=1):
            idx = int(item["tp_index"])
            result = await _place_one_tp(
                sequence,
                idx,
                float(item["price"]),
                float(item["qty"]),
            )
            tp_results.append(result)
            if result[4] is not None:
                break
            entry = _tp_order_entry_from_result_v1_0_6b(idx, float(item["price"]), float(item["qty"]), result[3])
            tp_orders.append(entry)
            confirmed_tp_id = _exchange_order_id_from_payload(result[3])
            if confirmed_tp_id:
                bingx_confirmed_tp_order_ids.add(confirmed_tp_id)
            if on_tp_confirmed is not None:
                outcome = on_tp_confirmed(entry, len(tp_orders))
                if hasattr(outcome, "__await__"):
                    await outcome
    else:
        tp_results = await asyncio.gather(
            *[
                _place_one_tp(
                    sequence,
                    int(item["tp_index"]),
                    float(item["price"]),
                    float(item["qty"]),
                )
                for sequence, item in enumerate(items_to_place, start=1)
            ]
        )
        tp_results.sort(key=lambda r: r[0])
        for i, target, tp_qty, order, exc in tp_results:
            if exc is not None:
                continue
            entry = _tp_order_entry_from_result_v1_0_6b(i, target, tp_qty, order)
            tp_orders.append(entry)
            if on_tp_confirmed is not None:
                outcome = on_tp_confirmed(entry, len(tp_orders))
                if hasattr(outcome, "__await__"):
                    await outcome
    tp_results.sort(key=lambda r: r[0])
    parity = {
        "enabled": bool(exchange == "bingx"),
        "attempted": len(tp_results),
        "planned": len(market_plan_items),
        "skipped_confirmed": len(skipped),
        "fail_fast": any(r[4] is not None for r in tp_results),
        "background_v1_0_6a": True,
        "durable_progress_v1_0_6b": True,
    }
    return tp_orders, tp_results, parity

def _tp_error_payload_v1_0_6a(
    failed_tps: list[tuple[int, float, float, Any, Exception | None]],
    tp_orders: list[dict[str, Any]],
    *,
    exchange: str,
) -> dict[str, Any]:
    first_failed = failed_tps[0]
    f_idx, f_target, f_qty, _, f_exc = first_failed
    return {
        "failed_count": len(failed_tps),
        "first_failed": {
            "tp_index": f_idx,
            "target": f_target,
            "qty": f_qty,
            "error": f"{type(f_exc).__name__}: {f_exc}",
        },
        "failed_indices": [r[0] for r in failed_tps],
        "failed_rows": [
            {
                "tp_index": r[0],
                "target": r[1],
                "qty": r[2],
                "error_type": type(r[4]).__name__,
                "error": str(r[4])[:1000],
            }
            for r in failed_tps
        ],
        "confirmed_before_failure": len(tp_orders),
        "mexc_parity_fail_fast": bool(exchange == "bingx"),
        "background_v1_0_6a": True,
    }




def _execution_payload_from_row_v1_0_6b(row: Any) -> dict[str, Any]:
    if not isinstance(row, dict):
        return {}
    try:
        payload = json.loads(row.get("exchange_order_ids_json") or "{}")
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _background_tp_record_v1_0_6b(payload: dict[str, Any]) -> dict[str, Any]:
    for key in ("tp_background_v1_0_6a", "tp_background_v1_0_6b"):
        raw = (payload or {}).get(key)
        if isinstance(raw, dict):
            return dict(raw)
    return {}


def _recoverable_background_tp_payload_v1_0_6b(payload: dict[str, Any]) -> bool:
    record = _background_tp_record_v1_0_6b(payload)
    if not bool(record.get("enabled")):
        return False
    state = str(record.get("state") or "").strip().lower()
    if state in {"completed", "partial_error"}:
        return False
    if state not in {"queued", "running", "retry", "error", ""}:
        return False
    return bool(snapshot_items((payload or {}).get(SNAPSHOT_KEY)))


def _confirmed_tp_indexes_from_payload_v1_0_6b(payload: dict[str, Any]) -> set[int]:
    indexes: set[int] = set()
    for item in (payload or {}).get("tp") or []:
        if not isinstance(item, dict):
            continue
        try:
            idx = int(item.get("tp_index") or 0)
        except (TypeError, ValueError, OverflowError):
            idx = 0
        if idx > 0:
            indexes.add(idx)
    return indexes


def _confirmed_tp_order_ids_from_payload_v1_0_6b(payload: dict[str, Any]) -> set[str]:
    ids: set[str] = set()
    for item in (payload or {}).get("tp") or []:
        if not isinstance(item, dict):
            continue
        order = item.get("order") if isinstance(item.get("order"), dict) else item
        oid = clean_exchange_id(
            (order or {}).get("_confirmed_order_id")
            or (order or {}).get("orderId")
            or (order or {}).get("orderID")
            or (order or {}).get("stopPlanOrderId")
        )
        if oid:
            ids.add(oid)
    return ids


def _background_market_position_id_from_payload_v1_0_6b(payload: dict[str, Any]) -> str:
    stop_payload = (payload or {}).get("market_post_fill_stop_v1")
    if isinstance(stop_payload, dict):
        for key in ("positionId", "position_id", "market_position_id"):
            value = clean_exchange_id(stop_payload.get(key))
            if value:
                return value
    snapshot = (payload or {}).get(SNAPSHOT_KEY)
    if isinstance(snapshot, dict):
        value = clean_exchange_id(snapshot.get("position_id"))
        if value:
            return value
    return ""

async def _background_market_tp_task_v1_0_6a(
    *,
    execution_id: int,
    api_row: Any,
    exchange: str,
    symbol: str,
    side: str,
    sig_hash: str,
    user_id: int,
    market_position_id: str,
    market_plan_items: list[dict[str, Any]],
    tp_parallel_limit: int,
    notify: Optional[NotifyFn],
    recovered: bool = False,
) -> None:
    started = time.monotonic()
    adapter = None
    _background_tp_stat("active", 1)
    try:
        async with db.execution_lock(int(execution_id)):
            current = await db.get_execution_by_id(int(execution_id))
            if not current or str(current.get("status") or "") != "protected":
                _background_tp_stat("skipped", 1)
                return
            current_payload = _execution_payload_from_row_v1_0_6b(current)
            if not _recoverable_background_tp_payload_v1_0_6b(current_payload):
                _background_tp_stat("skipped", 1)
                return
            prior_tp_indexes = _confirmed_tp_indexes_from_payload_v1_0_6b(current_payload)
            prior_tp_ids = _confirmed_tp_order_ids_from_payload_v1_0_6b(current_payload)
            planned_count = len(market_plan_items)
            if not planned_count:
                market_plan_items = snapshot_items(current_payload.get(SNAPSHOT_KEY))
                planned_count = len(market_plan_items)
            if not market_position_id:
                market_position_id = _background_market_position_id_from_payload_v1_0_6b(current_payload)

            running_patch = {
                "tp_background_v1_0_6a": {
                    "enabled": True,
                    "state": "running",
                    "planned": planned_count,
                    "confirmed": len(prior_tp_indexes),
                    "durable_recovery_v1_0_6b": True,
                    "recovered": bool(recovered),
                    "safety": "entry_and_stop_confirmed_before_background_tp",
                }
            }
            claimed = await db.update_execution_status_merge(
                int(execution_id),
                "protected",
                "MARKET entry and STOP confirmed; background TP placement running v1.0.6b",
                running_patch,
                expected_status=["protected"],
                write_flow_audit_stage="background_market_tp_running_v1_0_6b",
                write_flow_audit_status="protected",
            )
            if not claimed:
                _background_tp_stat("skipped", 1)
                return

            async def _persist_progress(order_entry: dict[str, Any], newly_confirmed: int) -> None:
                confirmed_total = len(prior_tp_indexes) + int(newly_confirmed or 0)
                ok = await db.update_execution_status_merge(
                    int(execution_id),
                    "protected",
                    "MARKET entry and STOP confirmed; background TP placement running v1.0.6b",
                    {
                        "tp": [order_entry],
                        "tp_background_v1_0_6a": {
                            "enabled": True,
                            "state": "running",
                            "planned": planned_count,
                            "confirmed": confirmed_total,
                            "last_confirmed_tp_index": int(order_entry.get("tp_index") or 0),
                            "durable_progress_v1_0_6b": True,
                            "durable_recovery_v1_0_6b": True,
                            "recovered": bool(recovered),
                        },
                    },
                    expected_status=["protected"],
                    write_flow_audit_stage="background_market_tp_progress_v1_0_6b",
                    write_flow_audit_status="protected",
                )
                if not ok:
                    raise RuntimeError(
                        "background TP progress could not be persisted; stopping TP writes to avoid stale execution state"
                    )

            adapter = build_adapter(api_row)
            tp_orders, tp_results, parity = await _place_market_tp_orders_v1_0_6a(
                adapter,
                exchange=exchange,
                symbol=symbol,
                side=side,
                sig_hash=sig_hash,
                user_id=user_id,
                market_position_id=market_position_id,
                market_plan_items=market_plan_items,
                tp_parallel_limit=tp_parallel_limit,
                initial_owned_order_ids=prior_tp_ids,
                skip_tp_indexes=prior_tp_indexes,
                on_tp_confirmed=_persist_progress,
            )
            failed_tps = [r for r in tp_results if r[4] is not None]
            elapsed_ms = int(max(0.0, time.monotonic() - started) * 1000)
            confirmed_total = len(prior_tp_indexes) + len(tp_orders)
            if failed_tps:
                first_failed = failed_tps[0]
                f_idx, _, _, _, f_exc = first_failed
                reason = (
                    f"BACKGROUND MARKET TP partially completed: {len(failed_tps)} failed, "
                    f"{confirmed_total} confirmed. First error on TP{f_idx}: "
                    f"{type(f_exc).__name__}: {f_exc}"
                )
                patch = {
                    "tp": tp_orders,
                    "tp_error": _tp_error_payload_v1_0_6a(failed_tps, tp_orders, exchange=exchange),
                    "bingx_tp_sequential_parity_v1": parity,
                    "tp_background_v1_0_6a": {
                        "enabled": True,
                        "state": "partial_error",
                        "elapsed_ms": elapsed_ms,
                        "attempted": len(tp_results),
                        "planned": planned_count,
                        "confirmed": confirmed_total,
                        "failed": len(failed_tps),
                        "durable_recovery_v1_0_6b": True,
                        "recovered": bool(recovered),
                    },
                }
                ok = await db.update_execution_status_merge(
                    int(execution_id),
                    "partial_error",
                    reason,
                    patch,
                    expected_status=["protected"],
                    write_flow_audit_stage="background_market_tp_partial_v1_0_6b",
                    write_flow_audit_status="partial_error",
                )
                _background_tp_stat("partial_error", 1)
                log.warning(
                    "background TP partial execution_id=%s symbol=%s confirmed=%s failed=%s db_updated=%s recovered=%s",
                    int(execution_id),
                    symbol,
                    confirmed_total,
                    len(failed_tps),
                    ok,
                    bool(recovered),
                )
                if ok:
                    await _maybe_notify(
                        notify,
                        user_id,
                        card(
                            "🟡 <b>TP УСТАНОВЛЕНЫ ЧАСТИЧНО</b>",
                            symbol=symbol,
                            side=side,
                            blocks=(
                                [
                                    f"✅ <b>Подтверждено TP:</b> {confirmed_total}",
                                    f"❌ <b>Не подтверждено TP:</b> {len(failed_tps)}",
                                    f"⚠️ <b>Первая ошибка:</b> TP{f_idx}",
                                ],
                                [details_line(f"{type(f_exc).__name__}: {str(f_exc)[:300]}")],
                                [
                                    "✅ STOP уже подтверждён",
                                    "🔄 Бот сохранил частичное состояние и запустит восстановление",
                                    "📱 Проверьте TP на BingX",
                                ],
                            ),
                        ),
                    )
                return

            patch = {
                "tp": tp_orders,
                "bingx_tp_sequential_parity_v1": parity,
                "tp_background_v1_0_6a": {
                    "enabled": True,
                    "state": "completed",
                    "elapsed_ms": elapsed_ms,
                    "attempted": len(tp_results),
                    "planned": planned_count,
                    "confirmed": confirmed_total,
                    "failed": 0,
                    "durable_recovery_v1_0_6b": True,
                    "recovered": bool(recovered),
                },
                "execution_timing_v1_0_4": {
                    "tp_completed_background_ms": elapsed_ms,
                },
            }
            ok = await db.update_execution_status_merge(
                int(execution_id),
                "opened",
                "",
                patch,
                expected_status=["protected"],
                write_flow_audit_stage="background_market_tp_success_v1_0_6b",
                write_flow_audit_status="opened",
            )
            _background_tp_stat("completed", 1)
            log.info(
                "background TP completed execution_id=%s symbol=%s confirmed=%s elapsed_ms=%s db_updated=%s recovered=%s",
                int(execution_id),
                symbol,
                confirmed_total,
                elapsed_ms,
                ok,
                bool(recovered),
            )
            if ok:
                await _maybe_notify(
                    notify,
                    user_id,
                    card(
                        "✅ <b>TP УСТАНОВЛЕНЫ</b>",
                        symbol=symbol,
                        side=side,
                        blocks=(
                            [
                                "✅ STOP уже подтверждён",
                                f"🎯 <b>TP подтверждено:</b> {confirmed_total}/{planned_count}",
                                f"⏱ <b>Фон:</b> {elapsed_ms / 1000:.1f} сек",
                            ],
                            ["🔒 Позиция полностью защищена по плану бота"],
                        ),
                    ),
                )
    except asyncio.CancelledError:
        _background_tp_stat("cancelled", 1)
        raise
    except Exception as exc:
        elapsed_ms = int(max(0.0, time.monotonic() - started) * 1000)
        reason = f"background MARKET TP task failed: {type(exc).__name__}: {exc}"
        patch = {
            "tp_background_v1_0_6a": {
                "enabled": True,
                "state": "error",
                "elapsed_ms": elapsed_ms,
                "error_type": type(exc).__name__,
                "error": str(exc)[:1000],
                "durable_recovery_v1_0_6b": True,
                "recovered": bool(recovered),
            },
            "tp_error": {
                "background_v1_0_6a": True,
                "durable_recovery_v1_0_6b": True,
                "error_type": type(exc).__name__,
                "error": str(exc)[:1000],
            },
        }
        await db.update_execution_status_merge(
            int(execution_id),
            "partial_error",
            reason,
            patch,
            expected_status=["protected"],
            write_flow_audit_stage="background_market_tp_error_v1_0_6b",
            write_flow_audit_status="partial_error",
        )
        _background_tp_stat("error", 1)
        log.exception(
            "background TP task failed execution_id=%s symbol=%s recovered=%s",
            int(execution_id),
            symbol,
            bool(recovered),
        )
        await _maybe_notify(
            notify,
            user_id,
            card(
                "🟡 <b>TP ТРЕБУЮТ ВОССТАНОВЛЕНИЯ</b>",
                symbol=symbol,
                side=side,
                blocks=(
                    ["✅ STOP уже подтверждён", "❌ Фоновая постановка TP завершилась ошибкой"],
                    [details_line(reason)],
                    ["🔄 Бот сохранил состояние для восстановления", "📱 Проверьте TP на BingX"],
                ),
            ),
        )
    finally:
        try:
            if adapter is not None:
                await adapter.close()
        except Exception:
            pass
        _background_tp_stat("active", -1)

def _background_tp_task_already_scheduled(execution_id: int) -> bool:
    eid = int(execution_id or 0)
    with _BACKGROUND_TP_LOCK:
        return eid in _BACKGROUND_TP_EXECUTION_IDS


def _schedule_background_market_tp_v1_0_6a(**kwargs: Any) -> bool:
    eid = int(kwargs.get("execution_id") or 0)
    if eid <= 0:
        return False
    duplicate = False
    with _BACKGROUND_TP_LOCK:
        if eid in _BACKGROUND_TP_EXECUTION_IDS:
            duplicate = True
        else:
            _BACKGROUND_TP_EXECUTION_IDS.add(eid)
    if duplicate:
        _background_tp_stat("skipped", 1)
        return False
    task = asyncio.create_task(
        _background_market_tp_task_v1_0_6a(**kwargs),
        name=f"market-tp-bg:{kwargs.get('user_id')}:{kwargs.get('symbol')}:{eid}",
    )
    _background_tp_stat("scheduled", 1)
    if bool(kwargs.get("recovered")):
        _background_tp_stat("recovered", 1)
    with _BACKGROUND_TP_LOCK:
        _BACKGROUND_TP_TASKS.add(task)

    def _done(done_task: asyncio.Task[None]) -> None:
        with _BACKGROUND_TP_LOCK:
            _BACKGROUND_TP_TASKS.discard(done_task)
            _BACKGROUND_TP_EXECUTION_IDS.discard(eid)
        try:
            done_task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("background TP task escaped done callback")

    task.add_done_callback(_done)
    return True


async def process_background_tp_recovery_once(
    notify: Optional[NotifyFn] = None,
    *,
    limit: int | None = None,
) -> int:
    """Re-schedule durable protected background-TP jobs after restart/deploy.

    v1.0.6a intentionally used only in-memory tasks. v1.0.6b treats the
    protected execution row as the durable job: entry and STOP are already
    confirmed, TP plan is reconstructed from the immutable snapshot, and every
    confirmed TP is persisted incrementally before the next one is attempted.
    """
    settings = get_settings()
    if not bool(getattr(settings, "MARKET_TP_BACKGROUND_ENABLED", False)):
        return 0
    if not bool(getattr(settings, "MARKET_TP_BACKGROUND_RECOVERY_ENABLED", True)):
        return 0
    rows = await db.protected_background_tp_executions(
        limit=max(1, int(limit or 50))
    )
    record_stage_rows(selected=len(rows), scanned=len(rows), source="database")
    scheduled = 0
    for row in rows:
        payload = _execution_payload_from_row_v1_0_6b(row)
        if not _recoverable_background_tp_payload_v1_0_6b(payload):
            continue
        execution_id = int(row.get("id") or 0)
        if execution_id <= 0 or _background_tp_task_already_scheduled(execution_id):
            continue
        user_id = int(row.get("user_id") or 0)
        if user_id <= 0:
            continue
        exchange = "bingx"
        api_row = await get_api_key_cache().get_or_fetch(
            (user_id, "api", exchange),
            lambda user_id=user_id, exchange=exchange: db.get_api_key(user_id, exchange),
        )
        if not api_row:
            await db.update_execution_status_merge(
                execution_id,
                "partial_error",
                "background TP recovery cannot continue: BingX API key is missing",
                {
                    "tp_background_v1_0_6a": {
                        "enabled": True,
                        "state": "error",
                        "error_type": "api_missing",
                        "error": "BingX API key is missing during v1.0.6b background TP recovery",
                        "durable_recovery_v1_0_6b": True,
                    }
                },
                expected_status=["protected"],
                write_flow_audit_stage="background_market_tp_recovery_api_missing_v1_0_6b",
                write_flow_audit_status="partial_error",
            )
            continue
        market_plan_items = snapshot_items(payload.get(SNAPSHOT_KEY))
        if not market_plan_items:
            await db.update_execution_status_merge(
                execution_id,
                "partial_error",
                "background TP recovery cannot continue: immutable TP snapshot is missing",
                {
                    "tp_background_v1_0_6a": {
                        "enabled": True,
                        "state": "error",
                        "error_type": "snapshot_missing",
                        "error": "Immutable TP snapshot is missing during v1.0.6b background TP recovery",
                        "durable_recovery_v1_0_6b": True,
                    }
                },
                expected_status=["protected"],
                write_flow_audit_stage="background_market_tp_recovery_snapshot_missing_v1_0_6b",
                write_flow_audit_status="partial_error",
            )
            continue
        tp_parallel_limit = max(1, int(getattr(settings, "TP_PARALLEL_LIMIT", 4) or 4))
        tp_parallel_limit = min(tp_parallel_limit, 4)
        if _schedule_background_market_tp_v1_0_6a(
            execution_id=execution_id,
            api_row=api_row,
            exchange=exchange,
            symbol=str(row.get("symbol") or "").upper(),
            side=str(row.get("side") or "").lower(),
            sig_hash=str(row.get("signal_hash") or ""),
            user_id=user_id,
            market_position_id=_background_market_position_id_from_payload_v1_0_6b(payload),
            market_plan_items=[dict(item) for item in market_plan_items],
            tp_parallel_limit=tp_parallel_limit,
            notify=notify,
            recovered=True,
        ):
            scheduled += 1
    if scheduled:
        log.warning("background TP durable recovery scheduled=%s", scheduled)
    return scheduled


async def _limit_entry_context(
    adapter: Any, symbol: str, side: str, entry: float
) -> dict[str, Any]:
    """Best-effort context for LIMIT entry relative to current market price.

    This must never block the trade: if ticker fetch fails, return an empty context.
    LONG buy limit below current price is normal and waits for pullback.
    SHORT sell limit above current price is normal and waits for pullback.
    """
    try:
        current = float(await adapter.fetch_last_price(symbol))
    except Exception as exc:
        return {"status": "unknown", "error": f"{type(exc).__name__}: {exc}"}

    side_l = str(side or "").lower()
    may_fill_immediately = False
    waiting_pullback = False
    if side_l == "long":
        waiting_pullback = current > entry
        may_fill_immediately = current <= entry
    elif side_l == "short":
        waiting_pullback = current < entry
        may_fill_immediately = current >= entry

    return {
        "status": "ok",
        "current_price": current,
        "entry": float(entry),
        "waiting_pullback": bool(waiting_pullback),
        "may_fill_immediately": bool(may_fill_immediately),
    }


def _market_entry_reference_price(
    signal_entry: Any, shared_current_price: Any
) -> float:
    """Keep an explicit signal entry authoritative for MARKET drift checks."""
    for value in (signal_entry, shared_current_price):
        if isinstance(value, bool):
            continue
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError):
            continue
        if math.isfinite(parsed) and parsed > 0:
            return parsed
    return 0.0


def _is_bingx_price_band_rejection(value: Any) -> bool:
    text = str(value or "").strip().lower()
    if re.search(r"(?:^|\b)code\s*=\s*2003(?:\b|:)", text):
        return True
    return any(
        marker in text
        for marker in (
            "exceeded the maximum buying price",
            "exceeded the minimum selling price",
            "maximum buying price",
            "minimum selling price",
        )
    )


async def _signal_price_anomaly_preflight(
    adapter: Any,
    signal: Signal,
    *,
    current_price_hint: float | None,
    max_price_ratio: float,
) -> dict[str, Any] | None:
    """Detect an extreme signal/current-price mismatch before any BingX write.

    Failure to fetch a public ticker does not block an otherwise valid trade;
    the exchange's own price-band checks remain the final fallback.
    """
    try:
        entry = float(signal.entry or 0.0)
    except (TypeError, ValueError, OverflowError):
        return None
    if entry <= 0:
        return None

    try:
        current = float(current_price_hint or 0.0)
    except (TypeError, ValueError, OverflowError):
        current = 0.0
    if not math.isfinite(current) or current <= 0:
        try:
            current = float(
                await get_market_price_cache().get_or_fetch(
                    ("bingx", signal.symbol.upper()),
                    lambda: adapter.fetch_last_price(signal.symbol),
                )
            )
        except Exception as exc:
            log.warning(
                "signal price anomaly preflight unavailable symbol=%s error=%s",
                str(signal.symbol).upper(),
                f"{type(exc).__name__}: {exc}",
            )
            return None

    anomaly = detect_signal_price_anomaly(
        entry,
        current,
        max_price_ratio=max_price_ratio,
    )
    if anomaly is None:
        return None
    settings = get_settings()
    payload = anomaly.as_payload()
    payload.update(
        decimal_normalization_preview_payload(
            signal,
            current,
            enabled=bool(settings.SIGNAL_DECIMAL_NORMALIZATION_PREVIEW_ENABLED),
            max_deviation_after_percent=float(
                settings.SIGNAL_DECIMAL_NORMALIZATION_MAX_DEVIATION_PERCENT or 0.0
            ),
            max_power=int(settings.SIGNAL_DECIMAL_NORMALIZATION_MAX_POWER or 0),
        )
    )
    return payload


async def _maybe_notify(notify: Optional[NotifyFn], user_id: int, text: str) -> None:
    if not notify:
        return
    try:
        res = notify(user_id, ensure_visual_card(text))
        if hasattr(res, "__await__"):
            await res  # type: ignore[misc]
    except Exception:
        pass


async def _execute_signal_for_user_unlocked(
    signal: Signal,
    user_id: int,
    *,
    source_chat_id: int | None = None,
    trade_group_id: int | None = None,
    market_entry_hint: float | None = None,
    notify: Optional[NotifyFn] = None,
    dispatch_context: dict[str, Any] | None = None,
) -> ExecutionResult:
    raw_signal_symbol = signal.symbol
    market_entry_hint = scale_bingx_1000_market_hint(
        raw_signal_symbol, market_entry_hint
    )
    signal = canonicalize_bingx_1000_signal(signal)
    settings = get_settings()
    dispatch_meta = dict(dispatch_context or {})
    _us_cache = get_user_settings_cache()
    user_settings = await _us_cache.get_or_fetch(
        (user_id, "settings"),
        lambda: db.get_user_settings(user_id),
    )
    sig_hash = signal_hash(signal, source_chat_id)
    exchange = (user_settings.exchange or settings.safe_default_exchange).lower()

    if user_settings.mode == UserMode.OFF:
        return ExecutionResult(
            user_id,
            "skipped",
            "VIP режим выключен",
            optional_trade_skip_payload("user_mode_off"),
        )

    selected_targets = limit_targets_for_mode(
        signal.targets, user_settings.tp_limit, user_settings.tp_mode
    )
    if not selected_targets:
        return ExecutionResult(
            user_id,
            "skipped",
            "нет TP в сигнале",
            mandatory_trade_warning_payload("signal_has_no_tp"),
        )
    signal_tp_percents = (
        _signal_distribution(signal, selected_targets)
        if bool(getattr(user_settings, "use_signal_tp_percents", False))
        else []
    )
    tp_percents = signal_tp_percents or choose_distribution(
        user_settings.tp_mode, len(selected_targets), user_settings.manual_tp_percents
    )

    if user_settings.mode == UserMode.PREVIEW:
        if await db.is_duplicate(sig_hash, user_id):
            return ExecutionResult(
                user_id,
                "skipped",
                "дубликат сигнала",
                optional_trade_skip_payload("duplicate_signal"),
            )
        await db.mark_duplicate(sig_hash, source_chat_id, signal.signal_id, user_id)
        return ExecutionResult(
            user_id,
            "preview",
            "режим просмотра",
            {
                "exchange": exchange,
                "targets": selected_targets,
                "tp_percents": tp_percents,
            },
        )

    if not settings.is_exchange_enabled(exchange):
        return ExecutionResult(
            user_id,
            "skipped",
            f"биржа {exchange_title(exchange)} отключена админом",
            mandatory_trade_warning_payload("exchange_disabled"),
        )

    # All read-only account checks are independent. Running them together
    # removes several sequential PostgreSQL round-trips from the critical path
    # while preserving fail-closed risk, dedup and same-symbol protection.
    _ak_cache = get_api_key_cache()
    duplicate, state, same_symbol, api_row = await asyncio.gather(
        db.is_duplicate(sig_hash, user_id),
        db.user_risk_state(user_id, settings_row=user_settings),
        db.other_active_symbol_executions(
            user_id, signal.symbol, exclude_execution_id=0, limit=1
        ),
        _ak_cache.get_or_fetch(
            (user_id, "api_execution", exchange),
            lambda: db.get_api_key(
                user_id, exchange, include_quarantine=True
            ),
        ),
    )
    api_quarantine = (
        dict(api_row)
        if isinstance(api_row, dict) and api_row.get("api_quarantined") is True
        else None
    )

    if api_quarantine:
        return ExecutionResult(
            user_id,
            "skipped",
            _api_quarantine_reason(),
            _api_quarantine_payload(
                api_quarantine, exchange=exchange, trade_group_id=trade_group_id
            ),
        )

    if duplicate:
        return ExecutionResult(
            user_id,
            "skipped",
            "дубликат сигнала",
            optional_trade_skip_payload("duplicate_signal"),
        )

    risk_error = _risk_limit_error(
        user_settings, state, float(user_settings.risk_per_trade_percent)
    )
    if risk_error:
        return ExecutionResult(
            user_id,
            "skipped",
            risk_error,
            optional_trade_skip_payload("risk_limit", {"risk_state": state}),
        )

    # BingX aggregates same-symbol exposure into one live position. Tracking two
    # independent plans for the same symbol would make STOP/TP attribution and
    # orphan-order cleanup ambiguous, so fail closed until the previous plan is
    # fully resolved.
    if same_symbol:
        row = same_symbol[0]
        return ExecutionResult(
            user_id,
            "skipped",
            (
                f"по {signal.symbol.upper()} уже есть активная или ожидающая сделка "
                f"(статус: {row.get('status') or 'unknown'}). Новая сделка пропущена, "
                "чтобы не смешивать STOP/TP одной агрегированной позиции"
            ),
            optional_trade_skip_payload(
                "same_symbol_active",
                {
                    "existing_execution_status": str(row.get("status") or "unknown"),
                    "symbol": signal.symbol.upper(),
                },
            ),
        )

    if not api_row:
        return ExecutionResult(
            user_id,
            "skipped",
            f"нет подключённого {exchange_title(exchange)} API",
            mandatory_trade_warning_payload("api_not_connected"),
        )

    api_credential_fingerprint = db.api_credential_fingerprint(api_row)
    adapter = build_adapter(api_row)

    anomaly_payload = await _signal_price_anomaly_preflight(
        adapter,
        signal,
        current_price_hint=market_entry_hint,
        max_price_ratio=float(settings.MAX_SIGNAL_ENTRY_PRICE_RATIO or 0.0),
    )
    if anomaly_payload is not None:
        # The exact malformed signal is now a durable duplicate, while a
        # corrected resend remains allowed because ENTRY/STOP/TP participate in
        # ``signal_hash``. Without this marker every repeated bad channel post
        # could spam the user/admin and, when public ticker reads fail, repeatedly
        # reach BingX's code=2003 fallback.
        try:
            await db.mark_duplicate(sig_hash, source_chat_id, signal.signal_id, user_id)
        except Exception:
            log.exception(
                "failed to persist anomalous signal dedup uid=%s symbol=%s",
                int(user_id),
                str(signal.symbol).upper(),
            )
        await adapter.close()
        ratio = float(anomaly_payload.get("price_ratio") or 0.0)
        deviation = float(anomaly_payload.get("price_deviation_percent") or 0.0)
        log.warning(
            "signal blocked before exchange write uid=%s symbol=%s entry=%s current=%s ratio=%.6f deviation_pct=%.6f",
            int(user_id),
            str(signal.symbol).upper(),
            anomaly_payload.get("signal_entry"),
            anomaly_payload.get("current_price"),
            ratio,
            deviation,
        )
        return ExecutionResult(
            user_id,
            "skipped",
            "аномальное расхождение цены сигнала и текущей цены BingX",
            mandatory_trade_warning_payload(
                "signal_price_anomaly", {"exchange": exchange, **anomaly_payload}
            ),
        )

    leverage = 0
    sizing = None
    entry_order = None
    tp_orders: list[dict] = []
    base_ids: dict[str, Any] = {
        "exchange": exchange,
        "tp": tp_orders,
        "trade_group_id": trade_group_id,
    }
    result_payload: dict[str, Any] = {
        "exchange": exchange,
        "trade_group_id": trade_group_id,
    }
    execution_timing_started = time.monotonic()
    execution_timing_v1_0_4: dict[str, int] = {}

    def _mark_execution_timing_v1_0_4(name: str) -> None:
        try:
            execution_timing_v1_0_4[f"{name}_ms"] = int(
                max(0.0, time.monotonic() - execution_timing_started) * 1000
            )
            result_payload["execution_timing_v1_0_4"] = dict(execution_timing_v1_0_4)
            base_ids["execution_timing_v1_0_4"] = dict(execution_timing_v1_0_4)
        except Exception:
            return

    if dispatch_meta:
        base_ids["dispatch"] = dict(dispatch_meta)
        result_payload["dispatch"] = dict(dispatch_meta)
    # Register: prevents BE-monitor/lifecycle-guard from doing cancel-all while
    # position is being opened. Cleared in finally block below.
    register_opening(user_id, signal.symbol)
    try:
        # Fetch independent preflight data concurrently. For MARKET signals
        # without ТВХ this removes one full BingX round-trip from the critical
        # path while preserving every risk and protection check.
        _ii_cache = get_instrument_info_cache()
        is_market_signal = (
            str(getattr(signal, "order_type", "LIMIT")).upper() == "MARKET"
        )
        needs_market_price = is_market_signal
        shared_market_hint = float(market_entry_hint or 0.0)
        market_price_coro = (
            get_market_price_cache().get_or_fetch(
                (exchange, signal.symbol.upper()),
                lambda: adapter.fetch_last_price(signal.symbol),
            )
            if needs_market_price
            else asyncio.sleep(0, result=None)
        )
        try:
            (
                balance_details,
                info,
                market_entry_value,
                live_positions,
                live_orders,
            ) = await asyncio.gather(
                adapter.fetch_balance_details(),
                _ii_cache.get_or_fetch(
                    (exchange, signal.symbol.upper()),
                    lambda: adapter.instrument_info(signal.symbol),
                ),
                market_price_coro,
                # Fail closed on external/manual state that is not present in
                # our DB. BingX aggregates same-symbol exposure; an old pending
                # entry can fill later and corrupt the new STOP/TP plan. Both
                # reads run in parallel, so they add no sequential round-trip.
                adapter.fetch_open_positions(signal.symbol),
                adapter.fetch_open_orders(signal.symbol),
            )
        except Exception as exc:
            await adapter.close()
            quarantine_result = await _quarantine_permission_result(
                user_id=user_id,
                exchange=exchange,
                trade_group_id=trade_group_id,
                exc=exc,
                credential_fingerprint=api_credential_fingerprint,
            )
            if quarantine_result is not None:
                return quarantine_result
            if _is_bingx_unsupported_symbol_error(exc):
                try:
                    await db.mark_duplicate(sig_hash, source_chat_id, signal.signal_id, user_id)
                except Exception:
                    pass
                return ExecutionResult(
                    user_id,
                    "skipped",
                    _unsupported_symbol_reason(signal),
                    _unsupported_symbol_result_payload(
                        signal,
                        exc,
                        exchange=exchange,
                        trade_group_id=trade_group_id,
                    ),
                )
            return ExecutionResult(
                user_id,
                "error",
                f"не удалось получить данные BingX для расчёта сделки: {exc}",
                {"exchange": exchange, "trade_group_id": trade_group_id},
            )

        _mark_execution_timing_v1_0_4("preflight_data_ready")
        balance, available_balance = _balance_preflight_values(balance_details)
        result_payload["balance_preflight"] = {
            "equity": balance,
            "available_balance": available_balance,
        }

        if live_positions:
            live_sides = (
                ", ".join(
                    sorted(
                        {
                            str(p.get("positionSide") or p.get("side") or "").upper()
                            for p in live_positions
                        }
                    )
                ).strip(", ")
                or "UNKNOWN"
            )
            await adapter.close()
            return ExecutionResult(
                user_id,
                "skipped",
                (
                    f"на BingX уже существует открытая позиция {signal.symbol.upper()} "
                    f"({live_sides}). Новая сделка пропущена, чтобы не объединять "
                    "разные торговые планы и не повредить STOP/TP"
                ),
                optional_trade_skip_payload(
                    "live_position_guard",
                    {"exchange": exchange, "live_position_guard": True},
                ),
            )

        open_order_guard_audit = _entry_order_guard_diagnostics(live_orders, signal.symbol)
        opening_orders = [
            order for order in (live_orders or []) if _is_live_regular_entry_order(order, signal.symbol)
        ]
        if opening_orders:
            log.warning(
                "BingX live entry guard blocked signal uid=%s symbol=%s audit=%s",
                user_id,
                signal.symbol.upper(),
                open_order_guard_audit,
            )
            await adapter.close()
            return ExecutionResult(
                user_id,
                "skipped",
                (
                    f"на BingX уже есть активный входной ордер {signal.symbol.upper()}. "
                    "Новая сделка пропущена, чтобы старый ордер не исполнился позже "
                    "и не создал лишнюю позицию без корректного торгового плана"
                ),
                optional_trade_skip_payload(
                    "live_entry_order_guard",
                    {
                        "exchange": exchange,
                        "live_entry_order_guard": True,
                        "open_entry_orders": len(opening_orders),
                        "open_order_guard_audit": open_order_guard_audit,
                    },
                ),
            )

        if needs_market_price:
            market_entry = float(market_entry_value or 0.0)
            if market_entry <= 0:
                await adapter.close()
                return ExecutionResult(
                    user_id,
                    "error",
                    f"не удалось получить текущую цену для MARKET-входа {exchange_title(exchange)}",
                )

            reference_price = _market_entry_reference_price(
                signal.entry, shared_market_hint
            )
            deviation_percent = (
                abs(market_entry - reference_price) / reference_price * 100.0
                if reference_price > 0
                else 0.0
            )
            max_deviation = float(settings.MAX_MARKET_ENTRY_DEVIATION_PERCENT or 0.0)
            result_payload["market_entry"] = {
                "reason": "fresh_price_before_market_entry",
                "price": market_entry,
                "reference_price": reference_price,
                "deviation_percent": deviation_percent,
                "max_deviation_percent": max_deviation,
                "source": f"{exchange_title(exchange)} futures price (short shared cache)",
            }
            if (
                max_deviation > 0
                and reference_price > 0
                and deviation_percent > max_deviation + 1e-12
            ):
                await adapter.close()
                return ExecutionResult(
                    user_id,
                    "skipped",
                    (
                        f"цена MARKET ушла на {deviation_percent:.3f}% от цены сигнала "
                        f"(лимит {max_deviation:.3f}%); вход безопасно пропущен"
                    ),
                    mandatory_trade_warning_payload(
                        "market_price_deviation",
                        {
                            **result_payload,
                            "market_price_deviation": True,
                        },
                    ),
                )
            # Risk sizing for MARKET must use the fresh price actually available
            # when this worker reaches the exchange, not the first user's hint.
            signal = replace(signal, entry=market_entry)

        try:
            normalized_signal, price_normalization = _normalize_signal_prices_to_tick(
                signal, info.price_tick
            )
        except ValueError as exc:
            await adapter.close()
            return ExecutionResult(
                user_id,
                "skipped",
                (
                    f"цены сигнала не удалось привести к допустимому шагу {exchange_title(exchange)} price_tick={info.price_tick}: {exc}. "
                    "Сделка пропущена, чтобы не отправлять на биржу некорректный ENTRY/STOP/TP."
                ),
                mandatory_trade_warning_payload(
                    "price_normalization_failed",
                    {
                        "price_tick": info.price_tick,
                        "price_normalization_error": str(exc),
                    },
                ),
            )
        if price_normalization:
            signal = normalized_signal
            selected_targets = limit_targets_for_mode(
                signal.targets, user_settings.tp_limit, user_settings.tp_mode
            )
            signal_tp_percents = (
                _signal_distribution(signal, selected_targets)
                if bool(getattr(user_settings, "use_signal_tp_percents", False))
                else []
            )
            tp_percents = signal_tp_percents or choose_distribution(
                user_settings.tp_mode,
                len(selected_targets),
                user_settings.manual_tp_percents,
            )
            result_payload["price_normalization"] = price_normalization
        try:
            exchange_max_leverage, leverage_source = await _resolve_exchange_max_leverage(adapter, info, signal)
        except Exception as exc:
            await adapter.close()
            quarantine_result = await _quarantine_permission_result(
                user_id=user_id,
                exchange=exchange,
                trade_group_id=trade_group_id,
                exc=exc,
                credential_fingerprint=api_credential_fingerprint,
            )
            if quarantine_result is not None:
                return quarantine_result
            return ExecutionResult(
                user_id,
                "error",
                f"не удалось получить максимальное плечо {exchange_title(exchange)} для {info.symbol} {signal.side.value.upper()}: {exc}",
            )
        result_payload["max_leverage_preflight"] = {
            "exchange_max": int(exchange_max_leverage),
            "source": leverage_source,
            "side": signal.side.value.upper(),
        }

        requested_leverage = int(exchange_max_leverage)
        if str(settings.MARGIN_MODE).lower() == "isolated":
            requested_leverage = _safe_isolated_leverage(
                signal,
                int(exchange_max_leverage),
                float(settings.ISOLATED_LIQUIDATION_BUFFER_PERCENT),
            )
            result_payload["isolated_leverage_cap"] = {
                "exchange_max": int(exchange_max_leverage),
                "requested": int(requested_leverage),
                "buffer_percent": float(settings.ISOLATED_LIQUIDATION_BUFFER_PERCENT),
                "stop_distance_percent": round(
                    abs(float(signal.entry) - float(signal.stop))
                    / float(signal.entry)
                    * 100.0,
                    8,
                ),
            }
        leverage = requested_leverage

        # Cross mode keeps the exchange maximum to minimize margin usage.
        # Isolated mode uses the conservative cap above so liquidation is not
        # intentionally placed closer than STOP. Risk sizing still comes from
        # entry-to-STOP distance and fees, never from leverage itself.
        try:
            leverage = await adapter.set_margin_and_max_leverage(
                info.symbol,
                requested_leverage,
                settings.MARGIN_MODE,
                signal.side.value,
            )
            if int(leverage) < int(requested_leverage):
                result_payload["leverage_warning"] = (
                    f"{exchange_title(exchange)} не подтвердила запрошенное плечо {requested_leverage}x через API; "
                    f"используется фактически установленное плечо {leverage}x."
                )
                result_payload["requested_leverage"] = int(requested_leverage)
                result_payload["effective_leverage"] = int(leverage)
        except SymbolNotSupportedErrors:
            raise
        except Exception as exc:
            await adapter.close()
            quarantine_result = await _quarantine_permission_result(
                user_id=user_id,
                exchange=exchange,
                trade_group_id=trade_group_id,
                exc=exc,
                credential_fingerprint=api_credential_fingerprint,
            )
            if quarantine_result is not None:
                return quarantine_result
            return ExecutionResult(
                user_id,
                "error",
                f"не удалось установить/проверить leverage {requested_leverage}x {exchange_title(exchange)}: {exc}",
            )

        metadata_fee_rate = float(getattr(info, "taker_fee_rate", 0.0) or 0.0)
        configured_fee_rate = float(settings.BINGX_API_TAKER_FEE_RATE)
        # Never size with a fee lower than the configured safety floor. Public
        # contract metadata can be stale or omit API-specific fee schedules.
        taker_fee_rate = max(metadata_fee_rate, configured_fee_rate)
        result_payload["risk_taker_fee_rate"] = taker_fee_rate

        result_payload["sizing_preflight"] = {
            "equity": balance,
            "available_balance": available_balance,
            "risk_percent": float(user_settings.risk_per_trade_percent),
            "entry": float(signal.entry),
            "stop": float(signal.stop),
            "targets": len(selected_targets),
            "leverage": int(leverage),
            "qty_step": float(info.qty_step),
            "min_qty": float(info.min_qty),
            "min_notional": float(info.min_notional),
            "taker_fee_rate": float(taker_fee_rate),
        }
        try:
            sizing = calculate_position_size(
                balance_usdt=balance,
                risk_percent=user_settings.risk_per_trade_percent,
                signal=signal,
                leverage=leverage,
                qty_step=info.qty_step,
                min_qty=info.min_qty,
                min_notional=info.min_notional,
                taker_fee_rate=taker_fee_rate,
            )
        except ValueError as exc:
            log.warning(
                "BingX pre-entry sizing rejected uid=%s symbol=%s reason=%s equity=%.8f available=%.8f entry=%.12g stop=%.12g leverage=%s qty_step=%.12g min_qty=%.12g min_notional=%.12g",
                int(user_id),
                str(signal.symbol).upper(),
                str(exc),
                float(balance),
                float(available_balance),
                float(signal.entry),
                float(signal.stop),
                int(leverage),
                float(info.qty_step),
                float(info.min_qty),
                float(info.min_notional),
            )
            await adapter.close()
            return ExecutionResult(
                user_id,
                "skipped",
                f"размер позиции не прошёл preflight BingX: {exc}",
                mandatory_trade_warning_payload(
                    "sizing_rejected",
                    {
                        **result_payload,
                        "sizing_rejected": True,
                        "sizing_error": str(exc),
                    },
                ),
            )
        if sizing.qty <= 0 or sizing.notional < info.min_notional:
            await adapter.close()
            return ExecutionResult(
                user_id,
                "skipped",
                f"размер позиции после округления меньше min qty/min notional {exchange_title(exchange)}",
                mandatory_trade_warning_payload("order_below_exchange_minimum"),
            )
        if sizing.required_margin > available_balance:
            await adapter.close()
            return ExecutionResult(
                user_id,
                "skipped",
                (
                    "не хватает свободной маржи при выбранном безопасном плече "
                    f"(нужно ≈{sizing.required_margin:.4f} USDT, доступно "
                    f"≈{available_balance:.4f} USDT)"
                ),
                mandatory_trade_warning_payload(
                    "insufficient_available_margin",
                    {
                        **result_payload,
                        "required_margin": float(sizing.required_margin),
                        "available_balance": available_balance,
                        "equity": balance,
                    },
                ),
            )

        statistics_risk_snapshot = _statistics_risk_snapshot(
            sizing=sizing,
            signal=signal,
            entry_price=float(signal.entry),
            taker_fee_rate=taker_fee_rate,
            source="pre_entry_fee_aware_sizing",
        )
        configured_tp_source = _configured_tp_distribution_source(
            signal_percentages=list(signal_tp_percents or []),
            tp_mode=user_settings.tp_mode,
        )

        order_type = (
            "market"
            if str(getattr(signal, "order_type", "LIMIT")).upper() == "MARKET"
            else entry_order_type(exchange)
        )
        is_limit_entry = str(order_type).lower() == "limit"
        # v1.6.57: BingX enforces clientOrderID uniqueness.  Re-sending the
        # same signal after a safe unlock/redeploy must produce a fresh entry
        # client id instead of reusing the old signal-hash based id that BingX
        # rejects with code=101400.  Keep the id stable within one execution by
        # using the trade_group_id when available, and fall back to a single
        # monotonic timestamp captured before the write.
        entry_attempt_key = str(trade_group_id or int(time.time() * 1000))
        entry_cid = _stable_client_id(
            "avc-open", sig_hash, user_id, f"entry:{entry_attempt_key}"
        )
        result_payload["entry_client_order_id_v1"] = {
            "scope": "per_execution",
            "attempt_key": entry_attempt_key,
        }
        limit_context: dict[str, Any] = {}
        limit_context_task: asyncio.Task[dict[str, Any]] | None = None
        if is_limit_entry:
            # This price read is informational only. Run it beside the write so
            # it cannot delay LIMIT placement and attached STOP protection.
            limit_context_task = asyncio.create_task(
                _limit_entry_context(
                    adapter, info.symbol, signal.side.value, signal.entry
                ),
                name=f"limit-context:{user_id}:{info.symbol}",
            )

        actual_entry = signal.entry
        # Initialize with defaults so the error-path db.log_execution can always
        # reference placed_targets/placed_pcts regardless of LIMIT vs MARKET path.
        placed_targets: list = list(selected_targets)
        placed_pcts: list = list(tp_percents)
        placed_tp_source = configured_tp_source
        placed_tp_locked = 0
        entry_intent_execution_id: int | None = None

        async def _persist_execution_record(
            *,
            status_value: str,
            reason_value: str,
            exchange_ids_payload: dict[str, Any],
            targets_value: list[Any],
            pcts_value: list[Any],
            qty_value: float,
            leverage_value: int,
        ) -> int:
            """Persist final execution state, reusing the durable entry intent row.

            v1.6.68 MEXC parity: once a private entry write is about to be sent,
            the exact clientOrderID is already durable in an `opening_intent` row.
            All success/partial/error paths update that same row so a crash after
            BingX accepts the entry cannot leave the bot without a recoverable
            external identity.
            """
            payload_json = json.dumps(
                exchange_ids_payload, ensure_ascii=False, default=str
            )
            if entry_intent_execution_id:
                ok = await db.update_execution_status(
                    entry_intent_execution_id,
                    status_value,
                    reason_value,
                    payload_json,
                    expected_status="opening_intent",
                )
                if ok:
                    submit_statistics_execution_linkage(entry_intent_execution_id)
                    return int(entry_intent_execution_id)

                # G59: a lifecycle/catch-up worker may legitimately advance the
                # durable opening_intent before this original write coroutine
                # persists its final response. A failed opening_intent CAS is
                # therefore not proof that the row is lost. Re-read it and keep
                # that single execution row. Only missing metadata is merged so
                # a newer lifecycle snapshot/status/reason cannot be overwritten.
                current_intent = await db.get_execution_by_id(entry_intent_execution_id)
                if current_intent:
                    def _missing_only_patch(base: Any, patch: Any) -> Any:
                        if not isinstance(base, dict) or not isinstance(patch, dict):
                            return patch if base in (None, "", {}, []) else None
                        out: dict[str, Any] = {}
                        for key, value in patch.items():
                            if key not in base or base.get(key) in (None, "", {}, []):
                                out[key] = value
                            elif isinstance(base.get(key), dict) and isinstance(value, dict):
                                child = _missing_only_patch(base.get(key), value)
                                if isinstance(child, dict) and child:
                                    out[key] = child
                        return out

                    try:
                        current_payload = json.loads(
                            current_intent.get("exchange_order_ids_json") or "{}"
                        )
                    except Exception:
                        current_payload = {}
                    if not isinstance(current_payload, dict):
                        current_payload = {}
                    additive_patch = _missing_only_patch(
                        current_payload, exchange_ids_payload
                    )
                    merged = True
                    if isinstance(additive_patch, dict) and additive_patch:
                        merged = False
                        for _g59_merge_attempt in range(3):
                            latest = await db.get_execution_by_id(
                                entry_intent_execution_id
                            )
                            if not latest:
                                break
                            latest_status = str(latest.get("status") or "")
                            try:
                                latest_payload = json.loads(
                                    latest.get("exchange_order_ids_json") or "{}"
                                )
                            except Exception:
                                latest_payload = {}
                            if not isinstance(latest_payload, dict):
                                latest_payload = {}
                            # Recompute against the newest payload on every CAS
                            # attempt so metadata written by the lifecycle worker
                            # after our first read is never overwritten.
                            latest_patch = _missing_only_patch(
                                latest_payload, exchange_ids_payload
                            )
                            if not isinstance(latest_patch, dict) or not latest_patch:
                                merged = True
                                break
                            if await db.merge_execution_metadata(
                                entry_intent_execution_id,
                                latest_patch,
                                expected_status=latest_status,
                                write_flow_audit_stage="g59_entry_intent_cas_race_merge",
                                write_flow_audit_status=latest_status,
                            ):
                                merged = True
                                break
                    log.warning(
                        "G59_ENTRY_INTENT_CAS_RACE_PRESERVED uid=%s symbol=%s "
                        "intent_execution_id=%s observed_status=%s requested_final_status=%s "
                        "metadata_merged=%s fallback_insert=0",
                        int(user_id),
                        str(signal.symbol).upper(),
                        int(entry_intent_execution_id),
                        str(current_intent.get("status") or ""),
                        status_value,
                        int(bool(merged)),
                    )
                    submit_statistics_execution_linkage(entry_intent_execution_id)
                    return int(entry_intent_execution_id)

                log.error(
                    "G59_ENTRY_INTENT_ROW_MISSING_FALLBACK uid=%s symbol=%s "
                    "intent_execution_id=%s final_status=%s fallback_insert=1",
                    int(user_id),
                    str(signal.symbol).upper(),
                    int(entry_intent_execution_id),
                    status_value,
                )
            execution_id = await db.log_execution(
                trade_group_id=trade_group_id,
                signal_hash=sig_hash,
                user_id=user_id,
                symbol=signal.symbol,
                side=signal.side.value,
                entry=signal.entry,
                stop=signal.stop,
                targets_json=json.dumps(targets_value),
                tp_distribution_json=json.dumps(pcts_value),
                tp_distribution_source=placed_tp_source,
                tp_distribution_locked=placed_tp_locked,
                tp_distribution_version=1,
                risk_percent=user_settings.risk_per_trade_percent,
                **statistics_risk_snapshot,
                qty=qty_value,
                leverage=leverage_value,
                status=status_value,
                reason=reason_value,
                exchange_order_ids_json=payload_json,
            )
            submit_statistics_execution_linkage(execution_id)
            return int(execution_id)

        # Durable pre-entry write intent (MEXC parity).  This is the last
        # checkpoint before the private BingX create-order write.  If the process
        # dies after BingX accepts the order but before the response is saved, the
        # next run still has the exact clientOrderID and same-symbol guard blocks
        # duplicate exposure instead of opening a second plan blindly.
        entry_write_intent = {
            "version": 1,
            "exchange": exchange,
            "symbol": info.symbol,
            "side": signal.side.value,
            "order_type": order_type,
            "clientOrderID": entry_cid,
            "planned_qty": float(sizing.qty),
            "planned_entry": float(signal.entry),
            "stop": float(signal.stop),
            "trade_group_id": trade_group_id,
            "attempt_key": entry_attempt_key,
            "created_at_ms": int(time.time() * 1000),
        }
        base_ids["entry_write_intent_v1"] = dict(entry_write_intent)
        result_payload["entry_write_intent_v1"] = dict(entry_write_intent)
        intent_payload = {
            **base_ids,
            "entry": {
                "clientOrderID": entry_cid,
                "clientOrderId": entry_cid,
                "symbol": info.symbol,
                "side": signal.side.value,
                "order_type": order_type,
                "_exchange": exchange,
            },
        }
        _attach_write_flow_audit(
            intent_payload, status="opening_intent", stage="pre_entry_write_intent"
        )
        entry_intent_execution_id = await db.log_execution(
            trade_group_id=trade_group_id,
            signal_hash=sig_hash,
            user_id=user_id,
            symbol=signal.symbol,
            side=signal.side.value,
            entry=signal.entry,
            stop=signal.stop,
            targets_json=json.dumps(placed_targets),
            tp_distribution_json=json.dumps(placed_pcts),
            tp_distribution_source=placed_tp_source,
            tp_distribution_locked=0,
            tp_distribution_version=1,
            risk_percent=user_settings.risk_per_trade_percent,
            **statistics_risk_snapshot,
            qty=sizing.qty,
            leverage=leverage,
            status="opening_intent",
            reason="durable pre-entry write intent before BingX create-order",
            exchange_order_ids_json=json.dumps(
                intent_payload, ensure_ascii=False, default=str
            ),
        )
        submit_statistics_execution_linkage(entry_intent_execution_id)
        _mark_execution_timing_v1_0_4("entry_intent_persisted")
        base_ids["entry_intent_execution_id"] = entry_intent_execution_id
        result_payload["entry_intent_execution_id"] = entry_intent_execution_id

        # Do not attach TP1 as a full-position native TP.
        # MARKET and LIMIT both create/handle partial TP separately, so TP1 cannot
        # accidentally close 100% of the position.
        try:
            entry_order = await adapter.create_entry_order_with_attached_stop(
                symbol=info.symbol,
                side=signal.side.value,
                qty=sizing.qty,
                entry=signal.entry,
                stop=signal.stop,
                order_type=order_type,
                take_profit=None,
                client_id=entry_cid,
            )
            _mark_execution_timing_v1_0_4("entry_write_returned")
        except BaseException:
            if limit_context_task is not None:
                limit_context_task.cancel()
                await asyncio.gather(limit_context_task, return_exceptions=True)
            raise
        if limit_context_task is not None:
            limit_context = await limit_context_task

        # An exchange may return an explicit quantity-step rejection after a write attempt.
        # The adapter retries once with quantity rounded DOWN. If that happened,
        # keep DB/TP sizing aligned with the real submitted quantity.
        tp_qty_step = order_required_qty_step(entry_order, info.qty_step)
        if (
            isinstance(entry_order, dict)
            and entry_order.get("_normalized_quantity")
            and sizing
        ):
            actual_qty = order_normalized_qty(entry_order, sizing.qty)
            if 0 < actual_qty < sizing.qty:
                actual_notional = actual_qty * signal.entry
                # Keep the displayed/stored risk consistent with the fee-aware
                # sizing formula used before submission.  Otherwise a quantity
                # normalized by BingX appeared safer than it really was because
                # the entry/exit fee buffer was omitted here.
                effective_stop_distance = (
                    abs(signal.entry - signal.stop)
                    + (signal.entry + signal.stop) * taker_fee_rate
                )
                actual_risk_usdt = actual_qty * effective_stop_distance
                sizing = replace(
                    sizing,
                    qty=round(actual_qty, 10),
                    risk_usdt=round(actual_risk_usdt, 8),
                    notional=round(actual_notional, 8),
                    required_margin=round(actual_notional / leverage, 8),
                )

        if not is_limit_entry:
            actual_entry = await _try_actual_entry(
                adapter, info.symbol, signal.side.value, signal.entry, entry_order
            )
            # v1.6.18: MARKET entry can slip between the pre-trade price used for
            # sizing (signal.entry) and the real fill (actual_entry). sizing.qty
            # was already fixed and submitted before this point and cannot be
            # changed after the fact, but the risk figure shown to the user and
            # stored for audit must reflect what was actually risked, not the
            # stale pre-trade estimate -- especially when STOP is close and the
            # gap matters most. This mirrors the existing target_risk_usdt vs
            # risk_usdt (qty-step) transparency pattern for a different source
            # of discrepancy: price slippage instead of quantity rounding.
            if sizing and actual_entry > 0:
                result_payload.update(
                    _market_fill_risk_snapshot(
                        sizing=sizing,
                        signal=signal,
                        actual_entry=actual_entry,
                        taker_fee_rate=taker_fee_rate,
                    )
                )
            _mark_execution_timing_v1_0_4("entry_confirmed")
        else:
            _mark_execution_timing_v1_0_4("limit_stop_attached")
        if entry_intent_execution_id and sizing:
            final_risk_snapshot = _statistics_risk_snapshot(
                sizing=sizing,
                signal=signal,
                entry_price=(
                    float(actual_entry)
                    if not is_limit_entry
                    else float(signal.entry)
                ),
                taker_fee_rate=taker_fee_rate,
                source=(
                    "market_fill_actual"
                    if not is_limit_entry
                    else "limit_executable_after_normalization"
                ),
            )
            statistics_risk_snapshot = dict(final_risk_snapshot)
            try:
                updated_snapshot = await db.update_execution_statistics_snapshot(
                    int(entry_intent_execution_id),
                    **final_risk_snapshot,
                    qty=float(sizing.qty),
                )
                if updated_snapshot:
                    submit_statistics_execution_linkage(entry_intent_execution_id)
                else:
                    log.warning(
                        "STATISTICS_RISK_SNAPSHOT_UPDATE_MISSED execution_id=%s uid=%s symbol=%s",
                        int(entry_intent_execution_id),
                        int(user_id),
                        str(signal.symbol).upper(),
                    )
            except Exception as snapshot_exc:
                # Statistics remain fail-open: trading/protection must not wait
                # for a reporting-only update.
                log.exception(
                    "STATISTICS_RISK_SNAPSHOT_UPDATE_FAILED execution_id=%s uid=%s symbol=%s error=%s",
                    int(entry_intent_execution_id),
                    int(user_id),
                    str(signal.symbol).upper(),
                    type(snapshot_exc).__name__,
                )

        # Notifications and downstream recovery must display the prices that were
        # actually normalized/submitted to BingX, not the raw text from Telegram.
        result_payload["actual_entry"] = actual_entry
        result_payload["actual_stop"] = signal.stop

        tp_policy = build_policy(
            targets=selected_targets,
            pcts=tp_percents,
            qty_step=tp_qty_step,
            min_rr=float(settings.MIN_TP_RR),
        )
        base_ids.update({
            "exchange": exchange,
            "trade_group_id": trade_group_id,
            "entry": entry_order,
            "tp": tp_orders,
            "post_fill_required": is_limit_entry,
            POLICY_KEY: tp_policy,
            "be_trigger_tp_index": int(
                getattr(user_settings, "be_trigger_tp_index", 1) or 0
            ),
            "actual_entry": actual_entry,
            "signal_entry": signal.entry,
            "requested_leverage": int(requested_leverage),
            "effective_leverage": int(leverage),
            "leverage_warning": result_payload.get("leverage_warning"),
            "limit_context": limit_context if is_limit_entry else {},
        })
        if not is_limit_entry:
            base_ids["market_fill_risk_v1"] = {
                key: result_payload[key]
                for key in (
                    "actual_entry",
                    "signal_entry",
                    "stop",
                    "qty",
                    "target_risk_usdt",
                    "pretrade_executable_risk_usdt",
                    "realized_risk_usdt",
                    "realized_risk_percent",
                    "market_slippage_percent",
                    "taker_fee_rate",
                )
                if key in result_payload
            }
        if is_limit_entry:
            base_ids[LIMIT_POLICY_KEY] = build_limit_policy(
                ttl_hours=getattr(user_settings, "limit_ttl_hours", 24),
                tp_mode=getattr(user_settings, "limit_tp_invalidation_mode", "half"),
                targets=selected_targets,
                preset=getattr(user_settings, "limit_policy_preset", "balanced"),
            )
        else:
            try:
                market_stop_audit = await _ensure_market_stop_before_tp(
                    adapter,
                    symbol=info.symbol,
                    side=signal.side.value,
                    stop=signal.stop,
                    qty=sizing.qty,
                    entry_order=entry_order if isinstance(entry_order, dict) else None,
                    sig_hash=sig_hash,
                    user_id=user_id,
                    trade_group_id=trade_group_id,
                    result_payload=result_payload,
                )
                base_ids["market_post_fill_stop_v1"] = market_stop_audit
                _mark_execution_timing_v1_0_4("stop_confirmed")
            except BingxMarketProtectionError:
                raise
            except Exception as stop_exc:
                base_ids["market_post_fill_stop_error_v1"] = {
                    "type": type(stop_exc).__name__,
                    "message": str(stop_exc),
                    "entry": entry_order,
                }
                result_payload["market_post_fill_stop_error_v1"] = base_ids["market_post_fill_stop_error_v1"]
                raise RuntimeError(
                    "MARKET entry submitted but protective STOP was not confirmed before TP placement; "
                    f"TP blocked: {type(stop_exc).__name__}: {stop_exc}"
                ) from stop_exc

        execution_status = "pending_limit" if is_limit_entry else "opened"
        reason_text = (
            "LIMIT entry: STOP установлен, TP будут выставлены после фактического fill"
            if is_limit_entry
            else ""
        )
        if is_limit_entry and bool(
            isinstance(entry_order, dict)
            and entry_order.get("_write_ambiguous_unresolved")
        ):
            reason_text = (
                "Результат создания LIMIT на BingX пока неизвестен после транспортного "
                "сбоя. Точный externalOid сохранён; повторная отправка запрещена, "
                "автоматическая сверка активна. Не отправляйте сигнал повторно."
            )
            result_payload["limit_write_ambiguous_unresolved"] = True

        if not is_limit_entry:
            # build_tp_plan decides how many TPs fit given sizing.qty and qty_step:
            # - If enough lots for all TPs: uses configured SMART/EQUAL proportions.
            # - If too few lots: takes the first N closest targets with 1 step each
            #   so every TP gets at least one executable lot and no lots are wasted.
            tp_plan, tp_mode = build_tp_plan(
                sizing.qty,
                tp_qty_step,
                selected_targets,
                tp_percents,
                entry=signal.entry,
                stop=signal.stop,
                min_rr=float(settings.MIN_TP_RR),
            )
            if tp_mode == "trim":
                log.info(
                    "TP plan trimmed by lot size: qty=%s step=%s viable=%d placed=%d",
                    sizing.qty,
                    tp_qty_step,
                    len(selected_targets),
                    len(tp_plan),
                )
                result_payload["tp_trimmed"] = {
                    "requested": len(selected_targets),
                    "placed": len(tp_plan),
                    "qty": sizing.qty,
                    "step": tp_qty_step,
                }

            base_ids[SNAPSHOT_KEY] = build_snapshot_from_plan(
                total_qty=sizing.qty,
                qty_step=tp_qty_step,
                targets=list(selected_targets),
                plan=list(tp_plan),
                mode=tp_mode,
                entry=float(actual_entry or signal.entry),
                stop=float(signal.stop),
                min_rr=float(tp_policy.get("min_rr") or 0.0),
                source="market_terminal_fill",
                entry_order_id=_exchange_order_id_from_payload(entry_order),
                entry_state=3,
                position_id=(
                    clean_exchange_id(
                        (entry_order.get("_market_order_detail") or {}).get(
                            "positionId"
                        )
                    )
                    if isinstance(entry_order, dict)
                    else ""
                ),
            )
            market_plan_items = snapshot_items(base_ids[SNAPSHOT_KEY])
            if not market_plan_items:
                raise RuntimeError("Immutable MARKET TP plan is empty or invalid")
            _mark_execution_timing_v1_0_4("tp_plan_ready")

            market_position_id = clean_exchange_id(
                (base_ids.get("market_post_fill_stop_v1") or {}).get("positionId")
                if isinstance(base_ids.get("market_post_fill_stop_v1"), dict)
                else ""
            )

            # Extract actual placed targets and proportional pcts from the exact
            # immutable plan. Original signal indexes remain stable even when
            # MIN_TP_RR filters earlier targets (for example TP4-TP6 only).
            placed_targets = [float(item["price"]) for item in market_plan_items]
            placed_qtys = [float(item["qty"]) for item in market_plan_items]
            placed_total = sum(placed_qtys)
            placed_pcts = (
                [round(q / placed_total * 100.0, 6) for q in placed_qtys]
                if placed_total > 0
                else []
            )
            placed_tp_source = "market_rounded_plan"
            placed_tp_locked = 1
            if entry_intent_execution_id:
                try:
                    tp_locked = await db.finalize_execution_tp_distribution(
                        int(entry_intent_execution_id),
                        targets_json=json.dumps(placed_targets),
                        tp_distribution_json=json.dumps(placed_pcts),
                        source=placed_tp_source,
                        version=1,
                    )
                    if tp_locked:
                        submit_statistics_execution_linkage(entry_intent_execution_id)
                    else:
                        log.error(
                            "STATISTICS_TP_DISTRIBUTION_LOCK_REJECTED execution_id=%s uid=%s symbol=%s",
                            int(entry_intent_execution_id),
                            int(user_id),
                            str(signal.symbol).upper(),
                        )
                except Exception as tp_snapshot_exc:
                    log.exception(
                        "STATISTICS_TP_DISTRIBUTION_LOCK_FAILED execution_id=%s uid=%s symbol=%s error=%s",
                        int(entry_intent_execution_id),
                        int(user_id),
                        str(signal.symbol).upper(),
                        type(tp_snapshot_exc).__name__,
                    )

            tp_parallel_limit = max(
                1, int(getattr(get_settings(), "TP_PARALLEL_LIMIT", 5))
            )
            if exchange == "bingx":
                tp_parallel_limit = min(tp_parallel_limit, 4)

            if bool(getattr(settings, "MARKET_TP_BACKGROUND_ENABLED", False)) and exchange == "bingx":
                base_ids["tp_background_v1_0_6a"] = {
                    "enabled": True,
                    "state": "queued",
                    "planned": len(market_plan_items),
                    "confirmed": 0,
                    "durable_recovery_v1_0_6b": True,
                    "safety": "entry_and_stop_confirmed_before_background_tp",
                }
                result_payload["protected_pending_tp"] = True
                result_payload["background_tp_enabled_v1_0_6a"] = True
                result_payload["tp_background_v1_0_6a"] = dict(base_ids["tp_background_v1_0_6a"])
                await db.mark_duplicate(sig_hash, source_chat_id, signal.signal_id, user_id)
                _mark_execution_timing_v1_0_4("result_ready")
                _attach_write_flow_audit(
                    base_ids,
                    status="protected",
                    stage="market_stop_confirmed_background_tp_queued_v1_0_6b",
                )
                protected_execution_id = await _persist_execution_record(
                    status_value="protected",
                    reason_value=(
                        "MARKET entry and STOP confirmed; TP placement queued in "
                        "durable background task v1.0.6b"
                    ),
                    exchange_ids_payload=base_ids,
                    targets_value=placed_targets,
                    pcts_value=placed_pcts,
                    qty_value=sizing.qty,
                    leverage_value=leverage,
                )
                _schedule_background_market_tp_v1_0_6a(
                    execution_id=int(protected_execution_id),
                    api_row=api_row,
                    exchange=exchange,
                    symbol=info.symbol,
                    side=signal.side.value,
                    sig_hash=sig_hash,
                    user_id=user_id,
                    market_position_id=market_position_id,
                    market_plan_items=[dict(item) for item in market_plan_items],
                    tp_parallel_limit=tp_parallel_limit,
                    notify=notify,
                )
                await adapter.close()
                opened_payload = {
                    "sizing": sizing,
                    "targets": placed_targets,
                    "tp_percents": placed_pcts,
                    "pending_limit": False,
                    "protected_pending_tp": True,
                    "background_tp_enabled_v1_0_6a": True,
                    "risk_percent": user_settings.risk_per_trade_percent,
                    "exchange": exchange,
                }
                opened_payload.update(result_payload)
                return ExecutionResult(
                    user_id,
                    "opened",
                    "позиция открыта и STOP подтверждён; TP ставятся фоном",
                    opened_payload,
                )

            # TP placement is exchange-specific below. BingX uses sequential
            # fail-fast for MEXC-parity recovery semantics; non-BingX legacy
            # paths may still use gather with a bounded semaphore.
            tp_parallel_limit = max(
                1, int(getattr(get_settings(), "TP_PARALLEL_LIMIT", 5))
            )
            if exchange == "bingx":
                tp_parallel_limit = min(tp_parallel_limit, 4)
            tp_sem = asyncio.Semaphore(tp_parallel_limit)
            bingx_confirmed_tp_order_ids: set[str] = set()

            async def _place_one_tp(
                sequence: int, idx: int, tp_target: float, tp_quantity: float
            ):
                if exchange == "bingx":
                    # Rate pacing is based on placement sequence, while the
                    # durable TP index remains the original signal index.
                    await asyncio.sleep(((sequence - 1) // 4) * 2.10)
                async with tp_sem:
                    try:
                        order_resp = await adapter.create_take_profit(
                            symbol=info.symbol,
                            side=signal.side.value,
                            qty=tp_quantity,
                            price=tp_target,
                            client_id=_stable_client_id(
                                "avc-tp", sig_hash, user_id, f"tp{idx}"
                            ),
                            position_id=market_position_id or None,
                            owned_order_ids=(
                                sorted(bingx_confirmed_tp_order_ids)
                                if exchange == "bingx"
                                else None
                            ),
                        )
                        return (idx, tp_target, tp_quantity, order_resp, None)
                    except Exception as exc:
                        return (idx, tp_target, tp_quantity, None, exc)

            if exchange == "bingx":
                # MEXC parity: do not fan out all TP writes at once.  Even with
                # adapter locks, parallel tasks can keep placing later TP after
                # an earlier TP has failed.  For BingX we place TP sequentially
                # and fail fast, so recovery sees a clean prefix of confirmed TP.
                tp_results = []
                for sequence, item in enumerate(market_plan_items, start=1):
                    result = await _place_one_tp(
                        sequence,
                        int(item["tp_index"]),
                        float(item["price"]),
                        float(item["qty"]),
                    )
                    tp_results.append(result)
                    if result[4] is not None:
                        break
                    confirmed_tp_id = _exchange_order_id_from_payload(result[3])
                    if confirmed_tp_id:
                        bingx_confirmed_tp_order_ids.add(confirmed_tp_id)
                result_payload["bingx_tp_sequential_parity_v1"] = {
                    "enabled": True,
                    "attempted": len(tp_results),
                    "planned": len(market_plan_items),
                    "fail_fast": any(r[4] is not None for r in tp_results),
                }
            else:
                tp_results = await asyncio.gather(
                    *[
                        _place_one_tp(
                            sequence,
                            int(item["tp_index"]),
                            float(item["price"]),
                            float(item["qty"]),
                        )
                        for sequence, item in enumerate(market_plan_items, start=1)
                    ]
                )
            # Sort by tp_index to preserve order in tp_orders[]
            tp_results.sort(key=lambda r: r[0])
            _mark_execution_timing_v1_0_4("tp_completed")

            failed_tps = [r for r in tp_results if r[4] is not None]
            for i, target, tp_qty, order, exc in tp_results:
                if exc is not None:
                    continue
                actual_tp_qty = order_normalized_qty(order, tp_qty)
                if isinstance(order, dict) and order.get("_reduce_only_fallback"):
                    log.warning(
                        "TP%d for %s %s placed WITHOUT reduceOnly (exchange fallback). "
                        "Monitor position closely.",
                        i,
                        info.symbol,
                        signal.side.value,
                    )
                tp_orders.append(
                    {
                        "tp_index": i,
                        "target": target,
                        "qty": actual_tp_qty,
                        "planned_qty": tp_qty,
                        "order": order,
                        "reduce_only_fallback": bool(
                            isinstance(order, dict)
                            and order.get("_reduce_only_fallback")
                        ),
                    }
                )

            if failed_tps:
                first_failed = failed_tps[0]
                f_idx, f_target, f_qty, _, f_exc = first_failed
                execution_status = "partial_error"
                reason_text = (
                    f"MARKET TP partially completed: {len(failed_tps)} failed, "
                    f"{len(tp_orders)} succeeded. First error on TP{f_idx}: "
                    f"{type(f_exc).__name__}: {f_exc}"
                )
                base_ids["tp"] = tp_orders
                base_ids["tp_error"] = {
                    "failed_count": len(failed_tps),
                    "first_failed": {
                        "tp_index": f_idx,
                        "target": f_target,
                        "qty": f_qty,
                        "error": f"{type(f_exc).__name__}: {f_exc}",
                    },
                    "failed_indices": [r[0] for r in failed_tps],
                    "failed_rows": [
                        {
                            "tp_index": r[0],
                            "target": r[1],
                            "qty": r[2],
                            "error_type": type(r[4]).__name__,
                            "error": str(r[4])[:1000],
                        }
                        for r in failed_tps
                    ],
                    "mexc_parity_fail_fast": bool(exchange == "bingx"),
                }
                await db.mark_duplicate(
                    sig_hash, source_chat_id, signal.signal_id, user_id
                )
                _mark_execution_timing_v1_0_4("result_ready")
                _attach_write_flow_audit(base_ids, status=execution_status, stage="market_tp_partial")
                await _persist_execution_record(
                    status_value=execution_status,
                    reason_value=reason_text,
                    exchange_ids_payload=base_ids,
                    targets_value=placed_targets,
                    pcts_value=placed_pcts,
                    qty_value=sizing.qty,
                    leverage_value=leverage,
                )
                await _maybe_notify(
                    notify,
                    user_id,
                    card(
                        "🟡 <b>TP УСТАНОВЛЕНЫ ЧАСТИЧНО</b>",
                        symbol=signal.symbol,
                        side=signal.side.value,
                        blocks=(
                            [
                                f"✅ <b>Подтверждено TP:</b> {len(tp_orders)}",
                                f"❌ <b>Не подтверждено TP:</b> {len(failed_tps)}",
                                f"⚠️ <b>Первая ошибка:</b> TP{f_idx}",
                            ],
                            [
                                details_line(
                                    f"{type(f_exc).__name__}: {str(f_exc)[:300]}"
                                )
                            ],
                            [
                                "🔄 Бот сохранил частичное состояние и запустит восстановление",
                                "📱 Проверьте позицию и защитные ордера на BingX",
                            ],
                        ),
                    ),
                )
                await adapter.close()
                partial_payload = {
                    "sizing": sizing,
                    "targets": placed_targets,
                    "tp_percents": placed_pcts,
                    "tp_orders": tp_orders,
                    "risk_percent": user_settings.risk_per_trade_percent,
                    "actual_entry": actual_entry,
                    "actual_stop": signal.stop,
                    "exchange": exchange,
                }
                partial_payload.update(result_payload)
                return ExecutionResult(
                    user_id, "partial_error", reason_text, partial_payload
                )

        await db.mark_duplicate(sig_hash, source_chat_id, signal.signal_id, user_id)
        _mark_execution_timing_v1_0_4("result_ready")
        _attach_write_flow_audit(base_ids, status=execution_status, stage="market_or_limit_success")
        await _persist_execution_record(
            status_value=execution_status,
            reason_value=reason_text,
            exchange_ids_payload=base_ids,
            targets_value=placed_targets,
            pcts_value=placed_pcts,
            qty_value=sizing.qty,
            leverage_value=leverage,
        )
        await adapter.close()
        opened_payload = {
            "sizing": sizing,
            "targets": placed_targets,
            "tp_percents": placed_pcts,
            "pending_limit": is_limit_entry,
            "limit_context": limit_context if is_limit_entry else {},
            "risk_percent": user_settings.risk_per_trade_percent,
            "exchange": exchange,
        }
        opened_payload.update(result_payload)
        return ExecutionResult(user_id, "opened", payload=opened_payload)

    except BingxMarketProtectionError as exc:
        reason = str(exc)
        status = "error" if exc.emergency_close_confirmed else "manual_required"
        base_ids["entry"] = exc.entry_order
        base_ids["post_fill_stop"] = exc.protection_order
        base_ids["emergency_close"] = exc.emergency_close_order
        base_ids["positionId"] = exc.position_id
        result_payload.update(
            {
                "market_protection_failed": True,
                "emergency_close_confirmed": exc.emergency_close_confirmed,
                "opened_qty": exc.opened_qty,
                "actual_entry": exc.actual_entry,
            }
        )
    except SymbolNotSupportedErrors as exc:
        reason = _unsupported_symbol_reason(signal)
        status = "skipped"
        # The pair is unavailable on the selected exchange; do not keep retrying the same
        # channel post until the dedup TTL expires. User can still reset dedup
        # manually after correcting/resending a valid symbol.
        try:
            await db.mark_duplicate(sig_hash, source_chat_id, signal.signal_id, user_id)
        except Exception:
            pass
        base_ids["symbol_unavailable"] = True
        base_ids["error_kind"] = "unsupported_symbol_on_bingx"
        result_payload.update(
            _unsupported_symbol_result_payload(
                signal,
                exc,
                exchange=exchange,
                trade_group_id=trade_group_id,
            )
        )
    except NetworkAmbiguousErrors as exc:
        # An ambiguous private-write outcome is never a harmless terminal error:
        # BingX may have accepted the entry even when transport, gateway response
        # or task cancellation did not prove it. Keep the execution visible to
        # protection/lifecycle workers unless rollback is independently confirmed.
        reason = (
            f"ambiguous {exchange_title(exchange)} private-write outcome unknown: {exc}"
        )
        status = "manual_required"
        try:
            positions = await adapter.fetch_open_positions(
                signal.symbol, signal.side.value.upper()
            )
            actual_pos_qty = sum(
                _f(p.get("size") or p.get("positionAmt"), 0.0) for p in positions
            )
            close_qty = actual_pos_qty if actual_pos_qty > 0 else 0.0
            if close_qty > 0:
                close_result = await adapter.emergency_close_market_confirmed(
                    symbol=signal.symbol,
                    side=signal.side.value,
                    qty=close_qty,
                    client_id=_stable_client_id(
                        "avc-emerg", sig_hash, user_id, "ambiguous"
                    ),
                )
                base_ids["emergency_close"] = close_result
                status = "error"
                reason += (
                    f" | найдена позиция {close_qty}; reduceOnly emergency close "
                    "исполнен и подтверждён"
                )
            else:
                reason += (
                    " | позиция не найдена при первой проверке; запись оставлена "
                    "manual_required из-за возможной задержки BingX"
                )
        except Exception as close_exc:
            reason += (
                " | emergency close не подтверждён, нужна ручная проверка: "
                f"{type(close_exc).__name__}: {close_exc}"
            )
    except Exception as exc:
        permission_quarantine_result = None
        if _is_bingx_api_permission_error(exc):
            permission_quarantine_result = await _quarantine_permission_result(
                user_id=user_id,
                exchange=exchange,
                trade_group_id=trade_group_id,
                exc=exc,
                credential_fingerprint=api_credential_fingerprint,
            )
            if entry_order is None and permission_quarantine_result is not None:
                return permission_quarantine_result
            if permission_quarantine_result is not None:
                result_payload.update(permission_quarantine_result.payload)

        # An unexpected exception after the entry write must never make the
        # execution disappear from protection/lifecycle workers.  The order may
        # already be filled even if a later informational read, TP-plan build,
        # notification, or DB preparation failed.  Keep it active and require a
        # live reconciliation instead of recording a harmless terminal error.
        if entry_order is not None:
            status = "manual_required"
            reason = (
                "unexpected error after entry submission; live position and protection "
                f"must be reconciled: {type(exc).__name__}: {exc}"
            )
            base_ids["post_entry_unexpected_error"] = {
                "type": type(exc).__name__,
                "message": str(exc),
            }
        else:
            log.exception(
                "unexpected BingX pre-entry execution error uid=%s symbol=%s stage=pre_entry",
                int(user_id),
                str(signal.symbol).upper(),
            )
            reason = str(exc)
            status = "error"
            result_payload["pre_entry_unexpected_error"] = {
                "type": type(exc).__name__,
                "message": str(exc),
            }
            # Defense-in-depth fallback: the public ticker guard may have been
            # unavailable before the write. If BingX itself rejects the order by
            # price band (code 2003), retry one public read and render the same
            # explicit blocked-signal card instead of the generic API message.
            if _is_bingx_price_band_rejection(exc):
                try:
                    fallback_current = float(market_entry_hint or 0.0)
                except (TypeError, ValueError, OverflowError):
                    fallback_current = 0.0
                if not math.isfinite(fallback_current) or fallback_current <= 0:
                    try:
                        fallback_current = float(
                            await adapter.fetch_last_price(signal.symbol)
                        )
                    except Exception:
                        fallback_current = 0.0
                fallback_anomaly = detect_signal_price_anomaly(
                    signal.entry,
                    fallback_current,
                    max_price_ratio=float(settings.MAX_SIGNAL_ENTRY_PRICE_RATIO or 0.0),
                )
                if fallback_anomaly is not None:
                    status = "skipped"
                    try:
                        await db.mark_duplicate(
                            sig_hash, source_chat_id, signal.signal_id, user_id
                        )
                    except Exception:
                        log.exception(
                            "failed to persist code=2003 anomaly dedup uid=%s symbol=%s",
                            int(user_id),
                            str(signal.symbol).upper(),
                        )
                    reason = (
                        "BingX отклонила аномальную цену до исполнения; "
                        "сделка не открыта"
                    )
                    result_payload.update(fallback_anomaly.as_payload())
                    result_payload.update(
                        mandatory_trade_warning_payload(
                            "signal_price_anomaly",
                            {"bingx_price_band_rejection": True},
                        )
                    )
    finally:
        # Any attempted entry/rollback may change the live position set. Drop the
        # short shared monitor cache before releasing the opening guard.
        get_global_positions_cache().invalidate(user_id, exchange)
        # Guaranteed: remove from opening registry after ALL paths including early returns
        unregister_opening(user_id, signal.symbol)

    _mark_execution_timing_v1_0_4("result_ready")
    final_ids = {**base_ids, "entry": entry_order, "tp": tp_orders}
    _attach_write_flow_audit(final_ids, status=status, stage="final_error_path")
    if "_persist_execution_record" in locals():
        await _persist_execution_record(
            status_value=status,
            reason_value=reason,
            exchange_ids_payload=final_ids,
            targets_value=locals().get("placed_targets") or selected_targets,
            pcts_value=locals().get("placed_pcts") or tp_percents,
            qty_value=sizing.qty if sizing else 0,
            leverage_value=leverage,
        )
    else:
        fallback_execution_id = await db.log_execution(
            trade_group_id=trade_group_id,
            signal_hash=sig_hash,
            user_id=user_id,
            symbol=signal.symbol,
            side=signal.side.value,
            entry=signal.entry,
            stop=signal.stop,
            targets_json=json.dumps(locals().get("placed_targets") or selected_targets),
            tp_distribution_json=json.dumps(locals().get("placed_pcts") or tp_percents),
            tp_distribution_source=(
                locals().get("placed_tp_source")
                or locals().get("configured_tp_source")
                or "configured_pre_fill"
            ),
            tp_distribution_locked=locals().get("placed_tp_locked", 0),
            tp_distribution_version=1,
            risk_percent=user_settings.risk_per_trade_percent,
            **locals().get("statistics_risk_snapshot", {}),
            qty=sizing.qty if sizing else 0,
            leverage=leverage,
            status=status,
            reason=reason,
            exchange_order_ids_json=json.dumps(
                final_ids,
                ensure_ascii=False,
                default=str,
            ),
        )
        submit_statistics_execution_linkage(fallback_execution_id)
    await adapter.close()
    return ExecutionResult(user_id, status, reason, result_payload)


async def execute_signal_for_user(
    signal: Signal,
    user_id: int,
    *,
    source_chat_id: int | None = None,
    trade_group_id: int | None = None,
    market_entry_hint: float | None = None,
    notify: Optional[NotifyFn] = None,
    dispatch_context: dict[str, Any] | None = None,
) -> ExecutionResult:
    """Safely serialize trade creation for one BingX account.

    Accounts remain fully parallel across users. The lock only prevents two
    signals for the same user from racing through duplicate/risk checks.
    """
    settings = get_settings()
    if not admin_only_trade_user_allowed(user_id, settings):
        log.info(
            "ADMIN_ONLY_TRADE_SUPPRESSED uid=%s symbol=%s",
            int(user_id),
            str(signal.symbol).upper(),
        )
        return ExecutionResult(
            int(user_id),
            "skipped",
            "ADMIN_ONLY_MODE: новые сделки разрешены только администраторам",
            optional_trade_skip_payload("admin_only_mode"),
        )

    raw_signal_symbol = signal.symbol
    market_entry_hint = scale_bingx_1000_market_hint(
        raw_signal_symbol, market_entry_hint
    )
    signal = canonicalize_bingx_1000_signal(signal)
    started = time.monotonic()
    async with _user_trade_lock(user_id):
        # Coordinate entry writes with BE/lifecycle cancel-and-replace actions.
        # The previous registry-only check had a time-of-check/time-of-use race:
        # cleanup could pass the check just before entry registration.
        async with db.symbol_action_lock(user_id, signal.symbol):
            result = await _execute_signal_for_user_unlocked(
                signal,
                user_id,
                source_chat_id=source_chat_id,
                trade_group_id=trade_group_id,
                market_entry_hint=market_entry_hint,
                notify=notify,
                dispatch_context=dispatch_context,
            )
    elapsed_ms = int((time.monotonic() - started) * 1000)
    if dispatch_context:
        dispatch_metrics = dict(result.payload.get("dispatch") or {})
        dispatch_metrics.update(dict(dispatch_context))
        dispatch_metrics["executor_duration_ms"] = elapsed_ms
        dispatch_metrics["signal_to_executor_result_ms"] = (
            int(dispatch_metrics.get("signal_age_at_start_ms", 0) or 0) + elapsed_ms
        )
        result.payload["dispatch"] = dispatch_metrics
        try:
            await db.merge_latest_execution_metadata(
                signal_hash(signal, source_chat_id),
                user_id,
                {"dispatch": dispatch_metrics},
            )
        except Exception:
            # Timing diagnostics must never change a confirmed trade result.
            log.exception(
                "failed to persist dispatch metrics uid=%s symbol=%s",
                int(user_id),
                str(signal.symbol).upper(),
            )
    log_method = log.warning if elapsed_ms >= 8000 else log.info
    log_method(
        "signal execution uid=%s symbol=%s status=%s duration_ms=%s",
        int(user_id),
        str(signal.symbol).upper(),
        str(result.status),
        elapsed_ms,
    )
    return result


# Compatibility alias for legacy tests/modules in the BingX port.
_is_mexc_price_band_rejection = _is_bingx_price_band_rejection
