from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_DOWN
import math

from app.services.models import PositionSizing, Signal


def _finite_float(value: object, *, label: str) -> float:
    """Parse a finite numeric setting without accepting bool/NaN/Infinity."""
    if isinstance(value, bool):
        raise ValueError(f"{label}: требуется число")
    try:
        val = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{label}: требуется число") from exc
    if not math.isfinite(val):
        raise ValueError(f"{label}: значение должно быть конечным числом")
    return val


def validate_risk_percent(risk_percent: float) -> float:
    val = _finite_float(risk_percent, label="Риск на сделку")
    if val < 0.5 or val > 5.0:
        raise ValueError("Риск разрешён только от 0.5% до 5%")
    return val


def validate_daily_risk_limit_percent(value: float) -> float:
    """0 explicitly means 'no daily risk limit'."""
    val = _finite_float(value, label="Дневной лимит риска")
    if val < 0 or val > 100:
        raise ValueError("Дневной лимит риска: от 0 (без лимита) до 100%")
    return val


def validate_max_portfolio_risk_percent(value: float) -> float:
    """0 explicitly means 'no portfolio risk limit'."""
    val = _finite_float(value, label="Портфельный риск")
    if val < 0 or val > 100:
        raise ValueError("Портфельный риск: от 0 (без лимита) до 100%")
    return val


def validate_max_open_trades(value: int) -> int:
    """Validate an integer trade count; 0 explicitly means 'no limit'."""
    if isinstance(value, bool):
        raise ValueError("Максимум открытых сделок: требуется целое число")
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, ValueError, TypeError, AttributeError) as exc:
        raise ValueError("Максимум открытых сделок: требуется целое число") from exc
    if not parsed.is_finite() or parsed != parsed.to_integral_value():
        raise ValueError("Максимум открытых сделок: требуется целое число от 0 до 100")
    val = int(parsed)
    if val < 0 or val > 100:
        raise ValueError("Максимум открытых сделок: от 0 (без лимита) до 100")
    return val


def calculate_position_size(
    *,
    balance_usdt: float,
    risk_percent: float,
    signal: Signal,
    leverage: int,
    qty_step: float = 0.001,
    min_qty: float = 0.0,
    min_notional: float = 0.0,
    taker_fee_rate: float = 0.0008,
) -> PositionSizing:
    signal.validate()
    risk_percent = validate_risk_percent(risk_percent)
    balance_usdt = _finite_float(balance_usdt, label="Баланс")
    leverage_value = _finite_float(leverage, label="Плечо")
    qty_step = _finite_float(qty_step, label="Шаг количества")
    min_qty = _finite_float(min_qty, label="Минимальное количество")
    min_notional = _finite_float(min_notional, label="Минимальный notional")
    taker_fee_rate = _finite_float(taker_fee_rate, label="Комиссия taker")
    if balance_usdt <= 0:
        raise ValueError("Баланс должен быть положительным")
    if leverage_value <= 0 or leverage_value != int(leverage_value):
        raise ValueError("Плечо должно быть положительным")
    leverage = int(leverage_value)
    if qty_step < 0 or min_qty < 0 or min_notional < 0:
        raise ValueError("Биржевые ограничения не могут быть отрицательными")
    if taker_fee_rate < 0 or taker_fee_rate > 0.05:
        raise ValueError("Комиссия taker должна быть от 0 до 0.05")
    # Use Decimal from the original string representations.  This preserves an
    # exactly valid lot (for example 0.20) without ever snapping a genuinely
    # smaller risk quantity upward.
    balance_d = Decimal(str(balance_usdt))
    risk_percent_d = Decimal(str(risk_percent))
    entry_d = Decimal(str(signal.entry))
    stop_d = Decimal(str(signal.stop))
    risk_usdt_d = balance_d * risk_percent_d / Decimal("100")
    stop_distance_d = abs(entry_d - stop_d)
    if stop_distance_d <= 0:
        raise ValueError("STOP не может совпадать с entry")

    # Fee-aware sizing: commission is charged on the actual entry and STOP
    # notionals. Using entry*2 understated SHORT risk because its STOP is above
    # entry; using entry+stop is exact for both directions.
    fee_rate_d = Decimal(str(taker_fee_rate))
    fee_distance_d = (entry_d + stop_d) * fee_rate_d
    effective_stop_distance_d = stop_distance_d + fee_distance_d

    raw_qty_d = risk_usdt_d / effective_stop_distance_d
    if qty_step > 0:
        step_d = Decimal(str(qty_step))
        qty_d = (raw_qty_d / step_d).to_integral_value(rounding=ROUND_DOWN) * step_d
    else:
        qty_d = raw_qty_d
    qty = round(float(qty_d), 10)
    target_risk_usdt = float(risk_usdt_d)
    # ``qty_d`` is the exact executable quantity after rounding DOWN to the
    # exchange step.  The true loss at STOP can therefore be materially lower
    # than the configured budget (for example one 0.1 SOL lot).
    actual_risk_usdt_d = qty_d * effective_stop_distance_d
    actual_risk_usdt = float(actual_risk_usdt_d)
    if qty <= 0 or qty < min_qty:
        raise ValueError("Размер позиции меньше минимального шага биржи")
    notional = qty * signal.entry
    if min_notional and notional < min_notional:
        raise ValueError("Notional меньше минимального значения биржи")
    required_margin = notional / leverage
    return PositionSizing(
        balance_usdt=balance_usdt,
        risk_usdt=round(actual_risk_usdt, 8),
        qty=qty,
        notional=round(notional, 8),
        required_margin=round(required_margin, 8),
        leverage=int(leverage),
        target_risk_usdt=round(target_risk_usdt, 8),
    )
