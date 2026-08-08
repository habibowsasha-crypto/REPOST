from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import math
import sys
from typing import Any


@dataclass(frozen=True)
class SignalPriceAnomaly:
    signal_entry: float
    current_price: float
    signed_deviation_percent: float
    price_ratio: float
    direction: str
    suggested_entry: float | None = None
    decimal_shift_power: int | None = None

    def as_payload(self) -> dict[str, Any]:
        return {
            "signal_price_anomaly": True,
            "signal_entry": self.signal_entry,
            "current_price": self.current_price,
            "price_deviation_percent": self.signed_deviation_percent,
            "price_ratio": self.price_ratio,
            "price_direction": self.direction,
            "suggested_entry": self.suggested_entry,
            "decimal_shift_power": self.decimal_shift_power,
        }


def _positive_finite(value: Any) -> float:
    if isinstance(value, bool):
        return 0.0
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    if not math.isfinite(parsed) or parsed <= 0:
        return 0.0
    return parsed


def _decimal_shift_suggestion(
    signal_entry: float,
    current_price: float,
    *,
    tolerance_percent: float = 5.0,
) -> tuple[float | None, int | None]:
    """Suggest only an obvious power-of-ten correction.

    The helper never edits a signal. It merely identifies a possible decimal
    shift when multiplying or dividing the entry by 10**n brings it within a
    narrow tolerance of the current market price.
    """
    best: tuple[float, float, int] | None = None
    for power in range(1, 9):
        factor = 10.0**power
        for signed_power, candidate in (
            (-power, signal_entry / factor),
            (power, signal_entry * factor),
        ):
            if not math.isfinite(candidate) or candidate <= 0:
                continue
            distance_percent = abs(candidate - current_price) / current_price * 100.0
            if best is None or distance_percent < best[0]:
                best = (distance_percent, candidate, signed_power)
    if best is None or best[0] > max(0.0, float(tolerance_percent)):
        return None, None
    return best[1], best[2]


def detect_signal_price_anomaly(
    signal_entry: Any,
    current_price: Any,
    *,
    max_price_ratio: float = 5.0,
    suggestion_tolerance_percent: float = 5.0,
) -> SignalPriceAnomaly | None:
    """Return a fail-safe anomaly description for an extreme price mismatch.

    Ratio is symmetric: both a signal ten times above and ten times below the
    market produce a ratio of ten. A disabled/non-sensical threshold or an
    unavailable market price does not block execution.
    """
    entry = _positive_finite(signal_entry)
    current = _positive_finite(current_price)
    try:
        threshold = float(max_price_ratio)
    except (TypeError, ValueError, OverflowError):
        return None
    if entry <= 0 or current <= 0 or not math.isfinite(threshold) or threshold <= 1.0:
        return None

    # Compute the comparison in Decimal space. A direct float division can
    # overflow to ``inf`` for two individually finite prices (for example
    # 1e308 versus 1e-308). Treating that overflow as "no anomaly" would fail
    # open exactly for the most dangerous mismatch. The public payload remains
    # finite by saturating diagnostic numbers at the largest representable
    # float; execution decisions are made from the uncapped Decimal ratio.
    try:
        entry_decimal = Decimal(str(entry))
        current_decimal = Decimal(str(current))
        threshold_decimal = Decimal(str(threshold))
        raw_ratio = max(
            entry_decimal / current_decimal,
            current_decimal / entry_decimal,
        )
    except (InvalidOperation, OverflowError, ZeroDivisionError):
        return None
    if raw_ratio < threshold_decimal:
        return None

    max_float_decimal = Decimal(str(sys.float_info.max))
    ratio = float(min(raw_ratio, max_float_decimal))
    try:
        raw_deviation = (
            (entry_decimal - current_decimal) / current_decimal * Decimal("100")
        )
        if raw_deviation > max_float_decimal:
            signed_deviation = sys.float_info.max
        elif raw_deviation < -max_float_decimal:
            signed_deviation = -sys.float_info.max
        else:
            signed_deviation = float(raw_deviation)
    except (InvalidOperation, OverflowError, ZeroDivisionError):
        signed_deviation = (
            sys.float_info.max if entry > current else -sys.float_info.max
        )
    suggested, shift_power = _decimal_shift_suggestion(
        entry,
        current,
        tolerance_percent=suggestion_tolerance_percent,
    )
    return SignalPriceAnomaly(
        signal_entry=entry,
        current_price=current,
        signed_deviation_percent=signed_deviation,
        price_ratio=ratio,
        direction="above" if entry > current else "below",
        suggested_entry=suggested,
        decimal_shift_power=shift_power,
    )
