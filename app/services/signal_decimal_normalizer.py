from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import math
from typing import Any

from app.services.models import Signal


@dataclass(frozen=True)
class DecimalNormalizationPreview:
    """Admin-only preview for a uniform decimal-shift correction.

    The object is intentionally diagnostic.  It does not authorize execution by
    itself and it is not shown to ordinary subscribers.  Every numeric level is
    shifted by the same power-of-ten factor and then validated through the
    normal Signal.validate() rules.
    """

    power: int
    multiplier: float
    original_entry: float
    original_stop: float
    original_targets: list[float]
    normalized_entry: float
    normalized_stop: float
    normalized_targets: list[float]
    current_price: float
    deviation_after_percent: float
    reason: str

    @property
    def factor_text(self) -> str:
        if self.power < 0:
            return f"/{10 ** abs(self.power):g}"
        return f"*{10 ** self.power:g}"

    def as_payload(self) -> dict[str, Any]:
        return {
            "decimal_normalization_preview": True,
            "decimal_normalization_power": self.power,
            "decimal_normalization_multiplier": self.multiplier,
            "decimal_normalization_factor_text": self.factor_text,
            "decimal_original_entry": self.original_entry,
            "decimal_original_stop": self.original_stop,
            "decimal_original_targets": list(self.original_targets),
            "decimal_normalized_entry": self.normalized_entry,
            "decimal_normalized_stop": self.normalized_stop,
            "decimal_normalized_targets": list(self.normalized_targets),
            "decimal_current_price": self.current_price,
            "decimal_deviation_after_percent": self.deviation_after_percent,
            "decimal_normalization_reason": self.reason,
            # Explicit visibility marker.  Renderers must treat this payload as
            # admin-private even though it rides together with the skipped result.
            "decimal_normalization_visibility": "admin_only",
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


def _scale(value: float, multiplier: float) -> float:
    try:
        scaled = Decimal(str(float(value))) * Decimal(str(float(multiplier)))
    except (InvalidOperation, ValueError, OverflowError):
        return 0.0
    try:
        result = float(scaled)
    except (ValueError, OverflowError):
        return 0.0
    if not math.isfinite(result) or result <= 0:
        return 0.0
    return result


def _candidate_signal(signal: Signal, multiplier: float) -> Signal | None:
    entry = _scale(signal.entry, multiplier)
    stop = _scale(signal.stop, multiplier)
    targets = [_scale(target, multiplier) for target in signal.targets]
    if entry <= 0 or stop <= 0 or any(target <= 0 for target in targets):
        return None
    candidate = Signal(
        symbol=signal.symbol,
        side=signal.side,
        entry=entry,
        stop=stop,
        targets=targets,
        order_type=signal.order_type,
        target_percents=list(signal.target_percents or []),
        signal_id=signal.signal_id,
        source_format=signal.source_format,
        raw_text=signal.raw_text,
    )
    try:
        candidate.validate()
    except ValueError:
        return None
    return candidate


def preview_decimal_normalization(
    signal: Signal,
    current_price: Any,
    *,
    max_deviation_after_percent: float = 3.0,
    max_power: int = 6,
) -> DecimalNormalizationPreview | None:
    """Return an admin-only decimal-shift preview when exactly one fix is safe.

    The function deliberately never returns a modified executable Signal.  It is
    only a diagnostic suggestion for admins.  A future AUTO_SAFE mode must add a
    separate decision layer and tests before execution is allowed.
    """
    current = _positive_finite(current_price)
    entry = _positive_finite(signal.entry)
    stop = _positive_finite(signal.stop)
    targets = [_positive_finite(target) for target in signal.targets]
    if (
        current <= 0
        or entry <= 0
        or stop <= 0
        or not targets
        or any(t <= 0 for t in targets)
    ):
        return None

    try:
        tolerance = float(max_deviation_after_percent)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(tolerance) or tolerance <= 0:
        return None
    tolerance = min(25.0, max(0.01, tolerance))

    try:
        power_limit = int(max_power)
    except (TypeError, ValueError, OverflowError):
        return None
    power_limit = max(1, min(8, power_limit))

    valid: list[DecimalNormalizationPreview] = []
    for power in range(-power_limit, power_limit + 1):
        if power == 0:
            continue
        multiplier = 10.0**power
        candidate = _candidate_signal(signal, multiplier)
        if candidate is None:
            continue
        deviation = (float(candidate.entry) - current) / current * 100.0
        if abs(deviation) > tolerance:
            continue
        valid.append(
            DecimalNormalizationPreview(
                power=power,
                multiplier=multiplier,
                original_entry=entry,
                original_stop=stop,
                original_targets=[float(t) for t in targets],
                normalized_entry=float(candidate.entry),
                normalized_stop=float(candidate.stop),
                normalized_targets=[float(t) for t in candidate.targets],
                current_price=current,
                deviation_after_percent=deviation,
                reason="uniform_power_of_ten_shift",
            )
        )

    if len(valid) != 1:
        return None
    return valid[0]


def decimal_normalization_preview_payload(
    signal: Signal,
    current_price: Any,
    *,
    enabled: bool = True,
    max_deviation_after_percent: float = 3.0,
    max_power: int = 6,
) -> dict[str, Any]:
    if not enabled:
        return {}
    preview = preview_decimal_normalization(
        signal,
        current_price,
        max_deviation_after_percent=max_deviation_after_percent,
        max_power=max_power,
    )
    return preview.as_payload() if preview is not None else {}
