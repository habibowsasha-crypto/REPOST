from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import math
import re
from typing import Any, Dict, List, Optional


class Side(str, Enum):
    LONG = "long"
    SHORT = "short"


class UserMode(str, Enum):
    OFF = "off"
    PREVIEW = "preview"
    AUTO = "auto"


class TpMode(str, Enum):
    SMART = "smart"
    EQUAL = "equal"
    BELL = "bell"
    ACCELERATION = "acceleration"
    EARLY_FIXATION = "early_fixation"
    MANUAL = "manual"


@dataclass(frozen=True)
class Signal:
    symbol: str
    side: Side
    entry: float
    stop: float
    targets: List[float]
    # LIMIT by default. MARKET means the original VIP signal had no ТВХ/entry;
    # executor must fetch current market price, then size risk from that real price.
    order_type: str = "LIMIT"
    # Optional explicit TP distribution parsed from the signal text.
    # Example: "тп 60.195 50%" / "тп 60.748 50%" => [50, 50].
    # Used only when complete and sums to 100%, otherwise executor falls back
    # to the user's configured TP scheme.
    target_percents: List[float] = field(default_factory=list)
    signal_id: Optional[str] = None
    source_format: str = "unknown"
    raw_text: str = ""

    def validate(self) -> None:
        symbol = str(self.symbol or "").strip().upper()
        if not re.fullmatch(r"[A-Z0-9]{1,30}USDT", symbol):
            raise ValueError("symbol должен быть USDT perpetual")
        order_type = str(self.order_type or "LIMIT").strip().upper()
        if order_type not in {"LIMIT", "MARKET"}:
            raise ValueError("order_type должен быть LIMIT или MARKET")

        if self.side not in {Side.LONG, Side.SHORT}:
            raise ValueError("side должен быть LONG или SHORT")

        numeric_values = [self.entry, self.stop, *self.targets]
        if any(isinstance(v, (bool, str, bytes)) for v in numeric_values):
            raise ValueError("цены сигнала должны быть числами")
        try:
            finite_values = [float(v) for v in numeric_values]
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("цены сигнала должны быть числами") from exc
        if not all(math.isfinite(v) for v in finite_values):
            raise ValueError("цены сигнала должны быть конечными числами")

        entry_value, stop_value, *target_values = finite_values
        if stop_value <= 0:
            raise ValueError("stop должен быть положительным")
        if not self.targets:
            raise ValueError("нужен хотя бы один TP")
        if len(self.targets) > 20:
            raise ValueError("поддерживается максимум 20 TP")

        is_market_without_entry = order_type == "MARKET" and entry_value <= 0
        if not is_market_without_entry and entry_value <= 0:
            raise ValueError("entry должен быть положительным")

        if self.target_percents:
            if any(isinstance(v, (bool, str, bytes)) for v in self.target_percents):
                raise ValueError("проценты TP должны быть числами")
            try:
                pcts = [float(v) for v in self.target_percents]
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError("проценты TP должны быть числами") from exc
            if len(pcts) != len(self.targets):
                raise ValueError("проценты TP должны совпадать с количеством целей")
            if any(not math.isfinite(v) or v <= 0 for v in pcts):
                raise ValueError("проценты TP должны быть конечными и > 0")
            if abs(sum(pcts) - 100.0) > 0.001:
                raise ValueError("проценты TP должны давать 100%")

        if self.side == Side.LONG:
            if any(a >= b for a, b in zip(target_values, target_values[1:])):
                raise ValueError("для LONG цели TP должны идти строго по возрастанию")
            if is_market_without_entry:
                if any(tp <= stop_value for tp in target_values):
                    raise ValueError("для MARKET LONG все TP должны быть выше STOP")
            else:
                if stop_value >= entry_value:
                    raise ValueError("для LONG stop должен быть ниже entry")
                if any(tp <= entry_value for tp in target_values):
                    raise ValueError("для LONG все TP должны быть выше entry")
        if self.side == Side.SHORT:
            if any(a <= b for a, b in zip(target_values, target_values[1:])):
                raise ValueError("для SHORT цели TP должны идти строго по убыванию")
            if is_market_without_entry:
                if any(tp >= stop_value for tp in target_values):
                    raise ValueError("для MARKET SHORT все TP должны быть ниже STOP")
            else:
                if stop_value <= entry_value:
                    raise ValueError("для SHORT stop должен быть выше entry")
                if any(tp >= entry_value for tp in target_values):
                    raise ValueError("для SHORT все TP должны быть ниже entry")


@dataclass
class UserSettings:
    telegram_id: int
    exchange: str = "bingx"
    mode: UserMode = UserMode.PREVIEW
    risk_per_trade_percent: float = 1.0
    daily_risk_limit_percent: float = 10.0
    max_open_trades: int = 10
    max_portfolio_risk_percent: float = 10.0
    # True = confirmed BE trades free risk/open-trade slots.
    exclude_be_trades_from_risk: bool = True
    tp_limit: str = "all"
    tp_mode: TpMode = TpMode.BELL
    be_after_tp1_enabled: bool = True
    # 0 = БУ выключено, 1/2/3 = переносить STOP в БУ после достижения TP1/TP2/TP3.
    # Для сигналов с большим количеством TP это даёт "умное БУ": можно защитить
    # остаток не сразу после TP1, а после TP2 или TP3.
    be_trigger_tp_index: int = 1
    use_signal_tp_percents: bool = False
    # False by default: ordinary "⏭ СИГНАЛ ПРОПУЩЕН" cards are optional.
    # Safety/error/manual-review notifications are classified separately and
    # never depend on this preference.
    skip_trade_notifications_enabled: bool = False
    manual_tp_percents: List[float] = field(default_factory=list)
    # Per-user stale LIMIT policy. Values are snapshotted into every new LIMIT
    # execution, so later menu changes do not rewrite an already-open order.
    limit_ttl_hours: int = 24
    limit_tp_invalidation_mode: str = "half"
    limit_policy_preset: str = "balanced"


@dataclass(frozen=True)
class PositionSizing:
    balance_usdt: float
    # Actual fee-aware loss at STOP after the exchange quantity step has been
    # applied.  Older builds stored the requested budget here, which made a
    # safely rounded-down position look as if it still risked the full percent.
    risk_usdt: float
    qty: float
    notional: float
    required_margin: float
    leverage: int
    # Requested risk budget before quantity-step rounding.  Kept separately so
    # notifications can show both the configured cap and the executable risk.
    target_risk_usdt: float = 0.0


@dataclass
class ExecutionResult:
    user_id: int
    status: str
    reason: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)
