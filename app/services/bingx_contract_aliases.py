"""Static BingX contract aliases for selected 1000-token perpetuals.

The VIP sources normally publish per-token symbols and prices (for example
``SHIBUSDT`` at ``0.00000416``), while BingX exposes the corresponding
perpetual as ``1000SHIBUSDT`` priced per 1000 tokens (``0.00416``).

This module is intentionally static.  It does not query BingX, discover new
aliases, or alter any symbol outside the five explicitly approved mappings.
"""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any, Final, Mapping

from app.services.models import Signal

_PRICE_MULTIPLIER: Final[Decimal] = Decimal("1000")

# Exact allow-list approved for this bot.  Keep immutable so runtime code cannot
# accidentally add or remove aliases in memory.
BINGX_1000_CONTRACT_ALIASES: Final[Mapping[str, str]] = MappingProxyType(
    {
        "BONKUSDT": "1000BONKUSDT",
        "PEPEUSDT": "1000PEPEUSDT",
        "CATUSDT": "1000CATUSDT",
        "CHEEMSUSDT": "1000CHEEMSUSDT",
        "SHIBUSDT": "1000SHIBUSDT",
    }
)


def _lookup_symbol(value: Any) -> str:
    """Normalize only for exact alias lookup; do not rewrite unrelated pairs."""

    return (
        str(value or "")
        .strip()
        .upper()
        .replace("/", "")
        .replace("-", "")
        .replace("_", "")
        .replace(" ", "")
    )


def bingx_1000_alias(symbol: Any) -> str | None:
    """Return the approved BingX 1000-contract symbol or ``None``."""

    return BINGX_1000_CONTRACT_ALIASES.get(_lookup_symbol(symbol))


def bingx_1000_price_multiplier(symbol: Any) -> Decimal:
    """Return 1000 only for an approved unprefixed alias, otherwise 1."""

    return _PRICE_MULTIPLIER if bingx_1000_alias(symbol) else Decimal("1")


def _scale_price(value: Any, multiplier: Decimal) -> float:
    if isinstance(value, bool):
        raise ValueError("BingX alias price must be numeric")
    try:
        scaled = Decimal(str(value)) * multiplier
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("BingX alias price must be numeric") from exc
    if not scaled.is_finite():
        raise ValueError("BingX alias price must be finite")
    return float(scaled)


def scale_bingx_1000_market_hint(symbol: Any, value: Any) -> float:
    """Scale a raw per-token market hint when the raw symbol needs an alias.

    A non-positive or invalid hint is preserved as ``0.0`` so the executor can
    perform its normal fresh market-price read.  Already-canonical symbols are
    never scaled again.
    """

    try:
        hint = Decimal(str(value or 0))
    except (InvalidOperation, TypeError, ValueError):
        return 0.0
    if not hint.is_finite() or hint <= 0:
        return 0.0
    return float(hint * bingx_1000_price_multiplier(symbol))


def canonicalize_bingx_1000_signal(signal: Signal) -> Signal:
    """Map one of the five approved symbols and scale every signal price.

    The operation is idempotent: a signal already using ``1000...USDT`` is
    returned unchanged.  TP percentages and all non-price metadata are kept.
    """

    alias = bingx_1000_alias(signal.symbol)
    if not alias:
        return signal

    mapped = replace(
        signal,
        symbol=alias,
        entry=_scale_price(signal.entry, _PRICE_MULTIPLIER),
        stop=_scale_price(signal.stop, _PRICE_MULTIPLIER),
        targets=[_scale_price(tp, _PRICE_MULTIPLIER) for tp in signal.targets],
        target_percents=list(signal.target_percents),
    )
    mapped.validate()
    return mapped
