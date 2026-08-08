"""Narrow BingX symbol aliases that cannot be represented by the generic parser.

The runtime keeps an alphanumeric canonical symbol so existing database keys,
locks, caches and strict model validation remain unchanged.  Only at the BingX
HTTP boundary is the exchange's literal TradFi contract name restored.
"""

from __future__ import annotations

from typing import Any, Final

GOLD_XAU_SIGNAL_SYMBOL: Final[str] = "XAUUSD"
GOLD_XAU_INTERNAL_SYMBOL: Final[str] = "GOLDXAUUSDT"
GOLD_XAU_EXCHANGE_SYMBOL: Final[str] = "GOLD(XAU)-USDT"


def _compact(value: Any) -> str:
    return (
        str(value or "")
        .strip()
        .upper()
        .replace(" ", "")
        .replace("/", "")
        .replace("-", "")
        .replace("_", "")
    )


def canonical_bingx_tradfi_symbol(value: Any) -> str | None:
    """Return the approved internal symbol for the BingX gold contract.

    The allow-list is intentionally exact.  In particular, ``XAUTUSDT`` is not
    an alias because it is the separate Tether Gold instrument.
    """

    compact = _compact(value)
    if compact == GOLD_XAU_SIGNAL_SYMBOL:
        return GOLD_XAU_INTERNAL_SYMBOL
    if compact in {"GOLD(XAU)USDT", GOLD_XAU_INTERNAL_SYMBOL}:
        return GOLD_XAU_INTERNAL_SYMBOL
    return None


def bingx_tradfi_exchange_symbol(value: Any) -> str | None:
    """Return BingX's literal contract symbol for an approved TradFi alias."""

    return (
        GOLD_XAU_EXCHANGE_SYMBOL
        if canonical_bingx_tradfi_symbol(value) == GOLD_XAU_INTERNAL_SYMBOL
        else None
    )
