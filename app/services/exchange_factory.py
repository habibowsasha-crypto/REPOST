from __future__ import annotations

from typing import Any
import importlib

from app.config import get_settings
from app.exchanges.bingx import (
    BingxAdapter,
    BingxNetworkAmbiguousError,
    BingxSymbolNotSupported,
)
from app.services.encryption import decrypt_text

def _legacy_mexc_error_class(name: str):
    """Return a legacy MEXC error class when the compatibility module exists.

    The runtime adapter is BingX-only, but old tests and recovery paths may still
    raise legacy MEXC-named exceptions.  Keeping the lookup here avoids a direct
    top-level import from app.exchanges.mexc while preserving safe error handling.
    """
    try:
        mexc_adapter = importlib.import_module("app.exchanges.mexc.adapter")
        cls = getattr(mexc_adapter, name, None)
    except Exception:
        cls = None
    return cls if isinstance(cls, type) else None


_LEGACY_MEXC_NETWORK_AMBIGUOUS = _legacy_mexc_error_class("MexcNetworkAmbiguousError")
_LEGACY_MEXC_SYMBOL_NOT_SUPPORTED = _legacy_mexc_error_class("MexcSymbolNotSupported")

NetworkAmbiguousErrors = tuple(
    cls
    for cls in (BingxNetworkAmbiguousError, _LEGACY_MEXC_NETWORK_AMBIGUOUS)
    if cls is not None
)
SymbolNotSupportedErrors = tuple(
    cls
    for cls in (BingxSymbolNotSupported, _LEGACY_MEXC_SYMBOL_NOT_SUPPORTED)
    if cls is not None
)


def exchange_title(exchange: str = "bingx") -> str:
    return "BingX"


def entry_order_type(exchange: str = "bingx") -> str:
    return get_settings().BINGX_ENTRY_ORDER_TYPE.lower().strip()


def build_adapter(api_row: dict[str, Any]):
    """Build the runtime adapter: BingX USDT-M Futures.

    Legacy DB rows/callbacks that still carry exchange=mexc are accepted and
    routed to BingX in this BingX-only build so users do not lose old menu flows.
    """
    settings = get_settings()
    exchange = str(api_row.get("exchange") or "bingx").lower().strip()
    if exchange not in {"bingx", "mexc", "all"}:
        raise ValueError("Эта сборка поддерживает только BingX.")

    api_key = decrypt_text(api_row["api_key_encrypted"], settings.ENCRYPTION_KEY)
    api_secret = decrypt_text(api_row["api_secret_encrypted"], settings.ENCRYPTION_KEY)
    return BingxAdapter(
        api_key,
        api_secret,
        "",
        testnet=bool(api_row.get("testnet", settings.BINGX_VST)),
        timeout_ms=int(
            (
                settings.EXCHANGE_CALL_TIMEOUT_SEC
                or settings.BINGX_REQUEST_TIMEOUT_SECONDS
            )
            * 1000
        ),
    )
