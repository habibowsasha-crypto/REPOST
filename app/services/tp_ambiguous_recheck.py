from __future__ import annotations

import math
import asyncio
from typing import Any

from app.services.exchange_identity import clean_exchange_id
from app.exchanges.bingx.symbols import canonical_bingx_tradfi_symbol


def _f(value: Any, default: float = 0.0) -> float:
    """Parse a finite non-negative exchange scalar without repairing corruption."""
    try:
        if value in (None, "") or isinstance(value, bool):
            return default
        parsed = float(value)
        return parsed if math.isfinite(parsed) and parsed >= 0 else default
    except (TypeError, ValueError, OverflowError):
        return default


def _s(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _symbol(value: Any) -> str:
    tradfi = canonical_bingx_tradfi_symbol(value)
    if tradfi:
        return tradfi
    text = _s(value).upper()
    for suffix in ("-PERP", "_PERP", "-SWAP", "_SWAP", "_UMCBL"):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
    text = text.replace(":USDT", "")
    return text.replace("/", "").replace(":", "").replace("-", "").replace("_", "")


def _order_client_id(order: dict[str, Any]) -> str:
    for key in (
        "clientAlgoId",
        "clientOrderId",
        "newClientOrderId",
        "clientOid",
        "clientId",
        "client_id",
        "orderClientId",
        "algoClientId",
    ):
        val = _s(order.get(key))
        if val:
            return val
    return ""


def _order_type(order: dict[str, Any]) -> str:
    return _s(
        order.get("type")
        or order.get("planType")
        or order.get("orderType")
        or order.get("algoType")
    ).upper()


def _conditional_plan_id(order: dict[str, Any]) -> str:
    raw = order.get("raw") if isinstance(order.get("raw"), dict) else {}
    data = order.get("data") if isinstance(order.get("data"), dict) else {}
    return clean_exchange_id(
        order.get("stopPlanOrderId")
        or order.get("stopOrderId")
        or order.get("_confirmed_stop_plan_id")
        or raw.get("stopPlanOrderId")
        or raw.get("id")
        or data.get("stopPlanOrderId")
        or data.get("id")
    )


def _order_trigger(order: dict[str, Any]) -> float:
    for key in (
        "takeProfitPrice",
        "triggerPrice",
        "trigger_price",
        "stopPrice",
        "tpTriggerPrice",
        "price",
        "executePrice",
    ):
        val = _f(order.get(key), 0.0)
        if val > 0:
            return val
    return 0.0


def _order_qty(order: dict[str, Any]) -> float:
    for key in ("quantity", "origQty", "size", "qty", "volume", "orderQty"):
        val = _f(order.get(key), 0.0)
        if val > 0:
            return val
    return 0.0


def _is_tp_like(order: dict[str, Any]) -> bool:
    typ = _order_type(order)
    return (
        _f(order.get("takeProfitPrice"), 0.0) > 0
        or "TAKE" in typ
        or "PROFIT" in typ
        or typ == "TP"
    )


def _side_matches(
    order: dict[str, Any], *, wanted_position_side: str, close_side: str
) -> bool:
    position_side = _s(order.get("positionSide")).upper()
    side_value = _s(order.get("side")).upper()
    if position_side and position_side != wanted_position_side:
        return False
    if side_value and side_value not in {wanted_position_side, close_side}:
        return False
    return True


def _price_close(a: float, b: float) -> bool:
    if a <= 0 or b <= 0:
        return False
    return abs(a - b) <= max(1e-12, abs(b) * 0.000001)


def _qty_close(a: float, b: float) -> bool:
    if a <= 0 or b <= 0:
        return False
    return abs(a - b) <= max(1e-10, abs(b) * 0.002)


async def find_tp_order_after_ambiguous_write(
    adapter: Any,
    *,
    symbol: str,
    side: str,
    tp_index: int,
    target: float,
    qty: float,
    client_id: str,
    position_id: str | int | None = None,
    delays: tuple[float, ...] = (2.0, 5.0, 10.0),
) -> dict[str, Any] | None:
    """Verify whether an ambiguous TP write actually reached BingX.

    This prevents blind duplicate TP creation after timeout/5xx while still
    allowing the bot to recover when BingX accepted the request but lost the HTTP
    response. Matching priority:
    1) exact clientAlgoId/client id;
    2) TP-like algo order with matching symbol + trigger price + close side + qty.
    """
    wanted_symbol = _symbol(symbol)
    wanted_client = clean_exchange_id(client_id)[:36]
    wanted_position_id = clean_exchange_id(position_id)
    is_long = _s(side).lower() == "long"
    close_side = "SELL" if is_long else "BUY"

    async def scan() -> dict[str, Any] | None:
        rows: list[dict[str, Any]] = []
        if hasattr(adapter, "fetch_open_algo_orders"):
            try:
                rows.extend(list(await adapter.fetch_open_algo_orders(symbol) or []))
            except Exception:
                pass
        if hasattr(adapter, "fetch_open_orders"):
            try:
                rows.extend(list(await adapter.fetch_open_orders(symbol) or []))
            except Exception:
                pass
        for order in rows:
            if not isinstance(order, dict):
                continue
            osym = _symbol(order.get("symbol"))
            if osym and osym != wanted_symbol:
                continue
            cid = clean_exchange_id(_order_client_id(order))
            if wanted_client and cid and cid == wanted_client:
                row_pid = clean_exchange_id(order.get("positionId"))
                if wanted_position_id and row_pid and row_pid != wanted_position_id:
                    continue
                wanted_position_side = "LONG" if is_long else "SHORT"
                if not _is_tp_like(order) or not _side_matches(
                    order,
                    wanted_position_side=wanted_position_side,
                    close_side=close_side,
                ):
                    continue
                if _conditional_plan_id(order):
                    return order
                    # Fuzzy price/qty adoption is allowed only for an exact positionId.
                    # Without it an unrelated/manual TP at the same price and quantity can
                    # be incorrectly journalled as the bot's ambiguous write.
        if not wanted_position_id:
            return None
        for order in rows:
            if not isinstance(order, dict):
                continue
            osym = _symbol(order.get("symbol"))
            if osym and osym != wanted_symbol:
                continue
            typ = _order_type(order)
            if "TAKE" not in typ and "PROFIT" not in typ and "TP" != typ:
                continue
            if clean_exchange_id(order.get("positionId")) != wanted_position_id:
                continue
            wanted_position_side = "LONG" if is_long else "SHORT"
            # BingX normalises position TP rows as side=long/short and also exposes
            # positionSide=LONG/SHORT. Generic adapters may expose SELL/BUY close
            # direction instead. Accept both representations.
            if not _side_matches(
                order,
                wanted_position_side=wanted_position_side,
                close_side=close_side,
            ):
                continue
            if _price_close(_order_trigger(order), float(target)) and _qty_close(
                _order_qty(order), float(qty)
            ):
                if _conditional_plan_id(order):
                    return order
        return None

    found = await scan()
    if found:
        return found
    for delay in delays:
        await asyncio.sleep(max(0.0, float(delay)))
        found = await scan()
        if found:
            return found
    return None
