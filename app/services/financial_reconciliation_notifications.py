"""Durable user notifications for terminal BingX financial reconciliation.

Only terminal financial jobs are eligible.  Claiming is performed in the
database before Telegram delivery, while the existing durable-notification
table supplies the final idempotency barrier and retry path across restarts.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from typing import Any

from app.database import db
from app.services.durable_notifications import NotifyFn, send_or_enqueue
from app.services.financial_reconciliation_models import (
    FINANCIAL_STATUS_AMBIGUOUS,
    FINANCIAL_STATUS_CONFIRMED,
    FINANCIAL_STATUS_PARTIAL,
    FINANCIAL_STATUS_UNAVAILABLE,
)
from app.services.notification_style import card, esc

log = logging.getLogger(__name__)

_CLAIM_STALE_AFTER_SEC = 120.0


def _decimal(value: Any) -> Decimal | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _money(value: Any, *, asset: str = "USDT", signed: bool = True) -> str:
    parsed = _decimal(value)
    if parsed is None:
        return "недоступно"
    if parsed == 0:
        number = "0.00"
    else:
        sign = "+" if signed and parsed > 0 else ("−" if signed and parsed < 0 else "")
        number = f"{sign}{abs(parsed):.8f}".rstrip("0").rstrip(".")
    return f"{number} {esc(str(asset or 'USDT').upper(), limit=16)}"


def _title(close_type: str) -> str:
    return {
        "be_stop": "🛡 <b>ИТОГ СДЕЛКИ · Б/У</b>",
        "stop": "🛑 <b>ИТОГ СДЕЛКИ · STOP</b>",
        "all_tps": "✅ <b>ИТОГ СДЕЛКИ · ВСЕ TP</b>",
    }.get(str(close_type or "").lower(), "🧾 <b>ИТОГ СДЕЛКИ</b>")


def _settlement_asset(job: Mapping[str, Any]) -> str:
    """Derive the strategy settlement asset without trusting incomplete fills."""

    symbol = str(job.get("symbol") or "").strip().upper().replace("-", "")
    return "USDC" if symbol.endswith("USDC") else "USDT"


def format_financial_reconciliation_notification(job: Mapping[str, Any]) -> str:
    """Render only proved values; incomplete results never expose a fake net PnL."""

    status = str(job.get("status") or "").strip().lower()
    settlement_asset = _settlement_asset(job)
    asset = str(job.get("fee_asset") or settlement_asset).strip().upper()
    strategy_gross = _money(
        job.get("strategy_gross_pnl"),
        asset=settlement_asset,
    )

    if status == FINANCIAL_STATUS_CONFIRMED:
        exchange_gross = _decimal(job.get("exchange_gross_pnl"))
        fee = _decimal(job.get("total_trading_fee"))
        net = _decimal(job.get("net_pnl_after_trading_fee"))
        if (
            exchange_gross is None
            or fee is None
            or net is None
            or not job.get("fee_asset")
            or asset != settlement_asset
            or net != exchange_gross + fee
        ):
            # A row must never look confirmed to the user when its money fields
            # are incomplete or arithmetically inconsistent, even if a future
            # migration or manual repair corrupts a terminal result.
            status = FINANCIAL_STATUS_AMBIGUOUS
        else:
            return card(
                _title(str(job.get("close_type") or "")),
                symbol=str(job.get("symbol") or ""),
                side=str(job.get("side") or ""),
                blocks=(
                    [
                        f"📈 <b>Валовой PnL:</b> {_money(exchange_gross, asset=asset)}",
                        f"💸 <b>Комиссии BingX:</b> {_money(fee, asset=asset)}",
                        f"✅ <b>Чистый PnL:</b> {_money(net, asset=asset)}",
                    ],
                    [
                        "🧾 Фактические исполнения подтверждены",
                        f"🔎 Исполнений: <b>{int(job.get('fill_count') or 0)}</b>",
                        "🔄 Фандинг не включён",
                    ],
                ),
            )

    status_line = {
        FINANCIAL_STATUS_PARTIAL: "⚠️ Комиссия подтверждена только частично",
        FINANCIAL_STATUS_AMBIGUOUS: "⚠️ Данные исполнений неоднозначны — комиссия не показана",
        FINANCIAL_STATUS_UNAVAILABLE: "⚠️ Фактическую комиссию подтвердить не удалось",
    }.get(status, "⚠️ Фактическую комиссию подтвердить не удалось")
    return card(
        _title(str(job.get("close_type") or "")),
        symbol=str(job.get("symbol") or ""),
        side=str(job.get("side") or ""),
        blocks=(
            [f"💰 <b>Расчётный PnL:</b> {strategy_gross}"],
            [status_line, "🚫 Чистый PnL не рассчитывается по неполным данным", "🔄 Фандинг не включён"],
        ),
    )


async def process_due_financial_notifications_once(
    notify: NotifyFn | None,
    *,
    limit: int = 20,
) -> int:
    """Claim and deliver terminal summaries without touching trading paths."""

    if notify is None:
        return 0
    rows = await db.claim_due_financial_reconciliation_notifications(
        limit=limit,
        stale_after_sec=_CLAIM_STALE_AFTER_SEC,
    )
    delivered_count = 0
    for row in rows:
        job_id = int(row.get("id") or 0)
        dedup_key = str(row.get("notification_dedup_key") or "").strip()
        try:
            delivered = await send_or_enqueue(
                notify,
                int(row.get("user_id") or 0),
                format_financial_reconciliation_notification(row),
                source="financial_reconciliation",
                event_key=f"execution:{int(row.get('execution_id') or 0)}:financial_final:v1",
                dedup_key_override=dedup_key,
            )
            if delivered:
                await db.set_financial_reconciliation_notification_status(
                    job_id=job_id,
                    status="delivered",
                )
                delivered_count += 1
        except Exception:
            # Leave the durable claim as queued.  It becomes reclaimable after
            # the bounded stale interval, while durable_notifications can retry
            # the exact same key independently.
            log.exception("FINANCIAL_NOTIFICATION_DELIVERY_FAILED job_id=%s", job_id)
    return delivered_count


__all__ = [
    "format_financial_reconciliation_notification",
    "process_due_financial_notifications_once",
]
