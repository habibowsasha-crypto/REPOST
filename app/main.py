from __future__ import annotations

import asyncio
import logging

from app.config import get_settings
from app.services.railway_diagnostics import log_railway_diagnostics
from app.database.db import close_db, init_db
from app.services.encryption import make_fernet
from app.services.notification_dispatcher import (
    send_queued_private_message,
    start_notification_dispatcher,
    stop_notification_dispatcher,
)
from app.services.execution_dispatcher import (
    start_trade_dispatcher,
    stop_trade_dispatcher,
)
from app.services.signal_executor import stop_background_tp_tasks
from app.services.signal_analytics_ingress import (
    start_signal_analytics_dispatcher,
    stop_signal_analytics_dispatcher,
)
from app.services.financial_reconciliation_worker import (
    start_financial_reconciliation_dispatcher,
    stop_financial_reconciliation_dispatcher,
)


def _run_railway_startup_diagnostics(settings) -> dict | None:
    """Run readiness diagnostics when logging is enabled or strict mode is requested."""

    if not (
        bool(getattr(settings, "RAILWAY_DIAGNOSTICS_ENABLED", False))
        or bool(getattr(settings, "STATISTICS_SHADOW_STRICT_STARTUP", False))
    ):
        return None
    return log_railway_diagnostics(settings)


async def _run_statistics_shadow_database_startup_diagnostics(settings) -> dict | None:
    """Run the DB probe without making non-strict diagnostics a startup dependency."""

    enabled = bool(
        getattr(settings, "STATISTICS_SHADOW_DB_DIAGNOSTICS_ENABLED", False)
    )
    strict = bool(getattr(settings, "STATISTICS_SHADOW_STRICT_STARTUP", False))
    if not (enabled or strict):
        return None

    from app.services.statistics_shadow_database import (
        log_statistics_shadow_database_report,
    )

    try:
        return await log_statistics_shadow_database_report(settings=settings)
    except Exception:
        logging.getLogger(__name__).exception(
            "STATISTICS_SHADOW_DB_DIAGNOSTICS_FAILED strict=%s", strict
        )
        if strict:
            raise
        return None


async def main() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    )
    # httpx INFO includes complete signed BingX URLs.  The adapter already emits
    # sanitized endpoint diagnostics, so suppress transport request lines.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    if not settings.BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан")
    if not settings.ENCRYPTION_KEY:
        raise RuntimeError("ENCRYPTION_KEY не задан")
    try:
        make_fernet(settings.ENCRYPTION_KEY)
    except Exception as exc:
        raise RuntimeError("ENCRYPTION_KEY имеет неверный Fernet-формат") from exc
    _run_railway_startup_diagnostics(settings)
    await init_db()
    await _run_statistics_shadow_database_startup_diagnostics(settings)
    from aiogram import Bot, Dispatcher
    from aiogram.client.default import DefaultBotProperties
    from aiogram.fsm.storage.memory import MemoryStorage
    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
    from app.bot.handlers import router
    from app.bot.admin_only_middleware import AdminOnlyAccessMiddleware

    bot = Bot(settings.BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    # MemoryStorage backs the multi-step /api wizard; FSM state is in-process,
    # so a redeploy aborts in-flight key-entry flows (user can just /api_setup
    # again — no harm done).
    dp = Dispatcher(storage=MemoryStorage())
    admin_only_gate = AdminOnlyAccessMiddleware()
    dp.message.outer_middleware(admin_only_gate)
    dp.callback_query.outer_middleware(admin_only_gate)
    dp.include_router(router)
    from app.services.monitor_workers import start_monitor_workers

    # Start process-wide queues before monitor workers and Telegram polling.
    await start_notification_dispatcher()
    await start_trade_dispatcher()
    await start_signal_analytics_dispatcher()

    def _markup_from_spec(spec):
        if not isinstance(spec, dict):
            return None
        raw_rows = spec.get("inline_keyboard")
        if not isinstance(raw_rows, list):
            return None
        rows = []
        for raw_row in raw_rows[:8]:
            if not isinstance(raw_row, list):
                continue
            buttons = []
            for raw_button in raw_row[:4]:
                if not isinstance(raw_button, dict):
                    continue
                text_value = str(raw_button.get("text") or "")[:64]
                callback_data = str(raw_button.get("callback_data") or "")
                if (
                    not text_value
                    or not callback_data
                    or len(callback_data.encode("utf-8")) > 64
                ):
                    continue
                buttons.append(
                    InlineKeyboardButton(text=text_value, callback_data=callback_data)
                )
            if buttons:
                rows.append(buttons)
        return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None

    async def notify_user(user_id: int, text: str, reply_markup_spec=None):
        outcome = await send_queued_private_message(
            bot,
            user_id,
            text,
            parse_mode="HTML",
            attempts=3,
            log_context="monitor notification",
            reply_markup=_markup_from_spec(reply_markup_spec),
        )
        if not outcome.delivered:
            logging.warning(
                "monitor notification unavailable uid=%s code=%s",
                user_id,
                outcome.code,
            )
            # Lifecycle monitors must receive the real delivery result. Earlier
            # versions returned None here, so TP rows could be marked notified even
            # when Telegram had rejected or dropped the message.
        return outcome

    # Financial reconciliation needs the same confirmed-delivery callback, but
    # remains a separate lowest-priority worker and never runs in trade paths.
    await start_financial_reconciliation_dispatcher(notify=notify_user)

    # Keep strong references to background tasks for the full bot lifetime.

    monitor_tasks = start_monitor_workers(notify=notify_user)
    from app import __version__ as _app_version

    logging.info(
        "ANTILUD VIP CORE v%s started: BingX-only, Telegram polling + monitor workers | admins configured: %s",
        _app_version,
        len(settings.admin_ids),
    )
    try:
        await dp.start_polling(bot)
    finally:
        for task in monitor_tasks:
            task.cancel()
        await asyncio.gather(*monitor_tasks, return_exceptions=True)
        # Stop accepting new entries before closing HTTP/DB resources. Pending
        # in-memory entries are rejected safely instead of surviving a redeploy.
        await stop_trade_dispatcher()
        await stop_background_tp_tasks()
        # Stop producers before the shared Telegram delivery queue.  Otherwise
        # a financial result could be finalized during shutdown after the queue
        # had already rejected new delivery work.
        await stop_financial_reconciliation_dispatcher()
        await stop_signal_analytics_dispatcher()
        await stop_notification_dispatcher()
        from app.exchanges.bingx.adapter import close_shared_http_client

        await close_shared_http_client()
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
