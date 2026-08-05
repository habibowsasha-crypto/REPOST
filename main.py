"""Entry point for Channel DM Bot."""

from __future__ import annotations

import asyncio
import sys

from loguru import logger

from config import ADMIN_ID_LIST, BOT_TOKEN, DB_PATH, LOG_LEVEL, app_version, bot
from db.schema import close_connection, init_db

# Register Telethon handlers on the shared bot client.
import handlers  # noqa: F401,E402


def configure_logging(*, sink=None) -> None:
    """Configure production logging without Loguru local-variable diagnostics."""
    logger.remove()
    logger.add(
        sys.stderr if sink is None else sink,
        level=LOG_LEVEL,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>"
        ),
        colorize=True,
        backtrace=False,
        diagnose=False,
    )


APOLOGY_SCHEDULER_INTERVAL_SECONDS = 1.0


async def _apology_due_loop() -> None:
    """Lightweight scheduler for due apology and link-help deadlines."""
    from services import dialog_engine

    while True:
        try:
            await dialog_engine.process_due_auto_links()
        except Exception as exc:
            logger.opt(exception=exc).error(
                "apology due loop failed error_type={}", type(exc).__name__
            )
        await asyncio.sleep(APOLOGY_SCHEDULER_INTERVAL_SECONDS)


async def _background_due_loop(monitor_svc) -> None:
    """Run heavier recovery and retention jobs on the historical 20 second cadence."""
    from services import dialog_engine
    from services import spambot as spambot_svc

    while True:
        try:
            await spambot_svc.process_due_checks()
        except Exception as exc:
            logger.opt(exception=exc).error(
                "spambot due loop failed error_type={}", type(exc).__name__
            )
        try:
            await monitor_svc.check_authorization_health()
        except Exception as exc:
            logger.opt(exception=exc).error(
                "account authorization health loop failed error_type={}",
                type(exc).__name__,
            )
        try:
            await dialog_engine.recover_ambiguous_dialog_messages()
        except Exception as exc:
            logger.opt(exception=exc).error(
                "dialog delivery recovery loop failed error_type={}",
                type(exc).__name__,
            )
        try:
            await dialog_engine.recover_pending_incoming_messages(limit=50)
        except Exception as exc:
            logger.opt(exception=exc).error(
                "incoming dialog recovery loop failed error_type={}",
                type(exc).__name__,
            )
        try:
            await dialog_engine.process_due_followups()
        except Exception as exc:
            logger.opt(exception=exc).error(
                "follow-up due loop failed error_type={}", type(exc).__name__
            )
        try:
            from services import retention as retention_svc

            await retention_svc.process_due_telegram_deletions()
            retention_svc.process_due_local_history_purge()
        except Exception as exc:
            logger.opt(exception=exc).error(
                "dialog retention loop failed error_type={}", type(exc).__name__
            )
        try:
            from services import queue as queue_svc

            n = queue_svc.release_stale_claims(older_than_seconds=900)
            if n:
                logger.warning("Released {} stale claimed leads", n)
        except Exception as exc:
            logger.opt(exception=exc).error(
                "stale claims loop failed error_type={}", type(exc).__name__
            )
        await asyncio.sleep(20)


def run() -> None:
    configure_logging()
    logger.info("Channel DM Bot - v{} starting", app_version())
    logger.info("DB_PATH={}", DB_PATH)

    if not ADMIN_ID_LIST:
        logger.warning(
            "ADMIN_ID_LIST is empty - nobody will be able to control the bot"
        )
    else:
        logger.info("Admins configured: {}", ADMIN_ID_LIST)

    init_db()
    logger.info("SQLite schema ready")
    from services import accounts as accounts_svc

    repaired_peerflood = accounts_svc.repair_inflated_peerflood_cooldowns()
    for item in repaired_peerflood:
        logger.warning(
            "Repaired inflated PeerFlood cooldown account={} old_until={} "
            "safe_until={} cleared={}",
            item.get("user_id"),
            item.get("old_until"),
            item.get("safe_until"),
            item.get("cleared"),
        )

    background_tasks: list[asyncio.Task] = []
    try:
        from services.ai_dialog import configured_channel_link

        configured_channel_link()
        logger.info("CHANNEL_LINK configured and validated")
    except Exception as exc:
        logger.error(
            "CHANNEL_LINK is not ready: link messages will not be sent until the "
            "admin variable is corrected ({})",
            exc,
        )

    try:
        bot.start(bot_token=BOT_TOKEN)
        me = bot.loop.run_until_complete(bot.get_me())
        username = getattr(me, "username", None) or me.id
        logger.info("Bot authorized as @{} (id={})", username, me.id)

        from services import dispatcher as dispatcher_svc
        from services import monitor as monitor_svc

        bot.loop.run_until_complete(monitor_svc.start_monitor())
        from services import dialog_engine as dialog_engine_svc

        recovered_outgoing = bot.loop.run_until_complete(
            dialog_engine_svc.recover_ambiguous_dialog_messages()
        )
        if recovered_outgoing:
            logger.warning(
                "Recovered ambiguous dialog deliveries after startup: {}",
                recovered_outgoing,
            )
        recovered_first_dm = bot.loop.run_until_complete(
            dispatcher_svc.recover_ambiguous_first_dms()
        )
        if recovered_first_dm:
            logger.warning(
                "Recovered ambiguous First-DM deliveries after startup: {}",
                recovered_first_dm,
            )
        recovered_incoming = bot.loop.run_until_complete(
            dialog_engine_svc.recover_pending_incoming_messages(
                reset_stale_processing=True,
                stale_after_seconds=1,
            )
        )
        if recovered_incoming:
            logger.warning(
                "Recovered pending incoming dialogs after startup: {}",
                recovered_incoming,
            )
        bot.loop.run_until_complete(dispatcher_svc.ensure_worker_from_runtime())

        background_tasks = [
            bot.loop.create_task(
                _background_due_loop(monitor_svc), name="background-due-loop"
            ),
            bot.loop.create_task(
                _apology_due_loop(), name="apology-due-loop"
            ),
        ]
        logger.info("Monitor + dispatcher + SpamBot + dialog delivery + retention ready")
        bot.run_until_disconnected()
    except KeyboardInterrupt:
        logger.info("Stopped by keyboard interrupt")
    finally:
        pending_background = [task for task in background_tasks if not task.done()]
        for background_task in pending_background:
            background_task.cancel()
            try:
                bot.loop.run_until_complete(
                    asyncio.gather(background_task, return_exceptions=True)
                )
            except Exception as exc:
                logger.debug(
                    "Background due-loop shutdown failed error_type={}",
                    type(exc).__name__,
                )
        try:
            from services import dispatcher as dispatcher_svc

            bot.loop.run_until_complete(dispatcher_svc.stop_worker())
        except Exception as exc:
            logger.debug("Dispatcher shutdown cleanup failed: {}", exc)
        try:
            from services import monitor as monitor_svc

            bot.loop.run_until_complete(monitor_svc.stop_monitor())
        except Exception as exc:
            logger.debug("Monitor shutdown cleanup failed: {}", exc)
        try:
            bot.loop.run_until_complete(bot.disconnect())
        except Exception as exc:
            logger.debug("Bot disconnect cleanup failed: {}", exc)
        close_connection()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    run()
