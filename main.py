"""Entry point: Channel DM Bot (v1.0.25)."""

from __future__ import annotations

import asyncio
import sys

from loguru import logger

from config import ADMIN_ID_LIST, BOT_TOKEN, DB_PATH, LOG_LEVEL, bot
from db.schema import init_db

# Register Telethon handlers on the shared bot client.
import handlers  # noqa: F401,E402


def configure_logging() -> None:
    logger.remove()
    logger.add(
        sys.stderr,
        level=LOG_LEVEL,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>"
        ),
        colorize=True,
    )


def run() -> None:
    configure_logging()
    logger.info("Channel DM Bot - v1.0.25 starting")
    logger.info("DB_PATH={}", DB_PATH)

    if not ADMIN_ID_LIST:
        logger.warning(
            "ADMIN_ID_LIST is empty - nobody will be able to control the bot"
        )
    else:
        logger.info("Admins configured: {}", ADMIN_ID_LIST)

    init_db()
    logger.info("SQLite schema ready")

    try:
        bot.start(bot_token=BOT_TOKEN)
        me = bot.loop.run_until_complete(bot.get_me())
        username = getattr(me, "username", None) or me.id
        logger.info("Bot authorized as @{} (id={})", username, me.id)

        from services import dispatcher as dispatcher_svc
        from services import monitor as monitor_svc

        bot.loop.run_until_complete(monitor_svc.start_monitor())
        bot.loop.run_until_complete(dispatcher_svc.ensure_worker_from_runtime())

        async def _background_due_loop():
            from services import dialog_engine
            from services import spambot as spambot_svc
            while True:
                try:
                    await spambot_svc.process_due_checks()
                except Exception as exc:
                    logger.exception("spambot due loop: {}", exc)
                try:
                    await dialog_engine.process_due_auto_links()
                except Exception as exc:
                    logger.exception("auto-link due loop: {}", exc)
                try:
                    from services import queue as queue_svc
                    n = queue_svc.release_stale_claims(older_than_seconds=900)
                    if n:
                        logger.warning("Released {} stale claimed leads", n)
                except Exception as exc:
                    logger.exception("stale claims loop: {}", exc)
                await asyncio.sleep(20)

        bot.loop.create_task(_background_due_loop())
        logger.info("Monitor + dispatcher + SpamBot + auto-link scheduler ready")
        bot.run_until_disconnected()
    except KeyboardInterrupt:
        logger.info("Stopped by keyboard interrupt")
    finally:
        try:
            from services import dispatcher as dispatcher_svc

            bot.loop.run_until_complete(dispatcher_svc.stop_worker())
        except Exception:
            pass
        try:
            from services import monitor as monitor_svc

            bot.loop.run_until_complete(monitor_svc.stop_monitor())
        except Exception:
            pass
        try:
            bot.loop.run_until_complete(bot.disconnect())
        except Exception:
            pass
        logger.info("Shutdown complete")


if __name__ == "__main__":
    run()
