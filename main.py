from __future__ import annotations

import asyncio
import os
import sys

from loguru import logger

# Importing handlers registers Telethon callbacks on the shared bot client.
from handlers import *  # noqa: F401,F403
from config import (
    BOT_TOKEN,
    DB_PATH,
    bot,
    conn,
    scheduler,
)
from handlers.dm.dm_handlers import (
    dm_account_dispatcher_tasks,
    dm_monitor_tasks,
    restore_dm_tasks,
    set_dm_runtime_shutting_down,
)
from services.account_profiles import refresh_stale_account_profiles
from services.ai_dialog_service import create_ai_tables
from services.dm_contact_analytics import create_contact_tables, expire_stale_dialogs
from services.dm_task_queue import recover_stale_queue
from utils.database import create_table, delete_table
from utils.database.database import create_dm_tables


def configure_logging() -> None:
    db_dir = os.path.dirname(DB_PATH)
    log_dir = os.path.join(db_dir, "logs") if db_dir else "logs"
    os.makedirs(log_dir, exist_ok=True)

    logger.remove()
    logger.add(
        sys.stderr,
        level="INFO",
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>"
        ),
        colorize=True,
    )
    logger.add(
        os.path.join(log_dir, "bot.log"),
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        rotation="10 MB",
        retention="30 days",
        compression="zip",
    )


async def validate_saved_sessions() -> None:
    cursor = conn.cursor()
    try:
        sessions = cursor.execute(
            "SELECT user_id, session_string FROM sessions"
        ).fetchall()
    finally:
        cursor.close()

    logger.info(f"Проверяю {len(sessions)} сохранённых сессий")
    if not sessions:
        return
    updated, failed, skipped = await refresh_stale_account_profiles(
        [(int(user_id), str(session_string)) for user_id, session_string in sessions],
        force=True,
        concurrency=3,
        timeout_seconds=10.0,
    )
    logger.info(
        "Проверка сессий завершена: "
        f"обновлено={updated}, ошибок/неавторизованных={failed}, пропущено={skipped}"
    )


async def _run_dialog_expiry_job() -> None:
    """Run lifecycle expiry on the bot event loop, not in a worker thread."""
    try:
        expire_stale_dialogs()
    except Exception as exc:
        logger.exception(f"[DM analytics] dialog expiry job failed: {exc}")




async def _run_queue_recovery_job() -> None:
    """Recover only genuinely stale queue claims during normal runtime."""
    try:
        result = recover_stale_queue()
        if result["claimed_recovered"] or result["sending_uncertain"]:
            logger.warning(f"[DM queue] periodic recovery: {result}")
    except Exception as exc:
        logger.exception(f"[DM queue] periodic recovery failed: {exc}")


async def setup_scheduler() -> None:
    if scheduler.get_job("dm-queue-recovery") is None:
        scheduler.add_job(
            _run_queue_recovery_job,
            "interval",
            minutes=5,
            id="dm-queue-recovery",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
    if scheduler.get_job("dm-dialog-expiry") is None:
        scheduler.add_job(
            _run_dialog_expiry_job,
            "interval",
            hours=1,
            id="dm-dialog-expiry",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
    if not scheduler.running:
        scheduler.start()
    logger.info("📅 Планировщик запущен")


async def shutdown_runtime() -> None:
    set_dm_runtime_shutting_down(True)
    for task in list(dm_monitor_tasks.values()):
        if not task.done():
            task.cancel()
    for task in list(dm_account_dispatcher_tasks.values()):
        if not task.done():
            task.cancel()
    runtime_tasks = [*dm_monitor_tasks.values(), *dm_account_dispatcher_tasks.values()]
    if runtime_tasks:
        await asyncio.gather(*runtime_tasks, return_exceptions=True)

    if scheduler.running:
        scheduler.shutdown(wait=False)
    delete_table()
    conn.close()


def run() -> None:
    configure_logging()
    logger.info("🤖 Инициализация бота...")
    create_table()
    create_dm_tables()
    create_ai_tables()
    create_contact_tables()
    # Ordinary broadcast jobs are not restorable by the legacy scheduler path.
    delete_table()

    logger.info("📱 Запуск бота...")
    set_dm_runtime_shutting_down(False)
    try:
        bot.start(bot_token=BOT_TOKEN)
        bot.loop.run_until_complete(validate_saved_sessions())
        bot.loop.run_until_complete(setup_scheduler())
        bot.loop.run_until_complete(restore_dm_tasks())
        logger.info("🚀 Бот запущен")
        bot.run_until_disconnected()
    finally:
        bot.loop.run_until_complete(shutdown_runtime())


if __name__ == "__main__":
    run()
