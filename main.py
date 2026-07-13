from __future__ import annotations

import asyncio
import os
import sys

from loguru import logger
from telethon import TelegramClient
from telethon.sessions import StringSession

# Importing handlers registers Telethon callbacks on the shared bot client.
from handlers import *  # noqa: F401,F403
from config import (
    API_HASH,
    API_ID,
    BOT_TOKEN,
    DB_PATH,
    bot,
    conn,
    scheduler,
)
from handlers.dm.dm_handlers import dm_monitor_tasks, restore_dm_tasks
from services.ai_dialog_service import create_ai_tables
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
    for user_id, session_string in sessions:
        client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
        try:
            await client.connect()
            if await client.is_user_authorized():
                logger.info(f"Сессия аккаунта {user_id} авторизована")
            else:
                logger.warning(f"Сессия аккаунта {user_id} больше не авторизована")
        except Exception as exc:
            logger.error(f"Ошибка проверки сессии аккаунта {user_id}: {exc}")
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass


async def setup_scheduler() -> None:
    if not scheduler.running:
        scheduler.start()
    logger.info("📅 Планировщик запущен")


async def shutdown_runtime() -> None:
    for task_id, task in list(dm_monitor_tasks.items()):
        if not task.done():
            task.cancel()
    if dm_monitor_tasks:
        await asyncio.gather(*list(dm_monitor_tasks.values()), return_exceptions=True)

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
    # Ordinary broadcast jobs are not restorable by the legacy scheduler path.
    delete_table()

    logger.info("📱 Запуск бота...")
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
