from loguru import logger
import os
import sys
from handlers import *
from config import (BOT_TOKEN, conn, bot, API_ID, API_HASH, user_clients, scheduler, cleanup_processed_callbacks)
from utils.database import create_table, delete_table
from utils.database.database import create_dm_tables
from services.ai_dialog_service import create_ai_tables
from handlers.dm.dm_handlers import restore_dm_tasks
from telethon import TelegramClient
from telethon.sessions import StringSession

# Настройка loguru для красивого отображения логов
os.makedirs("logs", exist_ok=True)
logger.remove()  # Удаляем стандартный обработчик

# Добавляем красивый форматированный лог
logger.add(
    sys.stderr,
    level="INFO",
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    colorize=True
)

# Добавляем логирование в файл
logger.add(
    "logs/bot.log",
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    rotation="10 MB",
    retention="30 days",
    compression="zip"
)

# Функция для загрузки сессий из базы данных при запуске бота
async def load_sessions():
    cursor = conn.cursor()
    try:
        # Получаем все сессии из базы данных
        sessions = cursor.execute("SELECT user_id, session_string FROM sessions").fetchall()
        logger.info(f"Загружаю {len(sessions)} сессий из базы данных")
        
        # Создаем директорию для хранения файлов сессий, если её нет
        os.makedirs(".sessions", exist_ok=True)
        
        for user_id, session_string in sessions:
            try:
                # Создаем файл сессии для каждого пользователя
                session_file = f".sessions/user_{user_id}.session"
                
                # Инициализируем клиент с StringSession и сохраняем его в файл
                client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
                await client.connect()
                
                # Проверяем авторизацию
                if await client.is_user_authorized():
                    logger.info(f"Сессия для пользователя {user_id} успешно загружена")
                else:
                    logger.warning(f"Сессия для пользователя {user_id} не авторизована")
                
                # Отключаем клиент
                await client.disconnect()
            except Exception as e:
                logger.error(f"Ошибка при загрузке сессии для пользователя {user_id}: {e}")
    except Exception as e:
        logger.error(f"Ошибка при загрузке сессий: {e}")
    finally:
        cursor.close()

async def setup_scheduler():
    """Настройка и запуск планировщика после старта бота"""
    scheduler.start()
    scheduler.add_job(
        cleanup_processed_callbacks,
        "interval",
        hours=1,  # Очищаем каждый час
        id="cleanup_callbacks"
    )
    logger.info("📅 Планировщик запущен с задачей очистки callback'ов")


if __name__ == "__main__":
    logger.info("🤖 Инициализация бота...")
    create_table()
    create_dm_tables()
    create_ai_tables()
    delete_table()
    logger.info("📱 Запуск бота...")
    bot.start(bot_token=BOT_TOKEN)
    
    # Загружаем сессии при запуске бота
    bot.loop.run_until_complete(load_sessions())
    
    # Запускаем планировщик после старта бота
    bot.loop.run_until_complete(setup_scheduler())
    
    # Восстанавливаем активные DM-задачи
    bot.loop.run_until_complete(restore_dm_tasks())
    
    # Используем только один способ вывода сообщения о запуске
    logger.info("🚀 Бот запущен...")
    
    bot.run_until_disconnected()
    delete_table()
    conn.close()
