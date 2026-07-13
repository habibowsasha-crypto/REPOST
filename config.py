import os
import sqlite3
from typing import Dict, List

import telethon.events
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from decouple import config
from telethon import TelegramClient

# Конфиг
API_ID: int = int(config("API_ID"))
API_HASH: str = config("API_HASH")
BOT_TOKEN: str = config("BOT_TOKEN")
ADMIN_ID_LIST: List[int] = [
    int(x.strip()) for x in config("ADMIN_ID_LIST").split(",") if x.strip()
]  # <-- ID разрешенных Telegram-аккаунтов через запятую

# Railway/Volume-ready paths
DB_PATH: str = config("DB_PATH", default="sessions.db")
_db_dir = os.path.dirname(DB_PATH)
if _db_dir:
    os.makedirs(_db_dir, exist_ok=True)

BOT_SESSION_PATH: str = config(
    "BOT_SESSION_PATH",
    default=(os.path.join(_db_dir, "bot") if _db_dir else "bot"),
)
MEDIA_DIR: str = config(
    "MEDIA_DIR",
    default=(os.path.join(_db_dir, "media") if _db_dir else "media"),
)
os.makedirs(MEDIA_DIR, exist_ok=True)

bot: TelegramClient = TelegramClient(BOT_SESSION_PATH, API_ID, API_HASH)
conn: sqlite3.Connection = sqlite3.connect(DB_PATH, timeout=30.0, check_same_thread=False)
conn.execute("PRAGMA busy_timeout = 30000")
try:
    conn.execute("PRAGMA journal_mode = WAL")
except Exception:
    # WAL может быть недоступен на некоторых FS; это не критично.
    pass

# Аннотирование
New_Message = telethon.events.NewMessage
Query = telethon.events.CallbackQuery
callback_query = Query.Event
callback_message = New_Message.Event
__Dict_int_str = Dict[int, str]
__Dict_int_dict = Dict[int, dict]


phone_waiting: Dict[int, bool] = {}  # Список пользователей ожидающие подтверждения телефона

code_waiting: __Dict_int_str = {}
user_states: __Dict_int_str = {}

password_waiting: __Dict_int_dict = {}
broadcast_all_state: __Dict_int_dict = {}
broadcast_solo_state: __Dict_int_dict = {}
broadcast_all_state_account: __Dict_int_dict = {}
user_sessions: __Dict_int_dict = {}

user_sessions_deleting: Dict[int, Dict[str, str]] = {}

user_clients: Dict[int, TelegramClient] = {}
scheduler: AsyncIOScheduler = AsyncIOScheduler()
