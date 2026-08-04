"""Runtime configuration loaded from environment variables."""

from __future__ import annotations

import os
from typing import List

from decouple import config
from telethon import TelegramClient

# ---------------------------------------------------------------------------
# Telegram core
# ---------------------------------------------------------------------------
API_ID: int = int(config("API_ID"))
API_HASH: str = config("API_HASH")
BOT_TOKEN: str = config("BOT_TOKEN")

_raw_admins = config("ADMIN_ID_LIST", default="")
ADMIN_ID_LIST: List[int] = [
    int(part.strip())
    for part in str(_raw_admins).split(",")
    if part.strip().isdigit()
]

# ---------------------------------------------------------------------------
# Paths (Railway Volume-friendly; local defaults work for smoke tests)
# ---------------------------------------------------------------------------
DB_PATH: str = config("DB_PATH", default="data/bot.db")
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

# ---------------------------------------------------------------------------
# Channel promo
# ---------------------------------------------------------------------------
CHANNEL_LINK: str = config("CHANNEL_LINK", default="").strip()
CHANNEL_PITCH: str = config(
    "CHANNEL_PITCH",
    default="Бесплатный канал: посты из VIP-каналов копируются моментально, платить не нужно",
).strip()

# ---------------------------------------------------------------------------
# AI
# ---------------------------------------------------------------------------
OPENAI_API_KEY: str = config("OPENAI_API_KEY", default="").strip()
AI_MODEL: str = config("AI_MODEL", default="gpt-4o-mini").strip() or "gpt-4o-mini"
AI_DM_ENABLED: bool = config("AI_DM_ENABLED", default="true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}

# ---------------------------------------------------------------------------
# Pacing (seconds unless noted)
# ---------------------------------------------------------------------------
DM_ACCOUNT_INTERVAL_MIN: int = int(config("DM_ACCOUNT_INTERVAL_MIN", default="600"))
DM_ACCOUNT_INTERVAL_MAX: int = int(config("DM_ACCOUNT_INTERVAL_MAX", default="900"))
DM_GLOBAL_SPACING_MIN: int = int(config("DM_GLOBAL_SPACING_MIN", default="90"))
DM_GLOBAL_SPACING_MAX: int = int(config("DM_GLOBAL_SPACING_MAX", default="180"))
DM_DAILY_LIMIT_PER_ACCOUNT: int = int(config("DM_DAILY_LIMIT_PER_ACCOUNT", default="45"))
AI_REPLY_DELAY_MIN: int = int(config("AI_REPLY_DELAY_MIN", default="30"))
AI_REPLY_DELAY_MAX: int = int(config("AI_REPLY_DELAY_MAX", default="90"))
AI_AUTO_LINK_DELAY_MIN: int = int(config("AI_AUTO_LINK_DELAY_MIN", default="60"))
AI_AUTO_LINK_DELAY_MAX: int = int(config("AI_AUTO_LINK_DELAY_MAX", default="120"))
PEER_FLOOD_MIN_COOLDOWN_MINUTES: int = int(
    config("PEER_FLOOD_MIN_COOLDOWN_MINUTES", default="30")
)
SPAMBOT_AUTO_RESUME: bool = config("SPAMBOT_AUTO_RESUME", default="true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
FLOODWAIT_EXTRA_SECONDS: int = int(config("FLOODWAIT_EXTRA_SECONDS", default="45"))

LOG_LEVEL: str = config("LOG_LEVEL", default="INFO").upper()

# Shared Telethon bot client (handlers import this singleton).
bot: TelegramClient = TelegramClient(BOT_SESSION_PATH, API_ID, API_HASH)


def is_admin(user_id: int | None) -> bool:
    """Return True if user_id is in the configured admin list."""
    if user_id is None:
        return False
    return int(user_id) in ADMIN_ID_LIST


def app_version() -> str:
    """Read VERSION file next to the package, fallback to 1.0.0."""
    import pathlib
    for candidate in (
        pathlib.Path(__file__).resolve().parent / "VERSION",
        pathlib.Path("VERSION"),
    ):
        try:
            v = candidate.read_text(encoding="utf-8").strip()
            if v:
                return v
        except Exception:
            continue
    return "1.0.0"
