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
    default=(
        "Бесплатный канал: софт автоматически почти сразу копирует новые посты "
        "из закрытых VIP-каналов, отдельные доступы покупать не нужно"
    ),
).strip()

# ---------------------------------------------------------------------------
# AI
# ---------------------------------------------------------------------------
OPENAI_API_KEY: str = config("OPENAI_API_KEY", default="").strip()
AI_MODEL: str = config("AI_MODEL", default="gpt-4o-mini").strip() or "gpt-4o-mini"
AI_REQUEST_TIMEOUT_SECONDS: float = max(
    5.0, float(config("AI_REQUEST_TIMEOUT_SECONDS", default="20"))
)
AI_DM_ENABLED: bool = config("AI_DM_ENABLED", default="true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}

# First DM content mode.
# - magnet: reviewed trading questions
# - short_hook: short answer-provoking hooks, including help only with a question
# - legacy: previous neutral openers for instant rollback
FIRST_DM_STYLE: str = config("FIRST_DM_STYLE", default="magnet").strip().lower()
if FIRST_DM_STYLE not in {"magnet", "short_hook", "legacy"}:
    raise RuntimeError(
        "FIRST_DM_STYLE must be exactly 'magnet', 'short_hook' or 'legacy'; "
        f"got {FIRST_DM_STYLE!r}"
    )

# ---------------------------------------------------------------------------
# Pacing (seconds unless noted)
# ---------------------------------------------------------------------------
DM_ACCOUNT_INTERVAL_MIN: int = int(config("DM_ACCOUNT_INTERVAL_MIN", default="120"))
DM_ACCOUNT_INTERVAL_MAX: int = int(config("DM_ACCOUNT_INTERVAL_MAX", default="420"))
DM_GLOBAL_SPACING_MIN: int = int(config("DM_GLOBAL_SPACING_MIN", default="90"))
DM_GLOBAL_SPACING_MAX: int = int(config("DM_GLOBAL_SPACING_MAX", default="180"))
DM_DAILY_LIMIT_PER_ACCOUNT: int = int(config("DM_DAILY_LIMIT_PER_ACCOUNT", default="125"))
AI_REPLY_DELAY_MIN: int = int(config("AI_REPLY_DELAY_MIN", default="20"))
AI_REPLY_DELAY_MAX: int = int(config("AI_REPLY_DELAY_MAX", default="60"))
AI_APOLOGY_DELAY_MIN: int = int(
    config(
        "AI_APOLOGY_DELAY_MIN",
        default=config("AI_AUTO_LINK_DELAY_MIN", default="60"),
    )
)
AI_APOLOGY_DELAY_MAX: int = int(
    config(
        "AI_APOLOGY_DELAY_MAX",
        default=config("AI_AUTO_LINK_DELAY_MAX", default="60"),
    )
)
# Backward-compatible aliases for existing runtime code and old deployments.
AI_AUTO_LINK_DELAY_MIN: int = AI_APOLOGY_DELAY_MIN
AI_AUTO_LINK_DELAY_MAX: int = AI_APOLOGY_DELAY_MAX
PEER_FLOOD_COOLDOWN_MIN_SECONDS: int = int(
    config("PEER_FLOOD_COOLDOWN_MIN_SECONDS", default="60")
)
PEER_FLOOD_COOLDOWN_MAX_SECONDS: int = int(
    config("PEER_FLOOD_COOLDOWN_MAX_SECONDS", default="90")
)
# Legacy compatibility for deployments that still define only the old minute key.
PEER_FLOOD_MIN_COOLDOWN_MINUTES: int = int(
    config("PEER_FLOOD_MIN_COOLDOWN_MINUTES", default="1")
)
SPAMBOT_AUTO_RESUME: bool = config("SPAMBOT_AUTO_RESUME", default="true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
FLOODWAIT_EXTRA_SECONDS: int = int(config("FLOODWAIT_EXTRA_SECONDS", default="45"))

# ---------------------------------------------------------------------------
# Dialog retention
# ---------------------------------------------------------------------------
TELEGRAM_DIALOG_DELETE_DAYS: int = max(1, int(
    config("TELEGRAM_DIALOG_DELETE_DAYS", default="30")
))
LOCAL_DIALOG_TEXT_RETENTION_DAYS: int = max(1, int(
    config("LOCAL_DIALOG_TEXT_RETENTION_DAYS", default="180")
))

LOG_LEVEL: str = config("LOG_LEVEL", default="INFO").upper()

# Shared Telethon bot client (handlers import this singleton).
bot: TelegramClient = TelegramClient(BOT_SESSION_PATH, API_ID, API_HASH)


def is_admin(user_id: int | None) -> bool:
    """Return True if user_id is in the configured admin list."""
    if user_id is None:
        return False
    return int(user_id) in ADMIN_ID_LIST


def app_version() -> str:
    """Read the authoritative VERSION file or fail loudly.

    A fabricated fallback version makes production diagnostics unreliable, so a
    missing or unreadable VERSION file is treated as a broken release.
    """
    import pathlib

    candidates = (
        pathlib.Path(__file__).resolve().parent / "VERSION",
        pathlib.Path("VERSION"),
    )
    errors: list[str] = []
    for candidate in candidates:
        try:
            value = candidate.read_text(encoding="utf-8").strip()
        except (OSError, UnicodeError) as exc:
            errors.append(f"{candidate}: {exc}")
            continue
        if value:
            return value
        errors.append(f"{candidate}: empty")
    detail = "; ".join(errors) or "no candidates"
    raise RuntimeError(f"VERSION file is missing, unreadable or empty ({detail})")
