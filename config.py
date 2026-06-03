import os
from dataclasses import dataclass
from typing import List


def _required(name: str) -> str:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        raise RuntimeError(f"Не задана обязательная переменная окружения: {name}")
    return value.strip()


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"Переменная {name} должна быть числом, сейчас: {raw!r}") from exc


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on", "да"}


def _admin_ids() -> List[int]:
    raw = _required("ADMIN_ID_LIST")
    result: List[int] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            result.append(int(item))
        except ValueError as exc:
            raise RuntimeError(f"ADMIN_ID_LIST содержит не число: {item!r}") from exc
    if not result:
        raise RuntimeError("ADMIN_ID_LIST пустой")
    return result


@dataclass(frozen=True)
class Settings:
    api_id: int
    api_hash: str
    bot_token: str
    admin_id_list: List[int]
    db_path: str
    media_dir: str
    log_level: str
    safe_mode: bool
    min_interval_minutes: int
    send_delay_seconds: int
    max_targets_per_broadcast: int
    max_messages_per_hour_per_account: int
    max_import_chats: int


settings = Settings(
    api_id=_int_env("API_ID", 0) if os.getenv("API_ID") else int(_required("API_ID")),
    api_hash=_required("API_HASH"),
    bot_token=_required("BOT_TOKEN"),
    admin_id_list=_admin_ids(),
    db_path=os.getenv("DB_PATH", "data/tg_broadcast_manager.db").strip(),
    media_dir=os.getenv("MEDIA_DIR", "media").strip(),
    log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
    safe_mode=_bool_env("SAFE_MODE", True),
    min_interval_minutes=_int_env("MIN_INTERVAL_MINUTES", 60),
    send_delay_seconds=_int_env("SEND_DELAY_SECONDS", 45),
    max_targets_per_broadcast=_int_env("MAX_TARGETS_PER_BROADCAST", 20),
    max_messages_per_hour_per_account=_int_env("MAX_MESSAGES_PER_HOUR_PER_ACCOUNT", 10),
    max_import_chats=_int_env("MAX_IMPORT_CHATS", 250),
)

os.makedirs(os.path.dirname(settings.db_path) or ".", exist_ok=True)
os.makedirs(settings.media_dir, exist_ok=True)
os.makedirs("logs", exist_ok=True)
