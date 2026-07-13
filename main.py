import asyncio
import logging
import os
import sqlite3
import tempfile
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime
from typing import Any, Awaitable, Callable, Iterable, Optional, TypeVar

from dotenv import load_dotenv
from telethon import TelegramClient, events, utils, types
from telethon.errors import (
    ChatAdminRequiredError,
    ChatRestrictedError,
    ChatWriteForbiddenError,
    FloodWaitError,
    RPCError,
    UserBannedInChannelError,
    UserNotParticipantError,
)
from telethon.sessions import StringSession


load_dotenv()


MAX_TEXT_LENGTH = 4096
MAX_CAPTION_LENGTH = 1024
DUPLICATE_CACHE_LIMIT = 20_000
DEFAULT_DB_PATH = "processed_messages.sqlite3"
DEFAULT_DELETE_NOTICE_TEXT = "⚠️ Источник удалил этот пост"
DEFAULT_EDIT_MEDIA_NOTICE_TEXT = "⚠️ Источник изменил пост, но бот не смог автоматически обновить копию."

T = TypeVar("T")


class ConfigError(RuntimeError):
    pass


class DuplicateCache:
    """Fast in-memory duplicate protection with bounded size."""

    def __init__(self, limit: int = DUPLICATE_CACHE_LIMIT) -> None:
        self.limit = limit
        self._items: set[str] = set()
        self._order: deque[str] = deque()

    def exists(self, key: str) -> bool:
        return key in self._items

    def add(self, key: str) -> None:
        if key in self._items:
            return
        self._items.add(key)
        self._order.append(key)
        while len(self._order) > self.limit:
            old_key = self._order.popleft()
            self._items.discard(old_key)


class ProcessedStore:
    """Persistent duplicate protection for restarts/redeploys."""

    def __init__(self, path: str, enabled: bool) -> None:
        self.path = Path(path)
        self.enabled = enabled
        self._conn: sqlite3.Connection | None = None

    def open(self) -> None:
        if not self.enabled:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path)
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS processed ("
            "key TEXT PRIMARY KEY, "
            "processed_at INTEGER NOT NULL"
            ")"
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_processed_at ON processed(processed_at)"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS message_map ("
            "source_chat_id TEXT NOT NULL, "
            "source_message_id INTEGER NOT NULL, "
            "target_chat_id TEXT NOT NULL, "
            "target_message_id INTEGER NOT NULL, "
            "target_thread_id INTEGER, "
            "label_enabled INTEGER NOT NULL DEFAULT 0, "
            "source_label TEXT, "
            "target_index INTEGER NOT NULL DEFAULT 0, "
            "grouped_id TEXT, "
            "created_at INTEGER NOT NULL, "
            "source_text TEXT, "
            "source_text_updated_at INTEGER, "
            "source_date INTEGER, "
            "PRIMARY KEY(source_chat_id, source_message_id, target_chat_id, target_message_id)"
            ")"
        )
        self._ensure_column("message_map", "target_thread_id", "INTEGER")
        self._ensure_column("message_map", "label_enabled", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("message_map", "source_label", "TEXT")
        self._ensure_column("message_map", "source_text", "TEXT")
        self._ensure_column("message_map", "source_text_updated_at", "INTEGER")
        self._ensure_column("message_map", "source_date", "INTEGER")
        self._migrate_message_map_primary_key()
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_message_map_source "
            "ON message_map(source_chat_id, source_message_id)"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS deletion_notices ("
            "source_chat_id TEXT NOT NULL, "
            "source_message_id INTEGER NOT NULL, "
            "notified_at INTEGER NOT NULL, "
            "PRIMARY KEY(source_chat_id, source_message_id)"
            ")"
        )
        self._conn.commit()

    def _ensure_column(self, table: str, column: str, ddl_type: str) -> None:
        if self._conn is None:
            return
        rows = self._conn.execute(f"PRAGMA table_info({table})").fetchall()
        existing = {str(row[1]) for row in rows}
        if column not in existing:
            self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl_type}")

    def _migrate_message_map_primary_key(self) -> None:
        """Rebuild message_map if it was created by an older version.

        Older builds used PRIMARY KEY(source_chat_id, source_message_id, target_message_id).
        That can lose mappings when one source post is copied to several target chats that
        happen to have the same message_id. The production schema includes target_chat_id
        in the primary key so edit/delete sync remains correct for multi-route copying.
        """
        if self._conn is None:
            return

        row = self._conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'message_map'"
        ).fetchone()
        sql = str(row[0] or "") if row else ""
        normalized = sql.replace(" ", "").replace("\n", "").lower()
        expected_pk = "primarykey(source_chat_id,source_message_id,target_chat_id,target_message_id)"
        if expected_pk in normalized:
            return

        self._conn.execute("DROP TABLE IF EXISTS message_map_v2")
        self._conn.execute(
            "CREATE TABLE message_map_v2 ("
            "source_chat_id TEXT NOT NULL, "
            "source_message_id INTEGER NOT NULL, "
            "target_chat_id TEXT NOT NULL, "
            "target_message_id INTEGER NOT NULL, "
            "target_thread_id INTEGER, "
            "label_enabled INTEGER NOT NULL DEFAULT 0, "
            "source_label TEXT, "
            "target_index INTEGER NOT NULL DEFAULT 0, "
            "grouped_id TEXT, "
            "created_at INTEGER NOT NULL, "
            "source_text TEXT, "
            "source_text_updated_at INTEGER, "
            "source_date INTEGER, "
            "PRIMARY KEY(source_chat_id, source_message_id, target_chat_id, target_message_id)"
            ")"
        )
        self._conn.execute(
            "INSERT OR IGNORE INTO message_map_v2("
            "source_chat_id, source_message_id, target_chat_id, target_message_id, target_thread_id, "
            "label_enabled, source_label, target_index, grouped_id, created_at, source_text, source_text_updated_at, source_date"
            ") "
            "SELECT source_chat_id, source_message_id, target_chat_id, target_message_id, target_thread_id, "
            "label_enabled, source_label, target_index, grouped_id, created_at, source_text, source_text_updated_at, source_date "
            "FROM message_map"
        )
        self._conn.execute("DROP TABLE message_map")
        self._conn.execute("ALTER TABLE message_map_v2 RENAME TO message_map")
        self._conn.commit()

    def exists(self, key: str) -> bool:
        if not self.enabled or self._conn is None:
            return False
        row = self._conn.execute("SELECT 1 FROM processed WHERE key = ? LIMIT 1", (key,)).fetchone()
        return row is not None

    def add(self, key: str) -> None:
        if not self.enabled or self._conn is None:
            return
        self._conn.execute(
            "INSERT OR IGNORE INTO processed(key, processed_at) VALUES (?, ?)",
            (key, int(time.time())),
        )
        self._conn.commit()


    def add_message_mapping(
        self,
        source_chat_id: str,
        source_message_id: int,
        target_chat_id: str,
        target_message_ids: list[int],
        target_thread_id: int | None = None,
        label_enabled: bool = False,
        source_label: str | None = None,
        grouped_id: str | None = None,
        source_text: str | None = None,
        source_date: int | None = None,
    ) -> None:
        if not self.enabled or self._conn is None or not target_message_ids:
            return
        now = int(time.time())
        rows = [
            (source_chat_id, source_message_id, target_chat_id, target_id, target_thread_id, 1 if label_enabled else 0, source_label, index, grouped_id, now, source_text, now, source_date)
            for index, target_id in enumerate(target_message_ids)
        ]
        self._conn.executemany(
            "INSERT OR IGNORE INTO message_map("
            "source_chat_id, source_message_id, target_chat_id, target_message_id, target_thread_id, "
            "label_enabled, source_label, target_index, grouped_id, created_at, source_text, source_text_updated_at, source_date"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        self._conn.commit()

    def get_target_message_ids(self, source_chat_id: str, source_message_id: int) -> list[int]:
        if not self.enabled or self._conn is None:
            return []
        rows = self._conn.execute(
            "SELECT target_message_id FROM message_map "
            "WHERE source_chat_id = ? AND source_message_id = ? "
            "ORDER BY target_chat_id ASC, target_index ASC, target_message_id ASC",
            (source_chat_id, source_message_id),
        ).fetchall()
        return [int(row[0]) for row in rows]

    def get_target_mappings(self, source_chat_id: str, source_message_id: int) -> list[tuple[str, int, int | None, bool, str | None]]:
        if not self.enabled or self._conn is None:
            return []
        rows = self._conn.execute(
            "SELECT target_chat_id, target_message_id, target_thread_id, label_enabled, source_label FROM message_map "
            "WHERE source_chat_id = ? AND source_message_id = ? "
            "ORDER BY target_chat_id ASC, target_index ASC, target_message_id ASC",
            (source_chat_id, source_message_id),
        ).fetchall()
        return [(str(row[0]), int(row[1]), None if row[2] is None else int(row[2]), bool(row[3]), None if row[4] is None else str(row[4])) for row in rows]

    def get_source_text(self, source_chat_id: str, source_message_id: int) -> str | None:
        if not self.enabled or self._conn is None:
            return None
        row = self._conn.execute(
            "SELECT source_text FROM message_map "
            "WHERE source_chat_id = ? AND source_message_id = ? "
            "ORDER BY target_index ASC, target_message_id ASC LIMIT 1",
            (source_chat_id, source_message_id),
        ).fetchone()
        return None if row is None else row[0]

    def get_source_snapshot(self, source_chat_id: str, source_message_id: int) -> tuple[str | None, int | None]:
        if not self.enabled or self._conn is None:
            return None, None
        row = self._conn.execute(
            "SELECT source_text, source_date FROM message_map "
            "WHERE source_chat_id = ? AND source_message_id = ? "
            "ORDER BY target_index ASC, target_message_id ASC LIMIT 1",
            (source_chat_id, source_message_id),
        ).fetchone()
        if row is None:
            return None, None
        text = None if row[0] is None else str(row[0])
        source_date = None if row[1] is None else int(row[1])
        return text, source_date

    def update_source_text(self, source_chat_id: str, source_message_id: int, source_text: str) -> None:
        if not self.enabled or self._conn is None:
            return
        self._conn.execute(
            "UPDATE message_map SET source_text = ?, source_text_updated_at = ? "
            "WHERE source_chat_id = ? AND source_message_id = ?",
            (source_text, int(time.time()), source_chat_id, source_message_id),
        )
        self._conn.commit()

    def deletion_notice_exists(self, source_chat_id: str, source_message_id: int) -> bool:
        if not self.enabled or self._conn is None:
            return False
        row = self._conn.execute(
            "SELECT 1 FROM deletion_notices WHERE source_chat_id = ? AND source_message_id = ? LIMIT 1",
            (source_chat_id, source_message_id),
        ).fetchone()
        return row is not None

    def mark_deletion_notice(self, source_chat_id: str, source_message_id: int) -> None:
        if not self.enabled or self._conn is None:
            return
        self._conn.execute(
            "INSERT OR IGNORE INTO deletion_notices(source_chat_id, source_message_id, notified_at) "
            "VALUES (?, ?, ?)",
            (source_chat_id, source_message_id, int(time.time())),
        )
        self._conn.commit()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


@dataclass(slots=True)
class TargetRoute:
    target_chat: int | str
    target_thread_id: int | None = None
    label_source: bool = False


@dataclass(slots=True)
class RepostJob:
    kind: str
    duplicate_key: str
    source_chat_id: str
    from_peer: Any
    target_chat: int | str
    target_thread_id: int | None = None
    source_label: str | None = None
    message: Any | None = None
    messages: list[Any] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)


def setup_logging() -> logging.Logger:
    log_level_raw = (os.getenv("LOG_LEVEL") or "INFO").strip().upper()
    level = getattr(logging, log_level_raw, logging.INFO)
    logging.basicConfig(
        level=level,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("telegram-userbot-reposter")


logger = setup_logging()
duplicate_cache = DuplicateCache()
processing_keys: set[str] = set()
processed_store: ProcessedStore | None = None
job_queue: asyncio.Queue[RepostJob] | None = None
queue_worker_task: asyncio.Task[None] | None = None


def parse_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None or str(value).strip() == "":
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_int(value: Optional[str], name: str) -> int:
    if not value or not str(value).strip():
        raise ConfigError(f"ENV {name} is required")
    try:
        return int(str(value).strip())
    except ValueError as exc:
        raise ConfigError(f"ENV {name} must be an integer") from exc


def parse_optional_int(value: Optional[str], name: str) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return int(str(value).strip())
    except ValueError as exc:
        raise ConfigError(f"ENV {name} must be an integer") from exc


def parse_float(value: Optional[str], name: str, default: float) -> float:
    if value is None or str(value).strip() == "":
        return default
    try:
        return float(str(value).strip())
    except ValueError as exc:
        raise ConfigError(f"ENV {name} must be a number") from exc


def parse_chat(value: str, name: str = "chat") -> int | str:
    value = value.strip()
    if not value:
        raise ConfigError(f"ENV {name} is required")
    if value.lstrip("-").isdigit():
        return int(value)
    return value


def parse_chat_list(value: Optional[str], name: str) -> list[int | str]:
    if not value or not value.strip():
        raise ConfigError(f"ENV {name} is required")
    chats = [parse_chat(part, name) for part in value.split(",") if part.strip()]
    if not chats:
        raise ConfigError(f"ENV {name} must contain at least one chat")
    return chats


def parse_source_chats_and_topics(value: Optional[str], name: str = "SOURCE_CHATS") -> tuple[list[int | str], dict[str, int]]:
    """
    Supports both formats:
      SOURCE_CHATS=-100111,-100222
      SOURCE_CHATS=-100111:4,-100222:10

    The optional :thread_id part means: posts from this source should be sent
    to TARGET_CHAT inside that forum topic.
    """
    if not value or not value.strip():
        raise ConfigError(f"ENV {name} is required")

    chats: list[int | str] = []
    topic_map: dict[str, int] = {}
    for raw_part in value.split(","):
        part = raw_part.strip()
        if not part:
            continue

        chat_part = part
        thread_part: str | None = None
        if ":" in part:
            left, right = part.rsplit(":", 1)
            if left.strip() and right.strip().isdigit():
                chat_part = left.strip()
                thread_part = right.strip()

        chat = parse_chat(chat_part, name)
        chats.append(chat)
        if thread_part is not None:
            if not isinstance(chat, int):
                raise ConfigError(f"ENV {name}: topic mapping with ':' is supported only for numeric chat ids")
            topic_map[str(chat)] = int(thread_part)

    if not chats:
        raise ConfigError(f"ENV {name} must contain at least one chat")
    return chats, topic_map


def parse_source_topic_map(value: Optional[str]) -> dict[str, int]:
    """Parse SOURCE_TOPIC_MAP=-100111:4,-100222=10."""
    if not value or not value.strip():
        return {}
    result: dict[str, int] = {}
    for raw_part in value.split(","):
        part = raw_part.strip()
        if not part:
            continue
        separator = "=" if "=" in part else ":"
        if separator not in part:
            raise ConfigError("ENV SOURCE_TOPIC_MAP must use source_id:thread_id or source_id=thread_id")
        source_raw, thread_raw = part.rsplit(separator, 1)
        source = parse_chat(source_raw.strip(), "SOURCE_TOPIC_MAP")
        if not isinstance(source, int):
            raise ConfigError("ENV SOURCE_TOPIC_MAP supports only numeric source chat ids")
        try:
            thread_id = int(thread_raw.strip())
        except ValueError as exc:
            raise ConfigError("ENV SOURCE_TOPIC_MAP thread ids must be integers") from exc
        result[str(source)] = thread_id
    return result


def parse_route_endpoint(value: str, name: str, require_thread: bool) -> tuple[int | str, int | None]:
    value = value.strip()
    if not value:
        raise ConfigError(f"ENV {name} contains an empty route endpoint")

    chat_raw = value
    thread_id: int | None = None
    if ":" in value:
        left, right = value.rsplit(":", 1)
        if left.strip() and right.strip().isdigit():
            chat_raw = left.strip()
            thread_id = int(right.strip())

    chat = parse_chat(chat_raw, name)
    if require_thread and thread_id is None:
        raise ConfigError(f"ENV {name}: source endpoint must be chat_id:thread_id")
    if thread_id is not None and not isinstance(chat, int):
        raise ConfigError(f"ENV {name}: topic routing supports only numeric chat ids")
    return chat, thread_id


def parse_topic_route_map(value: Optional[str]) -> dict[tuple[str, int], TargetRoute]:
    """
    Parse topic-to-topic routes.

    Format:
      TOPIC_ROUTE_MAP=-100source:4>-100target:10,-100source:8>-100target:12

    Meaning:
      message from source chat topic 4 -> target chat topic 10
    """
    if not value or not value.strip():
        return {}

    result: dict[tuple[str, int], TargetRoute] = {}
    for raw_part in value.split(","):
        part = raw_part.strip()
        if not part:
            continue
        if ">" not in part:
            raise ConfigError("ENV TOPIC_ROUTE_MAP must use source_chat:source_thread>target_chat:target_thread")

        source_raw, target_raw = part.split(">", 1)
        source_chat, source_thread_id = parse_route_endpoint(source_raw, "TOPIC_ROUTE_MAP", require_thread=True)
        target_chat, target_thread_id = parse_route_endpoint(target_raw, "TOPIC_ROUTE_MAP", require_thread=False)
        if source_thread_id is None:
            raise ConfigError("ENV TOPIC_ROUTE_MAP source thread id is required")
        result[(str(source_chat), int(source_thread_id))] = TargetRoute(target_chat=target_chat, target_thread_id=target_thread_id)

    return result


def parse_route_map(value: Optional[str], name: str = "ROUTE_MAP", label_source: bool = False) -> dict[str, list[TargetRoute]]:
    """
    Parse simple chat-to-chat routes for ordinary channels without topics.

    Format:
      ROUTE_MAP=-100source1>-100targetA,-100source2>-100targetA
      ROUTE_MAP=-100source1>-100targetA:10  # optional target topic

    Meaning:
      any message from source chat -> target chat, optionally into target topic.
    """
    if not value or not value.strip():
        return {}

    result: dict[str, list[TargetRoute]] = {}
    for raw_part in value.split(","):
        part = raw_part.strip()
        if not part:
            continue
        if ">" not in part:
            raise ConfigError(f"ENV {name} must use source_chat>target_chat or source_chat>target_chat:target_thread")

        source_raw, target_raw = part.split(">", 1)
        source_chat, source_thread_id = parse_route_endpoint(source_raw, name, require_thread=False)
        if source_thread_id is not None:
            raise ConfigError(f"ENV {name} source endpoint must be source_chat without thread_id. Use TOPIC_ROUTE_MAP or EXTRA_TOPIC_ROUTE_MAP for source topics.")
        target_chat, target_thread_id = parse_route_endpoint(target_raw, name, require_thread=False)
        result.setdefault(str(source_chat), []).append(TargetRoute(target_chat=target_chat, target_thread_id=target_thread_id, label_source=label_source))

    return result


def parse_source_label_map(value: Optional[str], name: str = "SOURCE_LABEL_MAP") -> dict[str, str]:
    """Parse SOURCE_LABEL_MAP=-100111=My Channel,-100222=Another Channel."""
    if not value or not value.strip():
        return {}
    result: dict[str, str] = {}
    for raw_part in value.split(","):
        part = raw_part.strip()
        if not part:
            continue
        if "=" not in part:
            raise ConfigError(f"ENV {name} must use source_chat=Label")
        source_raw, label_raw = part.split("=", 1)
        source = parse_chat(source_raw.strip(), name)
        label = label_raw.strip()
        if not label:
            raise ConfigError(f"ENV {name} contains an empty label for {source_raw.strip()}")
        result[str(source)] = label
    return result


def merge_label_maps(*maps: dict[str, str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for label_map in maps:
        result.update(label_map)
    return result


def merge_route_maps(*maps: dict[str, list[TargetRoute]]) -> dict[str, list[TargetRoute]]:
    """Merge ROUTE_MAP-like dictionaries without losing old routes."""
    result: dict[str, list[TargetRoute]] = {}
    for route_map in maps:
        for source_key, routes in route_map.items():
            result.setdefault(str(source_key), []).extend(routes)
    return result


def add_source_chats_from_env(
    value: Optional[str],
    name: str,
    source_chats: list[int | str],
    inline_source_topic_map: dict[str, int],
) -> None:
    """Append SOURCE_CHATS-like values to existing config lists/maps."""
    if not value or not value.strip():
        return
    parsed_sources, parsed_topic_map = parse_source_chats_and_topics(value, name)
    source_chats.extend(parsed_sources)
    inline_source_topic_map.update(parsed_topic_map)


def get_env_config() -> dict[str, Any]:
    api_id = parse_int(os.getenv("API_ID"), "API_ID")
    api_hash = (os.getenv("API_HASH") or "").strip()
    session_string = (os.getenv("SESSION_STRING") or "").strip()
    topic_route_map = {
        **parse_topic_route_map(os.getenv("TOPIC_ROUTE_MAP")),
        **parse_topic_route_map(os.getenv("EXTRA_TOPIC_ROUTE_MAP")),
    }
    route_map = merge_route_maps(
        parse_route_map(os.getenv("ROUTE_MAP"), "ROUTE_MAP"),
        parse_route_map(os.getenv("EXTRA_ROUTE_MAP"), "EXTRA_ROUTE_MAP"),
        parse_route_map(os.getenv("LABELED_ROUTE_MAP"), "LABELED_ROUTE_MAP", label_source=True),
        parse_route_map(os.getenv("EXTRA_LABELED_ROUTE_MAP"), "EXTRA_LABELED_ROUTE_MAP", label_source=True),
    )
    source_label_map = merge_label_maps(
        parse_source_label_map(os.getenv("SOURCE_LABEL_MAP"), "SOURCE_LABEL_MAP"),
        parse_source_label_map(os.getenv("EXTRA_SOURCE_LABEL_MAP"), "EXTRA_SOURCE_LABEL_MAP"),
    )

    source_chats: list[int | str] = []
    inline_source_topic_map: dict[str, int] = {}
    add_source_chats_from_env(os.getenv("SOURCE_CHATS"), "SOURCE_CHATS", source_chats, inline_source_topic_map)
    add_source_chats_from_env(os.getenv("EXTRA_SOURCE_CHATS"), "EXTRA_SOURCE_CHATS", source_chats, inline_source_topic_map)

    if source_chats:
        # These are the sources that must go to the main TARGET_CHAT / topic map.
        # EXTRA_SOURCE_CHATS is additive and works exactly like SOURCE_CHATS.
        # ROUTE_MAP / EXTRA_ROUTE_MAP are additional routes and must not override this behavior.
        default_source_keys = {str(source) for source in source_chats}
    elif topic_route_map or route_map:
        # Allow pure route mode without SOURCE_CHATS / EXTRA_SOURCE_CHATS.
        source_chats = [int(chat_id) if str(chat_id).lstrip("-").isdigit() else chat_id for chat_id, _ in topic_route_map.keys()]
        source_chats.extend(int(chat_id) if str(chat_id).lstrip("-").isdigit() else chat_id for chat_id in route_map.keys())
        default_source_keys = set()
    else:
        raise ConfigError("ENV SOURCE_CHATS or EXTRA_SOURCE_CHATS is required unless TOPIC_ROUTE_MAP/EXTRA_TOPIC_ROUTE_MAP or ROUTE_MAP/EXTRA_ROUTE_MAP/LABELED_ROUTE_MAP/EXTRA_LABELED_ROUTE_MAP is configured")

    explicit_source_topic_map = parse_source_topic_map(os.getenv("SOURCE_TOPIC_MAP"))
    extra_source_topic_map = parse_source_topic_map(os.getenv("EXTRA_SOURCE_TOPIC_MAP"))
    source_topic_map = {**inline_source_topic_map, **explicit_source_topic_map, **extra_source_topic_map}

    # TOPIC_ROUTE_MAP / ROUTE_MAP can add extra source chats that are not listed in SOURCE_CHATS.
    for source_chat_id, _source_thread_id in topic_route_map.keys():
        source_chats.append(int(source_chat_id) if str(source_chat_id).lstrip("-").isdigit() else source_chat_id)
    for source_chat_id in route_map.keys():
        source_chats.append(int(source_chat_id) if str(source_chat_id).lstrip("-").isdigit() else source_chat_id)

    # Deduplicate source chats while preserving order.
    seen_sources: set[str] = set()
    deduped_sources: list[int | str] = []
    for source in source_chats:
        key = str(source)
        if key not in seen_sources:
            seen_sources.add(key)
            deduped_sources.append(source)
    source_chats = deduped_sources

    topic_route_source_chats = {chat_id for chat_id, _thread_id in topic_route_map.keys()}
    route_source_chats = set(route_map.keys())

    target_chat_raw = (os.getenv("TARGET_CHAT") or "").strip()
    if target_chat_raw:
        target_chat = parse_chat(target_chat_raw, "TARGET_CHAT")
    elif topic_route_map or route_map:
        # In pure route mode every target is defined in the route itself.
        all_routes = list(topic_route_map.values()) + [route for routes in route_map.values() for route in routes]
        target_chat = all_routes[0].target_chat
    else:
        raise ConfigError("ENV TARGET_CHAT is required")

    if not api_hash:
        raise ConfigError("ENV API_HASH is required")
    if not session_string:
        raise ConfigError("ENV SESSION_STRING is required")

    copy_mode = (os.getenv("COPY_MODE") or "copy").strip().lower()
    if copy_mode not in {"copy", "forward"}:
        raise ConfigError("ENV COPY_MODE must be either 'copy' or 'forward'")

    delay_seconds = max(0.0, parse_float(os.getenv("DELAY_SECONDS"), "DELAY_SECONDS", 1.0))
    max_flood_wait_seconds = max(
        1,
        parse_int(os.getenv("MAX_FLOOD_WAIT_SECONDS") or "900", "MAX_FLOOD_WAIT_SECONDS"),
    )
    queue_maxsize = max(1, parse_int(os.getenv("QUEUE_MAXSIZE") or "1000", "QUEUE_MAXSIZE"))

    log_chat_raw = (os.getenv("LOG_CHAT") or "").strip()
    log_chat = parse_chat(log_chat_raw, "LOG_CHAT") if log_chat_raw else None

    return {
        "api_id": api_id,
        "api_hash": api_hash,
        "session_string": session_string,
        "source_chats": source_chats,
        "target_chat": target_chat,
        "target_thread_id": parse_optional_int(os.getenv("TARGET_THREAD_ID"), "TARGET_THREAD_ID"),
        "source_topic_map": source_topic_map,
        "topic_route_map": topic_route_map,
        "route_map": route_map,
        "source_label_map": source_label_map,
        "source_label_template": (os.getenv("SOURCE_LABEL_TEMPLATE") or "📡 Источник: {source_title}\n\n{text}").replace("\\n", "\n").strip(),
        "default_source_keys": default_source_keys,
        "topic_route_source_chats": topic_route_source_chats,
        "route_source_chats": route_source_chats,
        "log_chat": log_chat,
        "copy_mode": copy_mode,
        "delay_seconds": delay_seconds,
        "enable_albums": parse_bool(os.getenv("ENABLE_ALBUMS"), True),
        "enable_duplicate_protection": parse_bool(os.getenv("ENABLE_DUPLICATE_PROTECTION"), True),
        "persist_processed": parse_bool(os.getenv("PERSIST_PROCESSED"), True),
        "processed_db_path": (os.getenv("PROCESSED_DB_PATH") or DEFAULT_DB_PATH).strip(),
        "queue_maxsize": queue_maxsize,
        "link_preview": parse_bool(os.getenv("LINK_PREVIEW"), True),
        "preserve_buttons": parse_bool(os.getenv("PRESERVE_BUTTONS"), False),
        "max_flood_wait_seconds": max_flood_wait_seconds,
        "sync_deletes": parse_bool(os.getenv("SYNC_DELETES"), True),
        "sync_edits": parse_bool(os.getenv("SYNC_EDITS"), True),
        "delete_notice_text": (os.getenv("DELETE_NOTICE_TEXT") or DEFAULT_DELETE_NOTICE_TEXT).strip(),
        "edit_media_notice_text": (os.getenv("EDIT_MEDIA_NOTICE_TEXT") or DEFAULT_EDIT_MEDIA_NOTICE_TEXT).strip(),
    }


def create_client(config: dict[str, Any]) -> TelegramClient:
    try:
        session = StringSession(config["session_string"])
    except Exception as exc:  # noqa: BLE001 - user needs a clean config error
        raise ConfigError("SESSION_STRING is invalid or corrupted. Generate it again with generate_session.py") from exc

    return TelegramClient(session, config["api_id"], config["api_hash"])


CONFIG_ERROR: Optional[ConfigError] = None

try:
    CONFIG = get_env_config()
    client: Optional[TelegramClient] = create_client(CONFIG)
except ConfigError as exc:
    CONFIG_ERROR = exc
    CONFIG: dict[str, Any] = {}
    client = None


def chunk_text(text: str, limit: int = MAX_TEXT_LENGTH) -> list[str]:
    if not text:
        return []
    chunks: list[str] = []
    current = text
    while len(current) > limit:
        cut = current.rfind("\n", 0, limit)
        if cut <= 0:
            cut = limit
        chunks.append(current[:cut].strip())
        current = current[cut:].strip()
    if current:
        chunks.append(current)
    return chunks


def get_message_text(message: Any) -> str:
    return getattr(message, "message", None) or getattr(message, "raw_text", None) or ""


def source_label_from_event(source_chat_id: str, event: Any) -> str:
    mapped = CONFIG.get("source_label_map", {}).get(str(source_chat_id))
    if mapped:
        return mapped
    chat = getattr(event, "chat", None)
    title = getattr(chat, "title", None) or getattr(chat, "first_name", None) or getattr(chat, "username", None)
    return str(title or source_chat_id)


def apply_source_label(text: str, source_label: str | None, source_chat_id: str | None = None) -> str:
    if not source_label:
        return text or ""
    template = CONFIG.get("source_label_template") or "📡 Источник: {source_title}\n\n{text}"
    try:
        rendered = template.format(
            source_title=source_label,
            source_id=source_chat_id or "",
            text=text or "",
        )
    except Exception:
        rendered = f"📡 Источник: {source_label}\n\n{text or ''}"
    return rendered.strip()


def get_message_entities(message: Any) -> Optional[list[Any]]:
    return getattr(message, "entities", None)


def get_message_buttons(message: Any) -> Any | None:
    if not CONFIG.get("preserve_buttons"):
        return None
    return getattr(message, "buttons", None) or None


def media_type_name(media: Any) -> str:
    return media.__class__.__name__ if media is not None else "None"


def is_web_preview_media(media: Any) -> bool:
    return media_type_name(media) == "MessageMediaWebPage"


def is_unsupported_media(media: Any) -> bool:
    return media_type_name(media) in {"MessageMediaUnsupported", "MessageMediaEmpty"}


def has_real_media(message: Any) -> bool:
    media = getattr(message, "media", None)
    return bool(media and not is_web_preview_media(media) and not is_unsupported_media(media))


def get_chat_id_from_event(event: Any) -> str:
    """Best-effort chat id extraction for new/edit/delete events.

    Telethon deletion events may not always expose event.chat_id in the
    same way as NewMessage/MessageEdited. If we return "unknown", edit/delete
    sync cannot find the saved original -> copy mapping.
    """
    chat_id = getattr(event, "chat_id", None)
    if chat_id is None and getattr(event, "message", None) is not None:
        chat_id = getattr(event.message, "chat_id", None)
    if chat_id is None:
        for attr in ("_chat_peer", "peer", "_peer"):
            peer = getattr(event, attr, None)
            if peer is not None:
                try:
                    chat_id = utils.get_peer_id(peer)
                    break
                except Exception:
                    pass
    if chat_id is None:
        original_update = getattr(event, "original_update", None)
        if original_update is not None:
            peer = getattr(original_update, "peer", None)
            if peer is not None:
                try:
                    chat_id = utils.get_peer_id(peer)
                except Exception:
                    pass
            if chat_id is None and getattr(original_update, "channel_id", None) is not None:
                try:
                    chat_id = utils.get_peer_id(types.PeerChannel(int(original_update.channel_id)))
                except Exception:
                    pass
    return str(chat_id or "unknown")


def target_thread_for_source(source_chat_id: str) -> int | None:
    mapped_thread = CONFIG.get("source_topic_map", {}).get(str(source_chat_id))
    if mapped_thread is not None:
        return int(mapped_thread)
    return CONFIG.get("target_thread_id")


def get_message_thread_id(message: Any) -> int | None:
    # Bot API exposes message_thread_id. Telethon usually stores forum topic info in reply_to.
    direct_thread_id = getattr(message, "message_thread_id", None)
    if direct_thread_id is not None:
        try:
            return int(direct_thread_id)
        except (TypeError, ValueError):
            return None

    reply_to = getattr(message, "reply_to", None)
    if reply_to is None:
        return None

    for attr in ("reply_to_top_id", "reply_to_msg_id"):
        value = getattr(reply_to, attr, None)
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                return None
    return None


def dedupe_routes(routes: list[TargetRoute]) -> list[TargetRoute]:
    result: list[TargetRoute] = []
    seen: set[tuple[str, int | None]] = set()
    for route in routes:
        key = (str(route.target_chat), route.target_thread_id, bool(route.label_source))
        if key in seen:
            continue
        seen.add(key)
        result.append(route)
    return result


def resolve_target_routes(source_chat_id: str, message: Any | None = None) -> list[TargetRoute]:
    """Return every destination that must receive this source message.

    Important project rule:
    ROUTE_MAP is additive. It must not override SOURCE_CHATS / TARGET_CHAT topic routing.

    Example:
      SOURCE_CHATS="-100source:12"
      ROUTE_MAP="-100source>-100extra_target"

    Result:
      1) -100source -> TARGET_CHAT topic 12
      2) -100source -> -100extra_target
    """
    routes: list[TargetRoute] = []
    source_key = str(source_chat_id)
    source_thread_id = get_message_thread_id(message) if message is not None else None

    # 1) Explicit topic-to-topic routes.
    topic_route_matched = False
    if source_thread_id is not None:
        route = CONFIG.get("topic_route_map", {}).get((source_key, int(source_thread_id)))
        if route is not None:
            routes.append(route)
            topic_route_matched = True

    # 2) Simple chat-to-chat routes. These are EXTRA routes, not replacements.
    simple_routess = CONFIG.get("route_map", {}).get(source_key, [])
    if simple_routess:
        routes.extend(simple_routess)

    # 3) Old/default routing from SOURCE_CHATS -> TARGET_CHAT / mapped topic.
    # Add it when the source was explicitly listed in SOURCE_CHATS.
    if source_key in CONFIG.get("default_source_keys", set()):
        routes.append(TargetRoute(
            target_chat=CONFIG["target_chat"],
            target_thread_id=target_thread_for_source(source_chat_id),
        ))
    elif not routes and source_key not in CONFIG.get("topic_route_source_chats", set()):
        # Backward-safe fallback for legacy configs.
        routes.append(TargetRoute(
            target_chat=CONFIG["target_chat"],
            target_thread_id=target_thread_for_source(source_chat_id),
        ))

    # If this chat is controlled only by TOPIC_ROUTE_MAP and the current source topic
    # is not mapped, do not copy it anywhere unless ROUTE_MAP/default SOURCE_CHATS says so.
    if source_key in CONFIG.get("topic_route_source_chats", set()) and not topic_route_matched:
        if source_key not in CONFIG.get("default_source_keys", set()) and not simple_routess:
            return []

    return dedupe_routes(routes)


def route_suffix(route: TargetRoute) -> str:
    thread = route.target_thread_id if route.target_thread_id is not None else "root"
    label = "labeled" if route.label_source else "raw"
    return f"to:{route.target_chat}:{thread}:{label}"


def target_thread_kwargs(thread_id: int | None) -> dict[str, Any]:
    if thread_id is None:
        return {}
    # In Telethon a forum topic is normally addressed as reply_to=<topic starter/top message id>.
    return {"reply_to": thread_id}


def make_topic_reply_to(target_message_id: int, target_thread_id: int | None) -> Any:
    """
    Reply to a copied message inside the correct forum topic.

    A plain reply_to=target_message_id may sometimes be routed by Telegram/Telethon
    to General in forum groups. InputReplyToMessage with top_msg_id explicitly pins
    the reply to the topic starter/thread.
    """
    if target_thread_id is None:
        return target_message_id
    try:
        return types.InputReplyToMessage(reply_to_msg_id=target_message_id, top_msg_id=target_thread_id)
    except TypeError:
        return target_message_id


def normalize_sent_messages(result: Any) -> list[Any]:
    if result is None:
        return []
    if isinstance(result, list):
        return [item for item in result if item is not None]
    return [result]


def extract_message_ids(messages: Iterable[Any]) -> list[int]:
    ids: list[int] = []
    for message in messages:
        message_id = getattr(message, "id", None)
        if message_id is not None:
            ids.append(int(message_id))
    return ids


async def telegram_log(text: str, level: str = "info") -> None:
    if level == "error":
        logger.error(text)
    elif level == "warning":
        logger.warning(text)
    else:
        logger.info(text)

    if not CONFIG.get("log_chat") or client is None:
        return

    try:
        is_connected = getattr(client, "is_connected", None)
        if callable(is_connected) and not is_connected():
            return
        await client.send_message(CONFIG["log_chat"], text[:MAX_TEXT_LENGTH])
    except Exception as exc:  # noqa: BLE001 - logging must never crash the bot
        logger.warning("Could not send message to LOG_CHAT: %s", exc)


async def with_floodwait_retry(action_name: str, factory: Callable[[], Awaitable[T]]) -> T:
    while True:
        try:
            return await factory()
        except FloodWaitError as exc:
            wait_seconds = int(getattr(exc, "seconds", 0)) + 1
            if wait_seconds > CONFIG.get("max_flood_wait_seconds", 900):
                raise RuntimeError(
                    f"FloodWait is too long while {action_name}: {wait_seconds}s. "
                    "Increase MAX_FLOOD_WAIT_SECONDS if you want to wait."
                ) from exc
            await telegram_log(
                f"[WARN] FloodWait while {action_name}. Sleeping {wait_seconds}s",
                "warning",
            )
            await asyncio.sleep(wait_seconds)


async def throttle_after_send() -> None:
    delay = CONFIG.get("delay_seconds", 0)
    if delay:
        await asyncio.sleep(delay)


async def send_text(
    text: str,
    entities: Optional[list[Any]] = None,
    buttons: Any | None = None,
    link_preview: bool | None = None,
    target_chat: int | str | None = None,
    target_thread_id: int | None = None,
) -> list[Any]:
    if not text.strip():
        return []

    sent_messages: list[Any] = []
    chunks = chunk_text(text, MAX_TEXT_LENGTH)
    for index, chunk in enumerate(chunks):
        formatting_entities = entities if len(chunks) == 1 and index == 0 else None
        buttons_to_send = buttons if len(chunks) == 1 and index == 0 else None
        use_link_preview = CONFIG.get("link_preview") if link_preview is None else link_preview

        async def do_send() -> Any:
            kwargs: dict[str, Any] = {
                "formatting_entities": formatting_entities,
                "link_preview": use_link_preview,
                **target_thread_kwargs(target_thread_id),
            }
            if buttons_to_send:
                kwargs["buttons"] = buttons_to_send
            try:
                return await client.send_message(target_chat or CONFIG["target_chat"], chunk, **kwargs)
            except TypeError:
                kwargs.pop("formatting_entities", None)
                try:
                    return await client.send_message(target_chat or CONFIG["target_chat"], chunk, **kwargs)
                except TypeError:
                    kwargs.pop("link_preview", None)
                    kwargs.pop("buttons", None)
                    return await client.send_message(target_chat or CONFIG["target_chat"], chunk, **kwargs)

        result = await with_floodwait_retry("sending text", do_send)
        sent_messages.extend(normalize_sent_messages(result))
        await throttle_after_send()

    return sent_messages


async def send_file_copy(
    file_or_files: Any,
    caption: str = "",
    entities: Optional[list[Any]] = None,
    buttons: Any | None = None,
    target_chat: int | str | None = None,
    target_thread_id: int | None = None,
) -> list[Any]:
    caption_to_send = caption if len(caption) <= MAX_CAPTION_LENGTH else ""
    formatting_entities = entities if caption_to_send and len(caption) <= MAX_CAPTION_LENGTH else None
    buttons_to_send = buttons if caption_to_send and len(caption) <= MAX_CAPTION_LENGTH else None

    async def do_send() -> Any:
        kwargs: dict[str, Any] = {
            "caption": caption_to_send,
            "formatting_entities": formatting_entities,
            **target_thread_kwargs(target_thread_id),
        }
        if buttons_to_send:
            kwargs["buttons"] = buttons_to_send
        try:
            return await client.send_file(target_chat or CONFIG["target_chat"], file_or_files, **kwargs)
        except TypeError:
            kwargs.pop("formatting_entities", None)
            try:
                return await client.send_file(target_chat or CONFIG["target_chat"], file_or_files, **kwargs)
            except TypeError:
                kwargs.pop("buttons", None)
                return await client.send_file(target_chat or CONFIG["target_chat"], file_or_files, **kwargs)

    result = await with_floodwait_retry("sending media", do_send)
    sent_messages = normalize_sent_messages(result)
    await throttle_after_send()

    if caption and len(caption) > MAX_CAPTION_LENGTH:
        sent_messages.extend(await send_text(caption, entities=None, buttons=buttons, target_chat=target_chat, target_thread_id=target_thread_id))

    return sent_messages


async def forward_message(message: Any, from_peer: Any, target_chat: int | str | None = None) -> list[Any]:
    async def do_forward() -> Any:
        return await client.forward_messages(target_chat or CONFIG["target_chat"], [message.id], from_peer=from_peer)

    result = await with_floodwait_retry("forwarding message", do_forward)
    await throttle_after_send()
    return normalize_sent_messages(result)


async def forward_album(messages: list[Any], from_peer: Any, target_chat: int | str | None = None) -> list[Any]:
    message_ids = [message.id for message in messages]

    async def do_forward() -> Any:
        return await client.forward_messages(target_chat or CONFIG["target_chat"], message_ids, from_peer=from_peer)

    result = await with_floodwait_retry("forwarding album", do_forward)
    await throttle_after_send()
    return normalize_sent_messages(result)


def first_album_caption(messages: Iterable[Any]) -> tuple[str, Optional[list[Any]], Any | None]:
    for message in messages:
        text = get_message_text(message)
        if text:
            return text, get_message_entities(message), get_message_buttons(message)
    return "", None, None


async def copy_single_message(message: Any, target_chat: int | str | None = None, target_thread_id: int | None = None, source_label: str | None = None, source_chat_id: str | None = None) -> list[Any]:
    raw_text = get_message_text(message)
    text = apply_source_label(raw_text, source_label, source_chat_id) if source_label else raw_text
    entities = None if source_label else get_message_entities(message)
    buttons = get_message_buttons(message)
    media = getattr(message, "media", None)

    if media and is_web_preview_media(media):
        # Link preview is not a downloadable file. Copy it as text with preview enabled.
        if text:
            return await send_text(text, entities=entities, buttons=buttons, link_preview=True, target_chat=target_chat, target_thread_id=target_thread_id)
        await telegram_log(f"[WARN] Message {message.id} has only web preview without text. Skipped.", "warning")
        return []

    if media and is_unsupported_media(media):
        if text:
            return await send_text(text, entities=entities, buttons=buttons, target_chat=target_chat, target_thread_id=target_thread_id)
        await telegram_log(
            f"[WARN] Message {message.id} has unsupported Telegram media type: {media_type_name(media)}. Skipped.",
            "warning",
        )
        return []

    if media:
        try:
            return await send_file_copy(media, caption=text, entities=entities, buttons=buttons, target_chat=target_chat, target_thread_id=target_thread_id)
        except (RPCError, ValueError, TypeError, AttributeError) as exc:
            await telegram_log(
                f"[WARN] Direct media copy failed for message {message.id}: {exc}. Trying download fallback.",
                "warning",
            )

        with tempfile.TemporaryDirectory() as temp_dir:
            path = await message.download_media(file=temp_dir)
            if not path:
                raise RuntimeError("Could not download media for fallback copy")
            return await send_file_copy(path, caption=text, entities=entities, buttons=buttons, target_chat=target_chat, target_thread_id=target_thread_id)

    if text:
        return await send_text(text, entities=entities, buttons=buttons, target_chat=target_chat, target_thread_id=target_thread_id)

    await telegram_log(f"[WARN] Message {message.id} has no text/media. Skipped.", "warning")
    return []


async def copy_album(messages: list[Any], target_chat: int | str | None = None, target_thread_id: int | None = None, source_label: str | None = None, source_chat_id: str | None = None) -> list[Any]:
    media_messages = [message for message in messages if has_real_media(message)]
    if not media_messages:
        joined_text = "\n\n".join(filter(None, [get_message_text(message) for message in messages]))
        joined_text = apply_source_label(joined_text, source_label, source_chat_id) if source_label else joined_text
        return await send_text(joined_text, target_chat=target_chat, target_thread_id=target_thread_id)

    caption, entities, buttons = first_album_caption(messages)
    if source_label:
        caption = apply_source_label(caption, source_label, source_chat_id)
        entities = None
    media_items = [message.media for message in media_messages]

    try:
        return await send_file_copy(media_items, caption=caption, entities=entities, buttons=buttons, target_chat=target_chat, target_thread_id=target_thread_id)
    except (RPCError, ValueError, TypeError, AttributeError) as exc:
        await telegram_log(
            f"[WARN] Direct album copy failed: {exc}. Trying download fallback.",
            "warning",
        )

    with tempfile.TemporaryDirectory() as temp_dir:
        paths: list[str] = []
        for message in media_messages:
            path = await message.download_media(file=temp_dir)
            if path:
                paths.append(path)

        if not paths:
            raise RuntimeError("Could not download album media for fallback copy")

        return await send_file_copy(paths, caption=caption, entities=entities, buttons=buttons, target_chat=target_chat, target_thread_id=target_thread_id)


def save_message_mapping(source_chat_id: str, source_message_id: int, sent_messages: list[Any], source_text: str | None = None, target_chat: int | str | None = None, target_thread_id: int | None = None, source_date: int | None = None, label_enabled: bool = False, source_label: str | None = None) -> None:
    if processed_store is None:
        return
    target_ids = extract_message_ids(sent_messages)
    if not target_ids:
        logger.warning("No target message ids returned for source %s:%s", source_chat_id, source_message_id)
        return
    try:
        processed_store.add_message_mapping(
            source_chat_id=source_chat_id,
            source_message_id=source_message_id,
            target_chat_id=str(target_chat or CONFIG["target_chat"]),
            target_message_ids=target_ids,
            target_thread_id=target_thread_id,
            label_enabled=label_enabled,
            source_label=source_label,
            source_text=source_text,
            source_date=source_date,
        )
    except sqlite3.Error as exc:
        logger.warning("Could not save message mapping to sqlite: %s", exc)


def save_album_mapping(source_chat_id: str, source_messages: list[Any], sent_messages: list[Any], target_chat: int | str | None = None, target_thread_id: int | None = None, label_enabled: bool = False, source_label: str | None = None) -> None:
    if processed_store is None:
        return
    target_ids = extract_message_ids(sent_messages)
    if not target_ids:
        logger.warning("No target message ids returned for album from source %s", source_chat_id)
        return

    grouped_id = str(getattr(source_messages[0], "grouped_id", "") or "") if source_messages else None
    try:
        # In most cases Telegram returns one target message per album item.
        # When it does not, store the full target-id list for the first source message,
        # and a best-effort one-to-one mapping for the rest.
        if len(target_ids) == len(source_messages):
            for source_message, target_id in zip(source_messages, target_ids, strict=False):
                processed_store.add_message_mapping(
                    source_chat_id=source_chat_id,
                    source_message_id=int(source_message.id),
                    target_chat_id=str(target_chat or CONFIG["target_chat"]),
                    target_message_ids=[target_id],
                    target_thread_id=target_thread_id,
                    label_enabled=label_enabled,
                    source_label=source_label,
                    grouped_id=grouped_id,
                    source_text=get_message_text(source_message),
                    source_date=message_unix_date(source_message),
                )
        else:
            processed_store.add_message_mapping(
                source_chat_id=source_chat_id,
                source_message_id=int(source_messages[0].id),
                target_chat_id=str(target_chat or CONFIG["target_chat"]),
                target_message_ids=target_ids,
                target_thread_id=target_thread_id,
                label_enabled=label_enabled,
                source_label=source_label,
                grouped_id=grouped_id,
                source_text=get_message_text(source_messages[0]),
                source_date=message_unix_date(source_messages[0]),
            )
            for source_message in source_messages[1:]:
                processed_store.add_message_mapping(
                    source_chat_id=source_chat_id,
                    source_message_id=int(source_message.id),
                    target_chat_id=str(target_chat or CONFIG["target_chat"]),
                    target_message_ids=target_ids[:1],
                    target_thread_id=target_thread_id,
                    label_enabled=label_enabled,
                    source_label=source_label,
                    grouped_id=grouped_id,
                    source_text=get_message_text(source_message),
                    source_date=message_unix_date(source_message),
                )
    except sqlite3.Error as exc:
        logger.warning("Could not save album mapping to sqlite: %s", exc)


def get_all_target_mappings(source_chat_id: str, source_message_id: int) -> list[tuple[int | str, int, int | None, bool, str | None]]:
    if processed_store is None:
        return []
    try:
        mappings = processed_store.get_target_mappings(source_chat_id, source_message_id)
    except sqlite3.Error as exc:
        logger.warning("Could not read message mapping from sqlite: %s", exc)
        return []
    result: list[tuple[int | str, int, int | None, bool, str | None]] = []
    for target_chat_raw, target_message_id, target_thread_id, label_enabled, source_label in mappings:
        result.append((parse_chat(target_chat_raw, "saved target_chat_id"), target_message_id, target_thread_id, label_enabled, source_label))
    return result


def get_first_target_mapping(source_chat_id: str, source_message_id: int) -> tuple[int | str, int, int | None, bool, str | None] | None:
    mappings = get_all_target_mappings(source_chat_id, source_message_id)
    if not mappings:
        return None
    return mappings[0]


def get_first_target_id(source_chat_id: str, source_message_id: int) -> int | None:
    mapping = get_first_target_mapping(source_chat_id, source_message_id)
    return mapping[1] if mapping is not None else None


def get_saved_source_text(source_chat_id: str, source_message_id: int) -> str | None:
    if processed_store is None:
        return None
    try:
        return processed_store.get_source_text(source_chat_id, source_message_id)
    except sqlite3.Error as exc:
        logger.warning("Could not read source text from sqlite: %s", exc)
        return None


def update_saved_source_text(source_chat_id: str, source_message_id: int, source_text: str) -> None:
    if processed_store is None:
        return
    try:
        processed_store.update_source_text(source_chat_id, source_message_id, source_text)
    except sqlite3.Error as exc:
        logger.warning("Could not update source text in sqlite: %s", exc)


def normalize_message_text(text: str | None) -> str:
    return (text or "").replace("\r\n", "\n").replace("\r", "\n").strip()


def deletion_notice_already_sent(source_chat_id: str, source_message_id: int) -> bool:
    if processed_store is None:
        return False
    try:
        return processed_store.deletion_notice_exists(source_chat_id, source_message_id)
    except sqlite3.Error as exc:
        logger.warning("Could not read deletion notice state from sqlite: %s", exc)
        return False


def mark_deletion_notice_sent(source_chat_id: str, source_message_id: int) -> None:
    if processed_store is None:
        return
    try:
        processed_store.mark_deletion_notice(source_chat_id, source_message_id)
    except sqlite3.Error as exc:
        logger.warning("Could not save deletion notice state to sqlite: %s", exc)


def reserve_processing_key(key: str) -> bool:
    """Return True when key can be queued now. False means duplicate/in-flight."""
    if CONFIG.get("enable_duplicate_protection"):
        if duplicate_cache.exists(key):
            return False
        if processed_store is not None and processed_store.exists(key):
            duplicate_cache.add(key)
            return False
    if key in processing_keys:
        return False
    processing_keys.add(key)
    return True


def mark_processed_key(key: str) -> None:
    if not CONFIG.get("enable_duplicate_protection"):
        return
    duplicate_cache.add(key)
    if processed_store is not None:
        try:
            processed_store.add(key)
        except sqlite3.Error as exc:
            logger.warning("Could not save processed key to sqlite: %s", exc)


def release_processing_key(key: str) -> None:
    processing_keys.discard(key)


async def enqueue_job(job: RepostJob) -> None:
    if not reserve_processing_key(job.duplicate_key):
        await telegram_log(f"[INFO] Duplicate or in-flight item skipped: {job.duplicate_key}")
        return

    if job_queue is None:
        release_processing_key(job.duplicate_key)
        await telegram_log("[ERROR] Queue is not initialized. Item skipped.", "error")
        return

    try:
        job_queue.put_nowait(job)
        await telegram_log(f"[INFO] Queued {job.kind}: {job.duplicate_key} | queue={job_queue.qsize()}")
    except asyncio.QueueFull:
        release_processing_key(job.duplicate_key)
        await telegram_log(
            f"[ERROR] Queue is full. Increase QUEUE_MAXSIZE. Item skipped: {job.duplicate_key}",
            "error",
        )


async def process_job(job: RepostJob) -> None:
    if job.kind == "message":
        if job.message is None:
            raise RuntimeError("Message job has no message")
        await telegram_log(f"[INFO] Processing message: {job.duplicate_key}")
        if CONFIG["copy_mode"] == "forward" and not job.source_label:
            sent_messages = await forward_message(job.message, from_peer=job.from_peer, target_chat=job.target_chat)
        else:
            sent_messages = await copy_single_message(job.message, target_chat=job.target_chat, target_thread_id=job.target_thread_id, source_label=job.source_label, source_chat_id=job.source_chat_id)
        save_message_mapping(job.source_chat_id, int(job.message.id), sent_messages, source_text=get_message_text(job.message), target_chat=job.target_chat, target_thread_id=job.target_thread_id, source_date=message_unix_date(job.message), label_enabled=bool(job.source_label), source_label=job.source_label)
        await telegram_log(f"[INFO] Message copied to target: {job.duplicate_key}")
        return

    if job.kind == "album":
        if not job.messages:
            raise RuntimeError("Album job has no messages")
        await telegram_log(f"[INFO] Processing album: {job.duplicate_key} | items={len(job.messages)}")
        if CONFIG["copy_mode"] == "forward" and not job.source_label:
            sent_messages = await forward_album(job.messages, from_peer=job.from_peer, target_chat=job.target_chat)
        else:
            sent_messages = await copy_album(job.messages, target_chat=job.target_chat, target_thread_id=job.target_thread_id, source_label=job.source_label, source_chat_id=job.source_chat_id)
        save_album_mapping(job.source_chat_id, job.messages, sent_messages, target_chat=job.target_chat, target_thread_id=job.target_thread_id, label_enabled=bool(job.source_label), source_label=job.source_label)
        await telegram_log(f"[INFO] Album copied to target: {job.duplicate_key}")
        return

    raise RuntimeError(f"Unknown job kind: {job.kind}")


async def queue_worker() -> None:
    assert job_queue is not None
    await telegram_log("[INFO] Queue worker started")
    while True:
        job = await job_queue.get()
        try:
            await process_job(job)
            mark_processed_key(job.duplicate_key)
        except Exception as exc:  # noqa: BLE001 - bot must keep running
            message = f"[ERROR] Failed to process {job.duplicate_key}: {repr(exc)}"
            hint = permission_error_hint(exc)
            if hint:
                message = f"{message}\n{hint}"
            await telegram_log(message, "error")
        finally:
            release_processing_key(job.duplicate_key)
            job_queue.task_done()


def format_ts(ts: int | None) -> str:
    if ts is None:
        return "неизвестно"
    try:
        return datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M:%S")
    except Exception:
        return "неизвестно"


def compact_deleted_text(text: str | None) -> str:
    if not text or not text.strip():
        return "[текст отсутствует / медиа без подписи]"
    cleaned = text.strip()
    max_len = 1200
    if len(cleaned) > max_len:
        cleaned = cleaned[:max_len].rstrip() + "..."
    return cleaned


def message_unix_date(message: Any) -> int | None:
    value = getattr(message, "date", None)
    if value is None:
        return None
    try:
        return int(value.timestamp())
    except Exception:
        return None


def build_delete_notice(source_chat_id: str, source_message_id: int, target_message_id: int) -> str:
    base = CONFIG.get("delete_notice_text") or DEFAULT_DELETE_NOTICE_TEXT
    try:
        source_text, source_date = processed_store.get_source_snapshot(source_chat_id, source_message_id) if processed_store else (None, None)
    except Exception:
        source_text, source_date = None, None
    deleted_at = int(time.time())
    parts = [
        base,
        "",
        f"🕒 Обнаружено удаление: {format_ts(deleted_at)}",
        f"📝 Дата поста: {format_ts(source_date)}",
        "",
        "🧾 Текст удалённого поста:",
        compact_deleted_text(source_text),
        "",
        f"↪️ Пост-копия: {target_message_id}",
    ]
    return "\n".join(parts).strip()


async def send_delete_notice(source_chat_id: str, source_message_id: int, target_chat: int | str, target_message_id: int, target_thread_id: int | None = None) -> None:
    notice = build_delete_notice(source_chat_id, source_message_id, target_message_id)
    if not notice:
        return

    async def do_send() -> Any:
        # Stable mode: force message into the saved target topic.
        # Reply mode is unstable in some Telegram forum groups and can either go to General or fail silently.
        if target_thread_id is not None:
            kwargs = target_thread_kwargs(target_thread_id)
            return await client.send_message(target_chat, notice[:MAX_TEXT_LENGTH], **kwargs)

        return await client.send_message(
            target_chat,
            notice[:MAX_TEXT_LENGTH],
            reply_to=target_message_id,
        )

    await with_floodwait_retry("sending delete notice", do_send)
    await throttle_after_send()


async def send_edit_media_notice(target_chat: int | str, target_message_id: int, target_thread_id: int | None = None) -> None:
    notice = CONFIG.get("edit_media_notice_text") or DEFAULT_EDIT_MEDIA_NOTICE_TEXT
    if not notice:
        return

    async def do_send() -> Any:
        if target_thread_id is not None:
            kwargs = target_thread_kwargs(target_thread_id)
            topic_text = f"{notice[:MAX_TEXT_LENGTH - 80]}\n\n↪️ Пост-копия: {target_message_id}"
            return await client.send_message(target_chat, topic_text[:MAX_TEXT_LENGTH], **kwargs)

        return await client.send_message(
            target_chat,
            notice[:MAX_TEXT_LENGTH],
            reply_to=target_message_id,
        )

    await with_floodwait_retry("sending edit notice", do_send)
    await throttle_after_send()


async def sync_edited_message(source_chat_id: str, message: Any) -> None:
    source_message_id = int(message.id)
    target_mappings = get_all_target_mappings(source_chat_id, source_message_id)
    if not target_mappings:
        await telegram_log(f"[WARN] Edited source message has no saved target mapping: {source_chat_id}:{message.id}", "warning")
        return

    text = get_message_text(message)
    entities = get_message_entities(message)
    previous_text = get_saved_source_text(source_chat_id, source_message_id)

    # Telegram can emit edit events even when the visible text/caption did not change.
    if previous_text is not None and normalize_message_text(previous_text) == normalize_message_text(text):
        await telegram_log(f"[INFO] Edit skipped because text is unchanged: {source_chat_id}:{message.id}")
        return

    if not text:
        for target_chat, target_message_id, target_thread_id, _label_enabled, _source_label in target_mappings:
            await send_edit_media_notice(target_chat, target_message_id, target_thread_id)
        update_saved_source_text(source_chat_id, source_message_id, text)
        return

    limit = MAX_CAPTION_LENGTH if has_real_media(message) else MAX_TEXT_LENGTH
    edited_count = 0

    for target_chat, target_message_id, target_thread_id, label_enabled, source_label in target_mappings:
        text_for_target = apply_source_label(text, source_label or source_chat_id, source_chat_id) if label_enabled else text
        text_to_edit = text_for_target if len(text_for_target) <= limit else text_for_target[: limit - 20] + "\n…[обрезано]"
        entities_for_target = None if label_enabled else entities

        async def do_edit() -> Any:
            kwargs: dict[str, Any] = {}
            if entities_for_target:
                kwargs["formatting_entities"] = entities_for_target
            try:
                return await client.edit_message(target_chat, target_message_id, text_to_edit, **kwargs)
            except TypeError:
                kwargs.pop("formatting_entities", None)
                return await client.edit_message(target_chat, target_message_id, text_to_edit, **kwargs)

        try:
            await with_floodwait_retry("editing target message", do_edit)
            edited_count += 1
            await telegram_log(f"[INFO] Synced edit: {source_chat_id}:{message.id} -> {target_chat}:{target_message_id}")
        except Exception as exc:  # noqa: BLE001
            if "MessageNotModified" in exc.__class__.__name__ or "message was not modified" in str(exc).lower():
                edited_count += 1
                await telegram_log(f"[INFO] Edit was a no-op: {source_chat_id}:{message.id} -> {target_chat}:{target_message_id}")
                continue
            edit_message = f"[ERROR] Could not sync edit for {source_chat_id}:{message.id} -> {target_chat}:{target_message_id}: {repr(exc)}"
            edit_hint = permission_error_hint(exc)
            if edit_hint:
                edit_message = f"{edit_message}\n{edit_hint}"
            await telegram_log(edit_message, "error")
            await send_edit_media_notice(target_chat, target_message_id, target_thread_id)

    if edited_count > 0:
        update_saved_source_text(source_chat_id, source_message_id, text)

async def sync_deleted_messages(source_chat_id: str, deleted_ids: Iterable[int]) -> None:
    for source_message_id in deleted_ids:
        source_message_id = int(source_message_id)
        if deletion_notice_already_sent(source_chat_id, source_message_id):
            continue
        target_mappings = get_all_target_mappings(source_chat_id, source_message_id)
        if not target_mappings:
            await telegram_log(
                f"[WARN] Deleted source message has no saved target mapping: {source_chat_id}:{source_message_id}",
                "warning",
            )
            continue
        sent_count = 0
        for target_chat, target_message_id, target_thread_id, _label_enabled, _source_label in target_mappings:
            try:
                await send_delete_notice(source_chat_id, source_message_id, target_chat, target_message_id, target_thread_id)
                sent_count += 1
                await telegram_log(f"[INFO] Delete notice sent: {source_chat_id}:{source_message_id} -> {target_chat}:{target_message_id}")
            except Exception as exc:  # noqa: BLE001
                delete_message = f"[ERROR] Could not send delete notice for {source_chat_id}:{source_message_id} -> {target_chat}:{target_message_id}: {repr(exc)}"
                delete_hint = permission_error_hint(exc)
                if delete_hint:
                    delete_message = f"{delete_message}\n{delete_hint}"
                await telegram_log(delete_message, "error")
        if sent_count > 0:
            mark_deletion_notice_sent(source_chat_id, source_message_id)


async def entity_title(entity: Any) -> str:
    username = getattr(entity, "username", None)
    title = getattr(entity, "title", None) or getattr(entity, "first_name", None) or getattr(entity, "id", None)
    if username:
        return f"{title} (@{username})"
    return str(title)


async def describe_send_permissions(entity: Any) -> tuple[bool, str]:
    """Explain whether this account can currently send into `entity`, and why.

    Read-only diagnostics. Telegram itself decides who may post where, so this
    cannot grant rights and nothing here changes what gets sent - it only turns
    a silent/late send failure into a clear, upfront reason.

    Returns (is_ok, message). is_ok is False whenever a human should look at
    the target chat's Telegram settings before relying on it.
    """
    is_channel_type = isinstance(entity, types.Channel)
    is_broadcast = is_channel_type and bool(getattr(entity, "broadcast", False))

    try:
        my_perms = await client.get_permissions(entity, "me")
    except UserNotParticipantError:
        return False, "NOT A MEMBER of this chat - the account must join it first."
    except Exception as exc:  # noqa: BLE001 - diagnostics must never crash preflight
        return False, f"could not verify permissions ({type(exc).__name__}: {exc})"

    if my_perms.is_creator:
        return True, "OK, account is the creator."

    if getattr(my_perms, "has_left", False):
        return False, "ACCOUNT HAS LEFT this chat - it cannot send anything until it rejoins."

    if my_perms.is_banned:
        return False, "ACCOUNT IS RESTRICTED by an admin in this chat - ask an admin to lift the restriction."

    if my_perms.is_admin:
        if is_broadcast and not my_perms.post_messages:
            return False, "account is an admin here, but the 'Post Messages' right is off - ask the owner to grant it."
        return True, "OK, account is an admin here."

    # Regular, non-admin participant from here on - Telegram's own rules decide the rest.
    if is_broadcast:
        return False, (
            "REGULAR SUBSCRIBER of a broadcast channel. Telegram never lets non-admins post into "
            "a channel's feed - only accounts with the 'Post Messages' admin right can. This is a "
            "Telegram platform rule; no change in this bot's code can bypass it."
        )

    try:
        default_rights = await client.get_permissions(entity)
    except Exception as exc:  # noqa: BLE001
        return False, f"regular member, could not read the group's default permissions ({type(exc).__name__}: {exc})"

    if default_rights is not None and getattr(default_rights, "send_messages", False):
        return False, (
            "regular member, but this group currently restricts 'Send Messages' to admins only. "
            "An admin has to allow it in Telegram (Group settings -> Permissions -> Send Messages), "
            "or promote this account - this bot cannot change that from the outside."
        )

    return True, "OK, regular member and the group allows members to send messages."


async def log_send_permissions(label: str, entity: Any) -> None:
    is_ok, note = await describe_send_permissions(entity)
    await telegram_log(f"[INFO] {label} send permissions: {note}", "info" if is_ok else "warning")


def permission_error_hint(exc: Exception) -> str | None:
    """Turn a Telegram permission error into an actionable one-line hint.

    Telegram enforces these checks on its servers; no code change here can skip them.
    """
    if isinstance(exc, ChatWriteForbiddenError):
        return (
            "[HINT] Telegram refused this write (ChatWriteForbiddenError). In a broadcast channel "
            "this means the account lacks the 'Post Messages' admin right. In a group it means "
            "messaging is currently restricted for this account or for regular members."
        )
    if isinstance(exc, ChatAdminRequiredError):
        return "[HINT] Telegram requires admin rights for this action (ChatAdminRequiredError)."
    if isinstance(exc, ChatRestrictedError):
        return "[HINT] Telegram reports this action is restricted for this account here (ChatRestrictedError)."
    if isinstance(exc, UserBannedInChannelError):
        return "[HINT] Telegram reports this account is banned in that channel/group (UserBannedInChannelError)."
    return None


async def preflight_check() -> None:
    if client is None:
        raise ConfigError("Telegram client was not initialized")

    await telegram_log("[INFO] Preflight check started")

    try:
        target_entity = await client.get_entity(CONFIG["target_chat"])
    except Exception as exc:  # noqa: BLE001 - produce clear startup error
        raise ConfigError(f"Cannot access TARGET_CHAT={CONFIG['target_chat']}: {exc}") from exc

    target_peer_id = utils.get_peer_id(target_entity)
    await telegram_log(f"[INFO] TARGET OK: {target_peer_id} | {await entity_title(target_entity)}")
    await log_send_permissions("TARGET", target_entity)

    route_target_peer_ids: set[int] = set()
    route_map_flat = [route for routes in CONFIG.get("route_map", {}).values() for route in routes]
    for env_name, routes in (("TOPIC_ROUTE_MAP", CONFIG.get("topic_route_map", {}).values()), ("ROUTE_MAP", route_map_flat)):
        for route in routes:
            try:
                route_target_entity = await client.get_entity(route.target_chat)
            except Exception as exc:  # noqa: BLE001
                raise ConfigError(f"Cannot access {env_name} target={route.target_chat}: {exc}") from exc
            route_target_peer_id = utils.get_peer_id(route_target_entity)
            route_target_peer_ids.add(route_target_peer_id)
            await telegram_log(
                f"[INFO] {env_name} TARGET OK: {route_target_peer_id} "
                f"thread={route.target_thread_id} | {await entity_title(route_target_entity)}"
            )
            await log_send_permissions(f"{env_name} target={route_target_peer_id}", route_target_entity)

    source_peer_ids: set[int] = set()
    for source in CONFIG["source_chats"]:
        try:
            source_entity = await client.get_entity(source)
        except Exception as exc:  # noqa: BLE001 - produce clear startup error
            raise ConfigError(f"Cannot access SOURCE_CHAT={source}: {exc}") from exc

        source_peer_id = utils.get_peer_id(source_entity)
        source_peer_ids.add(source_peer_id)
        await telegram_log(f"[INFO] SOURCE OK: {source_peer_id} | {await entity_title(source_entity)}")

    if target_peer_id in source_peer_ids:
        if CONFIG.get("topic_route_map") or CONFIG.get("route_map"):
            await telegram_log(
                "[WARN] TARGET_CHAT is also present in SOURCE_CHATS, but ROUTE_MAP/TOPIC_ROUTE_MAP is enabled. "
                "Only explicitly mapped source routes should be copied. Check routes to avoid loops.",
                "warning",
            )
        else:
            raise ConfigError(
                "TARGET_CHAT is also present in SOURCE_CHATS. This can create an infinite repost loop. "
                "Remove target from SOURCE_CHATS."
            )

    if CONFIG.get("log_chat"):
        try:
            log_entity = await client.get_entity(CONFIG["log_chat"])
            await telegram_log(f"[INFO] LOG_CHAT OK: {utils.get_peer_id(log_entity)} | {await entity_title(log_entity)}")
            await log_send_permissions("LOG_CHAT", log_entity)
        except Exception as exc:  # noqa: BLE001
            raise ConfigError(f"Cannot access LOG_CHAT={CONFIG['log_chat']}: {exc}") from exc

    if (CONFIG.get("target_thread_id") or CONFIG.get("source_topic_map") or CONFIG.get("topic_route_map") or any(r.target_thread_id for routes in CONFIG.get("route_map", {}).values() for r in routes)) and CONFIG.get("copy_mode") == "forward":
        await telegram_log(
            "[WARN] TARGET_THREAD_ID/SOURCE_TOPIC_MAP are best supported in COPY_MODE=copy. "
            "Forward mode may ignore forum topic placement in some Telethon/Telegram cases.",
            "warning",
        )

    if CONFIG.get("source_topic_map"):
        await telegram_log(f"[INFO] SOURCE_TOPIC_MAP: {CONFIG['source_topic_map']}")
    await telegram_log(f"[INFO] DEFAULT_SOURCE_KEYS: {CONFIG.get('default_source_keys', set())}")
    if CONFIG.get("topic_route_map"):
        await telegram_log(f"[INFO] TOPIC_ROUTE_MAP: {CONFIG['topic_route_map']}")
    if CONFIG.get("route_map"):
        await telegram_log(f"[INFO] ROUTE_MAP: {CONFIG['route_map']}")

    await telegram_log("[INFO] Preflight check passed")


if client is not None:

    @client.on(events.NewMessage(chats=CONFIG["source_chats"]))
    async def on_new_message(event: events.NewMessage.Event) -> None:
        message = event.message

        # Do not block outgoing channel posts here.
        # If this account is owner/admin of a source channel, Telegram may mark
        # source posts as message.out=True. Duplicate protection and route checks
        # protect from loops, while this keeps owned channels copyable.
        # Grouped media is handled by Album event to avoid duplicates.
        if CONFIG["enable_albums"] and getattr(message, "grouped_id", None):
            return

        source_chat_id = get_chat_id_from_event(event)
        base_duplicate_key = f"msg:{source_chat_id}:{message.id}"
        routes = resolve_target_routes(source_chat_id, message)
        if not routes:
            await telegram_log(f"[INFO] No route for message, skipped: {base_duplicate_key}")
            return
        for route in routes:
            duplicate_key = f"{base_duplicate_key}:{route_suffix(route)}"
            job = RepostJob(
                kind="message",
                duplicate_key=duplicate_key,
                source_chat_id=source_chat_id,
                from_peer=event.chat_id,
                target_chat=route.target_chat,
                target_thread_id=route.target_thread_id,
                source_label=source_label_from_event(source_chat_id, event) if route.label_source else None,
                message=message,
            )
            await enqueue_job(job)

    if CONFIG["enable_albums"]:

        @client.on(events.Album(chats=CONFIG["source_chats"]))
        async def on_album(event: events.Album.Event) -> None:
            messages = list(event.messages)
            if not messages:
                return

            # Do not block outgoing albums from owned/admin source channels.
            source_chat_id = get_chat_id_from_event(event)
            grouped_id = getattr(messages[0], "grouped_id", None) or "-".join(str(m.id) for m in messages)
            base_duplicate_key = f"album:{source_chat_id}:{grouped_id}"
            routes = resolve_target_routes(source_chat_id, messages[0])
            if not routes:
                await telegram_log(f"[INFO] No route for album, skipped: {base_duplicate_key}")
                return
            for route in routes:
                duplicate_key = f"{base_duplicate_key}:{route_suffix(route)}"
                job = RepostJob(
                    kind="album",
                    duplicate_key=duplicate_key,
                    source_chat_id=source_chat_id,
                    from_peer=event.chat_id,
                    target_chat=route.target_chat,
                    target_thread_id=route.target_thread_id,
                    source_label=source_label_from_event(source_chat_id, event) if route.label_source else None,
                    messages=messages,
                )
                await enqueue_job(job)

    if CONFIG.get("sync_edits"):

        @client.on(events.MessageEdited(chats=CONFIG["source_chats"]))
        async def on_message_edited(event: events.MessageEdited.Event) -> None:
            source_chat_id = get_chat_id_from_event(event)
            await telegram_log(f"[INFO] Edit event received: {source_chat_id}:{getattr(event.message, 'id', 'unknown')}")
            await sync_edited_message(source_chat_id, event.message)

    if CONFIG.get("sync_deletes"):

        @client.on(events.MessageDeleted(chats=CONFIG["source_chats"]))
        async def on_message_deleted(event: events.MessageDeleted.Event) -> None:
            source_chat_id = get_chat_id_from_event(event)
            deleted_ids = getattr(event, "deleted_ids", []) or []
            await telegram_log(f"[INFO] Delete event received: {source_chat_id}:{list(deleted_ids)}")
            await sync_deleted_messages(source_chat_id, deleted_ids)


async def main() -> None:
    global processed_store, job_queue, queue_worker_task

    if CONFIG_ERROR is not None:
        raise CONFIG_ERROR
    if client is None:
        raise ConfigError("Telegram client was not initialized")

    processed_store = ProcessedStore(
        CONFIG["processed_db_path"],
        enabled=CONFIG["enable_duplicate_protection"] and CONFIG["persist_processed"],
    )
    processed_store.open()

    job_queue = asyncio.Queue(maxsize=CONFIG["queue_maxsize"])

    # Do not use client.start() here: on Railway it can try to ask for a phone/code
    # if SESSION_STRING is invalid. We want a clean error instead of an interactive hang.
    await client.connect()
    if not await client.is_user_authorized():
        raise ConfigError("SESSION_STRING is not authorized. Generate it again with generate_session.py")

    await telegram_log("[INFO] Userbot started")

    me = await client.get_me()
    await telegram_log(f"[INFO] Connected as: {getattr(me, 'username', None) or me.id}")
    await telegram_log(f"[INFO] SOURCE_CHATS: {CONFIG['source_chats']}")
    await telegram_log(f"[INFO] TARGET_CHAT: {CONFIG['target_chat']}")
    await telegram_log(f"[INFO] TARGET_THREAD_ID: {CONFIG['target_thread_id']}")
    await telegram_log(f"[INFO] SOURCE_TOPIC_MAP: {CONFIG['source_topic_map']}")
    await telegram_log(f"[INFO] DEFAULT_SOURCE_KEYS: {CONFIG.get('default_source_keys', set())}")
    await telegram_log(f"[INFO] COPY_MODE: {CONFIG['copy_mode']}")
    await telegram_log(f"[INFO] ENABLE_ALBUMS: {CONFIG['enable_albums']}")
    await telegram_log(f"[INFO] PERSIST_PROCESSED: {CONFIG['persist_processed']}")
    await telegram_log(f"[INFO] SYNC_DELETES: {CONFIG['sync_deletes']}")
    await telegram_log(f"[INFO] SYNC_EDITS: {CONFIG['sync_edits']}")

    await preflight_check()

    queue_worker_task = asyncio.create_task(queue_worker())
    await telegram_log("[INFO] Userbot is running. Waiting for new posts...")

    try:
        await client.run_until_disconnected()
    finally:
        if queue_worker_task is not None:
            queue_worker_task.cancel()
            try:
                await queue_worker_task
            except asyncio.CancelledError:
                pass
        if processed_store is not None:
            processed_store.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except ConfigError as exc:
        logger.error("Config error: %s", exc)
        raise SystemExit(1) from exc
    except KeyboardInterrupt:
        logger.info("Stopped by user")
