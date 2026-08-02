from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

AI_CHANNEL_MEMORY_WINDOW = 10
AI_CHANNEL_MEMORY_MIN_WINDOW = 5
AI_CHANNEL_MEMORY_FETCH_LIMIT = 50
AI_CHANNEL_POST_TEXT_MAX_LENGTH = 32_768
AI_CHANNEL_PROFILE_LIST_MAX_ITEMS = 50
AI_CHANNEL_PROFILE_ITEM_MAX_LENGTH = 128
MAX_DATABASE_ID = 9_223_372_036_854_775_807


class AIChannelMemoryError(RuntimeError):
    """Channel memory is missing, corrupt or cannot be updated safely."""


class AIChannelMemoryConflictError(AIChannelMemoryError):
    """A version guard rejected an update from a stale UI or transaction."""


@dataclass(frozen=True, slots=True)
class AIChannelProfileFieldSpec:
    code: str
    label: str
    prompt: str
    kind: Literal["text", "list"]
    max_length: int


AI_CHANNEL_PROFILE_FIELDS: dict[str, AIChannelProfileFieldSpec] = {
    "description": AIChannelProfileFieldSpec(
        "d", "📝 Описание", "Описание канала и его назначения", "text", 2_000
    ),
    "topic": AIChannelProfileFieldSpec(
        "t", "🏷 Тематика", "Краткая тематика канала", "text", 128
    ),
    "target_audience": AIChannelProfileFieldSpec(
        "u", "👥 Целевая аудитория", "Кто читает канал", "text", 1_000
    ),
    "language": AIChannelProfileFieldSpec(
        "l", "🌐 Язык", "Основной язык канала и комментариев", "text", 64
    ),
    "author_style": AIChannelProfileFieldSpec(
        "a", "✍️ Стиль автора", "Манера и тон автора", "text", 1_000
    ),
    "audience_style": AIChannelProfileFieldSpec(
        "g", "🗣 Стиль аудитории", "Манера общения аудитории", "text", 1_000
    ),
    "typical_comment_length": AIChannelProfileFieldSpec(
        "c", "📏 Длина комментариев", "Типичная длина комментария", "text", 128
    ),
    "main_terms": AIChannelProfileFieldSpec(
        "k", "🔑 Основные термины", "Термины через запятую или с новой строки", "list", 0
    ),
    "methodology": AIChannelProfileFieldSpec(
        "m", "📐 Методология", "Методы автора через запятую или с новой строки", "list", 0
    ),
    "allowed_topics": AIChannelProfileFieldSpec(
        "y", "✅ Разрешённые темы", "Разрешённые темы через запятую", "list", 0
    ),
    "forbidden_topics": AIChannelProfileFieldSpec(
        "f", "⛔ Запрещённые темы", "Запрещённые темы через запятую", "list", 0
    ),
    "advertising_restrictions": AIChannelProfileFieldSpec(
        "r", "📣 Реклама", "Рекламные ограничения канала", "text", 1_000
    ),
    "admin_notes": AIChannelProfileFieldSpec(
        "n", "🗒 Заметки", "Ручные заметки администратора", "text", 4_000
    ),
    "memory_summary": AIChannelProfileFieldSpec(
        "s", "🧠 Summary", "Контролируемая сводка памяти канала", "text", 4_000
    ),
}
AI_CHANNEL_PROFILE_FIELD_CODES = {
    spec.code: field for field, spec in AI_CHANNEL_PROFILE_FIELDS.items()
}


@dataclass(frozen=True, slots=True)
class AIChannelPostPayload:
    telegram_message_id: int
    posted_at: datetime
    edited_at: datetime | None = None
    text: str | None = None
    media_type: str | None = None
    media_caption: str | None = None
    detected_topics: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class AIChannelPostSnapshot:
    id: int
    channel_id: int | None
    telegram_channel_id: int
    telegram_message_id: int
    source_revision: int
    posted_at: datetime
    edited_at: datetime | None
    text: str | None
    media_type: str | None
    media_caption: str | None
    normalized_text_hash: str
    detected_topics: tuple[str, ...]
    linked_scenario_id: int | None
    deleted_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class AIChannelPostRevisionSnapshot:
    id: int
    post_id: int
    channel_id: int | None
    telegram_channel_id: int
    telegram_message_id: int
    source_revision: int
    revision_reason: str
    posted_at: datetime
    edited_at: datetime | None
    text: str | None
    media_type: str | None
    media_caption: str | None
    normalized_text_hash: str
    detected_topics: tuple[str, ...]
    deleted_at: datetime | None
    recorded_at: datetime


@dataclass(frozen=True, slots=True)
class AIChannelPostIngestResult:
    status: Literal["created", "updated", "unchanged", "stale", "restored", "deleted"]
    post: AIChannelPostSnapshot


@dataclass(frozen=True, slots=True)
class AIChannelScenarioSnapshot:
    id: int
    channel_id: int | None
    telegram_channel_id: int | None
    title: str
    symbol: str | None
    direction: str | None
    status: str
    factual_summary: str | None
    opened_by_post_id: int | None
    closed_by_post_id: int | None
    started_at: datetime
    ended_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class AIChannelProfileSnapshot:
    id: int
    channel_id: int | None
    telegram_channel_id: int | None
    channel_title: str
    description: str | None
    topic: str | None
    target_audience: str | None
    language: str | None
    author_style: str | None
    audience_style: str | None
    typical_comment_length: str | None
    main_terms: tuple[str, ...]
    methodology: tuple[str, ...]
    allowed_topics: tuple[str, ...]
    forbidden_topics: tuple[str, ...]
    advertising_restrictions: str | None
    admin_notes: str | None
    memory_summary: str | None
    enabled: bool
    profile_version: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class AIChannelMemorySnapshot:
    profile: AIChannelProfileSnapshot
    recent_posts: tuple[AIChannelPostSnapshot, ...]
    active_scenarios: tuple[AIChannelScenarioSnapshot, ...]
    stored_post_count: int
    deleted_post_count: int


def normalize_ai_datetime(value: datetime, *, field: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field} должен содержать дату и время")
    if value.tzinfo is not None:
        value = value.astimezone(UTC).replace(tzinfo=None)
    return value.replace(microsecond=value.microsecond)


def normalize_ai_memory_text(
    value: str | None,
    *,
    field: str,
    max_length: int,
    allow_empty: bool = True,
) -> str | None:
    if value is None:
        if allow_empty:
            return None
        raise ValueError(f"{field} не может быть пустым")
    if not isinstance(value, str):
        raise TypeError(f"{field} должен быть строкой")
    normalized = value.replace("\x00", "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not normalized:
        if allow_empty:
            return None
        raise ValueError(f"{field} не может быть пустым")
    if len(normalized) > max_length:
        raise ValueError(f"{field}: максимум {max_length} символов")
    return normalized


def normalize_ai_memory_list(value: object, *, field: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        raw_items = re.split(r"[,;\n]+", value)
    elif isinstance(value, (list, tuple)):
        raw_items = list(value)
    else:
        raise TypeError(f"{field} должен быть списком")
    result: list[str] = []
    seen: set[str] = set()
    for raw in raw_items:
        if not isinstance(raw, str):
            raise TypeError(f"{field} содержит нестроковое значение")
        item = normalize_ai_memory_text(
            raw,
            field=field,
            max_length=AI_CHANNEL_PROFILE_ITEM_MAX_LENGTH,
        )
        if item is None:
            continue
        key = item.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    if len(result) > AI_CHANNEL_PROFILE_LIST_MAX_ITEMS:
        raise ValueError(
            f"{field}: максимум {AI_CHANNEL_PROFILE_LIST_MAX_ITEMS} значений"
        )
    return tuple(result)


def parse_ai_channel_profile_input(field: str, raw: str | None) -> object:
    spec = AI_CHANNEL_PROFILE_FIELDS.get(field)
    if spec is None:
        raise ValueError("Некорректное поле профиля канала")
    if raw is None:
        raise ValueError("Отправьте текстовое значение")
    if raw.strip() == "-":
        return () if spec.kind == "list" else None
    if spec.kind == "list":
        return normalize_ai_memory_list(raw, field=spec.label)
    return normalize_ai_memory_text(
        raw,
        field=spec.label,
        max_length=spec.max_length,
        allow_empty=False,
    )


def canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def decode_json_object(raw: str, *, field: str) -> dict[str, object]:
    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise AIChannelMemoryError(f"{field} содержит некорректный JSON") from exc
    if not isinstance(value, dict):
        raise AIChannelMemoryError(f"{field} должен содержать JSON object")
    return dict(value)


def decode_json_list(raw: str, *, field: str) -> tuple[str, ...]:
    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise AIChannelMemoryError(f"{field} содержит некорректный JSON") from exc
    try:
        return normalize_ai_memory_list(value, field=field)
    except (TypeError, ValueError) as exc:
        raise AIChannelMemoryError(str(exc)) from exc


def normalized_ai_channel_post_hash(payload: AIChannelPostPayload) -> str:
    body = {
        "media_caption": payload.media_caption,
        "media_type": payload.media_type,
        "text": payload.text,
    }
    normalized = unicodedata.normalize("NFKC", canonical_json(body))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def validate_ai_channel_post_payload(payload: AIChannelPostPayload) -> AIChannelPostPayload:
    message_id = payload.telegram_message_id
    if (
        not isinstance(message_id, int)
        or isinstance(message_id, bool)
        or not 0 < message_id <= MAX_DATABASE_ID
    ):
        raise ValueError("Некорректный ID публикации")
    posted_at = normalize_ai_datetime(payload.posted_at, field="posted_at")
    edited_at = (
        normalize_ai_datetime(payload.edited_at, field="edited_at")
        if payload.edited_at is not None
        else None
    )
    if edited_at is not None and edited_at < posted_at:
        raise ValueError("edited_at не может быть раньше posted_at")
    text = normalize_ai_memory_text(
        payload.text,
        field="Текст публикации",
        max_length=AI_CHANNEL_POST_TEXT_MAX_LENGTH,
    )
    caption = normalize_ai_memory_text(
        payload.media_caption,
        field="Подпись к медиа",
        max_length=AI_CHANNEL_POST_TEXT_MAX_LENGTH,
    )
    media_type = normalize_ai_memory_text(
        payload.media_type,
        field="Тип медиа",
        max_length=32,
    )
    if text is None and caption is None and media_type is None:
        raise ValueError("Публикация не содержит текста или медиа")
    topics = normalize_ai_memory_list(
        payload.detected_topics,
        field="Темы публикации",
    )
    return AIChannelPostPayload(
        telegram_message_id=message_id,
        posted_at=posted_at,
        edited_at=edited_at,
        text=text,
        media_type=media_type,
        media_caption=caption,
        detected_topics=topics,
    )


def _message_media_type(message: object, *, grouped: bool) -> str | None:
    if grouped:
        return "album"
    if getattr(message, "photo", None) is not None:
        return "photo"
    document = getattr(message, "document", None)
    if document is not None:
        mime_type = str(getattr(document, "mime_type", "") or "").casefold()
        if mime_type.startswith("image/"):
            return "image"
        if mime_type.startswith("video/"):
            return "video"
        if mime_type.startswith("audio/"):
            return "audio"
        return "document"
    media = getattr(message, "media", None)
    if media is not None:
        name = type(media).__name__.casefold()
        if "webpage" in name:
            return "webpage"
        if "poll" in name:
            return "poll"
        return "media"
    return None


def _message_text(message: object) -> str | None:
    value = getattr(message, "message", None)
    if value is None:
        value = getattr(message, "raw_text", None)
    if not isinstance(value, str):
        return None
    return normalize_ai_memory_text(
        value,
        field="Текст Telegram-публикации",
        max_length=AI_CHANNEL_POST_TEXT_MAX_LENGTH,
    )


def canonical_ai_channel_post_payloads(
    messages: object,
    *,
    limit: int = AI_CHANNEL_MEMORY_WINDOW,
) -> tuple[AIChannelPostPayload, ...]:
    if not AI_CHANNEL_MEMORY_MIN_WINDOW <= limit <= AI_CHANNEL_MEMORY_WINDOW:
        raise ValueError("Окно памяти должно содержать от 5 до 10 публикаций")
    groups: dict[tuple[str, int], list[object]] = {}
    for message in list(messages or ()):
        if getattr(message, "action", None) is not None:
            continue
        message_id = getattr(message, "id", None)
        posted_at = getattr(message, "date", None)
        if (
            not isinstance(message_id, int)
            or isinstance(message_id, bool)
            or message_id <= 0
            or not isinstance(posted_at, datetime)
        ):
            continue
        grouped_id = getattr(message, "grouped_id", None)
        key = ("group", int(grouped_id)) if grouped_id else ("message", message_id)
        groups.setdefault(key, []).append(message)

    payloads: list[AIChannelPostPayload] = []
    for grouped_messages in groups.values():
        ordered = sorted(grouped_messages, key=lambda item: int(item.id))
        grouped = len(ordered) > 1 or getattr(ordered[0], "grouped_id", None) is not None
        texts: list[str] = []
        for item in ordered:
            value = _message_text(item)
            if value and value not in texts:
                texts.append(value)
        posted_values = [
            normalize_ai_datetime(item.date, field="Telegram date") for item in ordered
        ]
        edited_values = [
            normalize_ai_datetime(item.edit_date, field="Telegram edit_date")
            for item in ordered
            if isinstance(getattr(item, "edit_date", None), datetime)
        ]
        media_type = _message_media_type(ordered[0], grouped=grouped)
        combined = "\n\n".join(texts) or None
        payloads.append(
            validate_ai_channel_post_payload(
                AIChannelPostPayload(
                    telegram_message_id=min(int(item.id) for item in ordered),
                    posted_at=min(posted_values),
                    edited_at=max(edited_values) if edited_values else None,
                    text=None if media_type is not None else combined,
                    media_type=media_type,
                    media_caption=combined if media_type is not None else None,
                )
            )
        )

    payloads.sort(key=lambda item: (item.posted_at, item.telegram_message_id))
    return tuple(payloads[-limit:])
