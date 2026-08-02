from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Final

MAX_DATABASE_ID: Final[int] = 9_223_372_036_854_775_807
AI_ACCOUNT_PROFILE_PAGE_SIZE: Final[int] = 8
AI_ACCOUNT_PROFILE_UPDATE_ATTEMPTS: Final[int] = 4
AI_ACCOUNT_PROFILE_HISTORY_LIMIT: Final[int] = 20
AI_ACCOUNT_REPLY_HISTORY_LIMIT: Final[int] = 20
AI_ACCOUNT_PROFILE_MAX_MESSAGE_LENGTH: Final[int] = 1_000
AI_ACCOUNT_PROFILE_MAX_DAILY_LIMIT: Final[int] = 100
AI_ACCOUNT_PROFILE_MAX_REPLY_BONUS_SLOTS: Final[int] = 20
AI_ACCOUNT_PROFILE_MAX_COOLDOWN_SECONDS: Final[int] = 7 * 24 * 60 * 60
AI_ACCOUNT_PROFILE_SIMILARITY_THRESHOLD: Final[Decimal] = Decimal("0.78")

AI_ACCOUNT_KNOWLEDGE_LEVELS: Final[dict[str, str]] = {
    "novice": "Новичок",
    "basic": "Базовый",
    "intermediate": "Средний",
    "advanced": "Продвинутый",
}

AI_ACCOUNT_ROLES: Final[dict[str, str]] = {
    "asks": "Спрашивает",
    "answers": "Отвечает по основам",
    "asks_entry": "Торопится со входом",
    "cautions": "Осторожно уточняет",
    "doubts": "Сомневается",
    # v1/v2 allowed a free-form role and the historical migration fixture used
    # this value. Keep it readable/editable so upgrading never discards a valid
    # pre-step-9 persona. New automatic profiles use ``cautions`` above.
    "cautious": "Осторожный (совместимость)",
}

AI_ACCOUNT_PRESET_LABELS: Final[dict[str, str]] = {
    "novice": "Новичок",
    "basics": "Знающий основы",
    "rushed": "Торопливый",
    "cautious": "Осторожный",
    "skeptic": "Сомневающийся",
}

AI_ACCOUNT_UPPERCASE_MODES: Final[dict[str, str]] = {
    "never": "не использует",
    "rare": "редко",
    "sometimes": "иногда",
}

AI_ACCOUNT_PUNCTUATION_MODES: Final[dict[str, str]] = {
    "minimal": "минимальная",
    "loose": "разговорная",
    "clean": "аккуратная",
}

AI_ACCOUNT_MISTAKE_LEVELS: Final[dict[str, str]] = {
    "none": "без намеренных ошибок",
    "light": "редкие небольшие ошибки",
}

PROFILE_REVISION_REASONS: Final[frozenset[str]] = frozenset(
    {
        "auto_created",
        "backfill",
        "updated",
        "regenerated",
        "enabled",
        "disabled",
        "retired",
        "restored",
        "reattached",
    }
)

_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_WHITESPACE_RE = re.compile(r"[ \t\f\v]+")
_RATE_QUANT = Decimal("0.0001")


class AIAccountProfileError(RuntimeError):
    """A managed account profile is missing, invalid or unavailable."""


class AIAccountProfileConflictError(AIAccountProfileError):
    """An optimistic profile update lost a race and must be retried from UI."""


@dataclass(frozen=True, slots=True)
class AIAccountProfileData:
    name: str
    knowledge_level: str
    role: str
    style: dict[str, object]
    allowed_claims: tuple[str, ...]
    forbidden_claims: tuple[str, ...]
    min_length: int
    max_length: int
    emoji_rate: Decimal
    question_rate: Decimal
    reply_rate: Decimal
    disagreement_rate: Decimal
    daily_limit: int
    reply_bonus_slots: int
    cooldown_seconds: int


@dataclass(frozen=True, slots=True)
class AIAccountProfileSnapshot:
    id: int
    account_id: int | None
    telegram_user_id: int | None
    account_display_name: str | None
    account_username: str | None
    account_is_active: bool
    account_status: str | None
    account_has_session: bool
    name: str
    knowledge_level: str
    role: str
    preset_key: str | None
    tone: str | None
    vocabulary: str | None
    uppercase_mode: str
    punctuation_mode: str
    mistake_level: str
    favorite_words: tuple[str, ...]
    sentence_pattern: str | None
    persona_key: str | None
    generation: int
    allowed_claims: tuple[str, ...]
    forbidden_claims: tuple[str, ...]
    min_length: int
    max_length: int
    emoji_rate: Decimal
    question_rate: Decimal
    reply_rate: Decimal
    disagreement_rate: Decimal
    daily_limit: int
    cooldown_seconds: int
    enabled: bool
    retired: bool
    profile_version: int
    created_at: datetime
    updated_at: datetime
    reply_bonus_slots: int = 3


@dataclass(frozen=True, slots=True)
class AIAccountProfileRevisionSnapshot:
    id: int
    profile_id: int
    telegram_user_id: int | None
    profile_version: int
    change_reason: str
    snapshot_json: str
    snapshot_hash: str
    changed_by: int | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class AIAccountReplySnapshot:
    id: int
    text: str | None
    status: str
    telegram_message_id: int | None
    reply_to_telegram_message_id: int | None
    published_at: datetime | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class AIAccountProfileEligibility:
    profile_id: int
    allowed: bool
    reason: str
    comments_today: int
    daily_limit: int
    last_published_at: datetime | None
    cooldown_remaining_seconds: int
    reply_bonus_granted: int = 0
    reply_bonus_used: int = 0
    reply_bonus_available: int = 0
    day_key: str = ""
    day_timezone: str = "UTC"


def canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def validate_database_id(value: int, *, field: str) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 < value <= MAX_DATABASE_ID
    ):
        raise ValueError(f"Некорректный {field}")
    return value


def normalize_profile_text(
    value: object,
    *,
    field: str,
    max_length: int,
    allow_empty: bool = False,
) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} должен быть текстом")
    normalized = _CONTROL_RE.sub("", value.replace("\r\n", "\n").replace("\r", "\n"))
    normalized = "\n".join(
        _WHITESPACE_RE.sub(" ", line).strip() for line in normalized.split("\n")
    ).strip()
    if not normalized and not allow_empty:
        raise ValueError(f"{field} не может быть пустым")
    if len(normalized) > max_length:
        raise ValueError(f"{field} длиннее {max_length} символов")
    return normalized


def normalize_profile_list(
    value: object,
    *,
    field: str,
    max_items: int = 20,
    max_item_length: int = 120,
) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        raw_items: Iterable[object] = re.split(r"[,\n]", value)
    elif isinstance(value, (tuple, list)):
        raw_items = value
    else:
        raise TypeError(f"{field} должен быть списком или текстом через запятую")
    result: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        normalized = normalize_profile_text(
            item,
            field=field,
            max_length=max_item_length,
            allow_empty=True,
        )
        if not normalized or normalized == "-":
            continue
        key = normalized.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(normalized)
        if len(result) > max_items:
            raise ValueError(f"{field} содержит больше {max_items} значений")
    return tuple(result)


def decode_json_object(value: str, *, field: str) -> dict[str, object]:
    try:
        decoded = json.loads(value)
    except (TypeError, json.JSONDecodeError) as exc:
        raise AIAccountProfileError(f"{field} содержит некорректный JSON") from exc
    if not isinstance(decoded, dict):
        raise AIAccountProfileError(f"{field} должен содержать JSON object")
    return decoded


def decode_json_list(value: str, *, field: str) -> tuple[str, ...]:
    try:
        decoded = json.loads(value)
    except (TypeError, json.JSONDecodeError) as exc:
        raise AIAccountProfileError(f"{field} содержит некорректный JSON") from exc
    if not isinstance(decoded, list) or not all(isinstance(item, str) for item in decoded):
        raise AIAccountProfileError(f"{field} должен содержать JSON-массив строк")
    try:
        return normalize_profile_list(decoded, field=field)
    except (TypeError, ValueError) as exc:
        raise AIAccountProfileError(str(exc)) from exc


def normalize_rate(value: object, *, field: str) -> Decimal:
    if isinstance(value, bool):
        raise TypeError(f"{field} должен быть числом от 0 до 1")
    try:
        rate = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise TypeError(f"{field} должен быть числом от 0 до 1") from exc
    if not rate.is_finite() or rate < 0 or rate > 1:
        raise ValueError(f"{field} должен быть от 0 до 1")
    return rate.quantize(_RATE_QUANT, rounding=ROUND_HALF_UP)


def _bounded_int(value: object, *, field: str, minimum: int, maximum: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field} должен быть целым числом")
    if not minimum <= value <= maximum:
        raise ValueError(f"{field} должен быть от {minimum} до {maximum}")
    return value


def _style_text(
    style: Mapping[str, object],
    key: str,
    *,
    label: str,
    max_length: int,
) -> str | None:
    value = style.get(key)
    if value is None or value == "":
        return None
    return normalize_profile_text(
        value,
        field=label,
        max_length=max_length,
    )


def normalize_style(value: object) -> dict[str, object]:
    if value is None:
        raw: Mapping[str, object] = {}
    elif isinstance(value, Mapping):
        raw = value
    else:
        raise TypeError("Стиль профиля должен быть объектом")
    uppercase_mode = str(raw.get("uppercase_mode", "rare"))
    punctuation_mode = str(raw.get("punctuation_mode", "loose"))
    mistake_level = str(raw.get("mistake_level", "light"))
    if uppercase_mode not in AI_ACCOUNT_UPPERCASE_MODES:
        raise ValueError("Некорректный режим заглавных букв")
    if punctuation_mode not in AI_ACCOUNT_PUNCTUATION_MODES:
        raise ValueError("Некорректный режим пунктуации")
    if mistake_level not in AI_ACCOUNT_MISTAKE_LEVELS:
        raise ValueError("Некорректный уровень небольших ошибок")
    retired_raw = raw.get("retired", False)
    if type(retired_raw) is not bool:
        raise TypeError("retired должен быть boolean")
    generation_raw = raw.get("generation", 0)
    generation = _bounded_int(
        generation_raw,
        field="Номер генерации",
        minimum=0,
        maximum=1_000_000,
    )
    reply_bonus_slots = _bounded_int(
        raw.get("reply_bonus_slots", 3),
        field="Бонусных слотов за ответы",
        minimum=0,
        maximum=AI_ACCOUNT_PROFILE_MAX_REPLY_BONUS_SLOTS,
    )
    result: dict[str, object] = {
        "uppercase_mode": uppercase_mode,
        "punctuation_mode": punctuation_mode,
        "mistake_level": mistake_level,
        "favorite_words": list(
            normalize_profile_list(
                raw.get("favorite_words"),
                field="Любимые слова",
                max_items=12,
                max_item_length=60,
            )
        ),
        "retired": retired_raw,
        "generation": generation,
        "reply_bonus_slots": reply_bonus_slots,
    }
    for key, label, limit in (
        ("preset_key", "Preset", 32),
        ("tone", "Тон", 160),
        ("vocabulary", "Словарный запас", 240),
        ("sentence_pattern", "Манера фраз", 240),
        ("persona_key", "Ключ личности", 32),
    ):
        normalized = _style_text(raw, key, label=label, max_length=limit)
        if normalized is not None:
            result[key] = normalized
    return result


def normalize_profile_data(values: Mapping[str, object]) -> AIAccountProfileData:
    name = normalize_profile_text(
        values.get("name"),
        field="Название профиля",
        max_length=120,
    )
    knowledge_level = str(values.get("knowledge_level", ""))
    role = str(values.get("role", ""))
    if knowledge_level not in AI_ACCOUNT_KNOWLEDGE_LEVELS:
        raise ValueError("Некорректный уровень знаний")
    if role not in AI_ACCOUNT_ROLES:
        raise ValueError("Некорректная роль профиля")
    min_length = _bounded_int(
        values.get("min_length"),
        field="Минимальная длина",
        minimum=1,
        maximum=AI_ACCOUNT_PROFILE_MAX_MESSAGE_LENGTH,
    )
    max_length = _bounded_int(
        values.get("max_length"),
        field="Максимальная длина",
        minimum=1,
        maximum=AI_ACCOUNT_PROFILE_MAX_MESSAGE_LENGTH,
    )
    if max_length < min_length:
        raise ValueError("Максимальная длина не может быть меньше минимальной")
    daily_limit = _bounded_int(
        values.get("daily_limit"),
        field="Дневной лимит",
        minimum=0,
        maximum=AI_ACCOUNT_PROFILE_MAX_DAILY_LIMIT,
    )
    style = normalize_style(values.get("style"))
    reply_bonus_slots = _bounded_int(
        values.get("reply_bonus_slots", style.get("reply_bonus_slots", 3)),
        field="Бонусных слотов за ответы",
        minimum=0,
        maximum=AI_ACCOUNT_PROFILE_MAX_REPLY_BONUS_SLOTS,
    )
    style["reply_bonus_slots"] = reply_bonus_slots
    cooldown_seconds = _bounded_int(
        values.get("cooldown_seconds"),
        field="Cooldown",
        minimum=0,
        maximum=AI_ACCOUNT_PROFILE_MAX_COOLDOWN_SECONDS,
    )
    allowed_claims = normalize_profile_list(
        values.get("allowed_claims"),
        field="Разрешённые утверждения",
    )
    forbidden_claims = normalize_profile_list(
        values.get("forbidden_claims"),
        field="Запрещённые утверждения",
    )
    overlap = {item.casefold() for item in allowed_claims}.intersection(
        item.casefold() for item in forbidden_claims
    )
    if overlap:
        raise ValueError(
            "Одно утверждение не может быть одновременно разрешённым и запрещённым"
        )
    return AIAccountProfileData(
        name=name,
        knowledge_level=knowledge_level,
        role=role,
        style=style,
        allowed_claims=allowed_claims,
        forbidden_claims=forbidden_claims,
        min_length=min_length,
        max_length=max_length,
        emoji_rate=normalize_rate(values.get("emoji_rate"), field="Частота эмодзи"),
        question_rate=normalize_rate(
            values.get("question_rate"), field="Склонность задавать вопросы"
        ),
        reply_rate=normalize_rate(
            values.get("reply_rate"), field="Склонность отвечать"
        ),
        disagreement_rate=normalize_rate(
            values.get("disagreement_rate"), field="Склонность возражать"
        ),
        daily_limit=daily_limit,
        reply_bonus_slots=reply_bonus_slots,
        cooldown_seconds=cooldown_seconds,
    )


def profile_data_database_values(data: AIAccountProfileData) -> dict[str, object]:
    return {
        "name": data.name,
        "knowledge_level": data.knowledge_level,
        "role": data.role,
        "style_json": canonical_json(data.style),
        "allowed_claims_json": canonical_json(list(data.allowed_claims)),
        "forbidden_claims_json": canonical_json(list(data.forbidden_claims)),
        "min_length": data.min_length,
        "max_length": data.max_length,
        "emoji_rate": data.emoji_rate,
        "question_rate": data.question_rate,
        "reply_rate": data.reply_rate,
        "disagreement_rate": data.disagreement_rate,
        "daily_limit": data.daily_limit,
        "cooldown_seconds": data.cooldown_seconds,
    }


def profile_style_signature(style: Mapping[str, object], *, role: str) -> tuple[str, ...]:
    favorites = tuple(
        item.casefold()
        for item in normalize_profile_list(
            style.get("favorite_words"),
            field="Любимые слова",
            max_items=12,
            max_item_length=60,
        )[:3]
    )
    return (
        str(style.get("preset_key", "")),
        role,
        str(style.get("tone", "")),
        str(style.get("vocabulary", "")),
        str(style.get("uppercase_mode", "")),
        str(style.get("punctuation_mode", "")),
        str(style.get("mistake_level", "")),
        str(style.get("sentence_pattern", "")),
        "|".join(favorites),
    )


def profile_signatures_too_similar(
    left: Sequence[str],
    right: Sequence[str],
) -> bool:
    if len(left) != len(right) or not left:
        return False
    equal = sum(a == b for a, b in zip(left, right, strict=True))
    score = Decimal(equal) / Decimal(len(left))
    return score >= AI_ACCOUNT_PROFILE_SIMILARITY_THRESHOLD


_PRESETS: Final[tuple[dict[str, object], ...]] = (
    {
        "preset_key": "novice",
        "knowledge_level": "novice",
        "role": "asks",
        "min_length": 8,
        "max_length": 90,
        "emoji_rate": "0.12",
        "question_rate": "0.86",
        "reply_rate": "0.42",
        "disagreement_rate": "0.08",
        "daily_limit": 3,
        "reply_bonus_slots": 3,
        "cooldown_seconds": 10_800,
        "allowed_claims": ("уточнять термин", "задавать короткий вопрос"),
        "forbidden_claims": (
            "обещать прибыль",
            "утверждать точку входа как эксперт",
            "выдумывать личную сделку",
        ),
    },
    {
        "preset_key": "basics",
        "knowledge_level": "basic",
        "role": "answers",
        "min_length": 18,
        "max_length": 150,
        "emoji_rate": "0.08",
        "question_rate": "0.25",
        "reply_rate": "0.72",
        "disagreement_rate": "0.15",
        "daily_limit": 4,
        "reply_bonus_slots": 3,
        "cooldown_seconds": 9_000,
        "allowed_claims": ("объяснять базовый термин", "ссылаться на текст поста"),
        "forbidden_claims": (
            "обещать прибыль",
            "подтверждать актуальную цену без источника",
            "выдумывать личный опыт",
        ),
    },
    {
        "preset_key": "rushed",
        "knowledge_level": "basic",
        "role": "asks_entry",
        "min_length": 6,
        "max_length": 80,
        "emoji_rate": "0.18",
        "question_rate": "0.82",
        "reply_rate": "0.36",
        "disagreement_rate": "0.12",
        "daily_limit": 3,
        "reply_bonus_slots": 3,
        "cooldown_seconds": 12_600,
        "allowed_claims": ("спрашивать про подтверждение", "интересоваться входом"),
        "forbidden_claims": (
            "обещать прибыль",
            "называть выдуманную цену",
            "заявлять о несуществующей позиции",
        ),
    },
    {
        "preset_key": "cautious",
        "knowledge_level": "intermediate",
        "role": "cautions",
        "min_length": 20,
        "max_length": 180,
        "emoji_rate": "0.05",
        "question_rate": "0.40",
        "reply_rate": "0.62",
        "disagreement_rate": "0.26",
        "daily_limit": 4,
        "reply_bonus_slots": 3,
        "cooldown_seconds": 10_800,
        "allowed_claims": ("напоминать о подтверждении", "уточнять риск"),
        "forbidden_claims": (
            "обещать прибыль",
            "давать персональную гарантию",
            "подтверждать рынок без источника",
        ),
    },
    {
        "preset_key": "skeptic",
        "knowledge_level": "intermediate",
        "role": "doubts",
        "min_length": 16,
        "max_length": 170,
        "emoji_rate": "0.03",
        "question_rate": "0.64",
        "reply_rate": "0.68",
        "disagreement_rate": "0.58",
        "daily_limit": 3,
        "reply_bonus_slots": 3,
        "cooldown_seconds": 14_400,
        "allowed_claims": ("вежливо сомневаться", "просить фактическое подтверждение"),
        "forbidden_claims": (
            "обещать прибыль",
            "оскорблять автора",
            "выдумывать контраргумент как факт",
        ),
    },
)

_TONES: Final[tuple[str, ...]] = (
    "спокойный разговорный",
    "короткий и прямой",
    "доброжелательно осторожный",
    "сдержанный",
    "живой, но без пафоса",
    "слегка сомневающийся",
)
_VOCABULARIES: Final[tuple[str, ...]] = (
    "простые слова, минимум терминов",
    "базовые трейдерские термины",
    "разговорные сокращения без перегруза",
    "короткие технические формулировки",
    "простые вопросы по сути поста",
    "термины только из доступного контекста",
)
_SENTENCE_PATTERNS: Final[tuple[str, ...]] = (
    "одно короткое предложение",
    "короткий вопрос без вступления",
    "мысль и один уточняющий вопрос",
    "две короткие фразы",
    "сначала сомнение, затем вопрос",
    "краткое уточнение по факту поста",
)
_FAVORITE_WORD_GROUPS: Final[tuple[tuple[str, ...], ...]] = (
    ("получается", "тогда"),
    ("а тут", "верно"),
    ("пока", "логично"),
    ("по идее", "ждём"),
    ("понял", "уточню"),
    ("интересно", "значит"),
    ("а если", "сейчас"),
    ("без спешки", "подтверждение"),
)


def build_auto_profile(
    telegram_user_id: int,
    *,
    occupied_signatures: Sequence[Sequence[str]] = (),
    generation: int = 0,
) -> AIAccountProfileData:
    validate_database_id(telegram_user_id, field="Telegram ID")
    _bounded_int(
        generation,
        field="Номер генерации",
        minimum=0,
        maximum=1_000_000,
    )
    occupied = [tuple(item) for item in occupied_signatures]
    selected: AIAccountProfileData | None = None
    for attempt in range(64):
        digest = hashlib.sha256(
            f"likebot-ai-profile-v1:{telegram_user_id}:{generation}:{attempt}".encode()
        ).digest()
        digest_hex = hashlib.sha256(digest).hexdigest()
        preset = _PRESETS[digest[0] % len(_PRESETS)]
        preset_key = str(preset["preset_key"])
        style = {
            "preset_key": preset_key,
            "tone": _TONES[digest[1] % len(_TONES)],
            "vocabulary": _VOCABULARIES[digest[2] % len(_VOCABULARIES)],
            "uppercase_mode": ("never", "rare", "sometimes")[digest[3] % 3],
            "punctuation_mode": ("minimal", "loose", "clean")[digest[4] % 3],
            "mistake_level": ("none", "light")[digest[5] % 2],
            "favorite_words": list(
                _FAVORITE_WORD_GROUPS[digest[6] % len(_FAVORITE_WORD_GROUPS)]
            ),
            "sentence_pattern": _SENTENCE_PATTERNS[
                digest[7] % len(_SENTENCE_PATTERNS)
            ],
            "persona_key": digest_hex[:12],
            "retired": False,
            "generation": generation,
        }
        label = AI_ACCOUNT_PRESET_LABELS[preset_key]
        candidate = normalize_profile_data(
            {
                "name": f"{label} · {digest_hex[:4].upper()}",
                "knowledge_level": preset["knowledge_level"],
                "role": preset["role"],
                "style": style,
                "allowed_claims": preset["allowed_claims"],
                "forbidden_claims": preset["forbidden_claims"],
                "min_length": preset["min_length"],
                "max_length": preset["max_length"],
                "emoji_rate": preset["emoji_rate"],
                "question_rate": preset["question_rate"],
                "reply_rate": preset["reply_rate"],
                "disagreement_rate": preset["disagreement_rate"],
                "daily_limit": preset["daily_limit"],
                "reply_bonus_slots": preset["reply_bonus_slots"],
                "cooldown_seconds": preset["cooldown_seconds"],
            }
        )
        signature = profile_style_signature(candidate.style, role=candidate.role)
        if not any(
            profile_signatures_too_similar(signature, existing)
            for existing in occupied
        ):
            return candidate
        selected = candidate
    if selected is None:  # pragma: no cover - the loop always constructs a candidate
        raise AIAccountProfileError("Не удалось создать профиль аккаунта")
    # A finite style space may be exhausted on very large installations. The
    # stable persona key still makes the resulting profile distinct and auditable.
    return selected


def profile_data_from_snapshot(snapshot: AIAccountProfileSnapshot) -> AIAccountProfileData:
    return normalize_profile_data(
        {
            "name": snapshot.name,
            "knowledge_level": snapshot.knowledge_level,
            "role": snapshot.role,
            "style": {
                "preset_key": snapshot.preset_key,
                "tone": snapshot.tone,
                "vocabulary": snapshot.vocabulary,
                "uppercase_mode": snapshot.uppercase_mode,
                "punctuation_mode": snapshot.punctuation_mode,
                "mistake_level": snapshot.mistake_level,
                "favorite_words": list(snapshot.favorite_words),
                "sentence_pattern": snapshot.sentence_pattern,
                "persona_key": snapshot.persona_key,
                "retired": snapshot.retired,
                "generation": snapshot.generation,
            },
            "allowed_claims": snapshot.allowed_claims,
            "forbidden_claims": snapshot.forbidden_claims,
            "min_length": snapshot.min_length,
            "max_length": snapshot.max_length,
            "emoji_rate": snapshot.emoji_rate,
            "question_rate": snapshot.question_rate,
            "reply_rate": snapshot.reply_rate,
            "disagreement_rate": snapshot.disagreement_rate,
            "daily_limit": snapshot.daily_limit,
            "reply_bonus_slots": snapshot.reply_bonus_slots,
            "cooldown_seconds": snapshot.cooldown_seconds,
        }
    )


def profile_revision_payload(
    *,
    profile_id: int,
    account_id: int | None,
    telegram_user_id: int | None,
    data: AIAccountProfileData,
    enabled: bool,
    profile_version: int,
) -> dict[str, object]:
    payload = {
        "schema": 1,
        "profile_id": profile_id,
        "account_id": account_id,
        "telegram_user_id": telegram_user_id,
        "name": data.name,
        "knowledge_level": data.knowledge_level,
        "role": data.role,
        "style": data.style,
        "allowed_claims": list(data.allowed_claims),
        "forbidden_claims": list(data.forbidden_claims),
        "min_length": data.min_length,
        "max_length": data.max_length,
        "emoji_rate": format(data.emoji_rate, ".4f"),
        "question_rate": format(data.question_rate, ".4f"),
        "reply_rate": format(data.reply_rate, ".4f"),
        "disagreement_rate": format(data.disagreement_rate, ".4f"),
        "daily_limit": data.daily_limit,
        "reply_bonus_slots": data.reply_bonus_slots,
        "cooldown_seconds": data.cooldown_seconds,
        "enabled": enabled,
        "profile_version": profile_version,
    }
    return payload


def serialize_profile_revision_payload(**kwargs: object) -> tuple[str, str]:
    serialized = canonical_json(profile_revision_payload(**kwargs))
    return serialized, hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def serialize_profile_snapshot(snapshot: AIAccountProfileSnapshot) -> str:
    data = profile_data_from_snapshot(snapshot)
    return canonical_json(
        profile_revision_payload(
            profile_id=snapshot.id,
            account_id=snapshot.account_id,
            telegram_user_id=snapshot.telegram_user_id,
            data=data,
            enabled=snapshot.enabled,
            profile_version=snapshot.profile_version,
        )
    )
