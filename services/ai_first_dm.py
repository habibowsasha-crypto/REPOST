"""AI-generated First DM with magnet, short_hook and legacy modes."""

from __future__ import annotations

import asyncio
import random
import re
from difflib import SequenceMatcher
from typing import Optional

from loguru import logger

from config import (
    AI_DM_ENABLED,
    AI_MODEL,
    AI_REQUEST_TIMEOUT_SECONDS,
    FIRST_DM_STYLE,
    OPENAI_API_KEY,
)
from services import phrases as phrases_svc
from texts.first_dm import (
    MAGNET_FIRST_DM_TEMPLATES,
    SHORT_HOOK_FIRST_DM_TEMPLATES,
    pick_first_dm,
    templates_for_style,
)

_BAD_DASHES = ("\u2014", "\u2013", "\u2212")
_LINK_RE = re.compile(r"(https?://|t\.me/|telegram\.me/|www\.|\.com/|\.ru/)", re.IGNORECASE)
_WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9]+")
_LEADING_LIST_MARKER_RE = re.compile(r"^\s*(?:(?:[-*•]+)|(?:\d+[.)]))\s+")

_MAGNET_TOPIC_RE = re.compile(
    r"(сигнал|вход|движен|цен|уведомлен|сделк|спот|фьюч|торг|скальп|"
    r"стоп|импульс|канал|софт|анализ|крипт)",
    re.IGNORECASE,
)
_MAGNET_RESPONSE_RE = re.compile(
    r"(часто|бывал|что чаще|что хуже|или|успева|вручную|сам |смотри|"
    r"отслеж|открыва|заход|пропуска|проверя|ждёшь|ждешь|больше|всегда|бывает)",
    re.IGNORECASE,
)
_FORBIDDEN_MAGNET_RE = re.compile(
    r"(нужн[аоы]?\s+(твоя|моя)?\s*помощ|поможешь|можешь помочь|"
    r"можно\s+(один\s+)?вопрос|можно спросить|можно поговорить|"
    r"можно пару слов|могу спросить|что-то спросить|кое-что спросить|"
    r"есть минутк|не отвлек|ты занят|свободен сейчас|есть просьба)",
    re.IGNORECASE,
)

_LEGACY_TOPIC_RE = re.compile(
    r"(рынок|торг|сделк|позици|сигнал|график|крипт|биток|альт|золот|"
    r"таймфрейм|\bтф\b|разбор|стратег|вип|vip|канал|софт|ссылк)",
    re.IGNORECASE,
)
_LEGACY_HOOK_RE = re.compile(
    r"(привет|слушай|можно|вопрос|спросить|уточнить|узнать|занят|свобод|"
    r"отвлек|помеш|минут|секунд|удобно|ответить|момент)",
    re.IGNORECASE,
)

_SHORT_HOOK_HELP_RE = re.compile(
    r"(помощ|помог|помож|помоч|выруч|подскаж)",
    re.IGNORECASE,
)
_SHORT_HOOK_QUESTION_RE = re.compile(r"вопрос", re.IGNORECASE)
_SHORT_HOOK_GREETING_RE = re.compile(
    r"^(привет|слушай|салам|здарова)\b",
    re.IGNORECASE,
)

_MAGNET_SYSTEM_PROMPT = """Ты пишешь незнакомому человеку первый короткий DM в Telegram.

Цель - получить естественный короткий ответ от человека, который интересуется криптой или трейдингом.
Сообщение должно поднимать одну узнаваемую ситуацию:
- сигнал увидели после движения
- вход пропустили или зашли поздно
- сигналы отслеживают вручную или через софт
- уведомления приходят поздно
- спот или фьючерсы
- свой анализ или готовые сигналы

Строго:
- один конкретный вопрос
- 5-12 слов
- начинается с «Привет,»
- ответ возможен в 1-5 словах
- без ссылки, рекламы, обещаний и эмодзи
- без просьбы о помощи
- не спрашивай разрешения задать другой вопрос
- не используй «можно вопрос», «можно поговорить», «можно пару слов»
- только короткий дефис -
- не копируй недавние формулировки
- верни только готовое сообщение
"""

_LEGACY_SYSTEM_PROMPT = """Ты пишешь незнакомому человеку первое короткое сообщение в Telegram.
Нужен простой нейтральный заход без темы трейдинга и рекламы.
Строго: 2-7 слов, одна фраза, без ссылки, эмодзи и длинного тире. Верни только сообщение.
"""

_SHORT_HOOK_SYSTEM_PROMPT = """Ты выбираешь первое короткое сообщение незнакомому человеку в Telegram.

Верни ровно одну готовую фразу из утверждённого списка ниже, без изменений:
{approved}

Строго:
- не придумывай новые формулировки
- если используется помощь, она всегда нужна именно с вопросом
- нельзя писать просто «можешь помочь?» или «выручишь?»
- без ссылки, рекламы и эмодзи
- только короткий дефис -
- верни только одну фразу из списка
"""

MAX_AI_FIRST_DM_ATTEMPTS = 3
ANTI_REPEAT_WINDOW = 20


class FirstDMUnavailableError(RuntimeError):
    """No approved unique First DM is currently available."""


def _approved_magnet_norms() -> set[str]:
    return {_normalize(item) for item in MAGNET_FIRST_DM_TEMPLATES}


def _approved_short_hook_norms() -> set[str]:
    return {_normalize(item) for item in SHORT_HOOK_FIRST_DM_TEMPLATES}


def sanitize_dashes(text: str) -> str:
    out = str(text or "")
    for dash in _BAD_DASHES:
        out = out.replace(dash, "-")
    return out.strip()


def sanitize_ai_output(text: str | None) -> str:
    """Remove presentation wrappers that AI must never send to Telegram."""
    out = sanitize_dashes(text or "")
    if len(out) >= 2 and out[0] in "\"'«" and out[-1] in "\"'»":
        out = out[1:-1].strip()
    # Models sometimes return a Markdown list item despite a plain-text prompt.
    # Strip the marker before validation so it cannot be logged or sent.
    out = _LEADING_LIST_MARKER_RE.sub("", out, count=1).strip()
    return out


def validate_first_dm(text: str, *, style: str | None = None) -> tuple[bool, str]:
    selected = (style or FIRST_DM_STYLE).strip().lower()
    original = str(text or "")
    if _LEADING_LIST_MARKER_RE.match(original):
        return False, "leading_list_marker"
    raw = sanitize_dashes(original)
    if not raw:
        return False, "empty"
    if len(raw) > 120:
        return False, "too_long"
    if any(dash in str(text or "") for dash in _BAD_DASHES):
        return False, "bad_dash"
    if "\n" in raw:
        return False, "multiline"
    if _LINK_RE.search(raw):
        return False, "has_link"
    words = _WORD_RE.findall(raw)

    if selected == "legacy":
        if not 2 <= len(words) <= 7:
            return False, "word_count"
        if _LEGACY_TOPIC_RE.search(raw):
            return False, "has_topic"
        if not _LEGACY_HOOK_RE.search(raw):
            return False, "not_simple_hook"
        return True, "ok"


    if selected == "short_hook":
        if not 3 <= len(words) <= 10:
            return False, "word_count"
        if not _SHORT_HOOK_GREETING_RE.search(raw):
            return False, "no_approved_greeting"
        if _SHORT_HOOK_HELP_RE.search(raw) and not _SHORT_HOOK_QUESTION_RE.search(raw):
            return False, "help_without_question"
        if _normalize(raw) not in _approved_short_hook_norms():
            return False, "not_approved_short_hook"
        return True, "ok"

    if not 5 <= len(words) <= 12:
        return False, "word_count"
    if not raw.casefold().startswith("привет"):
        return False, "no_greeting"
    if not raw.endswith("?"):
        return False, "not_question"
    if re.search(r"помощ|помог|помож|помоч", raw, re.IGNORECASE):
        return False, "help_topic_forbidden"
    if _FORBIDDEN_MAGNET_RE.search(raw):
        return False, "forbidden_empty_hook"
    if not _MAGNET_TOPIC_RE.search(raw):
        return False, "no_trading_topic"
    if not _MAGNET_RESPONSE_RE.search(raw):
        return False, "not_answer_magnet"
    # Magnet mode is intentionally closed-world: only reviewed, grammatically
    # complete questions from the approved pool may be sent. AI can propose a
    # wording, but it must match one reviewed structure exactly after harmless
    # normalization. This prevents keyword-soup and semantic nonsense.
    if _normalize(raw) not in _approved_magnet_norms():
        return False, "not_approved_structure"
    return True, "ok"


def _normalize(text: str) -> str:
    value = sanitize_dashes(text).casefold().replace("ё", "е")
    value = re.sub(r"[^a-zа-я0-9]+", " ", value)
    return " ".join(value.split())


def _tokens(text: str) -> set[str]:
    return set(_normalize(text).split())


def _similarity(left: str, right: str) -> float:
    a = _normalize(left)
    b = _normalize(right)
    if not a or not b:
        return 0.0
    sequence = SequenceMatcher(None, a, b).ratio()
    ta = _tokens(a)
    tb = _tokens(b)
    union = ta | tb
    jaccard = len(ta & tb) / len(union) if union else 0.0
    return max(sequence, jaccard)


def _too_similar_recent(text: str, recent: list[str], *, threshold: float = 0.86) -> bool:
    normalized = _normalize(text)
    if not normalized:
        return True
    for old in recent[:ANTI_REPEAT_WINDOW]:
        if normalized == _normalize(old) or _similarity(text, old) >= threshold:
            return True
    return False


def _local_first_dm(recent: list[str], *, style: str | None = None) -> str:
    selected = (style or FIRST_DM_STYLE).strip().lower()
    candidates = list(templates_for_style(selected))
    random.shuffle(candidates)
    for candidate in candidates:
        clean = sanitize_dashes(candidate)
        ok, _reason = validate_first_dm(clean, style=selected)
        if ok and not _too_similar_recent(clean, recent):
            return clean

    # Do not weaken the similarity rule in fallback. Sending nothing is safer
    # than knowingly repeating a wording that the uniqueness guard rejected.
    raise FirstDMUnavailableError(
        f"no approved unique First DM available for style={selected}"
    )


async def generate_first_dm() -> str:
    """Generate a First DM using the configured style."""
    selected = FIRST_DM_STYLE
    recent = phrases_svc.recent_texts(phrases_svc.KIND_FIRST_DM, limit=ANTI_REPEAT_WINDOW)
    # short_hook is a reviewed closed pool. Calling AI here adds cost, latency and
    # malformed list markers without creating any allowed new wording.
    if selected == "short_hook":
        return _local_first_dm(recent, style=selected)
    if AI_DM_ENABLED and OPENAI_API_KEY:
        for attempt in range(MAX_AI_FIRST_DM_ATTEMPTS):
            try:
                text = await _openai_first_dm(recent, retry=attempt, style=selected)
            except Exception as exc:
                logger.warning("AI first DM attempt {} failed: {}", attempt + 1, exc)
                continue
            clean = sanitize_ai_output(text)
            ok, reason = validate_first_dm(clean, style=selected)
            if not ok:
                logger.warning("AI first DM rejected style={} reason={} text={!r}", selected, reason, clean[:120])
                continue
            if _too_similar_recent(clean, recent):
                logger.warning("AI first DM too similar attempt={} text={!r}", attempt + 1, clean[:120])
                continue
            return clean
    return _local_first_dm(recent, style=selected)


async def _openai_first_dm(recent: list[str], *, retry: int = 0, style: str = "magnet") -> Optional[str]:
    from openai import AsyncOpenAI

    client = AsyncOpenAI(api_key=OPENAI_API_KEY, timeout=AI_REQUEST_TIMEOUT_SECONDS, max_retries=1)
    avoid = ""
    if recent:
        avoid = "\nПоследние 20 сообщений, которые нельзя повторять или близко копировать:\n" + "\n".join(
            f"- {item}" for item in recent[:ANTI_REPEAT_WINDOW]
        )
    retry_note = "\nПредыдущий вариант не прошёл проверку. Напиши заметно иначе." if retry else ""
    response = await asyncio.wait_for(
        client.chat.completions.create(
            model=AI_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        _LEGACY_SYSTEM_PROMPT
                        if style == "legacy"
                        else (
                            _SHORT_HOOK_SYSTEM_PROMPT.format(
                                approved="\n".join(
                                    f"- {item}" for item in SHORT_HOOK_FIRST_DM_TEMPLATES
                                )
                            )
                            if style == "short_hook"
                            else _MAGNET_SYSTEM_PROMPT
                        )
                    ),
                },
                {"role": "user", "content": "Сформулируй новый First DM." + avoid + retry_note},
            ],
            temperature=1.0 if retry == 0 else 1.15,
            max_tokens=60,
            presence_penalty=0.7,
            frequency_penalty=0.5,
        ),
        timeout=AI_REQUEST_TIMEOUT_SECONDS + 2.0,
    )
    content = sanitize_ai_output(response.choices[0].message.content)
    return content or None
