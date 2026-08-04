"""AI-generated simple First DM with strict validation and anti-repeat."""

from __future__ import annotations

import asyncio
import random
import re
from difflib import SequenceMatcher
from typing import Optional

from loguru import logger

from config import AI_DM_ENABLED, AI_MODEL, AI_REQUEST_TIMEOUT_SECONDS, OPENAI_API_KEY
from services import phrases as phrases_svc
from texts.first_dm import FIRST_DM_TEMPLATES, pick_first_dm

_BAD_DASHES = ("\u2014", "\u2013", "\u2212")
_LINK_RE = re.compile(
    r"(https?://|t\.me/|telegram\.me/|www\.|\.com/|\.ru/)",
    re.IGNORECASE,
)
_TOPIC_RE = re.compile(
    r"(рынок|торг|сделк|позици|сигнал|график|крипт|биток|альт|золот|"
    r"таймфрейм|\bтф\b|разбор|стратег|вип|vip|канал|софт|ссылк)",
    re.IGNORECASE,
)
_SOFT_HOOK_RE = re.compile(
    r"(привет|слушай|можно|вопрос|спросить|уточнить|узнать|занят|свобод|"
    r"отвлек|помеш|минут|секунд|удобно|ответить|момент)",
    re.IGNORECASE,
)
_WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9]+")

_SYSTEM_PROMPT = """Ты пишешь незнакомому человеку первое короткое сообщение в Telegram.

Нужен только простой бытовой заход, на который легко ответить:
- «Привет, можно один вопрос?»
- «Привет, ты занят?»
- «Можно кое-что спросить?»
- «Не отвлеку?»

Строго:
- 2-7 слов
- одна короткая фраза
- без темы рынка, трейдинга, сигналов, позиции, канала и рекламы
- без ссылки, эмодзи, длинного тире и официального тона
- не объясняй, что именно хочешь спросить
- не копируй недавние формулировки
- верни только готовое сообщение
"""

# One initial generation plus at most two repeat generations.
MAX_AI_FIRST_DM_ATTEMPTS = 3
ANTI_REPEAT_WINDOW = 20


def sanitize_dashes(text: str) -> str:
    out = str(text or "")
    for dash in _BAD_DASHES:
        out = out.replace(dash, "-")
    return out.strip()


def validate_first_dm(text: str) -> tuple[bool, str]:
    raw = sanitize_dashes(text)
    if not raw:
        return False, "empty"
    if len(raw) > 60:
        return False, "too_long"
    if any(dash in str(text or "") for dash in _BAD_DASHES):
        return False, "bad_dash"
    if "\n" in raw:
        return False, "multiline"
    if _LINK_RE.search(raw):
        return False, "has_link"
    if _TOPIC_RE.search(raw):
        return False, "has_topic"
    words = _WORD_RE.findall(raw)
    if not 2 <= len(words) <= 7:
        return False, "word_count"
    if not _SOFT_HOOK_RE.search(raw):
        return False, "not_simple_hook"
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
        if normalized == _normalize(old):
            return True
        if _similarity(text, old) >= threshold:
            return True
    return False


def _local_first_dm(recent: list[str]) -> str:
    candidates = list(FIRST_DM_TEMPLATES)
    random.shuffle(candidates)
    for candidate in candidates:
        clean = sanitize_dashes(candidate)
        ok, _reason = validate_first_dm(clean)
        if ok and not _too_similar_recent(clean, recent):
            return clean

    # The local pool is deliberately larger than the 20-message window. If all
    # remaining candidates are semantically close, exact repetition is still forbidden.
    recent_exact = {_normalize(item) for item in recent[:ANTI_REPEAT_WINDOW]}
    exact_new = [item for item in candidates if _normalize(item) not in recent_exact]
    if exact_new:
        def score(item: str) -> float:
            return max((_similarity(item, old) for old in recent[:ANTI_REPEAT_WINDOW]), default=0.0)
        return sanitize_dashes(min(exact_new, key=score))
    return sanitize_dashes(pick_first_dm(recent=recent))


async def generate_first_dm() -> str:
    """Generate the approved simple First DM using the last 20 global messages."""
    recent = phrases_svc.recent_texts(
        phrases_svc.KIND_FIRST_DM,
        limit=ANTI_REPEAT_WINDOW,
    )
    if AI_DM_ENABLED and OPENAI_API_KEY:
        for attempt in range(MAX_AI_FIRST_DM_ATTEMPTS):
            try:
                text = await _openai_first_dm(recent, retry=attempt)
            except Exception as exc:
                logger.warning("AI first DM attempt {} failed: {}", attempt + 1, exc)
                continue
            clean = sanitize_dashes(text or "")
            ok, reason = validate_first_dm(clean)
            if not ok:
                logger.warning("AI first DM rejected reason={} text={!r}", reason, clean[:80])
                continue
            if _too_similar_recent(clean, recent):
                logger.warning("AI first DM too similar attempt={} text={!r}", attempt + 1, clean[:80])
                continue
            return clean
    return _local_first_dm(recent)


async def _openai_first_dm(recent: list[str], *, retry: int = 0) -> Optional[str]:
    from openai import AsyncOpenAI

    client = AsyncOpenAI(
        api_key=OPENAI_API_KEY,
        timeout=AI_REQUEST_TIMEOUT_SECONDS,
        max_retries=1,
    )
    avoid = ""
    if recent:
        avoid = "\nПоследние 20 сообщений, которые нельзя повторять или близко копировать:\n" + "\n".join(
            f"- {item}" for item in recent[:ANTI_REPEAT_WINDOW]
        )
    retry_note = (
        "\nПредыдущий вариант не прошёл проверку. Напиши заметно иначе."
        if retry else ""
    )
    response = await asyncio.wait_for(
        client.chat.completions.create(
            model=AI_MODEL,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": "Сформулируй новый простой First DM." + avoid + retry_note,
                },
            ],
            temperature=1.0 if retry == 0 else 1.15,
            max_tokens=40,
            presence_penalty=0.7,
            frequency_penalty=0.5,
        ),
        timeout=AI_REQUEST_TIMEOUT_SECONDS + 2.0,
    )
    content = (response.choices[0].message.content or "").strip()
    if len(content) >= 2 and content[0] in "\"'«" and content[-1] in "\"'»":
        content = content[1:-1].strip()
    return content or None
