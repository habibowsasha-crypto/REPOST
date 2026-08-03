"""AI-generated short first DM + strict validator + local fallback."""

from __future__ import annotations

import re
from typing import Optional

from loguru import logger

from config import AI_DM_ENABLED, AI_MODEL, OPENAI_API_KEY
from services import phrases as phrases_svc
from texts.first_dm import pick_first_dm

# Forbidden long dashes (user rule: only ASCII hyphen "-")
_BAD_DASHES = ("\u2014", "\u2013", "\u2212")  # em, en, minus

_LINK_RE = re.compile(
    r"(https?://|t\.me/|telegram\.me/|www\.|\.com/|\.ru/)",
    re.IGNORECASE,
)

# Survey / long personal quiz - not allowed in first DM
_SURVEY_RE = re.compile(
    r"(какие\s+фильмы|какой\s+фильм|хобби|чем\s+занимаешься|"
    r"где\s+работаешь|сколько\s+лет|из\s+какого\s+города|"
    r"любимый\s+(цвет|фильм|сериал)|расскажи\s+о\s+себе|"
    r"what\s+is\s+your\s+hobby|where\s+do\s+you\s+live)",
    re.IGNORECASE,
)

_SYSTEM_PROMPT = """Ты пишешь первое короткое сообщение незнакомцу в Telegram.
Цель: человек с большой вероятностью ответит одной короткой репликой.

Правила:
- Только 1 короткое предложение (можно с обращением).
- Смысл: мягко спросить разрешение спросить / есть ли секунда.
- Без ссылок, без рекламы, без названия канала, без оффера.
- Без опросников (хобби, фильмы, работа, город, возраст).
- Без эмодзи-спама (0-1 эмодзи максимум, лучше без).
- Только обычный дефис "-" если нужен. Запрещены длинные тире.
- Язык: русский.
- Не копируй дословно примеры, каждый раз новая формулировка.
- Максимум 90 символов.

Примеры смысла (не копируй):
- Можно спросить?
- Привет, не отвлекаю?
- Есть секунда?
"""


def validate_first_dm(text: str) -> tuple[bool, str]:
    """Return (ok, reason)."""
    raw = (text or "").strip()
    if not raw:
        return False, "empty"
    if len(raw) > 120:
        return False, "too_long"
    if any(d in raw for d in _BAD_DASHES):
        return False, "bad_dash"
    if _LINK_RE.search(raw):
        return False, "has_link"
    if _SURVEY_RE.search(raw):
        return False, "survey"
    # No multiline walls
    if raw.count("\n") > 1:
        return False, "multiline"
    # Should look like a question or soft probe
    soft_ok = ("?" in raw) or any(
        w in raw.lower()
        for w in ("можно", "секунд", "минут", "спросить", "отвлека", "помеша")
    )
    if not soft_ok:
        return False, "not_hook"
    return True, "ok"


def sanitize_dashes(text: str) -> str:
    out = text
    for d in _BAD_DASHES:
        out = out.replace(d, "-")
    return out.strip()


async def generate_first_dm() -> str:
    """
    Prefer OpenAI unique hook; fallback to local templates.
    Always returns a validator-passed string.
    """
    recent = phrases_svc.recent_texts(phrases_svc.KIND_FIRST_DM, limit=40)

    if AI_DM_ENABLED and OPENAI_API_KEY:
        try:
            text = await _openai_first_dm(recent)
            if text:
                text = sanitize_dashes(text)
                ok, reason = validate_first_dm(text)
                if ok and text not in recent:
                    return text
                logger.warning(
                    "AI first DM rejected ({}): {!r}", reason, (text or "")[:80]
                )
                # one retry
                text2 = await _openai_first_dm(recent + ([text] if text else []))
                if text2:
                    text2 = sanitize_dashes(text2)
                    ok2, reason2 = validate_first_dm(text2)
                    if ok2 and text2 not in recent:
                        return text2
                    logger.warning(
                        "AI first DM retry rejected ({}): {!r}",
                        reason2,
                        (text2 or "")[:80],
                    )
        except Exception as exc:
            logger.exception("AI first DM failed: {}", exc)

    # Local fallback
    return pick_first_dm(recent=recent)


async def _openai_first_dm(recent: list[str]) -> Optional[str]:
    from openai import AsyncOpenAI

    client = AsyncOpenAI(api_key=OPENAI_API_KEY)
    avoid = ""
    if recent:
        sample = "\n".join(f"- {t}" for t in recent[:15])
        avoid = f"\nНе повторяй эти недавние формулировки:\n{sample}\n"

    user = (
        "Сгенерируй одно новое короткое первое сообщение для Telegram ЛС.\n"
        "Только текст сообщения, без кавычек и без пояснений."
        + avoid
    )

    resp = await client.chat.completions.create(
        model=AI_MODEL,
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user},
        ],
        temperature=0.95,
        max_tokens=60,
    )
    content = (resp.choices[0].message.content or "").strip()
    # Strip wrapping quotes if model adds them
    if len(content) >= 2 and content[0] in "\"'«" and content[-1] in "\"'»":
        content = content[1:-1].strip()
    return content or None
