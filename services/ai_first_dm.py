"""AI-generated short first DM + strict validator + local fallback."""

from __future__ import annotations

import asyncio
import random
import re
from typing import Optional

from loguru import logger

from config import AI_DM_ENABLED, AI_MODEL, AI_REQUEST_TIMEOUT_SECONDS, OPENAI_API_KEY
from services import phrases as phrases_svc
from texts.first_dm import FIRST_DM_TEMPLATES, pick_first_dm

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
    r"what\s+is\s+your\s+hobby|where\s+do\s+you\s+live|"
    r"интересно[,\s]+как|здравствуй|подходишь\s+к\s+выбору|"
    r"реагируешь\s+на|как\s+ты\s+обычно\s+ищешь)",
    re.IGNORECASE,
)

_SYSTEM_PROMPT = """Ты — обычный парень из трейдинг-чата. Пишешь незнакомцу в личку ОДНО короткое сообщение.

Как пишут живые люди (важно):
- Неровно, по-свойски, без «менеджерского» тона.
- Можно: щас, а, ну, просто, коротко, без точки в конце иногда.
- Иногда вообще без «привет/слушай» — сразу вопрос.
- Иногда мягко: «можно спросить?», «не отвлеку?».
- Иногда по делу: сам или по сигналам, торгует ли сейчас.
- 1 фраза, до 65 символов. Как в мессенджере, не как пост.

Строго нельзя:
- Реклама, канал, ссылки, t.me, оффер
- Канцелярит: «хотел бы уточнить», «интересуюсь возможностью»
- Одинаковые начала подряд
- Эмодзи, длинные тире
- Опросы про хобби/город/возраст
- Выглядеть как рассылка или бот

Цель: чтобы человек С ХОДУ ответил коротко. Цепляет живой тон + лёгкий вопрос, не шаблон.
"""


def validate_first_dm(text: str) -> tuple[bool, str]:
    """Return (ok, reason)."""
    raw = (text or "").strip()
    if not raw:
        return False, "empty"
    if len(raw) > 75:
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
        for w in (
            "можно",
            "секунд",
            "минут",
            "спросить",
            "отвлека",
            "помеша",
            "торгу",
            "трейд",
            "сигнал",
            "стратег",
            "новичк",
            "потеря",
            "рынок",
            "сделк",
            "график",
            "иде",
            "разбор",
            "таймфрейм",
            "биток",
            "альт",
            "крипт",
            "золот",
        )
    )
    if not soft_ok:
        return False, "not_hook"
    return True, "ok"


def _too_similar_recent(text: str, recent: list[str]) -> bool:
    """Reject if same opening or high token overlap with recent DMs."""
    if not text or not recent:
        return False
    t = (text or "").strip().lower()
    # Opening: first 1-2 tokens
    opener = _opening_key(t)
    recent_openers = [_opening_key(r) for r in recent[:12]]
    if opener and recent_openers.count(opener) >= 2:
        return True
    # If last 3 all share same opener family "слушай"
    if opener == "слушай" and any(_opening_key(r) == "слушай" for r in recent[:3]):
        return True

    words = set(_tokens(t))
    if not words:
        return False
    for r in recent[:10]:
        rw = set(_tokens((r or "").lower()))
        if not rw:
            continue
        inter = words & rw
        if len(inter) >= 3 and len(inter) / max(len(words), 1) >= 0.55:
            return True
    return False


def _opening_key(text: str) -> str:
    t = (text or "").strip().lower().lstrip(",.!? ")
    # strip common soft prefixes
    for p in ("слушай", "эй", "привет", "кстати", "йоу", "хей"):
        if t.startswith(p):
            return p
    # first word
    parts = t.replace(",", " ").split()
    return parts[0] if parts else ""


def _tokens(text: str) -> list[str]:
    return [w for w in re.findall(r"[а-яa-z0-9]+", (text or "").lower()) if len(w) > 2]

# Style families are rotated by recent usage to reduce repeated openings and intent.
_STYLES = (
    "soft",
    "signals",
    "active",
    "market_view",
    "watchlist",
    "analysis_source",
    "format",
    "timeframe",
)

MAX_AI_FIRST_DM_ATTEMPTS = 2


def _detect_style(text: str) -> str:
    t = (text or "").lower()
    if any(x in t for x in ("таймфрейм", "тф")):
        return "timeframe"
    if any(x in t for x in ("короткий разбор", "подроб", "формат")):
        return "format"
    if any(x in t for x in ("сам график", "идеи смот", "чужие идеи", "сам анализ")):
        return "analysis_source"
    if any(x in t for x in ("биток", "альт", "крипт", "золот", "что сейчас смот")):
        return "watchlist"
    if any(x in t for x in ("рынок вид", "как рынок", "по рынку дума")):
        return "market_view"
    if any(x in t for x in ("сигнал", "сам торг", "по сигнал")):
        return "signals"
    if any(
        x in t
        for x in (
            "можно спросить",
            "не отвлек",
            "не помеш",
            "есть секунд",
            "есть минут",
            "на секунду",
        )
    ):
        return "soft"
    if any(x in t for x in ("торгу", "в рынке", "на паузе", "щас")):
        return "active"
    return "other"


def _pick_style(recent: list[str]) -> str:
    """Pick the least-used intent family from recent First DMs."""
    counts = {style: 0 for style in _STYLES}
    for item in recent[:24]:
        detected = _detect_style(item)
        if detected in counts:
            counts[detected] += 1
    minimum = min(counts.values())
    candidates = [style for style in _STYLES if counts[style] == minimum]
    recent_styles = {_detect_style(item) for item in recent[:4]}
    not_recent = [style for style in candidates if style not in recent_styles]
    return random.choice(not_recent or candidates)


def _style_instruction(style: str) -> str:
    instructions = {
        "soft": (
            "Стиль СЕЙЧАС: мягкий короткий вход без трейдинг-терминов. "
            "Не повторяй фразы «можно спросить», «есть секунда» и «есть минутка» "
            "если они уже есть в недавних сообщениях."
        ),
        "signals": (
            "Стиль СЕЙЧАС: коротко спроси, сам человек анализирует или смотрит сигналы. "
            "Не делай это анкетой и не начинай со слова «Слушай»."
        ),
        "active": (
            "Стиль СЕЙЧАС: одним живым вопросом уточни, торгует ли человек сейчас "
            "или пока наблюдает рынок."
        ),
        "market_view": (
            "Стиль СЕЙЧАС: спроси мнение о текущем рынке. Один конкретный вопрос, "
            "без прогноза от себя и без выдуманной личной истории."
        ),
        "watchlist": (
            "Стиль СЕЙЧАС: спроси, что человек сейчас больше наблюдает - биток, альты, "
            "крипту или золото. Коротко, без списка из многих вопросов."
        ),
        "analysis_source": (
            "Стиль СЕЙЧАС: спроси, сам человек разбирает график или смотрит чужие идеи. "
            "Не упоминай канал, рекламу или собственный опыт."
        ),
        "format": (
            "Стиль СЕЙЧАС: спроси, какой формат рыночного разбора удобнее - короткий "
            "или подробный. Только один вопрос."
        ),
        "timeframe": (
            "Стиль СЕЙЧАС: коротко спроси, какой таймфрейм человек чаще смотрит. "
            "Без второго вопроса и без менеджерского тона."
        ),
    }
    return instructions[style]


def _local_first_dm(recent: list[str]) -> str:
    """Pick a validator-safe local message that also respects recent similarity."""
    candidates = list(FIRST_DM_TEMPLATES)
    random.shuffle(candidates)
    recent_styles = {_detect_style(item) for item in recent[:4]}
    candidates.sort(key=lambda item: _detect_style(item) in recent_styles)
    for candidate in candidates:
        clean = sanitize_dashes(candidate)
        ok, _reason = validate_first_dm(clean)
        if ok and clean not in recent and not _too_similar_recent(clean, recent):
            return clean
    return sanitize_dashes(pick_first_dm(recent=recent))


def sanitize_dashes(text: str) -> str:
    out = text
    for d in _BAD_DASHES:
        out = out.replace(d, "-")
    return out.strip()


async def generate_first_dm() -> str:
    """
    Human-sounding first DM with forced style rotation for reply rate.
    Always returns a validator-passed string.
    """
    recent = phrases_svc.recent_texts(phrases_svc.KIND_FIRST_DM, limit=40)
    style = _pick_style(recent)
    logger.info("First DM style picked: {}", style)

    if AI_DM_ENABLED and OPENAI_API_KEY:
        try:
            for attempt in range(MAX_AI_FIRST_DM_ATTEMPTS):
                text = await _openai_first_dm(recent, style=style)
                if not text:
                    continue
                text = sanitize_dashes(text)
                ok, reason = validate_first_dm(text)
                if not ok:
                    logger.warning(
                        "AI first DM rejected ({}): {!r}", reason, text[:80]
                    )
                    continue
                if text in recent or _too_similar_recent(text, recent):
                    logger.warning("AI first DM too similar: {!r}", text[:80])
                    continue
                # Soft check: if we asked for signals, prefer that signal is present
                # but do not hard-fail — human wording varies.
                return text
        except Exception as exc:
            logger.exception("AI first DM failed: {}", exc)

    # Local fallback scans all approved families and keeps the same similarity check.
    return _local_first_dm(recent)


async def _openai_first_dm(
    recent: list[str], *, style: str = "soft"
) -> Optional[str]:
    from openai import AsyncOpenAI

    client = AsyncOpenAI(
        api_key=OPENAI_API_KEY,
        timeout=AI_REQUEST_TIMEOUT_SECONDS,
        max_retries=1,
    )
    avoid = ""
    if recent:
        sample = "\n".join(f"- {x}" for x in recent[:12])
        avoid = (
            "\nНедавно уже писали так (не повторяй ни смысл, ни начало):\n"
            f"{sample}\n"
        )

    style_line = _style_instruction(style)
    opener_ban = ""
    if any((r or "").strip().lower().startswith("слушай") for r in recent[:4]):
        opener_ban = " Не начинай со «Слушай»."

    user = (
        "Напиши одно сообщение в личку, как живой человек из чата.\n"
        f"{style_line}"
        f"{opener_ban}\n"
        "Только текст. Без кавычек. Без пояснений. Без эмодзи."
        f"{avoid}"
    )

    resp = await asyncio.wait_for(
        client.chat.completions.create(
            model=AI_MODEL,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": user},
            ],
            temperature=1.05,
            max_tokens=55,
            presence_penalty=0.6,
            frequency_penalty=0.4,
        ),
        timeout=AI_REQUEST_TIMEOUT_SECONDS + 2.0,
    )
    content = (resp.choices[0].message.content or "").strip()
    # Strip wrapping quotes if model adds them
    if len(content) >= 2 and content[0] in "\"'«" and content[-1] in "\"'»":
        content = content[1:-1].strip()
    return content or None
