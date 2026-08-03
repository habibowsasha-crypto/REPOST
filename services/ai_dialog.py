"""AI replies for post-first-DM funnel + stop detection.

Воронка (не small talk):
1) First DM уже ушёл (короткий вопрос).
2) Первый ответ юзера → generate_explain: ЗАЧЕМ написал (канал), БЕЗ ссылки.
3) Тишина 60–120с → auto-link со ссылкой.
4) Если юзер пишет дальше → кратко по делу + можно дать ссылку.
"""

from __future__ import annotations

import random
import re
from typing import Optional

from loguru import logger

from config import (
    AI_DM_ENABLED,
    AI_MODEL,
    CHANNEL_LINK,
    CHANNEL_PITCH,
    OPENAI_API_KEY,
)
from services import phrases as phrases_svc
from services.ai_first_dm import sanitize_dashes

_STOP_RE = re.compile(
    r"(не\s+пиши|не\s+пишите|отстань|отстаньте|заблокирую|в\s+блок|"
    r"не\s+пиш(и|ите)\s+мне|leave\s+me|stop\s+writing|do\s+not\s+write|"
    r"fuck\s+off|пошёл\s+на|пошел\s+на|иди\s+нахуй|отвали)",
    re.IGNORECASE,
)

_SOFT_NO_RE = re.compile(
    r"(неинтересно|не\s+интересно|не\s+надо|не\s+нужно|нет\s+спасибо|"
    r"не\s+хочу|не\s+актуально)",
    re.IGNORECASE,
)

_BAD_DASHES = ("\u2014", "\u2013", "\u2212")

# Small-talk that must NOT appear as "explain"
_SMALLTALK_RE = re.compile(
    r"(как\s+дела|что\s+делаешь|чем\s+занима|как\s+настроен|"
    r"как\s+жизнь|что\s+нового|как\s+сам|как\s+ты\b|"
    r"расскажи\s+о\s+себе|откуда\s+ты|сколько\s+лет)",
    re.IGNORECASE,
)

_CHANNEL_HINT_RE = re.compile(
    r"(канал|vip|вип|подписк|контент|пост|слив|бесплатн|закрыт)",
    re.IGNORECASE,
)

_EXPLAIN_FALLBACKS = [
    "Написал не просто так: есть бесплатный канал, туда выкладывают посты из закрытых VIP. Платить не нужно.",
    "Суть такая: бесплатный канал со сливами из платных випок. Можно просто глянуть.",
    "Хотел поделиться: есть канал без оплаты, контент из закрытых VIP-чатов.",
    "По делу: веду на бесплатный канал, где посты из закрытых випок. Без подписок на платное.",
]

_LINK_FALLBACKS = [
    "Извини что отвлёк. Можешь просто глянуть - подписываться не обязательно:\n{link}",
    "Если не в тему - извини за беспокойство. Ссылка, можно только посмотреть:\n{link}",
    "Не хотел навязываться. На досуге глянь, без обязательств:\n{link}",
    "Прости что побеспокоил. Вот ссылка, смотреть не обязательно:\n{link}",
]


def is_hard_stop(text: str) -> bool:
    return bool(_STOP_RE.search(text or ""))


def is_soft_decline(text: str) -> bool:
    return bool(_SOFT_NO_RE.search(text or ""))


def _strip_em_dash(text: str) -> str:
    out = sanitize_dashes(text)
    for d in _BAD_DASHES:
        out = out.replace(d, "-")
    return out.strip()


def apology_text() -> str:
    return random.choice(
        [
            "Понял, извини что побеспокоил. Больше писать не буду.",
            "Ок, извини. Больше не напишу.",
            "Понял тебя, извини за беспокойство. Больше не пишу.",
        ]
    )


def soft_close_text() -> str:
    return random.choice(
        [
            "Ок, не навязываю. Удачного дня.",
            "Понял, не буду отвлекать.",
            "Ок, тогда не настаиваю.",
        ]
    )


def _explain_ok(text: str) -> bool:
    """Reject pure greetings / small talk as explain."""
    t = (text or "").strip()
    if not t or len(t) < 20:
        return False
    if _SMALLTALK_RE.search(t):
        return False
    # Must somehow point to channel idea
    if not _CHANNEL_HINT_RE.search(t):
        return False
    if "http" in t.lower() or "t.me/" in t.lower():
        return False
    return True


async def generate_explain(history: list[dict]) -> str:
    """Explain WHY we wrote: free channel pitch. NO link. NO small talk."""
    recent = phrases_svc.recent_texts(phrases_svc.KIND_EXPLAIN, limit=20)
    pitch = CHANNEL_PITCH or "бесплатный канал с постами из закрытых VIP"

    if AI_DM_ENABLED and OPENAI_API_KEY:
        try:
            text = await _openai_reply(
                history,
                instruction=(
                    "Это НЕ светская беседа.\n"
                    "Задача: одним-двумя предложениями объяснить, ЗАЧЕМ ты написал.\n"
                    f"Суть оффера: {pitch}\n"
                    "ЖЁСТКИЕ запреты:\n"
                    "- не здоровайся снова (не пиши Привет/Здравствуй)\n"
                    "- не спрашивай как дела / чем занимается / что нового\n"
                    "- не задавай встречные вопросы ни о чём личном\n"
                    "- без ссылок, без t.me, без http\n"
                    "- без давления и обещаний дохода\n"
                    "- только обычный дефис '-', без длинного тире\n"
                    "Можно начать с 'По делу:' или сразу с сути.\n"
                    "Только текст ответа, 1-2 предложения."
                ),
            )
            if text:
                text = _strip_em_dash(text)
                if CHANNEL_LINK and CHANNEL_LINK in text:
                    text = text.replace(CHANNEL_LINK, "").strip()
                if _explain_ok(text) and text not in recent:
                    return text
                logger.warning("explain rejected by validator: {!r}", (text or "")[:100])
        except Exception as exc:
            logger.exception("generate_explain failed: {}", exc)

    pool = [t for t in _EXPLAIN_FALLBACKS if t not in recent] or list(_EXPLAIN_FALLBACKS)
    return random.choice(pool)


async def generate_link_wrap(history: list[dict]) -> str:
    """Soft CTA + channel link (auto after silence or explicit)."""
    link = CHANNEL_LINK or "ссылка не задана (CHANNEL_LINK)"
    recent = phrases_svc.recent_texts(phrases_svc.KIND_LINK, limit=20)
    if AI_DM_ENABLED and OPENAI_API_KEY:
        try:
            text = await _openai_reply(
                history,
                instruction=(
                    "Нужно мягко дать ссылку на канал и слегка извиниться за беспокойство.\n"
                    f"Ссылка обязательно одна и точная: {link}\n"
                    "Структура 2-3 предложения:\n"
                    "1) коротко извинись что отвлёк / побеспокоил\n"
                    "2) можно просто глянуть, подписываться не обязательно\n"
                    "3) вставь ссылку\n"
                    "Не здоровайся, не спрашивай как дела, без давления, без длинного тире."
                ),
            )
            if text:
                text = _strip_em_dash(text)
                if link not in text and CHANNEL_LINK:
                    text = f"{text}\n{link}"
                if text not in recent and "http" not in text.lower().replace(link.lower(), ""):
                    # link itself may contain t.me - ok
                    return text
                if text not in recent:
                    return text
        except Exception as exp:
            logger.exception("generate_link_wrap failed: {}", exp)

    pool = [t for t in _LINK_FALLBACKS if t.format(link=link) not in recent]
    if not pool:
        pool = list(_LINK_FALLBACKS)
    return random.choice(pool).format(link=link)


async def generate_contextual_reply(history: list[dict], *, include_link: bool) -> str:
    """
    User wrote again after explain.
    Not small talk: short answer + pitch, optionally with link.
    """
    link = CHANNEL_LINK or ""
    pitch = CHANNEL_PITCH or "бесплатный канал с постами из закрытых VIP"

    if AI_DM_ENABLED and OPENAI_API_KEY:
        try:
            if include_link and link:
                extra = (
                    f"Обязательно один раз вставь ссылку: {link}\n"
                    "Сначала коротко извинись за беспокойство, потом скажи что можно "
                    "просто глянуть без обязательств, и дай ссылку."
                )
            else:
                extra = (
                    "Ссылку не давай. Коротко напомни суть канала без ссылки. "
                    "Не спрашивай как дела / чем занимается."
                )
            text = await _openai_reply(
                history,
                instruction=(
                    "Это продолжение воронки в Telegram, не дружеский чат.\n"
                    f"Оффер: {pitch}\n"
                    f"{extra}\n"
                    "Запрещено: повторный привет, 'как дела', 'чем занимаешься', "
                    "личные вопросы, давление, длинное тире.\n"
                    "1-3 коротких предложения по делу."
                ),
            )
            if text:
                text = _strip_em_dash(text)
                # Reject pure small talk
                if _SMALLTALK_RE.search(text):
                    logger.warning("contextual smalltalk rejected: {!r}", text[:80])
                else:
                    if include_link and link and link not in text:
                        text = f"{text}\n{link}"
                    return text
        except Exception as exc:
            logger.exception("contextual reply failed: {}", exc)

    if include_link and link:
        return random.choice(_LINK_FALLBACKS).format(link=link)
    return random.choice(_EXPLAIN_FALLBACKS)


async def _openai_reply(history: list[dict], *, instruction: str) -> Optional[str]:
    from openai import AsyncOpenAI

    client = AsyncOpenAI(api_key=OPENAI_API_KEY)
    messages = [
        {
            "role": "system",
            "content": (
                "Ты пишешь от лица человека в Telegram, но цель - мягко "
                "донести бесплатный канал (посты из закрытых VIP), не болтать.\n"
                "Пиши коротко по-русски. Только дефис '-'. "
                "Не обещай доход. Не спорь. Не устраивай small talk."
            ),
        },
        {"role": "system", "content": instruction},
    ]
    for item in (history or [])[-8]:
        role = "assistant" if item.get("role") == "assistant" else "user"
        messages.append({"role": role, "content": str(item.get("text") or "")[:500]})

    resp = await client.chat.completions.create(
        model=AI_MODEL,
        messages=messages,
        temperature=0.7,
        max_tokens=160,
    )
    content = (resp.choices[0].message.content or "").strip()
    if len(content) >= 2 and content[0] in "\"'«" and content[-1] in "\"'»":
        content = content[1:-1].strip()
    return content or None
