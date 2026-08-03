"""AI replies for post-first-DM funnel + stop detection."""

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

_EXPLAIN_FALLBACKS = [
    "По делу: есть бесплатный канал, туда падают посты из закрытых випок. Платить не нужно.",
    "Коротко: бесплатный канал со сливами из платных VIP. Можно просто глянуть.",
    "Суть такая - бесплатный канал, контент из закрытых випок без оплаты.",
]

_LINK_FALLBACKS = [
    "Можешь просто глянуть - подписываться не обязательно:\n{link}",
    "Вот ссылка, можно только посмотреть:\n{link}",
    "На досуге глянь, без обязательств:\n{link}",
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


async def generate_explain(history: list[dict]) -> str:
    """Explain channel WITHOUT link."""
    recent = phrases_svc.recent_texts(phrases_svc.KIND_EXPLAIN, limit=20)
    if AI_DM_ENABLED and OPENAI_API_KEY:
        try:
            text = await _openai_reply(
                history,
                instruction=(
                    "Пользователь ответил на первое сообщение. "
                    "Коротко (1-2 предложения) объясни, зачем написал: "
                    f"{CHANNEL_PITCH}. "
                    "Без ссылки, без давления, без длинного тире, только '-' если нужно. "
                    "Не копируй шаблоны дословно."
                ),
            )
            if text:
                text = _strip_em_dash(text)
                if CHANNEL_LINK and CHANNEL_LINK in text:
                    text = text.replace(CHANNEL_LINK, "").strip()
                if text and text not in recent and "http" not in text.lower():
                    return text
        except Exception as exc:
            logger.exception("generate_explain failed: {}", exc)

    pool = [t for t in _EXPLAIN_FALLBACKS if t not in recent] or list(_EXPLAIN_FALLBACKS)
    return random.choice(pool)


async def generate_link_wrap(history: list[dict]) -> str:
    """Soft CTA + channel link."""
    link = CHANNEL_LINK or "ссылка не задана (CHANNEL_LINK)"
    recent = phrases_svc.recent_texts(phrases_svc.KIND_LINK, limit=20)
    if AI_DM_ENABLED and OPENAI_API_KEY:
        try:
            text = await _openai_reply(
                history,
                instruction=(
                    "Нужно мягко дать ссылку на бесплатный канал. "
                    f"Ссылка обязательно одна и точная: {link} "
                    "Текст 1-2 предложения: можно просто глянуть, подписываться не обязательно. "
                    "Без давления, без длинного тире."
                ),
            )
            if text:
                text = _strip_em_dash(text)
                if link not in text and CHANNEL_LINK:
                    text = f"{text}\n{link}"
                if text not in recent:
                    return text
        except Exception as exp:
            logger.exception("generate_link_wrap failed: {}", exp)

    pool = [t for t in _LINK_FALLBACKS if t.format(link=link) not in recent]
    if not pool:
        pool = list(_LINK_FALLBACKS)
    return random.choice(pool).format(link=link)


async def generate_contextual_reply(history: list[dict], *, include_link: bool) -> str:
    """Reply when user writes during/after explain."""
    link = CHANNEL_LINK or ""
    if AI_DM_ENABLED and OPENAI_API_KEY:
        try:
            extra = (
                f"Можно один раз дать ссылку: {link}"
                if include_link and link
                else "Ссылку не давай, если уже была или не просят."
            )
            text = await _openai_reply(
                history,
                instruction=(
                    "Ответь коротко по смыслу сообщения пользователя. "
                    f"{CHANNEL_PITCH}. {extra} "
                    "Без давления, без длинного тире, 1-3 предложения."
                ),
            )
            if text:
                return _strip_em_dash(text)
        except Exception as exc:
            logger.exception("contextual reply failed: {}", exc)

    if include_link and link:
        return f"Можешь глянуть канал, без обязательств:\n{link}"
    return "Ок, понял."


async def _openai_reply(history: list[dict], *, instruction: str) -> Optional[str]:
    from openai import AsyncOpenAI

    client = AsyncOpenAI(api_key=OPENAI_API_KEY)
    messages = [
        {
            "role": "system",
            "content": (
                "Ты обычный человек в Telegram. Пиши коротко по-русски. "
                "Только обычный дефис '-', без длинного тире. "
                "Не обещай доход. Не спорь агрессивно."
            ),
        },
        {"role": "system", "content": instruction},
    ]
    for item in (history or [])[-8:]:
        role = "assistant" if item.get("role") == "assistant" else "user"
        messages.append({"role": role, "content": str(item.get("text") or "")[:500]})

    resp = await client.chat.completions.create(
        model=AI_MODEL,
        messages=messages,
        temperature=0.85,
        max_tokens=180,
    )
    content = (resp.choices[0].message.content or "").strip()
    if len(content) >= 2 and content[0] in "\"'«" and content[-1] in "\"'»":
        content = content[1:-1].strip()
    return content or None
