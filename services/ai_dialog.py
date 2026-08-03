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
    r"(не\s+пиши|не\s+пишите|отстань|отстаньте|заблокирую|в\s+блок|жалоб|"
    r"не\s+пиш(и|ите)\s+мне|leave\s+me|stop\s+writing|do\s+not\s+write|"
    r"fuck\s+off|пошёл\s+на|пошел\s+на|иди\s+нахуй|отвали)",
    re.IGNORECASE,
)

_SOFT_NO_RE = re.compile(
    r"(^\s*нет\s*[.!?]?\s*$|"
    r"неинтересно|не\s+интересно|не\s+надо|не\s+нужно|нет\s+спасибо|"
    r"не\s+хочу|не\s+актуально|не\s+сейчас|"
    r"не\s+торгую|не\s+торгую\s+уже|уже\s+не\s+торгу|"
    r"отошёл\s+от\s+трейд|отошел\s+от\s+трейд|не\s+в\s+рынке)",
    re.IGNORECASE,
)

_BAD_DASHES = ("\u2014", "\u2013", "\u2212")

# Small-talk that must NOT appear as "explain"
_GREETING_RE = re.compile(
    r"^\s*(привет|здравствуй|здравствуйте|добрый\s+(день|вечер|утро)|hello|hi)\b",
    re.IGNORECASE,
)

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

_EXPLAIN_BY_BRANCH: dict[str, list[str]] = {
    "self": [
        "Понял, сам в рынке. Написал потому что есть бесплатный канал - туда падают посты из закрытых VIP в одном месте, без покупки доступов.",
        "Ок, если сам торгуешь: есть бесплатный канал со сливами из платных випок. Можно просто глянуть, без оплаты.",
    ],
    "signals": [
        "Понял, по сигналам. Как раз многие так делают - есть бесплатный канал, где собраны посты из закрытых VIP, платить за каждый доступ не нужно.",
        "Ясно. Если смотришь сигналы: есть бесплатный канал со сливами из платных випок в одном месте. Без подписки на платное.",
    ],
    "newbie": [
        "Ок, если новичок - тем более можно просто посмотреть, как выглядит контент из закрытых VIP. Есть бесплатный канал, платить не нужно.",
        "Понял. Для новичка без давления: бесплатный канал с постами из закрытых випок, можно просто глянуть.",
    ],
    "other": [
        "Написал не просто так: есть бесплатный канал, туда выкладывают посты из закрытых VIP. Платить не нужно.",
        "По делу: бесплатный канал со сливами из платных випок. Можно просто глянуть, без обязательств.",
        "Хотел поделиться: канал без оплаты, контент из закрытых VIP-чатов в одном месте.",
    ],
}

_EXPLAIN_FALLBACKS = [t for branch in _EXPLAIN_BY_BRANCH.values() for t in branch]

_SELF_RE = re.compile(
    r"(сам(а)?\s+торг|торгую\s+сам|сам(а)?\s+открыв|сво(я|и)\s+сделк|без\s+сигнал)",
    re.IGNORECASE,
)
_SIGNALS_RE = re.compile(
    r"(сигнал|по\s+сигнал|чужи(е|м)\s+сигнал|копирую|копитрейд|по\s+чужим)",
    re.IGNORECASE,
)
_NEWBIE_RE = re.compile(
    r"(нович|только\s+нач|не\s+разбира|учу(сь)?|обуча|не\s+умею|первый\s+раз)",
    re.IGNORECASE,
)


def classify_user_reply(text: str) -> str:
    """self | signals | newbie | other — for explain angle only."""
    raw = (text or "").strip()
    if not raw:
        return "other"
    if _SIGNALS_RE.search(raw):
        return "signals"
    if _NEWBIE_RE.search(raw):
        return "newbie"
    if _SELF_RE.search(raw):
        return "self"
    return "other"


def _last_user_text(history: list[dict]) -> str:
    for item in reversed(history or []):
        if (item.get("role") or "") == "user":
            return str(item.get("text") or "")
    return ""


def _branch_instruction(branch: str, pitch: str) -> str:
    angles = {
        "self": (
            "Юзер торгует САМ. Угол: чтобы не искать по куче чатов - "
            "посты из закрытых VIP в одном бесплатном канале."
        ),
        "signals": (
            "Юзер по СИГНАЛАМ / чужим идеям. Угол: можно смотреть, "
            "что кидают в платных VIP, без покупки каждого доступа."
        ),
        "newbie": (
            "Юзер новичок / просит подсказать. Угол: без давления, "
            "просто посмотреть, как выглядит контент из закрытых VIP."
        ),
        "other": (
            "Ответ общий. Угол: зачем написал - бесплатный канал с постами "
            "из закрытых VIP, платить не нужно, можно просто глянуть."
        ),
    }
    angle = angles.get(branch, angles["other"])
    return (
        "Это НЕ светская беседа.\n"
        "Задача: 1-2 предложениями объяснить ЗАЧЕМ ты написал, "
        "с учётом ответа юзера.\n"
        f"Ветка: {branch}. {angle}\n"
        f"Суть оффера: {pitch}\n"
        "Можно коротко отзеркалить ответ (сам / сигналы / новичок) и сразу мостик к каналу.\n"
        "ЖЁСТКИЕ запреты:\n"
        "- не здоровайся снова\n"
        "- не спрашивай как дела / чем занимается\n"
        "- не задавай новых вопросов\n"
        "- без ссылок, без t.me, без http\n"
        "- без давления и обещаний дохода\n"
        "- только обычный дефис '-'\n"
        "Только текст ответа, 1-2 предложения."
    )

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


def followup_silence_text() -> str:
    """Soft pity follow-up after silence on first DM. No channel, no link."""
    return random.choice(
        [
            "Видимо не вовремя написал, извини. Больше не потревожу.",
            "Похоже зря отвлёк. Извини, не буду больше писать.",
            "Не хотел мешать. Извини за беспокойство, больше не пишу.",
            "Ок, по тишине понял. Извини что потревожил.",
            "Видимо не до того. Извини, больше не потревожу.",
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
    if _GREETING_RE.search(t):
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
    """Explain WHY we wrote: free channel pitch. NO link. Branch by user reply."""
    recent = phrases_svc.recent_texts(phrases_svc.KIND_EXPLAIN, limit=20)
    pitch = CHANNEL_PITCH or "бесплатный канал с постами из закрытых VIP"
    user_text = _last_user_text(history)
    branch = classify_user_reply(user_text)
    logger.info("explain branch={} user={!r}", branch, user_text[:60])

    if AI_DM_ENABLED and OPENAI_API_KEY:
        try:
            text = await _openai_reply(
                history,
                instruction=_branch_instruction(branch, pitch),
            )
            if text:
                text = _strip_em_dash(text)
                if CHANNEL_LINK and CHANNEL_LINK in text:
                    text = text.replace(CHANNEL_LINK, "").strip()
                if _explain_ok(text) and text not in recent:
                    return text
                logger.warning("explain rejected: {!r}", (text or "")[:80])
        except Exception as exc:
            logger.exception("generate_explain failed: {}", exc)

    pool = list(_EXPLAIN_BY_BRANCH.get(branch) or _EXPLAIN_BY_BRANCH["other"])
    pool = [x for x in pool if x not in recent] or pool
    # also allow other-branch fallbacks if exhausted
    if len(pool) <= 1:
        extra = [x for x in _EXPLAIN_FALLBACKS if x not in recent and x not in pool]
        pool = pool + extra
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
