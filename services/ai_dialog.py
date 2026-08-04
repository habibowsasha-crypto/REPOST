"""AI replies for post-first-DM funnel + stop detection.

Воронка (не small talk):
1) First DM уже ушёл (короткий вопрос).
2) Первый ответ юзера → generate_explain: ЗАЧЕМ написал (канал), БЕЗ ссылки.
3) Тишина 60–120с → auto-link со ссылкой.
4) Если юзер пишет дальше → кратко по делу + можно дать ссылку.
"""

from __future__ import annotations

import asyncio
import random
import re
from typing import Optional

from loguru import logger

from config import (
    AI_DM_ENABLED,
    AI_MODEL,
    AI_REQUEST_TIMEOUT_SECONDS,
    CHANNEL_LINK,
    CHANNEL_PITCH,
    OPENAI_API_KEY,
)
from services import phrases as phrases_svc
from services.ai_first_dm import sanitize_dashes

_STOP_RE = re.compile(
    r"(не\s+пиши|не\s+пишите|больше\s+не\s+пиши|не\s+надо\s+писать|"
    r"не\s+нужно\s+писать|не\s+беспокой|не\s+отвлекай|убери\s+меня|"
    r"удали\s+меня|отстань|отстаньте|заблокирую|в\s+блок|жалоб|спам|"
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
_URL_RE = re.compile(
    r"(?i)(?:"
    r"(?:https?://|tg://|www\.|t\.me/)[^\s<>()]+"
    r"|(?:[a-z0-9-]+\.)+(?:com|net|org|io|ru|me|xyz|app|site)(?:/[^\s<>()]*)?"
    r")"
)
_TELEGRAM_HANDLE_RE = re.compile(r"(?<![\w@])@[A-Za-z][A-Za-z0-9_]{4,31}")


class ChannelLinkNotConfiguredError(RuntimeError):
    """Raised when a link step is reached without a valid admin CHANNEL_LINK."""


def configured_channel_link() -> str:
    link = (CHANNEL_LINK or "").strip()
    if not link or not re.match(r"(?i)^(?:https?://|tg://|t\.me/)", link):
        raise ChannelLinkNotConfiguredError(
            "CHANNEL_LINK is empty or invalid; exact admin link is required"
        )
    return link


def _enforce_admin_link(text: str, *, include_link: bool) -> str:
    """Remove every model-provided URL and optionally append the exact admin link once."""
    cleaned = _URL_RE.sub("", text or "")
    cleaned = _TELEGRAM_HANDLE_RE.sub("", cleaned)
    cleaned = re.sub(r"[ \t]+\n", "\n", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip(" \n-:")
    if not include_link:
        return cleaned
    link = configured_channel_link()
    return f"{cleaned}\n{link}".strip()

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
        "Понял, сам в рынке. Написал потому что есть бесплатный канал - туда моментально копируются посты из VIP-каналов, без покупки доступов.",
        "Ок. Если сам торгуешь: бесплатный канал, посты из VIP копируются сразу как выходят. Можно просто глянуть.",
    ],
    "signals": [
        "Понял, по сигналам. Как раз для этого - бесплатный канал, туда моментально летят посты из VIP-каналов, платить за випки не нужно.",
        "Ясно. Смотришь сигналы: у нас в бесплатном канале посты из VIP копируются сразу, всё в одном месте.",
    ],
    "newbie": [
        "Ок, если новичок - тем более. Есть бесплатный канал: посты из VIP-каналов копируются моментально, платить не нужно.",
        "Понял. Без давления: бесплатный канал, контент из VIP прилетает сразу. Можно просто глянуть.",
    ],
    "other": [
        "Написал не просто так: бесплатный канал, посты из VIP-каналов копируются моментально. Платить не нужно.",
        "По делу: канал без оплаты, посты из VIP копируются сразу как появляются. Можно глянуть без обязательств.",
        "Хотел поделиться: бесплатный канал, моментальные посты из VIP, без покупки доступов.",
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
    angle = {
        "self": "Юзер сам торгует. Отзеркаль и мостик: бесплатный канал, посты из VIP копируются моментально.",
        "signals": "Юзер по сигналам. Отзеркаль и мостик: VIP-посты прилетают сразу, без оплаты випок.",
        "newbie": "Новичок. Мягко: бесплатный канал, моментальные посты из VIP, можно просто глянуть.",
        "other": "Коротко зачем написал: VIP-посты копируются моментально в бесплатный канал.",
    }.get(branch, "Коротко зачем написал + мостик.")
    return (
        "Пиши как живой человек в Telegram, не как менеджер.\n"
        "1-2 коротких предложения. Можно отзеркалить ответ юзера.\n"
        f"Ветка: {branch}. {angle}\n"
        f"Суть: {pitch}\n"
        "БЕЗ ссылок, t.me, http. БЕЗ повторного приветствия.\n"
        "БЕЗ 'интересно как', БЕЗ давления и обещаний дохода.\n"
        "Не задавай новый длинный опрос. Можно максимум один короткий хвост.\n"
        "Только обычный дефис '-'. Только текст."
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
            "Понял, извини что отвлёк. Больше не буду писать.",
            "Ок, извини за беспокойство. Продолжать не буду.",
            "Понял тебя, извини. Больше не отвлекаю.",
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



def first_bot_was_soft(history: list[dict]) -> bool:
    """True if the first assistant message was a soft hook (можно спросить / не отвлеку)."""
    for h in history or []:
        if (h.get("role") or "") == "assistant":
            text = (h.get("text") or "").lower()
            soft_markers = (
                "можно спросить",
                "можно коротко",
                "можно на минут",
                "не отвлек",
                "не помеш",
                "не занят",
                "есть секунд",
                "есть минут",
                "есть минутка",
                "хотел спросить",
                "привет",
                "салют",
            )
            # Soft if markers and NOT already a trading question
            if any(m in text for m in soft_markers):
                if not any(
                    x in text
                    for x in ("сигнал", "торгу", "стратег", "рынк", "новичк", "график")
                ):
                    return True
            return False
    return False


def is_link_request(text: str) -> bool:
    t = (text or "").lower()
    return any(
        w in t
        for w in (
            "кинь",
            "скинь",
            "ссылк",
            "давай",
            "дай",
            "можно глянуть",
            "покажи",
            "хочу посмотреть",
            "интересн",
        )
    ) and not is_soft_decline(text) and not is_hard_stop(text)


_ENGAGE_FALLBACKS = [
    "А ты щас сам торгуешь или по сигналам?",
    "Ты сейчас в рынке или на паузе?",
    "Сам график смотришь или сигналы ловишь?",
    "А как обычно в сделки заходишь?",
]


async def generate_engage_question(history: list[dict]) -> str:
    """Second message after soft first DM: real short question, no pitch, no link."""
    instruction = (
        "Юзер уже ответил на твоё мягкое «можно спросить».\n"
        "Сейчас задай ОДИН короткий вопрос по трейдингу, как живой человек в чате.\n"
        "Варианты смысла: сам/сигналы, в рынке ли, как входит в сделки.\n"
        "ЖЁСТКО:\n"
        "- без рекламы, канала, ссылок, оффера\n"
        "- не здоровайся снова\n"
        "- 1 короткое предложение, до 70 символов\n"
        "- разговорно (щас, а, ну ок)\n"
        "- только обычный дефис '-'\n"
        "Только текст вопроса."
    )
    try:
        text = await _openai_reply(history, instruction=instruction)
        if text:
            text = _strip_em_dash(text)
            if text and len(text) <= 90 and "t.me" not in text.lower() and "http" not in text.lower():
                if not _GREETING_RE.search(text):
                    return text
    except Exception as exc:
        logger.warning("Engage-question AI fallback used: {}", exc)
    return random.choice(_ENGAGE_FALLBACKS)


async def generate_explain(history: list[dict]) -> str:
    """Explain WHY we wrote: free channel pitch. NO link. Branch by user reply."""
    recent = phrases_svc.recent_texts(phrases_svc.KIND_EXPLAIN, limit=20)
    pitch = CHANNEL_PITCH or "бесплатный канал: посты из VIP копируются моментально"
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
                text = _enforce_admin_link(text, include_link=False)
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
    """Soft CTA with exactly the link configured by the admin."""
    link = configured_channel_link()
    recent = phrases_svc.recent_texts(phrases_svc.KIND_LINK, limit=20)
    if AI_DM_ENABLED and OPENAI_API_KEY:
        try:
            text = await _openai_reply(
                history,
                instruction=(
                    "Нужно мягко дать ссылку на канал и слегка извиниться за беспокойство.\n"
                    f"Ссылка будет добавлена системой отдельно и точно: {link}\n"
                    "Сам не пиши никаких URL или @username.\n"
                    "Структура 1-2 коротких предложения: извинись и скажи, что можно "
                    "просто посмотреть без обязательств. Без давления и длинного тире."
                ),
            )
            if text:
                text = _strip_em_dash(text)
                text = _enforce_admin_link(text, include_link=True)
                if text not in recent:
                    return text
        except ChannelLinkNotConfiguredError:
            raise
        except Exception as exp:
            logger.exception("generate_link_wrap failed: {}", exp)

    pool = [t for t in _LINK_FALLBACKS if t.format(link=link) not in recent]
    if not pool:
        pool = list(_LINK_FALLBACKS)
    return _enforce_admin_link(random.choice(pool).format(link=link), include_link=True)


async def generate_contextual_reply(history: list[dict], *, include_link: bool) -> str:
    """Short funnel reply; any included URL is always the exact admin link."""
    link = configured_channel_link() if include_link else ""
    pitch = CHANNEL_PITCH or "бесплатный канал: посты из VIP копируются моментально"

    if AI_DM_ENABLED and OPENAI_API_KEY:
        try:
            if include_link:
                extra = (
                    "Сначала коротко извинись за беспокойство, затем скажи, что можно "
                    "просто посмотреть без обязательств. Ссылку добавит система: "
                    "не пиши URL или @username самостоятельно."
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
                    "Запрещено: повторный привет, small talk, личные вопросы, давление, "
                    "любые самостоятельно придуманные ссылки и длинное тире.\n"
                    "1-3 коротких предложения по делу."
                ),
            )
            if text:
                text = _strip_em_dash(text)
                if _SMALLTALK_RE.search(text):
                    logger.warning("contextual smalltalk rejected: {!r}", text[:80])
                else:
                    return _enforce_admin_link(text, include_link=include_link)
        except ChannelLinkNotConfiguredError:
            raise
        except Exception as exc:
            logger.exception("contextual reply failed: {}", exc)

    if include_link:
        return _enforce_admin_link(random.choice(_LINK_FALLBACKS).format(link=link), include_link=True)
    return _enforce_admin_link(random.choice(_EXPLAIN_FALLBACKS), include_link=False)


async def _openai_reply(history: list[dict], *, instruction: str) -> Optional[str]:
    from openai import AsyncOpenAI

    client = AsyncOpenAI(api_key=OPENAI_API_KEY, timeout=AI_REQUEST_TIMEOUT_SECONDS, max_retries=1)
    messages = [
        {
            "role": "system",
            "content": (
                "Ты пишешь от лица человека в Telegram, но цель - мягко "
                "донести бесплатный канал (посты из VIP-каналов (копируются моментально)), не болтать.\n"
                "Пиши коротко по-русски. Только дефис '-'. "
                "Не обещай доход. Не спорь. Не устраивай small talk."
            ),
        },
        {"role": "system", "content": instruction},
    ]
    for item in (history or [])[-8]:
        role = "assistant" if item.get("role") == "assistant" else "user"
        messages.append({"role": role, "content": str(item.get("text") or "")[:500]})

    resp = await asyncio.wait_for(
        client.chat.completions.create(
            model=AI_MODEL,
            messages=messages,
            temperature=0.7,
            max_tokens=160,
        ),
        timeout=AI_REQUEST_TIMEOUT_SECONDS + 2.0,
    )
    content = (resp.choices[0].message.content or "").strip()
    if len(content) >= 2 and content[0] in "\"'«" and content[-1] in "\"'»":
        content = content[1:-1].strip()
    return content or None
