"""AI personality and replies for the post-First-DM funnel.

Approved funnel:
1. First DM is already sent by the existing First DM module.
2. Any user reaction starts one short, contextual promo with the exact CHANNEL_LINK.
3. If the user stays silent, one short smoothing apology is sent after 5-60 seconds.
4. Remaining outgoing slots are used only to answer the user's own follow-up messages.
5. The absolute budget is five outgoing messages including First DM.

Voice notes, stickers, GIFs, photos, videos and emoji-only messages are always treated
as neutral or positive reactions. They are not transcribed or semantically inspected.
"""

from __future__ import annotations

import asyncio
import random
import re
import unicodedata
from difflib import SequenceMatcher
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

CATEGORY_NORMAL = "normal"
CATEGORY_UNCLEAR = "unclear"
CATEGORY_LINK_REQUEST = "link_request"
CATEGORY_SOFT_REFUSAL = "soft_refusal"
CATEGORY_STOP_REQUEST = "stop_request"
CATEGORY_AGGRESSIVE_REFUSAL = "aggressive_refusal"

ALLOWED_CATEGORIES = {
    CATEGORY_NORMAL,
    CATEGORY_UNCLEAR,
    CATEGORY_LINK_REQUEST,
    CATEGORY_SOFT_REFUSAL,
    CATEGORY_STOP_REQUEST,
    CATEGORY_AGGRESSIVE_REFUSAL,
}

_BAD_DASHES = ("\u2014", "\u2013", "\u2212")
_URL_RE = re.compile(
    r"(?i)(?:"
    r"(?:https?://|tg://|www\.|t\.me/)[^\s<>()]+"
    r"|(?:[a-z0-9-]+\.)+(?:com|net|org|io|ru|me|xyz|app|site)(?:/[^\s<>()]*)?"
    r")"
)
_TELEGRAM_HANDLE_RE = re.compile(r"(?<![\w@])@[A-Za-z][A-Za-z0-9_]{4,31}")

# Local rules are only a safety fallback. The main semantic classification is done by AI.
_STOP_REQUEST_RE = re.compile(
    r"(не\s+пиши|не\s+пишите|больше\s+не\s+пиши|не\s+надо\s+писать|"
    r"не\s+нужно\s+писать|не\s+беспокой|не\s+отвлекай|убери\s+меня|"
    r"удали\s+меня|отстань|отстаньте|не\s+пиш(и|ите)\s+мне|stop\s+writing|do\s+not\s+write)",
    re.IGNORECASE,
)
_AGGRESSIVE_RE = re.compile(
    r"(отъеб|отъе\s*б|иди\s+нах|пош[её]л\s+нах|пошла\s+нах|"
    r"заебал|заебала|долбо[её]б|мудак|чмо|сдохни|fuck\s+off|"
    r"отвали\s+нах|проваливай\s+нах|угрож|жалобу\s+кину)",
    re.IGNORECASE,
)
_SOFT_NO_RE = re.compile(
    r"(^\s*нет\s*[.!?]?\s*$|неинтересно|не\s+интересно|не\s+надо|"
    r"не\s+нужно|нет\s+спасибо|не\s+хочу|не\s+актуально|не\s+сейчас|"
    r"не\s+торгую|уже\s+не\s+торг|не\s+в\s+рынке|спасибо,?\s*не\s+надо)",
    re.IGNORECASE,
)
_LINK_REQUEST_RE = re.compile(
    r"(^|\s)(кинь|кидай|скинь|скидывай|дай|давай|ссылк|покажи|"
    r"хочу\s+посмотреть|где\s+канал|где\s+ссылка)(\s|$)",
    re.IGNORECASE,
)

_REQUIRED_SOFTWARE_RE = re.compile(r"(софт|программ)", re.IGNORECASE)
_REQUIRED_COPY_RE = re.compile(r"(копир|перенос|дублир|подхватыва)", re.IGNORECASE)
_REQUIRED_VIP_RE = re.compile(r"(vip|вип|закрыт)", re.IGNORECASE)
_REQUIRED_FREE_RE = re.compile(r"(бесплат|без\s+оплат|платить\s+не\s+надо)", re.IGNORECASE)
_REQUIRED_SPEED_RE = re.compile(
    r"(почти\s+сразу|почти\s+моменталь|сразу\s+после|минимальн.{0,8}задерж)",
    re.IGNORECASE,
)
_REQUIRED_BENEFIT_RE = re.compile(
    r"(пригод|полез|иде[юя]|сетап|анализ|сделк|сравн|разбор|посмотр)",
    re.IGNORECASE,
)
_FORBIDDEN_CLAIMS_RE = re.compile(
    r"(гарант|точно\s+заработ|прибыльн.{0,8}сигнал|всегда\s+отрабаты|"
    r"без\s+ошибок|100\s*%|я\s+заработ|мой\s+депозит|я\s+торгую\s+по\s+ним|"
    r"я\s+сам\s+по\s+ним|мои\s+сделки|лично\s+проверил)",
    re.IGNORECASE,
)

_PROMO_REACTIONS = {
    CATEGORY_NORMAL: [
        "понял тебя",
        "ага, понял",
        "ясно",
        "ок, понял",
        "понял)",
    ],
    CATEGORY_UNCLEAR: [
        "не совсем понял тебя 😅",
        "не до конца понял, что ты имел в виду",
        "чуть не понял реакцию)",
    ],
    CATEGORY_LINK_REQUEST: [
        "да, держи 👍",
        "ага, вот",
        "конечно, держи",
    ],
    CATEGORY_SOFT_REFUSAL: [
        "понял тебя",
        "ок, без проблем",
        "ясно, не буду уговаривать",
    ],
}
_PROMO_TRANSITIONS = [
    "я просто хотел скинуть бесплатный канал",
    "я вообще хотел поделиться бесплатным каналом",
    "написал по делу, есть бесплатный канал",
    "просто хотел оставить бесплатный канал",
]
_PROMO_MECHANICS = [
    "там софт почти сразу копирует новые посты из закрытых випок",
    "там программа автоматически переносит новые посты из закрытых випок почти сразу после выхода",
    "там софт почти моментально подхватывает публикации из закрытых VIP-каналов",
    "там программа сама копирует новые посты из закрытых випок с минимальной задержкой",
]
_PROMO_ACCESS = [
    "отдельные доступы покупать не надо",
    "платить за каждую випку отдельно не нужно",
    "без покупки отдельных VIP-доступов",
    "и за отдельные випки платить не надо",
]
_PROMO_BENEFITS = [
    "глянь, вдруг найдёшь что-то полезное для своих сделок",
    "может попадётся полезная идея для торговли",
    "вдруг что-то пригодится для твоего анализа",
    "может найдёшь интересный сетап или разбор",
    "можешь сравнить с тем, что сам сейчас смотришь",
]

_SMOOTHING_FALLBACKS = [
    "сорян что отвлёк, просто подумал вдруг тебе будет полезно",
    "извини что так в личку написал, просто решил поделиться",
    "не хотел навязываться, вдруг реально что-то пригодится",
    "сорян за внезапное сообщение, просто оставил на всякий случай",
    "извини если не вовремя, больше отвлекать не буду",
]

_FIRST_DM_SILENCE_FALLBACKS = [
    "видимо не вовремя написал, извини. больше не потревожу",
    "похоже зря отвлёк. извини, больше писать не буду",
    "не хотел мешать. извини за беспокойство",
    "ок, по тишине понял. извини что потревожил",
]

_AGGRESSIVE_CLOSE_FALLBACKS = [
    "понял, извини что побеспокоил",
    "ок, извини за беспокойство",
    "понял тебя, больше писать не буду",
]

_STOP_WITH_LINK_FALLBACKS = [
    "хорошо, больше писать не буду. только ссылку оставлю, вдруг когда-нибудь пригодится",
    "понял, больше не напишу. ссылку оставлю напоследок, вдруг потом пригодится",
    "ок, больше беспокоить не буду. только оставлю ссылку на случай если понадобится",
]

_SOFT_CLOSE_FALLBACKS = [
    "понял тебя, извини что отвлёк. больше уговаривать не буду",
    "ок, извини за беспокойство. больше не отвлекаю",
    "ясно, извини. тогда не буду грузить",
]

_QNA_FALLBACKS = [
    "там бесплатный канал с постами из закрытых випок, софт переносит их почти сразу после выхода",
    "точный список источников не подскажу, но посты из закрытых випок софт собирает в одном бесплатном канале",
    "можешь просто открыть и посмотреть, отдельные доступы покупать не надо",
    "ссылка выше, там всё можно спокойно посмотреть без покупки випок",
]


class ChannelLinkNotConfiguredError(RuntimeError):
    """Raised when a link step is reached without a valid admin CHANNEL_LINK."""


def configured_channel_link() -> str:
    link = (CHANNEL_LINK or "").strip()
    if not link or not re.match(r"(?i)^(?:https?://|tg://|t\.me/)", link):
        raise ChannelLinkNotConfiguredError(
            "CHANNEL_LINK is empty or invalid; exact admin link is required"
        )
    return link


def _strip_bad_dashes(text: str) -> str:
    out = sanitize_dashes(text or "")
    for dash in _BAD_DASHES:
        out = out.replace(dash, "-")
    return out.strip()


def _enforce_admin_link(text: str, *, include_link: bool) -> str:
    """Remove every model URL and optionally append the exact admin link once."""
    cleaned = _URL_RE.sub("", text or "")
    cleaned = _TELEGRAM_HANDLE_RE.sub("", cleaned)
    cleaned = re.sub(r"[ \t]+\n", "\n", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip(" \n-:")
    cleaned = _strip_bad_dashes(cleaned)
    if not include_link:
        return cleaned
    link = configured_channel_link()
    return f"{cleaned}\n{link}".strip()


def is_emoji_only(text: str) -> bool:
    """Return True for messages made only from emoji-like symbols and spacing."""
    raw = (text or "").strip()
    if not raw:
        return False
    saw_emoji = False
    for char in raw:
        if char.isspace() or char in {"\ufe0f", "\u200d", "\u20e3"}:
            continue
        category = unicodedata.category(char)
        codepoint = ord(char)
        if category in {"So", "Sk"} or 0x1F1E6 <= codepoint <= 0x1F1FF:
            saw_emoji = True
            continue
        return False
    return saw_emoji


def is_non_text_reaction(text: str, content_kind: str = "text") -> bool:
    return str(content_kind or "text") != "text" or is_emoji_only(text)


def local_category(text: str) -> str:
    raw = (text or "").strip()
    if not raw:
        return CATEGORY_NORMAL
    if _AGGRESSIVE_RE.search(raw):
        return CATEGORY_AGGRESSIVE_REFUSAL
    if _STOP_REQUEST_RE.search(raw):
        return CATEGORY_STOP_REQUEST
    if _SOFT_NO_RE.search(raw):
        return CATEGORY_SOFT_REFUSAL
    if _LINK_REQUEST_RE.search(raw):
        return CATEGORY_LINK_REQUEST
    if len(raw) <= 2 and not any(ch.isalnum() for ch in raw):
        return CATEGORY_UNCLEAR
    return CATEGORY_NORMAL


def is_hard_stop(text: str) -> bool:
    """Fast terminal safety hint used before the async AI classifier."""
    category = local_category(text)
    return category in {CATEGORY_STOP_REQUEST, CATEGORY_AGGRESSIVE_REFUSAL}


def is_soft_decline(text: str) -> bool:
    return local_category(text) == CATEGORY_SOFT_REFUSAL


def is_link_request(text: str) -> bool:
    return local_category(text) == CATEGORY_LINK_REQUEST


async def classify_user_message(
    history: list[dict],
    *,
    text: str,
    content_kind: str = "text",
) -> str:
    """Classify a textual reply. Every non-text or emoji-only reply is neutral."""
    if is_non_text_reaction(text, content_kind):
        return CATEGORY_NORMAL

    fallback = local_category(text)
    if not (AI_DM_ENABLED and OPENAI_API_KEY):
        return fallback

    instruction = (
        "Определи смысл последнего сообщения пользователя в Telegram.\n"
        "Верни ровно одно значение без пояснений:\n"
        "normal - обычный ответ или вопрос\n"
        "unclear - смысл нельзя уверенно понять\n"
        "link_request - просит ссылку или говорит скинуть\n"
        "soft_refusal - спокойно отказывается, не интересно, не торгует\n"
        "stop_request - спокойно просит больше не писать или удалить его\n"
        "aggressive_refusal - прямое оскорбление, агрессивный мат или угроза\n"
        "Одиночное 'не' после вопроса о торговле обычно означает обычный отрицательный "
        "ответ, а не просьбу прекратить сообщения."
    )
    try:
        result = await _openai_reply(history, instruction=instruction, temperature=0.0)
        category = (result or "").strip().lower().split()[0].strip(".,:;'")
        if category in ALLOWED_CATEGORIES:
            return category
    except Exception as exc:
        logger.warning("AI intent classifier fallback used: {}", exc)
    return fallback


def _normalize_similarity(text: str) -> str:
    value = _URL_RE.sub("", text or "").lower().replace("ё", "е")
    value = re.sub(r"[^a-zа-я0-9]+", " ", value)
    return " ".join(value.split())


def _token_set(text: str) -> set[str]:
    return set(_normalize_similarity(text).split())


def similarity_score(left: str, right: str) -> float:
    a = _normalize_similarity(left)
    b = _normalize_similarity(right)
    if not a or not b:
        return 0.0
    sequence = SequenceMatcher(None, a, b).ratio()
    ta = _token_set(a)
    tb = _token_set(b)
    union = ta | tb
    jaccard = len(ta & tb) / len(union) if union else 0.0
    return max(sequence, jaccard)


def is_too_similar(text: str, recent: list[str], *, threshold: float = 0.80) -> bool:
    normalized = _normalize_similarity(text)
    if not normalized:
        return True
    for old in recent:
        if normalized == _normalize_similarity(old):
            return True
        if similarity_score(text, old) >= threshold:
            return True
    return False


def _promo_instruction(category: str, pitch: str, *, retry: int) -> str:
    category_guidance = {
        CATEGORY_NORMAL: "Коротко отреагируй на ответ пользователя и естественно перейди к каналу.",
        CATEGORY_UNCLEAR: "Скажи, что не совсем понял, и без выдумок объясни, зачем написал.",
        CATEGORY_LINK_REQUEST: "Пользователь просит ссылку. Начни прямо: 'да, держи' или похоже.",
        CATEGORY_SOFT_REFUSAL: "Пользователь спокойно отказался. Не спорь. Сделай одну спокойную последнюю попытку.",
    }.get(category, "Коротко и естественно перейди к предложению.")
    retry_note = ""
    if retry:
        retry_note = (
            f"Это повторная генерация номер {retry}. Сильно измени начало, порядок фраз и "
            "формулировку пользы, но не меняй факты.\n"
        )
    return (
        "Напиши одно цельное сообщение как обычный пользователь Telegram, который интересуется криптой.\n"
        "Не пиши как менеджер, рекламный бот или официальный представитель.\n"
        "Обращайся на 'ты'. Пиши просто, обычно 1-3 коротких предложения.\n"
        f"Ситуация: {category}. {category_guidance}\n"
        f"Смысл канала: {pitch}\n"
        "Обязательные факты:\n"
        "- канал бесплатный\n"
        "- программа или софт автоматически копирует новые посты из закрытых VIP-каналов\n"
        "- посты появляются почти сразу после выхода\n"
        "- отдельные VIP-доступы покупать не нужно\n"
        "- скажи о возможной пользе: идея, сетап, анализ, разбор или сравнение со своим мнением\n"
        "Ссылку добавит система. Не пиши URL, t.me или @username.\n"
        "Не выдумывай свою биографию, сделки, прибыль, депозит, опыт или знакомых.\n"
        "Не обещай прибыль и не называй сигналы прибыльными.\n"
        "Не задавай новый вопрос. Не используй длинное тире. Только обычный дефис '-'.\n"
        f"{retry_note}"
        "Верни только готовое сообщение."
    )


def _promo_ok(text: str) -> bool:
    value = _strip_bad_dashes(text)
    if not 45 <= len(value) <= 420:
        return False
    if _URL_RE.search(value) or _TELEGRAM_HANDLE_RE.search(value):
        return False
    if _FORBIDDEN_CLAIMS_RE.search(value):
        return False
    checks = (
        _REQUIRED_SOFTWARE_RE,
        _REQUIRED_COPY_RE,
        _REQUIRED_VIP_RE,
        _REQUIRED_FREE_RE,
        _REQUIRED_SPEED_RE,
        _REQUIRED_BENEFIT_RE,
    )
    return all(pattern.search(value) for pattern in checks)


def _build_local_promo(category: str) -> str:
    reactions = _PROMO_REACTIONS.get(category) or _PROMO_REACTIONS[CATEGORY_NORMAL]
    reaction = random.choice(reactions)
    transition = random.choice(_PROMO_TRANSITIONS)
    mechanics = random.choice(_PROMO_MECHANICS)
    access = random.choice(_PROMO_ACCESS)
    benefit = random.choice(_PROMO_BENEFITS)

    shapes = [
        f"{reaction}. {transition}, {mechanics}. {access}, {benefit}",
        f"{reaction}. {transition} - {mechanics}. {benefit}, {access}",
        f"{reaction}, {transition}. {mechanics}, {access}. {benefit}",
        f"{reaction}. {mechanics}, а {access}. {benefit}",
    ]
    return _strip_bad_dashes(random.choice(shapes))


async def generate_promo(
    history: list[dict],
    *,
    category: str = CATEGORY_NORMAL,
    content_kind: str = "text",
) -> str:
    """Generate unique promo and append only the exact configured channel link."""
    if is_non_text_reaction(_last_user_text(history), content_kind):
        category = CATEGORY_NORMAL
    if category not in {
        CATEGORY_NORMAL,
        CATEGORY_UNCLEAR,
        CATEGORY_LINK_REQUEST,
        CATEGORY_SOFT_REFUSAL,
    }:
        category = CATEGORY_NORMAL

    recent = phrases_svc.recent_texts(phrases_svc.KIND_PROMO, limit=30)
    pitch = CHANNEL_PITCH or (
        "бесплатный канал, где софт почти сразу копирует новые посты из закрытых "
        "VIP-каналов, без покупки отдельных доступов"
    )

    if AI_DM_ENABLED and OPENAI_API_KEY:
        for retry in range(3):
            try:
                text = await _openai_reply(
                    history,
                    instruction=_promo_instruction(category, pitch, retry=retry),
                    temperature=0.82 if retry == 0 else 0.95,
                )
                text = _enforce_admin_link(text or "", include_link=False)
                if not _promo_ok(text):
                    logger.warning("Promo rejected by validator retry={} text={!r}", retry, text[:100])
                    continue
                if is_too_similar(text, recent):
                    logger.warning("Promo rejected as duplicate retry={} text={!r}", retry, text[:100])
                    continue
                return _enforce_admin_link(text, include_link=True)
            except ChannelLinkNotConfiguredError:
                raise
            except Exception as exc:
                logger.warning("Promo AI attempt {} failed: {}", retry + 1, exc)

    # Safe compositional fallback. Try several combinations against the same 30-message window.
    candidate = ""
    for _ in range(24):
        candidate = _build_local_promo(category)
        if _promo_ok(candidate) and not is_too_similar(candidate, recent):
            return _enforce_admin_link(candidate, include_link=True)
    return _enforce_admin_link(candidate or _build_local_promo(category), include_link=True)


async def generate_smoothing_apology(history: list[dict]) -> str:
    instruction = (
        "Напиши одно короткое человеческое сообщение после рекламы в Telegram.\n"
        "Нужно слегка извиниться за внезапное личное сообщение и сказать, что просто хотел поделиться.\n"
        "Не повторяй рекламу, не упоминай канал, VIP, софт или ссылку.\n"
        "Не задавай вопрос. До 100 символов. Можно разговорно: сорян, извини, не хотел навязываться.\n"
        "Только обычный дефис '-'. Верни только сообщение."
    )
    if AI_DM_ENABLED and OPENAI_API_KEY:
        try:
            text = await _openai_reply(history, instruction=instruction, temperature=0.8)
            text = _enforce_admin_link(text or "", include_link=False)
            if 10 <= len(text) <= 140 and not _URL_RE.search(text):
                if not re.search(r"(канал|vip|вип|софт|программ)", text, re.IGNORECASE):
                    return text
        except Exception as exc:
            logger.warning("Smoothing apology fallback used: {}", exc)
    return random.choice(_SMOOTHING_FALLBACKS)


async def generate_qna_reply(
    history: list[dict],
    *,
    category: str = CATEGORY_NORMAL,
    content_kind: str = "text",
    include_link: bool = False,
) -> str:
    """Answer one user-initiated follow-up inside the remaining message budget."""
    if is_non_text_reaction(_last_user_text(history), content_kind):
        base = random.choice(
            [
                "ага 👍 можешь спокойно глянуть, вдруг что-то пригодится",
                "понял) ссылка выше, можешь посмотреть когда будет удобно",
                "ок 👍 просто оставил, вдруг потом пригодится",
            ]
        )
        return _enforce_admin_link(base, include_link=include_link)

    instruction = (
        "Ответь на последнее сообщение пользователя как обычный человек из крипто-чата Telegram.\n"
        "Ответ должен быть коротким, простым и по сути, обычно 1-2 предложения.\n"
        "Факты, которые можно использовать:\n"
        "- канал бесплатный\n"
        "- софт автоматически копирует новые посты из закрытых VIP-каналов почти сразу после выхода\n"
        "- отдельные VIP-доступы покупать не нужно\n"
        "- пользователь может посмотреть и найти идею, сетап или материал для анализа\n"
        "Если точного ответа нет, честно скажи, что точно не подскажешь.\n"
        "Не выдумывай список каналов, свою биографию, сделки, прибыль, результаты или личный опыт.\n"
        "Не обещай прибыль. Не начинай новую рекламную воронку.\n"
        f"Категория сообщения: {category}.\n"
        "Системная ссылка уже была отправлена выше. Сам не пиши URL или @username.\n"
        "Только обычный дефис '-'. Верни только готовый ответ."
    )
    if AI_DM_ENABLED and OPENAI_API_KEY:
        try:
            text = await _openai_reply(history, instruction=instruction, temperature=0.65)
            text = _enforce_admin_link(text or "", include_link=False)
            if 2 <= len(text) <= 350 and not _FORBIDDEN_CLAIMS_RE.search(text):
                return _enforce_admin_link(text, include_link=include_link)
        except ChannelLinkNotConfiguredError:
            raise
        except Exception as exc:
            logger.warning("QNA AI fallback used: {}", exc)
    return _enforce_admin_link(random.choice(_QNA_FALLBACKS), include_link=include_link)


async def generate_terminal_reply(
    history: list[dict],
    *,
    category: str,
) -> str:
    """Generate the single terminal message for stop or aggressive refusal."""
    if category == CATEGORY_AGGRESSIVE_REFUSAL:
        return random.choice(_AGGRESSIVE_CLOSE_FALLBACKS)
    if category == CATEGORY_STOP_REQUEST:
        return _enforce_admin_link(random.choice(_STOP_WITH_LINK_FALLBACKS), include_link=True)
    return random.choice(_SOFT_CLOSE_FALLBACKS)


def apology_text() -> str:
    """Compatibility helper for an aggressive terminal apology."""
    return random.choice(_AGGRESSIVE_CLOSE_FALLBACKS)


def soft_close_text() -> str:
    return random.choice(_SOFT_CLOSE_FALLBACKS)


def followup_silence_text() -> str:
    """Existing 24-hour no-reply close after First DM. No promo and no link."""
    return random.choice(_FIRST_DM_SILENCE_FALLBACKS)


# Compatibility helpers retained for existing integrations and historical tests.
_SELF_RE = re.compile(r"(сам(а)?\s+торг|торгую\s+сам|сво(я|и)\s+сделк)", re.IGNORECASE)
_SIGNALS_RE = re.compile(r"(сигнал|по\s+сигнал|копитрейд)", re.IGNORECASE)
_NEWBIE_RE = re.compile(r"(нович|только\s+нач|не\s+разбира|учу(сь)?)", re.IGNORECASE)
_EXPLAIN_BY_BRANCH = {
    "self": ["понял, сам в рынке. хотел поделиться бесплатным каналом"],
    "signals": ["понял, смотришь сигналы. хотел поделиться бесплатным каналом"],
    "newbie": ["понял. хотел оставить бесплатный канал, вдруг пригодится"],
    "other": ["я вообще хотел поделиться бесплатным каналом"],
}
_EXPLAIN_FALLBACKS = [item for values in _EXPLAIN_BY_BRANCH.values() for item in values]


def classify_user_reply(text: str) -> str:
    raw = (text or "").strip()
    if _SIGNALS_RE.search(raw):
        return "signals"
    if _NEWBIE_RE.search(raw):
        return "newbie"
    if _SELF_RE.search(raw):
        return "self"
    return "other"


def first_bot_was_soft(history: list[dict]) -> bool:
    """Legacy helper. New funnel no longer inserts a second engage question."""
    return False


async def generate_engage_question(history: list[dict]) -> str:
    return random.choice(
        [
            "ты сейчас больше альты смотришь или биток?",
            "сам сделки ищешь или сигналы тоже смотришь?",
        ]
    )


async def generate_explain(history: list[dict]) -> str:
    """Legacy no-link explanation kept for compatibility only."""
    category = classify_user_reply(_last_user_text(history))
    branch = _EXPLAIN_BY_BRANCH.get(category) or _EXPLAIN_BY_BRANCH["other"]
    return random.choice(branch)


async def generate_link_wrap(history: list[dict]) -> str:
    """Legacy helper now returns a complete promo with the exact link."""
    return await generate_promo(history, category=CATEGORY_LINK_REQUEST)


async def generate_contextual_reply(history: list[dict], *, include_link: bool) -> str:
    return await generate_qna_reply(history, include_link=include_link)


def _last_user_text(history: list[dict]) -> str:
    for item in reversed(history or []):
        if (item.get("role") or "") == "user":
            return str(item.get("text") or "")
    return ""


async def _openai_reply(
    history: list[dict],
    *,
    instruction: str,
    temperature: float = 0.7,
) -> Optional[str]:
    from openai import AsyncOpenAI

    client = AsyncOpenAI(
        api_key=OPENAI_API_KEY,
        timeout=AI_REQUEST_TIMEOUT_SECONDS,
        max_retries=1,
    )
    messages = [
        {
            "role": "system",
            "content": (
                "Ты обычный пользователь Telegram, который интересуется криптой. "
                "Пиши коротко, просто и по-человечески. Не изображай продавца, "
                "эксперта или официального представителя. Не выдумывай личную "
                "биографию, сделки, прибыль, депозит, опыт или знакомства. "
                "Не обещай доход. Используй только обычный дефис '-'."
            ),
        },
        {"role": "system", "content": instruction},
    ]
    for item in (history or [])[-10:]:
        role = "assistant" if item.get("role") == "assistant" else "user"
        messages.append({"role": role, "content": str(item.get("text") or "")[:600]})

    response = await asyncio.wait_for(
        client.chat.completions.create(
            model=AI_MODEL,
            messages=messages,
            temperature=float(temperature),
            max_tokens=220,
        ),
        timeout=AI_REQUEST_TIMEOUT_SECONDS + 2.0,
    )
    content = (response.choices[0].message.content or "").strip()
    if len(content) >= 2 and content[0] in "\"'«" and content[-1] in "\"'»":
        content = content[1:-1].strip()
    return _strip_bad_dashes(content) or None
