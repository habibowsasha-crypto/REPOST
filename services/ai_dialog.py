"""AI personality and replies for the post-First-DM funnel.

Approved funnel:
1. First DM is already sent by the existing First DM module.
2. Any allowed user reaction starts one varied promo with the exact CHANNEL_LINK.
3. One varied smoothing apology is sent after the configured delay.
4. One varied link-opening instruction is sent after the next configured delay.
5. One remaining outgoing slot may answer the user's own follow-up message.
6. The absolute budget is five outgoing messages including First DM.

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

# A greeting is allowed only in the First DM. Every later funnel message must
# continue the existing conversation instead of greeting the same person again.
_POST_FIRST_DM_GREETING_RE = re.compile(
    r"^\s*(?:(?:привет(?:ик|ики)?|приветствую|здравствуй(?:те)?|"
    r"добрый\s+(?:день|вечер|утро)|доброго\s+(?:дня|вечера|утра)|"
    r"хай|здарова|салют|hello|hi)\b[\s,!.:;?_-]*)+",
    re.IGNORECASE,
)


def starts_with_post_first_dm_greeting(text: str) -> bool:
    """Return True when a post-First-DM message starts with a new greeting."""
    return bool(_POST_FIRST_DM_GREETING_RE.match(str(text or "")))


def _capitalize_first_letter(text: str) -> str:
    chars = list(str(text or ""))
    for index, char in enumerate(chars):
        if char.isalpha():
            chars[index] = char.upper()
            break
    return "".join(chars)


def sanitize_post_first_dm_text(text: str) -> str:
    """Remove an accidental repeated greeting from a later dialog message.

    Generators reject such output and retry. This sanitizer is the final delivery
    guard for legacy prepared outbox rows and external integrations. Text without
    a greeting keeps its original letter case.
    """
    value = _strip_bad_dashes(str(text or ""))
    greeting_removed = False
    previous = None
    while value and value != previous:
        previous = value
        updated = _POST_FIRST_DM_GREETING_RE.sub("", value, count=1)
        if updated == value:
            break
        greeting_removed = True
        value = updated.lstrip(" ,.!:;?-_")
    value = re.sub(r"[ \t]+", " ", value).strip()
    return _capitalize_first_letter(value) if greeting_removed else value

# Local rules are only a safety fallback. The main semantic classification is done by AI.
_STOP_REQUEST_RE = re.compile(
    r"(\bне\s+пиши(?:те)?(?:\s+мне)?\b|\bбольше\s+не\s+пиши(?:те)?\b|"
    r"\bне\s+надо\s+писать\b|\bне\s+нужно\s+писать\b|"
    r"\bне\s+беспокой(?:те)?\s+меня\b|\bбольше\s+не\s+беспокой(?:те)?\b|"
    r"\bне\s+беспокой(?:те)?(?:\s+меня)?\s+больше\b|"
    r"\bне\s+отвлекай(?:те)?\s+меня\b|\bбольше\s+не\s+отвлекай(?:те)?\b|"
    r"\bне\s+отвлекай(?:те)?(?:\s+меня)?\s+больше\b|"
    r"\bубери\s+меня\b|\bудали\s+меня\b|\bоставь\s+меня\s+в\s+покое\b|"
    r"\bотстань(?:те)?\b|"
    r"\bstop\s+writing\b|\bdo\s+not\s+write\b)",
    re.IGNORECASE,
)
_AGGRESSIVE_RE = re.compile(
    r"(отъеб|отъе\s*б|иди\s+нах|пош[её]л\s+нах|пошла\s+нах|"
    r"заебал|заебала|долбо[её]б|мудак|чмо|сдохни|fuck\s+off|"
    r"\bотвали\b|\bпроваливай\b|\bзаткнись\b|\bдостал(?:а)?(?:\s+уже)?\b|"
    r"\bиди\s+(?:лесом|к\s+ч[её]рту|на\s+хер|нахер)\b|"
    r"\bпош[её]л\s+ты\b|\bна\s+хер\b|\bнахер\b|угрож|жалобу\s+кину)",
    re.IGNORECASE,
)
_SOFT_NO_RE = re.compile(
    r"(^\s*нет\s*[.!?]?\s*$|неинтересно|не\s+интересно|не\s+надо|"
    r"не\s+нужно|нет\s*[,!.]?\s*спасибо|не\s+хочу|не\s+актуально|не\s+сейчас|"
    r"не\s+торгую|уже\s+не\s+торг|не\s+в\s+рынке|спасибо,?\s*не\s+надо)",
    re.IGNORECASE,
)
_LINK_REQUEST_RE = re.compile(
    r"(^|\s)(кинь|кидай|скинь|скидывай|дай|давай|ссылк|покажи|"
    r"хочу\s+посмотреть|где\s+канал|где\s+ссылка)(\s|$)",
    re.IGNORECASE,
)

_REQUIRED_SOFTWARE_RE = re.compile(r"(софт|программ)", re.IGNORECASE)
_REQUIRED_COPY_RE = re.compile(r"(копир|перенос|дублир|подхватыва|собира|попада)", re.IGNORECASE)
_REQUIRED_VIP_RE = re.compile(r"(vip|вип|закрыт)", re.IGNORECASE)
_REQUIRED_FREE_RE = re.compile(r"(бесплат|без\s+оплат|платить\s+не\s+надо)", re.IGNORECASE)
_REQUIRED_SPEED_RE = re.compile(
    r"(почти\s+сразу|почти\s+моменталь|сразу\s+после|минимальн.{0,8}задерж|быстро|оперативн)",
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
        "ок, понял",
        "ага, понял тебя",
        "ясно, тогда коротко",
        "понял, хорошо",
        "окей, тогда по делу",
        "ага, спасибо что ответил",
    ],
    CATEGORY_UNCLEAR: [
        "не совсем понял реакцию, но коротко объясню",
        "чуть не понял тебя, поэтому просто скажу",
        "не до конца уловил, тогда коротко",
    ],
    CATEGORY_LINK_REQUEST: [
        "да, держи",
        "ага, вот ссылка",
        "конечно, скидываю",
    ],
    CATEGORY_SOFT_REFUSAL: [
        "понял тебя, просто оставлю на всякий случай",
        "ок, без уговоров, только скину",
        "ясно, тогда просто оставлю ссылку",
    ],
}
_PROMO_TRANSITIONS = [
    "просто хотел оставить бесплатный канал",
    "на самом деле хотел поделиться бесплатным каналом",
    "я только хотел скинуть один бесплатный канал",
    "просто решил показать бесплатный канал",
    "хотел оставить тебе ссылку на бесплатный канал",
    "написал только ради одного бесплатного канала",
]
_PROMO_MECHANICS = [
    "там софт почти сразу копирует новые посты из закрытых випок",
    "туда программа почти моментально переносит публикации из закрытых VIP-каналов",
    "там софт быстро подхватывает материалы из закрытых торговых випок",
    "программа автоматически и с минимальной задержкой переносит туда посты из закрытых VIP-каналов",
    "программа почти сразу собирает и переносит свежие публикации из закрытых випок",
    "софт оперативно переносит туда новые материалы из закрытых VIP-каналов",
]
_PROMO_ACCESS = [
    "отдельные доступы покупать не надо",
    "платить за каждую випку отдельно не нужно",
    "можно обойтись без покупки отдельных VIP-доступов",
    "за несколько закрытых подписок отдельно платить не придётся",
    "не нужно оплачивать доступ к каждой випке по отдельности",
    "всё можно смотреть без отдельных платных подписок",
]
_PROMO_BENEFITS = [
    "глянь, вдруг найдёшь что-то полезное для своих сделок",
    "может попадётся полезная идея для торговли",
    "вдруг что-нибудь пригодится для твоего анализа",
    "может найдёшь интересный сетап или разбор",
    "посмотри, возможно там будет что-то полезное для твоих сделок",
    "можешь глянуть, вдруг найдётся подходящая торговая идея",
]

_SMOOTHING_FALLBACKS = [
    "Сорян, не хотел навязываться. Просто делюсь, вдруг пригодится тебе.",
    "Извини за внезапное сообщение. Просто решил поделиться, вдруг будет полезно.",
    "Не хотел напрягать, просто оставил на всякий случай. Может пригодится.",
    "Сорян, если отвлёк. Я только хотел поделиться полезной находкой.",
    "Извини, что написал без предупреждения. Просто подумал, что тебе может пригодиться.",
    "Не хотел быть навязчивым. Просто скинул, а смотреть или нет - уже тебе решать.",
    "Сорян за такой заход. Просто решил оставить, вдруг окажется полезным.",
    "Извини, если сообщение не к месту. Я лишь хотел поделиться.",
    "Не хотел мешать. Просто оставил информацию на случай, если пригодится.",
    "Сорян за внезапную личку. Просто делюсь без всяких уговоров.",
    "Извини, если не вовремя. Решил скинуть, вдруг найдёшь что-то полезное.",
    "Не хотел навязывать. Просто показал вариант, который может пригодиться.",
    "Сорян, что отвлёк. Просто подумал, что информация может быть полезной.",
    "Извини за неожиданное сообщение. Ничего не навязываю, просто поделился.",
    "Не хотел грузить. Просто оставил на всякий случай.",
    "Сорян, если помешал. Решил поделиться и больше не отвлекаю.",
    "Извини за сообщение в личку. Просто хотел оставить полезный вариант.",
    "Не хотел напрягать тебя. Просто скинул, вдруг когда-нибудь пригодится.",
    "Сорян за беспокойство. Просто решил поделиться без лишних уговоров.",
    "Извини, если отвлёк от дел. Просто оставил информацию на всякий случай.",
    "Не хотел показаться навязчивым. Просто делюсь тем, что может быть полезно.",
    "Сорян за неожиданность. Просто хотел, чтобы эта информация была у тебя.",
    "Извини, что в личку. Просто решил поделиться, вдруг зайдёт.",
    "Не хотел отвлекать надолго. Просто оставил и всё.",
]

_LINK_HELP_FALLBACKS = [
    "На всякий случай закрой крестиком панель «Заблокировать / Добавить» над чатом. Потом нажми ссылку ещё раз. Если Telegram всё равно не пускает - скопируй её вручную.",
    "Если ссылка не открывается, нажми крестик на панели «Заблокировать / Добавить» сверху чата и попробуй перейти повторно. Не поможет - скопируй ссылку вручную.",
    "Чтобы Telegram пропустил по ссылке, сначала закрой крестиком блок «Заблокировать / Добавить» над перепиской. Затем нажми ссылку снова, а если не сработает - скопируй её вручную.",
    "На случай проблемы со ссылкой: убери крестиком панель «Заблокировать / Добавить» над сообщениями и нажми ещё раз. Если переход не заработает - скопируй ссылку вручную.",
    "Если Telegram не даёт перейти, закрой крестиком окно «Заблокировать / Добавить» над чатом. После этого повторно нажми ссылку. В крайнем случае скопируй её вручную.",
    "Если ссылка сразу не нажимается, убери крестиком панель «Заблокировать / Добавить» сверху переписки и попробуй снова. Если всё равно не пускает - скопируй ссылку вручную.",
    "Небольшая подсказка: закрой крестиком панель «Заблокировать / Добавить» над диалогом, затем ещё раз нажми ссылку. Если Telegram не откроет - скопируй её вручную.",
    "Если с переходом проблема, сначала нажми крестик у панели «Заблокировать / Добавить» над чатом. Потом повтори нажатие на ссылку, а если не выйдет - скопируй её вручную.",
    "Telegram иногда мешает открыть ссылку. Закрой крестиком панель «Заблокировать / Добавить» над перепиской и нажми ссылку заново. Не сработает - скопируй вручную.",
    "Если канал не открывается, убери крестиком блок «Заблокировать / Добавить» сверху чата и снова нажми на ссылку. Если без результата - скопируй ссылку вручную.",
    "Для перехода закрой крестиком панель «Заблокировать / Добавить», которая находится над чатом, и нажми ссылку ещё раз. Если Telegram откажет - скопируй её вручную.",
    "Если Telegram тормозит переход, нажми крестик на панели «Заблокировать / Добавить» над сообщениями, затем попробуй ссылку повторно. Не получится - скопируй её вручную.",
    "Если по ссылке не пускает, сначала закрой крестиком верхнюю панель «Заблокировать / Добавить». Потом нажми ссылку снова. Если ничего не изменится - скопируй вручную.",
    "На всякий случай: убери крестиком предложение «Заблокировать / Добавить» над перепиской и повтори переход по ссылке. Если не откроется - скопируй её вручную.",
    "Когда ссылка не работает, закрой крестиком панель «Заблокировать / Добавить» вверху чата и попробуй ещё раз. Если Telegram снова не пустит - скопируй ссылку вручную.",
    "Если не получается зайти, нажми крестик возле панели «Заблокировать / Добавить» над чатом, после чего повторно открой ссылку. Не поможет - скопируй её вручную.",
    "Если Telegram блокирует нажатие, закрой крестиком панель «Заблокировать / Добавить» над диалогом и снова нажми ссылку. В крайнем случае скопируй её вручную.",
    "Если ссылка не срабатывает, убери крестиком верхний блок «Заблокировать / Добавить» и попробуй открыть её ещё раз. Если не выйдет - скопируй вручную.",
    "Telegram может не пускать из-за панели над чатом. Закрой крестиком «Заблокировать / Добавить», снова нажми ссылку, а если не откроется - скопируй её вручную.",
    "Если переход не открывается, сначала убери крестиком панель «Заблокировать / Добавить» над перепиской. Затем повтори нажатие, а при неудаче скопируй ссылку вручную.",
    "Для надёжности закрой крестиком блок «Заблокировать / Добавить» над сообщениями и нажми ссылку повторно. Если Telegram всё равно не откроет - скопируй её вручную.",
    "Если Telegram не реагирует на ссылку, нажми крестик у панели «Заблокировать / Добавить» сверху чата и повтори попытку. Не поможет - скопируй вручную.",
    "Если не пускает в канал, убери крестиком панель «Заблокировать / Добавить» над диалогом и ещё раз нажми ссылку. Если не получится - скопируй её вручную.",
    "Если ссылка остаётся недоступной, закрой крестиком «Заблокировать / Добавить» над чатом и попробуй снова. В крайнем случае скопируй ссылку вручную.",
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

_STOP_CLOSE_FALLBACKS = [
    "хорошо, больше писать не буду. извини за беспокойство",
    "понял, больше не напишу. извини что отвлёк",
    "ок, больше беспокоить не буду. всего доброго",
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
    # Terminal local decisions are safety rules, not hints for the model. A calm
    # refusal remains a non-terminal soft_refusal category and is allowed to enter
    # the approved promo branch. Only a request to stop or aggressive refusal may
    # terminate the funnel.
    if fallback in {
        CATEGORY_STOP_REQUEST,
        CATEGORY_AGGRESSIVE_REFUSAL,
    }:
        return fallback
    if fallback == CATEGORY_SOFT_REFUSAL:
        return fallback
    if not (AI_DM_ENABLED and OPENAI_API_KEY):
        return fallback

    instruction = (
        "Определи смысл последнего сообщения пользователя в Telegram.\n"
        "Верни ровно одно значение без пояснений:\n"
        "normal - обычный ответ или вопрос\n"
        "unclear - смысл нельзя уверенно понять\n"
        "link_request - просит ссылку или говорит скинуть\n"
        "soft_refusal - спокойно отказывается, не интересно, не торгует; эта категория не блокирует рекламу\n"
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


MAX_GENERATION_ATTEMPTS = 3
ANTI_REPEAT_WINDOW = 20


def _redact_for_ai(text: str) -> str:
    """Remove invite links and Telegram handles before sending context to OpenAI."""
    value = str(text or "")
    exact_link = (CHANNEL_LINK or "").strip()
    if exact_link:
        value = value.replace(exact_link, "[ссылка]")
    value = _URL_RE.sub("[ссылка]", value)
    value = _TELEGRAM_HANDLE_RE.sub("[username]", value)
    return value.strip()


def _history_messages_for_ai(history: list[dict]) -> list[dict[str, str]]:
    """Build a bounded and redacted model history without Telegram identifiers."""
    messages: list[dict[str, str]] = []
    for item in (history or [])[-10:]:
        role = "assistant" if item.get("role") == "assistant" else "user"
        content = _redact_for_ai(str(item.get("text") or ""))[:600]
        if content:
            messages.append({"role": role, "content": content})
    return messages


def _recent_block(recent: list[str]) -> str:
    if not recent:
        return ""
    # Do not send the administrator's invite link or Telegram handles back to AI.
    # Similarity checks already ignore URLs, so redaction does not weaken uniqueness.
    redacted = [_redact_for_ai(item) for item in recent[:ANTI_REPEAT_WINDOW]]
    return (
        "\nПоследние формулировки этого типа, которые нельзя повторять или близко копировать:\n"
        + "\n".join(f"- {item}" for item in redacted if item)
    )


def _pick_unique_fallback(
    candidates: list[str],
    recent: list[str],
    *,
    threshold: float,
) -> str:
    pool = list(candidates)
    random.shuffle(pool)
    for candidate in pool:
        clean = _strip_bad_dashes(candidate)
        if not is_too_similar(clean, recent, threshold=threshold):
            return clean
    recent_normalized = {_normalize_similarity(item) for item in recent[:ANTI_REPEAT_WINDOW]}
    exact_new = [item for item in pool if _normalize_similarity(item) not in recent_normalized]
    if exact_new:
        def max_score(item: str) -> float:
            return max((similarity_score(item, old) for old in recent[:ANTI_REPEAT_WINDOW]), default=0.0)
        return _strip_bad_dashes(min(exact_new, key=max_score))
    return _strip_bad_dashes(random.choice(pool))


def _build_local_promo_candidates(category: str, count: int = 100) -> list[str]:
    reactions = _PROMO_REACTIONS.get(category) or _PROMO_REACTIONS[CATEGORY_NORMAL]
    candidates: list[str] = []
    shapes = (
        "{reaction}. {transition} - {mechanics}. {benefit}, {access}",
        "{reaction}, {transition}. {mechanics}. {access}, {benefit}",
        "{reaction}. {transition}: {mechanics}. {access}. {benefit}",
        "{reaction}. {transition}, {mechanics}. {benefit}. {access}",
        "{reaction}, тогда коротко: {transition}. {mechanics}, поэтому {access}. {benefit}",
        "{reaction}. {transition}. {mechanics}, а {access}. {benefit}",
    )
    for _ in range(max(24, count)):
        candidates.append(
            _strip_bad_dashes(
                random.choice(shapes).format(
                    reaction=random.choice(reactions),
                    transition=random.choice(_PROMO_TRANSITIONS),
                    mechanics=random.choice(_PROMO_MECHANICS),
                    access=random.choice(_PROMO_ACCESS),
                    benefit=random.choice(_PROMO_BENEFITS),
                )
            )
        )
    return candidates


def _promo_opening_ok(text: str) -> bool:
    value = _strip_bad_dashes(text)
    if starts_with_post_first_dm_greeting(value):
        return False
    if not 2 <= len(value) <= 90:
        return False
    if "?" in value or "\n" in value:
        return False
    if _URL_RE.search(value) or _TELEGRAM_HANDLE_RE.search(value):
        return False
    if _FORBIDDEN_CLAIMS_RE.search(value):
        return False
    return True


def _build_promo_with_opening(opening: str, category: str) -> str:
    reaction = _strip_bad_dashes(opening).strip(" .,!?:;-")
    if not reaction:
        reaction = random.choice(
            _PROMO_REACTIONS.get(category) or _PROMO_REACTIONS[CATEGORY_NORMAL]
        )
    shapes = (
        "{reaction}. {transition} - {mechanics}. {benefit}, {access}",
        "{reaction}, {transition}. {mechanics}. {access}, {benefit}",
        "{reaction}. {transition}: {mechanics}. {access}. {benefit}",
        "{reaction}. {transition}, {mechanics}. {benefit}. {access}",
    )
    return _strip_bad_dashes(
        random.choice(shapes).format(
            reaction=reaction,
            transition=random.choice(_PROMO_TRANSITIONS),
            mechanics=random.choice(_PROMO_MECHANICS),
            access=random.choice(_PROMO_ACCESS),
            benefit=random.choice(_PROMO_BENEFITS),
        )
    )


def _promo_validation_errors(text: str) -> list[str]:
    value = _strip_bad_dashes(text)
    errors: list[str] = []
    if starts_with_post_first_dm_greeting(value):
        errors.append("repeated_greeting")
    if not 70 <= len(value) <= 520:
        errors.append("length")
    if _URL_RE.search(value) or _TELEGRAM_HANDLE_RE.search(value):
        errors.append("foreign_link")
    if _FORBIDDEN_CLAIMS_RE.search(value):
        errors.append("forbidden_claim")
    checks = (
        ("software", _REQUIRED_SOFTWARE_RE),
        ("copy", _REQUIRED_COPY_RE),
        ("vip", _REQUIRED_VIP_RE),
        ("free", _REQUIRED_FREE_RE),
        ("speed", _REQUIRED_SPEED_RE),
        ("benefit", _REQUIRED_BENEFIT_RE),
    )
    errors.extend(name for name, pattern in checks if not pattern.search(value))
    return errors


def _promo_ok(text: str) -> bool:
    return not _promo_validation_errors(text)


async def generate_promo(
    history: list[dict],
    *,
    category: str = CATEGORY_NORMAL,
    content_kind: str = "text",
) -> str:
    """Generate one varied promo with the exact configured channel link."""
    if is_non_text_reaction(_last_user_text(history), content_kind):
        category = CATEGORY_NORMAL
    if category not in {
        CATEGORY_NORMAL,
        CATEGORY_UNCLEAR,
        CATEGORY_LINK_REQUEST,
        CATEGORY_SOFT_REFUSAL,
    }:
        category = CATEGORY_NORMAL

    recent = phrases_svc.recent_texts(
        phrases_svc.KIND_PROMO,
        limit=ANTI_REPEAT_WINDOW,
    )
    instruction = (
        "Напиши одно живое рекламное сообщение для Telegram после короткого ответа пользователя.\n"
        "Смысл нужно передать каждый раз другими словами:\n"
        "- просто хотел оставить бесплатный канал\n"
        "- софт или программа почти сразу переносит публикации из закрытых VIP-каналов\n"
        "- не нужно платить за каждую випку отдельно\n"
        "- человеку может пригодиться идея, разбор или материал для его сделок\n"
        "Пиши разговорно, без давления, одним сообщением до 420 символов.\n"
        "Диалог уже начат. Не начинай с приветствия и не здоровайся повторно.\n"
        "Не задавай вопрос, не обещай прибыль, не выдумывай личный опыт.\n"
        "Не вставляй URL или @username - системная ссылка добавится автоматически.\n"
        "Используй только обычный дефис '-'. Верни только готовый текст.\n"
        f"Категория реакции пользователя: {category}.\n"
        f"Контекст администратора: {(CHANNEL_PITCH or 'бесплатный канал с материалами из закрытых VIP-каналов')[:300]}."
        + _recent_block(recent)
    )
    if AI_DM_ENABLED and OPENAI_API_KEY:
        for attempt in range(MAX_GENERATION_ATTEMPTS):
            try:
                retry_note = (
                    "\nПредыдущий вариант не прошёл проверку. Перестрой фразы и порядок мыслей заметно иначе."
                    if attempt else ""
                )
                text = await _openai_reply(
                    history,
                    instruction=instruction + retry_note,
                    temperature=0.85 if attempt == 0 else 1.0,
                )
                clean = _enforce_admin_link(text or "", include_link=False)
                errors = _promo_validation_errors(clean)
                if errors and _promo_opening_ok(clean):
                    clean = _build_promo_with_opening(clean, category)
                    errors = _promo_validation_errors(clean)
                if errors:
                    logger.warning(
                        "Promo rejected attempt={} reasons={} text={!r}",
                        attempt + 1,
                        ",".join(errors),
                        clean[:140],
                    )
                    continue
                if is_too_similar(clean, recent, threshold=0.82):
                    logger.warning("Promo too similar attempt={} text={!r}", attempt + 1, clean[:140])
                    continue
                return _enforce_admin_link(clean, include_link=True)
            except ChannelLinkNotConfiguredError:
                raise
            except Exception as exc:
                logger.warning("Promo AI attempt {} failed: {}", attempt + 1, exc)

    candidates = [item for item in _build_local_promo_candidates(category) if _promo_ok(item)]
    fallback = _pick_unique_fallback(candidates, recent, threshold=0.82)
    return _enforce_admin_link(fallback, include_link=True)


def _apology_ok(text: str) -> bool:
    value = _strip_bad_dashes(text)
    if starts_with_post_first_dm_greeting(value):
        return False
    if not 15 <= len(value) <= 180:
        return False
    if "?" in value or _URL_RE.search(value) or _TELEGRAM_HANDLE_RE.search(value):
        return False
    if re.search(r"(канал|vip|вип|софт|программ|ссылк)", value, re.IGNORECASE):
        return False
    return bool(re.search(r"(сорян|извин|не хотел|не хотел|прост|делюсь|поделиться|навяз)", value, re.IGNORECASE))


async def generate_smoothing_apology(history: list[dict]) -> str:
    recent = phrases_svc.recent_texts(
        phrases_svc.KIND_APOLOGY,
        limit=ANTI_REPEAT_WINDOW,
    )
    instruction = (
        "Напиши одно короткое человеческое сообщение после рекламы в Telegram.\n"
        "Смысл: слегка извиниться за внезапное личное сообщение, сказать, что не хотел навязываться и просто поделился, вдруг пригодится.\n"
        "Каждый раз передавай этот смысл по-разному. Не упоминай канал, VIP, софт или ссылку.\n"
        "Диалог уже идёт. Не начинай с приветствия и не здоровайся повторно.\n"
        "Не задавай вопрос. До 140 символов. Разговорный русский.\n"
        "Только обычный дефис '-'. Верни только сообщение."
        + _recent_block(recent)
    )
    if AI_DM_ENABLED and OPENAI_API_KEY:
        for attempt in range(MAX_GENERATION_ATTEMPTS):
            try:
                retry_note = "\nНапиши заметно иначе, чем предыдущая попытка." if attempt else ""
                text = await _openai_reply(
                    history,
                    instruction=instruction + retry_note,
                    temperature=0.9 if attempt == 0 else 1.05,
                )
                clean = _enforce_admin_link(text or "", include_link=False)
                if not _apology_ok(clean):
                    logger.warning("Apology rejected attempt={} text={!r}", attempt + 1, clean[:120])
                    continue
                if is_too_similar(clean, recent, threshold=0.84):
                    logger.warning("Apology too similar attempt={} text={!r}", attempt + 1, clean[:120])
                    continue
                return clean
            except Exception as exc:
                logger.warning("Apology AI attempt {} failed: {}", attempt + 1, exc)
    return _pick_unique_fallback(_SMOOTHING_FALLBACKS, recent, threshold=0.84)


def _link_help_ok(text: str) -> bool:
    value = _strip_bad_dashes(text)
    if starts_with_post_first_dm_greeting(value):
        return False
    if not 90 <= len(value) <= 360:
        return False
    if _URL_RE.search(value) or _TELEGRAM_HANDLE_RE.search(value):
        return False
    required = (
        re.search(r"заблокировать", value, re.IGNORECASE),
        re.search(r"добавить", value, re.IGNORECASE),
        re.search(r"(крестик|крестиком|закрой|убери|нажми\s+крест)", value, re.IGNORECASE),
        re.search(r"ссылк", value, re.IGNORECASE),
        re.search(r"(ещё\s+раз|снова|повтор|заново)", value, re.IGNORECASE),
        re.search(r"(скопируй|скопировать).{0,30}вручн|вручн.{0,30}(скопируй|скопировать)", value, re.IGNORECASE),
    )
    return all(required)


async def generate_link_open_help(history: list[dict]) -> str:
    """Generate the automatic instruction sent after the smoothing apology."""
    recent = phrases_svc.recent_texts(
        phrases_svc.KIND_LINK_HELP,
        limit=ANTI_REPEAT_WINDOW,
    )
    instruction = (
        "Напиши короткую понятную инструкцию для Telegram.\n"
        "Обязательный смысл:\n"
        "1. Закрыть крестиком панель «Заблокировать / Добавить» над чатом.\n"
        "2. Нажать ссылку ещё раз.\n"
        "3. Если Telegram всё равно не пускает - скопировать ссылку вручную.\n"
        "Передай этот смысл другими словами, но не потеряй ни один шаг.\n"
        "Диалог уже идёт. Не начинай с приветствия и не здоровайся повторно.\n"
        "Не вставляй сам URL. Не задавай вопрос. До 300 символов.\n"
        "Только обычный дефис '-'. Верни только готовую инструкцию."
        + _recent_block(recent)
    )
    if AI_DM_ENABLED and OPENAI_API_KEY:
        for attempt in range(MAX_GENERATION_ATTEMPTS):
            try:
                retry_note = "\nПредыдущая формулировка не подошла. Перестрой текст заметно иначе." if attempt else ""
                text = await _openai_reply(
                    history,
                    instruction=instruction + retry_note,
                    temperature=0.85 if attempt == 0 else 1.0,
                )
                clean = _enforce_admin_link(text or "", include_link=False)
                if not _link_help_ok(clean):
                    logger.warning("Link help rejected attempt={} text={!r}", attempt + 1, clean[:160])
                    continue
                if is_too_similar(clean, recent, threshold=0.88):
                    logger.warning("Link help too similar attempt={} text={!r}", attempt + 1, clean[:160])
                    continue
                return clean
            except Exception as exc:
                logger.warning("Link help AI attempt {} failed: {}", attempt + 1, exc)
    valid = [item for item in _LINK_HELP_FALLBACKS if _link_help_ok(item)]
    return _pick_unique_fallback(valid, recent, threshold=0.88)

def _qna_ok(text: str) -> bool:
    value = _strip_bad_dashes(text)
    if not 2 <= len(value) <= 350:
        return False
    if starts_with_post_first_dm_greeting(value):
        return False
    if _FORBIDDEN_CLAIMS_RE.search(value):
        return False
    return True


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
        "Диалог уже идёт. Не начинай с приветствия и не здоровайся повторно.\n"
        f"Категория сообщения: {category}.\n"
        "Системная ссылка уже была отправлена выше. Сам не пиши URL или @username.\n"
        "Только обычный дефис '-'. Верни только готовый ответ."
    )
    if AI_DM_ENABLED and OPENAI_API_KEY:
        try:
            text = await _openai_reply(history, instruction=instruction, temperature=0.65)
            text = _enforce_admin_link(text or "", include_link=False)
            if _qna_ok(text):
                return _enforce_admin_link(text, include_link=include_link)
            logger.warning("QNA rejected repeated greeting or invalid text={!r}", text[:140])
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
        return sanitize_post_first_dm_text(random.choice(_AGGRESSIVE_CLOSE_FALLBACKS))
    if category == CATEGORY_STOP_REQUEST:
        return sanitize_post_first_dm_text(random.choice(_STOP_CLOSE_FALLBACKS))
    return sanitize_post_first_dm_text(random.choice(_SOFT_CLOSE_FALLBACKS))


def apology_text() -> str:
    """Compatibility helper for an aggressive terminal apology."""
    return sanitize_post_first_dm_text(random.choice(_AGGRESSIVE_CLOSE_FALLBACKS))


def soft_close_text() -> str:
    return sanitize_post_first_dm_text(random.choice(_SOFT_CLOSE_FALLBACKS))


def followup_silence_text() -> str:
    """Existing 24-hour no-reply close after First DM. No promo and no link."""
    return sanitize_post_first_dm_text(random.choice(_FIRST_DM_SILENCE_FALLBACKS))


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
                "Диалог уже начат, поэтому не здоровайся повторно после First DM. "
                "Пиши коротко, просто и по-человечески. Не изображай продавца, "
                "эксперта или официального представителя. Не выдумывай личную "
                "биографию, сделки, прибыль, депозит, опыт или знакомства. "
                "Не обещай доход. Используй только обычный дефис '-'."
            ),
        },
        {"role": "system", "content": instruction},
    ]
    messages.extend(_history_messages_for_ai(history))

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
