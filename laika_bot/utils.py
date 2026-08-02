from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import ROUND_FLOOR, Decimal, InvalidOperation
from random import SystemRandom

import regex


@dataclass(frozen=True)
class ParsedChannelLink:
    kind: str
    value: str
    canonical: str
    invite_hash: str | None = None


def parse_channel_link(raw: str) -> ParsedChannelLink:
    value = raw.strip()
    if not value:
        raise ValueError("Пустая ссылка")

    if value.startswith("@"):
        username = value[1:].strip()
        _validate_username(username)
        return ParsedChannelLink("public", username, f"https://t.me/{username}")

    value = re.sub(r"^tg://resolve\?domain=", "https://t.me/", value, flags=re.IGNORECASE)
    match = re.match(r"^https?://(?:www\.)?(?:t\.me|telegram\.me)/(.+?)/?$", value, flags=re.IGNORECASE)
    if not match:
        raise ValueError("Нужна ссылка t.me, telegram.me или @username")

    path = match.group(1).split("?", 1)[0].strip("/")
    if path.startswith("+"):
        invite_hash = path[1:]
        if not invite_hash:
            raise ValueError("Некорректная пригласительная ссылка")
        return ParsedChannelLink("private", invite_hash, f"https://t.me/+{invite_hash}", invite_hash)
    if path.lower().startswith("joinchat/"):
        invite_hash = path.split("/", 1)[1]
        if not invite_hash:
            raise ValueError("Некорректная пригласительная ссылка")
        return ParsedChannelLink("private", invite_hash, f"https://t.me/+{invite_hash}", invite_hash)

    username = path.split("/", 1)[0]
    _validate_username(username)
    return ParsedChannelLink("public", username, f"https://t.me/{username}")


def _validate_username(username: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9_]{5,64}", username):
        raise ValueError("Некорректный username канала")


def parse_reactions(text: str, *, max_items: int = 10) -> list[str]:
    clusters = [item for item in regex.findall(r"\X", text.strip()) if not item.isspace()]
    result: list[str] = []
    for item in clusters:
        if item in {",", ";", "|"}:
            continue
        if item not in result:
            result.append(item)
        if len(result) >= max_items:
            break
    if not result:
        raise ValueError("Не удалось распознать реакции")
    return result


_REACTION_TOKEN_PATTERN = regex.compile(
    r"(?P<reaction>\X)(?:\s*[:=]?\s*(?P<weight>\d+(?:[.,]\d+)?)(?:\s*%)?)?"
)
_REACTION_SEPARATOR_CLUSTERS = {",", ";", "|", ":", "=", "%", "·"}
_MAX_REACTION_WEIGHT = Decimal("1000000000")


def _is_emoji_cluster(value: str) -> bool:
    return bool(regex.search(r"\p{Extended_Pictographic}|\p{Regional_Indicator}|\u20e3", value))


def parse_weighted_reactions(text: str, *, max_items: int = 10) -> dict[str, float]:
    """Parse emoji reactions with optional positive numeric weights.

    Accepted examples: ``👍60 ❤️20 🔥20``, ``👍 6 ❤️ 2 🔥 2`` and
    ``👍 ❤️ 🔥``. When no numbers are supplied, reactions receive equal
    weights. Explicit and implicit weights cannot be mixed in one message.
    """

    source = text.strip()
    if not source:
        raise ValueError("Не удалось распознать реакции")
    if regex.search(r"(?:^|[\s:=])-\s*\d", source):
        raise ValueError("Вес реакции не может быть отрицательным")

    parsed: list[tuple[str, str | None]] = []
    consumed: list[tuple[int, int]] = []
    for match in _REACTION_TOKEN_PATTERN.finditer(source):
        reaction = match.group("reaction")
        if reaction.isspace() or reaction in _REACTION_SEPARATOR_CLUSTERS:
            continue
        if not _is_emoji_cluster(reaction):
            continue
        parsed.append((reaction, match.group("weight")))
        consumed.append(match.span())

    if not parsed:
        raise ValueError("Не удалось распознать реакции")

    # Reject unexpected text instead of silently accepting typos.
    leftovers = list(source)
    for start, end in consumed:
        for index in range(start, end):
            leftovers[index] = " "
    remainder = "".join(leftovers)
    remainder = regex.sub(r"[\s,;|:=·%]+", "", remainder)
    if remainder:
        raise ValueError("Используйте формат: 👍60 ❤️20 🔥20")

    has_explicit = [raw is not None for _, raw in parsed]
    if any(has_explicit) and not all(has_explicit):
        raise ValueError("Укажите вес для каждой реакции либо отправьте только эмодзи")

    result: dict[str, Decimal] = {}
    for reaction, raw_weight in parsed:
        weight = Decimal("1")
        if raw_weight is not None:
            try:
                weight = Decimal(raw_weight.replace(",", "."))
            except InvalidOperation as exc:
                raise ValueError("Вес реакции должен быть числом") from exc
            if not weight.is_finite():
                raise ValueError("Вес реакции должен быть конечным числом")
            if weight < 0:
                raise ValueError("Вес реакции не может быть отрицательным")
            if weight > _MAX_REACTION_WEIGHT:
                raise ValueError("Слишком большой вес реакции")
        result[reaction] = result.get(reaction, Decimal("0")) + weight
        if len(result) > max_items:
            raise ValueError(f"Можно указать не больше {max_items} реакций")

    if sum(result.values(), Decimal("0")) <= 0:
        raise ValueError("Хотя бы одна реакция должна иметь вес больше нуля")
    return {reaction: float(weight) for reaction, weight in result.items()}


def normalize_reaction_percentages(
    weights: Mapping[str, float], *, decimals: int = 2
) -> list[tuple[str, Decimal]]:
    """Return displayed percentages whose total is exactly 100%."""

    if decimals < 0 or decimals > 6:
        raise ValueError("Некорректная точность процентов")
    prepared: list[tuple[str, Decimal]] = []
    for reaction, raw_weight in weights.items():
        try:
            weight = Decimal(str(raw_weight))
        except InvalidOperation as exc:
            raise ValueError("Некорректный вес реакции") from exc
        if not weight.is_finite() or weight < 0:
            raise ValueError("Некорректный вес реакции")
        prepared.append((str(reaction), weight))
    total = sum((weight for _, weight in prepared), Decimal("0"))
    if not prepared or total <= 0:
        raise ValueError("Сумма весов должна быть больше нуля")

    scale = 10**decimals
    total_units = 100 * scale
    exact_units = [(weight / total) * total_units for _, weight in prepared]
    floor_units = [int(value.to_integral_value(rounding=ROUND_FLOOR)) for value in exact_units]
    remainder = total_units - sum(floor_units)
    order = sorted(
        range(len(prepared)),
        key=lambda index: (exact_units[index] - floor_units[index], -index),
        reverse=True,
    )
    for index in order[:remainder]:
        floor_units[index] += 1

    divisor = Decimal(scale)
    return [
        (prepared[index][0], Decimal(floor_units[index]) / divisor)
        for index in range(len(prepared))
    ]


def format_reaction_weights(weights: Mapping[str, float], *, decimals: int = 2) -> str:
    parts: list[str] = []
    for reaction, percentage in normalize_reaction_percentages(weights, decimals=decimals):
        rendered = format(percentage, "f")
        if "." in rendered:
            rendered = rendered.rstrip("0").rstrip(".")
        parts.append(f"{reaction} {rendered}%")
    return " · ".join(parts)


def choose_weighted_reaction(
    weights: Mapping[str, float], rng: SystemRandom | None = None
) -> str:
    """Choose one reaction according to its configured positive weight."""

    population: list[str] = []
    numeric_weights: list[float] = []
    for reaction, raw_weight in weights.items():
        weight = float(raw_weight)
        if weight > 0:
            population.append(str(reaction))
            numeric_weights.append(weight)
    if not population:
        raise ValueError("Нет реакций с положительным весом")
    generator = rng or SystemRandom()
    return generator.choices(population, weights=numeric_weights, k=1)[0]


def display_account_name(display_name: str, username: str | None) -> str:
    return f"{display_name} (@{username})" if username else display_name


_EMAIL_LOCAL_RE = re.compile(r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+$")
_EMAIL_DOMAIN_LABEL_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$")
_EMAIL_PROVIDER_NAMES = {
    "gmail.com": "Gmail",
    "googlemail.com": "Gmail",
    "outlook.com": "Microsoft",
    "hotmail.com": "Microsoft",
    "live.com": "Microsoft",
    "icloud.com": "iCloud",
    "me.com": "iCloud",
    "mac.com": "iCloud",
    "yahoo.com": "Yahoo",
    "mail.ru": "Mail.ru",
    "inbox.ru": "Mail.ru",
    "list.ru": "Mail.ru",
    "bk.ru": "Mail.ru",
    "yandex.ru": "Яндекс",
    "yandex.com": "Яндекс",
    "ya.ru": "Яндекс",
    "rambler.ru": "Rambler",
    "proton.me": "Proton Mail",
    "protonmail.com": "Proton Mail",
}


def normalize_email_login(raw: str) -> tuple[str, str]:
    """Validate an email login and return ``(normalized, provider)``.

    Only the domain is lower-cased. The local part is preserved because its
    case-sensitivity is controlled by the receiving mail service. Passwords,
    recovery codes and one-time codes are intentionally outside this model.
    """

    value = (raw or "").strip()
    if not value or len(value) > 320:
        raise ValueError("Введите корректный адрес почты")
    if any(character.isspace() or ord(character) < 32 for character in value):
        raise ValueError("Адрес почты не должен содержать пробелы")
    if value.count("@") != 1:
        raise ValueError("Введите адрес в формате name@example.com")

    local, domain = value.rsplit("@", 1)
    if not local or len(local) > 64 or local.startswith(".") or local.endswith("."):
        raise ValueError("Некорректная часть адреса до @")
    if ".." in local or not _EMAIL_LOCAL_RE.fullmatch(local):
        raise ValueError("Некорректная часть адреса до @")

    domain = domain.rstrip(".").lower()
    if not domain or len(domain) > 255 or "." not in domain or ".." in domain:
        raise ValueError("Некорректный домен почты")
    labels = domain.split(".")
    if any(not _EMAIL_DOMAIN_LABEL_RE.fullmatch(label) for label in labels):
        raise ValueError("Некорректный домен почты")
    if len(labels[-1]) < 2:
        raise ValueError("Некорректный домен почты")

    normalized = f"{local}@{domain}"
    provider = _EMAIL_PROVIDER_NAMES.get(domain, domain)
    return normalized, provider


def truncate(text: str | None, limit: int = 160) -> str:
    if not text:
        return "-"
    compact = " ".join(text.split())
    return compact if len(compact) <= limit else compact[: limit - 1] + "…"


# Official Telegram service notifications account (login codes, etc.)
TELEGRAM_SERVICE_USER_ID = 777000

_LOGIN_CODE_PATTERNS = (
    re.compile(
        r"(?:login\s*code|код\s*(?:для\s*)?(?:входа(?:\s*в\s*telegram)?))"
        r"[^\d]{0,30}(\d{5,6})",
        re.IGNORECASE | re.UNICODE,
    ),
    re.compile(
        r"(?:your\s+)?(?:login|verification)\s+code\s*(?:is)?\s*[:：]?\s*(\d{5,6})",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:код\s*подтверждения|код\s*авторизации)[^\d]{0,20}(\d{5,6})",
        re.IGNORECASE | re.UNICODE,
    ),
)


def extract_login_code(text: str | None) -> str | None:
    """Extract a one-time Telegram login code from a service message body.

    Returns the digits only, or None when the text is not a login-code notice.
    The code itself must never be written to logs or persisted.
    """

    if not text:
        return None
    compact = " ".join(str(text).split())
    for pattern in _LOGIN_CODE_PATTERNS:
        match = pattern.search(compact)
        if match:
            return match.group(1)
    # Fallback: service notices that only contain a single 5-6 digit token
    # together with an explicit "code/код" keyword.
    lowered = compact.casefold()
    if any(token in lowered for token in ("login", "код", "code")):
        digits = re.findall(r"(?<!\d)(\d{5,6})(?!\d)", compact)
        if len(digits) == 1:
            return digits[0]
    return None
