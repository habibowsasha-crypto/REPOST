from __future__ import annotations

import re
from datetime import datetime
from typing import Final

from .models import Account, Channel, utcnow

MAX_MANAGEMENT_QUERY_LENGTH: Final[int] = 80
ACCOUNT_FILTERS: Final[dict[str, str]] = {
    "all": "Все",
    "active": "Активные",
    "disabled": "Выключенные",
    "problem": "Проблемные",
    "flood": "FloodWait",
    "no_email": "Без почты",
    "error": "С ошибками",
}
TARGET_FILTERS: Final[dict[str, str]] = {
    "all": "Все",
    "active": "Активные",
    "disabled": "Выключенные",
    "error": "С ошибками",
}
TARGET_KIND_CODES: Final[dict[str, str]] = {"c": "channel", "g": "group"}
BULK_ACCOUNT_ACTIONS: Final[set[str]] = {"enable", "disable", "audit", "refresh"}


def normalize_management_query(value: str | None) -> str:
    query = re.sub(r"\s+", " ", (value or "").strip())
    if not query:
        raise ValueError("Введите непустой поисковый запрос")
    if not query.isprintable():
        raise ValueError("Поисковый запрос содержит недопустимые управляющие символы")
    if len(query) > MAX_MANAGEMENT_QUERY_LENGTH:
        raise ValueError(
            f"Поисковый запрос не может быть длиннее {MAX_MANAGEMENT_QUERY_LENGTH} символов"
        )
    return query


def _contains_query(values: tuple[object, ...], query: str | None) -> bool:
    if not query:
        return True
    needle = query.casefold()
    return any(needle in str(value or "").casefold() for value in values)


def account_matches(
    account: Account,
    *,
    filter_key: str,
    query: str | None = None,
    now: datetime | None = None,
) -> bool:
    if filter_key not in ACCOUNT_FILTERS:
        raise ValueError("Недопустимый фильтр аккаунтов")
    current = now or utcnow()
    is_problem = account.status == "unauthorized"
    has_future_flood = bool(account.flood_until and account.flood_until > current)
    matches_filter = {
        "all": True,
        "active": account.is_active and not is_problem,
        "disabled": not account.is_active and not is_problem,
        "problem": is_problem,
        "flood": has_future_flood,
        "no_email": not bool(account.email_login),
        "error": bool(account.last_error or account.problem_reason),
    }[filter_key]
    if not matches_filter:
        return False
    return _contains_query(
        (
            account.display_name,
            account.username,
            account.phone,
            account.email_login,
            account.telegram_user_id,
        ),
        query,
    )


def target_matches(
    target: Channel,
    *,
    kind: str,
    filter_key: str,
    query: str | None = None,
) -> bool:
    if kind not in {"channel", "group"}:
        raise ValueError("Недопустимый тип цели")
    if filter_key not in TARGET_FILTERS:
        raise ValueError("Недопустимый фильтр целей")
    if target.kind != kind:
        return False
    matches_filter = {
        "all": True,
        "active": target.is_active,
        "disabled": not target.is_active,
        "error": bool(target.last_error),
    }[filter_key]
    if not matches_filter:
        return False
    # Private links and invite hashes are intentionally excluded from search state.
    return _contains_query(
        (target.title, target.username, target.telegram_channel_id), query
    )


def parse_account_management_callback(data: str | None) -> tuple[str, int]:
    parts = (data or "").split(":")
    if len(parts) != 4 or parts[:2] != ["manage", "a"]:
        raise ValueError("Некорректная команда управления аккаунтами")
    filter_key = parts[2]
    if filter_key not in ACCOUNT_FILTERS:
        raise ValueError("Недопустимый фильтр аккаунтов")
    try:
        page = int(parts[3])
    except ValueError as exc:
        raise ValueError("Некорректная страница") from exc
    if page < 0 or page > 100_000:
        raise ValueError("Некорректная страница")
    return filter_key, page


def parse_target_management_callback(data: str | None) -> tuple[str, str, int]:
    parts = (data or "").split(":")
    if len(parts) != 5 or parts[:2] != ["manage", "t"]:
        raise ValueError("Некорректная команда управления целями")
    kind = TARGET_KIND_CODES.get(parts[2])
    if kind is None:
        raise ValueError("Недопустимый тип цели")
    filter_key = parts[3]
    if filter_key not in TARGET_FILTERS:
        raise ValueError("Недопустимый фильтр целей")
    try:
        page = int(parts[4])
    except ValueError as exc:
        raise ValueError("Некорректная страница") from exc
    if page < 0 or page > 100_000:
        raise ValueError("Некорректная страница")
    return kind, filter_key, page


def parse_bulk_account_action(data: str | None, *, confirmed: bool) -> str:
    prefix = ["manage", "abc" if confirmed else "ab"]
    parts = (data or "").split(":")
    if len(parts) != 3 or parts[:2] != prefix:
        raise ValueError("Некорректная массовая команда")
    action = parts[2]
    if action not in BULK_ACCOUNT_ACTIONS:
        raise ValueError("Недопустимое массовое действие")
    return action


def parse_copy_channel_callback(
    data: str | None, *, action: str
) -> tuple[int, int]:
    expected = {
        "list": "copy",
        "confirm": "copyc",
        "apply": "copya",
    }.get(action)
    if expected is None:
        raise ValueError("Неизвестное действие копирования")
    parts = (data or "").split(":")
    if len(parts) != 4 or parts[:2] != ["channel", expected]:
        raise ValueError("Некорректная команда копирования")
    try:
        source_id = int(parts[2])
        second = int(parts[3])
    except ValueError as exc:
        raise ValueError("Некорректный идентификатор канала") from exc
    if source_id < 1 or second < 0:
        raise ValueError("Некорректный идентификатор канала")
    if action != "list":
        if second < 1:
            raise ValueError("Некорректный идентификатор канала")
        if second == source_id:
            raise ValueError("Нельзя копировать настройки канала в него самого")
    elif second > 100_000:
        raise ValueError("Некорректная страница")
    return source_id, second
