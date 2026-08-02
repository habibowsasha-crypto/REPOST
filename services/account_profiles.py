from __future__ import annotations

import asyncio
import datetime
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence

from loguru import logger
from telethon import TelegramClient
from telethon.sessions import StringSession

from config import API_HASH, API_ID, conn

PROFILE_MAX_AGE_HOURS = 24
PROFILE_REFRESH_TIMEOUT_SECONDS = 8.0
PROFILE_REFRESH_CONCURRENCY = 3


@dataclass(frozen=True)
class AccountProfile:
    user_id: int
    username: Optional[str]
    first_name: Optional[str]
    last_name: Optional[str]
    updated_at: Optional[str]


def _clean(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = " ".join(str(value).replace("\n", " ").split()).strip()
    return text or None


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _parse_timestamp(value: Optional[str]) -> Optional[datetime.datetime]:
    if not value:
        return None
    try:
        parsed = datetime.datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed.astimezone(datetime.timezone.utc)


def save_account_profile(entity: Any) -> AccountProfile:
    """Persist display-only Telegram profile fields for a connected account."""
    user_id = int(getattr(entity, "id"))
    username = _clean(getattr(entity, "username", None))
    first_name = _clean(getattr(entity, "first_name", None))
    last_name = _clean(getattr(entity, "last_name", None))
    updated_at = _now_iso()
    with conn:
        conn.execute(
            """
            UPDATE sessions
               SET username=?, first_name=?, last_name=?, profile_updated_at=?
             WHERE user_id=?
            """,
            (username, first_name, last_name, updated_at, user_id),
        )
    return AccountProfile(user_id, username, first_name, last_name, updated_at)


def get_account_profile(user_id: int) -> AccountProfile:
    row = conn.execute(
        """
        SELECT user_id, username, first_name, last_name, profile_updated_at
          FROM sessions WHERE user_id=?
        """,
        (int(user_id),),
    ).fetchone()
    if not row:
        return AccountProfile(int(user_id), None, None, None, None)
    return AccountProfile(
        user_id=int(row[0]),
        username=_clean(row[1]),
        first_name=_clean(row[2]),
        last_name=_clean(row[3]),
        updated_at=row[4],
    )


def profile_needs_refresh(
    profile: AccountProfile,
    *,
    max_age_hours: int = PROFILE_MAX_AGE_HOURS,
    now: Optional[datetime.datetime] = None,
) -> bool:
    """Return True for empty or stale cached Telegram profile data."""
    if not (profile.username or profile.first_name or profile.last_name):
        return True
    updated = _parse_timestamp(profile.updated_at)
    if updated is None:
        return True
    current = now or datetime.datetime.now(datetime.timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=datetime.timezone.utc)
    return current.astimezone(datetime.timezone.utc) - updated > datetime.timedelta(
        hours=max(1, int(max_age_hours))
    )


def account_display_name(profile: AccountProfile) -> str:
    if profile.username:
        return f"@{profile.username.lstrip('@')}"
    full_name = " ".join(
        part for part in (profile.first_name, profile.last_name) if part
    ).strip()
    return full_name or "Аккаунт"


def _truncate_name(text: str, limit: int) -> str:
    text = " ".join((text or "").replace("\n", " ").split()).strip()
    if limit <= 0:
        return ""
    if len(text) <= limit:
        return text
    if limit == 1:
        return "…"
    return text[: limit - 1].rstrip() + "…"


def format_account_label(
    user_id: int,
    *,
    include_id: bool = True,
    max_length: int = 44,
) -> str:
    """Format a UI label while never truncating the stable numeric Telegram ID."""
    profile = get_account_profile(int(user_id))
    name = account_display_name(profile)
    numeric_id = str(profile.user_id)

    if not include_id:
        if name == "Аккаунт":
            return f"Аккаунт #{numeric_id}"
        return _truncate_name(name, max(1, int(max_length)))

    if name == "Аккаунт":
        return f"Аккаунт #{numeric_id}"

    suffix = f" | {numeric_id}"
    available_for_name = max(1, int(max_length) - len(suffix))
    return f"{_truncate_name(name, available_for_name)}{suffix}"


async def refresh_account_profile(
    user_id: int,
    session_string: Optional[str] = None,
    *,
    client: Optional[TelegramClient] = None,
) -> Optional[AccountProfile]:
    """Refresh cached profile data using an active client when available."""
    own_client = client is None
    if session_string is None:
        row = conn.execute(
            "SELECT session_string FROM sessions WHERE user_id=?", (int(user_id),)
        ).fetchone()
        if not row:
            return None
        session_string = str(row[0])

    tg_client = client or TelegramClient(StringSession(session_string), API_ID, API_HASH)
    try:
        if own_client and not tg_client.is_connected():
            await tg_client.connect()
        if not await tg_client.is_user_authorized():
            logger.warning(f"Сессия аккаунта {user_id} не авторизована при обновлении профиля")
            return None
        me = await tg_client.get_me()
        if int(getattr(me, "id", 0) or 0) != int(user_id):
            logger.warning(
                f"Сессия вернула другой user_id: ожидался {user_id}, получен {getattr(me, 'id', None)}"
            )
            return None
        return save_account_profile(me)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.warning(f"Не удалось обновить профиль аккаунта {user_id}: {exc}")
        return None
    finally:
        if own_client:
            try:
                await tg_client.disconnect()
            except Exception as exc:
                logger.debug(f"Не удалось отключить клиент профиля {user_id}: {exc}")


async def refresh_stale_account_profiles(
    rows: Sequence[tuple[int, str]],
    *,
    active_clients: Optional[Mapping[int, TelegramClient]] = None,
    force: bool = False,
    concurrency: int = PROFILE_REFRESH_CONCURRENCY,
    timeout_seconds: float = PROFILE_REFRESH_TIMEOUT_SECONDS,
) -> tuple[int, int, int]:
    """Refresh empty/stale profiles with bounded parallelism and per-account timeout.

    Returns ``(updated, failed, skipped)``. Failures never erase the last cached
    display name, so the menu remains usable during Telegram outages.
    """
    active_clients = active_clients or {}
    semaphore = asyncio.Semaphore(max(1, int(concurrency)))
    updated = 0
    failed = 0
    skipped = 0

    async def refresh_one(user_id: int, session_string: str) -> str:
        profile = get_account_profile(user_id)
        if not force and not profile_needs_refresh(profile):
            return "skipped"
        async with semaphore:
            try:
                result = await asyncio.wait_for(
                    refresh_account_profile(
                        user_id,
                        session_string,
                        client=active_clients.get(int(user_id)),
                    ),
                    timeout=max(0.05, float(timeout_seconds)),
                )
            except asyncio.TimeoutError:
                logger.warning(f"Тайм-аут обновления профиля аккаунта {user_id}")
                return "failed"
            return "updated" if result is not None else "failed"

    results = await asyncio.gather(
        *(refresh_one(int(user_id), str(session_string)) for user_id, session_string in rows),
        return_exceptions=True,
    )
    for result in results:
        if isinstance(result, BaseException):
            if isinstance(result, asyncio.CancelledError):
                raise result
            logger.warning(f"Неожиданная ошибка обновления профиля: {result}")
            failed += 1
        elif result == "updated":
            updated += 1
        elif result == "skipped":
            skipped += 1
        else:
            failed += 1
    return updated, failed, skipped
