"""Safe @SpamBot restriction monitoring for Telegram user accounts.

By default the monitor does not resume first-DM sending automatically.  It
checks the official Telegram @SpamBot status after a PeerFlood pause and
notifies the administrator when the account reports that it is free from
restrictions.  Per-account auto_resume may optionally clear the PeerFlood
pause after a free reply; FloodWait cooldowns are never bypassed.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import inspect
import re
import threading
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

from loguru import logger

from config import conn

UTC = dt.timezone.utc
SPAMBOT_USERNAME = "@SpamBot"
SPAMBOT_COMMAND = "/start"
RESTRICTION_GRACE_SECONDS = 60
NO_RESPONSE_RETRY_SECONDS = 15 * 60
UNRECOGNIZED_RETRY_SECONDS = 15 * 60
CLIENT_UNAVAILABLE_RETRY_SECONDS = 60
CHECK_LEASE_SECONDS = 5 * 60
RESPONSE_TIMEOUT_SECONDS = 35

_STATUS_DISABLED = "disabled"
_STATUS_IDLE = "idle"
_STATUS_PENDING = "pending"
_STATUS_CHECKING = "checking"
_STATUS_RESTRICTED = "restricted"
_STATUS_WAITING_CLIENT = "waiting_client"
_STATUS_ERROR = "error"
_STATUS_FREE = "free_detected"

_db_lock = threading.RLock()
_runtime_locks: dict[int, tuple[asyncio.AbstractEventLoop, asyncio.Lock]] = {}

_FREE_PHRASES = (
    "ваш аккаунт свободен от каких-либо ограничений",
    "ваш аккаунт свободен от каких либо ограничений",
    "your account is free from any limitations",
    "no limits are currently applied to your account",
    "you're free as a bird",
    "you are free as a bird",
)

_MONTHS = {
    "jan": 1,
    "january": 1,
    "янв": 1,
    "января": 1,
    "feb": 2,
    "february": 2,
    "фев": 2,
    "февраля": 2,
    "mar": 3,
    "march": 3,
    "мар": 3,
    "марта": 3,
    "apr": 4,
    "april": 4,
    "апр": 4,
    "апреля": 4,
    "may": 5,
    "май": 5,
    "мая": 5,
    "jun": 6,
    "june": 6,
    "июн": 6,
    "июня": 6,
    "jul": 7,
    "july": 7,
    "июл": 7,
    "июля": 7,
    "aug": 8,
    "august": 8,
    "авг": 8,
    "августа": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "сен": 9,
    "сент": 9,
    "сентября": 9,
    "oct": 10,
    "october": 10,
    "окт": 10,
    "октября": 10,
    "nov": 11,
    "november": 11,
    "ноя": 11,
    "ноября": 11,
    "dec": 12,
    "december": 12,
    "дек": 12,
    "декабря": 12,
}

_RESTRICTION_DATE_RE = re.compile(
    r"(?P<day>\d{1,2})\s+"
    r"(?P<month>[A-Za-zА-Яа-яЁё.]+)\s+"
    r"(?P<year>\d{4})\s*,?\s*"
    r"(?P<hour>\d{1,2}):(?P<minute>\d{2})\s*UTC\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class SpamBotMonitorState:
    account_user_id: int
    is_enabled: bool
    auto_resume: bool
    status: str
    next_check_at: Optional[str]
    restriction_until: Optional[str]
    last_checked_at: Optional[str]
    last_response_text: Optional[str]
    last_error: Optional[str]
    updated_at: str


@dataclass(frozen=True)
class SpamBotCheckResult:
    account_user_id: int
    outcome: str
    response_text: Optional[str] = None
    restriction_until: Optional[str] = None
    next_check_at: Optional[str] = None
    error: Optional[str] = None
    auto_resumed: bool = False


def utc_now() -> dt.datetime:
    return dt.datetime.now(UTC)


def _iso(value: dt.datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()



def _clean_text(value: Any, limit: int = 4000) -> Optional[str]:
    if value is None:
        return None
    text = " ".join(str(value).replace("\x00", " ").split()).strip()
    if not text:
        return None
    return text[:limit]


def _normalize(value: Any) -> str:
    text = str(value or "").lower().replace("ё", "е")
    text = re.sub(r"[^a-zа-я0-9]+", " ", text, flags=re.IGNORECASE)
    return " ".join(text.split())


def is_spambot_free_response(text: Any) -> bool:
    normalized = _normalize(text)
    return any(_normalize(phrase) in normalized for phrase in _FREE_PHRASES)


def parse_spambot_restriction_until(text: Any) -> Optional[dt.datetime]:
    raw = str(text or "")
    match = _RESTRICTION_DATE_RE.search(raw)
    if not match:
        return None
    month_token = match.group("month").lower().replace("ё", "е").rstrip(".")
    month = _MONTHS.get(month_token)
    if month is None:
        return None
    try:
        return dt.datetime(
            int(match.group("year")),
            int(month),
            int(match.group("day")),
            int(match.group("hour")),
            int(match.group("minute")),
            tzinfo=UTC,
        )
    except ValueError:
        return None


def _ensure_row(account_user_id: int) -> None:
    now = _iso(utc_now())
    with _db_lock, conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO dm_spambot_monitor (
                account_user_id, is_enabled, auto_resume, status, next_check_at,
                restriction_until, last_checked_at, last_response_text,
                last_error, updated_at
            ) VALUES (?, 0, 0, ?, NULL, NULL, NULL, NULL, NULL, ?)
            """,
            (int(account_user_id), _STATUS_DISABLED, now),
        )


def get_spambot_monitor_state(account_user_id: int) -> SpamBotMonitorState:
    _ensure_row(account_user_id)
    row = conn.execute(
        """
        SELECT account_user_id, is_enabled, COALESCE(auto_resume, 0), status,
               next_check_at, restriction_until, last_checked_at,
               last_response_text, last_error, updated_at
          FROM dm_spambot_monitor
         WHERE account_user_id=?
        """,
        (int(account_user_id),),
    ).fetchone()
    assert row is not None
    return SpamBotMonitorState(
        account_user_id=int(row[0]),
        is_enabled=bool(row[1]),
        auto_resume=bool(row[2]),
        status=str(row[3] or _STATUS_DISABLED),
        next_check_at=row[4],
        restriction_until=row[5],
        last_checked_at=row[6],
        last_response_text=_clean_text(row[7]),
        last_error=_clean_text(row[8]),
        updated_at=str(row[9] or ""),
    )


def _account_is_peer_flood_paused(account_user_id: int) -> bool:
    row = conn.execute(
        """
        SELECT is_paused, pause_reason
          FROM dm_account_dispatch
         WHERE account_user_id=?
        """,
        (int(account_user_id),),
    ).fetchone()
    if not row or not bool(row[0]):
        return False
    return "peerflood" in _normalize(row[1]).replace(" ", "")


def set_spambot_monitor_enabled(
    account_user_id: int,
    enabled: bool,
) -> SpamBotMonitorState:
    _ensure_row(account_user_id)
    now = utc_now()
    if enabled:
        due = _iso(now) if _account_is_peer_flood_paused(account_user_id) else None
        status = _STATUS_PENDING if due else _STATUS_IDLE
        with _db_lock, conn:
            conn.execute(
                """
                UPDATE dm_spambot_monitor
                   SET is_enabled=1, status=?, next_check_at=?,
                       restriction_until=NULL, last_error=NULL, updated_at=?
                 WHERE account_user_id=?
                """,
                (status, due, _iso(now), int(account_user_id)),
            )
    else:
        with _db_lock, conn:
            conn.execute(
                """
                UPDATE dm_spambot_monitor
                   SET is_enabled=0, status=?, next_check_at=NULL,
                       restriction_until=NULL, last_error=NULL, updated_at=?
                 WHERE account_user_id=?
                """,
                (_STATUS_DISABLED, _iso(now), int(account_user_id)),
            )
    return get_spambot_monitor_state(account_user_id)


def set_spambot_auto_resume(
    account_user_id: int,
    enabled: bool,
) -> SpamBotMonitorState:
    """Enable or disable automatic first-DM resume after a free @SpamBot reply.

    Default remains manual. Auto-resume only acts when the account is on a
    PeerFlood pause and monitoring is enabled.
    """
    _ensure_row(account_user_id)
    now = _iso(utc_now())
    with _db_lock, conn:
        conn.execute(
            """
            UPDATE dm_spambot_monitor
               SET auto_resume=?, updated_at=?
             WHERE account_user_id=?
            """,
            (1 if enabled else 0, now, int(account_user_id)),
        )
    return get_spambot_monitor_state(account_user_id)


def maybe_auto_resume_after_free(account_user_id: int) -> bool:
    """Resume PeerFlood-paused first DMs when auto_resume is enabled.

    Returns True only when the account pause was actually cleared.
    FloodWait and PeerFlood cooldowns are left intact.
    """
    state = get_spambot_monitor_state(account_user_id)
    if not state.is_enabled or not state.auto_resume:
        return False
    if state.status != _STATUS_FREE:
        return False
    if not _account_is_peer_flood_paused(account_user_id):
        return False
    # Local import keeps startup free of circular module initialization.
    from services.dm_task_queue import (
        get_account_dispatch_state,
        parse_iso,
        resume_account,
    )

    # Do not resume while PeerFlood cooldown is still active (SpamBot free
    # often arrives earlier than Telegram allows cold DMs again).
    dispatch = get_account_dispatch_state(int(account_user_id))
    cool = parse_iso(dispatch.cooldown_until)
    if cool is not None and cool > utc_now():
        remaining = int((cool - utc_now()).total_seconds())
        logger.info(
            f"[SpamBot monitor] free but PeerFlood cooldown active "
            f"account={int(account_user_id)} remaining={remaining}s — skip auto-resume"
        )
        return False

    resume_account(int(account_user_id))
    try:
        mark_spambot_manual_resume(int(account_user_id))
    except Exception as exc:
        logger.error(
            f"[SpamBot monitor] auto-resume monitor mark failed "
            f"account={int(account_user_id)}: {exc}"
        )
        try:
            mark_spambot_manual_resume(int(account_user_id))
        except Exception as exc2:
            logger.error(
                f"[SpamBot monitor] auto-resume monitor mark retry failed "
                f"account={int(account_user_id)}: {exc2}"
            )
            # Account is already resumed; still report success so dispatcher starts.
    logger.info(
        f"[SpamBot monitor] auto-resume applied account={int(account_user_id)}"
    )
    return True


def trigger_peer_flood_monitor(account_user_id: int) -> bool:
    """Queue one immediate official @SpamBot check when monitoring is enabled."""
    _ensure_row(account_user_id)
    state = get_spambot_monitor_state(account_user_id)
    if not state.is_enabled:
        return False
    now = _iso(utc_now())
    with _db_lock, conn:
        conn.execute(
            """
            UPDATE dm_spambot_monitor
               SET status=?, next_check_at=?, restriction_until=NULL,
                   last_error=NULL, updated_at=?
             WHERE account_user_id=? AND is_enabled=1
            """,
            (_STATUS_PENDING, now, now, int(account_user_id)),
        )
    return True


def mark_spambot_manual_resume(account_user_id: int) -> SpamBotMonitorState:
    """Keep monitoring enabled but finish the current restriction cycle."""
    _ensure_row(account_user_id)
    now = _iso(utc_now())
    with _db_lock, conn:
        conn.execute(
            """
            UPDATE dm_spambot_monitor
               SET status=CASE WHEN is_enabled=1 THEN ? ELSE ? END,
                   next_check_at=NULL, restriction_until=NULL,
                   last_error=NULL, updated_at=?
             WHERE account_user_id=?
            """,
            (_STATUS_IDLE, _STATUS_DISABLED, now, int(account_user_id)),
        )
    return get_spambot_monitor_state(account_user_id)


def list_due_spambot_accounts(limit: int = 20) -> list[int]:
    now = _iso(utc_now())
    rows = conn.execute(
        """
        SELECT account_user_id
          FROM dm_spambot_monitor
         WHERE is_enabled=1
           AND next_check_at IS NOT NULL
           AND next_check_at<=?
           AND status<>?
         ORDER BY next_check_at, account_user_id
         LIMIT ?
        """,
        (now, _STATUS_FREE, max(1, min(int(limit), 100))),
    ).fetchall()
    return [int(row[0]) for row in rows]


def _runtime_lock(account_user_id: int) -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    current = _runtime_locks.get(int(account_user_id))
    if current is None or current[0] is not loop:
        lock = asyncio.Lock()
        _runtime_locks[int(account_user_id)] = (loop, lock)
        return lock
    return current[1]


def _claim_due(account_user_id: int) -> bool:
    now = utc_now()
    lease_until = now + dt.timedelta(seconds=CHECK_LEASE_SECONDS)
    with _db_lock, conn:
        cursor = conn.execute(
            """
            UPDATE dm_spambot_monitor
               SET status=?, next_check_at=?, updated_at=?
             WHERE account_user_id=?
               AND is_enabled=1
               AND next_check_at IS NOT NULL
               AND next_check_at<=?
               AND status<>?
            """,
            (
                _STATUS_CHECKING,
                _iso(lease_until),
                _iso(now),
                int(account_user_id),
                _iso(now),
                _STATUS_FREE,
            ),
        )
    return int(cursor.rowcount or 0) == 1


def _save_retry(
    account_user_id: int,
    *,
    status: str,
    retry_seconds: int,
    error: Optional[str],
    response_text: Optional[str] = None,
) -> SpamBotCheckResult:
    now = utc_now()
    next_check = now + dt.timedelta(seconds=max(60, int(retry_seconds)))
    with _db_lock, conn:
        conn.execute(
            """
            UPDATE dm_spambot_monitor
               SET status=?, next_check_at=?, last_checked_at=?,
                   last_response_text=COALESCE(?, last_response_text),
                   last_error=?, updated_at=?
             WHERE account_user_id=? AND is_enabled=1
            """,
            (
                status,
                _iso(next_check),
                _iso(now),
                _clean_text(response_text),
                _clean_text(error, 1000),
                _iso(now),
                int(account_user_id),
            ),
        )
    return SpamBotCheckResult(
        account_user_id=int(account_user_id),
        outcome=status,
        response_text=_clean_text(response_text),
        next_check_at=_iso(next_check),
        error=_clean_text(error, 1000),
    )


def _save_restricted(
    account_user_id: int,
    response_text: str,
    restriction_until: dt.datetime,
) -> SpamBotCheckResult:
    now = utc_now()
    intended = restriction_until.astimezone(UTC) + dt.timedelta(
        seconds=RESTRICTION_GRACE_SECONDS
    )
    # If SpamBot repeats an already elapsed timestamp, avoid a tight /start loop.
    next_check = intended if intended > now else now + dt.timedelta(minutes=5)
    with _db_lock, conn:
        cursor = conn.execute(
            """
            UPDATE dm_spambot_monitor
               SET status=?, next_check_at=?, restriction_until=?,
                   last_checked_at=?, last_response_text=?, last_error=NULL,
                   updated_at=?
             WHERE account_user_id=? AND is_enabled=1
            """,
            (
                _STATUS_RESTRICTED,
                _iso(next_check),
                _iso(restriction_until),
                _iso(now),
                _clean_text(response_text),
                _iso(now),
                int(account_user_id),
            ),
        )
    if int(cursor.rowcount or 0) != 1:
        return SpamBotCheckResult(int(account_user_id), _STATUS_DISABLED)
    return SpamBotCheckResult(
        account_user_id=int(account_user_id),
        outcome=_STATUS_RESTRICTED,
        response_text=_clean_text(response_text),
        restriction_until=_iso(restriction_until),
        next_check_at=_iso(next_check),
    )


def _save_free(account_user_id: int, response_text: str) -> SpamBotCheckResult:
    now = _iso(utc_now())
    with _db_lock, conn:
        cursor = conn.execute(
            """
            UPDATE dm_spambot_monitor
               SET status=?, next_check_at=NULL, restriction_until=NULL,
                   last_checked_at=?, last_response_text=?, last_error=NULL,
                   updated_at=?
             WHERE account_user_id=? AND is_enabled=1
            """,
            (
                _STATUS_FREE,
                now,
                _clean_text(response_text),
                now,
                int(account_user_id),
            ),
        )
    if int(cursor.rowcount or 0) != 1:
        return SpamBotCheckResult(int(account_user_id), _STATUS_DISABLED)
    return SpamBotCheckResult(
        account_user_id=int(account_user_id),
        outcome=_STATUS_FREE,
        response_text=_clean_text(response_text),
    )


async def _request_spambot_status(client: Any) -> str:
    conversation_factory = getattr(client, "conversation", None)
    if not callable(conversation_factory):
        raise RuntimeError("Telegram-клиент не поддерживает conversation()")
    async with conversation_factory(
        SPAMBOT_USERNAME,
        timeout=RESPONSE_TIMEOUT_SECONDS,
        exclusive=False,
    ) as conversation:
        await conversation.send_message(SPAMBOT_COMMAND)
        response = await conversation.get_response()
    return str(getattr(response, "raw_text", None) or getattr(response, "text", None) or "")


async def check_spambot_account(
    account_user_id: int,
    client_resolver: Callable[[int], Any],
) -> SpamBotCheckResult:
    account_user_id = int(account_user_id)
    async with _runtime_lock(account_user_id):
        if not _claim_due(account_user_id):
            return SpamBotCheckResult(account_user_id, "not_due")

        client = client_resolver(account_user_id)
        if inspect.isawaitable(client):
            client = await client
        if client is None or not bool(getattr(client, "is_connected", lambda: False)()):
            return _save_retry(
                account_user_id,
                status=_STATUS_WAITING_CLIENT,
                retry_seconds=CLIENT_UNAVAILABLE_RETRY_SECONDS,
                error="Нет подключённого клиента активной DM-задачи",
            )

        try:
            response_text = await _request_spambot_status(client)
        except asyncio.TimeoutError:
            return _save_retry(
                account_user_id,
                status=_STATUS_ERROR,
                retry_seconds=NO_RESPONSE_RETRY_SECONDS,
                error="@SpamBot не ответил за отведённое время",
            )
        except asyncio.CancelledError:
            # The lease makes the row eligible again after a restart.
            raise
        except Exception as exc:
            wait_seconds = int(getattr(exc, "seconds", 0) or 0)
            retry_seconds = wait_seconds + 60 if wait_seconds > 0 else NO_RESPONSE_RETRY_SECONDS
            return _save_retry(
                account_user_id,
                status=_STATUS_ERROR,
                retry_seconds=retry_seconds,
                error=f"{type(exc).__name__}: {exc}",
            )

        if is_spambot_free_response(response_text):
            return _save_free(account_user_id, response_text)

        restriction_until = parse_spambot_restriction_until(response_text)
        if restriction_until is not None:
            return _save_restricted(account_user_id, response_text, restriction_until)

        return _save_retry(
            account_user_id,
            status=_STATUS_ERROR,
            retry_seconds=UNRECOGNIZED_RETRY_SECONDS,
            error="Ответ @SpamBot не распознан",
            response_text=response_text,
        )


async def process_due_spambot_checks(
    client_resolver: Callable[[int], Any],
    on_free: Optional[Callable[[SpamBotCheckResult], Awaitable[None]]] = None,
    *,
    limit: int = 20,
) -> list[SpamBotCheckResult]:
    """Process due checks sequentially to avoid noisy account bursts."""
    results: list[SpamBotCheckResult] = []
    for account_user_id in list_due_spambot_accounts(limit=limit):
        try:
            result = await check_spambot_account(account_user_id, client_resolver)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception(
                f"[SpamBot monitor] account={account_user_id} unexpected failure: {exc}"
            )
            result = _save_retry(
                account_user_id,
                status=_STATUS_ERROR,
                retry_seconds=NO_RESPONSE_RETRY_SECONDS,
                error=f"unexpected {type(exc).__name__}: {exc}",
            )
        if result.outcome == _STATUS_FREE:
            try:
                if maybe_auto_resume_after_free(account_user_id):
                    result = SpamBotCheckResult(
                        account_user_id=result.account_user_id,
                        outcome=result.outcome,
                        response_text=result.response_text,
                        restriction_until=result.restriction_until,
                        next_check_at=result.next_check_at,
                        error=result.error,
                        auto_resumed=True,
                    )
            except Exception as exc:
                logger.exception(
                    f"[SpamBot monitor] auto-resume failed account={account_user_id}: {exc}"
                )
        results.append(result)
        if result.outcome == _STATUS_FREE and on_free is not None:
            try:
                await on_free(result)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception(
                    f"[SpamBot monitor] free notification failed account={account_user_id}: {exc}"
                )
    return results


def spambot_status_label(state: SpamBotMonitorState) -> str:
    labels = {
        _STATUS_DISABLED: "выключен",
        _STATUS_IDLE: "ожидает PeerFlood",
        _STATUS_PENDING: "проверка поставлена в очередь",
        _STATUS_CHECKING: "проверяется",
        _STATUS_RESTRICTED: "ограничение активно",
        _STATUS_WAITING_CLIENT: "ожидает подключённую DM-задачу",
        _STATUS_ERROR: "ожидает повторной проверки",
        _STATUS_FREE: "ограничений нет — нужен ручной запуск DM",
    }
    return labels.get(state.status, state.status)


__all__ = [
    "SpamBotCheckResult",
    "SpamBotMonitorState",
    "check_spambot_account",
    "get_spambot_monitor_state",
    "is_spambot_free_response",
    "list_due_spambot_accounts",
    "mark_spambot_manual_resume",
    "maybe_auto_resume_after_free",
    "parse_spambot_restriction_until",
    "process_due_spambot_checks",
    "set_spambot_auto_resume",
    "set_spambot_monitor_enabled",
    "spambot_status_label",
    "trigger_peer_flood_monitor",
]
