"""Pacing helpers: global spacing, per-account interval, daily limits."""

from __future__ import annotations

import datetime as dt
import random
from typing import Any, Optional

from config import (
    DM_ACCOUNT_INTERVAL_MAX,
    DM_ACCOUNT_INTERVAL_MIN,
    DM_DAILY_LIMIT_PER_ACCOUNT,
    DM_GLOBAL_SPACING_MAX,
    DM_GLOBAL_SPACING_MIN,
    FLOODWAIT_EXTRA_SECONDS,
)
from db.schema import db_lock, get_connection

_GLOBAL_NEXT_SEND_AT: Optional[dt.datetime] = None


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _parse_iso(value: str | None) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        raw = value.replace("Z", "+00:00")
        parsed = dt.datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed
    except Exception:
        return None


def random_account_interval_seconds() -> int:
    lo = min(DM_ACCOUNT_INTERVAL_MIN, DM_ACCOUNT_INTERVAL_MAX)
    hi = max(DM_ACCOUNT_INTERVAL_MIN, DM_ACCOUNT_INTERVAL_MAX)
    return random.randint(lo, hi)


def random_global_spacing_seconds() -> int:
    lo = min(DM_GLOBAL_SPACING_MIN, DM_GLOBAL_SPACING_MAX)
    hi = max(DM_GLOBAL_SPACING_MIN, DM_GLOBAL_SPACING_MAX)
    return random.randint(lo, hi)


def global_ready() -> bool:
    global _GLOBAL_NEXT_SEND_AT
    if _GLOBAL_NEXT_SEND_AT is None:
        return True
    return _now() >= _GLOBAL_NEXT_SEND_AT


def mark_global_sent() -> None:
    global _GLOBAL_NEXT_SEND_AT
    _GLOBAL_NEXT_SEND_AT = _now() + dt.timedelta(seconds=random_global_spacing_seconds())


def seconds_until_global_ready() -> float:
    if _GLOBAL_NEXT_SEND_AT is None:
        return 0.0
    delta = (_GLOBAL_NEXT_SEND_AT - _now()).total_seconds()
    return max(0.0, delta)


def _today_str() -> str:
    return _now().date().isoformat()


def account_is_send_ready(acc: dict[str, Any]) -> tuple[bool, str]:
    if not acc:
        return False, "no_account"
    if not acc.get("participates"):
        return False, "not_participating"
    if acc.get("is_paused"):
        return False, "paused"

    cooldown = _parse_iso(acc.get("cooldown_until"))
    if cooldown and cooldown > _now():
        return False, "cooldown"

    # Random 10-15 min window stored as next_send_at after each success.
    next_send = _parse_iso(acc.get("next_send_at"))
    if next_send and next_send > _now():
        return False, "account_interval"

    # Fallback if next_send_at missing but last_send_at recent (old rows).
    last_send = _parse_iso(acc.get("last_send_at"))
    if last_send and not next_send:
        elapsed = (_now() - last_send).total_seconds()
        if elapsed < DM_ACCOUNT_INTERVAL_MIN:
            return False, "account_interval"

    today = _today_str()
    sent_date = (acc.get("daily_sent_date") or "")[:10]
    count = int(acc.get("daily_sent_count") or 0)
    if sent_date == today and count >= DM_DAILY_LIMIT_PER_ACCOUNT:
        return False, "daily_limit"

    return True, "ok"


def record_successful_send(account_user_id: int) -> None:
    now = _now()
    today = now.date().isoformat()
    interval = random_account_interval_seconds()
    next_send = (now + dt.timedelta(seconds=interval)).isoformat()
    conn = get_connection()
    with db_lock(), conn:
        row = conn.execute(
            "SELECT daily_sent_count, daily_sent_date FROM accounts WHERE user_id=?",
            (int(account_user_id),),
        ).fetchone()
        if not row:
            return
        prev_date = (row["daily_sent_date"] or "")[:10]
        count = int(row["daily_sent_count"] or 0)
        if prev_date != today:
            count = 0
        count += 1
        conn.execute(
            """
            UPDATE accounts
               SET last_send_at=?,
                   next_send_at=?,
                   daily_sent_count=?,
                   daily_sent_date=?,
                   updated_at=?
             WHERE user_id=?
            """,
            (
                now.isoformat(),
                next_send,
                count,
                today,
                now.isoformat(),
                int(account_user_id),
            ),
        )


def apply_floodwait(account_user_id: int, seconds: int) -> None:
    wait = max(0, int(seconds)) + int(FLOODWAIT_EXTRA_SECONDS)
    until = (_now() + dt.timedelta(seconds=wait)).isoformat()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE accounts
               SET cooldown_until=?,
                   pause_reason=?,
                   updated_at=?
             WHERE user_id=?
            """,
            (until, f"FloodWait {wait}s", _now().isoformat(), int(account_user_id)),
        )


def set_paused(account_user_id: int, reason: str, *, paused: bool = True) -> None:
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE accounts
               SET is_paused=?,
                   pause_reason=?,
                   updated_at=?
             WHERE user_id=?
            """,
            (
                1 if paused else 0,
                reason if paused else None,
                _now().isoformat(),
                int(account_user_id),
            ),
        )
