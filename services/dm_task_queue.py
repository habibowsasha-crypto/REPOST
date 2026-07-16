from __future__ import annotations

import datetime as dt
import random
import secrets
import sqlite3
import threading
from dataclasses import dataclass
from typing import Any, Optional

from config import conn

UTC = dt.timezone.utc
MAX_DELAY_SECONDS = 30 * 24 * 60 * 60
MAX_COOLDOWN_SECONDS = 10 * 365 * 24 * 60 * 60
DEFAULT_PACING_MIN_SECONDS = 30
DEFAULT_PACING_MAX_SECONDS = 60
MIN_PACING_SECONDS = 5
STALE_CLAIM_SECONDS = 30 * 60

SENDABLE_STATUSES = ("pending", "retry_wait", "unresolved_peer")
NONTERMINAL_STATUSES = (
    "pending",
    "claimed",
    "sending",
    "retry_wait",
    "unresolved_peer",
    "uncertain_delivery",
)
TERMINAL_STATUSES = ("sent", "cancelled")

_db_lock = threading.RLock()


@dataclass(frozen=True)
class AccountDispatchState:
    account_user_id: int
    pacing_min: int
    pacing_max: int
    last_send_at: Optional[str]
    next_send_at: Optional[str]
    cooldown_until: Optional[str]
    is_paused: bool
    pause_reason: Optional[str]
    updated_at: str


def utc_now() -> dt.datetime:
    return dt.datetime.now(UTC)


def iso(value: dt.datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def parse_iso(value: Any) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = " ".join(str(value).replace("\n", " ").split()).strip()
    return text or None


def validate_delay_range(
    low: int,
    high: int,
    *,
    minimum: int = 0,
    maximum: int = MAX_DELAY_SECONDS,
) -> tuple[int, int]:
    low = int(low)
    high = int(high)
    if low < minimum or high < low or high > maximum:
        raise ValueError(
            f"delay range must satisfy {minimum} <= min <= max <= {maximum}"
        )
    return low, high


def random_delay_seconds(low: int, high: int) -> int:
    low, high = validate_delay_range(low, high)
    return random.randint(low, high)


def _active_status_sql() -> str:
    return ",".join("?" for _ in NONTERMINAL_STATUSES)


def ensure_account_settings(account_user_id: int) -> None:
    now = iso(utc_now())
    with _db_lock, conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO dm_account_dispatch (
                account_user_id, pacing_min, pacing_max, last_send_at,
                next_send_at, cooldown_until, is_paused, pause_reason, updated_at
            ) VALUES (?, ?, ?, NULL, NULL, NULL, 0, NULL, ?)
            """,
            (
                int(account_user_id),
                DEFAULT_PACING_MIN_SECONDS,
                DEFAULT_PACING_MAX_SECONDS,
                now,
            ),
        )


def get_account_dispatch_state(account_user_id: int) -> AccountDispatchState:
    ensure_account_settings(account_user_id)
    row = conn.execute(
        """
        SELECT account_user_id, pacing_min, pacing_max, last_send_at,
               next_send_at, cooldown_until, is_paused, pause_reason, updated_at
          FROM dm_account_dispatch
         WHERE account_user_id=?
        """,
        (int(account_user_id),),
    ).fetchone()
    assert row is not None
    return AccountDispatchState(
        account_user_id=int(row[0]),
        pacing_min=int(row[1] or DEFAULT_PACING_MIN_SECONDS),
        pacing_max=int(row[2] or DEFAULT_PACING_MAX_SECONDS),
        last_send_at=row[3],
        next_send_at=row[4],
        cooldown_until=row[5],
        is_paused=bool(row[6]),
        pause_reason=clean_text(row[7]),
        updated_at=str(row[8] or ""),
    )


def set_account_pacing(account_user_id: int, low: int, high: int) -> None:
    low, high = validate_delay_range(
        low,
        high,
        minimum=MIN_PACING_SECONDS,
        maximum=MAX_DELAY_SECONDS,
    )
    ensure_account_settings(account_user_id)
    now = utc_now()
    with _db_lock, conn:
        row = conn.execute(
            "SELECT last_send_at FROM dm_account_dispatch WHERE account_user_id=?",
            (int(account_user_id),),
        ).fetchone()
        last_send = parse_iso(row[0]) if row and row[0] else None
        next_send_at = None
        if last_send is not None:
            next_send_at = iso(
                last_send + dt.timedelta(seconds=random.randint(low, high))
            )
        conn.execute(
            """
            UPDATE dm_account_dispatch
               SET pacing_min=?, pacing_max=?, next_send_at=?, updated_at=?
             WHERE account_user_id=?
            """,
            (low, high, next_send_at, iso(now), int(account_user_id)),
        )


def mark_account_send_completed(account_user_id: int) -> int:
    state = get_account_dispatch_state(account_user_id)
    delay = random.randint(state.pacing_min, state.pacing_max)
    now = utc_now()
    next_send_at = now + dt.timedelta(seconds=delay)
    with _db_lock, conn:
        conn.execute(
            """
            UPDATE dm_account_dispatch
               SET last_send_at=?, next_send_at=?, updated_at=?
             WHERE account_user_id=?
            """,
            (iso(now), iso(next_send_at), iso(now), int(account_user_id)),
        )
    return delay


def set_account_cooldown(account_user_id: int, seconds: int, reason: str) -> str:
    # FloodWait is Telegram-owned and must not be truncated to the admin delay limit.
    seconds = max(1, min(int(seconds), MAX_COOLDOWN_SECONDS))
    candidate = utc_now() + dt.timedelta(seconds=seconds)
    ensure_account_settings(account_user_id)
    with _db_lock, conn:
        row = conn.execute(
            "SELECT cooldown_until FROM dm_account_dispatch WHERE account_user_id=?",
            (int(account_user_id),),
        ).fetchone()
        existing = parse_iso(row[0]) if row and row[0] else None
        until = max(candidate, existing) if existing is not None else candidate
        conn.execute(
            """
            UPDATE dm_account_dispatch
               SET cooldown_until=?, pause_reason=?, updated_at=?
             WHERE account_user_id=?
            """,
            (iso(until), clean_text(reason), iso(utc_now()), int(account_user_id)),
        )
    return iso(until)


def pause_account(account_user_id: int, reason: str) -> None:
    ensure_account_settings(account_user_id)
    with _db_lock, conn:
        conn.execute(
            """
            UPDATE dm_account_dispatch
               SET is_paused=1, pause_reason=?, updated_at=?
             WHERE account_user_id=?
            """,
            (clean_text(reason) or "manual", iso(utc_now()), int(account_user_id)),
        )


def resume_account(account_user_id: int) -> None:
    """Resume a manual/PeerFlood pause without bypassing Telegram FloodWait."""
    ensure_account_settings(account_user_id)
    with _db_lock, conn:
        conn.execute(
            """
            UPDATE dm_account_dispatch
               SET is_paused=0, pause_reason=NULL, updated_at=?
             WHERE account_user_id=?
            """,
            (iso(utc_now()), int(account_user_id)),
        )
        conn.execute(
            """
            UPDATE dm_pending_queue
               SET status='pending', eligible_at=CASE
                     WHEN eligible_at < ? THEN ? ELSE eligible_at END,
                   updated_at=?
             WHERE account_user_id=? AND status='retry_wait'
            """,
            (iso(utc_now()), iso(utc_now()), iso(utc_now()), int(account_user_id)),
        )


def account_gate_wait_seconds(account_user_id: int) -> Optional[float]:
    state = get_account_dispatch_state(account_user_id)
    if state.is_paused:
        return None
    now = utc_now()
    waits: list[float] = []
    for raw in (state.next_send_at, state.cooldown_until):
        parsed = parse_iso(raw)
        if parsed and parsed > now:
            waits.append((parsed - now).total_seconds())
    return max(waits) if waits else 0.0


def enqueue_pending(
    *,
    dm_task_id: int,
    account_user_id: int,
    target_user_id: int,
    target_access_hash: Optional[int],
    target_username: Optional[str],
    target_first_name: Optional[str],
    target_last_name: Optional[str],
    source_chat_id: Optional[int],
    source_chat_title: Optional[str],
    delay_min: int,
    delay_max: int,
) -> tuple[bool, int]:
    """Create one account-wide pending contact and record its source.

    Returns ``(created, pending_id)``. If another task of the same Telegram
    account already owns an active row for this user, that row is reused and the
    additional source is attached instead of creating a duplicate first DM.
    """
    delay_min, delay_max = validate_delay_range(delay_min, delay_max)
    now = utc_now()
    due = now + dt.timedelta(seconds=random.randint(delay_min, delay_max))
    now_iso = iso(now)
    due_iso = iso(due)
    account_user_id = int(account_user_id)
    target_user_id = int(target_user_id)

    with _db_lock, conn:
        existing = conn.execute(
            f"""
            SELECT id, dm_task_id FROM dm_pending_queue
             WHERE account_user_id=? AND target_user_id=?
               AND status IN ({_active_status_sql()})
             ORDER BY id DESC LIMIT 1
            """,
            (account_user_id, target_user_id, *NONTERMINAL_STATUSES),
        ).fetchone()
        created = False
        if existing:
            pending_id = int(existing[0])
            owner_task_id = int(existing[1])
            owner_active = conn.execute(
                "SELECT is_active FROM dm_tasks WHERE id=?",
                (owner_task_id,),
            ).fetchone()
            if not owner_active or not bool(owner_active[0]):
                conn.execute(
                    """
                    UPDATE dm_pending_queue
                       SET dm_task_id=?, source_chat_id=?, source_chat_title=?, updated_at=?
                     WHERE id=?
                    """,
                    (
                        int(dm_task_id),
                        int(source_chat_id) if source_chat_id is not None else None,
                        clean_text(source_chat_title),
                        now_iso,
                        pending_id,
                    ),
                )
            conn.execute(
                """
                UPDATE dm_pending_queue
                   SET target_access_hash=COALESCE(?, target_access_hash),
                       target_username=COALESCE(?, target_username),
                       target_first_name=COALESCE(?, target_first_name),
                       target_last_name=COALESCE(?, target_last_name),
                       updated_at=?
                 WHERE id=?
                """,
                (
                    int(target_access_hash) if target_access_hash is not None else None,
                    clean_text(target_username),
                    clean_text(target_first_name),
                    clean_text(target_last_name),
                    now_iso,
                    pending_id,
                ),
            )
        else:
            values = (
                int(dm_task_id),
                account_user_id,
                target_user_id,
                int(target_access_hash) if target_access_hash is not None else None,
                clean_text(target_username),
                clean_text(target_first_name),
                clean_text(target_last_name),
                int(source_chat_id) if source_chat_id is not None else None,
                clean_text(source_chat_title),
                now_iso,
                due_iso,
                now_iso,
            )
            try:
                cursor = conn.execute(
                    """
                    INSERT INTO dm_pending_queue (
                        dm_task_id, account_user_id, target_user_id,
                        target_access_hash, target_username, target_first_name,
                        target_last_name, source_chat_id, source_chat_title,
                        enqueued_at, eligible_at, status, retry_count,
                        resolve_attempts, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, 0, ?)
                    """,
                    values,
                )
                pending_id = int(cursor.lastrowid)
                created = True
            except sqlite3.IntegrityError:
                # Another process/task may have won the account+target unique race.
                raced = conn.execute(
                    f"""
                    SELECT id FROM dm_pending_queue
                     WHERE account_user_id=? AND target_user_id=?
                       AND status IN ({_active_status_sql()})
                     ORDER BY id DESC LIMIT 1
                    """,
                    (account_user_id, target_user_id, *NONTERMINAL_STATUSES),
                ).fetchone()
                if not raced:
                    raise
                pending_id = int(raced[0])

        if source_chat_id is not None:
            conn.execute(
                """
                INSERT INTO dm_pending_sources (
                    pending_id, dm_task_id, source_chat_id, source_chat_title,
                    first_seen_at, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(pending_id, dm_task_id, source_chat_id) DO UPDATE SET
                    source_chat_title=COALESCE(excluded.source_chat_title, dm_pending_sources.source_chat_title),
                    last_seen_at=excluded.last_seen_at
                """,
                (
                    pending_id,
                    int(dm_task_id),
                    int(source_chat_id),
                    clean_text(source_chat_title),
                    now_iso,
                    now_iso,
                ),
            )
    ensure_account_settings(account_user_id)
    return created, pending_id


def get_due_pending(account_user_id: int) -> Optional[dict[str, Any]]:
    now_iso = iso(utc_now())
    row = conn.execute(
        """
        SELECT q.id, q.dm_task_id, q.account_user_id, q.target_user_id,
               q.target_access_hash, q.target_username, q.target_first_name,
               q.target_last_name, q.source_chat_id, q.source_chat_title,
               q.enqueued_at, q.eligible_at, q.status, q.retry_count,
               q.resolve_attempts
          FROM dm_pending_queue AS q
          JOIN dm_tasks AS t ON t.id=q.dm_task_id
         WHERE q.account_user_id=?
           AND t.is_active=1
           AND q.status IN ('pending','retry_wait','unresolved_peer')
           AND q.eligible_at<=?
         ORDER BY q.eligible_at, q.id
         LIMIT 1
        """,
        (int(account_user_id), now_iso),
    ).fetchone()
    if not row:
        return None
    keys = (
        "id",
        "dm_task_id",
        "account_user_id",
        "target_user_id",
        "target_access_hash",
        "target_username",
        "target_first_name",
        "target_last_name",
        "source_chat_id",
        "source_chat_title",
        "enqueued_at",
        "eligible_at",
        "status",
        "retry_count",
        "resolve_attempts",
    )
    return dict(zip(keys, row))


def earliest_due_at(account_user_id: int) -> Optional[dt.datetime]:
    row = conn.execute(
        """
        SELECT MIN(q.eligible_at)
          FROM dm_pending_queue AS q
          JOIN dm_tasks AS t ON t.id=q.dm_task_id
         WHERE q.account_user_id=? AND t.is_active=1
           AND q.status IN ('pending','retry_wait','unresolved_peer')
        """,
        (int(account_user_id),),
    ).fetchone()
    return parse_iso(row[0]) if row and row[0] else None


def claim_pending(row_id: int) -> Optional[str]:
    token = secrets.token_urlsafe(18)
    now_iso = iso(utc_now())
    with _db_lock, conn:
        cursor = conn.execute(
            """
            UPDATE dm_pending_queue
               SET status='claimed', claim_token=?, claimed_at=?, updated_at=?
             WHERE id=?
               AND status IN ('pending','retry_wait','unresolved_peer')
               AND eligible_at<=?
            """,
            (token, now_iso, now_iso, int(row_id), now_iso),
        )
        return token if int(cursor.rowcount or 0) == 1 else None


def mark_sending(row_id: int, claim_token: str) -> bool:
    now_iso = iso(utc_now())
    with _db_lock, conn:
        cursor = conn.execute(
            """
            UPDATE dm_pending_queue
               SET status='sending', send_started_at=?, updated_at=?
             WHERE id=? AND status='claimed' AND claim_token=?
            """,
            (now_iso, now_iso, int(row_id), str(claim_token)),
        )
        return int(cursor.rowcount or 0) == 1


def finalize_sent(row_id: int, claim_token: str) -> bool:
    now_iso = iso(utc_now())
    with _db_lock, conn:
        cursor = conn.execute(
            """
            UPDATE dm_pending_queue
               SET status='sent', sent_at=?, updated_at=?, last_error=NULL
             WHERE id=? AND status='sending' AND claim_token=?
            """,
            (now_iso, now_iso, int(row_id), str(claim_token)),
        )
        return int(cursor.rowcount or 0) == 1


def cancel_row(
    row_id: int,
    reason: str,
    *,
    claim_token: Optional[str] = None,
) -> bool:
    """Cancel a row safely. Sending rows require the exact dispatcher claim."""
    with _db_lock, conn:
        if claim_token is None:
            cursor = conn.execute(
                """
                UPDATE dm_pending_queue
                   SET status='cancelled', last_error=?, updated_at=?
                 WHERE id=?
                   AND status IN ('pending','claimed','retry_wait','unresolved_peer')
                """,
                (clean_text(reason), iso(utc_now()), int(row_id)),
            )
        else:
            cursor = conn.execute(
                """
                UPDATE dm_pending_queue
                   SET status='cancelled', last_error=?, updated_at=?
                 WHERE id=? AND claim_token=?
                   AND status IN ('claimed','sending')
                """,
                (
                    clean_text(reason),
                    iso(utc_now()),
                    int(row_id),
                    str(claim_token),
                ),
            )
        return int(cursor.rowcount or 0) == 1


def schedule_retry(
    row_id: int,
    *,
    seconds: int,
    error: str,
    status: str = "retry_wait",
    claim_token: Optional[str] = None,
) -> bool:
    seconds = max(1, min(int(seconds), MAX_DELAY_SECONDS))
    if status not in ("retry_wait", "unresolved_peer"):
        raise ValueError("unsupported retry status")
    due = utc_now() + dt.timedelta(seconds=seconds)
    with _db_lock, conn:
        token_clause = " AND claim_token=?" if claim_token is not None else ""
        params: tuple[Any, ...] = (
            status,
            iso(due),
            status,
            clean_text(error),
            iso(utc_now()),
            int(row_id),
        )
        if claim_token is not None:
            params += (str(claim_token),)
        cursor = conn.execute(
            f"""
            UPDATE dm_pending_queue
               SET status=?, eligible_at=?, claim_token=NULL, claimed_at=NULL,
                   send_started_at=NULL, retry_count=retry_count+1,
                   resolve_attempts=resolve_attempts + CASE WHEN ?='unresolved_peer' THEN 1 ELSE 0 END,
                   last_error=?, updated_at=?
             WHERE id=? AND status IN ('claimed','sending','retry_wait','unresolved_peer')
                   {token_clause}
            """,
            params,
        )
        return int(cursor.rowcount or 0) == 1


def mark_uncertain(row_id: int, error: str) -> None:
    with _db_lock, conn:
        conn.execute(
            """
            UPDATE dm_pending_queue
               SET status='uncertain_delivery', last_error=?, updated_at=?
             WHERE id=? AND status IN ('claimed','sending')
            """,
            (clean_text(error), iso(utc_now()), int(row_id)),
        )


def recover_stale_queue() -> dict[str, int]:
    cutoff = iso(utc_now() - dt.timedelta(seconds=STALE_CLAIM_SECONDS))
    now_iso = iso(utc_now())
    with _db_lock, conn:
        claimed = conn.execute(
            """
            UPDATE dm_pending_queue
               SET status='pending', claim_token=NULL, claimed_at=NULL,
                   last_error='stale_claim_recovered', updated_at=?
             WHERE status='claimed' AND COALESCE(claimed_at, updated_at)<?
            """,
            (now_iso, cutoff),
        ).rowcount
        sending = conn.execute(
            """
            UPDATE dm_pending_queue
               SET status='uncertain_delivery',
                   last_error='process_restarted_during_send', updated_at=?
             WHERE status='sending' AND COALESCE(send_started_at, claimed_at, updated_at)<?
            """,
            (now_iso, cutoff),
        ).rowcount
    return {"claimed_recovered": int(claimed or 0), "sending_uncertain": int(sending or 0)}


def count_pending(dm_task_id: int, source_chat_id: Optional[int] = None) -> int:
    if source_chat_id is None:
        row = conn.execute(
            f"""
            SELECT COUNT(*) FROM dm_pending_queue
             WHERE dm_task_id=? AND status IN ({_active_status_sql()})
            """,
            (int(dm_task_id), *NONTERMINAL_STATUSES),
        ).fetchone()
    else:
        row = conn.execute(
            f"""
            SELECT COUNT(DISTINCT q.id)
              FROM dm_pending_queue AS q
              JOIN dm_pending_sources AS s ON s.pending_id=q.id
             WHERE s.dm_task_id=? AND s.source_chat_id=?
               AND q.status IN ({_active_status_sql()})
            """,
            (int(dm_task_id), int(source_chat_id), *NONTERMINAL_STATUSES),
        ).fetchone()
    return int(row[0] or 0) if row else 0


def count_clearable_pending(dm_task_id: int) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) FROM dm_pending_queue
         WHERE dm_task_id=?
           AND status IN ('pending','retry_wait','unresolved_peer','claimed')
        """,
        (int(dm_task_id),),
    ).fetchone()
    return int(row[0] or 0) if row else 0


def count_account_pending(account_user_id: int) -> int:
    row = conn.execute(
        f"""
        SELECT COUNT(*) FROM dm_pending_queue
         WHERE account_user_id=? AND status IN ({_active_status_sql()})
        """,
        (int(account_user_id), *NONTERMINAL_STATUSES),
    ).fetchone()
    return int(row[0] or 0) if row else 0


def list_pending_page(dm_task_id: int, *, offset: int, limit: int) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 50))
    safe_offset = max(0, int(offset))
    rows = conn.execute(
        f"""
        SELECT id, dm_task_id, account_user_id, target_user_id,
               target_username, target_first_name, target_last_name,
               source_chat_id, source_chat_title, enqueued_at, eligible_at,
               status, retry_count, resolve_attempts, last_error
          FROM dm_pending_queue
         WHERE dm_task_id=? AND status IN ({_active_status_sql()})
         ORDER BY eligible_at, id
         LIMIT ? OFFSET ?
        """,
        (int(dm_task_id), *NONTERMINAL_STATUSES, safe_limit, safe_offset),
    ).fetchall()
    keys = (
        "id",
        "dm_task_id",
        "account_user_id",
        "target_user_id",
        "target_username",
        "target_first_name",
        "target_last_name",
        "source_chat_id",
        "source_chat_title",
        "enqueued_at",
        "eligible_at",
        "status",
        "retry_count",
        "resolve_attempts",
        "last_error",
    )
    return [dict(zip(keys, row)) for row in rows]


def clear_task_pending(dm_task_id: int, reason: str = "admin_clear") -> int:
    with _db_lock, conn:
        cursor = conn.execute(
            """
            UPDATE dm_pending_queue
               SET status='cancelled', last_error=?, updated_at=?
             WHERE dm_task_id=?
               AND status IN ('pending','retry_wait','unresolved_peer','claimed')
            """,
            (clean_text(reason), iso(utc_now()), int(dm_task_id)),
        )
        return int(cursor.rowcount or 0)


def cancel_account_target(account_user_id: int, target_user_id: int, reason: str) -> int:
    with _db_lock, conn:
        cursor = conn.execute(
            """
            UPDATE dm_pending_queue
               SET status='cancelled', last_error=?, updated_at=?
             WHERE account_user_id=? AND target_user_id=?
               AND status IN ('pending','retry_wait','unresolved_peer','claimed')
            """,
            (
                clean_text(reason),
                iso(utc_now()),
                int(account_user_id),
                int(target_user_id),
            ),
        )
        return int(cursor.rowcount or 0)


def cancel_target_globally(target_user_id: int, reason: str) -> int:
    with _db_lock, conn:
        cursor = conn.execute(
            """
            UPDATE dm_pending_queue
               SET status='cancelled', last_error=?, updated_at=?
             WHERE target_user_id=?
               AND status IN ('pending','retry_wait','unresolved_peer','claimed')
            """,
            (clean_text(reason), iso(utc_now()), int(target_user_id)),
        )
        return int(cursor.rowcount or 0)


def reschedule_task_pending(dm_task_id: int, delay_min: int, delay_max: int) -> int:
    delay_min, delay_max = validate_delay_range(delay_min, delay_max)
    rows = conn.execute(
        """
        SELECT id FROM dm_pending_queue
         WHERE dm_task_id=? AND status IN ('pending','retry_wait','unresolved_peer')
         ORDER BY id
        """,
        (int(dm_task_id),),
    ).fetchall()
    now = utc_now()
    calculated = [
        (iso(now + dt.timedelta(seconds=random.randint(delay_min, delay_max))), int(row[0]))
        for row in rows
    ]
    with _db_lock, conn:
        conn.executemany(
            """
            UPDATE dm_pending_queue SET eligible_at=?, updated_at=? WHERE id=?
            """,
            [(due, iso(utc_now()), row_id) for due, row_id in calculated],
        )
    return len(calculated)


def remove_chat_source(dm_task_id: int, source_chat_id: int, *, cancel_orphans: bool) -> int:
    rows = conn.execute(
        f"""
        SELECT DISTINCT q.id
          FROM dm_pending_queue AS q
          JOIN dm_pending_sources AS s ON s.pending_id=q.id
         WHERE s.dm_task_id=? AND s.source_chat_id=?
           AND q.status IN ({_active_status_sql()})
        """,
        (int(dm_task_id), int(source_chat_id), *NONTERMINAL_STATUSES),
    ).fetchall()
    pending_ids = [int(row[0]) for row in rows]
    if not pending_ids:
        return 0
    with _db_lock, conn:
        conn.execute(
            """
            DELETE FROM dm_pending_sources
             WHERE dm_task_id=? AND source_chat_id=?
            """,
            (int(dm_task_id), int(source_chat_id)),
        )
        cancelled = 0
        for pending_id in pending_ids:
            queue_row = conn.execute(
                "SELECT dm_task_id, source_chat_id, status FROM dm_pending_queue WHERE id=?",
                (pending_id,),
            ).fetchone()
            if not queue_row:
                continue
            owner_task_id, primary_chat_id, status = queue_row
            owner_removed = (
                int(owner_task_id) == int(dm_task_id)
                and primary_chat_id is not None
                and int(primary_chat_id) == int(source_chat_id)
            )
            if owner_removed:
                candidate = _source_candidate(pending_id)
                if candidate is not None:
                    _assign_pending_to_source(pending_id, candidate)
                    continue
            has_sources = conn.execute(
                "SELECT 1 FROM dm_pending_sources WHERE pending_id=? LIMIT 1",
                (pending_id,),
            ).fetchone()
            if cancel_orphans and not has_sources and str(status) in SENDABLE_STATUSES + ("claimed",):
                cancelled += int(
                    conn.execute(
                        """
                        UPDATE dm_pending_queue
                           SET status='cancelled', claim_token=NULL, claimed_at=NULL,
                               last_error='source_chat_removed', updated_at=?
                         WHERE id=?
                        """,
                        (iso(utc_now()), pending_id),
                    ).rowcount
                    or 0
                )
        return cancelled


def _source_candidate(
    pending_id: int,
    *,
    excluded_task_ids: tuple[int, ...] = (),
    require_active: bool = False,
) -> Optional[tuple[int, int, Optional[str]]]:
    clauses = ["s.pending_id=?"]
    params: list[Any] = [int(pending_id)]
    if excluded_task_ids:
        placeholders = ",".join("?" for _ in excluded_task_ids)
        clauses.append(f"s.dm_task_id NOT IN ({placeholders})")
        params.extend(int(value) for value in excluded_task_ids)
    if require_active:
        clauses.append("t.is_active=1")
    row = conn.execute(
        f"""
        SELECT s.dm_task_id, s.source_chat_id, s.source_chat_title
          FROM dm_pending_sources AS s
          JOIN dm_tasks AS t ON t.id=s.dm_task_id
         WHERE {' AND '.join(clauses)}
         ORDER BY t.is_active DESC, s.first_seen_at, s.dm_task_id, s.source_chat_id
         LIMIT 1
        """,
        tuple(params),
    ).fetchone()
    if not row:
        return None
    return int(row[0]), int(row[1]), clean_text(row[2])


def _assign_pending_to_source(
    pending_id: int, source: tuple[int, int, Optional[str]]
) -> None:
    task_id, source_chat_id, source_chat_title = source
    conn.execute(
        """
        UPDATE dm_pending_queue
           SET dm_task_id=?, source_chat_id=?, source_chat_title=?, updated_at=?
         WHERE id=?
        """,
        (task_id, source_chat_id, source_chat_title, iso(utc_now()), int(pending_id)),
    )


def reassign_task_pending_to_active_sources(dm_task_id: int) -> int:
    """Move account-wide pending rows to another active source task when possible."""
    rows = conn.execute(
        """
        SELECT id FROM dm_pending_queue
         WHERE dm_task_id=? AND status IN ('pending','retry_wait','unresolved_peer')
         ORDER BY id
        """,
        (int(dm_task_id),),
    ).fetchall()
    moved = 0
    with _db_lock, conn:
        for (pending_id,) in rows:
            candidate = _source_candidate(
                int(pending_id),
                excluded_task_ids=(int(dm_task_id),),
                require_active=True,
            )
            if candidate is not None:
                _assign_pending_to_source(int(pending_id), candidate)
                moved += 1
    return moved


def prepare_tasks_for_deletion(task_ids: list[int] | tuple[int, ...]) -> dict[str, int]:
    """Detach task sources without removing uncertain-delivery duplicate guards."""
    unique_ids = tuple(sorted({int(value) for value in task_ids}))
    if not unique_ids:
        return {"reassigned": 0, "cancelled": 0, "uncertain": 0}
    placeholders = ",".join("?" for _ in unique_ids)
    rows = conn.execute(
        f"""
        SELECT id, status FROM dm_pending_queue
         WHERE dm_task_id IN ({placeholders})
           AND status IN (
               'pending','claimed','sending','retry_wait',
               'unresolved_peer','uncertain_delivery'
           )
         ORDER BY id
        """,
        unique_ids,
    ).fetchall()
    result = {"reassigned": 0, "cancelled": 0, "uncertain": 0}
    with _db_lock, conn:
        conn.execute(
            f"DELETE FROM dm_pending_sources WHERE dm_task_id IN ({placeholders})",
            unique_ids,
        )
        for pending_id, status in rows:
            pending_id = int(pending_id)
            status = str(status)
            candidate = _source_candidate(
                pending_id, excluded_task_ids=unique_ids, require_active=True
            )
            if candidate is not None and status in SENDABLE_STATUSES + ("claimed",):
                _assign_pending_to_source(pending_id, candidate)
                if status == "claimed":
                    conn.execute(
                        """
                        UPDATE dm_pending_queue
                           SET status='pending', claim_token=NULL, claimed_at=NULL,
                               last_error='owner_task_deleted_reassigned', updated_at=?
                         WHERE id=?
                        """,
                        (iso(utc_now()), pending_id),
                    )
                result["reassigned"] += 1
                continue
            if status == "sending":
                conn.execute(
                    """
                    UPDATE dm_pending_queue
                       SET status='uncertain_delivery',
                           last_error='owner_task_deleted_during_send', updated_at=?
                     WHERE id=?
                    """,
                    (iso(utc_now()), pending_id),
                )
                result["uncertain"] += 1
            elif status in SENDABLE_STATUSES + ("claimed",):
                conn.execute(
                    """
                    UPDATE dm_pending_queue
                       SET status='cancelled', claim_token=NULL, claimed_at=NULL,
                           last_error='owner_task_deleted', updated_at=?
                     WHERE id=?
                    """,
                    (iso(utc_now()), pending_id),
                )
                result["cancelled"] += 1
            # uncertain_delivery intentionally remains as a permanent duplicate guard.
    return result


def format_pending_target(row: dict[str, Any]) -> str:
    username = clean_text(row.get("target_username"))
    user_id = int(row["target_user_id"])
    if username:
        return f"@{username.lstrip('@')} | {user_id}"
    name = " ".join(
        part
        for part in (
            clean_text(row.get("target_first_name")),
            clean_text(row.get("target_last_name")),
        )
        if part
    ).strip()
    return f"{name or 'Пользователь'} | {user_id}"


def queue_status_counts(dm_task_id: int) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT status, COUNT(*) FROM dm_pending_queue
         WHERE dm_task_id=? GROUP BY status
        """,
        (int(dm_task_id),),
    ).fetchall()
    return {str(status): int(count) for status, count in rows}
