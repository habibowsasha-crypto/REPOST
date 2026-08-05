"""Persistent anti-repeat windows for outbound funnel phrases.

The history is updated when an outbound message is durably prepared, not only after
Telegram returns success. This closes the crash window where a message could be sent
but never enter the uniqueness history.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import weakref
from typing import List

from db.schema import db_lock, get_connection

KIND_FIRST_DM = "first_dm"
KIND_EXPLAIN = "explain"
KIND_LINK = "link_wrap"
KIND_PROMO = "promo"
KIND_APOLOGY = "apology"
KIND_LINK_HELP = "link_help"

ANTI_REPEAT_KINDS = {
    KIND_FIRST_DM,
    KIND_PROMO,
    KIND_APOLOGY,
    KIND_LINK_HELP,
}
ANTI_REPEAT_WINDOW = 20

# Locks are scoped to the currently running event loop so repeated asyncio.run calls in
# tests and administrative utilities never reuse a lock bound to a closed loop.
_loop_locks: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[str, asyncio.Lock]]" = (
    weakref.WeakKeyDictionary()
)


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def generation_lock(kind: str) -> asyncio.Lock:
    """Serialize generation plus durable prepare for one phrase kind in this process."""
    loop = asyncio.get_running_loop()
    locks = _loop_locks.setdefault(loop, {})
    key = str(kind)
    lock = locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        locks[key] = lock
    return lock


def _prepared_rows(kind: str, conn) -> list[tuple[str, str]]:
    """Return prepared texts that must also block duplicate generation."""
    rows: list[tuple[str, str]] = []
    if kind == KIND_FIRST_DM:
        found = conn.execute(
            """
            SELECT text, prepared_at AS created_at
              FROM first_dm_outbox
             WHERE status='prepared'
            """
        ).fetchall()
        rows.extend((str(row["text"] or ""), str(row["created_at"] or "")) for row in found)
    elif kind in {KIND_PROMO, KIND_APOLOGY, KIND_LINK_HELP}:
        message_kind = {
            KIND_PROMO: "promo",
            KIND_APOLOGY: "smooth_apology",
            KIND_LINK_HELP: "link_help",
        }[kind]
        found = conn.execute(
            """
            SELECT text, prepared_at AS created_at
              FROM dialog_outbox
             WHERE status='prepared' AND message_kind=?
            """,
            (message_kind,),
        ).fetchall()
        rows.extend((str(row["text"] or ""), str(row["created_at"] or "")) for row in found)
    return rows


def recent_texts(kind: str, limit: int = ANTI_REPEAT_WINDOW) -> List[str]:
    """Return recent prepared or sent texts, newest first, without duplicates."""
    key = str(kind)
    requested = max(1, int(limit))
    conn = get_connection()
    with db_lock():
        rows = conn.execute(
            """
            SELECT text, created_at FROM sent_phrases
             WHERE kind=?
             ORDER BY id DESC
             LIMIT ?
            """,
            (key, max(requested * 3, ANTI_REPEAT_WINDOW)),
        ).fetchall()
        merged = [
            (str(row["text"] or ""), str(row["created_at"] or ""))
            for row in rows
        ]
        merged.extend(_prepared_rows(key, conn))
    merged.sort(key=lambda item: item[1], reverse=True)

    result: list[str] = []
    seen: set[str] = set()
    for text, _created_at in merged:
        value = text.strip()
        if not value:
            continue
        fingerprint = " ".join(value.casefold().split())
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        result.append(value)
        if len(result) >= requested:
            break
    return result


def _remember_with_conn(
    conn,
    kind: str,
    text: str,
    *,
    delivery_key: str | None,
    created_at: str,
) -> None:
    value = (text or "").strip()
    if not value:
        return
    key = str(kind)
    delivery = str(delivery_key).strip() if delivery_key else None
    if delivery:
        existing = conn.execute(
            """
            SELECT id FROM sent_phrases
             WHERE kind=? AND delivery_key=?
            """,
            (key, delivery),
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE sent_phrases
                   SET text=?, created_at=?
                 WHERE id=?
                """,
                (value, created_at, int(existing["id"])),
            )
        else:
            conn.execute(
                """
                INSERT INTO sent_phrases (kind, text, created_at, delivery_key)
                VALUES (?, ?, ?, ?)
                """,
                (key, value, created_at, delivery),
            )
    else:
        conn.execute(
            """
            INSERT INTO sent_phrases (kind, text, created_at, delivery_key)
            VALUES (?, ?, ?, NULL)
            """,
            (key, value, created_at),
        )

    keep_limit = ANTI_REPEAT_WINDOW if key in ANTI_REPEAT_KINDS else 200
    conn.execute(
        """
        DELETE FROM sent_phrases
         WHERE kind=?
           AND id NOT IN (
               SELECT id FROM sent_phrases
                WHERE kind=?
                ORDER BY id DESC
                LIMIT ?
           )
        """,
        (key, key, keep_limit),
    )


def remember(
    kind: str,
    text: str,
    *,
    delivery_key: str | None = None,
    conn=None,
    created_at: str | None = None,
) -> None:
    """Store or refresh one phrase journal entry.

    ``delivery_key`` makes prepare and recovery idempotent. Passing an existing SQLite
    connection keeps the phrase journal in the same transaction as the outbox state.
    """
    timestamp = created_at or _now_iso()
    if conn is not None:
        _remember_with_conn(
            conn,
            str(kind),
            text,
            delivery_key=delivery_key,
            created_at=timestamp,
        )
        return

    database = get_connection()
    with db_lock(), database:
        _remember_with_conn(
            database,
            str(kind),
            text,
            delivery_key=delivery_key,
            created_at=timestamp,
        )
