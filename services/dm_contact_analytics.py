from __future__ import annotations

import datetime
import sqlite3
import threading
import uuid
from collections.abc import Callable
from typing import Any, Optional

from decouple import config
from loguru import logger

from config import conn


CompletedQueuePurger = Callable[[int], int]
_completed_queue_purgers: list[CompletedQueuePurger] = []
_table_ready = False
_db_lock = threading.RLock()
_FIRST_DM_CLAIM_TTL_MINUTES = 30

_OPEN_STATUSES = ("first_dm_sent", "active", "post_link_active")
_BEFORE_LINK_STATUSES = ("first_dm_sent", "active")
_TERMINAL_STATUSES = ("completed", "opted_out", "abandoned")


def _utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _now_iso() -> str:
    return _utc_now().isoformat()


def _hours(name: str, default: int) -> int:
    try:
        return max(1, min(720, int(config(name, default=str(default)))))
    except Exception:
        return default


def dialog_timeout_settings() -> dict[str, int]:
    return {
        "before_link_hours": _hours("DM_DIALOG_ABANDON_HOURS", 72),
        "post_link_hours": _hours("DM_DIALOG_POST_LINK_COMPLETE_HOURS", 72),
    }


def register_completed_queue_purger(callback: CompletedQueuePurger) -> None:
    """Register a global live-queue cleanup callback once."""
    if callback not in _completed_queue_purgers:
        _completed_queue_purgers.append(callback)


def _purge_completed_contact_from_queues(target_user_id: int) -> int:
    """Remove a globally completed user from every live/persistent first-DM queue."""
    removed = 0
    for callback in tuple(_completed_queue_purgers):
        try:
            removed += max(0, int(callback(int(target_user_id)) or 0))
        except Exception as exc:
            logger.error(
                "[DM analytics] global completed-contact queue purge failed: "
                f"user={target_user_id}, error={exc}"
            )
    return removed


def _create_global_completed_contacts_table(cur: sqlite3.Cursor, table_name: str) -> None:
    cur.execute(
        f"""
        CREATE TABLE {table_name} (
            target_user_id INTEGER PRIMARY KEY,
            account_user_id INTEGER NOT NULL,
            source_chat_id INTEGER,
            source_chat_title TEXT,
            cycle_id INTEGER,
            completed_at TEXT NOT NULL,
            completion_reason TEXT
        )
        """
    )


def _migrate_completed_contacts_to_global(cur: sqlite3.Cursor) -> None:
    """Migrate the old account-scoped registry to one global row per user.

    For users completed by several accounts in older versions, the earliest
    completion becomes the global origin record. Historical cycles stay intact.
    """
    table = cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='dm_completed_contacts'"
    ).fetchone()
    if not table:
        _create_global_completed_contacts_table(cur, "dm_completed_contacts")
        return

    columns = cur.execute("PRAGMA table_info(dm_completed_contacts)").fetchall()
    pk_by_name = {str(row[1]): int(row[5] or 0) for row in columns}
    if pk_by_name.get("target_user_id") == 1 and pk_by_name.get("account_user_id", 0) == 0:
        return

    cur.execute("DROP TABLE IF EXISTS dm_completed_contacts_global_new")
    _create_global_completed_contacts_table(cur, "dm_completed_contacts_global_new")
    cur.execute(
        """
        INSERT OR IGNORE INTO dm_completed_contacts_global_new
            (target_user_id, account_user_id, source_chat_id, source_chat_title,
             cycle_id, completed_at, completion_reason)
        SELECT target_user_id, account_user_id, source_chat_id, source_chat_title,
               cycle_id, completed_at, completion_reason
          FROM dm_completed_contacts
         ORDER BY completed_at ASC, rowid ASC
        """
    )
    cur.execute("DROP INDEX IF EXISTS idx_dm_completed_chat")
    cur.execute("DROP TABLE dm_completed_contacts")
    cur.execute(
        "ALTER TABLE dm_completed_contacts_global_new RENAME TO dm_completed_contacts"
    )


def _create_global_first_dm_claims_table(cur: sqlite3.Cursor, table_name: str) -> None:
    cur.execute(
        f"""
        CREATE TABLE {table_name} (
            target_user_id INTEGER PRIMARY KEY,
            account_user_id INTEGER NOT NULL,
            claim_token TEXT NOT NULL UNIQUE,
            dm_task_id INTEGER,
            source_chat_id INTEGER,
            claimed_at TEXT NOT NULL
        )
        """
    )


def _migrate_first_dm_claims_to_global(cur: sqlite3.Cursor) -> None:
    """Allow only one in-flight first-DM claim for a user across all accounts."""
    table = cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='dm_first_dm_claims'"
    ).fetchone()
    if not table:
        _create_global_first_dm_claims_table(cur, "dm_first_dm_claims")
        return

    columns = cur.execute("PRAGMA table_info(dm_first_dm_claims)").fetchall()
    pk_by_name = {str(row[1]): int(row[5] or 0) for row in columns}
    if pk_by_name.get("target_user_id") == 1 and pk_by_name.get("account_user_id", 0) == 0:
        return

    cur.execute("DROP TABLE IF EXISTS dm_first_dm_claims_global_new")
    _create_global_first_dm_claims_table(cur, "dm_first_dm_claims_global_new")
    cur.execute(
        """
        INSERT OR IGNORE INTO dm_first_dm_claims_global_new
            (target_user_id, account_user_id, claim_token, dm_task_id,
             source_chat_id, claimed_at)
        SELECT target_user_id, account_user_id, claim_token, dm_task_id,
               source_chat_id, claimed_at
          FROM dm_first_dm_claims
         ORDER BY claimed_at ASC, rowid ASC
        """
    )
    cur.execute("DROP INDEX IF EXISTS idx_dm_first_claims_at")
    cur.execute("DROP TABLE dm_first_dm_claims")
    cur.execute(
        "ALTER TABLE dm_first_dm_claims_global_new RENAME TO dm_first_dm_claims"
    )


def create_contact_tables() -> None:
    global _table_ready
    with _db_lock:
        if _table_ready:
            return

        cur = conn.cursor()
        try:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS dm_contact_cycles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dm_task_id INTEGER NOT NULL,
                    account_user_id INTEGER NOT NULL,
                    target_user_id INTEGER NOT NULL,
                    source_chat_id INTEGER,
                    source_chat_title TEXT,
                    status TEXT NOT NULL DEFAULT 'first_dm_sent',
                    completion_reason TEXT,
                    first_dm_at TEXT NOT NULL,
                    first_reply_at TEXT,
                    link_sent_at TEXT,
                    completed_at TEXT,
                    dialog_completed_at TEXT,
                    last_activity_at TEXT NOT NULL
                )
                """
            )
            _migrate_completed_contacts_to_global(cur)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS dm_contact_sources (
                    account_user_id INTEGER NOT NULL,
                    target_user_id INTEGER NOT NULL,
                    source_chat_id INTEGER NOT NULL,
                    source_chat_title TEXT,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    PRIMARY KEY (account_user_id, target_user_id, source_chat_id)
                )
                """
            )
            _migrate_first_dm_claims_to_global(cur)
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_dm_cycles_chat "
                "ON dm_contact_cycles(source_chat_id, status)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_dm_cycles_account_target "
                "ON dm_contact_cycles(account_user_id, target_user_id, id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_dm_cycles_status_activity "
                "ON dm_contact_cycles(status, last_activity_at)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_dm_completed_chat "
                "ON dm_completed_contacts(source_chat_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_dm_sources_chat "
                "ON dm_contact_sources(source_chat_id, last_seen_at)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_dm_first_claims_at "
                "ON dm_first_dm_claims(claimed_at)"
            )

            columns = {row[1] for row in cur.execute("PRAGMA table_info(dm_contact_cycles)")}
            if "dialog_completed_at" not in columns:
                cur.execute(
                    "ALTER TABLE dm_contact_cycles ADD COLUMN dialog_completed_at TEXT"
                )
            cur.execute(
                """
                UPDATE dm_contact_cycles
                   SET dialog_completed_at = completed_at
                 WHERE status = 'completed'
                   AND completed_at IS NOT NULL
                   AND dialog_completed_at IS NULL
                """
            )
            conn.commit()
            _table_ready = True
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()


def record_source_seen(
    *,
    account_user_id: int,
    target_user_id: int,
    source_chat_id: int,
    source_chat_title: Optional[str],
) -> None:
    """Remember every source chat where this account saw the user."""
    create_contact_tables()
    now = _now_iso()
    with _db_lock, conn:
        conn.execute(
            """
            INSERT INTO dm_contact_sources
                (account_user_id, target_user_id, source_chat_id, source_chat_title,
                 first_seen_at, last_seen_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_user_id, target_user_id, source_chat_id) DO UPDATE SET
                source_chat_title = COALESCE(
                    excluded.source_chat_title,
                    dm_contact_sources.source_chat_title
                ),
                last_seen_at = excluded.last_seen_at
            """,
            (
                int(account_user_id),
                int(target_user_id),
                int(source_chat_id),
                source_chat_title,
                now,
                now,
            ),
        )


def is_completed_contact(account_user_id: int, target_user_id: int) -> bool:
    """Return global completed-contact protection for the Telegram user.

    ``account_user_id`` is kept in the signature for backward compatibility; a
    completion by any connected account blocks every account until admin cleanup.
    """
    del account_user_id
    create_contact_tables()
    with _db_lock:
        row = conn.execute(
            "SELECT 1 FROM dm_completed_contacts WHERE target_user_id=? LIMIT 1",
            (int(target_user_id),),
        ).fetchone()
    return row is not None


def is_contact_in_progress(account_user_id: int, target_user_id: int) -> bool:
    """Return True while any connected account has a live contact cycle."""
    del account_user_id
    create_contact_tables()
    expire_stale_dialogs(
        target_user_id=int(target_user_id),
        log_result=False,
    )
    cutoff = (
        _utc_now()
        - datetime.timedelta(hours=dialog_timeout_settings()["before_link_hours"])
    ).isoformat()
    with _db_lock:
        row = conn.execute(
            """
            SELECT 1 FROM dm_contact_cycles
            WHERE target_user_id=?
              AND status IN ('first_dm_sent','active','post_link_active')
            LIMIT 1
            """,
            (int(target_user_id),),
        ).fetchone()
        if row is not None:
            return True

        # Recovery guard: if Telegram accepted a first DM but both contact/AI
        # persistence paths failed, the sent log still prevents a duplicate for
        # the normal before-link observation window.
        sent_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='dm_sent_log'"
        ).fetchone()
        if sent_table and conn.execute(
            """
            SELECT 1 FROM dm_sent_log AS log
            WHERE log.target_user_id=?
              AND log.status='sent' AND log.sent_at>=?
            LIMIT 1
            """,
            (int(target_user_id), cutoff),
        ).fetchone():
            return True

        # If the contact-cycle write failed, the AI dialog can still exist.
        # Treat a recent active AI row as in progress; old orphan rows expire.
        ai_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ai_dialogs'"
        ).fetchone()
        if not ai_table:
            return False
        row = conn.execute(
            """
            SELECT 1 FROM ai_dialogs
            WHERE target_user_id=?
              AND status='active'
              AND COALESCE(updated_at, created_at, '') >= ?
            LIMIT 1
            """,
            (int(target_user_id), cutoff),
        ).fetchone()
    return row is not None


def try_claim_first_dm(
    *,
    account_user_id: int,
    target_user_id: int,
    dm_task_id: int,
    source_chat_id: Optional[int],
) -> Optional[str]:
    """Atomically reserve one recipient globally before Telegram send.

    The persistent claim prevents two tasks, accounts or process instances from
    delivering concurrent first DMs to the same Telegram user.
    """
    create_contact_tables()
    now = _utc_now()
    cutoff = (now - datetime.timedelta(minutes=_FIRST_DM_CLAIM_TTL_MINUTES)).isoformat()
    token = uuid.uuid4().hex
    with _db_lock:
        try:
            with conn:
                conn.execute(
                    "DELETE FROM dm_first_dm_claims WHERE claimed_at < ?",
                    (cutoff,),
                )
                if conn.execute(
                    "SELECT 1 FROM dm_completed_contacts WHERE target_user_id=? LIMIT 1",
                    (int(target_user_id),),
                ).fetchone():
                    return None
                if conn.execute(
                    """
                    SELECT 1 FROM dm_contact_cycles
                    WHERE target_user_id=?
                      AND status IN ('first_dm_sent','active','post_link_active')
                    LIMIT 1
                    """,
                    (int(target_user_id),),
                ).fetchone():
                    return None
                sent_table = conn.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' AND name='dm_sent_log'"
                ).fetchone()
                sent_cutoff = (
                    now
                    - datetime.timedelta(
                        hours=dialog_timeout_settings()["before_link_hours"]
                    )
                ).isoformat()
                if sent_table and conn.execute(
                    """
                    SELECT 1 FROM dm_sent_log AS log
                    WHERE log.target_user_id=?
                      AND log.status='sent' AND log.sent_at>=?
                    LIMIT 1
                    """,
                    (int(target_user_id), sent_cutoff),
                ).fetchone():
                    return None
                ai_table = conn.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' AND name='ai_dialogs'"
                ).fetchone()
                if ai_table:
                    ai_cutoff = (
                        now
                        - datetime.timedelta(
                            hours=dialog_timeout_settings()["before_link_hours"]
                        )
                    ).isoformat()
                    if conn.execute(
                        """
                        SELECT 1 FROM ai_dialogs
                        WHERE target_user_id=?
                          AND status='active'
                          AND COALESCE(updated_at, created_at, '') >= ?
                        LIMIT 1
                        """,
                        (int(target_user_id), ai_cutoff),
                    ).fetchone():
                        return None
                if conn.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' AND name='dm_opt_out_users'"
                ).fetchone() and conn.execute(
                    "SELECT 1 FROM dm_opt_out_users WHERE target_user_id=? LIMIT 1",
                    (int(target_user_id),),
                ).fetchone():
                    return None
                conn.execute(
                    """
                    INSERT INTO dm_first_dm_claims
                        (account_user_id, target_user_id, claim_token, dm_task_id,
                         source_chat_id, claimed_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(account_user_id),
                        int(target_user_id),
                        token,
                        int(dm_task_id),
                        source_chat_id,
                        now.isoformat(),
                    ),
                )
        except sqlite3.IntegrityError:
            return None
    return token


def release_first_dm_claim(
    account_user_id: int, target_user_id: int, claim_token: Optional[str]
) -> bool:
    if not claim_token:
        return False
    create_contact_tables()
    with _db_lock, conn:
        cur = conn.execute(
            """
            DELETE FROM dm_first_dm_claims
             WHERE account_user_id=? AND target_user_id=? AND claim_token=?
            """,
            (int(account_user_id), int(target_user_id), str(claim_token)),
        )
        return int(cur.rowcount or 0) > 0


def record_first_dm(
    *,
    dm_task_id: int,
    account_user_id: int,
    target_user_id: int,
    source_chat_id: Optional[int],
    source_chat_title: Optional[str],
    claim_token: Optional[str] = None,
) -> int:
    """Record a delivered first DM and consume its persistent claim atomically."""
    create_contact_tables()
    now = _now_iso()
    with _db_lock:
        cur = conn.cursor()
        try:
            cur.execute("BEGIN IMMEDIATE")
            cur.execute(
                """
                INSERT INTO dm_contact_cycles
                    (dm_task_id, account_user_id, target_user_id, source_chat_id,
                     source_chat_title, status, first_dm_at, last_activity_at)
                VALUES (?, ?, ?, ?, ?, 'first_dm_sent', ?, ?)
                """,
                (
                    int(dm_task_id),
                    int(account_user_id),
                    int(target_user_id),
                    source_chat_id,
                    source_chat_title,
                    now,
                    now,
                ),
            )
            cycle_id = int(cur.lastrowid)
            if claim_token:
                cur.execute(
                    """
                    DELETE FROM dm_first_dm_claims
                     WHERE account_user_id=? AND target_user_id=? AND claim_token=?
                    """,
                    (
                        int(account_user_id),
                        int(target_user_id),
                        str(claim_token),
                    ),
                )
            conn.commit()
            return cycle_id
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()


def mark_first_reply(cycle_id: Optional[int]) -> None:
    if not cycle_id:
        return
    now = _now_iso()
    with _db_lock, conn:
        conn.execute(
            """
            UPDATE dm_contact_cycles
               SET first_reply_at=COALESCE(first_reply_at, ?),
                   status=CASE WHEN status='first_dm_sent' THEN 'active' ELSE status END,
                   last_activity_at=?
             WHERE id=?
               AND status IN ('first_dm_sent','active','post_link_active')
            """,
            (now, now, int(cycle_id)),
        )


def mark_latest_first_reply(account_user_id: int, target_user_id: int) -> Optional[int]:
    """Idempotently record the newest open cycle's first real private reply."""
    create_contact_tables()
    with _db_lock:
        row = conn.execute(
            """
            SELECT id FROM dm_contact_cycles
            WHERE account_user_id=? AND target_user_id=?
              AND status IN ('first_dm_sent','active','post_link_active')
            ORDER BY id DESC LIMIT 1
            """,
            (int(account_user_id), int(target_user_id)),
        ).fetchone()
        if not row:
            return None
        cycle_id = int(row[0])
        now = _now_iso()
        with conn:
            conn.execute(
                """
                UPDATE dm_contact_cycles
                   SET first_reply_at=COALESCE(first_reply_at, ?),
                       status=CASE WHEN status='first_dm_sent' THEN 'active' ELSE status END,
                       last_activity_at=?
                 WHERE id=?
                   AND status IN ('first_dm_sent','active','post_link_active')
                """,
                (now, now, cycle_id),
            )
        return cycle_id


def touch_cycle(cycle_id: Optional[int]) -> None:
    if not cycle_id:
        return
    with _db_lock, conn:
        conn.execute(
            """
            UPDATE dm_contact_cycles SET last_activity_at=?
            WHERE id=? AND status IN ('first_dm_sent','active','post_link_active')
            """,
            (_now_iso(), int(cycle_id)),
        )


def mark_link_sent(cycle_id: Optional[int]) -> None:
    if not cycle_id:
        return
    now = _now_iso()
    with _db_lock, conn:
        conn.execute(
            """
            UPDATE dm_contact_cycles
               SET status='post_link_active',
                   link_sent_at=COALESCE(link_sent_at, ?),
                   last_activity_at=?
             WHERE id=? AND status IN ('first_dm_sent','active','post_link_active')
            """,
            (now, now, int(cycle_id)),
        )


def _cycle_row(
    cycle_id: int,
) -> Optional[tuple[int, int, Optional[int], Optional[str], str, str]]:
    with _db_lock:
        row = conn.execute(
            """
            SELECT account_user_id, target_user_id, source_chat_id,
                   source_chat_title, status, last_activity_at
            FROM dm_contact_cycles WHERE id=?
            """,
            (int(cycle_id),),
        ).fetchone()
    if not row:
        return None
    return int(row[0]), int(row[1]), row[2], row[3], str(row[4]), str(row[5])


def _finish_cycle(
    cycle_id: int,
    *,
    status: str,
    reason: str,
    block_repeat: bool,
    activity_before: Optional[str] = None,
) -> Optional[tuple[int, int]]:
    """Atomically apply one terminal transition and optional repeat protection."""
    create_contact_tables()
    now = _now_iso()
    with _db_lock:
        cur = conn.cursor()
        try:
            cur.execute("BEGIN IMMEDIATE")
            row = cur.execute(
                """
                SELECT account_user_id, target_user_id, source_chat_id,
                       source_chat_title, status, last_activity_at
                FROM dm_contact_cycles WHERE id=?
                """,
                (int(cycle_id),),
            ).fetchone()
            if not row:
                conn.rollback()
                return None

            account_user_id = int(row[0])
            target_user_id = int(row[1])
            source_chat_id = row[2]
            source_chat_title = row[3]
            current_status = str(row[4])
            last_activity_at = str(row[5])

            if activity_before is not None and last_activity_at >= activity_before:
                conn.rollback()
                return None

            if status == "completed":
                if current_status in {"opted_out", "abandoned"}:
                    conn.rollback()
                    return None
                if current_status == "completed":
                    if cur.execute(
                        "SELECT 1 FROM dm_completed_contacts WHERE target_user_id=? LIMIT 1",
                        (target_user_id,),
                    ).fetchone():
                        conn.rollback()
                        return None
            elif status == "abandoned":
                if current_status not in _BEFORE_LINK_STATUSES:
                    conn.rollback()
                    return None
            elif status == "opted_out":
                if current_status == "opted_out":
                    conn.rollback()
                    return None
            else:
                conn.rollback()
                raise ValueError(f"Unsupported terminal contact status: {status}")

            if status == "completed":
                cur.execute(
                    """
                    UPDATE dm_contact_cycles
                       SET status=?, completion_reason=?, completed_at=?,
                           dialog_completed_at=COALESCE(dialog_completed_at, ?),
                           last_activity_at=?
                     WHERE id=? AND status=?
                    """,
                    (
                        status,
                        reason,
                        now,
                        now,
                        now,
                        int(cycle_id),
                        current_status,
                    ),
                )
            else:
                cur.execute(
                    """
                    UPDATE dm_contact_cycles
                       SET status=?, completion_reason=?, completed_at=?, last_activity_at=?
                     WHERE id=? AND status=?
                    """,
                    (status, reason, now, now, int(cycle_id), current_status),
                )
            if int(cur.rowcount or 0) != 1:
                conn.rollback()
                return None

            # A terminal cycle must not leave an old first-DM reservation behind.
            cur.execute(
                "DELETE FROM dm_first_dm_claims WHERE target_user_id=?",
                (target_user_id,),
            )

            if block_repeat:
                cur.execute(
                    """
                    INSERT INTO dm_completed_contacts
                        (account_user_id, target_user_id, source_chat_id,
                         source_chat_title, cycle_id, completed_at,
                         completion_reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(target_user_id) DO NOTHING
                    """,
                    (
                        account_user_id,
                        target_user_id,
                        source_chat_id,
                        source_chat_title,
                        int(cycle_id),
                        now,
                        reason,
                    ),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
    return account_user_id, target_user_id


def _complete_contact_without_cycle(
    *,
    account_user_id: int,
    target_user_id: int,
    source_chat_id: Optional[int],
    source_chat_title: Optional[str],
    reason: str,
) -> tuple[int, int]:
    """Create repeat protection when a delivered DM lacks an analytics cycle.

    This is a recovery path for the narrow failure window after Telegram accepts
    the first message but before ``dm_contact_cycles`` is persisted. Historical
    counters cannot be reconstructed safely, but the user must still be protected
    from another first DM by any connected account.
    """
    create_contact_tables()
    now = _now_iso()
    account_user_id = int(account_user_id)
    target_user_id = int(target_user_id)
    with _db_lock:
        cur = conn.cursor()
        try:
            cur.execute("BEGIN IMMEDIATE")
            cur.execute(
                """
                INSERT INTO dm_completed_contacts
                    (account_user_id, target_user_id, source_chat_id,
                     source_chat_title, cycle_id, completed_at,
                     completion_reason)
                VALUES (?, ?, ?, ?, NULL, ?, ?)
                ON CONFLICT(target_user_id) DO NOTHING
                """,
                (
                    account_user_id,
                    target_user_id,
                    source_chat_id,
                    source_chat_title,
                    now,
                    reason,
                ),
            )
            cur.execute(
                "DELETE FROM dm_first_dm_claims WHERE target_user_id=?",
                (target_user_id,),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
    return account_user_id, target_user_id


def mark_completed(
    cycle_id: Optional[int],
    reason: str,
    *,
    account_user_id: Optional[int] = None,
    target_user_id: Optional[int] = None,
    source_chat_id: Optional[int] = None,
    source_chat_title: Optional[str] = None,
) -> None:
    if cycle_id:
        pair = _finish_cycle(
            int(cycle_id),
            status="completed",
            reason=reason,
            block_repeat=True,
        )
    elif account_user_id is not None and target_user_id is not None:
        pair = _complete_contact_without_cycle(
            account_user_id=int(account_user_id),
            target_user_id=int(target_user_id),
            source_chat_id=source_chat_id,
            source_chat_title=source_chat_title,
            reason=reason,
        )
    else:
        return
    if pair:
        removed = _purge_completed_contact_from_queues(pair[1])
        if removed:
            logger.info(
                "[DM analytics] completed contact removed from live queues: "
                f"user={pair[1]}, removed={removed}"
            )


def mark_opted_out(cycle_id: Optional[int], reason: str = "explicit_stop") -> None:
    if cycle_id:
        _finish_cycle(
            int(cycle_id),
            status="opted_out",
            reason=reason,
            block_repeat=False,
        )


def mark_abandoned(cycle_id: Optional[int], reason: str = "inactive_before_link") -> None:
    if cycle_id:
        _finish_cycle(
            int(cycle_id),
            status="abandoned",
            reason=reason,
            block_repeat=False,
        )


def _ai_dialogs_table_exists() -> bool:
    with _db_lock:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ai_dialogs'"
        ).fetchone()
    return row is not None


def _sync_ai_terminal_status(cycle_id: int, status: str, reason: str) -> None:
    if not _ai_dialogs_table_exists():
        return
    with _db_lock, conn:
        conn.execute(
            """
            UPDATE ai_dialogs
               SET status=?, stage=?, stopped_reason=?, updated_at=?
             WHERE contact_cycle_id=?
               AND status IN ('active','human_needed','send_error')
            """,
            (status, status, reason, _now_iso(), int(cycle_id)),
        )


def expire_stale_dialogs(
    *,
    account_user_id: Optional[int] = None,
    target_user_id: Optional[int] = None,
    log_result: bool = True,
) -> dict[str, int]:
    """Finalize stale cycles with a cutoff re-check inside each transaction."""
    create_contact_tables()
    settings = dialog_timeout_settings()
    now = _utc_now()
    before_cutoff = (
        now - datetime.timedelta(hours=settings["before_link_hours"])
    ).isoformat()
    after_cutoff = (
        now - datetime.timedelta(hours=settings["post_link_hours"])
    ).isoformat()

    filters = ""
    params_before: list[Any] = [before_cutoff]
    params_after: list[Any] = [after_cutoff]
    if account_user_id is not None:
        filters += " AND account_user_id=?"
        params_before.append(int(account_user_id))
        params_after.append(int(account_user_id))
    if target_user_id is not None:
        filters += " AND target_user_id=?"
        params_before.append(int(target_user_id))
        params_after.append(int(target_user_id))

    with _db_lock:
        rows_before = conn.execute(
            """
            SELECT id FROM dm_contact_cycles
            WHERE status IN ('first_dm_sent','active') AND last_activity_at < ?
            """ + filters,
            tuple(params_before),
        ).fetchall()
        rows_after = conn.execute(
            """
            SELECT id FROM dm_contact_cycles
            WHERE status='post_link_active' AND last_activity_at < ?
            """ + filters,
            tuple(params_after),
        ).fetchall()

    abandoned = 0
    completed = 0
    before_reason = f"inactive_{settings['before_link_hours']}h_before_link"
    after_reason = f"timeout_{settings['post_link_hours']}h_after_link"

    for (cycle_id,) in rows_before:
        pair = _finish_cycle(
            int(cycle_id),
            status="abandoned",
            reason=before_reason,
            block_repeat=False,
            activity_before=before_cutoff,
        )
        if pair:
            abandoned += 1
            _sync_ai_terminal_status(int(cycle_id), "abandoned", before_reason)

    for (cycle_id,) in rows_after:
        pair = _finish_cycle(
            int(cycle_id),
            status="completed",
            reason=after_reason,
            block_repeat=True,
            activity_before=after_cutoff,
        )
        if pair:
            completed += 1
            removed = _purge_completed_contact_from_queues(pair[1])
            if removed:
                logger.info(
                    "[DM analytics] timeout-completed contact removed from live queues: "
                    f"user={pair[1]}, removed={removed}"
                )
            _sync_ai_terminal_status(int(cycle_id), "completed", after_reason)

    if log_result and (abandoned or completed):
        logger.info(
            f"[DM analytics] stale cycles: abandoned={abandoned}, completed={completed}"
        )
    return {"abandoned": abandoned, "completed": completed}


def _scalar(sql: str, params: tuple[Any, ...] = ()) -> int:
    with _db_lock:
        return int(conn.execute(sql, params).fetchone()[0] or 0)


def _global_opt_out_count() -> int:
    # Local import avoids coupling table creation order to the analytics module.
    from services.dm_opt_out import opt_out_count

    return opt_out_count()


def overall_stats() -> dict[str, int]:
    create_contact_tables()
    expire_stale_dialogs()
    return {
        "seen_recipients": _scalar(
            "SELECT COUNT(DISTINCT target_user_id) FROM dm_contact_sources"
        ),
        "unique_recipients": _scalar(
            "SELECT COUNT(DISTINCT target_user_id) FROM dm_contact_cycles"
        ),
        "first_dms": _scalar("SELECT COUNT(*) FROM dm_contact_cycles"),
        "replied": _scalar(
            "SELECT COUNT(DISTINCT target_user_id) FROM dm_contact_cycles "
            "WHERE first_reply_at IS NOT NULL"
        ),
        "replied_dialogs": _scalar(
            "SELECT COUNT(*) FROM dm_contact_cycles WHERE first_reply_at IS NOT NULL"
        ),
        "link_sent": _scalar(
            "SELECT COUNT(*) FROM dm_contact_cycles WHERE link_sent_at IS NOT NULL"
        ),
        "completed": _scalar(
            "SELECT COUNT(*) FROM dm_contact_cycles "
            "WHERE dialog_completed_at IS NOT NULL"
        ),
        "completed_unique_people": _scalar(
            "SELECT COUNT(DISTINCT target_user_id) FROM dm_contact_cycles "
            "WHERE dialog_completed_at IS NOT NULL"
        ),
        "completed_account_contacts": _scalar(
            "SELECT COUNT(*) FROM ("
            "SELECT account_user_id,target_user_id FROM dm_contact_cycles "
            "WHERE dialog_completed_at IS NOT NULL "
            "GROUP BY account_user_id,target_user_id)"
        ),
        "blocked_records": _scalar("SELECT COUNT(*) FROM dm_completed_contacts"),
        "opted_out": _global_opt_out_count(),
        "active": _scalar(
            "SELECT COUNT(*) FROM dm_contact_cycles "
            "WHERE status IN ('active','post_link_active')"
        ),
        "waiting": _scalar(
            "SELECT COUNT(*) FROM dm_contact_cycles WHERE status='first_dm_sent'"
        ),
        "abandoned": _scalar(
            "SELECT COUNT(*) FROM dm_contact_cycles WHERE status='abandoned'"
        ),
        "active_claims": _scalar("SELECT COUNT(*) FROM dm_first_dm_claims"),
    }


def _latest_chat_title(chat_id: int) -> str:
    with _db_lock:
        row = conn.execute(
            """
            SELECT source_chat_title FROM dm_contact_sources
            WHERE source_chat_id=? AND source_chat_title IS NOT NULL
            ORDER BY last_seen_at DESC LIMIT 1
            """,
            (int(chat_id),),
        ).fetchone()
        if row and row[0]:
            return str(row[0])
        row = conn.execute(
            """
            SELECT source_chat_title FROM dm_contact_cycles
            WHERE source_chat_id=? AND source_chat_title IS NOT NULL
            ORDER BY id DESC LIMIT 1
            """,
            (int(chat_id),),
        ).fetchone()
    return str(row[0]) if row and row[0] else str(chat_id)


def chat_rows() -> list[tuple[int, str, int, int]]:
    """Return chat id, latest title, first-DM count and unique seen users."""
    create_contact_tables()
    with _db_lock:
        ids = [
            int(row[0])
            for row in conn.execute(
                """
                SELECT source_chat_id FROM dm_contact_sources
                UNION
                SELECT source_chat_id FROM dm_contact_cycles
                 WHERE source_chat_id IS NOT NULL
                """
            ).fetchall()
            if row[0] is not None
        ]
    rows: list[tuple[int, str, int, int]] = []
    for chat_id in ids:
        rows.append(
            (
                chat_id,
                _latest_chat_title(chat_id),
                _scalar(
                    "SELECT COUNT(*) FROM dm_contact_cycles WHERE source_chat_id=?",
                    (chat_id,),
                ),
                _scalar(
                    "SELECT COUNT(DISTINCT target_user_id) FROM dm_contact_sources "
                    "WHERE source_chat_id=?",
                    (chat_id,),
                ),
            )
        )
    rows.sort(key=lambda item: item[1].casefold())
    return rows


def chat_stats(chat_id: int) -> dict[str, Any]:
    create_contact_tables()
    expire_stale_dialogs()
    chat_id = int(chat_id)
    params = (chat_id,)
    return {
        "title": _latest_chat_title(chat_id),
        "seen_recipients": _scalar(
            "SELECT COUNT(DISTINCT target_user_id) FROM dm_contact_sources "
            "WHERE source_chat_id=?",
            params,
        ),
        "accounts_count": _scalar(
            "SELECT COUNT(DISTINCT account_user_id) FROM dm_contact_sources "
            "WHERE source_chat_id=?",
            params,
        ),
        "unique_recipients": _scalar(
            "SELECT COUNT(DISTINCT target_user_id) FROM dm_contact_cycles "
            "WHERE source_chat_id=?",
            params,
        ),
        "first_dms": _scalar(
            "SELECT COUNT(*) FROM dm_contact_cycles WHERE source_chat_id=?",
            params,
        ),
        "replied": _scalar(
            "SELECT COUNT(DISTINCT target_user_id) FROM dm_contact_cycles "
            "WHERE source_chat_id=? AND first_reply_at IS NOT NULL",
            params,
        ),
        "replied_dialogs": _scalar(
            "SELECT COUNT(*) FROM dm_contact_cycles "
            "WHERE source_chat_id=? AND first_reply_at IS NOT NULL",
            params,
        ),
        "link_sent": _scalar(
            "SELECT COUNT(*) FROM dm_contact_cycles "
            "WHERE source_chat_id=? AND link_sent_at IS NOT NULL",
            params,
        ),
        "completed": _scalar(
            "SELECT COUNT(*) FROM dm_contact_cycles "
            "WHERE source_chat_id=? AND dialog_completed_at IS NOT NULL",
            params,
        ),
        "completed_unique_people": _scalar(
            "SELECT COUNT(DISTINCT target_user_id) FROM dm_contact_cycles "
            "WHERE source_chat_id=? AND dialog_completed_at IS NOT NULL",
            params,
        ),
        "completed_account_contacts": _scalar(
            "SELECT COUNT(*) FROM ("
            "SELECT account_user_id,target_user_id FROM dm_contact_cycles "
            "WHERE source_chat_id=? AND dialog_completed_at IS NOT NULL "
            "GROUP BY account_user_id,target_user_id)",
            params,
        ),
        "opted_out": _scalar(
            "SELECT COUNT(DISTINCT target_user_id) FROM dm_contact_cycles "
            "WHERE source_chat_id=? AND status='opted_out'",
            params,
        ),
        "active": _scalar(
            "SELECT COUNT(*) FROM dm_contact_cycles "
            "WHERE source_chat_id=? AND status IN ('active','post_link_active')",
            params,
        ),
        "waiting": _scalar(
            "SELECT COUNT(*) FROM dm_contact_cycles "
            "WHERE source_chat_id=? AND status='first_dm_sent'",
            params,
        ),
        "abandoned": _scalar(
            "SELECT COUNT(*) FROM dm_contact_cycles "
            "WHERE source_chat_id=? AND status='abandoned'",
            params,
        ),
        "blocked_records": _scalar(
            "SELECT COUNT(*) FROM dm_completed_contacts WHERE source_chat_id=?",
            params,
        ),
    }


def clear_completed_for_chat(chat_id: int) -> int:
    """Remove current completed-contact protection attributed to one chat.

    Historical counters and permanent global opt-out records are preserved. No
    queued item is restored, so a fresh watched-chat message is still required.
    """
    create_contact_tables()
    with _db_lock:
        cur = conn.cursor()
        try:
            cur.execute(
                "DELETE FROM dm_completed_contacts WHERE source_chat_id=?",
                (int(chat_id),),
            )
            affected = int(cur.rowcount or 0)
            conn.commit()
            return affected
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
