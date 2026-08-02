"""Unified first-DM lead pool — Step 1 (schema, mode flag, migration, shadow sync).

This module prepares a global lead pool for a future unified sending mode.
While mode is ``per_account`` (default), live first-DM delivery still uses
``dm_pending_queue`` and existing account dispatchers unchanged.

Step 1 responsibilities:
- persist queue mode (``per_account`` | ``unified``), default ``per_account``;
- create unified lead tables;
- migrate active rows from ``dm_pending_queue`` into the pool (idempotent);
- shadow-sync new/updated pending rows so the pool stays current before Step 2.

Step 2 enables live sending from this pool when mode is ``unified``:
account dispatchers reserve due leads under a single global spacing window,
materialize a per-account pending row for the existing send pipeline, and keep
PeerFlood / FloodWait account limits intact.
"""

from __future__ import annotations

import datetime as dt
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Optional

from loguru import logger

from config import conn

UTC = dt.timezone.utc
MODE_PER_ACCOUNT = "per_account"
MODE_UNIFIED = "unified"
VALID_MODES = frozenset({MODE_PER_ACCOUNT, MODE_UNIFIED})

# Statuses that still represent an unfinished first-DM contact.
ACTIVE_LEAD_STATUSES = (
    "pending",
    "reserved",
    "claimed",
    "sending",
    "retry_wait",
    "unresolved_peer",
    "uncertain_delivery",
)

_db_lock = threading.RLock()


@dataclass(frozen=True)
class QueueRuntimeState:
    mode: str
    global_spacing_min: int
    global_spacing_max: int
    last_global_send_at: Optional[str]
    next_global_send_at: Optional[str]
    lease_owner_account_user_id: Optional[int]
    lease_token: Optional[str]
    lease_expires_at: Optional[str]
    updated_at: str
    updated_by_admin_id: Optional[int]


def utc_now() -> dt.datetime:
    return dt.datetime.now(UTC)


def _iso(value: dt.datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _clean(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = " ".join(str(value).replace("\n", " ").split()).strip()
    return text or None


def ensure_unified_queue_schema() -> None:
    """Create unified-queue tables and the single-row runtime control."""
    now = _iso(utc_now())
    with _db_lock, conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dm_queue_runtime (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                mode TEXT NOT NULL DEFAULT 'per_account',
                global_spacing_min INTEGER NOT NULL DEFAULT 30,
                global_spacing_max INTEGER NOT NULL DEFAULT 60,
                last_global_send_at TEXT,
                next_global_send_at TEXT,
                lease_owner_account_user_id INTEGER,
                lease_token TEXT,
                lease_expires_at TEXT,
                updated_at TEXT NOT NULL,
                updated_by_admin_id INTEGER
            )
            """
        )
        # Backward-compatible migration for databases created before b27.
        runtime_columns = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(dm_queue_runtime)").fetchall()
        }
        for column_name, column_sql in (
            ("lease_owner_account_user_id", "INTEGER"),
            ("lease_token", "TEXT"),
            ("lease_expires_at", "TEXT"),
        ):
            if column_name not in runtime_columns:
                conn.execute(
                    f"ALTER TABLE dm_queue_runtime ADD COLUMN {column_name} {column_sql}"
                )
        conn.execute(
            """
            INSERT OR IGNORE INTO dm_queue_runtime (
                id, mode, global_spacing_min, global_spacing_max,
                last_global_send_at, next_global_send_at,
                lease_owner_account_user_id, lease_token, lease_expires_at,
                updated_at, updated_by_admin_id
            ) VALUES (1, 'per_account', 30, 60, NULL, NULL, NULL, NULL, NULL, ?, NULL)
            """,
            (now,),
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dm_unified_leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                target_user_id INTEGER NOT NULL,
                preferred_account_user_id INTEGER,
                source_account_user_id INTEGER,
                dm_task_id INTEGER,
                source_chat_id INTEGER,
                source_chat_title TEXT,
                source_chat_username TEXT,
                source_message_id INTEGER,
                target_access_hash INTEGER,
                target_username TEXT,
                target_first_name TEXT,
                target_last_name TEXT,
                first_dm_module TEXT,
                enqueued_at TEXT NOT NULL,
                eligible_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                reserved_by_account_user_id INTEGER,
                reserved_at TEXT,
                reserve_token TEXT,
                sent_at TEXT,
                sent_by_account_user_id INTEGER,
                legacy_pending_id INTEGER,
                retry_count INTEGER NOT NULL DEFAULT 0,
                resolve_attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dm_unified_lead_sources (
                lead_id INTEGER NOT NULL,
                dm_task_id INTEGER NOT NULL,
                account_user_id INTEGER,
                source_chat_id INTEGER NOT NULL,
                source_chat_title TEXT,
                source_chat_username TEXT,
                source_message_id INTEGER,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY (lead_id, dm_task_id, source_chat_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dm_unified_lead_accounts (
                lead_id INTEGER NOT NULL,
                account_user_id INTEGER NOT NULL,
                is_preferred INTEGER NOT NULL DEFAULT 0,
                access_hash INTEGER,
                target_username TEXT,
                PRIMARY KEY (lead_id, account_user_id)
            )
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_dm_unified_leads_active_target
            ON dm_unified_leads(target_user_id)
            WHERE status IN (
                'pending','reserved','claimed','sending','retry_wait',
                'unresolved_peer','uncertain_delivery'
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_dm_unified_leads_due
            ON dm_unified_leads(status, eligible_at, id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_dm_unified_leads_legacy
            ON dm_unified_leads(legacy_pending_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_dm_unified_lead_accounts_account
            ON dm_unified_lead_accounts(account_user_id, lead_id)
            """
        )


def get_queue_runtime_state() -> QueueRuntimeState:
    ensure_unified_queue_schema()
    row = conn.execute(
        """
        SELECT mode, global_spacing_min, global_spacing_max,
               last_global_send_at, next_global_send_at,
               lease_owner_account_user_id, lease_token, lease_expires_at,
               updated_at, updated_by_admin_id
          FROM dm_queue_runtime
         WHERE id=1
        """
    ).fetchone()
    assert row is not None
    mode = str(row[0] or MODE_PER_ACCOUNT)
    if mode not in VALID_MODES:
        mode = MODE_PER_ACCOUNT
    return QueueRuntimeState(
        mode=mode,
        global_spacing_min=int(row[1] or 30),
        global_spacing_max=int(row[2] or 60),
        last_global_send_at=row[3],
        next_global_send_at=row[4],
        lease_owner_account_user_id=int(row[5]) if row[5] is not None else None,
        lease_token=str(row[6]) if row[6] is not None else None,
        lease_expires_at=row[7],
        updated_at=str(row[8] or ""),
        updated_by_admin_id=int(row[9]) if row[9] is not None else None,
    )


def is_unified_queue_mode() -> bool:
    return get_queue_runtime_state().mode == MODE_UNIFIED


def set_queue_mode(
    mode: str,
    *,
    admin_id: Optional[int] = None,
) -> QueueRuntimeState:
    """Persist queue mode. Step 1 keeps live sending on per-account path always."""
    normalized = str(mode or "").strip().lower()
    if normalized not in VALID_MODES:
        raise ValueError(
            f"queue mode must be one of: {', '.join(sorted(VALID_MODES))}"
        )
    now = _iso(utc_now())
    ensure_unified_queue_schema()
    with _db_lock, conn:
        conn.execute(
            """
            UPDATE dm_queue_runtime
               SET mode=?, updated_at=?, updated_by_admin_id=?
             WHERE id=1
            """,
            (normalized, now, int(admin_id) if admin_id is not None else None),
        )
    state = get_queue_runtime_state()
    logger.info(
        f"[DM unified queue] mode set to {state.mode} "
        f"admin={admin_id if admin_id is not None else '-'}"
    )
    return state


def set_global_spacing(low: int, high: int) -> QueueRuntimeState:
    """Store global first-DM spacing for Step 2 (not applied while mode is off)."""
    low = int(low)
    high = int(high)
    if low < 5 or high < low or high > 30 * 24 * 60 * 60:
        raise ValueError("global spacing must satisfy 5 <= min <= max <= 30 days")
    now = _iso(utc_now())
    ensure_unified_queue_schema()
    with _db_lock, conn:
        conn.execute(
            """
            UPDATE dm_queue_runtime
               SET global_spacing_min=?, global_spacing_max=?, updated_at=?
             WHERE id=1
            """,
            (low, high, now),
        )
    return get_queue_runtime_state()


def _map_pending_status(status: str) -> str:
    value = str(status or "pending").strip().lower()
    if value in ACTIVE_LEAD_STATUSES or value in {"sent", "cancelled"}:
        return value
    return "pending"


def _task_module(dm_task_id: Optional[int]) -> Optional[str]:
    if dm_task_id is None:
        return None
    row = conn.execute(
        "SELECT first_dm_module FROM dm_tasks WHERE id=?",
        (int(dm_task_id),),
    ).fetchone()
    if not row:
        return None
    return _clean(row[0]) or "default"


def _attach_lead_account(
    lead_id: int,
    account_user_id: int,
    *,
    is_preferred: bool,
    access_hash: Optional[int],
    target_username: Optional[str],
) -> None:
    conn.execute(
        """
        INSERT INTO dm_unified_lead_accounts (
            lead_id, account_user_id, is_preferred, access_hash, target_username
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(lead_id, account_user_id) DO UPDATE SET
            is_preferred=MAX(dm_unified_lead_accounts.is_preferred, excluded.is_preferred),
            access_hash=COALESCE(excluded.access_hash, dm_unified_lead_accounts.access_hash),
            target_username=COALESCE(
                excluded.target_username, dm_unified_lead_accounts.target_username
            )
        """,
        (
            int(lead_id),
            int(account_user_id),
            1 if is_preferred else 0,
            int(access_hash) if access_hash is not None else None,
            _clean(target_username),
        ),
    )


def _attach_lead_sources(lead_id: int, pending_id: int, account_user_id: int) -> None:
    rows = conn.execute(
        """
        SELECT dm_task_id, source_chat_id, source_chat_title,
               source_chat_username, source_message_id, first_seen_at, last_seen_at
          FROM dm_pending_sources
         WHERE pending_id=?
        """,
        (int(pending_id),),
    ).fetchall()
    if not rows:
        # Fall back to the pending row's single source fields.
        row = conn.execute(
            """
            SELECT dm_task_id, source_chat_id, source_chat_title,
                   source_chat_username, source_message_id, enqueued_at, updated_at
              FROM dm_pending_queue
             WHERE id=?
            """,
            (int(pending_id),),
        ).fetchone()
        if not row or row[1] is None:
            return
        rows = [
            (
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                row[5],
                row[6] or row[5],
            )
        ]
    for (
        dm_task_id,
        source_chat_id,
        source_chat_title,
        source_chat_username,
        source_message_id,
        first_seen_at,
        last_seen_at,
    ) in rows:
        if source_chat_id is None or dm_task_id is None:
            continue
        conn.execute(
            """
            INSERT INTO dm_unified_lead_sources (
                lead_id, dm_task_id, account_user_id, source_chat_id,
                source_chat_title, source_chat_username, source_message_id,
                first_seen_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(lead_id, dm_task_id, source_chat_id) DO UPDATE SET
                account_user_id=COALESCE(
                    excluded.account_user_id, dm_unified_lead_sources.account_user_id
                ),
                source_chat_title=COALESCE(
                    excluded.source_chat_title, dm_unified_lead_sources.source_chat_title
                ),
                source_chat_username=COALESCE(
                    excluded.source_chat_username,
                    dm_unified_lead_sources.source_chat_username
                ),
                source_message_id=COALESCE(
                    excluded.source_message_id, dm_unified_lead_sources.source_message_id
                ),
                last_seen_at=excluded.last_seen_at
            """,
            (
                int(lead_id),
                int(dm_task_id),
                int(account_user_id),
                int(source_chat_id),
                _clean(source_chat_title),
                _clean(source_chat_username),
                int(source_message_id) if source_message_id is not None else None,
                str(first_seen_at or _iso(utc_now())),
                str(last_seen_at or first_seen_at or _iso(utc_now())),
            ),
        )


def _find_active_lead(target_user_id: int) -> Optional[int]:
    row = conn.execute(
        f"""
        SELECT id FROM dm_unified_leads
         WHERE target_user_id=?
           AND status IN ({",".join("?" for _ in ACTIVE_LEAD_STATUSES)})
         ORDER BY id
         LIMIT 1
        """,
        (int(target_user_id), *ACTIVE_LEAD_STATUSES),
    ).fetchone()
    return int(row[0]) if row else None


def shadow_sync_pending_row(pending_id: int) -> Optional[int]:
    """Mirror one legacy pending row into the unified pool without affecting send."""
    ensure_unified_queue_schema()
    pending_id = int(pending_id)
    with _db_lock, conn:
        row = conn.execute(
            """
            SELECT id, dm_task_id, account_user_id, target_user_id,
                   target_access_hash, target_username, target_first_name,
                   target_last_name, source_chat_id, source_chat_title,
                   source_chat_username, source_message_id,
                   enqueued_at, eligible_at, status, retry_count,
                   resolve_attempts, last_error, claim_token, claimed_at,
                   send_started_at, sent_at
              FROM dm_pending_queue
             WHERE id=?
            """,
            (pending_id,),
        ).fetchone()
        if not row:
            return None

        (
            _pid,
            dm_task_id,
            account_user_id,
            target_user_id,
            target_access_hash,
            target_username,
            target_first_name,
            target_last_name,
            source_chat_id,
            source_chat_title,
            source_chat_username,
            source_message_id,
            enqueued_at,
            eligible_at,
            status,
            retry_count,
            resolve_attempts,
            last_error,
            claim_token,
            claimed_at,
            _send_started_at,
            sent_at,
        ) = row

        mapped_status = _map_pending_status(str(status))
        if mapped_status in {"sent", "cancelled"}:
            # Terminal legacy rows are not kept as active unified leads.
            existing = conn.execute(
                "SELECT id FROM dm_unified_leads WHERE legacy_pending_id=?",
                (pending_id,),
            ).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE dm_unified_leads
                       SET status=?, sent_at=COALESCE(?, sent_at),
                           sent_by_account_user_id=COALESCE(sent_by_account_user_id, ?),
                           updated_at=?
                     WHERE id=?
                    """,
                    (
                        mapped_status,
                        sent_at,
                        int(account_user_id) if account_user_id is not None else None,
                        _iso(utc_now()),
                        int(existing[0]),
                    ),
                )
                return int(existing[0])
            return None

        lead_id = _find_active_lead(int(target_user_id))
        module = _task_module(int(dm_task_id) if dm_task_id is not None else None)
        now = _iso(utc_now())

        if lead_id is None:
            cursor = conn.execute(
                """
                INSERT INTO dm_unified_leads (
                    target_user_id, preferred_account_user_id, source_account_user_id,
                    dm_task_id, source_chat_id, source_chat_title, source_chat_username,
                    source_message_id, target_access_hash, target_username,
                    target_first_name, target_last_name, first_dm_module,
                    enqueued_at, eligible_at, status,
                    reserved_by_account_user_id, reserved_at, reserve_token,
                    sent_at, sent_by_account_user_id, legacy_pending_id,
                    retry_count, resolve_attempts, last_error, updated_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?
                )
                """,
                (
                    int(target_user_id),
                    int(account_user_id) if account_user_id is not None else None,
                    int(account_user_id) if account_user_id is not None else None,
                    int(dm_task_id) if dm_task_id is not None else None,
                    int(source_chat_id) if source_chat_id is not None else None,
                    _clean(source_chat_title),
                    _clean(source_chat_username),
                    int(source_message_id) if source_message_id is not None else None,
                    int(target_access_hash) if target_access_hash is not None else None,
                    _clean(target_username),
                    _clean(target_first_name),
                    _clean(target_last_name),
                    module,
                    str(enqueued_at or now),
                    str(eligible_at or now),
                    mapped_status,
                    int(account_user_id)
                    if mapped_status in {"claimed", "sending", "reserved"}
                    and account_user_id is not None
                    else None,
                    claimed_at
                    if mapped_status in {"claimed", "sending", "reserved"}
                    else None,
                    claim_token
                    if mapped_status in {"claimed", "sending", "reserved"}
                    else None,
                    pending_id,
                    int(retry_count or 0),
                    int(resolve_attempts or 0),
                    _clean(last_error),
                    now,
                ),
            )
            lead_id = int(cursor.lastrowid)
        else:
            # Merge: keep earliest eligible_at, refresh profile fields, prefer source account.
            conn.execute(
                """
                UPDATE dm_unified_leads
                   SET preferred_account_user_id=COALESCE(
                           preferred_account_user_id, ?
                       ),
                       source_account_user_id=COALESCE(source_account_user_id, ?),
                       dm_task_id=COALESCE(dm_task_id, ?),
                       source_chat_id=COALESCE(source_chat_id, ?),
                       source_chat_title=COALESCE(?, source_chat_title),
                       source_chat_username=COALESCE(?, source_chat_username),
                       source_message_id=COALESCE(?, source_message_id),
                       target_access_hash=COALESCE(?, target_access_hash),
                       target_username=COALESCE(?, target_username),
                       target_first_name=COALESCE(?, target_first_name),
                       target_last_name=COALESCE(?, target_last_name),
                       first_dm_module=COALESCE(first_dm_module, ?),
                       eligible_at=CASE
                         WHEN eligible_at IS NULL OR eligible_at > ? THEN ?
                         ELSE eligible_at
                       END,
                       status=CASE
                         WHEN status IN ('uncertain_delivery','sending','claimed')
                           THEN status
                         WHEN ? IN ('uncertain_delivery','sending','claimed')
                           THEN ?
                         ELSE status
                       END,
                       legacy_pending_id=COALESCE(legacy_pending_id, ?),
                       retry_count=MAX(retry_count, ?),
                       resolve_attempts=MAX(resolve_attempts, ?),
                       last_error=COALESCE(?, last_error),
                       updated_at=?
                 WHERE id=?
                """,
                (
                    int(account_user_id) if account_user_id is not None else None,
                    int(account_user_id) if account_user_id is not None else None,
                    int(dm_task_id) if dm_task_id is not None else None,
                    int(source_chat_id) if source_chat_id is not None else None,
                    _clean(source_chat_title),
                    _clean(source_chat_username),
                    int(source_message_id) if source_message_id is not None else None,
                    int(target_access_hash) if target_access_hash is not None else None,
                    _clean(target_username),
                    _clean(target_first_name),
                    _clean(target_last_name),
                    module,
                    str(eligible_at or now),
                    str(eligible_at or now),
                    mapped_status,
                    mapped_status,
                    pending_id,
                    int(retry_count or 0),
                    int(resolve_attempts or 0),
                    _clean(last_error),
                    now,
                    int(lead_id),
                ),
            )

        if account_user_id is not None:
            preferred_id = conn.execute(
                "SELECT preferred_account_user_id FROM dm_unified_leads WHERE id=?",
                (int(lead_id),),
            ).fetchone()
            is_preferred = (
                preferred_id is not None
                and preferred_id[0] is not None
                and int(preferred_id[0]) == int(account_user_id)
            )
            _attach_lead_account(
                int(lead_id),
                int(account_user_id),
                is_preferred=is_preferred,
                access_hash=int(target_access_hash)
                if target_access_hash is not None
                else None,
                target_username=_clean(target_username),
            )
        _attach_lead_sources(
            int(lead_id),
            pending_id,
            int(account_user_id) if account_user_id is not None else 0,
        )
        return int(lead_id)


def migrate_pending_queue_to_unified_pool() -> dict[str, int]:
    """Idempotent copy of active legacy queue rows into the unified lead pool."""
    ensure_unified_queue_schema()
    rows = conn.execute(
        f"""
        SELECT id FROM dm_pending_queue
         WHERE status IN ({",".join("?" for _ in ACTIVE_LEAD_STATUSES)})
         ORDER BY id
        """,
        ACTIVE_LEAD_STATUSES,
    ).fetchall()
    synced = 0
    failed = 0
    for (pending_id,) in rows:
        try:
            lead_id = shadow_sync_pending_row(int(pending_id))
            if lead_id is not None:
                synced += 1
        except Exception as exc:
            failed += 1
            logger.exception(
                f"[DM unified queue] migration failed pending_id={pending_id}: {exc}"
            )
    active_leads = conn.execute(
        f"""
        SELECT COUNT(*) FROM dm_unified_leads
         WHERE status IN ({",".join("?" for _ in ACTIVE_LEAD_STATUSES)})
        """,
        ACTIVE_LEAD_STATUSES,
    ).fetchone()
    result = {
        "pending_rows_seen": len(rows),
        "pending_rows_synced": synced,
        "pending_rows_failed": failed,
        "active_unified_leads": int(active_leads[0] if active_leads else 0),
    }
    logger.info(f"[DM unified queue] migration result: {result}")
    return result


def count_active_unified_leads() -> int:
    ensure_unified_queue_schema()
    row = conn.execute(
        f"""
        SELECT COUNT(*) FROM dm_unified_leads
         WHERE status IN ({",".join("?" for _ in ACTIVE_LEAD_STATUSES)})
        """,
        ACTIVE_LEAD_STATUSES,
    ).fetchone()
    return int(row[0] if row else 0)



def list_unified_participating_accounts() -> list[dict]:
    """Accounts that can send from the unified pool right now.

    Rule: has an active DM task with a non-empty session_string.
    """
    ensure_unified_queue_schema()
    rows = conn.execute(
        """
        SELECT t.user_id,
               COUNT(DISTINCT t.id) AS task_count,
               COALESCE(d.is_paused, 0) AS is_paused,
               d.pause_reason,
               d.cooldown_until
          FROM dm_tasks AS t
          LEFT JOIN dm_account_dispatch AS d ON d.account_user_id = t.user_id
         WHERE t.is_active = 1
           AND t.session_string IS NOT NULL
           AND TRIM(t.session_string) <> ''
         GROUP BY t.user_id
         ORDER BY t.user_id
        """
    ).fetchall()
    result = []
    for row in rows:
        result.append(
            {
                "account_user_id": int(row[0]),
                "task_count": int(row[1] or 0),
                "is_paused": bool(row[2]),
                "pause_reason": row[3],
                "cooldown_until": row[4],
            }
        )
    return result


def unified_queue_stats() -> dict[str, Any]:
    """Admin-facing snapshot of the unified pool and runtime mode."""
    from services.dm_task_queue import parse_iso

    ensure_unified_queue_schema()
    state = get_queue_runtime_state()
    status_rows = conn.execute(
        """
        SELECT status, COUNT(*)
          FROM dm_unified_leads
         GROUP BY status
         ORDER BY COUNT(*) DESC, status
        """
    ).fetchall()
    by_status = {str(status): int(count) for status, count in status_rows}
    active = sum(by_status.get(s, 0) for s in ACTIVE_LEAD_STATUSES)
    ready = conn.execute(
        f"""
        SELECT COUNT(*) FROM dm_unified_leads
         WHERE status IN ('pending','retry_wait','unresolved_peer')
           AND eligible_at<=?
        """,
        (_iso(utc_now()),),
    ).fetchone()
    reserved = int(by_status.get("reserved", 0))
    sent = int(by_status.get("sent", 0))
    cancelled = int(by_status.get("cancelled", 0))
    accounts = conn.execute(
        """
        SELECT COUNT(DISTINCT account_user_id) FROM dm_unified_lead_accounts
        """
    ).fetchone()
    preferred_paused = 0
    try:
        preferred_paused = conn.execute(
            """
            SELECT COUNT(*) FROM dm_unified_leads AS l
              JOIN dm_account_dispatch AS d
                ON d.account_user_id=l.preferred_account_user_id
             WHERE l.status IN ('pending','retry_wait','unresolved_peer','reserved')
               AND d.is_paused=1
            """
        ).fetchone()[0]
    except Exception:
        preferred_paused = 0
    next_at = parse_iso(state.next_global_send_at)
    now = utc_now()
    next_in = None
    if next_at is not None:
        next_in = max(0, int((next_at - now).total_seconds()))
    return {
        "mode": state.mode,
        "global_spacing_min": state.global_spacing_min,
        "global_spacing_max": state.global_spacing_max,
        "last_global_send_at": state.last_global_send_at,
        "next_global_send_at": state.next_global_send_at,
        "next_global_send_in_seconds": next_in,
        "active_leads": int(active),
        "ready_leads": int(ready[0] if ready else 0),
        "reserved_leads": reserved,
        "sent_leads": sent,
        "cancelled_leads": cancelled,
        "by_status": by_status,
        "eligible_account_links": int(accounts[0] if accounts else 0),
        "preferred_account_paused_leads": int(preferred_paused or 0),
        "updated_at": state.updated_at,
        "updated_by_admin_id": state.updated_by_admin_id,
    }



def safe_shadow_sync_pending_row(pending_id: int) -> None:
    """Best-effort shadow sync: never breaks the live per-account enqueue path."""
    try:
        shadow_sync_pending_row(int(pending_id))
    except Exception as exc:
        logger.warning(
            f"[DM unified queue] shadow sync skipped pending_id={pending_id}: {exc}"
        )


def global_gate_wait_seconds() -> Optional[float]:
    """Seconds until the next global first-DM slot, or 0 if ready.

    Only meaningful in unified mode. Returns None when a global first-DM pause
    is active (same signal shape as account_gate_wait_seconds).
    """
    from services.dm_task_queue import is_global_first_dm_paused, parse_iso

    if is_global_first_dm_paused():
        return None
    if not is_unified_queue_mode():
        return 0.0
    state = get_queue_runtime_state()
    now = utc_now()
    lease_expires_at = parse_iso(state.lease_expires_at)
    if state.lease_token and lease_expires_at is not None and lease_expires_at > now:
        return max(0.0, (lease_expires_at - now).total_seconds())
    next_at = parse_iso(state.next_global_send_at)
    if next_at is None:
        return 0.0
    remaining = (next_at - now).total_seconds()
    return remaining if remaining > 0 else 0.0


def try_claim_global_send_lease(
    account_user_id: int, *, lease_seconds: int = 300
) -> Optional[str]:
    """Atomically reserve the global send window and return its owner token."""
    from services.dm_task_queue import is_global_first_dm_paused

    if is_global_first_dm_paused() or not is_unified_queue_mode():
        return None
    now = utc_now()
    now_iso = _iso(now)
    lease_until = now + dt.timedelta(seconds=max(30, int(lease_seconds)))
    token = uuid.uuid4().hex
    ensure_unified_queue_schema()
    with _db_lock, conn:
        cursor = conn.execute(
            """
            UPDATE dm_queue_runtime
               SET lease_owner_account_user_id=?, lease_token=?,
                   lease_expires_at=?, updated_at=?
             WHERE id=1
               AND (next_global_send_at IS NULL OR next_global_send_at<=?)
               AND (lease_token IS NULL OR lease_expires_at IS NULL OR lease_expires_at<=?)
            """,
            (
                int(account_user_id),
                token,
                _iso(lease_until),
                now_iso,
                now_iso,
                now_iso,
            ),
        )
        return token if int(cursor.rowcount or 0) == 1 else None


def renew_global_send_lease(
    account_user_id: int, lease_token: str, *, lease_seconds: int = 300
) -> bool:
    """Extend a live lease only when the same account and token still own it."""
    now = utc_now()
    with _db_lock, conn:
        cursor = conn.execute(
            """
            UPDATE dm_queue_runtime
               SET lease_expires_at=?, updated_at=?
             WHERE id=1
               AND lease_owner_account_user_id=?
               AND lease_token=?
               AND lease_expires_at>?
            """,
            (
                _iso(now + dt.timedelta(seconds=max(30, int(lease_seconds)))),
                _iso(now),
                int(account_user_id),
                str(lease_token),
                _iso(now),
            ),
        )
        return int(cursor.rowcount or 0) == 1


def complete_global_send_window(account_user_id: int, lease_token: str) -> bool:
    """Apply global spacing only when the caller still owns the lease."""
    import random

    state = get_queue_runtime_state()
    low = max(5, int(state.global_spacing_min or 30))
    high = max(low, int(state.global_spacing_max or 60))
    delay = random.randint(low, high)
    now = utc_now()
    with _db_lock, conn:
        cursor = conn.execute(
            """
            UPDATE dm_queue_runtime
               SET last_global_send_at=?, next_global_send_at=?,
                   lease_owner_account_user_id=NULL, lease_token=NULL,
                   lease_expires_at=NULL, updated_at=?
             WHERE id=1
               AND lease_owner_account_user_id=?
               AND lease_token=?
            """,
            (
                _iso(now),
                _iso(now + dt.timedelta(seconds=delay)),
                _iso(now),
                int(account_user_id),
                str(lease_token),
            ),
        )
        return int(cursor.rowcount or 0) == 1


def release_global_send_lease(
    account_user_id: int, lease_token: str, *, retry_seconds: int = 2
) -> bool:
    """Release only the lease owned by this sender; stale senders cannot clear a new lease."""
    now = utc_now()
    due = now + dt.timedelta(seconds=max(0, int(retry_seconds)))
    with _db_lock, conn:
        cursor = conn.execute(
            """
            UPDATE dm_queue_runtime
               SET next_global_send_at=?, lease_owner_account_user_id=NULL,
                   lease_token=NULL, lease_expires_at=NULL, updated_at=?
             WHERE id=1
               AND lease_owner_account_user_id=?
               AND lease_token=?
            """,
            (
                _iso(due),
                _iso(now),
                int(account_user_id),
                str(lease_token),
            ),
        )
        return int(cursor.rowcount or 0) == 1


def _account_flood_or_paused(account_user_id: int) -> bool:
    from services.dm_task_queue import get_account_dispatch_state, parse_iso

    state = get_account_dispatch_state(int(account_user_id))
    if state.is_paused:
        return True
    cooldown = parse_iso(state.cooldown_until)
    if cooldown is not None and cooldown > utc_now():
        return True
    return False


def _pick_active_task_for_account(
    account_user_id: int,
    preferred_task_id: Optional[int],
) -> Optional[int]:
    if preferred_task_id is not None:
        row = conn.execute(
            """
            SELECT id FROM dm_tasks
             WHERE id=? AND user_id=? AND is_active=1
            """,
            (int(preferred_task_id), int(account_user_id)),
        ).fetchone()
        if row:
            return int(row[0])
    row = conn.execute(
        """
        SELECT id FROM dm_tasks
         WHERE user_id=? AND is_active=1
         ORDER BY id DESC
         LIMIT 1
        """,
        (int(account_user_id),),
    ).fetchone()
    return int(row[0]) if row else None


def _load_pending_row(pending_id: int) -> Optional[dict[str, Any]]:
    row = conn.execute(
        """
        SELECT id, dm_task_id, account_user_id, target_user_id,
               target_access_hash, target_username, target_first_name,
               target_last_name, source_chat_id, source_chat_title,
               source_chat_username, source_message_id,
               enqueued_at, eligible_at, status, retry_count, resolve_attempts
          FROM dm_pending_queue
         WHERE id=?
        """,
        (int(pending_id),),
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
        "source_chat_username",
        "source_message_id",
        "enqueued_at",
        "eligible_at",
        "status",
        "retry_count",
        "resolve_attempts",
    )
    return dict(zip(keys, row))


def _ensure_pending_row_for_account(
    lead: dict[str, Any],
    account_user_id: int,
    task_id: int,
) -> Optional[int]:
    """Create or reuse a legacy pending row owned by the sending account."""
    target_user_id = int(lead["target_user_id"])
    now = _iso(utc_now())
    existing = conn.execute(
        f"""
        SELECT id, status FROM dm_pending_queue
         WHERE account_user_id=? AND target_user_id=?
           AND status IN ({",".join("?" for _ in ACTIVE_LEAD_STATUSES)})
         ORDER BY id
         LIMIT 1
        """,
        (int(account_user_id), target_user_id, *ACTIVE_LEAD_STATUSES),
    ).fetchone()
    if existing:
        pending_id = int(existing[0])
        prev_status = str(existing[1] or "")
        # Once a Telegram request started, delivery may already have happened.
        # Preserve the permanent duplicate guard instead of reopening it.
        if prev_status in {"sending", "uncertain_delivery"}:
            conn.execute(
                """
                UPDATE dm_pending_queue
                   SET status='uncertain_delivery', claim_token=NULL, claimed_at=NULL,
                       last_error=CASE
                         WHEN status='sending' THEN 'unified_send_result_unknown'
                         ELSE last_error
                       END,
                       updated_at=?
                 WHERE id=?
                """,
                (now, pending_id),
            )
            conn.execute(
                """
                UPDATE dm_unified_leads
                   SET status='uncertain_delivery', legacy_pending_id=?,
                       reserved_by_account_user_id=NULL, reserved_at=NULL,
                       reserve_token=NULL,
                       last_error='pending_uncertain_duplicate_guard', updated_at=?
                 WHERE id=?
                """,
                (pending_id, now, int(lead["id"])),
            )
            return None

        # A stale claim that never started Telegram delivery is safe to reopen.
        reopen = prev_status == "claimed"
        conn.execute(
            """
            UPDATE dm_pending_queue
               SET dm_task_id=?,
                   target_access_hash=COALESCE(?, target_access_hash),
                   target_username=COALESCE(?, target_username),
                   target_first_name=COALESCE(?, target_first_name),
                   target_last_name=COALESCE(?, target_last_name),
                   source_chat_id=COALESCE(?, source_chat_id),
                   source_chat_title=COALESCE(?, source_chat_title),
                   source_chat_username=COALESCE(?, source_chat_username),
                   source_message_id=COALESCE(?, source_message_id),
                   eligible_at=CASE
                     WHEN ? = 1 THEN ?
                     WHEN status IN ('pending','retry_wait','unresolved_peer')
                          AND eligible_at > ? THEN ?
                     ELSE eligible_at
                   END,
                   status=CASE
                     WHEN ? = 1 THEN 'pending'
                     ELSE 'pending'
                   END,
                   claim_token=NULL,
                   claimed_at=NULL,
                   send_started_at=NULL,
                   last_error=CASE
                     WHEN ? = 1 THEN 'reopened_for_unified_send'
                     ELSE last_error
                   END,
                   updated_at=?
             WHERE id=?
            """,
            (
                int(task_id),
                lead.get("target_access_hash"),
                lead.get("target_username"),
                lead.get("target_first_name"),
                lead.get("target_last_name"),
                lead.get("source_chat_id"),
                lead.get("source_chat_title"),
                lead.get("source_chat_username"),
                lead.get("source_message_id"),
                1 if reopen else 0,
                now,
                now,
                now,
                1 if reopen else 0,
                1 if reopen else 0,
                now,
                pending_id,
            ),
        )
        return pending_id

    cursor = conn.execute(
        """
        INSERT INTO dm_pending_queue (
            dm_task_id, account_user_id, target_user_id,
            target_access_hash, target_username, target_first_name,
            target_last_name, source_chat_id, source_chat_title,
            source_chat_username, source_message_id,
            enqueued_at, eligible_at, status, retry_count,
            resolve_attempts, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, 0, ?)
        """,
        (
            int(task_id),
            int(account_user_id),
            target_user_id,
            lead.get("target_access_hash"),
            lead.get("target_username"),
            lead.get("target_first_name"),
            lead.get("target_last_name"),
            lead.get("source_chat_id"),
            lead.get("source_chat_title"),
            lead.get("source_chat_username"),
            lead.get("source_message_id"),
            lead.get("enqueued_at") or now,
            now,
            now,
        ),
    )
    pending_id = int(cursor.lastrowid)
    if lead.get("source_chat_id") is not None and lead.get("dm_task_id") is not None:
        conn.execute(
            """
            INSERT OR IGNORE INTO dm_pending_sources (
                pending_id, dm_task_id, source_chat_id, source_chat_title,
                source_chat_username, source_message_id, first_seen_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pending_id,
                int(lead["dm_task_id"]),
                int(lead["source_chat_id"]),
                lead.get("source_chat_title"),
                lead.get("source_chat_username"),
                lead.get("source_message_id"),
                lead.get("enqueued_at") or now,
                now,
            ),
        )
    return pending_id


def _cancel_other_account_pending(target_user_id: int, keep_account_user_id: int) -> int:
    cursor = conn.execute(
        """
        UPDATE dm_pending_queue
           SET status='cancelled',
               last_error='unified_queue_taken_by_other_account',
               claim_token=NULL,
               claimed_at=NULL,
               updated_at=?
         WHERE target_user_id=?
           AND account_user_id<>?
           AND status IN (
                'pending','claimed','retry_wait','unresolved_peer'
           )
        """,
        (_iso(utc_now()), int(target_user_id), int(keep_account_user_id)),
    )
    return int(cursor.rowcount or 0)


def prepare_unified_send_for_account(account_user_id: int) -> Optional[dict[str, Any]]:
    """Reserve a due unified lead and return a legacy pending row for the sender.

    Returns None when unified mode is off, the account cannot send right now,
    or no due lead is available for this account.
    """
    from services.dm_task_queue import (
        ensure_account_settings,
        is_global_first_dm_paused,
    )

    account_user_id = int(account_user_id)
    if not is_unified_queue_mode() or is_global_first_dm_paused():
        return None
    if _account_flood_or_paused(account_user_id):
        return None
    global_lease_token = try_claim_global_send_lease(account_user_id)
    if global_lease_token is None:
        return None

    ensure_unified_queue_schema()
    try:
        recover_stale_pending_claims(older_than_seconds=120)
    except Exception as exc:
        logger.debug(f"[DM unified] stale pending recovery skipped: {exc}")
    now = utc_now()
    now_iso = _iso(now)
    try:
        with _db_lock, conn:
            # Any due lead is claimable by any active account.
            # Prefer: this account as preferred → linked in lead_accounts → others.
            # Old filter (must have username OR account link) blocked most of the
            # pool: UI "ready" counted all due rows, but send found none.
            lead_row = conn.execute(
                """
                SELECT l.id, l.target_user_id, l.preferred_account_user_id,
                       l.source_account_user_id, l.dm_task_id, l.source_chat_id,
                       l.source_chat_title, l.source_chat_username, l.source_message_id,
                       l.target_access_hash, l.target_username, l.target_first_name,
                       l.target_last_name, l.first_dm_module, l.enqueued_at,
                       l.eligible_at, l.status, l.legacy_pending_id
                  FROM dm_unified_leads AS l
                 WHERE l.status IN ('pending','retry_wait','unresolved_peer')
                   AND l.eligible_at<=?
                 ORDER BY
                    CASE
                      WHEN l.preferred_account_user_id=? THEN 0
                      WHEN EXISTS (
                        SELECT 1 FROM dm_unified_lead_accounts AS a
                         WHERE a.lead_id=l.id AND a.account_user_id=?
                      ) THEN 1
                      ELSE 2
                    END,
                    l.eligible_at,
                    l.id
                 LIMIT 1
                """,
                (now_iso, account_user_id, account_user_id),
            ).fetchone()
            if not lead_row:
                # Diagnostic: how many look "ready" but were not selected.
                try:
                    due_count = conn.execute(
                        """
                        SELECT COUNT(*) FROM dm_unified_leads
                         WHERE status IN ('pending','retry_wait','unresolved_peer')
                           AND eligible_at<=?
                        """,
                        (now_iso,),
                    ).fetchone()[0]
                    logger.info(
                        f"[DM unified] no lead for account={account_user_id} "
                        f"due_in_pool={int(due_count or 0)}"
                    )
                except Exception:
                    pass
                release_global_send_lease(account_user_id, global_lease_token, retry_seconds=2)
                return None

            lead = {
                "id": int(lead_row[0]),
                "target_user_id": int(lead_row[1]),
                "preferred_account_user_id": int(lead_row[2]) if lead_row[2] is not None else None,
                "source_account_user_id": int(lead_row[3]) if lead_row[3] is not None else None,
                "dm_task_id": int(lead_row[4]) if lead_row[4] is not None else None,
                "source_chat_id": int(lead_row[5]) if lead_row[5] is not None else None,
                "source_chat_title": lead_row[6],
                "source_chat_username": lead_row[7],
                "source_message_id": int(lead_row[8]) if lead_row[8] is not None else None,
                "target_access_hash": int(lead_row[9]) if lead_row[9] is not None else None,
                "target_username": lead_row[10],
                "target_first_name": lead_row[11],
                "target_last_name": lead_row[12],
                "first_dm_module": lead_row[13],
                "enqueued_at": lead_row[14],
                "eligible_at": lead_row[15],
                "status": lead_row[16],
                "legacy_pending_id": int(lead_row[17]) if lead_row[17] is not None else None,
            }

            # access_hash is account-scoped in Telegram. Never copy a hash learned
            # by another account into the current sender's pending row.
            account_link = conn.execute(
                """
                SELECT access_hash, target_username
                  FROM dm_unified_lead_accounts
                 WHERE lead_id=? AND account_user_id=?
                """,
                (int(lead["id"]), account_user_id),
            ).fetchone()
            if account_link is not None:
                lead["target_access_hash"] = (
                    int(account_link[0]) if account_link[0] is not None else None
                )
                if account_link[1]:
                    lead["target_username"] = account_link[1]
            elif account_user_id not in {
                lead.get("preferred_account_user_id"),
                lead.get("source_account_user_id"),
            }:
                lead["target_access_hash"] = None

            task_id = _pick_active_task_for_account(account_user_id, lead.get("dm_task_id"))
            if task_id is None:
                release_global_send_lease(account_user_id, global_lease_token, retry_seconds=5)
                return None

            # Reserve lead for this account.
            cursor = conn.execute(
                """
                UPDATE dm_unified_leads
                   SET status='reserved',
                       reserved_by_account_user_id=?,
                       reserved_at=?,
                       reserve_token=?,
                       dm_task_id=COALESCE(dm_task_id, ?),
                       updated_at=?
                 WHERE id=? AND status IN ('pending','retry_wait','unresolved_peer')
                """,
                (
                    account_user_id,
                    now_iso,
                    global_lease_token,
                    int(task_id),
                    now_iso,
                    int(lead["id"]),
                ),
            )
            if int(cursor.rowcount or 0) != 1:
                release_global_send_lease(account_user_id, global_lease_token, retry_seconds=1)
                return None

            _attach_lead_account(
                int(lead["id"]),
                account_user_id,
                is_preferred=(lead.get("preferred_account_user_id") == account_user_id),
                access_hash=lead.get("target_access_hash"),
                target_username=lead.get("target_username"),
            )
            _cancel_other_account_pending(int(lead["target_user_id"]), account_user_id)
            pending_id = _ensure_pending_row_for_account(lead, account_user_id, int(task_id))
            if pending_id is None:
                current = conn.execute(
                    "SELECT status FROM dm_unified_leads WHERE id=?",
                    (int(lead["id"]),),
                ).fetchone()
                if not current or str(current[0]) != "uncertain_delivery":
                    conn.execute(
                        """
                        UPDATE dm_unified_leads
                           SET status='pending', reserved_by_account_user_id=NULL,
                               reserved_at=NULL, reserve_token=NULL, updated_at=?
                         WHERE id=?
                        """,
                        (now_iso, int(lead["id"])),
                    )
                release_global_send_lease(
                    account_user_id, global_lease_token, retry_seconds=2
                )
                return None

            conn.execute(
                """
                UPDATE dm_unified_leads
                   SET legacy_pending_id=?, updated_at=?
                 WHERE id=?
                """,
                (int(pending_id), now_iso, int(lead["id"])),
            )
            ensure_account_settings(account_user_id)
            row = _load_pending_row(int(pending_id))
            if row is None:
                release_global_send_lease(account_user_id, global_lease_token, retry_seconds=2)
                return None
            row["_unified_lead_id"] = int(lead["id"])
            row["_global_lease_token"] = global_lease_token
            return row
    except Exception:
        release_global_send_lease(account_user_id, global_lease_token, retry_seconds=2)
        raise


def mark_unified_lead_sent(pending_id: int, account_user_id: int) -> None:
    """Mark the lead sent and drop the same user from every other queue/pool row."""
    now = _iso(utc_now())
    pending_id = int(pending_id)
    account_user_id = int(account_user_id)
    target_user_id: int | None = None
    with _db_lock, conn:
        row = conn.execute(
            "SELECT target_user_id FROM dm_pending_queue WHERE id=?",
            (pending_id,),
        ).fetchone()
        if row:
            target_user_id = int(row[0])
        if target_user_id is None:
            row = conn.execute(
                """
                SELECT target_user_id FROM dm_unified_leads
                 WHERE legacy_pending_id=? OR id=?
                 LIMIT 1
                """,
                (pending_id, pending_id),
            ).fetchone()
            if row:
                target_user_id = int(row[0])
        conn.execute(
            """
            UPDATE dm_unified_leads
               SET status='sent',
                   sent_at=?,
                   sent_by_account_user_id=?,
                   reserved_by_account_user_id=NULL,
                   reserved_at=NULL,
                   reserve_token=NULL,
                   legacy_pending_id=COALESCE(legacy_pending_id, ?),
                   updated_at=?
             WHERE legacy_pending_id=?
                OR (
                    ? IS NOT NULL
                    AND target_user_id=?
                    AND status IN (
                        'reserved','claimed','sending','pending',
                        'retry_wait','unresolved_peer'
                    )
                )
            """,
            (
                now,
                account_user_id,
                pending_id,
                now,
                pending_id,
                target_user_id,
                target_user_id,
            ),
        )
        if target_user_id is not None:
            # Any other unified leads for the same person become cancelled.
            conn.execute(
                """
                UPDATE dm_unified_leads
                   SET status='cancelled',
                       last_error='sent_by_another_account',
                       reserved_by_account_user_id=NULL,
                       reserved_at=NULL,
                       reserve_token=NULL,
                       updated_at=?
                 WHERE target_user_id=?
                   AND status NOT IN ('sent','cancelled')
                """,
                (now, target_user_id),
            )
    if target_user_id is not None:
        try:
            from services.dm_task_queue import cancel_target_globally

            cancel_target_globally(
                target_user_id,
                "sent_via_unified_queue",
                except_pending_id=pending_id,
            )
        except Exception as exc:
            logger.warning(
                f"[DM unified] global cancel after send failed "
                f"target={target_user_id}: {exc}"
            )


def release_unified_lead_for_pending(
    pending_id: int,
    *,
    status: str = "pending",
    error: Optional[str] = None,
    retry_seconds: Optional[int] = None,
) -> None:
    """Return a reserved lead to the pool so another account may take it."""
    now = utc_now()
    now_iso = _iso(now)
    due_iso = (
        _iso(now + dt.timedelta(seconds=max(1, int(retry_seconds))))
        if retry_seconds is not None
        else None
    )
    mapped = _map_pending_status(status)
    with _db_lock, conn:
        conn.execute(
            f"""
            UPDATE dm_unified_leads
               SET status=?,
                   eligible_at=COALESCE(?, eligible_at),
                   reserved_by_account_user_id=NULL,
                   reserved_at=NULL,
                   reserve_token=NULL,
                   retry_count=retry_count + CASE
                     WHEN ? IN ('pending','retry_wait','unresolved_peer') THEN 1 ELSE 0
                   END,
                   last_error=COALESCE(?, last_error),
                   updated_at=?
             WHERE legacy_pending_id=?
                OR (
                    status='reserved'
                    AND target_user_id=(
                        SELECT target_user_id FROM dm_pending_queue WHERE id=?
                    )
                )
            """,
            (
                mapped if mapped in ACTIVE_LEAD_STATUSES or mapped == "cancelled" else "pending",
                due_iso,
                mapped,
                _clean(error),
                now_iso,
                int(pending_id),
                int(pending_id),
            ),
        )



def recover_stale_pending_claims(*, older_than_seconds: int = 180) -> int:
    """Recover stale claims without ever retrying an unknown Telegram send."""
    ensure_unified_queue_schema()
    cutoff = _iso(utc_now() - dt.timedelta(seconds=max(30, int(older_than_seconds))))
    now = _iso(utc_now())
    with _db_lock, conn:
        claimed = conn.execute(
            """
            UPDATE dm_pending_queue
               SET status='pending', claim_token=NULL, claimed_at=NULL,
                   send_started_at=NULL, last_error='stale_claim_reopened', updated_at=?
             WHERE status='claimed'
               AND COALESCE(claimed_at, updated_at) < ?
               AND NOT EXISTS (
                 SELECT 1
                   FROM dm_unified_leads AS l
                   JOIN dm_queue_runtime AS r ON r.id=1
                  WHERE l.legacy_pending_id=dm_pending_queue.id
                    AND l.status='reserved'
                    AND l.reserve_token=r.lease_token
                    AND l.reserved_by_account_user_id=r.lease_owner_account_user_id
                    AND r.lease_expires_at>?
               )
            """,
            (now, cutoff, now),
        ).rowcount
        sending = conn.execute(
            """
            UPDATE dm_pending_queue
               SET status='uncertain_delivery', claim_token=NULL, claimed_at=NULL,
                   last_error='stale_send_marked_uncertain', updated_at=?
             WHERE status='sending'
               AND COALESCE(send_started_at, claimed_at, updated_at) < ?
               AND NOT EXISTS (
                 SELECT 1
                   FROM dm_unified_leads AS l
                   JOIN dm_queue_runtime AS r ON r.id=1
                  WHERE l.legacy_pending_id=dm_pending_queue.id
                    AND l.status='reserved'
                    AND l.reserve_token=r.lease_token
                    AND l.reserved_by_account_user_id=r.lease_owner_account_user_id
                    AND r.lease_expires_at>?
               )
            """,
            (now, cutoff, now),
        ).rowcount
        return int(claimed or 0) + int(sending or 0)


def recover_stale_unified_reservations(*, max_age_seconds: int = 180) -> int:
    """Recover abandoned reservations while preserving active leases and send guards."""
    now = utc_now()
    now_iso = _iso(now)
    cutoff = _iso(now - dt.timedelta(seconds=max(60, int(max_age_seconds))))
    with _db_lock, conn:
        cursor = conn.execute(
            """
            UPDATE dm_unified_leads
               SET status=CASE
                     WHEN EXISTS (
                       SELECT 1 FROM dm_pending_queue AS q
                        WHERE q.id=dm_unified_leads.legacy_pending_id
                          AND q.status IN ('sending','uncertain_delivery')
                     ) THEN 'uncertain_delivery'
                     ELSE 'pending'
                   END,
                   reserved_by_account_user_id=NULL,
                   reserved_at=NULL,
                   reserve_token=NULL,
                   last_error=CASE
                     WHEN EXISTS (
                       SELECT 1 FROM dm_pending_queue AS q
                        WHERE q.id=dm_unified_leads.legacy_pending_id
                          AND q.status IN ('sending','uncertain_delivery')
                     ) THEN 'stale_reservation_preserved_uncertain'
                     ELSE 'stale_unified_reservation_recovered'
                   END,
                   updated_at=?
             WHERE status='reserved'
               AND COALESCE(reserved_at, updated_at) < ?
               AND NOT EXISTS (
                 SELECT 1 FROM dm_queue_runtime AS r
                  WHERE r.id=1
                    AND r.lease_token=dm_unified_leads.reserve_token
                    AND r.lease_owner_account_user_id=dm_unified_leads.reserved_by_account_user_id
                    AND r.lease_expires_at>?
               )
            """,
            (now_iso, cutoff, now_iso),
        )
        return int(cursor.rowcount or 0)


def list_active_unified_lead_ids() -> list[int]:
    """Stable ordered IDs of unfinished unified leads for admin browsing."""
    ensure_unified_queue_schema()
    rows = conn.execute(
        f"""
        SELECT id FROM dm_unified_leads
         WHERE status IN ({",".join("?" for _ in ACTIVE_LEAD_STATUSES)})
         ORDER BY
            CASE status
              WHEN 'pending' THEN 0
              WHEN 'retry_wait' THEN 1
              WHEN 'unresolved_peer' THEN 2
              WHEN 'reserved' THEN 3
              WHEN 'claimed' THEN 4
              WHEN 'sending' THEN 5
              WHEN 'uncertain_delivery' THEN 6
              ELSE 9
            END,
            eligible_at,
            id
        """,
        ACTIVE_LEAD_STATUSES,
    ).fetchall()
    return [int(row[0]) for row in rows]


def list_unified_lead_rows_by_ids(lead_ids: list[int]) -> list[dict[str, Any]]:
    """Load lead rows in the same order as ``lead_ids``."""
    if not lead_ids:
        return []
    ensure_unified_queue_schema()
    placeholders = ",".join("?" for _ in lead_ids)
    rows = conn.execute(
        f"""
        SELECT id, target_user_id, preferred_account_user_id, source_account_user_id,
               dm_task_id, source_chat_id, source_chat_title, source_chat_username,
               source_message_id, target_access_hash, target_username,
               target_first_name, target_last_name, first_dm_module,
               enqueued_at, eligible_at, status, reserved_by_account_user_id,
               reserved_at, sent_at, sent_by_account_user_id, legacy_pending_id,
               retry_count, last_error, updated_at
          FROM dm_unified_leads
         WHERE id IN ({placeholders})
        """,
        [int(x) for x in lead_ids],
    ).fetchall()
    keys = (
        "id",
        "target_user_id",
        "preferred_account_user_id",
        "source_account_user_id",
        "dm_task_id",
        "source_chat_id",
        "source_chat_title",
        "source_chat_username",
        "source_message_id",
        "target_access_hash",
        "target_username",
        "target_first_name",
        "target_last_name",
        "first_dm_module",
        "enqueued_at",
        "eligible_at",
        "status",
        "reserved_by_account_user_id",
        "reserved_at",
        "sent_at",
        "sent_by_account_user_id",
        "legacy_pending_id",
        "retry_count",
        "last_error",
        "updated_at",
    )
    by_id = {int(row[0]): dict(zip(keys, row)) for row in rows}
    ordered: list[dict[str, Any]] = []
    for lead_id in lead_ids:
        row = by_id.get(int(lead_id))
        if row is not None:
            ordered.append(row)
    return ordered


__all__ = [
    "MODE_PER_ACCOUNT",
    "MODE_UNIFIED",
    "QueueRuntimeState",
    "complete_global_send_window",
    "count_active_unified_leads",
    "unified_queue_stats",
    "list_unified_participating_accounts",
    "ensure_unified_queue_schema",
    "get_queue_runtime_state",
    "global_gate_wait_seconds",
    "is_unified_queue_mode",
    "list_active_unified_lead_ids",
    "list_unified_lead_rows_by_ids",
    "mark_unified_lead_sent",
    "migrate_pending_queue_to_unified_pool",
    "prepare_unified_send_for_account",
    "recover_stale_unified_reservations",
    "release_global_send_lease",
    "renew_global_send_lease",
    "release_unified_lead_for_pending",
    "safe_shadow_sync_pending_row",
    "set_global_spacing",
    "set_queue_mode",
    "shadow_sync_pending_row",
    "try_claim_global_send_lease",
]
