"""SQLite connection and schema."""

from __future__ import annotations

import sqlite3
import threading
from typing import Optional

from config import DB_PATH

_lock = threading.RLock()
_conn: Optional[sqlite3.Connection] = None


def get_connection() -> sqlite3.Connection:
    """Return a process-wide SQLite connection (check_same_thread=False)."""
    global _conn
    with _lock:
        if _conn is None:
            _conn = sqlite3.connect(DB_PATH, timeout=30.0, check_same_thread=False)
            _conn.row_factory = sqlite3.Row
            _conn.execute("PRAGMA busy_timeout = 30000")
            try:
                _conn.execute("PRAGMA journal_mode = WAL")
            except sqlite3.Error:
                pass
        return _conn


def db_lock() -> threading.RLock:
    return _lock


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    names = {str(r[1]) for r in rows}
    if column not in names:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def init_db() -> None:
    """Create tables required from Step 5 onward."""
    conn = get_connection()
    with _lock, conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runtime_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                applied_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                user_id INTEGER PRIMARY KEY,
                session_string TEXT NOT NULL,
                phone TEXT,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                participates INTEGER NOT NULL DEFAULT 0,
                is_paused INTEGER NOT NULL DEFAULT 0,
                cooldown_until TEXT,
                pause_reason TEXT,
                last_send_at TEXT,
                chat_mode TEXT NOT NULL DEFAULT 'manual',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        _ensure_column(
            conn, "accounts", "chat_mode", "chat_mode TEXT NOT NULL DEFAULT 'manual'"
        )
        _ensure_column(
            conn, "accounts", "daily_sent_count", "daily_sent_count INTEGER NOT NULL DEFAULT 0"
        )
        _ensure_column(
            conn, "accounts", "daily_sent_date", "daily_sent_date TEXT"
        )
        _ensure_column(
            conn, "accounts", "next_send_at", "next_send_at TEXT"
        )
        _ensure_column(
            conn, "accounts", "dm_interval_min_sec", "dm_interval_min_sec INTEGER"
        )
        _ensure_column(
            conn, "accounts", "dm_interval_max_sec", "dm_interval_max_sec INTEGER"
        )
        _ensure_column(
            conn, "accounts", "peerflood_streak", "peerflood_streak INTEGER NOT NULL DEFAULT 0"
        )
        _ensure_column(
            conn, "accounts", "peerflood_last_at", "peerflood_last_at TEXT"
        )
        _ensure_column(
            conn, "accounts", "interval_backup_min", "interval_backup_min INTEGER"
        )
        _ensure_column(
            conn, "accounts", "interval_backup_max", "interval_backup_max INTEGER"
        )
        _ensure_column(
            conn, "accounts", "interval_backoff_until", "interval_backoff_until TEXT"
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_accounts_participates
            ON accounts(participates)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS account_discovered_chats (
                account_user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                title TEXT,
                username TEXT,
                peer_type TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (account_user_id, chat_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS account_selected_chats (
                account_user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                PRIMARY KEY (account_user_id, chat_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS account_excluded_chats (
                account_user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                PRIMARY KEY (account_user_id, chat_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS opt_out (
                user_id INTEGER PRIMARY KEY,
                reason TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS contacts (
                target_user_id INTEGER PRIMARY KEY,
                status TEXT NOT NULL,
                sender_account_id INTEGER,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS leads (
                target_user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                source_chat_id INTEGER,
                source_account_user_id INTEGER,
                status TEXT NOT NULL DEFAULT 'pending',
                eligible_at TEXT,
                claimed_by_account INTEGER,
                claimed_at TEXT,
                last_seen_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_leads_status_eligible
            ON leads(status, eligible_at)
            """
        )
        _ensure_column(
            conn, "leads", "send_attempts", "send_attempts INTEGER NOT NULL DEFAULT 0"
        )
        _ensure_column(conn, "leads", "access_hash", "access_hash INTEGER")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS spambot_state (
                account_user_id INTEGER PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'idle',
                last_reply TEXT,
                next_check_at TEXT,
                limited_until TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sent_phrases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                text TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_sent_phrases_kind
            ON sent_phrases(kind, id DESC)
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dialogs (
                target_user_id INTEGER PRIMARY KEY,
                account_user_id INTEGER NOT NULL,
                stage TEXT NOT NULL,
                outgoing_count INTEGER NOT NULL DEFAULT 0,
                link_sent INTEGER NOT NULL DEFAULT 0,
                auto_link_at TEXT,
                history_json TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        _ensure_column(conn, "dialogs", "first_dm_at", "first_dm_at TEXT")
        _ensure_column(
            conn, "dialogs", "first_dm_message_id", "first_dm_message_id INTEGER"
        )
        _ensure_column(
            conn, "dialogs", "telegram_delete_at", "telegram_delete_at TEXT"
        )
        _ensure_column(
            conn, "dialogs", "telegram_deleted_at", "telegram_deleted_at TEXT"
        )
        _ensure_column(
            conn,
            "dialogs",
            "telegram_delete_next_attempt_at",
            "telegram_delete_next_attempt_at TEXT",
        )
        _ensure_column(
            conn,
            "dialogs",
            "telegram_delete_attempts",
            "telegram_delete_attempts INTEGER NOT NULL DEFAULT 0",
        )
        _ensure_column(
            conn,
            "dialogs",
            "telegram_delete_last_error",
            "telegram_delete_last_error TEXT",
        )
        _ensure_column(
            conn, "dialogs", "history_purge_at", "history_purge_at TEXT"
        )
        _ensure_column(
            conn, "dialogs", "history_purged_at", "history_purged_at TEXT"
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dialog_outbox (
                target_user_id INTEGER NOT NULL,
                action_kind TEXT NOT NULL,
                account_user_id INTEGER NOT NULL,
                text TEXT NOT NULL,
                status TEXT NOT NULL,
                prepared_at TEXT NOT NULL,
                telegram_message_id INTEGER,
                sent_at TEXT,
                last_error TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (target_user_id, action_kind)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_dialog_outbox_status_prepared
            ON dialog_outbox(status, prepared_at)
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS first_dm_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_key TEXT NOT NULL UNIQUE,
                target_user_id INTEGER NOT NULL,
                account_user_id INTEGER,
                telegram_message_id INTEGER,
                sent_at TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_first_dm_events_sent_at
            ON first_dm_events(sent_at)
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS first_dm_outbox (
                target_user_id INTEGER PRIMARY KEY,
                account_user_id INTEGER NOT NULL,
                text TEXT NOT NULL,
                status TEXT NOT NULL,
                prepared_at TEXT NOT NULL,
                telegram_message_id INTEGER,
                sent_at TEXT,
                last_error TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_first_dm_outbox_status_prepared
            ON first_dm_outbox(status, prepared_at)
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS audience (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                source TEXT NOT NULL DEFAULT 'dm',
                source_chat_id INTEGER,
                first_seen_at TEXT NOT NULL,
                first_dm_at TEXT,
                last_touch_at TEXT NOT NULL,
                notes TEXT
            )
            """
        )
        _ensure_column(conn, "audience", "access_hash", "access_hash INTEGER")
        _ensure_column(
            conn,
            "audience",
            "source_account_user_id",
            "source_account_user_id INTEGER",
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_audience_last_touch
            ON audience(last_touch_at DESC)
            """
        )

        # Backfill a durable baseline for the all-time First-DM counter.
        conn.execute(
            """
            INSERT OR IGNORE INTO first_dm_events (
                event_key, target_user_id, account_user_id, telegram_message_id,
                sent_at, created_at
            )
            SELECT 'legacy:' || a.user_id, a.user_id, a.source_account_user_id,
                   o.telegram_message_id, a.first_dm_at, a.first_dm_at
              FROM audience a
              LEFT JOIN first_dm_outbox o ON o.target_user_id=a.user_id
             WHERE a.first_dm_at IS NOT NULL
               AND NOT EXISTS (
                   SELECT 1 FROM first_dm_events e
                    WHERE e.target_user_id=a.user_id
               )
            """
        )

        # Backfill retention timestamps for dialogs created by v1.0.47.
        rows = conn.execute(
            """
            SELECT d.target_user_id, d.first_dm_at, d.first_dm_message_id,
                   o.sent_at, o.telegram_message_id, a.first_dm_at AS audience_dm_at
              FROM dialogs d
              LEFT JOIN first_dm_outbox o ON o.target_user_id=d.target_user_id
              LEFT JOIN audience a ON a.user_id=d.target_user_id
             WHERE d.first_dm_at IS NULL OR d.history_purge_at IS NULL
                OR d.telegram_delete_at IS NULL
            """
        ).fetchall()
        if rows:
            import datetime as _dt
            from config import (
                LOCAL_DIALOG_TEXT_RETENTION_DAYS,
                TELEGRAM_DIALOG_DELETE_DAYS,
            )

            for row in rows:
                raw = row["first_dm_at"] or row["sent_at"] or row["audience_dm_at"]
                if not raw:
                    continue
                try:
                    base = _dt.datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
                    if base.tzinfo is None:
                        base = base.replace(tzinfo=_dt.timezone.utc)
                except (TypeError, ValueError):
                    continue
                conn.execute(
                    """
                    UPDATE dialogs
                       SET first_dm_at=COALESCE(first_dm_at, ?),
                           first_dm_message_id=COALESCE(first_dm_message_id, ?),
                           telegram_delete_at=COALESCE(telegram_delete_at, ?),
                           history_purge_at=COALESCE(history_purge_at, ?)
                     WHERE target_user_id=?
                    """,
                    (
                        base.isoformat(),
                        row["telegram_message_id"],
                        (base + _dt.timedelta(days=TELEGRAM_DIALOG_DELETE_DAYS)).isoformat(),
                        (base + _dt.timedelta(days=LOCAL_DIALOG_TEXT_RETENTION_DAYS)).isoformat(),
                        int(row["target_user_id"]),
                    ),
                )
