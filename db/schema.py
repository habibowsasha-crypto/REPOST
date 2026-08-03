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
