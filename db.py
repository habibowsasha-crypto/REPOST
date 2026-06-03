import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Iterable, Optional, Dict, Any, List, Tuple

from config import settings


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(settings.db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def db():
    conn = connect()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                user_id INTEGER PRIMARY KEY,
                phone TEXT,
                display_name TEXT,
                username TEXT,
                session_string TEXT NOT NULL,
                added_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                username TEXT,
                type TEXT NOT NULL,
                allowed INTEGER NOT NULL DEFAULT 0,
                added_at TEXT NOT NULL,
                UNIQUE(account_user_id, chat_id)
            );

            CREATE TABLE IF NOT EXISTS broadcasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_user_id INTEGER NOT NULL,
                text TEXT,
                media_path TEXT,
                mode TEXT NOT NULL,
                interval_minutes INTEGER,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                stopped_at TEXT,
                last_sent_at TEXT,
                error_reason TEXT
            );

            CREATE TABLE IF NOT EXISTS broadcast_targets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                broadcast_id INTEGER NOT NULL,
                chat_db_id INTEGER NOT NULL,
                UNIQUE(broadcast_id, chat_db_id)
            );

            CREATE TABLE IF NOT EXISTS send_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                broadcast_id INTEGER,
                account_user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                chat_title TEXT NOT NULL,
                sent_at TEXT NOT NULL,
                status TEXT NOT NULL,
                message_preview TEXT,
                error_reason TEXT
            );
            """
        )


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def upsert_account(user_id: int, phone: str, display_name: str, username: Optional[str], session_string: str) -> None:
    with db() as conn:
        conn.execute(
            """
            INSERT INTO accounts (user_id, phone, display_name, username, session_string, added_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                phone=excluded.phone,
                display_name=excluded.display_name,
                username=excluded.username,
                session_string=excluded.session_string
            """,
            (user_id, phone, display_name, username, session_string, now_iso()),
        )


def get_accounts() -> List[sqlite3.Row]:
    with db() as conn:
        return list(conn.execute("SELECT * FROM accounts ORDER BY added_at DESC"))


def get_account(user_id: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute("SELECT * FROM accounts WHERE user_id = ?", (user_id,)).fetchone()


def delete_account(user_id: int) -> None:
    with db() as conn:
        conn.execute("DELETE FROM broadcast_targets WHERE broadcast_id IN (SELECT id FROM broadcasts WHERE account_user_id = ?)", (user_id,))
        conn.execute("DELETE FROM broadcasts WHERE account_user_id = ?", (user_id,))
        conn.execute("DELETE FROM chats WHERE account_user_id = ?", (user_id,))
        conn.execute("DELETE FROM accounts WHERE user_id = ?", (user_id,))


def upsert_chat(account_user_id: int, chat_id: int, title: str, username: Optional[str], chat_type: str, allowed: int = 0) -> None:
    with db() as conn:
        conn.execute(
            """
            INSERT INTO chats (account_user_id, chat_id, title, username, type, allowed, added_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_user_id, chat_id) DO UPDATE SET
                title=excluded.title,
                username=excluded.username,
                type=excluded.type
            """,
            (account_user_id, chat_id, title, username, chat_type, allowed, now_iso()),
        )


def list_chats(account_user_id: int, allowed: Optional[int] = None) -> List[sqlite3.Row]:
    query = "SELECT * FROM chats WHERE account_user_id = ?"
    params: Tuple[Any, ...] = (account_user_id,)
    if allowed is not None:
        query += " AND allowed = ?"
        params = (account_user_id, allowed)
    query += " ORDER BY allowed DESC, title COLLATE NOCASE"
    with db() as conn:
        return list(conn.execute(query, params))


def get_chat(chat_db_id: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute("SELECT * FROM chats WHERE id = ?", (chat_db_id,)).fetchone()


def set_chat_allowed(chat_db_id: int, allowed: int) -> None:
    with db() as conn:
        conn.execute("UPDATE chats SET allowed = ? WHERE id = ?", (allowed, chat_db_id))


def create_broadcast(account_user_id: int, text: Optional[str], media_path: Optional[str], mode: str, interval_minutes: Optional[int], chat_db_ids: Iterable[int], status: str) -> int:
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO broadcasts (account_user_id, text, media_path, mode, interval_minutes, status, created_at, started_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (account_user_id, text, media_path, mode, interval_minutes, status, now_iso(), now_iso() if status == "active" else None),
        )
        broadcast_id = int(cur.lastrowid)
        for chat_db_id in chat_db_ids:
            conn.execute("INSERT OR IGNORE INTO broadcast_targets (broadcast_id, chat_db_id) VALUES (?, ?)", (broadcast_id, chat_db_id))
        return broadcast_id


def get_broadcast(broadcast_id: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute("SELECT * FROM broadcasts WHERE id = ?", (broadcast_id,)).fetchone()


def get_broadcast_targets(broadcast_id: int) -> List[sqlite3.Row]:
    with db() as conn:
        return list(conn.execute(
            """
            SELECT c.* FROM broadcast_targets bt
            JOIN chats c ON c.id = bt.chat_db_id
            WHERE bt.broadcast_id = ?
            ORDER BY c.title COLLATE NOCASE
            """,
            (broadcast_id,),
        ))


def active_broadcasts() -> List[sqlite3.Row]:
    with db() as conn:
        return list(conn.execute("SELECT * FROM broadcasts WHERE status = 'active' AND mode = 'recurring'"))


def mark_broadcast_status(broadcast_id: int, status: str, error_reason: Optional[str] = None) -> None:
    with db() as conn:
        conn.execute(
            """
            UPDATE broadcasts
            SET status = ?, stopped_at = CASE WHEN ? != 'active' THEN ? ELSE stopped_at END, error_reason = ?
            WHERE id = ?
            """,
            (status, status, now_iso(), error_reason, broadcast_id),
        )


def touch_broadcast_sent(broadcast_id: int) -> None:
    with db() as conn:
        conn.execute("UPDATE broadcasts SET last_sent_at = ?, error_reason = NULL WHERE id = ?", (now_iso(), broadcast_id))


def add_history(broadcast_id: Optional[int], account_user_id: int, chat_id: int, chat_title: str, status: str, preview: Optional[str], error_reason: Optional[str] = None) -> None:
    with db() as conn:
        conn.execute(
            """
            INSERT INTO send_history (broadcast_id, account_user_id, chat_id, chat_title, sent_at, status, message_preview, error_reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (broadcast_id, account_user_id, chat_id, chat_title, now_iso(), status, preview, error_reason),
        )


def latest_history(limit: int = 15) -> List[sqlite3.Row]:
    with db() as conn:
        return list(conn.execute("SELECT * FROM send_history ORDER BY sent_at DESC LIMIT ?", (limit,)))
