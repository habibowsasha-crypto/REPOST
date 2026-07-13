import sqlite3

from loguru import logger

from config import conn


def _normalize_group_id_value(value) -> int:
    number = int(value)
    absolute = abs(number)
    digits = str(absolute)
    if digits.startswith("100") and (number <= -1_000_000_000_000 or absolute >= 1_000_000_000_000):
        return int(digits[3:])
    return absolute


def _normalize_table_group_ids(table: str, column: str) -> None:
    """Migrate old Bot API-style ids to Telethon's canonical positive ids."""
    cursor = conn.cursor()
    try:
        rows = cursor.execute(f"SELECT rowid, {column} FROM {table}").fetchall()
        for rowid, raw_value in rows:
            if raw_value is None:
                continue
            canonical = _normalize_group_id_value(raw_value)
            if canonical == raw_value:
                continue
            try:
                cursor.execute(
                    f"UPDATE {table} SET {column} = ? WHERE rowid = ?",
                    (canonical, rowid),
                )
            except sqlite3.IntegrityError:
                # A canonical duplicate already exists.  Keep the canonical row.
                cursor.execute(f"DELETE FROM {table} WHERE rowid = ?", (rowid,))
        conn.commit()
    finally:
        cursor.close()


def _column_exists(table: str, column: str) -> bool:
    cursor = conn.cursor()
    try:
        return any(row[1] == column for row in cursor.execute(f"PRAGMA table_info({table})"))
    finally:
        cursor.close()


def _add_column_if_missing(table: str, column: str, ddl: str) -> None:
    if _column_exists(table, column):
        return
    try:
        conn.execute(ddl)
        conn.commit()
    except sqlite3.OperationalError as exc:
        logger.warning(f"Не удалось выполнить миграцию {table}.{column}: {exc}")


def create_table() -> None:
    """Create and migrate the core SQLite schema without deleting user data."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS pre_groups (
                group_id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_username TEXT UNIQUE
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS groups (
                group_id INTEGER,
                group_username TEXT,
                user_id INTEGER
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                user_id INTEGER PRIMARY KEY,
                session_string TEXT NOT NULL
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS discovered_groups (
                user_id INTEGER NOT NULL,
                group_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                username TEXT,
                access_hash INTEGER,
                peer_type TEXT NOT NULL,
                is_admin INTEGER DEFAULT 0,
                is_creator INTEGER DEFAULT 0,
                is_available INTEGER DEFAULT 1,
                is_enabled INTEGER DEFAULT 1,
                last_seen_at TEXT,
                PRIMARY KEY (user_id, group_id)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS broadcasts (
                user_id INTEGER,
                group_id INTEGER,
                session_string TEXT,
                broadcast_text TEXT,
                interval_minutes INTEGER,
                is_active BOOLEAN,
                error_reason TEXT,
                photo_url TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS send_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                group_id INTEGER,
                group_name TEXT,
                sent_at TEXT,
                message_text TEXT
            )
            """
        )

        # Normalize old -100... / abs(-100...) identifiers before creating
        # uniqueness constraints.
        conn.commit()
        cursor.close()
        for table_name in ("pre_groups", "groups", "broadcasts", "send_history"):
            _normalize_table_group_ids(table_name, "group_id")
        cursor = conn.cursor()

        # Existing installations may contain duplicate account/group links.
        cursor.execute(
            """
            DELETE FROM groups
            WHERE rowid NOT IN (
                SELECT MIN(rowid) FROM groups GROUP BY user_id, group_id
            )
            """
        )
        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_groups_user_group
            ON groups(user_id, group_id)
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_groups_user
            ON groups(user_id)
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_discovered_groups_user_available
            ON discovered_groups(user_id, is_available)
            """
        )
        cursor.execute(
            """
            DELETE FROM broadcasts
            WHERE rowid NOT IN (
                SELECT MAX(rowid) FROM broadcasts GROUP BY user_id, group_id
            )
            """
        )
        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_broadcasts_user_group
            ON broadcasts(user_id, group_id)
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_broadcasts_user_active
            ON broadcasts(user_id, is_active)
            """
        )
        conn.commit()
    finally:
        cursor.close()

    _add_column_if_missing(
        "broadcasts", "error_reason", "ALTER TABLE broadcasts ADD COLUMN error_reason TEXT"
    )
    _add_column_if_missing(
        "broadcasts", "photo_url", "ALTER TABLE broadcasts ADD COLUMN photo_url TEXT"
    )
    _add_column_if_missing(
        "discovered_groups",
        "is_enabled",
        "ALTER TABLE discovered_groups ADD COLUMN is_enabled INTEGER DEFAULT 1",
    )


def delete_table() -> None:
    """Mark ordinary broadcast jobs inactive during process shutdown/startup."""
    with conn:
        conn.execute(
            "UPDATE broadcasts SET is_active = ? WHERE is_active = ?",
            (False, True),
        )


def create_dm_tables() -> None:
    """Create and migrate DM tables."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS dm_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                session_string TEXT NOT NULL,
                post_text TEXT NOT NULL,
                photo_url TEXT,
                interval_minutes INTEGER NOT NULL,
                is_active BOOLEAN DEFAULT 1,
                created_at TEXT,
                delay_min INTEGER DEFAULT 30,
                delay_max INTEGER DEFAULT 90
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS dm_sent_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dm_task_id INTEGER NOT NULL,
                target_user_id INTEGER NOT NULL,
                sent_at TEXT NOT NULL,
                status TEXT DEFAULT 'sent'
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS dm_watched_chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dm_task_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        cursor.close()

    _add_column_if_missing(
        "dm_tasks", "delay_min", "ALTER TABLE dm_tasks ADD COLUMN delay_min INTEGER DEFAULT 30"
    )
    _add_column_if_missing(
        "dm_tasks", "delay_max", "ALTER TABLE dm_tasks ADD COLUMN delay_max INTEGER DEFAULT 90"
    )
    _add_column_if_missing(
        "dm_sent_log", "status", "ALTER TABLE dm_sent_log ADD COLUMN status TEXT DEFAULT 'sent'"
    )

    _normalize_table_group_ids("dm_watched_chats", "chat_id")

    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            DELETE FROM dm_watched_chats
            WHERE rowid NOT IN (
                SELECT MIN(rowid) FROM dm_watched_chats GROUP BY dm_task_id, chat_id
            )
            """
        )
        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_dm_watched_task_chat
            ON dm_watched_chats(dm_task_id, chat_id)
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_dm_tasks_active
            ON dm_tasks(is_active)
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_dm_sent_lookup
            ON dm_sent_log(dm_task_id, target_user_id, status, sent_at)
            """
        )
        conn.commit()
    finally:
        cursor.close()
