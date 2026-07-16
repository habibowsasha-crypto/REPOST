import sqlite3

from loguru import logger

from config import conn
from services.dm_opt_out import create_opt_out_table
from services.dm_contact_analytics import create_contact_tables


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


def _rebuild_legacy_dm_pending_queue() -> None:
    """Remove the v1.0.21 task+target UNIQUE constraint without losing rows."""
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='dm_pending_queue'"
    ).fetchone()
    schema_sql = str(row[0] or "") if row else ""
    compact = "".join(schema_sql.lower().split())
    if "unique(dm_task_id,target_user_id)" not in compact:
        return

    columns = {
        str(info[1]) for info in conn.execute("PRAGMA table_info(dm_pending_queue)")
    }
    legacy = "dm_pending_queue_legacy_v122b"

    def source(column: str, fallback: str) -> str:
        return column if column in columns else fallback

    with conn:
        conn.execute(f"DROP TABLE IF EXISTS {legacy}")
        conn.execute(f"ALTER TABLE dm_pending_queue RENAME TO {legacy}")
        conn.execute(
            """
            CREATE TABLE dm_pending_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dm_task_id INTEGER NOT NULL,
                account_user_id INTEGER,
                target_user_id INTEGER NOT NULL,
                target_access_hash INTEGER,
                target_username TEXT,
                target_first_name TEXT,
                target_last_name TEXT,
                source_chat_id INTEGER,
                source_chat_title TEXT,
                enqueued_at TEXT NOT NULL,
                eligible_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                claim_token TEXT,
                claimed_at TEXT,
                send_started_at TEXT,
                sent_at TEXT,
                retry_count INTEGER NOT NULL DEFAULT 0,
                resolve_attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                updated_at TEXT
            )
            """
        )
        account_expr = source(
            "account_user_id",
            "(SELECT user_id FROM dm_tasks WHERE dm_tasks.id=dm_task_id)",
        )
        select_parts = [
            source("id", "NULL"),
            "dm_task_id",
            account_expr,
            "target_user_id",
            source("target_access_hash", "NULL"),
            source("target_username", "NULL"),
            source("target_first_name", "NULL"),
            source("target_last_name", "NULL"),
            source("source_chat_id", "NULL"),
            source("source_chat_title", "NULL"),
            source("enqueued_at", "strftime('%Y-%m-%dT%H:%M:%f+00:00','now')"),
            source("eligible_at", "strftime('%Y-%m-%dT%H:%M:%f+00:00','now')"),
            source("status", "'pending'"),
            source("claim_token", "NULL"),
            source("claimed_at", "NULL"),
            source("send_started_at", "NULL"),
            source("sent_at", "NULL"),
            source("retry_count", "0"),
            source("resolve_attempts", "0"),
            source("last_error", "NULL"),
            source("updated_at", source("enqueued_at", "strftime('%Y-%m-%dT%H:%M:%f+00:00','now')")),
        ]
        conn.execute(
            f"""
            INSERT INTO dm_pending_queue (
                id, dm_task_id, account_user_id, target_user_id,
                target_access_hash, target_username, target_first_name,
                target_last_name, source_chat_id, source_chat_title,
                enqueued_at, eligible_at, status, claim_token, claimed_at,
                send_started_at, sent_at, retry_count, resolve_attempts,
                last_error, updated_at
            )
            SELECT {', '.join(select_parts)} FROM {legacy}
            """
        )
        conn.execute(f"DROP TABLE {legacy}")


def _ensure_dm_pending_sources_schema() -> None:
    """Add task ownership to source links and rebuild the old primary key safely."""
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='dm_pending_sources'"
    ).fetchone()
    if not row:
        return
    columns = {
        str(info[1]): info for info in conn.execute("PRAGMA table_info(dm_pending_sources)")
    }
    pk_columns = [
        str(info[1])
        for info in sorted(columns.values(), key=lambda item: int(item[5] or 0))
        if int(info[5] or 0) > 0
    ]
    if "dm_task_id" in columns and pk_columns == [
        "pending_id",
        "dm_task_id",
        "source_chat_id",
    ]:
        return

    legacy = "dm_pending_sources_legacy_v122b"
    has_task = "dm_task_id" in columns
    with conn:
        conn.execute(f"DROP TABLE IF EXISTS {legacy}")
        conn.execute(f"ALTER TABLE dm_pending_sources RENAME TO {legacy}")
        conn.execute(
            """
            CREATE TABLE dm_pending_sources (
                pending_id INTEGER NOT NULL,
                dm_task_id INTEGER NOT NULL,
                source_chat_id INTEGER NOT NULL,
                source_chat_title TEXT,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY (pending_id, dm_task_id, source_chat_id)
            )
            """
        )
        task_expr = "COALESCE(l.dm_task_id, q.dm_task_id)" if has_task else "q.dm_task_id"
        conn.execute(
            f"""
            INSERT OR IGNORE INTO dm_pending_sources (
                pending_id, dm_task_id, source_chat_id, source_chat_title,
                first_seen_at, last_seen_at
            )
            SELECT l.pending_id, {task_expr}, l.source_chat_id, l.source_chat_title,
                   l.first_seen_at, l.last_seen_at
              FROM {legacy} AS l
              JOIN dm_pending_queue AS q ON q.id=l.pending_id
             WHERE l.source_chat_id IS NOT NULL
               AND {task_expr} IS NOT NULL
            """
        )
        conn.execute(f"DROP TABLE {legacy}")


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
                session_string TEXT NOT NULL,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                profile_updated_at TEXT
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
    _add_column_if_missing(
        "sessions", "username", "ALTER TABLE sessions ADD COLUMN username TEXT"
    )
    _add_column_if_missing(
        "sessions", "first_name", "ALTER TABLE sessions ADD COLUMN first_name TEXT"
    )
    _add_column_if_missing(
        "sessions", "last_name", "ALTER TABLE sessions ADD COLUMN last_name TEXT"
    )
    _add_column_if_missing(
        "sessions",
        "profile_updated_at",
        "ALTER TABLE sessions ADD COLUMN profile_updated_at TEXT",
    )


def delete_table() -> None:
    """Mark ordinary broadcast jobs inactive during process shutdown/startup."""
    with conn:
        conn.execute(
            "UPDATE broadcasts SET is_active = ? WHERE is_active = ?",
            (False, True),
        )


def create_dm_tables() -> None:
    """Create and migrate DM task, queue and account-dispatch tables."""
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
                interval_minutes INTEGER NOT NULL DEFAULT 0,
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
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS dm_pending_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dm_task_id INTEGER NOT NULL,
                account_user_id INTEGER,
                target_user_id INTEGER NOT NULL,
                target_access_hash INTEGER,
                target_username TEXT,
                target_first_name TEXT,
                target_last_name TEXT,
                source_chat_id INTEGER,
                source_chat_title TEXT,
                enqueued_at TEXT NOT NULL,
                eligible_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                claim_token TEXT,
                claimed_at TEXT,
                send_started_at TEXT,
                sent_at TEXT,
                retry_count INTEGER NOT NULL DEFAULT 0,
                resolve_attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                updated_at TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS dm_pending_sources (
                pending_id INTEGER NOT NULL,
                dm_task_id INTEGER NOT NULL,
                source_chat_id INTEGER NOT NULL,
                source_chat_title TEXT,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY (pending_id, dm_task_id, source_chat_id)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS dm_global_first_dm_control (
                id INTEGER PRIMARY KEY CHECK (id=1),
                is_paused INTEGER NOT NULL DEFAULT 0,
                paused_at TEXT,
                paused_by_admin_id INTEGER,
                updated_at TEXT NOT NULL
            )
            """
        )
        cursor.execute(
            """
            INSERT OR IGNORE INTO dm_global_first_dm_control (
                id, is_paused, paused_at, paused_by_admin_id, updated_at
            ) VALUES (1, 0, NULL, NULL, datetime('now'))
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS dm_account_dispatch (
                account_user_id INTEGER PRIMARY KEY,
                pacing_min INTEGER NOT NULL DEFAULT 30,
                pacing_max INTEGER NOT NULL DEFAULT 60,
                last_send_at TEXT,
                next_send_at TEXT,
                cooldown_until TEXT,
                is_paused INTEGER NOT NULL DEFAULT 0,
                pause_reason TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        cursor.close()

    _rebuild_legacy_dm_pending_queue()
    _ensure_dm_pending_sources_schema()

    _add_column_if_missing(
        "dm_tasks", "delay_min", "ALTER TABLE dm_tasks ADD COLUMN delay_min INTEGER DEFAULT 30"
    )
    _add_column_if_missing(
        "dm_tasks", "delay_max", "ALTER TABLE dm_tasks ADD COLUMN delay_max INTEGER DEFAULT 90"
    )
    _add_column_if_missing(
        "dm_sent_log", "status", "ALTER TABLE dm_sent_log ADD COLUMN status TEXT DEFAULT 'sent'"
    )

    queue_columns = {
        "account_user_id": "ALTER TABLE dm_pending_queue ADD COLUMN account_user_id INTEGER",
        "status": "ALTER TABLE dm_pending_queue ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'",
        "claim_token": "ALTER TABLE dm_pending_queue ADD COLUMN claim_token TEXT",
        "claimed_at": "ALTER TABLE dm_pending_queue ADD COLUMN claimed_at TEXT",
        "send_started_at": "ALTER TABLE dm_pending_queue ADD COLUMN send_started_at TEXT",
        "sent_at": "ALTER TABLE dm_pending_queue ADD COLUMN sent_at TEXT",
        "retry_count": "ALTER TABLE dm_pending_queue ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0",
        "resolve_attempts": "ALTER TABLE dm_pending_queue ADD COLUMN resolve_attempts INTEGER NOT NULL DEFAULT 0",
        "last_error": "ALTER TABLE dm_pending_queue ADD COLUMN last_error TEXT",
        "updated_at": "ALTER TABLE dm_pending_queue ADD COLUMN updated_at TEXT",
    }
    for column, ddl in queue_columns.items():
        _add_column_if_missing("dm_pending_queue", column, ddl)
    _add_column_if_missing(
        "dm_account_dispatch",
        "last_send_at",
        "ALTER TABLE dm_account_dispatch ADD COLUMN last_send_at TEXT",
    )

    _normalize_table_group_ids("dm_watched_chats", "chat_id")

    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE dm_pending_queue
               SET account_user_id=(
                       SELECT user_id FROM dm_tasks WHERE dm_tasks.id=dm_pending_queue.dm_task_id
                   )
             WHERE account_user_id IS NULL
            """
        )
        cursor.execute(
            """
            UPDATE dm_pending_queue
               SET status=COALESCE(NULLIF(status, ''), 'pending'),
                   retry_count=COALESCE(retry_count, 0),
                   resolve_attempts=COALESCE(resolve_attempts, 0),
                   updated_at=COALESCE(updated_at, enqueued_at)
            """
        )
        cursor.execute(
            """
            INSERT OR IGNORE INTO dm_pending_sources (
                pending_id, dm_task_id, source_chat_id, source_chat_title, first_seen_at, last_seen_at
            )
            SELECT id, dm_task_id, source_chat_id, source_chat_title, enqueued_at, COALESCE(updated_at, enqueued_at)
              FROM dm_pending_queue
             WHERE source_chat_id IS NOT NULL
            """
        )
        cursor.execute(
            """
            INSERT OR IGNORE INTO dm_pending_sources (
                pending_id, dm_task_id, source_chat_id, source_chat_title, first_seen_at, last_seen_at
            )
            SELECT keeper.id, sources.dm_task_id, sources.source_chat_id, sources.source_chat_title,
                   sources.first_seen_at, sources.last_seen_at
              FROM dm_pending_sources AS sources
              JOIN dm_pending_queue AS duplicate ON duplicate.id=sources.pending_id
              JOIN dm_pending_queue AS keeper
                ON keeper.id=(
                    SELECT MIN(candidate.id)
                      FROM dm_pending_queue AS candidate
                     WHERE candidate.account_user_id=duplicate.account_user_id
                       AND candidate.target_user_id=duplicate.target_user_id
                       AND candidate.status IN (
                            'pending','claimed','sending','retry_wait',
                            'unresolved_peer','uncertain_delivery'
                       )
                )
             WHERE duplicate.id<>keeper.id
            """
        )
        cursor.execute(
            """
            UPDATE dm_pending_queue
               SET status='cancelled',
                   last_error='migration_duplicate_account_target'
             WHERE status IN (
                    'pending','claimed','sending','retry_wait',
                    'unresolved_peer','uncertain_delivery'
                  )
               AND EXISTS (
                    SELECT 1 FROM dm_pending_queue AS older
                     WHERE older.account_user_id=dm_pending_queue.account_user_id
                       AND older.target_user_id=dm_pending_queue.target_user_id
                       AND older.status IN (
                            'pending','claimed','sending','retry_wait',
                            'unresolved_peer','uncertain_delivery'
                       )
                       AND older.id < dm_pending_queue.id
               )
            """
        )
        inactive_owner_rows = cursor.execute(
            """
            SELECT q.id
              FROM dm_pending_queue AS q
              LEFT JOIN dm_tasks AS owner ON owner.id=q.dm_task_id
             WHERE q.status IN ('pending','claimed','retry_wait','unresolved_peer')
               AND COALESCE(owner.is_active, 0)=0
             ORDER BY q.id
            """
        ).fetchall()
        for (pending_id,) in inactive_owner_rows:
            candidate = cursor.execute(
                """
                SELECT s.dm_task_id, s.source_chat_id, s.source_chat_title
                  FROM dm_pending_sources AS s
                  JOIN dm_tasks AS t ON t.id=s.dm_task_id
                 WHERE s.pending_id=? AND t.is_active=1
                 ORDER BY s.first_seen_at, s.dm_task_id, s.source_chat_id
                 LIMIT 1
                """,
                (int(pending_id),),
            ).fetchone()
            if candidate:
                cursor.execute(
                    """
                    UPDATE dm_pending_queue
                       SET dm_task_id=?, source_chat_id=?, source_chat_title=?,
                           status=CASE WHEN status='claimed' THEN 'pending' ELSE status END,
                           claim_token=CASE WHEN status='claimed' THEN NULL ELSE claim_token END,
                           claimed_at=CASE WHEN status='claimed' THEN NULL ELSE claimed_at END,
                           last_error='migration_reassigned_to_active_source'
                     WHERE id=?
                    """,
                    (int(candidate[0]), int(candidate[1]), candidate[2], int(pending_id)),
                )

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
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_dm_pending_account_due
            ON dm_pending_queue(account_user_id, status, eligible_at, id)
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_dm_pending_task_due
            ON dm_pending_queue(dm_task_id, status, eligible_at, id)
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_dm_pending_target
            ON dm_pending_queue(target_user_id)
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_dm_pending_source_chat
            ON dm_pending_sources(source_chat_id, pending_id)
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_dm_pending_source_task
            ON dm_pending_sources(dm_task_id, source_chat_id, pending_id)
            """
        )
        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_dm_pending_active_account_target
            ON dm_pending_queue(account_user_id, target_user_id)
            WHERE status IN (
                'pending','claimed','sending','retry_wait',
                'unresolved_peer','uncertain_delivery'
            )
            """
        )
        cursor.execute(
            """
            INSERT OR IGNORE INTO dm_account_dispatch (
                account_user_id, pacing_min, pacing_max, last_send_at, next_send_at,
                cooldown_until, is_paused, pause_reason, updated_at
            )
            SELECT DISTINCT user_id, 30, 60, NULL, NULL, NULL, 0, NULL,
                   strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
              FROM dm_tasks
            """
        )
        conn.commit()
    finally:
        cursor.close()

    create_opt_out_table()
    create_contact_tables()
