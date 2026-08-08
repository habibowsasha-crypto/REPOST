"""SQLite connection and schema."""

from __future__ import annotations

import sqlite3
import threading
from typing import Optional

from loguru import logger

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
            except sqlite3.Error as exc:
                logger.warning("SQLite WAL mode could not be enabled: {}", exc)
        return _conn


def close_connection() -> None:
    """Close and forget the process-wide SQLite connection safely."""
    global _conn
    with _lock:
        conn = _conn
        _conn = None
        if conn is None:
            return
        try:
            conn.close()
        except sqlite3.Error as exc:
            logger.warning("SQLite connection close failed: {}", exc)


def db_lock() -> threading.RLock:
    return _lock


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    names = {str(r[1]) for r in rows}
    if column not in names:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def init_db() -> None:
    """Create or migrate every SQLite table required by the current release."""
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
            conn, "accounts", "peerflood_window_started_at",
            "peerflood_window_started_at TEXT"
        )
        _ensure_column(
            conn, "accounts", "peerflood_burst_applied_at",
            "peerflood_burst_applied_at TEXT"
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
        _ensure_column(
            conn, "accounts", "auth_status",
            "auth_status TEXT NOT NULL DEFAULT 'unknown'"
        )
        _ensure_column(conn, "accounts", "auth_error", "auth_error TEXT")
        _ensure_column(conn, "accounts", "auth_lost_at", "auth_lost_at TEXT")
        _ensure_column(conn, "accounts", "auth_notified_at", "auth_notified_at TEXT")

        # v1.0.62 removes the old "second rapid PeerFlood" interval backoff.
        # Restore any backed-up per-account interval once, but do not touch the
        # ordinary PeerFlood range, pacing settings, limits or active dialogs.
        migration_name = "v1_0_62_peerflood_five_in_ten"
        applied = conn.execute(
            "SELECT 1 FROM schema_migrations WHERE name=?",
            (migration_name,),
        ).fetchone()
        if not applied:
            rejected_v1061 = conn.execute(
                """
                SELECT applied_at FROM schema_migrations
                 WHERE name='v1_0_61_peerflood_four_in_ten_base_10m'
                """
            ).fetchone()
            if rejected_v1061:
                # v1.0.61 wrote 600-600 automatically. Restore 60-90 only when
                # those exact values have not been edited after that migration.
                rows = conn.execute(
                    """
                    SELECT key, value, updated_at FROM runtime_meta
                     WHERE key IN (
                         'peer_flood_cooldown_lo_seconds',
                         'peer_flood_cooldown_hi_seconds',
                         'peer_flood_min_cooldown_seconds'
                     )
                    """
                ).fetchall()
                values = {str(row['key']): row for row in rows}
                lo_row = values.get('peer_flood_cooldown_lo_seconds')
                hi_row = values.get('peer_flood_cooldown_hi_seconds')
                migration_at = str(rejected_v1061['applied_at'] or '')
                untouched_automatic_values = bool(
                    lo_row
                    and hi_row
                    and str(lo_row['value']) == '600'
                    and str(hi_row['value']) == '600'
                    and str(lo_row['updated_at'] or '') <= migration_at
                    and str(hi_row['updated_at'] or '') <= migration_at
                )
                if untouched_automatic_values:
                    now_sql = "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"
                    for key, value in (
                        ('peer_flood_cooldown_lo_seconds', '60'),
                        ('peer_flood_cooldown_hi_seconds', '90'),
                        ('peer_flood_min_cooldown_seconds', '75'),
                    ):
                        conn.execute(
                            f"""
                            INSERT INTO runtime_meta(key, value, updated_at)
                            VALUES (?, ?, {now_sql})
                            ON CONFLICT(key) DO UPDATE SET
                                value=excluded.value,
                                updated_at=excluded.updated_at
                            """,
                            (key, value),
                        )

            conn.execute(
                """
                UPDATE accounts
                   SET dm_interval_min_sec=CASE
                           WHEN COALESCE(interval_backup_min, interval_backup_max, -1)=-1
                           THEN NULL
                           ELSE COALESCE(interval_backup_min, interval_backup_max)
                       END,
                       dm_interval_max_sec=CASE
                           WHEN COALESCE(interval_backup_max, interval_backup_min, -1)=-1
                           THEN NULL
                           ELSE COALESCE(interval_backup_max, interval_backup_min)
                       END,
                       interval_backup_min=NULL,
                       interval_backup_max=NULL,
                       interval_backoff_until=NULL,
                       peerflood_streak=0,
                       peerflood_window_started_at=NULL
                 WHERE interval_backup_min IS NOT NULL
                    OR interval_backup_max IS NOT NULL
                    OR interval_backoff_until IS NOT NULL
                """
            )
            conn.execute(
                "UPDATE accounts SET peerflood_streak=0, peerflood_window_started_at=NULL"
            )
            # The table is created later in this transaction on clean installs;
            # legacy databases cannot contain it yet, so no event rows exist.
            conn.execute(
                """
                INSERT INTO schema_migrations(name, applied_at)
                VALUES (?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                """,
                (migration_name,),
            )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_accounts_participates
            ON accounts(participates)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_accounts_auth_status
            ON accounts(auth_status)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dialog_peerflood_account_gates (
                account_user_id INTEGER PRIMARY KEY,
                blocked_until TEXT NOT NULL,
                probe_claim_until TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS peerflood_hits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_user_id INTEGER NOT NULL,
                occurred_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_peerflood_hits_account_time
            ON peerflood_hits(account_user_id, occurred_at)
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
            CREATE TABLE IF NOT EXISTS account_chat_entity_sync (
                account_user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                last_sync_at TEXT,
                next_sync_at TEXT,
                last_error TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (account_user_id, chat_id)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_account_chat_entity_sync_due
            ON account_chat_entity_sync(account_user_id, next_sync_at, last_sync_at)
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
            CREATE TABLE IF NOT EXISTS first_dm_exclusions (
                target_user_id INTEGER PRIMARY KEY,
                reason TEXT NOT NULL,
                source_chat_id INTEGER,
                detected_by_account INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
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
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_leads_status_target
            ON leads(status, target_user_id)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lead_sources (
                target_user_id INTEGER NOT NULL,
                source_chat_id INTEGER NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY (target_user_id, source_chat_id)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_lead_sources_chat_target
            ON lead_sources(source_chat_id, target_user_id)
            """
        )
        _ensure_column(
            conn, "leads", "send_attempts", "send_attempts INTEGER NOT NULL DEFAULT 0"
        )
        _ensure_column(conn, "leads", "access_hash", "access_hash INTEGER")
        _ensure_column(conn, "leads", "last_error", "last_error TEXT")
        _ensure_column(conn, "leads", "failure_reason", "failure_reason TEXT")
        _ensure_column(conn, "leads", "failure_at", "failure_at TEXT")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lead_account_failures (
                target_user_id INTEGER NOT NULL,
                account_user_id INTEGER NOT NULL,
                failure_kind TEXT NOT NULL,
                error TEXT,
                attempted_at TEXT NOT NULL,
                PRIMARY KEY (target_user_id, account_user_id, failure_kind)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_lead_account_failures_target
            ON lead_account_failures(target_user_id, failure_kind, account_user_id)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lead_account_entities (
                target_user_id INTEGER NOT NULL,
                account_user_id INTEGER NOT NULL,
                access_hash INTEGER,
                username TEXT,
                source_chat_id INTEGER,
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY (target_user_id, account_user_id)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_lead_account_entities_target
            ON lead_account_entities(target_user_id, last_seen_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_lead_account_entities_account
            ON lead_account_entities(account_user_id, last_seen_at)
            """
        )

        migration_name = "v1_0_102_lead_sources"
        applied = conn.execute(
            "SELECT 1 FROM schema_migrations WHERE name=?",
            (migration_name,),
        ).fetchone()
        if not applied:
            conn.execute(
                """
                INSERT OR IGNORE INTO lead_sources (
                    target_user_id, source_chat_id, first_seen_at, last_seen_at
                )
                SELECT target_user_id, source_chat_id,
                       COALESCE(created_at, updated_at, last_seen_at),
                       COALESCE(last_seen_at, updated_at, created_at)
                  FROM leads
                 WHERE source_chat_id IS NOT NULL
                """
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO lead_sources (
                    target_user_id, source_chat_id, first_seen_at, last_seen_at
                )
                SELECT target_user_id, source_chat_id, last_seen_at, last_seen_at
                  FROM lead_account_entities
                 WHERE source_chat_id IS NOT NULL
                """
            )
            conn.execute(
                """
                INSERT INTO schema_migrations(name, applied_at)
                VALUES (?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                """,
                (migration_name,),
            )

        migration_name = "v1_0_77_account_owned_entity_evidence"
        applied = conn.execute(
            "SELECT 1 FROM schema_migrations WHERE name=?",
            (migration_name,),
        ).fetchone()
        if not applied:
            conn.execute(
                """
                INSERT OR IGNORE INTO lead_account_entities (
                    target_user_id, account_user_id, access_hash, username,
                    source_chat_id, last_seen_at
                )
                SELECT target_user_id, source_account_user_id, access_hash, username,
                       source_chat_id, COALESCE(last_seen_at, updated_at, created_at)
                  FROM leads
                 WHERE source_account_user_id IS NOT NULL
                """
            )
            conn.execute(
                """
                INSERT INTO schema_migrations(name, applied_at)
                VALUES (?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                """,
                (migration_name,),
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

        # v1.0.71 repairs the exact v1.0.70 production state where SpamBot had
        # automatically resumed an account while the global First-DM worker was
        # paused. Re-arm those accounts as FREE_PENDING and place a short rolling
        # Telegram cooldown so overdue dialog messages cannot immediately probe
        # the account before the new runtime guard takes over.
        migration_name = "v1_0_71_global_pause_spambot_guard"
        applied = conn.execute(
            "SELECT 1 FROM schema_migrations WHERE name=?",
            (migration_name,),
        ).fetchone()
        if not applied:
            worker_row = conn.execute(
                "SELECT value FROM runtime_meta WHERE key='dm_worker_enabled'"
            ).fetchone()
            worker_enabled = bool(
                worker_row
                and str(worker_row["value"] or "").strip().lower()
                in {"1", "true", "yes", "on"}
            )
            if not worker_enabled:
                hold_until = conn.execute(
                    "SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+60 seconds') AS value"
                ).fetchone()["value"]
                conn.execute(
                    """
                    UPDATE accounts
                       SET is_paused=1,
                           pause_reason='PeerFlood',
                           cooldown_until=CASE
                               WHEN cooldown_until IS NOT NULL
                                AND julianday(cooldown_until) > julianday(?)
                               THEN cooldown_until
                               ELSE ?
                           END,
                           next_send_at=CASE
                               WHEN next_send_at IS NOT NULL
                                AND julianday(next_send_at) > julianday(?)
                               THEN next_send_at
                               ELSE ?
                           END,
                           updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                     WHERE is_paused=0
                       AND user_id IN (
                           SELECT account_user_id
                             FROM spambot_state
                            WHERE last_reply LIKE 'resumed:spambot_%'
                       )
                    """,
                    (hold_until, hold_until, hold_until, hold_until),
                )
                conn.execute(
                    """
                    UPDATE spambot_state
                       SET status='free_pending_resume',
                           next_check_at=?,
                           updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                     WHERE last_reply LIKE 'resumed:spambot_%'
                    """,
                    (hold_until,),
                )
            conn.execute(
                """
                INSERT INTO schema_migrations(name, applied_at)
                VALUES (?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                """,
                (migration_name,),
            )

        # v1.0.79 separates the global First DM switch from active-dialog
        # delivery. Older releases kept SpamBot-free accounts inside a rolling
        # PeerFlood cooldown while the global worker was paused, which also
        # blocked replies in already-open dialogs. Clear only proven FREE_PENDING
        # rows and preserve a normal next_send_at guard for future First DMs.
        migration_name = "v1_0_79_active_dialogs_ignore_global_first_dm_pause"
        applied = conn.execute(
            "SELECT 1 FROM schema_migrations WHERE name=?",
            (migration_name,),
        ).fetchone()
        if not applied:
            worker_row = conn.execute(
                "SELECT value FROM runtime_meta WHERE key='dm_worker_enabled'"
            ).fetchone()
            worker_enabled = bool(
                worker_row
                and str(worker_row["value"] or "").strip().lower()
                in {"1", "true", "yes", "on"}
            )
            if not worker_enabled:
                next_first_dm_at = conn.execute(
                    "SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+120 seconds') AS value"
                ).fetchone()["value"]
                conn.execute(
                    """
                    UPDATE accounts
                       SET is_paused=0,
                           pause_reason=NULL,
                           cooldown_until=NULL,
                           next_send_at=CASE
                               WHEN next_send_at IS NOT NULL
                                AND julianday(next_send_at) > julianday(?)
                               THEN next_send_at
                               ELSE ?
                           END,
                           peerflood_burst_applied_at=NULL,
                           updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                     WHERE pause_reason='PeerFlood'
                       AND user_id IN (
                           SELECT account_user_id
                             FROM spambot_state
                            WHERE status='free_pending_resume'
                       )
                    """,
                    (next_first_dm_at, next_first_dm_at),
                )
                conn.execute(
                    """
                    UPDATE spambot_state
                       SET status='idle',
                           last_reply='resumed:v1.0.79_dialog_unblock',
                           next_check_at=NULL,
                           limited_until=NULL,
                           updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                     WHERE status='free_pending_resume'
                    """
                )
            conn.execute(
                """
                INSERT INTO schema_migrations(name, applied_at)
                VALUES (?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                """,
                (migration_name,),
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
        _ensure_column(
            conn,
            "sent_phrases",
            "delivery_key",
            "delivery_key TEXT",
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_sent_phrases_kind
            ON sent_phrases(kind, id DESC)
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_sent_phrases_delivery_key
            ON sent_phrases(kind, delivery_key)
            WHERE delivery_key IS NOT NULL
            """
        )

        # v1.0.65 keeps separate global anti-repeat windows for every
        # approved funnel message type. Older rows are safely trimmed on startup.
        for phrase_kind in ("first_dm", "promo", "apology", "link_help"):
            conn.execute(
                """
                DELETE FROM sent_phrases
                 WHERE kind=?
                   AND id NOT IN (
                       SELECT id FROM sent_phrases
                        WHERE kind=?
                        ORDER BY id DESC
                        LIMIT 20
                   )
                """,
                (phrase_kind, phrase_kind),
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
        _ensure_column(
            conn, "dialogs", "lifecycle_completed_at", "lifecycle_completed_at TEXT"
        )
        _ensure_column(
            conn, "dialogs", "last_message_at", "last_message_at TEXT"
        )
        _ensure_column(
            conn,
            "dialogs",
            "telegram_delete_abandoned_at",
            "telegram_delete_abandoned_at TEXT",
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dialog_archives (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                target_user_id INTEGER NOT NULL,
                account_user_id INTEGER NOT NULL,
                stage TEXT NOT NULL,
                outgoing_count INTEGER NOT NULL DEFAULT 0,
                link_sent INTEGER NOT NULL DEFAULT 0,
                auto_link_at TEXT,
                history_json TEXT NOT NULL DEFAULT '[]',
                original_updated_at TEXT NOT NULL,
                first_dm_at TEXT,
                first_dm_message_id INTEGER,
                telegram_delete_at TEXT,
                telegram_deleted_at TEXT,
                telegram_delete_next_attempt_at TEXT,
                telegram_delete_attempts INTEGER NOT NULL DEFAULT 0,
                telegram_delete_last_error TEXT,
                telegram_delete_until_message_id INTEGER,
                next_attempt_first_dm_at TEXT,
                history_purge_at TEXT,
                history_purged_at TEXT,
                lifecycle_completed_at TEXT,
                last_message_at TEXT,
                telegram_delete_abandoned_at TEXT,
                first_dm_text TEXT NOT NULL DEFAULT '',
                first_dm_prepared_at TEXT,
                first_dm_sent_at TEXT,
                first_dm_outbox_status TEXT,
                dialog_outbox_json TEXT NOT NULL DEFAULT '[]',
                dialog_inbox_json TEXT NOT NULL DEFAULT '[]',
                archived_reason TEXT NOT NULL,
                archived_at TEXT NOT NULL
            )
            """
        )
        _ensure_column(
            conn,
            "dialog_archives",
            "lifecycle_completed_at",
            "lifecycle_completed_at TEXT",
        )
        _ensure_column(
            conn,
            "dialog_archives",
            "last_message_at",
            "last_message_at TEXT",
        )
        _ensure_column(
            conn,
            "dialog_archives",
            "telegram_delete_abandoned_at",
            "telegram_delete_abandoned_at TEXT",
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_dialog_archives_target
            ON dialog_archives(target_user_id, id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_dialog_archives_tg_retention
            ON dialog_archives(telegram_deleted_at, telegram_delete_at,
                               telegram_delete_next_attempt_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_dialog_archives_local_retention
            ON dialog_archives(history_purged_at, history_purge_at)
            """
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
        _ensure_column(conn, "dialog_outbox", "message_kind", "message_kind TEXT")
        _ensure_column(conn, "dialog_outbox", "transition_json", "transition_json TEXT")
        _ensure_column(conn, "dialog_outbox", "source_inbox_id", "source_inbox_id INTEGER")
        _ensure_column(
            conn,
            "dialog_outbox",
            "allow_opt_out",
            "allow_opt_out INTEGER NOT NULL DEFAULT 0",
        )
        _ensure_column(
            conn,
            "dialog_outbox",
            "recovery_attempts",
            "recovery_attempts INTEGER NOT NULL DEFAULT 0",
        )
        _ensure_column(
            conn, "dialog_outbox", "recovery_next_at", "recovery_next_at TEXT"
        )
        _ensure_column(
            conn,
            "dialog_outbox",
            "recovery_last_error",
            "recovery_last_error TEXT",
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_dialog_outbox_target_status
            ON dialog_outbox(target_user_id, status, prepared_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_dialog_outbox_inbox
            ON dialog_outbox(source_inbox_id, status)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_dialog_outbox_recovery_due
            ON dialog_outbox(status, recovery_next_at, prepared_at)
            """
        )

        migration_name = "v1_0_70_peerflood_resume_and_dialog_recovery"
        applied = conn.execute(
            "SELECT 1 FROM schema_migrations WHERE name=?",
            (migration_name,),
        ).fetchone()
        if not applied:
            conn.execute(
                """
                UPDATE dialog_outbox
                   SET recovery_attempts=COALESCE(recovery_attempts, 0),
                       recovery_next_at=NULL,
                       recovery_last_error=NULL
                 WHERE status='prepared'
                """
            )
            conn.execute(
                """
                INSERT INTO schema_migrations(name, applied_at)
                VALUES (?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                """,
                (migration_name,),
            )
        # v1.0.65 never sends an advertising link after a request to stop. Cancel
        # any old prepared terminal message from v1.0.64 that still contains a URL.
        conn.execute(
            """
            UPDATE dialog_outbox
               SET status='failed',
                   last_error='v1.0.65_stop_reply_link_removed',
                   updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE status='prepared'
               AND message_kind='stop_close'
               AND (
                    lower(text) LIKE '%http://%'
                    OR lower(text) LIKE '%https://%'
                    OR lower(text) LIKE '%t.me/%'
               )
            """
        )


        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dialog_inbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_user_id INTEGER NOT NULL,
                target_user_id INTEGER NOT NULL,
                telegram_message_id INTEGER,
                text TEXT NOT NULL,
                is_hard_stop INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending',
                received_at TEXT NOT NULL,
                processing_started_at TEXT,
                processed_at TEXT,
                last_error TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        _ensure_column(
            conn,
            "dialog_inbox",
            "history_appended",
            "history_appended INTEGER NOT NULL DEFAULT 0",
        )
        _ensure_column(
            conn,
            "dialog_inbox",
            "content_kind",
            "content_kind TEXT NOT NULL DEFAULT 'text'",
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_dialog_inbox_telegram_message
            ON dialog_inbox(account_user_id, target_user_id, telegram_message_id)
            WHERE telegram_message_id IS NOT NULL
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_dialog_inbox_pending
            ON dialog_inbox(account_user_id, target_user_id, status, is_hard_stop, id)
            """
        )

        # v1.0.80 keeps global pause authoritative for every pre-reply
        # autonomous touch while still unblocking durable incoming dialogs. It
        # also normalizes old FREE_PENDING rows without emitting startup notices.
        migration_name = "v1_0_80_global_pause_pre_reply_loop_fix"
        applied = conn.execute(
            "SELECT 1 FROM schema_migrations WHERE name=?",
            (migration_name,),
        ).fetchone()
        if not applied:
            worker_row = conn.execute(
                "SELECT value FROM runtime_meta WHERE key='dm_worker_enabled'"
            ).fetchone()
            worker_enabled = bool(
                worker_row
                and str(worker_row["value"] or "").strip().lower()
                in {"1", "true", "yes", "on"}
            )
            if not worker_enabled:
                next_first_dm_at = conn.execute(
                    "SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+120 seconds') AS value"
                ).fetchone()["value"]
                conn.execute(
                    """
                    UPDATE dialog_outbox
                       SET status='failed',
                           last_error='v1.0.80_global_pause_pre_reply_hold',
                           recovery_next_at=NULL,
                           recovery_last_error=NULL,
                           updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                     WHERE status='prepared'
                       AND COALESCE(message_kind, action_kind)='followup'
                       AND source_inbox_id IS NULL
                       AND NOT EXISTS (
                           SELECT 1
                             FROM dialog_inbox i
                             JOIN dialogs d
                               ON d.target_user_id=dialog_outbox.target_user_id
                            WHERE i.target_user_id=dialog_outbox.target_user_id
                              AND i.account_user_id=dialog_outbox.account_user_id
                              AND (d.first_dm_at IS NULL
                                   OR julianday(i.received_at)>=julianday(d.first_dm_at))
                       )
                    """
                )
                conn.execute(
                    """
                    UPDATE accounts
                       SET is_paused=0,
                           pause_reason=NULL,
                           cooldown_until=NULL,
                           next_send_at=CASE
                               WHEN next_send_at IS NOT NULL
                                AND julianday(next_send_at) > julianday(?)
                               THEN next_send_at
                               ELSE ?
                           END,
                           peerflood_burst_applied_at=NULL,
                           updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                     WHERE COALESCE(auth_status, 'unknown')!='reauth_required'
                       AND user_id IN (
                           SELECT account_user_id
                             FROM spambot_state
                            WHERE status IN ('free_pending_resume', 'resuming')
                       )
                    """,
                    (next_first_dm_at, next_first_dm_at),
                )
                conn.execute(
                    """
                    UPDATE spambot_state
                       SET status='idle',
                           last_reply='resumed:v1.0.80_dialog_unblock',
                           next_check_at=NULL,
                           limited_until=NULL,
                           updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                     WHERE status IN ('free_pending_resume', 'resuming')
                       AND account_user_id IN (
                           SELECT user_id FROM accounts
                            WHERE COALESCE(auth_status, 'unknown')!='reauth_required'
                       )
                    """
                )
            conn.execute(
                """
                INSERT INTO schema_migrations(name, applied_at)
                VALUES (?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                """,
                (migration_name,),
            )

        # v1.0.66 restores the approved rule: a calm refusal is not terminal and may
        # still receive the promo. Cancel any unsent v1.0.65 soft-close action so a
        # restart cannot deliver the obsolete close and terminate the dialog.
        conn.execute(
            """
            UPDATE dialog_outbox
               SET status='failed',
                   last_error='v1.0.66_soft_refusal_close_cancelled',
                   updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE status='prepared'
               AND message_kind='close'
               AND source_inbox_id IN (
                   SELECT id
                     FROM dialog_inbox
                    WHERE is_hard_stop=0
                      AND (
                           trim(lower(text)) IN ('нет', 'нет.', 'нет!', 'нет?')
                           OR lower(text) LIKE '%неинтересно%'
                           OR lower(text) LIKE '%не интересно%'
                           OR lower(text) LIKE '%не надо%'
                           OR lower(text) LIKE '%не нужно%'
                           OR lower(text) LIKE '%нет спасибо%'
                           OR lower(text) LIKE '%нет, спасибо%'
                           OR lower(text) LIKE '%не хочу%'
                           OR lower(text) LIKE '%не актуально%'
                           OR lower(text) LIKE '%не сейчас%'
                           OR lower(text) LIKE '%не торгую%'
                           OR lower(text) LIKE '%уже не торг%'
                           OR lower(text) LIKE '%не в рынке%'
                      )
               )
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_dialogs_lifecycle
            ON dialogs(stage, lifecycle_completed_at)
            """
        )
        conn.execute(
            """
            UPDATE dialogs
               SET lifecycle_completed_at=COALESCE(lifecycle_completed_at, updated_at)
             WHERE stage IN ('followup_sent', 'link_sent', 'closed')
            """
        )
        conn.execute(
            """
            UPDATE dialog_archives
               SET lifecycle_completed_at=COALESCE(
                       lifecycle_completed_at, original_updated_at, archived_at
                   )
             WHERE stage IN ('followup_sent', 'link_sent', 'closed')
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
        _ensure_column(
            conn,
            "first_dm_outbox",
            "recovery_attempts",
            "recovery_attempts INTEGER NOT NULL DEFAULT 0",
        )
        _ensure_column(
            conn,
            "first_dm_outbox",
            "recovery_next_at",
            "recovery_next_at TEXT",
        )
        _ensure_column(
            conn,
            "first_dm_outbox",
            "recovery_last_error",
            "recovery_last_error TEXT",
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_first_dm_outbox_status_prepared
            ON first_dm_outbox(status, prepared_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_first_dm_outbox_recovery_due
            ON first_dm_outbox(status, recovery_next_at, prepared_at)
            """
        )

        # v1.0.63 bounds legacy First DM recovery and removes only stale
        # cooldown artifacts created before the accepted v1.0.62 migration.
        migration_name = "v1_0_63_recovery_and_peerflood_cleanup"
        applied = conn.execute(
            "SELECT 1 FROM schema_migrations WHERE name=?",
            (migration_name,),
        ).fetchone()
        if not applied:
            conn.execute(
                """
                UPDATE first_dm_outbox
                   SET recovery_next_at=NULL
                 WHERE status='prepared'
                   AND (
                       recovery_last_error LIKE 'peer_id_invalid%'
                       OR recovery_last_error='entity_unavailable'
                   )
                """
            )

            rejected_v1061 = conn.execute(
                """
                SELECT applied_at FROM schema_migrations
                 WHERE name='v1_0_61_peerflood_four_in_ten_base_10m'
                """
            ).fetchone()
            accepted_v1062 = conn.execute(
                """
                SELECT applied_at FROM schema_migrations
                 WHERE name='v1_0_62_peerflood_five_in_ten'
                """
            ).fetchone()
            if rejected_v1061 and accepted_v1062:
                hi_row = conn.execute(
                    """
                    SELECT value FROM runtime_meta
                     WHERE key='peer_flood_cooldown_hi_seconds'
                    """
                ).fetchone()
                try:
                    ordinary_hi = max(60, min(86400, int(hi_row['value'])))
                except (TypeError, ValueError, KeyError):
                    ordinary_hi = 90
                cutoff = str(accepted_v1062['applied_at'] or '')
                conn.execute(
                    """
                    UPDATE accounts
                       SET cooldown_until=strftime(
                               '%Y-%m-%dT%H:%M:%fZ',
                               'now',
                               '+' || ? || ' seconds'
                           ),
                           next_send_at=CASE
                               WHEN next_send_at IS NOT NULL
                                AND julianday(next_send_at) > julianday(
                                    'now', '+' || ? || ' seconds'
                                )
                               THEN NULL
                               ELSE next_send_at
                           END,
                           updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                     WHERE is_paused=1
                       AND pause_reason='PeerFlood'
                       AND peerflood_last_at IS NOT NULL
                       AND peerflood_last_at <= ?
                       AND julianday(cooldown_until) > julianday(
                           'now', '+' || ? || ' seconds'
                       )
                    """,
                    (ordinary_hi, ordinary_hi, cutoff, ordinary_hi),
                )

            conn.execute(
                """
                INSERT INTO schema_migrations(name, applied_at)
                VALUES (?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                """,
                (migration_name,),
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
                   d.updated_at, d.last_message_at, o.sent_at, o.telegram_message_id,
                   a.first_dm_at AS audience_dm_at,
                   (SELECT MAX(i.received_at) FROM dialog_inbox i
                     WHERE i.target_user_id=d.target_user_id) AS latest_incoming_at,
                   (SELECT MAX(x.sent_at) FROM dialog_outbox x
                     WHERE x.target_user_id=d.target_user_id
                       AND x.sent_at IS NOT NULL) AS latest_outgoing_at
              FROM dialogs d
              LEFT JOIN first_dm_outbox o ON o.target_user_id=d.target_user_id
              LEFT JOIN audience a ON a.user_id=d.target_user_id
             WHERE d.first_dm_at IS NULL OR d.history_purge_at IS NULL
                OR d.telegram_delete_at IS NULL OR d.last_message_at IS NULL
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
                activity_candidates = [
                    row["last_message_at"],
                    row["latest_incoming_at"],
                    row["latest_outgoing_at"],
                    row["sent_at"],
                    base.isoformat(),
                ]
                if not any(activity_candidates):
                    activity_candidates.append(row["updated_at"])
                parsed_activity = []
                for candidate in activity_candidates:
                    if not candidate:
                        continue
                    try:
                        value = _dt.datetime.fromisoformat(
                            str(candidate).replace("Z", "+00:00")
                        )
                        if value.tzinfo is None:
                            value = value.replace(tzinfo=_dt.timezone.utc)
                        parsed_activity.append(value.astimezone(_dt.timezone.utc))
                    except (TypeError, ValueError):
                        continue
                last_message = max(parsed_activity) if parsed_activity else base
                conn.execute(
                    """
                    UPDATE dialogs
                       SET first_dm_at=COALESCE(first_dm_at, ?),
                           first_dm_message_id=COALESCE(first_dm_message_id, ?),
                           last_message_at=COALESCE(last_message_at, ?),
                           telegram_delete_at=COALESCE(telegram_delete_at, ?),
                           history_purge_at=COALESCE(history_purge_at, ?)
                     WHERE target_user_id=?
                    """,
                    (
                        base.isoformat(),
                        row["telegram_message_id"],
                        last_message.isoformat(),
                        (base + _dt.timedelta(days=TELEGRAM_DIALOG_DELETE_DAYS)).isoformat(),
                        (base + _dt.timedelta(days=LOCAL_DIALOG_TEXT_RETENTION_DAYS)).isoformat(),
                        int(row["target_user_id"]),
                    ),
                )

        # v1.0.84 opt-in mode schedules Telegram deletion from the latest real
        # incoming or outgoing message instead of from the original First DM.
        from config import DIALOG_AUTO_DELETE_AFTER_DAYS, DIALOG_AUTO_DELETE_ENABLED

        if DIALOG_AUTO_DELETE_ENABLED:
            import datetime as _dt

            pending_rows = conn.execute(
                """
                SELECT target_user_id, last_message_at, first_dm_at
                  FROM dialogs
                 WHERE telegram_deleted_at IS NULL
                   AND telegram_delete_abandoned_at IS NULL
                """
            ).fetchall()
            for pending in pending_rows:
                raw_activity = pending["last_message_at"] or pending["first_dm_at"]
                if not raw_activity:
                    continue
                try:
                    activity = _dt.datetime.fromisoformat(
                        str(raw_activity).replace("Z", "+00:00")
                    )
                    if activity.tzinfo is None:
                        activity = activity.replace(tzinfo=_dt.timezone.utc)
                    activity = activity.astimezone(_dt.timezone.utc)
                except (TypeError, ValueError):
                    continue
                conn.execute(
                    """
                    UPDATE dialogs
                       SET last_message_at=COALESCE(last_message_at, ?),
                           telegram_delete_at=?,
                           telegram_delete_next_attempt_at=NULL,
                           telegram_delete_attempts=0,
                           telegram_delete_last_error=NULL
                     WHERE target_user_id=?
                       AND telegram_deleted_at IS NULL
                       AND telegram_delete_abandoned_at IS NULL
                    """,
                    (
                        activity.isoformat(),
                        (
                            activity
                            + _dt.timedelta(days=max(1, DIALOG_AUTO_DELETE_AFTER_DAYS))
                        ).isoformat(),
                        int(pending["target_user_id"]),
                    ),
                )
