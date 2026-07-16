"""AI dialog layer for TgBlaster DM.

The old DM module creates the first contact. This service handles only follow-up
incoming private messages and keeps short dialog history in SQLite.
"""

from __future__ import annotations

import asyncio
import datetime
import random
import sqlite3
from dataclasses import dataclass
from typing import Any, Optional
from weakref import WeakValueDictionary

from decouple import config
from loguru import logger
from telethon import TelegramClient
from telethon.tl.types import User

from config import conn
from services.maxim_sales_funnel import (
    PIRATE_VIP_LINK_TOKEN,
    LINK_ACCESS_HELP_VARIANTS,
    FunnelPlan,
    build_local_plan,
    generate_plan,
    generate_post_link_plan,
    is_explicit_stop,
    is_human_takeover_request,
    make_media_reaction_text,
    post_link_final_messages,
    validate_link_access_help,
)
from services.dm_opt_out import (
    add_opt_out,
    is_opted_out,
    migrate_legacy_closed_dialogs,
    opt_out_count,
)
from services.dm_contact_analytics import (
    is_completed_contact,
    mark_completed as mark_contact_completed,
    mark_first_reply as mark_contact_first_reply,
    mark_latest_first_reply,
    mark_link_sent as mark_contact_link_sent,
    mark_opted_out as mark_contact_opted_out,
    touch_cycle as touch_contact_cycle,
)


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _truthy(name: str, default: str = "false") -> bool:
    value = config(name, default=default).strip().lower()
    return value in {"1", "true", "yes", "on", "y", "да"}


def ai_enabled() -> bool:
    return _truthy("AI_DM_ENABLED", "false")


def ai_dry_run() -> bool:
    return _truthy("AI_DM_DRY_RUN", "false")


def _csv_words(name: str, default: str) -> list[str]:
    raw = config(name, default=default)
    return [w.strip().lower() for w in raw.split(",") if w.strip()]


def _safe_int(name: str, default: int, min_value: int | None = None, max_value: int | None = None) -> int:
    try:
        value = int(config(name, default=str(default)))
    except Exception:
        value = default
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def _claim_incoming_message(account_user_id: int, target_user_id: int, telegram_message_id: int | None) -> bool:
    """Returns False when this exact Telegram PM was already handled.

    This prevents duplicate AI replies when several active DM tasks are running
    under the same sender account and each client receives the same private msg.
    """
    if telegram_message_id is None:
        return True
    cursor = conn.cursor()
    try:
        cursor.execute(
            """INSERT INTO ai_processed_messages
               (account_user_id, target_user_id, telegram_message_id, processed_at)
               VALUES (?,?,?,?)""",
            (account_user_id, target_user_id, telegram_message_id, _now_iso()),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    except Exception as exc:
        logger.error(f"[AI DM] incoming-message de-duplication failed: {exc}")
        # Do not drop a real user reply because of a transient DB issue.
        return True
    finally:
        cursor.close()


def _daily_dialog_limit_reached() -> bool:
    limit = _safe_int("AI_DAILY_DIALOG_LIMIT", 0, min_value=0)
    if limit <= 0:
        return False
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM ai_dialogs WHERE date(created_at) = date('now')")
        count = int(cursor.fetchone()[0] or 0)
        return count >= limit
    finally:
        cursor.close()


async def _safe_send_message(client: TelegramClient, target: User, text: str, context: str) -> bool:
    target_id = int(getattr(target, "id", 0) or 0)
    try:
        await client.send_message(target, text)
        return True
    except Exception as exc:
        logger.error(f"[AI DM] send failed ({context}) user={target_id}: {exc}")
        return False


STOP_WORDS_DEFAULT = "не пиши,больше не пиши,отстань,не интересно,не надо,заблокирую"
HUMAN_WORDS_DEFAULT = "админ,оператор,человек,менеджер,живой"

# Per-user async locks protect against duplicate/parallel AI replies when several
# DM tasks for the same account receive the same private message.
_dialog_locks: WeakValueDictionary[tuple[int, int], asyncio.Lock] = WeakValueDictionary()


def _get_dialog_lock(account_user_id: int, target_user_id: int) -> asyncio.Lock:
    key = (account_user_id, target_user_id)
    lock = _dialog_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _dialog_locks[key] = lock
    return lock


@dataclass
class DialogRow:
    id: int
    dm_task_id: int
    account_user_id: int
    target_user_id: int
    username: Optional[str]
    first_name: Optional[str]
    source_chat_id: Optional[int]
    source_chat_title: Optional[str]
    contact_cycle_id: Optional[int]
    stage: str
    status: str
    message_count: int
    stopped_reason: Optional[str]


def create_ai_tables() -> None:
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ai_dialogs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dm_task_id INTEGER,
            account_user_id INTEGER NOT NULL,
            target_user_id INTEGER NOT NULL,
            username TEXT,
            first_name TEXT,
            source_chat_id INTEGER,
            source_chat_title TEXT,
            contact_cycle_id INTEGER,
            stage TEXT DEFAULT 'new_contact',
            status TEXT DEFAULT 'active',
            message_count INTEGER DEFAULT 0,
            last_incoming_at TEXT,
            last_outgoing_at TEXT,
            created_at TEXT,
            updated_at TEXT,
            stopped_reason TEXT,
            UNIQUE(account_user_id, target_user_id)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ai_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dialog_id INTEGER NOT NULL,
            direction TEXT NOT NULL,
            message_text TEXT,
            created_at TEXT,
            provider TEXT,
            model TEXT,
            tokens_used INTEGER DEFAULT 0
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ai_processed_messages (
            account_user_id INTEGER NOT NULL,
            target_user_id INTEGER NOT NULL,
            telegram_message_id INTEGER NOT NULL,
            processed_at TEXT NOT NULL,
            PRIMARY KEY (account_user_id, target_user_id, telegram_message_id)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ai_link_help_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_user_id INTEGER NOT NULL,
            target_user_id INTEGER NOT NULL,
            dialog_id INTEGER NOT NULL,
            variant_index INTEGER NOT NULL,
            used_at TEXT NOT NULL
        )
    """)
    # Older installations may have been created before the UNIQUE constraint.
    # Merge duplicate dialog rows without losing their message history.
    duplicates = cursor.execute(
        """
        SELECT account_user_id, target_user_id, MAX(id) AS keep_id
        FROM ai_dialogs
        GROUP BY account_user_id, target_user_id
        HAVING COUNT(*) > 1
        """
    ).fetchall()
    for account_user_id, target_user_id, keep_id in duplicates:
        duplicate_ids = [
            int(row[0])
            for row in cursor.execute(
                """
                SELECT id FROM ai_dialogs
                WHERE account_user_id = ? AND target_user_id = ? AND id <> ?
                """,
                (account_user_id, target_user_id, keep_id),
            ).fetchall()
        ]
        for duplicate_id in duplicate_ids:
            cursor.execute(
                "UPDATE ai_messages SET dialog_id = ? WHERE dialog_id = ?",
                (keep_id, duplicate_id),
            )
            cursor.execute("DELETE FROM ai_dialogs WHERE id = ?", (duplicate_id,))

    cursor.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_ai_dialogs_account_target "
        "ON ai_dialogs(account_user_id, target_user_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_ai_dialogs_status_updated ON ai_dialogs(status, updated_at)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_ai_messages_dialog_id ON ai_messages(dialog_id, id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_ai_processed_at ON ai_processed_messages(processed_at)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_ai_link_help_account_id "
        "ON ai_link_help_usage(account_user_id, id DESC)"
    )

    # Лёгкие миграции для старых БД.
    for table, col, ddl in [
        ("ai_dialogs", "stopped_reason", "ALTER TABLE ai_dialogs ADD COLUMN stopped_reason TEXT"),
        ("ai_dialogs", "message_count", "ALTER TABLE ai_dialogs ADD COLUMN message_count INTEGER DEFAULT 0"),
        ("ai_dialogs", "source_chat_id", "ALTER TABLE ai_dialogs ADD COLUMN source_chat_id INTEGER"),
        ("ai_dialogs", "source_chat_title", "ALTER TABLE ai_dialogs ADD COLUMN source_chat_title TEXT"),
        ("ai_dialogs", "contact_cycle_id", "ALTER TABLE ai_dialogs ADD COLUMN contact_cycle_id INTEGER"),
        ("ai_messages", "tokens_used", "ALTER TABLE ai_messages ADD COLUMN tokens_used INTEGER DEFAULT 0"),
    ]:
        try:
            cursor.execute(ddl)
            conn.commit()
        except Exception:
            pass
    conn.commit()
    cursor.close()
    migrate_legacy_closed_dialogs()


def _row_to_dialog(row: tuple | None) -> Optional[DialogRow]:
    if not row:
        return None
    return DialogRow(*row)


def _get_dialog_by_target(account_user_id: int, target_user_id: int) -> Optional[DialogRow]:
    cursor = conn.cursor()
    cursor.execute(
        """SELECT id, dm_task_id, account_user_id, target_user_id, username, first_name,
                  source_chat_id, source_chat_title, contact_cycle_id, stage, status, message_count, stopped_reason
           FROM ai_dialogs WHERE account_user_id = ? AND target_user_id = ?""",
        (account_user_id, target_user_id),
    )
    row = cursor.fetchone()
    cursor.close()
    return _row_to_dialog(row)


def _get_dialog_by_id(dialog_id: int) -> Optional[DialogRow]:
    cursor = conn.cursor()
    cursor.execute(
        """SELECT id, dm_task_id, account_user_id, target_user_id, username, first_name,
                  source_chat_id, source_chat_title, contact_cycle_id, stage, status, message_count, stopped_reason
           FROM ai_dialogs WHERE id = ?""",
        (dialog_id,),
    )
    row = cursor.fetchone()
    cursor.close()
    return _row_to_dialog(row)


def _upsert_dialog(
    *,
    dm_task_id: int,
    account_user_id: int,
    target_user_id: int,
    username: Optional[str],
    first_name: Optional[str],
    source_chat_id: Optional[int] = None,
    source_chat_title: Optional[str] = None,
    contact_cycle_id: Optional[int] = None,
) -> DialogRow:
    existing = _get_dialog_by_target(account_user_id, target_user_id)
    now = _now_iso()
    cursor = conn.cursor()
    if existing:
        cursor.execute(
            """UPDATE ai_dialogs SET dm_task_id = ?, username = COALESCE(?, username),
                      first_name = COALESCE(?, first_name),
                      source_chat_id = COALESCE(?, source_chat_id),
                      source_chat_title = COALESCE(?, source_chat_title),
                      contact_cycle_id = COALESCE(?, contact_cycle_id), updated_at = ?
               WHERE id = ?""",
            (
                dm_task_id,
                username,
                first_name,
                source_chat_id,
                source_chat_title,
                contact_cycle_id,
                now,
                existing.id,
            ),
        )
        conn.commit()
        cursor.close()
        return _get_dialog_by_id(existing.id) or existing

    cursor.execute(
        """INSERT INTO ai_dialogs
           (dm_task_id, account_user_id, target_user_id, username, first_name,
            source_chat_id, source_chat_title, contact_cycle_id, stage,
            status, message_count, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?,?,'new_contact','active',0,?,?)""",
        (
            dm_task_id,
            account_user_id,
            target_user_id,
            username,
            first_name,
            source_chat_id,
            source_chat_title,
            contact_cycle_id,
            now,
            now,
        ),
    )
    dialog_id = cursor.lastrowid
    conn.commit()
    cursor.close()
    return _get_dialog_by_id(dialog_id)  # type: ignore[return-value]


def _save_message(
    dialog_id: int,
    direction: str,
    text: str,
    provider: str = "local",
    model: str = "",
    tokens_used: int = 0,
) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO ai_messages
           (dialog_id, direction, message_text, created_at, provider, model, tokens_used)
           VALUES (?,?,?,?,?,?,?)""",
        (dialog_id, direction, text, _now_iso(), provider, model, tokens_used),
    )
    conn.commit()
    cursor.close()


def _set_dialog_status(dialog_id: int, status: str, reason: str = "", stage: Optional[str] = None) -> None:
    cursor = conn.cursor()
    if stage:
        cursor.execute(
            """UPDATE ai_dialogs SET status = ?, stopped_reason = ?, stage = ?, updated_at = ?
               WHERE id = ?""",
            (status, reason, stage, _now_iso(), dialog_id),
        )
    else:
        cursor.execute(
            """UPDATE ai_dialogs SET status = ?, stopped_reason = ?, updated_at = ? WHERE id = ?""",
            (status, reason, _now_iso(), dialog_id),
        )
    conn.commit()
    cursor.close()


def _set_stage(dialog_id: int, stage: str) -> None:
    cursor = conn.cursor()
    cursor.execute("UPDATE ai_dialogs SET stage = ?, updated_at = ? WHERE id = ?", (stage, _now_iso(), dialog_id))
    conn.commit()
    cursor.close()


def _finalize_completed_dialog(dialog: DialogRow, reason: str) -> None:
    """Persist global repeat protection before closing the AI row.

    The contact table is the source of truth for future first-DM eligibility. It
    is written first so a partial database failure cannot leave a user exposed to
    another first message after Максим has already finished the conversation.
    When the original contact-cycle insert was lost after Telegram delivery, the
    identity fields create a recovery protection record without inventing history.
    """
    mark_contact_completed(
        dialog.contact_cycle_id,
        reason,
        account_user_id=dialog.account_user_id,
        target_user_id=dialog.target_user_id,
        source_chat_id=dialog.source_chat_id,
        source_chat_title=dialog.source_chat_title,
    )
    _set_dialog_status(dialog.id, "completed", reason, stage="completed")


def _mark_incoming(dialog_id: int) -> None:
    cursor = conn.cursor()
    cursor.execute("UPDATE ai_dialogs SET last_incoming_at = ?, updated_at = ? WHERE id = ?", (_now_iso(), _now_iso(), dialog_id))
    conn.commit()
    cursor.close()


def _mark_outgoing(dialog_id: int) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """UPDATE ai_dialogs
           SET last_outgoing_at = ?, updated_at = ?, message_count = COALESCE(message_count,0) + 1
           WHERE id = ?""",
        (_now_iso(), _now_iso(), dialog_id),
    )
    conn.commit()
    cursor.close()


def record_first_dm(
    *,
    dm_task_id: int,
    account_user_id: int,
    target: User,
    text: str,
    source_chat_id: Optional[int] = None,
    source_chat_title: Optional[str] = None,
    contact_cycle_id: Optional[int] = None,
) -> None:
    """Create a new AI response cycle after a real first DM was delivered.

    A dialog row is reused per sender/recipient pair. Older versions only updated
    metadata for an existing row, so a recipient whose previous cycle was already
    ``completed`` stayed completed and the AI silently ignored the new reply.

    A newly delivered first DM re-opens ordinary completed/error cycles. Explicit
    opt-out, admin stop and human-takeover states remain closed as before.
    """
    if not ai_enabled():
        return
    create_ai_tables()
    existing = _get_dialog_by_target(account_user_id, target.id)
    if existing is None and _daily_dialog_limit_reached():
        logger.warning(f"[AI DM] daily dialog limit reached; skip dialog for user={target.id}")
        return
    dialog = _upsert_dialog(
        dm_task_id=dm_task_id,
        account_user_id=account_user_id,
        target_user_id=target.id,
        username=getattr(target, "username", None),
        first_name=getattr(target, "first_name", None),
        source_chat_id=source_chat_id,
        source_chat_title=source_chat_title,
        contact_cycle_id=contact_cycle_id,
    )
    _save_message(dialog.id, "outgoing", text, provider="dm_first", model="first_dm")

    protected_closed_statuses = {"closed_negative", "admin_stopped", "human_needed"}
    cursor = conn.cursor()
    try:
        if dialog.status in protected_closed_statuses:
            cursor.execute(
                "UPDATE ai_dialogs SET last_outgoing_at = ?, updated_at = ? WHERE id = ?",
                (_now_iso(), _now_iso(), dialog.id),
            )
            logger.info(
                f"[AI DM] first DM recorded without reopening protected state: "
                f"dialog={dialog.id}, user={target.id}, status={dialog.status}"
            )
        else:
            cursor.execute(
                """UPDATE ai_dialogs
                   SET status = 'active', stage = 'first_dm_sent', stopped_reason = NULL,
                       source_chat_id = COALESCE(?, source_chat_id),
                       source_chat_title = COALESCE(?, source_chat_title),
                       contact_cycle_id = COALESCE(?, contact_cycle_id),
                       last_outgoing_at = ?, updated_at = ?
                   WHERE id = ?""",
                (
                    source_chat_id,
                    source_chat_title,
                    contact_cycle_id,
                    _now_iso(),
                    _now_iso(),
                    dialog.id,
                ),
            )
            logger.info(
                f"[AI DM] response cycle opened after first DM: "
                f"dialog={dialog.id}, user={target.id}, previous_status={dialog.status}"
            )
        conn.commit()
    finally:
        cursor.close()


def _latest_first_dm_message_id(dialog_id: int) -> int:
    """Return the message boundary for the current first-DM response cycle."""
    cursor = conn.cursor()
    try:
        row = cursor.execute(
            """
            SELECT MAX(id) FROM ai_messages
            WHERE dialog_id = ? AND provider = 'dm_first'
            """,
            (dialog_id,),
        ).fetchone()
        return int((row or [0])[0] or 0)
    finally:
        cursor.close()


def _current_cycle_history(dialog_id: int, limit: int = 16) -> list[tuple[str, str]]:
    """Load the current first-DM cycle in chronological order for the model."""
    cycle_start_id = _latest_first_dm_message_id(dialog_id)
    cursor = conn.cursor()
    try:
        rows = cursor.execute(
            """
            SELECT direction, COALESCE(message_text, '')
            FROM ai_messages
            WHERE dialog_id = ?
              AND id >= ?
              AND direction IN ('incoming', 'outgoing')
              AND provider <> 'dry_run'
            ORDER BY id ASC
            """,
            (dialog_id, cycle_start_id),
        ).fetchall()
    finally:
        cursor.close()
    clean = [(str(direction), str(message)) for direction, message in rows if str(message).strip()]
    return clean[-max(4, limit):]


def _current_cycle_followup_count(dialog_id: int) -> int:
    """Count only Maxim's follow-up messages, never the first DM itself."""
    cycle_start_id = _latest_first_dm_message_id(dialog_id)
    cursor = conn.cursor()
    try:
        row = cursor.execute(
            """
            SELECT COUNT(*)
            FROM ai_messages
            WHERE dialog_id = ?
              AND id > ?
              AND direction = 'outgoing'
              AND provider <> 'dm_first'
              AND model NOT IN ('stop_reply', 'post_offer_apology', 'link_access_auto_help')
            """,
            (dialog_id, cycle_start_id),
        ).fetchone()
        return int((row or [0])[0] or 0)
    finally:
        cursor.close()


def _dialog_has_sent_offer(dialog_id: int) -> bool:
    """Detect the exact invitation link sent after the latest first DM."""
    cycle_start_id = _latest_first_dm_message_id(dialog_id)
    cursor = conn.cursor()
    try:
        row = cursor.execute(
            """
            SELECT 1 FROM ai_messages
            WHERE dialog_id = ?
              AND id > ?
              AND direction = 'outgoing'
              AND instr(COALESCE(message_text, ''), ?) > 0
            LIMIT 1
            """,
            (dialog_id, cycle_start_id, PIRATE_VIP_LINK_TOKEN),
        ).fetchone()
        return row is not None
    finally:
        cursor.close()


def _dialog_has_post_offer_apology(dialog_id: int) -> bool:
    cycle_start_id = _latest_first_dm_message_id(dialog_id)
    cursor = conn.cursor()
    try:
        row = cursor.execute(
            """
            SELECT 1 FROM ai_messages
            WHERE dialog_id = ?
              AND id > ?
              AND direction = 'outgoing'
              AND model = 'post_offer_apology'
            LIMIT 1
            """,
            (dialog_id, cycle_start_id),
        ).fetchone()
        return row is not None
    finally:
        cursor.close()


def _post_offer_apology() -> str:
    default = "Понял, извини, что побеспокоил. Больше писать не буду."
    return config("AI_POST_OFFER_APOLOGY", default=default).strip() or default


def _persist_global_opt_out(
    *, dialog: DialogRow, sender: User, reason: str
) -> None:
    """Honor a user's request not to be contacted across all future DM tasks."""
    add_opt_out(
        int(sender.id),
        reason=reason,
        source_dialog_id=dialog.id,
        source_account_user_id=dialog.account_user_id,
        source_dm_task_id=dialog.dm_task_id,
        username=getattr(sender, "username", None),
        first_name=getattr(sender, "first_name", None),
    )
    mark_contact_opted_out(dialog.contact_cycle_id, reason)
    _set_dialog_status(dialog.id, "closed_negative", reason, stage="closed_negative")


async def _send_post_offer_apology(
    *, dialog: DialogRow, client: TelegramClient, sender: User
) -> None:
    """Send at most one courtesy apology after the link, then close."""
    _persist_global_opt_out(dialog=dialog, sender=sender, reason="post_offer_explicit_stop")
    if _dialog_has_post_offer_apology(dialog.id):
        _set_dialog_status(
            dialog.id,
            "closed_negative",
            "post_offer_apology_already_sent",
            stage="closed_negative",
        )
        return

    reply = _post_offer_apology()
    if ai_dry_run():
        logger.info(f"[AI DM DRY RUN post-offer apology] user={sender.id}: {reply}")
        _save_message(
            dialog.id,
            "system",
            f"[DRY RUN post-offer apology] {reply}",
            provider="dry_run",
            model="post_offer_apology",
        )
        return

    sent = await _safe_send_message(client, sender, reply, "post_offer_apology")
    if not sent:
        logger.warning(
            f"[AI DM] opt-out saved but apology send failed: dialog={dialog.id}, user={sender.id}"
        )
        return
    _save_message(dialog.id, "outgoing", reply, provider="local", model="post_offer_apology")
    _mark_outgoing(dialog.id)
    logger.info(f"[AI DM] one-time post-offer apology sent: dialog={dialog.id}, user={sender.id}")


async def _reply_delay() -> None:
    dmin = _safe_int("AI_REPLY_DELAY_MIN_SECONDS", 20, min_value=0, max_value=3600)
    dmax = _safe_int("AI_REPLY_DELAY_MAX_SECONDS", 50, min_value=0, max_value=3600)
    if dmax < dmin:
        dmax = dmin
    delay = random.randint(dmin, dmax)
    if delay:
        await asyncio.sleep(delay)


async def _burst_delay() -> None:
    dmin = _safe_int("AI_BURST_DELAY_MIN_SECONDS", 2, min_value=0, max_value=60)
    dmax = _safe_int("AI_BURST_DELAY_MAX_SECONDS", 5, min_value=0, max_value=60)
    if dmax < dmin:
        dmax = dmin
    delay = random.randint(dmin, dmax)
    if delay:
        await asyncio.sleep(delay)


async def _link_help_delay() -> None:
    """A short independent pause makes the hint look like a human afterthought."""
    dmin = _safe_int("AI_LINK_HELP_DELAY_MIN_SECONDS", 3, min_value=0, max_value=60)
    dmax = _safe_int("AI_LINK_HELP_DELAY_MAX_SECONDS", 7, min_value=0, max_value=60)
    if dmax < dmin:
        dmax = dmin
    delay = random.randint(dmin, dmax)
    if delay:
        await asyncio.sleep(delay)


def _select_link_help_variant(dialog: DialogRow) -> tuple[int, str]:
    """Choose a persistent non-recent variant for this connected account.

    The pool is intentionally curated instead of generated freely by AI.  This
    preserves the exact Telegram UI instruction while avoiding visible template
    repetition across users and process restarts.
    """
    variants = LINK_ACCESS_HELP_VARIANTS
    if not variants:
        raise RuntimeError("LINK_ACCESS_HELP_VARIANTS is empty")

    recent_window = min(12, max(1, len(variants) - 1))
    cursor = conn.cursor()
    try:
        recent_rows = cursor.execute(
            """
            SELECT variant_index
            FROM ai_link_help_usage
            WHERE account_user_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (dialog.account_user_id, recent_window),
        ).fetchall()
        recent = {int(row[0]) for row in recent_rows if 0 <= int(row[0]) < len(variants)}
        candidates = [index for index in range(len(variants)) if index not in recent]
        if not candidates:
            last = int(recent_rows[0][0]) if recent_rows else -1
            candidates = [index for index in range(len(variants)) if index != last]
        variant_index = random.choice(candidates or list(range(len(variants))))
        message = variants[variant_index]
        if not validate_link_access_help(message):
            raise ValueError(f"invalid link-help variant: {variant_index}")

        cursor.execute(
            """
            INSERT INTO ai_link_help_usage
                (account_user_id, target_user_id, dialog_id, variant_index, used_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                dialog.account_user_id,
                dialog.target_user_id,
                dialog.id,
                variant_index,
                _now_iso(),
            ),
        )
        # Keep the table bounded without losing enough history for rotation.
        cursor.execute(
            """
            DELETE FROM ai_link_help_usage
            WHERE account_user_id = ?
              AND id NOT IN (
                  SELECT id FROM ai_link_help_usage
                  WHERE account_user_id = ?
                  ORDER BY id DESC
                  LIMIT 240
              )
            """,
            (dialog.account_user_id, dialog.account_user_id),
        )
        conn.commit()
        return variant_index, message
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


async def _send_automatic_link_help(
    *, dialog: DialogRow, client: TelegramClient, sender: User
) -> bool:
    """Send one varied hint after a delivered invitation link.

    Failure of this optional hint never converts a successfully delivered link
    into a Telegram send error for the whole dialog.
    """
    try:
        variant_index, message = _select_link_help_variant(dialog)
    except Exception as exc:
        logger.error(
            f"[AI DM] link-help variant selection failed: dialog={dialog.id}, "
            f"user={sender.id}: {exc}"
        )
        return False

    await _link_help_delay()
    sent = await _safe_send_message(
        client, sender, message, f"link_access_auto_help_{variant_index}"
    )
    if not sent:
        logger.warning(
            f"[AI DM] invitation link delivered but automatic help failed: "
            f"dialog={dialog.id}, user={sender.id}, variant={variant_index}"
        )
        return False

    _save_message(
        dialog.id,
        "outgoing",
        message,
        provider="local",
        model="link_access_auto_help",
    )
    _mark_outgoing(dialog.id)
    logger.info(
        f"[AI DM] automatic link help sent: dialog={dialog.id}, "
        f"user={sender.id}, variant={variant_index}"
    )
    return True


def _fit_plan_to_remaining(plan: FunnelPlan, remaining: int) -> FunnelPlan:
    """Keep the approved follow-up cap while preserving a link-bearing final reply."""
    remaining = max(0, remaining)
    if remaining <= 0 or len(plan.messages) <= remaining:
        return plan
    if remaining == 1:
        combined = " ".join(message.strip() for message in plan.messages if message.strip())
        return FunnelPlan(
            action=plan.action,
            next_stage=plan.next_stage,
            close_after=plan.close_after,
            messages=[combined],
            tokens_used=plan.tokens_used,
            model=plan.model,
        )
    return FunnelPlan(
        action=plan.action,
        next_stage=plan.next_stage,
        close_after=plan.close_after,
        messages=plan.messages[:remaining],
        tokens_used=plan.tokens_used,
        model=plan.model,
    )


async def _send_maxim_plan(
    *,
    dialog: DialogRow,
    client: TelegramClient,
    sender: User,
    plan: FunnelPlan,
) -> bool:
    """Send one or two context-aware Maxim messages and persist each one."""
    if ai_dry_run():
        for index, message in enumerate(plan.messages, start=1):
            logger.info(
                f"[AI DM DRY RUN Maxim] action={plan.action} user={sender.id} "
                f"message={index}: {message}"
            )
            _save_message(
                dialog.id,
                "system",
                f"[DRY RUN Maxim action={plan.action} message={index}] {message}",
                provider="dry_run",
                model=plan.model,
                tokens_used=plan.tokens_used if index == 1 else 0,
            )
        return False

    provider = "local" if plan.model.startswith("local_") else "openai"
    invitation_link_delivered = False
    for index, message in enumerate(plan.messages, start=1):
        sent = await _safe_send_message(client, sender, message, f"maxim_{plan.action}_{index}")
        if not sent:
            _set_dialog_status(dialog.id, "send_error", "telegram_send_failed", stage="send_error")
            return False
        _save_message(
            dialog.id,
            "outgoing",
            message,
            provider=provider,
            model=plan.model,
            tokens_used=plan.tokens_used if index == 1 else 0,
        )
        _mark_outgoing(dialog.id)
        if PIRATE_VIP_LINK_TOKEN in message:
            invitation_link_delivered = True
        if index < len(plan.messages):
            await _burst_delay()

    if invitation_link_delivered:
        await _send_automatic_link_help(
            dialog=dialog,
            client=client,
            sender=sender,
        )
    return True


async def _send_stop_reply(
    *, dialog: DialogRow, client: TelegramClient, sender: User
) -> None:
    reply = config(
        "AI_STOP_REPLY",
        default="Понял, извини, что побеспокоил. Больше писать не буду.",
    ).strip()
    _persist_global_opt_out(dialog=dialog, sender=sender, reason="explicit_stop")
    if ai_dry_run():
        logger.info(f"[AI DM DRY RUN stop] user={sender.id}: {reply}")
        _save_message(
            dialog.id,
            "system",
            f"[DRY RUN stop draft] {reply}",
            provider="dry_run",
            model="stop_reply",
        )
        return
    sent = await _safe_send_message(client, sender, reply, "stop_reply")
    if not sent:
        logger.warning(
            f"[AI DM] opt-out saved but stop reply send failed: dialog={dialog.id}, user={sender.id}"
        )
        return
    _save_message(dialog.id, "outgoing", reply, provider="local", model="stop_reply")
    _mark_outgoing(dialog.id)
    logger.info(f"[AI DM] explicit stop persisted: dialog={dialog.id}, user={sender.id}")


async def _handle_stop_without_dialog(
    *,
    dm_task_id: int,
    account_user_id: int,
    client: TelegramClient,
    sender: User,
    contact_cycle_id: Optional[int],
) -> None:
    """Persist an explicit stop even if the AI row is missing or AI is disabled."""
    already_saved = is_opted_out(int(sender.id))
    add_opt_out(
        int(sender.id),
        reason="explicit_stop_without_ai_dialog",
        source_account_user_id=int(account_user_id),
        source_dm_task_id=int(dm_task_id),
        username=getattr(sender, "username", None),
        first_name=getattr(sender, "first_name", None),
    )
    mark_contact_opted_out(contact_cycle_id, "explicit_stop_without_ai_dialog")
    if already_saved:
        return

    reply = config(
        "AI_STOP_REPLY",
        default="Понял, извини, что побеспокоил. Больше писать не буду.",
    ).strip()
    if ai_dry_run():
        logger.info(f"[AI DM DRY RUN stop without dialog] user={sender.id}: {reply}")
        return
    sent = await _safe_send_message(client, sender, reply, "stop_without_ai_dialog")
    if not sent:
        logger.warning(
            f"[AI DM] opt-out saved but stop reply without dialog failed: user={sender.id}"
        )


async def handle_private_incoming(
    *,
    dm_task_id: int,
    account_user_id: int,
    client: TelegramClient,
    sender: User,
    text: str,
    message_id: int | None = None,
    media_kind: str | None = None,
) -> None:
    """Handle replies through the Maxim context-aware sales funnel.

    The first DM is still selected and sent exclusively by the existing DM path.
    This function starts only after the recipient replies to that delivered DM.
    """
    text = (text or "").strip()
    if not text and media_kind:
        text = make_media_reaction_text(media_kind)
    if not text:
        return

    # De-duplication and explicit opt-out remain active even when AI replies are
    # temporarily disabled. A direct request to stop must never be lost.
    create_ai_tables()
    if not _claim_incoming_message(account_user_id, sender.id, message_id):
        logger.debug(
            f"[AI DM] duplicate private message ignored: "
            f"account={account_user_id}, user={sender.id}, message_id={message_id}"
        )
        return

    contact_cycle_id = mark_latest_first_reply(account_user_id, sender.id)
    stop_words = _csv_words("AI_STOP_WORDS", STOP_WORDS_DEFAULT)
    explicit_stop = is_explicit_stop(text, stop_words)

    lock = _get_dialog_lock(account_user_id, sender.id)
    async with lock:
        dialog = _get_dialog_by_target(account_user_id, sender.id)

        if not ai_enabled():
            if explicit_stop:
                if dialog:
                    _save_message(dialog.id, "incoming", text, provider="telegram")
                    _mark_incoming(dialog.id)
                    mark_contact_first_reply(dialog.contact_cycle_id)
                    touch_contact_cycle(dialog.contact_cycle_id)
                    if _dialog_has_sent_offer(dialog.id):
                        await _send_post_offer_apology(
                            dialog=dialog, client=client, sender=sender
                        )
                    else:
                        await _send_stop_reply(
                            dialog=dialog, client=client, sender=sender
                        )
                else:
                    await _handle_stop_without_dialog(
                        dm_task_id=dm_task_id,
                        account_user_id=account_user_id,
                        client=client,
                        sender=sender,
                        contact_cycle_id=contact_cycle_id,
                    )
            return

        if not dialog:
            if explicit_stop:
                await _handle_stop_without_dialog(
                    dm_task_id=dm_task_id,
                    account_user_id=account_user_id,
                    client=client,
                    sender=sender,
                    contact_cycle_id=contact_cycle_id,
                )
                return
            if _truthy("AI_REPLY_ONLY_KNOWN_DIALOGS", "true"):
                return
            if _daily_dialog_limit_reached():
                logger.warning(
                    f"[AI DM] daily dialog limit reached; skip incoming user={sender.id}"
                )
                return
            dialog = _upsert_dialog(
                dm_task_id=dm_task_id,
                account_user_id=account_user_id,
                target_user_id=sender.id,
                username=getattr(sender, "username", None),
                first_name=getattr(sender, "first_name", None),
                contact_cycle_id=contact_cycle_id,
            )

        human_words = _csv_words("AI_HUMAN_TAKEOVER_WORDS", HUMAN_WORDS_DEFAULT)
        offer_already_sent = _dialog_has_sent_offer(dialog.id)

        # Self-heal a rare partial-commit state: if the contact protection was
        # saved but the AI row stayed active, close the AI side before processing
        # another ordinary message. Explicit stop is still honoured below.
        if dialog.status == "active" and is_completed_contact(
            account_user_id, sender.id
        ):
            _set_dialog_status(
                dialog.id,
                "completed",
                "completed_contact_guard",
                stage="completed",
            )
            dialog = _get_dialog_by_id(dialog.id) or dialog

        # A completed dialog is truly closed. The only later message we still
        # process is an explicit request never to be contacted again.
        if dialog.status != "active":
            if explicit_stop and dialog.status != "closed_negative":
                _save_message(dialog.id, "incoming", text, provider="telegram")
                _mark_incoming(dialog.id)
                mark_contact_first_reply(dialog.contact_cycle_id)
                touch_contact_cycle(dialog.contact_cycle_id)
                if offer_already_sent:
                    await _send_post_offer_apology(
                        dialog=dialog, client=client, sender=sender
                    )
                else:
                    await _send_stop_reply(dialog=dialog, client=client, sender=sender)
            return

        _save_message(dialog.id, "incoming", text, provider="telegram")
        _mark_incoming(dialog.id)
        mark_contact_first_reply(dialog.contact_cycle_id)
        touch_contact_cycle(dialog.contact_cycle_id)

        if explicit_stop:
            if offer_already_sent:
                await _send_post_offer_apology(
                    dialog=dialog, client=client, sender=sender
                )
            else:
                await _send_stop_reply(dialog=dialog, client=client, sender=sender)
            return

        if is_human_takeover_request(text, human_words):
            reply = config(
                "AI_HUMAN_TAKEOVER_REPLY",
                default="Понял, лучше передам человеку, чтобы ответили точнее.",
            ).strip()
            if ai_dry_run():
                logger.info(f"[AI DM DRY RUN human] user={sender.id}: {reply}")
                _save_message(
                    dialog.id,
                    "system",
                    f"[DRY RUN human draft] {reply}",
                    provider="dry_run",
                    model="human_takeover",
                )
                return
            sent = await _safe_send_message(client, sender, reply, "human_takeover")
            if not sent:
                _set_dialog_status(
                    dialog.id, "send_error", "telegram_send_failed", stage="send_error"
                )
                return
            _save_message(
                dialog.id, "outgoing", reply, provider="local", model="human_takeover"
            )
            _mark_outgoing(dialog.id)
            _set_dialog_status(
                dialog.id, "human_needed", "human_takeover", stage="human_needed"
            )
            logger.info(
                f"[AI DM] human takeover: dialog={dialog.id}, user={sender.id}"
            )
            return

        if offer_already_sent:
            await _reply_delay()
            dialog = _get_dialog_by_id(dialog.id) or dialog
            if dialog.status != "active":
                return

            history = _current_cycle_history(dialog.id)
            try:
                plan = await generate_post_link_plan(
                    history=history,
                    source_chat_title=dialog.source_chat_title,
                )
            except Exception as exc:
                logger.error(
                    f"[AI DM] post-link generation error for dialog={dialog.id}, "
                    f"user={sender.id}: {exc}"
                )
                plan = FunnelPlan(
                    action="post_link_final",
                    next_stage="completed",
                    close_after=True,
                    messages=post_link_final_messages(
                        text, source_chat_title=dialog.source_chat_title
                    ),
                    model="local_post_link_fallback",
                )
            sent = await _send_maxim_plan(
                dialog=dialog, client=client, sender=sender, plan=plan
            )
            if not sent:
                return
            completion_reason = (
                "completed_no_interest"
                if plan.action == "soft_decline"
                else "natural_finish_after_link"
            )
            _finalize_completed_dialog(dialog, completion_reason)
            logger.info(
                f"[AI DM] post-link dialog completed: dialog={dialog.id}, user={sender.id}"
            )
            return

        await _reply_delay()

        # Re-read state after the human-like delay to avoid racing parallel events.
        dialog = _get_dialog_by_id(dialog.id) or dialog
        if dialog.status != "active" or _dialog_has_sent_offer(dialog.id):
            return

        max_followups = _safe_int(
            "AI_MAX_FOLLOWUP_MESSAGES", 7, min_value=3, max_value=12
        )
        followup_count = _current_cycle_followup_count(dialog.id)
        # The configured number limits ordinary follow-ups. If an old or unusual
        # cycle has already reached it without a link, allow exactly one final
        # link-bearing response instead of silently closing while the user is active.
        effective_followup_count = min(followup_count, max_followups - 1)
        if followup_count >= max_followups:
            logger.warning(
                f"[AI DM] ordinary follow-up cap reached; using one final closure "
                f"reply: dialog={dialog.id}, user={sender.id}, count={followup_count}"
            )

        history = _current_cycle_history(dialog.id)
        try:
            plan = await generate_plan(
                stage=dialog.stage,
                history=history,
                source_chat_title=dialog.source_chat_title,
                followup_count=effective_followup_count,
                max_followups=max_followups,
            )
        except Exception as exc:
            logger.error(
                f"[AI DM] Maxim generation error for dialog={dialog.id}, user={sender.id}: {exc}"
            )
            plan = build_local_plan(
                stage=dialog.stage,
                history=history,
                source_chat_title=dialog.source_chat_title,
                followup_count=effective_followup_count,
                max_followups=max_followups,
            )

        remaining = max(1, max_followups - followup_count)
        plan = _fit_plan_to_remaining(plan, remaining)
        sent = await _send_maxim_plan(
            dialog=dialog,
            client=client,
            sender=sender,
            plan=plan,
        )
        if not sent:
            return

        has_link = any(PIRATE_VIP_LINK_TOKEN in message for message in plan.messages)
        if has_link:
            _set_dialog_status(
                dialog.id,
                "active",
                "link_sent_waiting_final",
                stage="post_link_active",
            )
            mark_contact_link_sent(dialog.contact_cycle_id)
            logger.info(
                f"[AI DM] Maxim link sent; waiting for final reply or timeout: "
                f"dialog={dialog.id}, user={sender.id}, action={plan.action}"
            )
        elif plan.close_after:
            completion_reason = (
                "completed_no_interest"
                if plan.action == "soft_decline"
                else "logical_finish"
            )
            _finalize_completed_dialog(dialog, completion_reason)
            logger.info(
                f"[AI DM] Maxim dialog completed without link: "
                f"dialog={dialog.id}, user={sender.id}, action={plan.action}"
            )
        else:
            _set_stage(dialog.id, plan.next_stage)
            logger.info(
                f"[AI DM] Maxim step sent: dialog={dialog.id}, user={sender.id}, "
                f"action={plan.action}, next_stage={plan.next_stage}"
            )

def stop_dialog_by_user(target_user_id: int, reason: str = "admin_stop") -> bool:
    cursor = conn.cursor()
    cursor.execute(
        """UPDATE ai_dialogs SET status='admin_stopped', stopped_reason=?, updated_at=?
           WHERE target_user_id=? AND status='active'""",
        (reason, _now_iso(), target_user_id),
    )
    affected = cursor.rowcount
    conn.commit()
    cursor.close()
    return affected > 0


def resume_dialog_by_user(target_user_id: int) -> bool:
    cursor = conn.cursor()
    cursor.execute(
        """UPDATE ai_dialogs SET status='active', stopped_reason=NULL, updated_at=?
           WHERE target_user_id=?""",
        (_now_iso(), target_user_id),
    )
    affected = cursor.rowcount
    conn.commit()
    cursor.close()
    return affected > 0


def clear_opt_out_dialog_state_by_user(target_user_id: int) -> bool:
    """Make an explicitly stopped dialog reopenable by a future first DM.

    The dialog is not made active immediately, so an old private message cannot
    restart the funnel by itself after an administrator removes the opt-out.
    """
    cursor = conn.cursor()
    cursor.execute(
        """UPDATE ai_dialogs
           SET status='completed', stage='completed', stopped_reason=NULL, updated_at=?
           WHERE target_user_id=? AND status='closed_negative'""",
        (_now_iso(), target_user_id),
    )
    affected = cursor.rowcount
    conn.commit()
    cursor.close()
    return affected > 0


def ai_stats() -> dict[str, Any]:
    create_ai_tables()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM ai_dialogs")
    total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM ai_dialogs WHERE status='active'")
    active = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM ai_messages WHERE date(created_at) = date('now')")
    messages_today = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM ai_dialogs WHERE date(created_at) = date('now')")
    dialogs_today = cursor.fetchone()[0]
    cursor.close()
    return {
        "enabled": ai_enabled(),
        "dry_run": ai_dry_run(),
        "model": config("AI_MODEL", default="gpt-4o-mini"),
        "total_dialogs": total,
        "active_dialogs": active,
        "messages_today": messages_today,
        "dialogs_today": dialogs_today,
        "daily_dialog_limit": _safe_int("AI_DAILY_DIALOG_LIMIT", 0, min_value=0),
        "funnel_mode": "maxim_context_sales",
        "persona": "Максим",
        "max_followups": _safe_int("AI_MAX_FOLLOWUP_MESSAGES", 7, min_value=3, max_value=12),
        "free_source_count": _safe_int("AI_FREE_VIP_SOURCE_COUNT", 6, min_value=1, max_value=999),
        "paid_source_count": _safe_int("AI_PAID_VIP_SOURCE_COUNT", 50, min_value=1, max_value=9999),
        "close_after_offer": True,
        "post_offer_apology": True,
        "persistent_opt_out_users": opt_out_count(),
    }


def recent_dialogs(limit: int = 10) -> list[tuple]:
    create_ai_tables()
    cursor = conn.cursor()
    cursor.execute(
        """SELECT target_user_id, username, first_name, stage, status, message_count, updated_at
           FROM ai_dialogs ORDER BY updated_at DESC LIMIT ?""",
        (limit,),
    )
    rows = cursor.fetchall()
    cursor.close()
    return rows


def export_dialogs_text(limit: int = 200) -> str:
    create_ai_tables()
    cursor = conn.cursor()
    cursor.execute(
        """SELECT d.target_user_id, d.username, d.stage, d.status, m.direction, m.message_text, m.created_at
           FROM ai_messages m
           JOIN ai_dialogs d ON d.id = m.dialog_id
           ORDER BY m.id DESC LIMIT ?""",
        (limit,),
    )
    rows = list(reversed(cursor.fetchall()))
    cursor.close()
    lines = ["AI dialogs export"]
    for uid, username, stage, status, direction, text, created in rows:
        lines.append(f"[{created}] user={uid} @{username or '-'} stage={stage} status={status} {direction}: {text}")
    return "\n".join(lines)
