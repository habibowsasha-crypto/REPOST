"""Durable First-DM delivery journal and crash reconciliation state."""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

from config import LOCAL_DIALOG_TEXT_RETENTION_DAYS
from db.schema import db_lock, get_connection
from services import dialog_retention_policy
from services import phrases as phrases_svc

STATUS_PREPARED = "prepared"
STATUS_SENT = "sent"
STATUS_FAILED = "failed"

PROVISIONAL_STAGE = "first_dm_sending"
WAITING_STAGE = "waiting_reply"
FOLLOWUP_DELAY_HOURS = 24


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def prepare(
    target_user_id: int,
    account_user_id: int,
    text: str,
) -> bool:
    """Atomically prepare a First DM before touching Telegram.

    A provisional dialog is created in the same SQLite transaction. Therefore the
    network send is never allowed to happen without recoverable dialog state.
    """
    target = int(target_user_id)
    account = int(account_user_id)
    now = _now_iso()
    provisional_purge_at = (
        _now() + dt.timedelta(days=LOCAL_DIALOG_TEXT_RETENTION_DAYS)
    ).isoformat()
    history = json.dumps([{"role": "assistant", "text": str(text), "at": now}], ensure_ascii=False)
    conn = get_connection()
    with db_lock(), conn:
        lead = conn.execute(
            "SELECT status FROM leads WHERE target_user_id=?",
            (target,),
        ).fetchone()
        if not lead or str(lead["status"]) != "claimed":
            return False

        contact = conn.execute(
            "SELECT status FROM contacts WHERE target_user_id=?",
            (target,),
        ).fetchone()
        if contact and str(contact["status"]) in {"in_progress", "completed"}:
            return False

        existing = conn.execute(
            "SELECT status FROM first_dm_outbox WHERE target_user_id=?",
            (target,),
        ).fetchone()
        if existing and str(existing["status"]) == STATUS_SENT:
            return False

        conn.execute(
            """
            INSERT INTO first_dm_outbox (
                target_user_id, account_user_id, text, status,
                prepared_at, telegram_message_id, sent_at, last_error, updated_at,
                recovery_attempts, recovery_next_at, recovery_last_error
            ) VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, ?, 0, NULL, NULL)
            ON CONFLICT(target_user_id) DO UPDATE SET
                account_user_id=excluded.account_user_id,
                text=excluded.text,
                status=excluded.status,
                prepared_at=excluded.prepared_at,
                telegram_message_id=NULL,
                sent_at=NULL,
                last_error=NULL,
                updated_at=excluded.updated_at,
                recovery_attempts=0,
                recovery_next_at=NULL,
                recovery_last_error=NULL
            """,
            (target, account, str(text), STATUS_PREPARED, now, now),
        )
        conn.execute(
            """
            INSERT INTO contacts (target_user_id, status, sender_account_id, updated_at)
            VALUES (?, 'sending', ?, ?)
            ON CONFLICT(target_user_id) DO UPDATE SET
                status='sending',
                sender_account_id=excluded.sender_account_id,
                updated_at=excluded.updated_at
            """,
            (target, account, now),
        )
        conn.execute(
            """
            INSERT INTO dialogs (
                target_user_id, account_user_id, stage, outgoing_count,
                link_sent, auto_link_at, history_json, updated_at, history_purge_at
            ) VALUES (?, ?, ?, 0, 0, NULL, ?, ?, ?)
            ON CONFLICT(target_user_id) DO UPDATE SET
                account_user_id=excluded.account_user_id,
                stage=excluded.stage,
                outgoing_count=0,
                link_sent=0,
                auto_link_at=NULL,
                history_json=excluded.history_json,
                history_purge_at=excluded.history_purge_at,
                history_purged_at=NULL,
                updated_at=excluded.updated_at
            """,
            (target, account, PROVISIONAL_STAGE, history, now, provisional_purge_at),
        )
        conn.execute(
            """
            UPDATE leads
               SET claimed_by_account=?, updated_at=?
             WHERE target_user_id=? AND status='claimed'
            """,
            (account, now, target),
        )
        phrases_svc.remember(
            phrases_svc.KIND_FIRST_DM,
            str(text),
            delivery_key=f"first_dm:{target}:{now}",
            conn=conn,
            created_at=now,
        )
    return True


def commit_sent(
    target_user_id: int,
    *,
    telegram_message_id: int | None = None,
    sent_at: str | None = None,
) -> bool:
    """Atomically confirm First DM, lead, contact and dialog as sent."""
    target = int(target_user_id)
    now = _now_iso()
    sent = sent_at or now
    follow_at = (_now() + dt.timedelta(hours=FOLLOWUP_DELAY_HOURS)).isoformat()
    conn = get_connection()
    with db_lock(), conn:
        row = conn.execute(
            """
            SELECT account_user_id, text, status, prepared_at
              FROM first_dm_outbox
             WHERE target_user_id=?
            """,
            (target,),
        ).fetchone()
        if not row:
            return False
        account = int(row["account_user_id"])
        text = str(row["text"] or "")
        if str(row["status"]) == STATUS_SENT:
            return True
        opted_out = conn.execute(
            "SELECT 1 FROM opt_out WHERE user_id=?",
            (target,),
        ).fetchone() is not None
        final_stage = "closed" if opted_out else WAITING_STAGE
        contact_status = "completed" if opted_out else "in_progress"
        final_auto_link = None if opted_out else follow_at
        history = json.dumps([{"role": "assistant", "text": text, "at": sent}], ensure_ascii=False)
        try:
            sent_dt = dt.datetime.fromisoformat(str(sent).replace("Z", "+00:00"))
            if sent_dt.tzinfo is None:
                sent_dt = sent_dt.replace(tzinfo=dt.timezone.utc)
        except (TypeError, ValueError):
            sent_dt = _now()
            sent = sent_dt.isoformat()
        telegram_delete_at = dialog_retention_policy.due_at(sent_dt)
        history_purge_at = (
            sent_dt + dt.timedelta(days=LOCAL_DIALOG_TEXT_RETENTION_DAYS)
        ).isoformat()
        event_key = f"{target}:{account}:{row['prepared_at']}"

        # Bound all previous archived attempts before activating the new one.
        # This prevents an older cleanup job from deleting this new dialog.
        from services import dialog_archive

        dialog_archive.bound_previous_attempts(
            target,
            next_account_user_id=account,
            next_first_dm_message_id=telegram_message_id,
            next_first_dm_at=sent,
            conn=conn,
        )

        conn.execute(
            """
            UPDATE first_dm_outbox
               SET status=?, telegram_message_id=COALESCE(?, telegram_message_id),
                   sent_at=COALESCE(sent_at, ?), last_error=NULL, updated_at=?,
                   recovery_attempts=0, recovery_next_at=NULL,
                   recovery_last_error=NULL
             WHERE target_user_id=?
            """,
            (STATUS_SENT, telegram_message_id, sent, now, target),
        )
        conn.execute(
            """
            UPDATE leads
               SET status='sent', claimed_by_account=?, claimed_at=NULL,
                   last_error=NULL, failure_reason=NULL, failure_at=NULL, updated_at=?
             WHERE target_user_id=?
            """,
            (account, now, target),
        )
        conn.execute(
            "DELETE FROM lead_account_failures WHERE target_user_id=?",
            (target,),
        )
        conn.execute(
            """
            INSERT INTO contacts (target_user_id, status, sender_account_id, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(target_user_id) DO UPDATE SET
                status=excluded.status,
                sender_account_id=excluded.sender_account_id,
                updated_at=excluded.updated_at
            """,
            (target, contact_status, account, now),
        )
        conn.execute(
            """
            INSERT INTO dialogs (
                target_user_id, account_user_id, stage, outgoing_count,
                link_sent, auto_link_at, history_json, updated_at,
                first_dm_at, first_dm_message_id, last_message_at, telegram_delete_at,
                telegram_deleted_at, telegram_delete_next_attempt_at,
                telegram_delete_attempts, telegram_delete_last_error,
                history_purge_at, history_purged_at
            ) VALUES (?, ?, ?, 1, 0, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, 0, NULL, ?, NULL)
            ON CONFLICT(target_user_id) DO UPDATE SET
                account_user_id=excluded.account_user_id,
                stage=excluded.stage,
                outgoing_count=CASE
                    WHEN dialogs.outgoing_count < 1 THEN 1 ELSE dialogs.outgoing_count
                END,
                link_sent=0,
                auto_link_at=CASE
                    WHEN dialogs.auto_link_at IS NULL THEN excluded.auto_link_at
                    ELSE dialogs.auto_link_at
                END,
                history_json=CASE
                    WHEN dialogs.history_json IS NULL OR dialogs.history_json='[]'
                    THEN excluded.history_json ELSE dialogs.history_json
                END,
                first_dm_at=COALESCE(dialogs.first_dm_at, excluded.first_dm_at),
                first_dm_message_id=COALESCE(
                    dialogs.first_dm_message_id, excluded.first_dm_message_id
                ),
                last_message_at=CASE
                    WHEN dialogs.last_message_at IS NULL
                         OR julianday(dialogs.last_message_at) < julianday(excluded.last_message_at)
                    THEN excluded.last_message_at ELSE dialogs.last_message_at
                END,
                telegram_delete_at=excluded.telegram_delete_at,
                telegram_deleted_at=NULL,
                telegram_delete_next_attempt_at=NULL,
                telegram_delete_attempts=0,
                telegram_delete_last_error=NULL,
                telegram_delete_abandoned_at=NULL,
                history_purge_at=excluded.history_purge_at,
                history_purged_at=NULL,
                updated_at=excluded.updated_at
            """,
            (
                target, account, final_stage, final_auto_link, history, now, sent,
                telegram_message_id, sent, telegram_delete_at, history_purge_at,
            ),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO first_dm_events (
                event_key, target_user_id, account_user_id, telegram_message_id,
                sent_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (event_key, target, account, telegram_message_id, sent, now),
        )
        # Idempotent recovery backfill for rows prepared by older versions.
        phrases_svc.remember(
            phrases_svc.KIND_FIRST_DM,
            text,
            delivery_key=f"first_dm:{target}:{row['prepared_at']}",
            conn=conn,
            created_at=sent,
        )
    return True


def confirm_from_incoming(target_user_id: int, account_user_id: int) -> bool:
    """An incoming reply is definitive proof that the First DM was delivered."""
    row = get_prepared(target_user_id)
    if not row or int(row["account_user_id"]) != int(account_user_id):
        return False
    return commit_sent(target_user_id)


def rollback(target_user_id: int, error: str, *, as_pending: bool = True) -> None:
    """Roll back a definitely failed or disproved prepared delivery."""
    target = int(target_user_id)
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE first_dm_outbox
               SET status=?, last_error=?, updated_at=?
             WHERE target_user_id=? AND status=?
            """,
            (STATUS_FAILED, str(error)[:500], now, target, STATUS_PREPARED),
        )
        conn.execute(
            "DELETE FROM contacts WHERE target_user_id=? AND status='sending'",
            (target,),
        )
        conn.execute(
            "DELETE FROM dialogs WHERE target_user_id=? AND stage=?",
            (target, PROVISIONAL_STAGE),
        )
        conn.execute(
            """
            UPDATE leads
               SET status=?, claimed_by_account=NULL, claimed_at=NULL,
                   last_error=?, updated_at=?
             WHERE target_user_id=? AND status='claimed'
            """,
            ("pending" if as_pending else "cancelled", str(error)[:500], now, target),
        )


def get_prepared(target_user_id: int) -> dict[str, Any] | None:
    conn = get_connection()
    row = conn.execute(
        """
        SELECT target_user_id, account_user_id, text, status, prepared_at,
               telegram_message_id, sent_at, last_error, updated_at,
               recovery_attempts, recovery_next_at, recovery_last_error
          FROM first_dm_outbox
         WHERE target_user_id=? AND status=?
        """,
        (int(target_user_id), STATUS_PREPARED),
    ).fetchone()
    return dict(row) if row else None


def defer_recovery(
    target_user_id: int,
    error: str,
    *,
    delay_seconds: int,
) -> int:
    """Keep an ambiguous First DM safe while backing off the next history check."""
    target = int(target_user_id)
    now = _now()
    next_at = (now + dt.timedelta(seconds=max(1, int(delay_seconds)))).isoformat()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE first_dm_outbox
               SET recovery_attempts=COALESCE(recovery_attempts, 0) + 1,
                   recovery_next_at=?, recovery_last_error=?, updated_at=?
             WHERE target_user_id=? AND status=?
            """,
            (next_at, str(error)[:500], now.isoformat(), target, STATUS_PREPARED),
        )
        row = conn.execute(
            "SELECT recovery_attempts FROM first_dm_outbox WHERE target_user_id=?",
            (target,),
        ).fetchone()
    return int(row["recovery_attempts"] or 0) if row else 0


def list_stale_prepared(*, older_than_seconds: int = 45, limit: int = 20) -> list[dict[str, Any]]:
    now = _now()
    cutoff = (now - dt.timedelta(seconds=max(1, int(older_than_seconds)))).isoformat()
    rows = get_connection().execute(
        """
        SELECT o.target_user_id, o.account_user_id, o.text, o.prepared_at,
               o.recovery_attempts, o.recovery_next_at, o.recovery_last_error,
               l.username, l.first_name, l.last_name, l.access_hash,
               l.source_chat_id, l.source_account_user_id
          FROM first_dm_outbox o
          LEFT JOIN leads l ON l.target_user_id=o.target_user_id
         WHERE o.status=? AND o.prepared_at <= ?
           AND (o.recovery_next_at IS NULL OR o.recovery_next_at <= ?)
         ORDER BY o.prepared_at ASC
         LIMIT ?
        """,
        (STATUS_PREPARED, cutoff, now.isoformat(), int(limit)),
    ).fetchall()
    return [dict(r) for r in rows]


def has_prepared(target_user_id: int) -> bool:
    return get_prepared(target_user_id) is not None
