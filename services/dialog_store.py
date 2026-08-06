"""Persistent dialog state after first DM."""

from __future__ import annotations

import datetime as dt
import json
from typing import Any, Optional

from config import LOCAL_DIALOG_TEXT_RETENTION_DAYS
from services import dialog_retention_policy
from db.schema import db_lock, get_connection

STAGE_FIRST_DM_SENDING = "first_dm_sending"
STAGE_WAITING_REPLY = "waiting_reply"
STAGE_FOLLOWUP_SENT = "followup_sent"
STAGE_ENGAGED = "engaged"
STAGE_EXPLAINED = "explained"  # legacy pre-v1.0.55 state
STAGE_LINK_SENT = "link_sent"  # legacy pre-v1.0.55 terminal state
STAGE_PROMO_SENT = "promo_sent"
STAGE_APOLOGY_SENT = "apology_sent"
STAGE_LINK_HELP_SENT = "link_help_sent"
STAGE_CLOSED = "closed"

ACTIVE_STAGES = (
    STAGE_FIRST_DM_SENDING,
    STAGE_WAITING_REPLY,
    STAGE_ENGAGED,
    STAGE_EXPLAINED,
    STAGE_PROMO_SENT,
    STAGE_APOLOGY_SENT,
    STAGE_LINK_HELP_SENT,
)
TERMINAL_STAGES = (STAGE_FOLLOWUP_SENT, STAGE_LINK_SENT, STAGE_CLOSED)


def is_active_stage(stage: str | None) -> bool:
    return str(stage or "") in ACTIVE_STAGES


def is_terminal_stage(stage: str | None) -> bool:
    return str(stage or "") in TERMINAL_STAGES

# Silence after first DM before soft follow-up (hours)
FOLLOWUP_DELAY_HOURS = 24

MAX_OUTGOING = 5


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def get_dialog(target_user_id: int) -> Optional[dict[str, Any]]:
    conn = get_connection()
    row = conn.execute(
        """
        SELECT target_user_id, account_user_id, stage, outgoing_count,
               link_sent, auto_link_at, history_json, updated_at,
               first_dm_at, first_dm_message_id, telegram_delete_at,
               telegram_deleted_at, telegram_delete_next_attempt_at,
               telegram_delete_attempts, telegram_delete_last_error,
               history_purge_at, history_purged_at, lifecycle_completed_at,
               last_message_at, telegram_delete_abandoned_at
          FROM dialogs
         WHERE target_user_id=?
        """,
        (int(target_user_id),),
    ).fetchone()
    if not row:
        return None
    data = dict(row)
    try:
        data["history"] = json.loads(data.get("history_json") or "[]")
    except (TypeError, json.JSONDecodeError):
        data["history"] = []
    return data


def create_after_first_dm(target_user_id: int, account_user_id: int, first_text: str) -> None:
    now = _now_iso()
    # Reuse auto_link_at while stage=waiting_reply as "follow-up due at"
    first_dm_at = now
    follow_at = (_now() + dt.timedelta(hours=FOLLOWUP_DELAY_HOURS)).isoformat()
    telegram_delete_at = dialog_retention_policy.due_at(first_dm_at)
    history_purge_at = (
        _now() + dt.timedelta(days=LOCAL_DIALOG_TEXT_RETENTION_DAYS)
    ).isoformat()
    history = [{"role": "assistant", "text": first_text, "at": first_dm_at}]
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            INSERT INTO dialogs (
                target_user_id, account_user_id, stage, outgoing_count,
                link_sent, auto_link_at, history_json, updated_at,
                first_dm_at, last_message_at, telegram_delete_at, history_purge_at
            ) VALUES (?, ?, ?, 1, 0, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(target_user_id) DO UPDATE SET
                account_user_id=excluded.account_user_id,
                stage=excluded.stage,
                outgoing_count=1,
                link_sent=0,
                auto_link_at=excluded.auto_link_at,
                history_json=excluded.history_json,
                first_dm_at=COALESCE(dialogs.first_dm_at, excluded.first_dm_at),
                last_message_at=CASE
                    WHEN dialogs.last_message_at IS NULL
                    THEN excluded.last_message_at ELSE dialogs.last_message_at
                END,
                telegram_delete_at=CASE
                    WHEN dialogs.telegram_delete_at IS NULL
                    THEN excluded.telegram_delete_at ELSE dialogs.telegram_delete_at
                END,
                history_purge_at=COALESCE(
                    dialogs.history_purge_at, excluded.history_purge_at
                ),
                lifecycle_completed_at=NULL,
                telegram_delete_abandoned_at=NULL,
                updated_at=excluded.updated_at
            """,
            (
                int(target_user_id),
                int(account_user_id),
                STAGE_WAITING_REPLY,
                follow_at,
                json.dumps(history, ensure_ascii=False),
                now,
                first_dm_at,
                first_dm_at,
                telegram_delete_at,
                history_purge_at,
            ),
        )
        dialog_retention_policy.touch_dialog_activity(
            conn, int(target_user_id), first_dm_at
        )


def has_incoming_reply(
    target_user_id: int,
    account_user_id: int | None = None,
) -> bool:
    """Return durable proof that the current dialog received a user reply.

    The inbox row is written before processing, so pending messages count and the
    proof survives restarts. History is a compatibility fallback for older rows.
    """
    target = int(target_user_id)
    dialog = get_dialog(target)
    if not dialog:
        return False
    owner = int(dialog.get("account_user_id") or 0)
    if account_user_id is not None and owner != int(account_user_id):
        return False
    first_dm_at = str(dialog.get("first_dm_at") or "")
    conn = get_connection()
    params: list[Any] = [target, owner]
    received_filter = ""
    if first_dm_at:
        received_filter = " AND julianday(received_at)>=julianday(?)"
        params.append(first_dm_at)
    row = conn.execute(
        f"""
        SELECT 1 FROM dialog_inbox
         WHERE target_user_id=? AND account_user_id=?{received_filter}
         LIMIT 1
        """,
        tuple(params),
    ).fetchone()
    if row is not None:
        return True

    first_dt: dt.datetime | None = None
    if first_dm_at:
        try:
            first_dt = dt.datetime.fromisoformat(first_dm_at.replace("Z", "+00:00"))
            if first_dt.tzinfo is None:
                first_dt = first_dt.replace(tzinfo=dt.timezone.utc)
        except (TypeError, ValueError):
            first_dt = None
    for item in dialog.get("history") or []:
        if not isinstance(item, dict) or str(item.get("role") or "") != "user":
            continue
        if first_dt is None:
            return True
        try:
            item_dt = dt.datetime.fromisoformat(str(item.get("at") or "").replace("Z", "+00:00"))
            if item_dt.tzinfo is None:
                item_dt = item_dt.replace(tzinfo=dt.timezone.utc)
        except (TypeError, ValueError):
            continue
        if item_dt >= first_dt:
            return True
    return False


def append_history(target_user_id: int, role: str, text: str) -> None:
    conn = get_connection()
    with db_lock(), conn:
        row = conn.execute(
            "SELECT history_json, history_purge_at FROM dialogs WHERE target_user_id=?",
            (int(target_user_id),),
        ).fetchone()
        if not row:
            return
        try:
            history = json.loads(row["history_json"] or "[]")
        except (TypeError, json.JSONDecodeError):
            history = []
        if not isinstance(history, list):
            history = []
        now = _now_iso()
        history.append({"role": role, "text": text, "at": now})
        history = history[-20:]
        purge_due = (
            dt.datetime.fromisoformat(now) + dt.timedelta(days=LOCAL_DIALOG_TEXT_RETENTION_DAYS)
        ).isoformat()
        conn.execute(
            """
            UPDATE dialogs
               SET history_json=?, updated_at=?,
                   history_purge_at=CASE
                       WHEN history_purge_at IS NULL OR history_purge_at>? THEN ?
                       ELSE history_purge_at
                   END,
                   history_purged_at=NULL
             WHERE target_user_id=?
            """,
            (
                json.dumps(history, ensure_ascii=False),
                now,
                purge_due,
                purge_due,
                int(target_user_id),
            ),
        )


def set_stage(
    target_user_id: int,
    stage: str,
    *,
    bump_outgoing: bool = False,
    link_sent: bool | None = None,
    auto_link_at: str | None = None,
    clear_auto_link: bool = False,
) -> None:
    d = get_dialog(target_user_id)
    if not d:
        return
    # Closed is terminal for normal dialog processing. Explicit requeue/import
    # creates a fresh dialog separately and must not be achieved by a late task.
    if d.get("stage") == STAGE_CLOSED and stage != STAGE_CLOSED:
        return
    outgoing = int(d.get("outgoing_count") or 0)
    if bump_outgoing:
        outgoing += 1
    ls = int(d.get("link_sent") or 0)
    if link_sent is not None:
        ls = 1 if link_sent else 0
    ala = d.get("auto_link_at")
    if clear_auto_link:
        ala = None
    elif auto_link_at is not None:
        ala = auto_link_at
    now = _now_iso()
    completed_at = d.get("lifecycle_completed_at")
    if is_active_stage(stage):
        completed_at = None
    elif is_terminal_stage(stage) and not completed_at:
        completed_at = now
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE dialogs
               SET stage=?,
                   outgoing_count=?,
                   link_sent=?,
                   auto_link_at=?,
                   lifecycle_completed_at=?,
                   updated_at=?
             WHERE target_user_id=?
            """,
            (
                stage,
                outgoing,
                ls,
                ala,
                completed_at,
                now,
                int(target_user_id),
            ),
        )

def list_due_auto_links(limit: int = 50) -> list[dict[str, Any]]:
    """Return due legacy links, apologies and link-opening instructions.

    The historical function name is retained for the main loop. New dialogs use
    STAGE_PROMO_SENT for the apology deadline and STAGE_APOLOGY_SENT for the
    following link-help deadline. Legacy STAGE_EXPLAINED rows remain recoverable.
    """
    now = _now_iso()
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT target_user_id, account_user_id, stage, outgoing_count,
               link_sent, auto_link_at, history_json, updated_at,
               first_dm_at, first_dm_message_id, telegram_delete_at,
               telegram_deleted_at, telegram_delete_next_attempt_at,
               telegram_delete_attempts, telegram_delete_last_error,
               history_purge_at, history_purged_at, last_message_at
          FROM dialogs
         WHERE auto_link_at IS NOT NULL
           AND auto_link_at <= ?
           AND (
                (stage=? AND link_sent=0)
                OR
                (stage=? AND link_sent=1)
                OR
                (stage=? AND link_sent=1)
           )
         ORDER BY auto_link_at ASC, target_user_id ASC
         LIMIT ?
        """,
        (
            now,
            STAGE_EXPLAINED,
            STAGE_PROMO_SENT,
            STAGE_APOLOGY_SENT,
            max(1, int(limit)),
        ),
    ).fetchall()
    result = []
    for r in rows:
        data = dict(r)
        try:
            data["history"] = json.loads(data.get("history_json") or "[]")
        except (TypeError, json.JSONDecodeError):
            data["history"] = []
        result.append(data)
    return result


def list_due_followups(limit: int = 50) -> list[dict[str, Any]]:
    """waiting_reply dialogs past follow-up deadline (stored in auto_link_at)."""
    now = _now_iso()
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT target_user_id, account_user_id, stage, outgoing_count,
               link_sent, auto_link_at, history_json, updated_at,
               first_dm_at, first_dm_message_id, telegram_delete_at,
               telegram_deleted_at, telegram_delete_next_attempt_at,
               telegram_delete_attempts, telegram_delete_last_error,
               history_purge_at, history_purged_at, last_message_at
          FROM dialogs
         WHERE stage=?
           AND auto_link_at IS NOT NULL
           AND auto_link_at <= ?
         ORDER BY auto_link_at ASC, target_user_id ASC
         LIMIT ?
        """,
        (STAGE_WAITING_REPLY, now, max(1, int(limit))),
    ).fetchall()
    result = []
    for r in rows:
        data = dict(r)
        try:
            data["history"] = json.loads(data.get("history_json") or "[]")
        except (TypeError, json.JSONDecodeError):
            data["history"] = []
        result.append(data)
    return result


def count_active() -> int:
    return count_by_stage(*ACTIVE_STAGES)


def mark_contact_completed(target_user_id: int) -> None:
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE contacts
               SET status='completed', updated_at=?
             WHERE target_user_id=?
            """,
            (_now_iso(), int(target_user_id)),
        )


def count_open_for_account(account_user_id: int) -> int:
    """Count only dialogs that still require a live Telegram client."""
    conn = get_connection()
    placeholders = ",".join("?" for _ in ACTIVE_STAGES)
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS c
          FROM dialogs
         WHERE account_user_id=?
           AND stage IN ({placeholders})
        """,
        (int(account_user_id), *ACTIVE_STAGES),
    ).fetchone()
    return int(row["c"] if row else 0)


def has_open_for_account(account_user_id: int) -> bool:
    return count_open_for_account(account_user_id) > 0


def count_retention_waiting_for_account(account_user_id: int) -> int:
    """Completed attempts that still wait for Telegram cleanup."""
    conn = get_connection()
    placeholders = ",".join("?" for _ in TERMINAL_STAGES)
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS c FROM dialogs
         WHERE account_user_id=?
           AND stage IN ({placeholders})
           AND telegram_deleted_at IS NULL
           AND telegram_delete_abandoned_at IS NULL
           AND telegram_delete_at IS NOT NULL
        """,
        (int(account_user_id), *TERMINAL_STAGES),
    ).fetchone()
    return int(row["c"] if row else 0)


def close_for_opt_out(target_user_id: int) -> None:
    """Keep dialog history but make every scheduled action inactive."""
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE dialogs
               SET stage=?, auto_link_at=NULL,
                   lifecycle_completed_at=COALESCE(lifecycle_completed_at, ?),
                   updated_at=?
             WHERE target_user_id=?
            """,
            (STAGE_CLOSED, now, now, int(target_user_id)),
        )
        conn.execute(
            """
            UPDATE contacts
               SET status='completed', updated_at=?
             WHERE target_user_id=?
            """,
            (now, int(target_user_id)),
        )
        conn.execute(
            "DELETE FROM dialog_outbox WHERE target_user_id=?",
            (int(target_user_id),),
        )


def close_open_for_account(account_user_id: int) -> int:
    """Archive open dialogs on account deletion without erasing their history."""
    now = _now_iso()
    uid = int(account_user_id)
    conn = get_connection()
    with db_lock(), conn:
        placeholders = ",".join("?" for _ in ACTIVE_STAGES)
        rows = conn.execute(
            f"""
            SELECT target_user_id
              FROM dialogs
             WHERE account_user_id=? AND stage IN ({placeholders})
            """,
            (uid, *ACTIVE_STAGES),
        ).fetchall()
        targets = [int(r["target_user_id"]) for r in rows]
        conn.execute(
            f"""
            UPDATE dialogs
               SET stage=?, auto_link_at=NULL,
                   lifecycle_completed_at=COALESCE(lifecycle_completed_at, ?),
                   updated_at=?
             WHERE account_user_id=? AND stage IN ({placeholders})
            """,
            (STAGE_CLOSED, now, now, uid, *ACTIVE_STAGES),
        )
        if targets:
            placeholders = ",".join("?" for _ in targets)
            conn.execute(
                f"""
                UPDATE contacts
                   SET status='completed', updated_at=?
                 WHERE target_user_id IN ({placeholders})
                """,
                (now, *targets),
            )
        return len(targets)

def count_by_stage(*stages: str) -> int:
    if not stages:
        return 0
    conn = get_connection()
    placeholders = ",".join("?" for _ in stages)
    row = conn.execute(
        f"SELECT COUNT(*) AS c FROM dialogs WHERE stage IN ({placeholders})",
        tuple(stages),
    ).fetchone()
    return int(row["c"] if row else 0)


def count_closed_today() -> int:
    admin_tz = dt.timezone(dt.timedelta(hours=3))
    local_now = dt.datetime.now(admin_tz)
    local_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_utc = local_start.astimezone(dt.timezone.utc).isoformat()
    end_utc = (local_start + dt.timedelta(days=1)).astimezone(dt.timezone.utc).isoformat()
    conn = get_connection()
    placeholders = ",".join("?" for _ in TERMINAL_STAGES)
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS c FROM dialogs
         WHERE stage IN ({placeholders})
           AND lifecycle_completed_at >= ? AND lifecycle_completed_at < ?
        """,
        (*TERMINAL_STAGES, start_utc, end_utc),
    ).fetchone()
    return int(row["c"] if row else 0)


def list_recent(*, active_only: bool = True, limit: int = 15) -> list[dict[str, Any]]:
    conn = get_connection()
    stages = ACTIVE_STAGES if active_only else TERMINAL_STAGES
    placeholders = ",".join("?" for _ in stages)
    order_field = "d.updated_at" if active_only else "d.lifecycle_completed_at"
    rows = conn.execute(
        f"""
        SELECT d.target_user_id, d.account_user_id, d.stage, d.outgoing_count,
               d.link_sent, d.updated_at, d.first_dm_at, d.telegram_delete_at,
               d.history_purge_at, d.lifecycle_completed_at,
               a.username, a.first_name, a.last_name
          FROM dialogs d
          LEFT JOIN audience a ON a.user_id=d.target_user_id
         WHERE d.stage IN ({placeholders})
         ORDER BY {order_field} DESC
         LIMIT ?
        """,
        (*stages, int(limit)),
    ).fetchall()
    return [dict(r) for r in rows]


def list_recent_closed(limit: int = 15) -> list[dict[str, Any]]:
    return list_recent(active_only=False, limit=limit)
