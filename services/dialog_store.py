"""Persistent dialog state after first DM."""

from __future__ import annotations

import datetime as dt
import json
from typing import Any, Optional

from config import LOCAL_DIALOG_TEXT_RETENTION_DAYS, TELEGRAM_DIALOG_DELETE_DAYS
from db.schema import db_lock, get_connection

STAGE_FIRST_DM_SENDING = "first_dm_sending"
STAGE_WAITING_REPLY = "waiting_reply"
STAGE_FOLLOWUP_SENT = "followup_sent"
STAGE_ENGAGED = "engaged"
STAGE_EXPLAINED = "explained"
STAGE_LINK_SENT = "link_sent"
STAGE_CLOSED = "closed"

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
               history_purge_at, history_purged_at
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
    telegram_delete_at = (
        _now() + dt.timedelta(days=TELEGRAM_DIALOG_DELETE_DAYS)
    ).isoformat()
    history_purge_at = (
        _now() + dt.timedelta(days=LOCAL_DIALOG_TEXT_RETENTION_DAYS)
    ).isoformat()
    history = [{"role": "assistant", "text": first_text}]
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            INSERT INTO dialogs (
                target_user_id, account_user_id, stage, outgoing_count,
                link_sent, auto_link_at, history_json, updated_at,
                first_dm_at, telegram_delete_at, history_purge_at
            ) VALUES (?, ?, ?, 1, 0, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(target_user_id) DO UPDATE SET
                account_user_id=excluded.account_user_id,
                stage=excluded.stage,
                outgoing_count=1,
                link_sent=0,
                auto_link_at=excluded.auto_link_at,
                history_json=excluded.history_json,
                first_dm_at=COALESCE(dialogs.first_dm_at, excluded.first_dm_at),
                telegram_delete_at=COALESCE(
                    dialogs.telegram_delete_at, excluded.telegram_delete_at
                ),
                history_purge_at=COALESCE(
                    dialogs.history_purge_at, excluded.history_purge_at
                ),
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
                telegram_delete_at,
                history_purge_at,
            ),
        )


def append_history(target_user_id: int, role: str, text: str) -> None:
    d = get_dialog(target_user_id)
    if not d:
        return
    history = list(d.get("history") or [])
    history.append({"role": role, "text": text})
    history = history[-20:]
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE dialogs
               SET history_json=?, updated_at=?
             WHERE target_user_id=?
            """,
            (json.dumps(history, ensure_ascii=False), _now_iso(), int(target_user_id)),
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
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE dialogs
               SET stage=?,
                   outgoing_count=?,
                   link_sent=?,
                   auto_link_at=?,
                   updated_at=?
             WHERE target_user_id=?
            """,
            (stage, outgoing, ls, ala, _now_iso(), int(target_user_id)),
        )


def list_due_auto_links() -> list[dict[str, Any]]:
    now = _now_iso()
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT target_user_id, account_user_id, stage, outgoing_count,
               link_sent, auto_link_at, history_json, updated_at,
               first_dm_at, first_dm_message_id, telegram_delete_at,
               telegram_deleted_at, telegram_delete_next_attempt_at,
               telegram_delete_attempts, telegram_delete_last_error,
               history_purge_at, history_purged_at
          FROM dialogs
         WHERE stage=?
           AND link_sent=0
           AND auto_link_at IS NOT NULL
           AND auto_link_at <= ?
        """,
        (STAGE_EXPLAINED, now),
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


def list_due_followups() -> list[dict[str, Any]]:
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
               history_purge_at, history_purged_at
          FROM dialogs
         WHERE stage=?
           AND auto_link_at IS NOT NULL
           AND auto_link_at <= ?
        """,
        (STAGE_WAITING_REPLY, now),
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
    conn = get_connection()
    row = conn.execute(
        """
        SELECT COUNT(*) AS c FROM dialogs
         WHERE stage IN (?, ?, ?, ?)
        """,
        (STAGE_WAITING_REPLY, STAGE_FOLLOWUP_SENT, STAGE_ENGAGED, STAGE_EXPLAINED),
    ).fetchone()
    return int(row["c"] if row else 0)


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
    """Count dialogs that still belong to an account and are not closed."""
    conn = get_connection()
    row = conn.execute(
        """
        SELECT COUNT(*) AS c
          FROM dialogs
         WHERE account_user_id=?
           AND stage<>?
        """,
        (int(account_user_id), STAGE_CLOSED),
    ).fetchone()
    return int(row["c"] if row else 0)


def has_open_for_account(account_user_id: int) -> bool:
    return count_open_for_account(account_user_id) > 0


def close_for_opt_out(target_user_id: int) -> None:
    """Keep dialog history but make every scheduled action inactive."""
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE dialogs
               SET stage=?, auto_link_at=NULL, updated_at=?
             WHERE target_user_id=?
            """,
            (STAGE_CLOSED, now, int(target_user_id)),
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
        rows = conn.execute(
            """
            SELECT target_user_id
              FROM dialogs
             WHERE account_user_id=? AND stage<>?
            """,
            (uid, STAGE_CLOSED),
        ).fetchall()
        targets = [int(r["target_user_id"]) for r in rows]
        conn.execute(
            """
            UPDATE dialogs
               SET stage=?, auto_link_at=NULL, updated_at=?
             WHERE account_user_id=? AND stage<>?
            """,
            (STAGE_CLOSED, now, uid, STAGE_CLOSED),
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
    row = conn.execute(
        """
        SELECT COUNT(*) AS c FROM dialogs
         WHERE stage=? AND updated_at >= ? AND updated_at < ?
        """,
        (STAGE_CLOSED, start_utc, end_utc),
    ).fetchone()
    return int(row["c"] if row else 0)


def list_recent(*, active_only: bool = True, limit: int = 15) -> list[dict[str, Any]]:
    conn = get_connection()
    where = "WHERE d.stage<>'closed'" if active_only else ""
    rows = conn.execute(
        f"""
        SELECT d.target_user_id, d.account_user_id, d.stage, d.outgoing_count,
               d.link_sent, d.updated_at, d.first_dm_at, d.telegram_delete_at,
               d.history_purge_at, a.username, a.first_name, a.last_name
          FROM dialogs d
          LEFT JOIN audience a ON a.user_id=d.target_user_id
          {where}
         ORDER BY d.updated_at DESC
         LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    return [dict(r) for r in rows]



def list_recent_closed(limit: int = 15) -> list[dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT d.target_user_id, d.account_user_id, d.stage, d.outgoing_count,
               d.link_sent, d.updated_at, d.first_dm_at, d.telegram_delete_at,
               d.history_purge_at, a.username, a.first_name, a.last_name
          FROM dialogs d
          LEFT JOIN audience a ON a.user_id=d.target_user_id
         WHERE d.stage=?
         ORDER BY d.updated_at DESC
         LIMIT ?
        """,
        (STAGE_CLOSED, int(limit)),
    ).fetchall()
    return [dict(r) for r in rows]
