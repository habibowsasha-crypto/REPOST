"""Persistent dialog state after first DM."""

from __future__ import annotations

import datetime as dt
import json
from typing import Any, Optional

from db.schema import db_lock, get_connection

STAGE_WAITING_REPLY = "waiting_reply"
STAGE_FOLLOWUP_SENT = "followup_sent"
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
               link_sent, auto_link_at, history_json, updated_at
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
    except Exception:
        data["history"] = []
    return data


def create_after_first_dm(target_user_id: int, account_user_id: int, first_text: str) -> None:
    now = _now_iso()
    # Reuse auto_link_at while stage=waiting_reply as "follow-up due at"
    follow_at = (_now() + dt.timedelta(hours=FOLLOWUP_DELAY_HOURS)).isoformat()
    history = [{"role": "assistant", "text": first_text}]
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            INSERT INTO dialogs (
                target_user_id, account_user_id, stage, outgoing_count,
                link_sent, auto_link_at, history_json, updated_at
            ) VALUES (?, ?, ?, 1, 0, ?, ?, ?)
            ON CONFLICT(target_user_id) DO UPDATE SET
                account_user_id=excluded.account_user_id,
                stage=excluded.stage,
                outgoing_count=1,
                link_sent=0,
                auto_link_at=excluded.auto_link_at,
                history_json=excluded.history_json,
                updated_at=excluded.updated_at
            """,
            (
                int(target_user_id),
                int(account_user_id),
                STAGE_WAITING_REPLY,
                follow_at,
                json.dumps(history, ensure_ascii=False),
                now,
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
               link_sent, auto_link_at, history_json, updated_at
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
        except Exception:
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
               link_sent, auto_link_at, history_json, updated_at
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
        except Exception:
            data["history"] = []
        result.append(data)
    return result


def count_active() -> int:
    conn = get_connection()
    row = conn.execute(
        """
        SELECT COUNT(*) AS c FROM dialogs
         WHERE stage IN (?, ?, ?)
        """,
        (STAGE_WAITING_REPLY, STAGE_FOLLOWUP_SENT, STAGE_EXPLAINED),
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
