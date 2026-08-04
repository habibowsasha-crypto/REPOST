"""Durable outbox for automatic link and silence follow-up messages."""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

from db.schema import db_lock, get_connection

KIND_AUTO_LINK = "auto_link"
KIND_FOLLOWUP = "followup"

STATUS_PREPARED = "prepared"
STATUS_SENT = "sent"
STATUS_FAILED = "failed"


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def prepare(
    target_user_id: int,
    account_user_id: int,
    action_kind: str,
    text: str,
) -> bool:
    """Persist an automatic outbound message before touching Telegram."""
    if action_kind not in {KIND_AUTO_LINK, KIND_FOLLOWUP}:
        raise ValueError(f"unsupported action kind: {action_kind}")
    target = int(target_user_id)
    account = int(account_user_id)
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        dialog = conn.execute(
            "SELECT stage, link_sent FROM dialogs WHERE target_user_id=?",
            (target,),
        ).fetchone()
        if not dialog:
            return False
        if action_kind == KIND_AUTO_LINK:
            if str(dialog["stage"]) != "explained" or int(dialog["link_sent"] or 0):
                return False
        elif str(dialog["stage"]) != "waiting_reply":
            return False

        existing = conn.execute(
            """
            SELECT status FROM dialog_outbox
             WHERE target_user_id=? AND action_kind=?
            """,
            (target, action_kind),
        ).fetchone()
        if existing and str(existing["status"]) in {STATUS_PREPARED, STATUS_SENT}:
            return False

        conn.execute(
            """
            INSERT INTO dialog_outbox (
                target_user_id, action_kind, account_user_id, text, status,
                prepared_at, telegram_message_id, sent_at, last_error, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?)
            ON CONFLICT(target_user_id, action_kind) DO UPDATE SET
                account_user_id=excluded.account_user_id,
                text=excluded.text,
                status=excluded.status,
                prepared_at=excluded.prepared_at,
                telegram_message_id=NULL,
                sent_at=NULL,
                last_error=NULL,
                updated_at=excluded.updated_at
            """,
            (target, action_kind, account, str(text), STATUS_PREPARED, now, now),
        )
    return True


def _append_history_json(raw: str | None, text: str) -> str:
    try:
        history = json.loads(raw or "[]")
        if not isinstance(history, list):
            history = []
    except (TypeError, json.JSONDecodeError):
        history = []
    history.append({"role": "assistant", "text": str(text)})
    return json.dumps(history[-20:], ensure_ascii=False)


def commit_sent(
    target_user_id: int,
    action_kind: str,
    *,
    telegram_message_id: int | None = None,
    sent_at: str | None = None,
) -> bool:
    """Atomically confirm Telegram delivery and the matching dialog transition."""
    target = int(target_user_id)
    now = _now_iso()
    sent = sent_at or now
    conn = get_connection()
    with db_lock(), conn:
        row = conn.execute(
            """
            SELECT account_user_id, text, status
              FROM dialog_outbox
             WHERE target_user_id=? AND action_kind=?
            """,
            (target, action_kind),
        ).fetchone()
        if not row:
            return False
        if str(row["status"]) == STATUS_SENT:
            return True
        dialog = conn.execute(
            """
            SELECT stage, outgoing_count, link_sent, history_json
              FROM dialogs WHERE target_user_id=?
            """,
            (target,),
        ).fetchone()
        if not dialog:
            return False

        text = str(row["text"] or "")
        history_json = _append_history_json(dialog["history_json"], text)
        outgoing = int(dialog["outgoing_count"] or 0) + 1
        if action_kind == KIND_AUTO_LINK:
            stage = "link_sent"
            link_sent = 1
        elif action_kind == KIND_FOLLOWUP:
            stage = "followup_sent"
            link_sent = int(dialog["link_sent"] or 0)
        else:
            return False

        conn.execute(
            """
            UPDATE dialog_outbox
               SET status=?, telegram_message_id=COALESCE(?, telegram_message_id),
                   sent_at=COALESCE(sent_at, ?), last_error=NULL, updated_at=?
             WHERE target_user_id=? AND action_kind=?
            """,
            (
                STATUS_SENT,
                telegram_message_id,
                sent,
                now,
                target,
                action_kind,
            ),
        )
        conn.execute(
            """
            UPDATE dialogs
               SET stage=?, outgoing_count=?, link_sent=?, auto_link_at=NULL,
                   history_json=?, updated_at=?
             WHERE target_user_id=?
            """,
            (stage, outgoing, link_sent, history_json, now, target),
        )
    return True


def mark_failed(target_user_id: int, action_kind: str, error: str) -> None:
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE dialog_outbox
               SET status=?, last_error=?, updated_at=?
             WHERE target_user_id=? AND action_kind=? AND status=?
            """,
            (
                STATUS_FAILED,
                str(error)[:500],
                _now_iso(),
                int(target_user_id),
                action_kind,
                STATUS_PREPARED,
            ),
        )


def get_prepared(target_user_id: int, action_kind: str) -> dict[str, Any] | None:
    conn = get_connection()
    row = conn.execute(
        """
        SELECT target_user_id, action_kind, account_user_id, text, status,
               prepared_at, telegram_message_id, sent_at, last_error, updated_at
          FROM dialog_outbox
         WHERE target_user_id=? AND action_kind=? AND status=?
        """,
        (int(target_user_id), action_kind, STATUS_PREPARED),
    ).fetchone()
    return dict(row) if row else None


def list_stale_prepared(
    *, older_than_seconds: int = 90, limit: int = 20
) -> list[dict[str, Any]]:
    cutoff = (_now() - dt.timedelta(seconds=max(1, int(older_than_seconds)))).isoformat()
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT o.target_user_id, o.action_kind, o.account_user_id, o.text,
               o.prepared_at, a.username, a.access_hash
          FROM dialog_outbox o
          LEFT JOIN audience a ON a.user_id=o.target_user_id
         WHERE o.status=? AND o.prepared_at <= ?
         ORDER BY o.prepared_at ASC
         LIMIT ?
        """,
        (STATUS_PREPARED, cutoff, int(limit)),
    ).fetchall()
    return [dict(r) for r in rows]


def clear_for_target(target_user_id: int) -> None:
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            "DELETE FROM dialog_outbox WHERE target_user_id=?",
            (int(target_user_id),),
        )
