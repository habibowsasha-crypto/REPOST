"""Account persistence and display helpers."""

from __future__ import annotations

import datetime as dt
from typing import Any, Optional

from db.schema import db_lock, get_connection


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _row_to_dict(row: Any) -> dict[str, Any]:
    return dict(row) if row is not None else {}


def count_accounts() -> int:
    conn = get_connection()
    row = conn.execute("SELECT COUNT(*) AS c FROM accounts").fetchone()
    return int(row["c"] if row else 0)


def count_participating() -> int:
    conn = get_connection()
    row = conn.execute(
        "SELECT COUNT(*) AS c FROM accounts WHERE participates=1"
    ).fetchone()
    return int(row["c"] if row else 0)


def list_accounts() -> list[dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT user_id, session_string, phone, username, first_name, last_name,
               participates, is_paused, cooldown_until, pause_reason,
               last_send_at, next_send_at, COALESCE(chat_mode, 'manual') AS chat_mode,
               COALESCE(daily_sent_count, 0) AS daily_sent_count,
               daily_sent_date,
               created_at, updated_at
          FROM accounts
         ORDER BY created_at DESC
        """
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_account(user_id: int) -> Optional[dict[str, Any]]:
    conn = get_connection()
    row = conn.execute(
        """
        SELECT user_id, session_string, phone, username, first_name, last_name,
               participates, is_paused, cooldown_until, pause_reason,
               last_send_at, next_send_at, COALESCE(chat_mode, 'manual') AS chat_mode,
               COALESCE(daily_sent_count, 0) AS daily_sent_count,
               daily_sent_date,
               created_at, updated_at
          FROM accounts
         WHERE user_id=?
        """,
        (int(user_id),),
    ).fetchone()
    return _row_to_dict(row) if row else None


def upsert_account(
    *,
    user_id: int,
    session_string: str,
    phone: str | None = None,
    username: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
) -> None:
    """Insert or refresh session/profile. Does not reset participates flag on update."""
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        existing = conn.execute(
            "SELECT participates FROM accounts WHERE user_id=?",
            (int(user_id),),
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE accounts
                   SET session_string=?,
                       phone=COALESCE(?, phone),
                       username=?,
                       first_name=?,
                       last_name=?,
                       updated_at=?
                 WHERE user_id=?
                """,
                (
                    session_string,
                    phone,
                    username,
                    first_name,
                    last_name,
                    now,
                    int(user_id),
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO accounts (
                    user_id, session_string, phone, username, first_name, last_name,
                    participates, is_paused, chat_mode, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 'all_with_exclusions', ?, ?)
                """,
                (
                    int(user_id),
                    session_string,
                    phone,
                    username,
                    first_name,
                    last_name,
                    now,
                    now,
                ),
            )


def set_participates(user_id: int, value: bool) -> bool:
    conn = get_connection()
    with db_lock(), conn:
        cur = conn.execute(
            """
            UPDATE accounts
               SET participates=?, updated_at=?
             WHERE user_id=?
            """,
            (1 if value else 0, _now_iso(), int(user_id)),
        )
        return int(cur.rowcount or 0) == 1


def delete_account(user_id: int) -> bool:
    conn = get_connection()
    uid = int(user_id)
    with db_lock(), conn:
        conn.execute(
            "DELETE FROM account_discovered_chats WHERE account_user_id=?", (uid,)
        )
        conn.execute(
            "DELETE FROM account_selected_chats WHERE account_user_id=?", (uid,)
        )
        conn.execute(
            "DELETE FROM account_excluded_chats WHERE account_user_id=?", (uid,)
        )
        conn.execute(
            "DELETE FROM spambot_state WHERE account_user_id=?", (uid,)
        )
        cur = conn.execute("DELETE FROM accounts WHERE user_id=?", (uid,))
        return int(cur.rowcount or 0) == 1


def format_account_label(acc: dict[str, Any], *, include_id: bool = True) -> str:
    username = (acc.get("username") or "").strip().lstrip("@")
    first = (acc.get("first_name") or "").strip()
    last = (acc.get("last_name") or "").strip()
    name = " ".join(p for p in (first, last) if p).strip()
    uid = int(acc["user_id"])
    if username:
        label = f"@{username}"
    elif name:
        label = name
    else:
        label = f"id{uid}"
    if include_id:
        return f"{label} ({uid})"
    return label


def account_status_line(acc: dict[str, Any]) -> str:
    parts = []
    if acc.get("participates"):
        parts.append("участвует")
    else:
        parts.append("выключен")
    if acc.get("is_paused"):
        reason = (acc.get("pause_reason") or "пауза").strip()
        parts.append(f"пауза: {reason}")
    return " | ".join(parts)
