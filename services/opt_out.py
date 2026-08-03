"""Global opt-out list: never message these users."""

from __future__ import annotations

import datetime as dt
from typing import Any

from db.schema import db_lock, get_connection


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def is_opted_out(user_id: int) -> bool:
    conn = get_connection()
    row = conn.execute(
        "SELECT 1 FROM opt_out WHERE user_id=?",
        (int(user_id),),
    ).fetchone()
    return row is not None


def add(user_id: int, reason: str | None = None) -> None:
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            INSERT INTO opt_out (user_id, reason, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET reason=excluded.reason
            """,
            (int(user_id), reason, _now_iso()),
        )


def remove(user_id: int) -> bool:
    conn = get_connection()
    with db_lock(), conn:
        cur = conn.execute("DELETE FROM opt_out WHERE user_id=?", (int(user_id),))
        return int(cur.rowcount or 0) == 1


def list_all(limit: int = 50) -> list[dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT user_id, reason, created_at
          FROM opt_out
         ORDER BY created_at DESC
         LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    return [dict(r) for r in rows]


def count() -> int:
    conn = get_connection()
    row = conn.execute("SELECT COUNT(*) AS c FROM opt_out").fetchone()
    return int(row["c"] if row else 0)
