"""Persistent anti-repeat for outbound phrases (first DM, later dialog)."""

from __future__ import annotations

import datetime as dt
from typing import List

from db.schema import db_lock, get_connection

KIND_FIRST_DM = "first_dm"
KIND_EXPLAIN = "explain"
KIND_LINK = "link_wrap"


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def recent_texts(kind: str, limit: int = 40) -> List[str]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT text FROM sent_phrases
         WHERE kind=?
         ORDER BY id DESC
         LIMIT ?
        """,
        (kind, int(limit)),
    ).fetchall()
    return [str(r["text"]) for r in rows]


def remember(kind: str, text: str) -> None:
    text = (text or "").strip()
    if not text:
        return
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            INSERT INTO sent_phrases (kind, text, created_at)
            VALUES (?, ?, ?)
            """,
            (kind, text, _now_iso()),
        )
        # Keep table small
        conn.execute(
            """
            DELETE FROM sent_phrases
             WHERE kind=?
               AND id NOT IN (
                   SELECT id FROM sent_phrases
                    WHERE kind=?
                    ORDER BY id DESC
                    LIMIT 200
               )
            """,
            (kind, kind),
        )
