"""Persistent anti-repeat windows for outbound funnel phrases."""

from __future__ import annotations

import datetime as dt
from typing import List

from db.schema import db_lock, get_connection

KIND_FIRST_DM = "first_dm"
KIND_EXPLAIN = "explain"
KIND_LINK = "link_wrap"
KIND_PROMO = "promo"
KIND_APOLOGY = "apology"
KIND_LINK_HELP = "link_help"

ANTI_REPEAT_KINDS = {
    KIND_FIRST_DM,
    KIND_PROMO,
    KIND_APOLOGY,
    KIND_LINK_HELP,
}
ANTI_REPEAT_WINDOW = 20


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def recent_texts(kind: str, limit: int = ANTI_REPEAT_WINDOW) -> List[str]:
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
    return [str(row["text"]) for row in rows]


def remember(kind: str, text: str) -> None:
    value = (text or "").strip()
    if not value:
        return
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            INSERT INTO sent_phrases (kind, text, created_at)
            VALUES (?, ?, ?)
            """,
            (kind, value, _now_iso()),
        )
        keep_limit = ANTI_REPEAT_WINDOW if kind in ANTI_REPEAT_KINDS else 200
        conn.execute(
            """
            DELETE FROM sent_phrases
             WHERE kind=?
               AND id NOT IN (
                   SELECT id FROM sent_phrases
                    WHERE kind=?
                    ORDER BY id DESC
                    LIMIT ?
               )
            """,
            (kind, kind, keep_limit),
        )
