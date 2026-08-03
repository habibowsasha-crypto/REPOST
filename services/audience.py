"""Crypto audience base: everyone we DM'd + import/export for re-queue."""

from __future__ import annotations

import datetime as dt
from typing import Any, Optional

from db.schema import db_lock, get_connection
from services import opt_out as opt_out_svc
from services import queue as queue_svc


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def count() -> int:
    conn = get_connection()
    row = conn.execute("SELECT COUNT(*) AS c FROM audience").fetchone()
    return int(row["c"] if row else 0)


def upsert(
    user_id: int,
    *,
    username: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
    source: str = "dm",
    source_chat_id: int | None = None,
    touched_dm: bool = False,
) -> None:
    """Insert or refresh audience row. first_dm_at set once when touched_dm."""
    now = _now_iso()
    user_id = int(user_id)
    un = (username or "").strip().lstrip("@") or None
    fn = (first_name or "").strip() or None
    ln = (last_name or "").strip() or None
    conn = get_connection()
    with db_lock(), conn:
        row = conn.execute(
            "SELECT user_id, first_dm_at FROM audience WHERE user_id=?",
            (user_id,),
        ).fetchone()
        if row is None:
            conn.execute(
                """
                INSERT INTO audience (
                    user_id, username, first_name, last_name, source,
                    source_chat_id, first_seen_at, first_dm_at, last_touch_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    un,
                    fn,
                    ln,
                    source,
                    source_chat_id,
                    now,
                    now if touched_dm else None,
                    now,
                ),
            )
            return
        # update profile + touch
        conn.execute(
            """
            UPDATE audience
               SET username=COALESCE(?, username),
                   first_name=COALESCE(?, first_name),
                   last_name=COALESCE(?, last_name),
                   last_touch_at=?,
                   first_dm_at=CASE
                        WHEN first_dm_at IS NULL AND ? THEN ?
                        ELSE first_dm_at
                   END,
                   source_chat_id=COALESCE(source_chat_id, ?)
             WHERE user_id=?
            """,
            (
                un,
                fn,
                ln,
                now,
                1 if touched_dm else 0,
                now,
                source_chat_id,
                user_id,
            ),
        )


def record_first_dm(
    user_id: int,
    *,
    username: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
    source_chat_id: int | None = None,
) -> None:
    upsert(
        user_id,
        username=username,
        first_name=first_name,
        last_name=last_name,
        source="dm",
        source_chat_id=source_chat_id,
        touched_dm=True,
    )


def list_recent(limit: int = 20) -> list[dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT user_id, username, first_name, last_name, source,
               first_dm_at, last_touch_at
          FROM audience
         ORDER BY last_touch_at DESC
         LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    return [dict(r) for r in rows]


def export_lines(*, only_with_dm: bool = True) -> list[str]:
    """CSV-like lines: user_id,username,first_dm_at"""
    conn = get_connection()
    if only_with_dm:
        rows = conn.execute(
            """
            SELECT user_id, username, first_dm_at
              FROM audience
             WHERE first_dm_at IS NOT NULL
             ORDER BY first_dm_at ASC
            """
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT user_id, username, first_dm_at
              FROM audience
             ORDER BY last_touch_at DESC
            """
        ).fetchall()
    lines = ["user_id,username,first_dm_at"]
    for r in rows:
        uid = int(r["user_id"])
        un = (r["username"] or "").replace(",", " ")
        dm = r["first_dm_at"] or ""
        lines.append(f"{uid},{un},{dm}")
    return lines


def import_user_ids(ids: list[int], *, source: str = "import") -> dict[str, int]:
    """
    Add ids to audience + force into pending queue (skip opt-out only).
    Re-opens previously sent/completed contacts for a new first DM.
    """
    stats = {
        "added_or_touch": 0,
        "queued": 0,
        "skipped_opt_out": 0,
        "skipped_invalid": 0,
        "skipped_queue": 0,
    }
    for raw in ids:
        try:
            uid = int(raw)
        except (TypeError, ValueError):
            stats["skipped_invalid"] += 1
            continue
        if uid <= 0:
            stats["skipped_invalid"] += 1
            continue
        if opt_out_svc.is_opted_out(uid):
            stats["skipped_opt_out"] += 1
            continue
        upsert(uid, source=source, touched_dm=False)
        stats["added_or_touch"] += 1
        ok = queue_svc.force_requeue(
            target_user_id=uid,
            username=None,
            first_name=None,
            last_name=None,
        )
        if ok:
            stats["queued"] += 1
        else:
            stats["skipped_queue"] += 1
    return stats


def parse_ids_from_text(text: str) -> list[int]:
    """Extract user ids from free text / CSV (first column)."""
    found: list[int] = []
    seen: set[int] = set()
    for line in (text or "").splitlines():
        line = line.strip()
        if not line or line.lower().startswith("user_id"):
            continue
        # CSV: id,username,...
        part = line.split(",")[0].strip()
        # plain id or @ ignored
        if part.lstrip("-").isdigit():
            uid = int(part)
            if uid not in seen and uid > 0:
                seen.add(uid)
                found.append(uid)
                continue
        # whitespace-separated ids on one line
        for tok in line.replace(",", " ").split():
            if tok.lstrip("-").isdigit():
                uid = int(tok)
                if uid not in seen and uid > 0:
                    seen.add(uid)
                    found.append(uid)
    return found


def format_line(row: dict[str, Any]) -> str:
    uid = int(row["user_id"])
    un = (row.get("username") or "").strip().lstrip("@")
    label = f"@{un}" if un else f"`{uid}`"
    dm = (row.get("first_dm_at") or "")[:16].replace("T", " ")
    if dm:
        return f"• {label} · `{uid}` · DM {dm}"
    return f"• {label} · `{uid}`"
