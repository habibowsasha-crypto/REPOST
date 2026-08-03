"""Runtime flags persisted in runtime_meta (worker on/off)."""

from __future__ import annotations

import datetime as dt

from db.schema import db_lock, get_connection

KEY_WORKER = "dm_worker_enabled"


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def is_worker_enabled() -> bool:
    conn = get_connection()
    row = conn.execute(
        "SELECT value FROM runtime_meta WHERE key=?",
        (KEY_WORKER,),
    ).fetchone()
    if not row:
        return False
    return str(row["value"]).lower() in {"1", "true", "yes", "on"}


def set_worker_enabled(enabled: bool) -> None:
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            INSERT INTO runtime_meta (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value=excluded.value,
                updated_at=excluded.updated_at
            """,
            (KEY_WORKER, "1" if enabled else "0", _now_iso()),
        )
