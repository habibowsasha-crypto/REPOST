"""Runtime flags persisted in runtime_meta (worker, peerflood cooldown, …)."""

from __future__ import annotations

import datetime as dt

from db.schema import db_lock, get_connection

KEY_WORKER = "dm_worker_enabled"
KEY_PEER_FLOOD_SEC = "peer_flood_min_cooldown_seconds"
# legacy key (minutes) — still read if new key missing
KEY_PEER_FLOOD_MIN = "peer_flood_min_cooldown_minutes"

PEER_FLOOD_MIN_ALLOWED_SEC = 60        # 1 minute
PEER_FLOOD_MAX_ALLOWED_SEC = 24 * 3600  # 24 hours


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _get(key: str) -> str | None:
    conn = get_connection()
    row = conn.execute(
        "SELECT value FROM runtime_meta WHERE key=?",
        (key,),
    ).fetchone()
    if not row:
        return None
    return str(row["value"])


def _set(key: str, value: str) -> None:
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
            (key, value, _now_iso()),
        )


def is_worker_enabled() -> bool:
    val = _get(KEY_WORKER)
    if val is None:
        return False
    return val.lower() in {"1", "true", "yes", "on"}


def set_worker_enabled(enabled: bool) -> None:
    _set(KEY_WORKER, "1" if enabled else "0")


def get_peer_flood_min_seconds() -> int:
    """Effective PeerFlood min pause in seconds."""
    from config import PEER_FLOOD_MIN_COOLDOWN_MINUTES

    raw = _get(KEY_PEER_FLOOD_SEC)
    if raw is not None and str(raw).strip() != "":
        try:
            n = int(str(raw).strip())
            return max(PEER_FLOOD_MIN_ALLOWED_SEC, min(PEER_FLOOD_MAX_ALLOWED_SEC, n))
        except ValueError:
            pass

    # legacy minutes key
    raw_m = _get(KEY_PEER_FLOOD_MIN)
    if raw_m is not None and str(raw_m).strip() != "":
        try:
            n = int(str(raw_m).strip()) * 60
            return max(PEER_FLOOD_MIN_ALLOWED_SEC, min(PEER_FLOOD_MAX_ALLOWED_SEC, n))
        except ValueError:
            pass

    return max(
        PEER_FLOOD_MIN_ALLOWED_SEC,
        min(PEER_FLOOD_MAX_ALLOWED_SEC, int(PEER_FLOOD_MIN_COOLDOWN_MINUTES) * 60),
    )


def set_peer_flood_min_seconds(seconds: int) -> int:
    """Clamp and persist seconds. Returns stored value."""
    n = int(seconds)
    n = max(PEER_FLOOD_MIN_ALLOWED_SEC, min(PEER_FLOOD_MAX_ALLOWED_SEC, n))
    _set(KEY_PEER_FLOOD_SEC, str(n))
    return n


def format_peer_flood_pause(seconds: int | None = None) -> str:
    """Human label: 5 мин / 1 ч 30 мин / 90 сек."""
    if seconds is None:
        seconds = get_peer_flood_min_seconds()
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds} сек"
    if seconds % 3600 == 0:
        h = seconds // 3600
        return f"{h} ч"
    if seconds % 60 == 0:
        m = seconds // 60
        return f"{m} мин"
    m, s = divmod(seconds, 60)
    if m >= 60:
        h, m = divmod(m, 60)
        return f"{h} ч {m} мин" + (f" {s} сек" if s else "")
    return f"{m} мин {s} сек"


# Back-compat aliases used by older call sites
def get_peer_flood_min_minutes() -> int:
    return max(1, (get_peer_flood_min_seconds() + 59) // 60)


def set_peer_flood_min_minutes(minutes: int) -> int:
    return set_peer_flood_min_seconds(int(minutes) * 60) // 60
