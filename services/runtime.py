"""Runtime flags persisted in runtime_meta (worker, peerflood cooldown range, …)."""

from __future__ import annotations

import datetime as dt
import random

from db.schema import db_lock, get_connection

KEY_WORKER = "dm_worker_enabled"
KEY_PEER_FLOOD_SEC = "peer_flood_min_cooldown_seconds"  # legacy single value
KEY_PEER_FLOOD_MIN = "peer_flood_min_cooldown_minutes"  # legacy minutes
KEY_PEER_FLOOD_LO = "peer_flood_cooldown_lo_seconds"
KEY_PEER_FLOOD_HI = "peer_flood_cooldown_hi_seconds"

PEER_FLOOD_MIN_ALLOWED_SEC = 60         # 1 minute
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


def _clamp_sec(n: int) -> int:
    return max(PEER_FLOOD_MIN_ALLOWED_SEC, min(PEER_FLOOD_MAX_ALLOWED_SEC, int(n)))


def get_peer_flood_range_seconds() -> tuple[int, int]:
    """Return (lo, hi) inclusive seconds for PeerFlood min pause."""
    from config import PEER_FLOOD_MIN_COOLDOWN_MINUTES

    raw_lo = _get(KEY_PEER_FLOOD_LO)
    raw_hi = _get(KEY_PEER_FLOOD_HI)
    if raw_lo is not None and raw_hi is not None:
        try:
            lo = _clamp_sec(int(str(raw_lo).strip()))
            hi = _clamp_sec(int(str(raw_hi).strip()))
            if lo > hi:
                lo, hi = hi, lo
            return lo, hi
        except ValueError:
            pass

    # Legacy single-second key → fixed range
    raw = _get(KEY_PEER_FLOOD_SEC)
    if raw is not None and str(raw).strip() != "":
        try:
            n = _clamp_sec(int(str(raw).strip()))
            return n, n
        except ValueError:
            pass

    raw_m = _get(KEY_PEER_FLOOD_MIN)
    if raw_m is not None and str(raw_m).strip() != "":
        try:
            n = _clamp_sec(int(str(raw_m).strip()) * 60)
            return n, n
        except ValueError:
            pass

    # Default: honor the configured environment value exactly. The previous
    # implicit 3–10 minute rewrite contradicted PEER_FLOOD_MIN_COOLDOWN_MINUTES.
    base = _clamp_sec(int(PEER_FLOOD_MIN_COOLDOWN_MINUTES) * 60)
    return base, base


def set_peer_flood_range_seconds(lo: int, hi: int) -> tuple[int, int]:
    """Persist range. Returns clamped (lo, hi)."""
    a = _clamp_sec(lo)
    b = _clamp_sec(hi)
    if a > b:
        a, b = b, a
    _set(KEY_PEER_FLOOD_LO, str(a))
    _set(KEY_PEER_FLOOD_HI, str(b))
    # Keep legacy key as midpoint for older readers
    mid = (a + b) // 2
    _set(KEY_PEER_FLOOD_SEC, str(mid))
    return a, b


def pick_peer_flood_seconds() -> int:
    """Random pause in configured [lo, hi] for this PeerFlood event."""
    lo, hi = get_peer_flood_range_seconds()
    if lo >= hi:
        return lo
    return random.randint(lo, hi)


def format_duration(seconds: int) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds} сек"
    if seconds % 3600 == 0:
        return f"{seconds // 3600} ч"
    if seconds % 60 == 0:
        m = seconds // 60
        return f"{m} мин"
    m, s = divmod(seconds, 60)
    if m >= 60:
        h, m = divmod(m, 60)
        return f"{h} ч {m} мин" + (f" {s} сек" if s else "")
    return f"{m} мин {s} сек"


def format_peer_flood_range() -> str:
    lo, hi = get_peer_flood_range_seconds()
    if lo == hi:
        return format_duration(lo)
    return f"{format_duration(lo)} – {format_duration(hi)}"


# --- Back-compat aliases ---

def get_peer_flood_min_seconds() -> int:
    """Lower bound of range (compat). Prefer pick_peer_flood_seconds() at event time."""
    lo, _hi = get_peer_flood_range_seconds()
    return lo


def set_peer_flood_min_seconds(seconds: int) -> int:
    """Compat: set fixed range lo=hi=seconds."""
    n = _clamp_sec(seconds)
    set_peer_flood_range_seconds(n, n)
    return n


def get_peer_flood_min_minutes() -> int:
    return max(1, (get_peer_flood_min_seconds() + 59) // 60)


def set_peer_flood_min_minutes(minutes: int) -> int:
    return set_peer_flood_min_seconds(int(minutes) * 60) // 60


def format_peer_flood_pause(seconds: int | None = None) -> str:
    """Human label for a concrete pause or the configured range."""
    if seconds is None:
        return format_peer_flood_range()
    return format_duration(int(seconds))


# --- Pacing (admin-editable; defaults from env/config) ---

KEY_PACE_ACC_LO = "pace_account_lo_sec"
KEY_PACE_ACC_HI = "pace_account_hi_sec"
KEY_PACE_GLOB_LO = "pace_global_lo_sec"
KEY_PACE_GLOB_HI = "pace_global_hi_sec"
KEY_PACE_DAILY = "pace_daily_limit"
KEY_PACE_AI_LO = "pace_ai_reply_lo_sec"
KEY_PACE_AI_HI = "pace_ai_reply_hi_sec"
KEY_PACE_LINK_LO = "pace_auto_link_lo_sec"
KEY_PACE_LINK_HI = "pace_auto_link_hi_sec"


def _int_or(default: int, raw: str | None, *, lo: int, hi: int) -> int:
    if raw is None or str(raw).strip() == "":
        return default
    try:
        n = int(str(raw).strip())
    except ValueError:
        return default
    return max(lo, min(hi, n))


def get_account_interval_range() -> tuple[int, int]:
    from config import DM_ACCOUNT_INTERVAL_MAX, DM_ACCOUNT_INTERVAL_MIN

    lo = _int_or(DM_ACCOUNT_INTERVAL_MIN, _get(KEY_PACE_ACC_LO), lo=60, hi=86400)
    hi = _int_or(DM_ACCOUNT_INTERVAL_MAX, _get(KEY_PACE_ACC_HI), lo=60, hi=86400)
    if lo > hi:
        lo, hi = hi, lo
    return lo, hi


def set_account_interval_range(lo: int, hi: int) -> tuple[int, int]:
    lo, hi = int(lo), int(hi)
    if lo > hi:
        lo, hi = hi, lo
    lo = max(60, min(86400, lo))
    hi = max(60, min(86400, hi))
    _set(KEY_PACE_ACC_LO, str(lo))
    _set(KEY_PACE_ACC_HI, str(hi))
    return lo, hi


def get_global_spacing_range() -> tuple[int, int]:
    from config import DM_GLOBAL_SPACING_MAX, DM_GLOBAL_SPACING_MIN

    lo = _int_or(DM_GLOBAL_SPACING_MIN, _get(KEY_PACE_GLOB_LO), lo=10, hi=3600)
    hi = _int_or(DM_GLOBAL_SPACING_MAX, _get(KEY_PACE_GLOB_HI), lo=10, hi=3600)
    if lo > hi:
        lo, hi = hi, lo
    return lo, hi


def set_global_spacing_range(lo: int, hi: int) -> tuple[int, int]:
    lo, hi = int(lo), int(hi)
    if lo > hi:
        lo, hi = hi, lo
    lo = max(10, min(3600, lo))
    hi = max(10, min(3600, hi))
    _set(KEY_PACE_GLOB_LO, str(lo))
    _set(KEY_PACE_GLOB_HI, str(hi))
    return lo, hi


def get_daily_limit() -> int:
    from config import DM_DAILY_LIMIT_PER_ACCOUNT

    return _int_or(DM_DAILY_LIMIT_PER_ACCOUNT, _get(KEY_PACE_DAILY), lo=1, hi=500)


def set_daily_limit(n: int) -> int:
    n = max(1, min(500, int(n)))
    _set(KEY_PACE_DAILY, str(n))
    return n


def get_ai_reply_delay_range() -> tuple[int, int]:
    from config import AI_REPLY_DELAY_MAX, AI_REPLY_DELAY_MIN

    lo = _int_or(AI_REPLY_DELAY_MIN, _get(KEY_PACE_AI_LO), lo=0, hi=600)
    hi = _int_or(AI_REPLY_DELAY_MAX, _get(KEY_PACE_AI_HI), lo=0, hi=600)
    if lo > hi:
        lo, hi = hi, lo
    return lo, hi


def set_ai_reply_delay_range(lo: int, hi: int) -> tuple[int, int]:
    lo, hi = int(lo), int(hi)
    if lo > hi:
        lo, hi = hi, lo
    lo = max(0, min(600, lo))
    hi = max(0, min(600, hi))
    _set(KEY_PACE_AI_LO, str(lo))
    _set(KEY_PACE_AI_HI, str(hi))
    return lo, hi


def get_auto_link_delay_range() -> tuple[int, int]:
    from config import AI_AUTO_LINK_DELAY_MAX, AI_AUTO_LINK_DELAY_MIN

    lo = _int_or(AI_AUTO_LINK_DELAY_MIN, _get(KEY_PACE_LINK_LO), lo=10, hi=1800)
    hi = _int_or(AI_AUTO_LINK_DELAY_MAX, _get(KEY_PACE_LINK_HI), lo=10, hi=1800)
    if lo > hi:
        lo, hi = hi, lo
    return lo, hi


def set_auto_link_delay_range(lo: int, hi: int) -> tuple[int, int]:
    lo, hi = int(lo), int(hi)
    if lo > hi:
        lo, hi = hi, lo
    lo = max(10, min(1800, lo))
    hi = max(10, min(1800, hi))
    _set(KEY_PACE_LINK_LO, str(lo))
    _set(KEY_PACE_LINK_HI, str(hi))
    return lo, hi


def format_sec_range(lo: int, hi: int) -> str:
    if lo == hi:
        return format_duration(lo)
    # prefer minutes when both >= 60
    if lo >= 60 and hi >= 60:
        return f"{lo // 60}–{hi // 60} мин"
    return f"{lo}–{hi} сек"
