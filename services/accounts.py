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
               dm_interval_min_sec, dm_interval_max_sec,
               COALESCE(peerflood_streak, 0) AS peerflood_streak,
               peerflood_last_at,
               interval_backup_min, interval_backup_max, interval_backoff_until,
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
               dm_interval_min_sec, dm_interval_max_sec,
               COALESCE(peerflood_streak, 0) AS peerflood_streak,
               peerflood_last_at,
               interval_backup_min, interval_backup_max, interval_backoff_until,
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
                ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 'manual', ?, ?)
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


def format_cooldown_left(acc: dict[str, Any]) -> str | None:
    """Human remaining pause from cooldown_until, or None if not applicable."""
    raw = acc.get("cooldown_until")
    if not raw:
        return None
    try:
        s = str(raw).replace("Z", "+00:00")
        until = dt.datetime.fromisoformat(s)
        if until.tzinfo is None:
            until = until.replace(tzinfo=dt.timezone.utc)
        now = dt.datetime.now(dt.timezone.utc)
        sec = int((until - now).total_seconds())
    except Exception:
        return None
    if sec <= 0:
        return "скоро"
    if sec < 60:
        return f"~{sec}с"
    if sec < 3600:
        return f"~{sec // 60}м"
    h, m = divmod(sec // 60, 60)
    if m:
        return f"~{h}ч {m}м"
    return f"~{h}ч"


def dashboard_account_line(acc: dict[str, Any]) -> str:
    """One compact line for main menu: status + optional pause timer."""
    label = format_account_label(acc, include_id=False)
    if len(label) > 22:
        label = label[:21] + "…"
    participates = bool(acc.get("participates"))
    paused = bool(acc.get("is_paused"))
    if not participates:
        return f"⚪ {label}  выкл"
    if paused:
        reason = (acc.get("pause_reason") or "пауза").strip()
        # Short reason for dashboard
        if "peerflood" in reason.lower() or reason.lower() == "peerflood":
            reason_s = "PeerFlood"
        elif "flood" in reason.lower():
            reason_s = "FloodWait"
        else:
            reason_s = reason[:16]
        left = format_cooldown_left(acc)
        if left:
            return f"🔴 {label}  {reason_s} · {left}"
        return f"🔴 {label}  {reason_s}"
    return f"🟢 {label}"


def dashboard_accounts_block(*, limit: int = 8) -> str:
    """Multi-line block for main menu."""
    rows = list_accounts()
    if not rows:
        return "нет аккаунтов"
    lines = [dashboard_account_line(a) for a in rows[:limit]]
    extra = len(rows) - limit
    if extra > 0:
        lines.append(f"… ещё {extra} в 👤 Аккаунты")
    return "\n".join(lines)


def set_dm_interval(
    user_id: int,
    min_sec: int | None,
    max_sec: int | None,
) -> None:
    """None/None = use global pacing. Otherwise per-account range in seconds."""
    if min_sec is not None and max_sec is not None:
        lo, hi = int(min_sec), int(max_sec)
        if lo > hi:
            lo, hi = hi, lo
        lo = max(30, min(86400, lo))
        hi = max(30, min(86400, hi))
    else:
        lo, hi = None, None
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE accounts
               SET dm_interval_min_sec=?,
                   dm_interval_max_sec=?,
                   updated_at=?
             WHERE user_id=?
            """,
            (lo, hi, _now_iso(), int(user_id)),
        )


def format_dm_interval(acc: dict) -> str:
    """Human label for account interval override or global."""
    lo = acc.get("dm_interval_min_sec")
    hi = acc.get("dm_interval_max_sec")
    if lo is None or hi is None:
        from services import runtime as runtime_svc
        glo, ghi = runtime_svc.get_account_interval_range()
        return f"как в настройках ({glo // 60}–{ghi // 60} мин)"
    lo, hi = int(lo), int(hi)
    if lo >= 60 and hi >= 60:
        return f"свой: {lo // 60}–{hi // 60} мин"
    return f"свой: {lo}–{hi} сек"



def register_peerflood_hit(user_id: int) -> dict:
    """
    If PeerFlood repeats sooner than 10–15 min on this account → force ~30 min pause
    and temporarily slow first-DM interval; restore later via maybe_restore_*.

    Returns: streak, pause_seconds (absolute), interval_bumped.
    """
    import datetime as dt
    import random

    user_id = int(user_id)
    now = dt.datetime.now(dt.timezone.utc)
    now_iso = now.isoformat()
    acc = get_account(user_id) or {}

    last = None
    raw_last = acc.get("peerflood_last_at")
    if raw_last:
        try:
            last = dt.datetime.fromisoformat(str(raw_last).replace("Z", "+00:00"))
            if last.tzinfo is None:
                last = last.replace(tzinfo=dt.timezone.utc)
        except Exception:
            last = None

    # "Too fast" = previous PeerFlood within 10–15 min (random band)
    window_sec = random.randint(10 * 60, 15 * 60)
    rapid = bool(last and (now - last).total_seconds() <= window_sec)

    streak = int(acc.get("peerflood_streak") or 0)
    if rapid:
        streak = min(streak + 1, 10)
    else:
        streak = 1

    # Base pause from global PeerFlood settings
    from services import runtime as runtime_svc
    base = int(runtime_svc.pick_peer_flood_seconds())

    interval_bumped = False
    # Rapid loop → fixed ~30 min pause (not escalating forever)
    if rapid:
        pause_seconds = 30 * 60
        lo = acc.get("dm_interval_min_sec")
        hi = acc.get("dm_interval_max_sec")
        backup_min = acc.get("interval_backup_min")
        backup_max = acc.get("interval_backup_max")

        sets = {
            "peerflood_streak": streak,
            "peerflood_last_at": now_iso,
            "updated_at": now_iso,
        }
        if backup_min is None and backup_max is None:
            sets["interval_backup_min"] = -1 if lo is None else int(lo)
            sets["interval_backup_max"] = -1 if hi is None else int(hi)
        # Milder DM pace while cooling: 20–40 min between first DMs
        sets["dm_interval_min_sec"] = 20 * 60
        sets["dm_interval_max_sec"] = 40 * 60
        sets["interval_backoff_until"] = (now + dt.timedelta(minutes=30)).isoformat()
        interval_bumped = True

        cols = ", ".join(f"{k}=?" for k in sets)
        vals = list(sets.values()) + [user_id]
        conn = get_connection()
        with db_lock(), conn:
            conn.execute(f"UPDATE accounts SET {cols} WHERE user_id=?", vals)
    else:
        pause_seconds = base
        conn = get_connection()
        with db_lock(), conn:
            conn.execute(
                """
                UPDATE accounts
                   SET peerflood_streak=?,
                       peerflood_last_at=?,
                       updated_at=?
                 WHERE user_id=?
                """,
                (streak, now_iso, now_iso, user_id),
            )

    return {
        "streak": streak,
        "pause_seconds": int(pause_seconds),
        "pause_multiplier": 1,
        "interval_bumped": interval_bumped,
        "rapid": rapid,
    }


def maybe_restore_interval_after_success(user_id: int) -> bool:
    """Decay streak after successful DM; restore interval when safe."""
    import datetime as dt

    user_id = int(user_id)
    acc = get_account(user_id)
    if not acc:
        return False
    now = dt.datetime.now(dt.timezone.utc)
    now_iso = now.isoformat()
    streak = max(0, int(acc.get("peerflood_streak") or 0) - 1)
    backup_min = acc.get("interval_backup_min")
    backup_max = acc.get("interval_backup_max")
    until = None
    raw_until = acc.get("interval_backoff_until")
    if raw_until:
        try:
            until = dt.datetime.fromisoformat(str(raw_until).replace("Z", "+00:00"))
            if until.tzinfo is None:
                until = until.replace(tzinfo=dt.timezone.utc)
        except Exception:
            until = None

    restored = False
    conn = get_connection()
    with db_lock(), conn:
        if backup_min is not None and (
            streak <= 0 or (until is not None and until <= now)
        ):
            try:
                bmin = int(backup_min)
            except (TypeError, ValueError):
                bmin = -1
            try:
                bmax = int(backup_max) if backup_max is not None else bmin
            except (TypeError, ValueError):
                bmax = bmin
            if bmin == -1 or bmax == -1:
                new_lo, new_hi = None, None
            else:
                new_lo, new_hi = bmin, bmax
                if new_lo > new_hi:
                    new_lo, new_hi = new_hi, new_lo
            conn.execute(
                """
                UPDATE accounts
                   SET peerflood_streak=?,
                       dm_interval_min_sec=?,
                       dm_interval_max_sec=?,
                       interval_backup_min=NULL,
                       interval_backup_max=NULL,
                       interval_backoff_until=NULL,
                       updated_at=?
                 WHERE user_id=?
                """,
                (streak, new_lo, new_hi, now_iso, user_id),
            )
            restored = True
        else:
            conn.execute(
                """
                UPDATE accounts
                   SET peerflood_streak=?,
                       updated_at=?
                 WHERE user_id=?
                """,
                (streak, now_iso, user_id),
            )
    return restored
