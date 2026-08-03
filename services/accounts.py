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
