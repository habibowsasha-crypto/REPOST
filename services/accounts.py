"""Account persistence and display helpers."""

from __future__ import annotations

import datetime as dt
from typing import Any, Optional

from db.schema import db_lock, get_connection


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _row_to_dict(row: Any) -> dict[str, Any]:
    return dict(row) if row is not None else {}


def _clear_expired_floodwaits(user_id: int | None = None) -> int:
    """Auto-clear only expired Telegram FloodWait display state.

    PeerFlood/SpamBot pauses are intentionally untouched and remain under the
    dedicated resume flow.
    """
    now = _now_iso()
    conn = get_connection()
    where = "" if user_id is None else " AND user_id=?"
    params: tuple[Any, ...] = (now,) if user_id is None else (now, int(user_id))
    with db_lock(), conn:
        cur = conn.execute(
            f"""
            UPDATE accounts
               SET is_paused=0,
                   cooldown_until=NULL,
                   pause_reason=NULL,
                   updated_at=?
             WHERE is_paused=1
               AND cooldown_until IS NOT NULL
               AND cooldown_until <= ?
               AND LOWER(COALESCE(pause_reason, '')) LIKE 'floodwait%'
               {where}
            """,
            (now, *params),
        )
        return int(cur.rowcount or 0)


def count_accounts() -> int:
    conn = get_connection()
    row = conn.execute("SELECT COUNT(*) AS c FROM accounts").fetchone()
    return int(row["c"] if row else 0)


def count_participating() -> int:
    conn = get_connection()
    row = conn.execute(
        """
        SELECT COUNT(*) AS c
          FROM accounts
         WHERE participates=1
           AND COALESCE(auth_status, 'unknown') != 'reauth_required'
        """
    ).fetchone()
    return int(row["c"] if row else 0)


def count_authorized() -> int:
    conn = get_connection()
    row = conn.execute(
        "SELECT COUNT(*) AS c FROM accounts WHERE auth_status='authorized'"
    ).fetchone()
    return int(row["c"] if row else 0)


def count_reauth_required() -> int:
    conn = get_connection()
    row = conn.execute(
        "SELECT COUNT(*) AS c FROM accounts WHERE auth_status='reauth_required'"
    ).fetchone()
    return int(row["c"] if row else 0)


def list_reauth_required() -> list[dict[str, Any]]:
    return [
        row
        for row in list_accounts()
        if str(row.get("auth_status") or "unknown") == "reauth_required"
    ]


def is_reauth_required(acc: dict[str, Any] | None) -> bool:
    return bool(
        acc
        and str(acc.get("auth_status") or "unknown") == "reauth_required"
    )


def list_accounts() -> list[dict[str, Any]]:
    _clear_expired_floodwaits()
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
               COALESCE(auth_status, 'unknown') AS auth_status,
               auth_error, auth_lost_at, auth_notified_at,
               created_at, updated_at
          FROM accounts
         ORDER BY created_at DESC
        """
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_account(user_id: int) -> Optional[dict[str, Any]]:
    _clear_expired_floodwaits(int(user_id))
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
               COALESCE(auth_status, 'unknown') AS auth_status,
               auth_error, auth_lost_at, auth_notified_at,
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
                       auth_status='authorized',
                       auth_error=NULL,
                       auth_lost_at=NULL,
                       auth_notified_at=NULL,
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
                    participates, is_paused, chat_mode, auth_status,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 'manual', 'authorized', ?, ?)
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
    uid = int(user_id)
    conn = get_connection()
    became_participating = False
    with db_lock(), conn:
        previous = conn.execute(
            "SELECT participates FROM accounts WHERE user_id=?", (uid,)
        ).fetchone()
        cur = conn.execute(
            """
            UPDATE accounts
               SET participates=?, updated_at=?
             WHERE user_id=?
            """,
            (1 if value else 0, _now_iso(), uid),
        )
        changed = int(cur.rowcount or 0) == 1
        became_participating = bool(
            changed and value and previous and not bool(previous["participates"])
        )
    if became_participating:
        from services import queue as queue_svc

        queue_svc.reopen_entity_failures_for_new_account(uid)
    return changed


def delete_account(user_id: int) -> bool:
    """Delete credentials/config while preserving history as inactive records."""
    conn = get_connection()
    uid = int(user_id)
    now = _now_iso()
    with db_lock(), conn:
        # If deletion happens during an ambiguous First DM, the session needed for
        # Telegram reconciliation is about to disappear. Archive it as cancelled
        # instead of risking a duplicate from another account later.
        ambiguous_rows = conn.execute(
            """
            SELECT target_user_id
              FROM first_dm_outbox
             WHERE account_user_id=? AND status='prepared'
            """,
            (uid,),
        ).fetchall()
        ambiguous_targets = [int(r["target_user_id"]) for r in ambiguous_rows]
        conn.execute(
            """
            UPDATE first_dm_outbox
               SET status='failed', last_error='account_deleted_before_reconcile',
                   updated_at=?
             WHERE account_user_id=? AND status='prepared'
            """,
            (now, uid),
        )
        if ambiguous_targets:
            placeholders = ",".join("?" for _ in ambiguous_targets)
            conn.execute(
                f"""
                UPDATE leads
                   SET status='cancelled', claimed_by_account=NULL, claimed_at=NULL,
                       updated_at=?
                 WHERE target_user_id IN ({placeholders})
                """,
                (now, *ambiguous_targets),
            )
            conn.execute(
                f"""
                UPDATE contacts
                   SET status='completed', updated_at=?
                 WHERE target_user_id IN ({placeholders})
                """,
                (now, *ambiguous_targets),
            )

        # Only live conversations are closed. Completed attempts keep their final
        # stage, but every pending Telegram cleanup becomes explicitly abandoned
        # because the session is intentionally being removed.
        active_stages = (
            "first_dm_sending",
            "waiting_reply",
            "engaged",
            "explained",
        )
        placeholders = ",".join("?" for _ in active_stages)
        targets = conn.execute(
            f"SELECT target_user_id FROM dialogs "
            f"WHERE account_user_id=? AND stage IN ({placeholders})",
            (uid, *active_stages),
        ).fetchall()
        target_ids = [int(r["target_user_id"]) for r in targets]
        conn.execute(
            f"""
            UPDATE dialogs
               SET stage='closed', auto_link_at=NULL,
                   lifecycle_completed_at=COALESCE(lifecycle_completed_at, ?),
                   updated_at=?
             WHERE account_user_id=? AND stage IN ({placeholders})
            """,
            (now, now, uid, *active_stages),
        )
        conn.execute(
            """
            UPDATE dialogs
               SET telegram_delete_abandoned_at=?,
                   telegram_delete_next_attempt_at=NULL,
                   telegram_delete_last_error='account_deleted_before_retention'
             WHERE account_user_id=?
               AND telegram_deleted_at IS NULL
               AND telegram_delete_abandoned_at IS NULL
               AND telegram_delete_at IS NOT NULL
            """,
            (now, uid),
        )
        conn.execute(
            """
            UPDATE dialog_archives
               SET telegram_delete_abandoned_at=?,
                   telegram_delete_next_attempt_at=NULL,
                   telegram_delete_last_error='account_deleted_before_retention'
             WHERE account_user_id=?
               AND telegram_deleted_at IS NULL
               AND telegram_delete_abandoned_at IS NULL
               AND telegram_delete_at IS NOT NULL
            """,
            (now, uid),
        )
        if target_ids:
            placeholders = ",".join("?" for _ in target_ids)
            conn.execute(
                f"""
                UPDATE contacts
                   SET status='completed', updated_at=?
                 WHERE target_user_id IN ({placeholders})
                """,
                (now, *target_ids),
            )

        # A pre-send claim is not a historical dialog: release it safely.
        conn.execute(
            "DELETE FROM contacts WHERE sender_account_id=? AND status='sending'",
            (uid,),
        )
        conn.execute(
            """
            UPDATE leads
               SET status='pending', claimed_by_account=NULL, claimed_at=NULL, updated_at=?
             WHERE claimed_by_account=? AND status='claimed'
            """,
            (now, uid),
        )

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
        conn.execute(
            "DELETE FROM dialog_outbox WHERE account_user_id=? AND status='prepared'",
            (uid,),
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
    if is_reauth_required(acc):
        return "требуется повторный вход"
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
    except (TypeError, ValueError, OverflowError):
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
    """Compact two-line account status block for the main dashboard."""
    from services import dialog_store as dialog_store_svc

    label = format_account_label(acc, include_id=False)
    if len(label) > 24:
        label = label[:23] + "…"
    dialogs = dialog_store_svc.count_open_for_account(int(acc["user_id"]))
    retention_waiting = dialog_store_svc.count_retention_waiting_for_account(
        int(acc["user_id"])
    )
    participates = bool(acc.get("participates"))
    paused = bool(acc.get("is_paused"))

    if is_reauth_required(acc):
        return f"🔴 **{label}**\n└ Требуется повторный вход · диалогов {dialogs}"

    if paused:
        reason = (acc.get("pause_reason") or "пауза").strip()
        if "peerflood" in reason.lower():
            reason_s = "PeerFlood"
        elif "flood" in reason.lower():
            reason_s = "FloodWait"
        else:
            reason_s = reason[:22]
        left = format_cooldown_left(acc)
        detail = f"{reason_s} · ещё {left.lstrip('~')}" if left else reason_s
        return f"🔴 **{label}**\n└ {detail} · диалогов {dialogs}"

    if not participates:
        if dialogs:
            return (
                f"🟡 **{label}**\n"
                f"└ First DM отключены · завершает диалогов {dialogs}"
            )
        if retention_waiting:
            return (
                f"🟡 **{label}**\n"
                f"└ First DM отключены · очистка {retention_waiting}"
            )
        return f"🟡 **{label}**\n└ First DM отключены · диалогов 0"

    return f"🟢 **{label}**\n└ First DM включены · диалогов {dialogs}"


def dashboard_accounts_block(*, limit: int = 8) -> str:
    """Multi-line account list for the main menu."""
    rows = list_accounts()
    if not rows:
        return "└ Аккаунты ещё не добавлены"
    blocks = [dashboard_account_line(a) for a in rows[:limit]]
    extra = len(rows) - limit
    if extra > 0:
        blocks.append(f"… ещё {extra} в разделе «Аккаунты»")
    return "\n\n".join(blocks)


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
        except (TypeError, ValueError, OverflowError):
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
        except (TypeError, ValueError, OverflowError):
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
