"""Common lead queue (one row per target user)."""

from __future__ import annotations

import datetime as dt
from typing import Any, Optional

from db.schema import db_lock, get_connection
from services import opt_out as opt_out_svc

STATUS_PENDING = "pending"
STATUS_CLAIMED = "claimed"
STATUS_SENT = "sent"
STATUS_CANCELLED = "cancelled"


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _contact_status(target_user_id: int) -> Optional[str]:
    conn = get_connection()
    row = conn.execute(
        "SELECT status FROM contacts WHERE target_user_id=?",
        (int(target_user_id),),
    ).fetchone()
    return str(row["status"]) if row else None


def upsert_from_activity(
    *,
    target_user_id: int,
    username: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
    source_chat_id: int | None = None,
    source_account_user_id: int | None = None,
) -> str:
    """
    Upsert a lead from group activity.
    Returns action: created | refreshed | skipped_<reason>
    """
    target_user_id = int(target_user_id)

    if opt_out_svc.is_opted_out(target_user_id):
        return "skipped_opt_out"

    contact = _contact_status(target_user_id)
    if contact in {"sending", "in_progress", "completed"}:
        return f"skipped_contact_{contact}"

    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        existing = conn.execute(
            "SELECT status FROM leads WHERE target_user_id=?",
            (target_user_id,),
        ).fetchone()
        if existing:
            status = str(existing["status"])
            if status in {STATUS_CLAIMED, STATUS_SENT}:
                # Keep last_seen only; do not reopen claimed/sent here.
                conn.execute(
                    """
                    UPDATE leads
                       SET username=COALESCE(?, username),
                           first_name=COALESCE(?, first_name),
                           last_name=COALESCE(?, last_name),
                           last_seen_at=?,
                           updated_at=?
                     WHERE target_user_id=?
                    """,
                    (
                        username,
                        first_name,
                        last_name,
                        now,
                        now,
                        target_user_id,
                    ),
                )
                return f"skipped_status_{status}"

            # pending or cancelled → refresh to pending
            conn.execute(
                """
                UPDATE leads
                   SET username=COALESCE(?, username),
                       first_name=COALESCE(?, first_name),
                       last_name=COALESCE(?, last_name),
                       source_chat_id=COALESCE(?, source_chat_id),
                       source_account_user_id=COALESCE(?, source_account_user_id),
                       status=?,
                       eligible_at=COALESCE(eligible_at, ?),
                       claimed_by_account=NULL,
                       claimed_at=NULL,
                       last_seen_at=?,
                       updated_at=?
                 WHERE target_user_id=?
                """,
                (
                    username,
                    first_name,
                    last_name,
                    source_chat_id,
                    source_account_user_id,
                    STATUS_PENDING,
                    now,
                    now,
                    now,
                    target_user_id,
                ),
            )
            return "refreshed"

        conn.execute(
            """
            INSERT INTO leads (
                target_user_id, username, first_name, last_name,
                source_chat_id, source_account_user_id, status,
                eligible_at, last_seen_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                target_user_id,
                username,
                first_name,
                last_name,
                source_chat_id,
                source_account_user_id,
                STATUS_PENDING,
                now,
                now,
                now,
                now,
            ),
        )
        return "created"


def count_by_status(status: str | None = None) -> int:
    conn = get_connection()
    if status:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM leads WHERE status=?",
            (status,),
        ).fetchone()
    else:
        row = conn.execute("SELECT COUNT(*) AS c FROM leads").fetchone()
    return int(row["c"] if row else 0)


def list_recent(limit: int = 15, status: str | None = STATUS_PENDING) -> list[dict[str, Any]]:
    conn = get_connection()
    if status:
        rows = conn.execute(
            """
            SELECT target_user_id, username, first_name, last_name,
                   source_chat_id, source_account_user_id, status,
                   eligible_at, last_seen_at, created_at
              FROM leads
             WHERE status=?
             ORDER BY COALESCE(last_seen_at, created_at) DESC
             LIMIT ?
            """,
            (status, int(limit)),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT target_user_id, username, first_name, last_name,
                   source_chat_id, source_account_user_id, status,
                   eligible_at, last_seen_at, created_at
              FROM leads
             ORDER BY COALESCE(last_seen_at, created_at) DESC
             LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
    return [dict(r) for r in rows]


def clear_pending() -> int:
    conn = get_connection()
    with db_lock(), conn:
        cur = conn.execute(
            "DELETE FROM leads WHERE status=?",
            (STATUS_PENDING,),
        )
        return int(cur.rowcount or 0)


def format_lead_line(lead: dict[str, Any]) -> str:
    uid = lead.get("target_user_id")
    username = (lead.get("username") or "").strip().lstrip("@")
    first = (lead.get("first_name") or "").strip()
    if username:
        who = f"@{username}"
    elif first:
        who = first
    else:
        who = f"id{uid}"
    seen = (lead.get("last_seen_at") or lead.get("created_at") or "")[:19]
    return f"`{uid}` {who} | {seen}"



def claim_random_pending(account_user_id: int) -> dict[str, Any] | None:
    """Claim one random eligible pending lead for account. Returns lead dict or None.

    Skips opt-out users and anyone with contact status sending/in_progress/completed.
    """
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        row = conn.execute(
            """
            SELECT l.target_user_id, l.username, l.first_name, l.last_name,
                   l.source_chat_id, l.source_account_user_id, l.status,
                   l.eligible_at, l.last_seen_at, l.created_at
              FROM leads l
             WHERE l.status=?
               AND (l.eligible_at IS NULL OR l.eligible_at <= ?)
               AND NOT EXISTS (
                     SELECT 1 FROM opt_out o WHERE o.user_id = l.target_user_id
               )
               AND NOT EXISTS (
                     SELECT 1 FROM contacts c
                      WHERE c.target_user_id = l.target_user_id
                        AND c.status IN ('sending', 'in_progress', 'completed')
               )
             ORDER BY RANDOM()
             LIMIT 1
            """,
            (STATUS_PENDING, now),
        ).fetchone()
        if not row:
            return None
        target = int(row["target_user_id"])
        cur = conn.execute(
            """
            UPDATE leads
               SET status=?,
                   claimed_by_account=?,
                   claimed_at=?,
                   updated_at=?
             WHERE target_user_id=?
               AND status=?
            """,
            (
                STATUS_CLAIMED,
                int(account_user_id),
                now,
                now,
                target,
                STATUS_PENDING,
            ),
        )
        if int(cur.rowcount or 0) != 1:
            return None
        data = dict(row)
        data["status"] = STATUS_CLAIMED
        data["claimed_by_account"] = int(account_user_id)
        return data


def release_claim(target_user_id: int, *, as_pending: bool = True) -> None:
    now = _now_iso()
    status = STATUS_PENDING if as_pending else STATUS_CANCELLED
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE leads
               SET status=?,
                   claimed_by_account=NULL,
                   claimed_at=NULL,
                   updated_at=?
             WHERE target_user_id=?
               AND status=?
            """,
            (status, now, int(target_user_id), STATUS_CLAIMED),
        )


def mark_sent(target_user_id: int, account_user_id: int) -> None:
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE leads
               SET status=?,
                   claimed_by_account=?,
                   updated_at=?
             WHERE target_user_id=?
            """,
            (STATUS_SENT, int(account_user_id), now, int(target_user_id)),
        )
        conn.execute(
            """
            INSERT INTO contacts (target_user_id, status, sender_account_id, updated_at)
            VALUES (?, 'in_progress', ?, ?)
            ON CONFLICT(target_user_id) DO UPDATE SET
                status='in_progress',
                sender_account_id=excluded.sender_account_id,
                updated_at=excluded.updated_at
            """,
            (int(target_user_id), int(account_user_id), now),
        )


def cancel_lead(target_user_id: int, reason: str | None = None) -> None:
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE leads
               SET status=?,
                   claimed_by_account=NULL,
                   claimed_at=NULL,
                   updated_at=?
             WHERE target_user_id=?
            """,
            (STATUS_CANCELLED, now, int(target_user_id)),
        )
        # Prevent re-queue from group activity for privacy/invalid targets.
        conn.execute(
            """
            INSERT INTO contacts (target_user_id, status, sender_account_id, updated_at)
            VALUES (?, 'completed', NULL, ?)
            ON CONFLICT(target_user_id) DO UPDATE SET
                status='completed',
                updated_at=excluded.updated_at
            """,
            (int(target_user_id), now),
        )



def release_stale_claims(*, older_than_seconds: int = 900) -> int:
    """Recover stuck claims.

    - contact status 'sending' (pre-send) → back to pending, drop contact
      (send may never have happened — do NOT mark sent)
    - contact in_progress/completed (post successful mark_sent) → mark lead sent
    - no contact → back to pending
    """
    import datetime as dt

    cutoff = (
        dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=int(older_than_seconds))
    ).isoformat()
    now = _now_iso()
    conn = get_connection()
    released = 0
    with db_lock(), conn:
        rows = conn.execute(
            """
            SELECT target_user_id FROM leads
             WHERE status=?
               AND claimed_at IS NOT NULL
               AND claimed_at < ?
            """,
            (STATUS_CLAIMED, cutoff),
        ).fetchall()
        for row in rows:
            tid = int(row["target_user_id"])
            contact = conn.execute(
                "SELECT status FROM contacts WHERE target_user_id=?",
                (tid,),
            ).fetchone()
            cstatus = str(contact["status"]) if contact else None
            if cstatus in {"in_progress", "completed"}:
                # Message was acknowledged as sent at least once.
                conn.execute(
                    """
                    UPDATE leads
                       SET status=?,
                           updated_at=?
                     WHERE target_user_id=?
                    """,
                    (STATUS_SENT, now, tid),
                )
            else:
                # 'sending' or no contact — safe to retry as pending.
                conn.execute(
                    """
                    UPDATE leads
                       SET status=?,
                           claimed_by_account=NULL,
                           claimed_at=NULL,
                           updated_at=?
                     WHERE target_user_id=?
                    """,
                    (STATUS_PENDING, now, tid),
                )
                if cstatus == "sending":
                    conn.execute(
                        "DELETE FROM contacts WHERE target_user_id=? AND status='sending'",
                        (tid,),
                    )
            released += 1
    return released




def count_first_dm_today() -> int:
    """Sum of accounts' daily_sent_count for today (UTC date)."""
    import datetime as dt

    today = dt.datetime.now(dt.timezone.utc).date().isoformat()
    conn = get_connection()
    row = conn.execute(
        """
        SELECT COALESCE(SUM(COALESCE(daily_sent_count, 0)), 0) AS c
          FROM accounts
         WHERE daily_sent_date = ?
        """,
        (today,),
    ).fetchone()
    return int(row["c"] if row else 0)


MAX_SEND_ATTEMPTS = 5


def mark_sending(target_user_id: int, account_user_id: int) -> None:
    """Mark contact as 'sending' BEFORE network send.

    Only mark_sent upgrades to in_progress (real success).
    Stale 'sending' is released back to pending — no false sent.
    """
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            INSERT INTO contacts (target_user_id, status, sender_account_id, updated_at)
            VALUES (?, 'sending', ?, ?)
            ON CONFLICT(target_user_id) DO UPDATE SET
                status='sending',
                sender_account_id=excluded.sender_account_id,
                updated_at=excluded.updated_at
            """,
            (int(target_user_id), int(account_user_id), now),
        )
        conn.execute(
            """
            UPDATE leads
               SET claimed_by_account=?,
                   updated_at=?
             WHERE target_user_id=?
            """,
            (int(account_user_id), now, int(target_user_id)),
        )


def ensure_claim(target_user_id: int, account_user_id: int) -> bool:
    """Make sure lead is claimed by account. Returns False if lead is not claimable."""
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        row = conn.execute(
            "SELECT status FROM leads WHERE target_user_id=?",
            (int(target_user_id),),
        ).fetchone()
        if not row:
            return False
        status = str(row["status"])
        if status in {STATUS_SENT, STATUS_CANCELLED}:
            return False
        if status == STATUS_PENDING:
            cur = conn.execute(
                """
                UPDATE leads
                   SET status=?,
                       claimed_by_account=?,
                       claimed_at=?,
                       updated_at=?
                 WHERE target_user_id=?
                   AND status=?
                """,
                (
                    STATUS_CLAIMED,
                    int(account_user_id),
                    now,
                    now,
                    int(target_user_id),
                    STATUS_PENDING,
                ),
            )
            return int(cur.rowcount or 0) == 1
        # already claimed — reassign sender
        conn.execute(
            """
            UPDATE leads
               SET claimed_by_account=?,
                   claimed_at=?,
                   updated_at=?
             WHERE target_user_id=?
               AND status=?
            """,
            (
                int(account_user_id),
                now,
                now,
                int(target_user_id),
                STATUS_CLAIMED,
            ),
        )
        return True


def reassign_claim(target_user_id: int, account_user_id: int) -> None:
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE leads
               SET claimed_by_account=?,
                   claimed_at=?,
                   updated_at=?
             WHERE target_user_id=?
               AND status=?
            """,
            (
                int(account_user_id),
                now,
                now,
                int(target_user_id),
                STATUS_CLAIMED,
            ),
        )


def bump_send_attempts(target_user_id: int) -> int:
    """Increment and return new attempt count."""
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE leads
               SET send_attempts=COALESCE(send_attempts, 0) + 1,
                   updated_at=?
             WHERE target_user_id=?
            """,
            (now, int(target_user_id)),
        )
        row = conn.execute(
            "SELECT COALESCE(send_attempts, 0) AS c FROM leads WHERE target_user_id=?",
            (int(target_user_id),),
        ).fetchone()
        return int(row["c"] if row else 0)


def get_send_attempts(target_user_id: int) -> int:
    conn = get_connection()
    row = conn.execute(
        "SELECT COALESCE(send_attempts, 0) AS c FROM leads WHERE target_user_id=?",
        (int(target_user_id),),
    ).fetchone()
    return int(row["c"] if row else 0)


def clear_sending_contact(target_user_id: int) -> None:
    """Remove pre-send / in_progress contact if first DM did not complete."""
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            DELETE FROM contacts
             WHERE target_user_id=?
               AND status IN ('sending', 'in_progress')
            """,
            (int(target_user_id),),
        )


def format_target_label(lead: dict[str, Any]) -> str:
    """Nice label for lead: @user or name or id."""
    username = (lead.get("username") or "").strip().lstrip("@")
    first = (lead.get("first_name") or "").strip()
    last = (lead.get("last_name") or "").strip()
    name = " ".join(p for p in (first, last) if p).strip()
    tid = int(lead.get("target_user_id") or 0)
    if username:
        return f"@{username}"
    if name:
        return name
    return f"id {tid}"
