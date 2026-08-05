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

_claim_cursor = 0


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
    access_hash: int | None = None,
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
            """
            SELECT status, username, access_hash, source_account_user_id, failure_reason
              FROM leads WHERE target_user_id=?
            """,
            (target_user_id,),
        ).fetchone()
        if existing:
            status = str(existing["status"])
            identity_improved = (
                (username is not None and username != existing["username"])
                or (access_hash is not None and access_hash != existing["access_hash"])
                or (
                    source_account_user_id is not None
                    and source_account_user_id != existing["source_account_user_id"]
                )
            )
            reopen_no_entity_terminal = bool(
                status == STATUS_CANCELLED
                and existing["failure_reason"] == "no_entity_all_accounts"
            )
            if status == STATUS_SENT:
                conn.execute(
                    """
                    UPDATE leads
                       SET username=COALESCE(?, username),
                           first_name=COALESCE(?, first_name),
                           last_name=COALESCE(?, last_name),
                           access_hash=COALESCE(?, access_hash),
                           last_seen_at=?, updated_at=?
                     WHERE target_user_id=?
                    """,
                    (username, first_name, last_name, access_hash, now, now, target_user_id),
                )
                return f"skipped_status_{status}"

            if status == STATUS_CLAIMED:
                if identity_improved:
                    conn.execute(
                        """
                        UPDATE leads
                           SET username=COALESCE(?, username),
                               first_name=COALESCE(?, first_name),
                               last_name=COALESCE(?, last_name),
                               access_hash=COALESCE(?, access_hash),
                               source_chat_id=COALESCE(?, source_chat_id),
                               source_account_user_id=COALESCE(?, source_account_user_id),
                               send_attempts=0, last_error=NULL,
                               failure_reason=NULL, failure_at=NULL,
                               last_seen_at=?, updated_at=?
                         WHERE target_user_id=?
                        """,
                        (username, first_name, last_name, access_hash, source_chat_id,
                         source_account_user_id, now, now, target_user_id),
                    )
                    conn.execute(
                        "DELETE FROM lead_account_failures WHERE target_user_id=?",
                        (target_user_id,),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE leads
                           SET username=COALESCE(?, username),
                               first_name=COALESCE(?, first_name),
                               last_name=COALESCE(?, last_name),
                               access_hash=COALESCE(?, access_hash),
                               last_seen_at=?, updated_at=?
                         WHERE target_user_id=?
                        """,
                        (username, first_name, last_name, access_hash, now, now,
                         target_user_id),
                    )
                return f"skipped_status_{status}"

            if status == STATUS_CANCELLED and not (
                identity_improved or reopen_no_entity_terminal
            ):
                conn.execute(
                    """
                    UPDATE leads
                       SET username=COALESCE(?, username),
                           first_name=COALESCE(?, first_name),
                           last_name=COALESCE(?, last_name),
                           access_hash=COALESCE(?, access_hash),
                           last_seen_at=?, updated_at=?
                     WHERE target_user_id=?
                    """,
                    (username, first_name, last_name, access_hash, now, now,
                     target_user_id),
                )
                return f"skipped_status_{status}"

            # Pending technical retries keep counters/evidence unless Telegram
            # identity improved. Cancelled technical failures reopen on activity.
            if identity_improved or reopen_no_entity_terminal:
                conn.execute(
                    """
                    UPDATE leads
                       SET username=COALESCE(?, username),
                           first_name=COALESCE(?, first_name),
                           last_name=COALESCE(?, last_name),
                           access_hash=COALESCE(?, access_hash),
                           source_chat_id=COALESCE(?, source_chat_id),
                           source_account_user_id=COALESCE(?, source_account_user_id),
                           status=?, eligible_at=?, claimed_by_account=NULL, claimed_at=NULL,
                           send_attempts=0, last_error=NULL,
                           failure_reason=NULL, failure_at=NULL,
                           last_seen_at=?, updated_at=?
                     WHERE target_user_id=?
                    """,
                    (username, first_name, last_name, access_hash, source_chat_id,
                     source_account_user_id, STATUS_PENDING, now, now, now,
                     target_user_id),
                )
                conn.execute(
                    "DELETE FROM lead_account_failures WHERE target_user_id=?",
                    (target_user_id,),
                )
            else:
                conn.execute(
                    """
                    UPDATE leads
                       SET username=COALESCE(?, username),
                           first_name=COALESCE(?, first_name),
                           last_name=COALESCE(?, last_name),
                           access_hash=COALESCE(?, access_hash),
                           source_chat_id=COALESCE(?, source_chat_id),
                           source_account_user_id=COALESCE(?, source_account_user_id),
                           status=?, last_seen_at=?, updated_at=?
                     WHERE target_user_id=?
                    """,
                    (username, first_name, last_name, access_hash, source_chat_id,
                     source_account_user_id, STATUS_PENDING, now, now, target_user_id),
                )
            return "refreshed"

        conn.execute(
            """
            INSERT INTO leads (
                target_user_id, username, first_name, last_name, access_hash,
                source_chat_id, source_account_user_id, status,
                eligible_at, last_seen_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                target_user_id,
                username,
                first_name,
                last_name,
                access_hash,
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


def get_lead(target_user_id: int) -> dict[str, Any] | None:
    """Return one lead row for diagnostics and deterministic tests."""
    conn = get_connection()
    row = conn.execute(
        """
        SELECT target_user_id, username, first_name, last_name, access_hash,
               source_chat_id, source_account_user_id, status, eligible_at,
               claimed_by_account, claimed_at, send_attempts, last_error,
               failure_reason, failure_at, last_seen_at, created_at, updated_at
          FROM leads WHERE target_user_id=?
        """,
        (int(target_user_id),),
    ).fetchone()
    return dict(row) if row else None


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
            SELECT target_user_id, username, first_name, last_name, access_hash,
                   source_chat_id, source_account_user_id, status,
                   eligible_at, last_seen_at, created_at, last_error,
                   failure_reason, failure_at
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
            SELECT target_user_id, username, first_name, last_name, access_hash,
                   source_chat_id, source_account_user_id, status,
                   eligible_at, last_seen_at, created_at, last_error,
                   failure_reason, failure_at
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
        conn.execute(
            """
            DELETE FROM lead_account_failures
             WHERE target_user_id IN (SELECT target_user_id FROM leads WHERE status=?)
            """,
            (STATUS_PENDING,),
        )
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



def _eligible_pending_where() -> str:
    return """
        l.status=?
        AND (l.eligible_at IS NULL OR l.eligible_at <= ?)
        AND NOT EXISTS (
              SELECT 1 FROM opt_out o WHERE o.user_id = l.target_user_id
        )
        AND NOT EXISTS (
              SELECT 1 FROM contacts c
               WHERE c.target_user_id = l.target_user_id
                 AND c.status IN ('sending', 'in_progress', 'completed')
        )
    """


def _select_pending_after_cursor(conn, now: str, cursor: int, *, wrap: bool):
    comparison = "<=" if wrap else ">"
    return conn.execute(
        f"""
        SELECT l.target_user_id, l.username, l.first_name, l.last_name, l.access_hash,
               l.source_chat_id, l.source_account_user_id, l.status,
               l.eligible_at, l.last_seen_at, l.created_at, l.last_error,
               l.failure_reason, l.failure_at
          FROM leads l
         WHERE {_eligible_pending_where()}
           AND l.target_user_id {comparison} ?
         ORDER BY l.target_user_id ASC
         LIMIT 1
        """,
        (STATUS_PENDING, now, int(cursor)),
    ).fetchone()


def claim_random_pending(account_user_id: int) -> dict[str, Any] | None:
    """Claim one eligible lead using an indexed rotating cursor.

    The public name is kept for compatibility, but no full-table random sort is
    performed. Each successful claim advances a process-local primary-key cursor
    and wraps at the end of the queue.
    """
    global _claim_cursor
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        row = _select_pending_after_cursor(conn, now, _claim_cursor, wrap=False)
        if row is None:
            row = _select_pending_after_cursor(conn, now, _claim_cursor, wrap=True)
        if row is None:
            return None
        target = int(row["target_user_id"])
        cur = conn.execute(
            """
            UPDATE leads
               SET status=?, claimed_by_account=?, claimed_at=?, updated_at=?
             WHERE target_user_id=? AND status=?
            """,
            (STATUS_CLAIMED, int(account_user_id), now, now, target, STATUS_PENDING),
        )
        if int(cur.rowcount or 0) != 1:
            return None
        _claim_cursor = target
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
                   last_error=NULL,
                   failure_reason=NULL,
                   failure_at=NULL,
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
        conn.execute(
            "DELETE FROM lead_account_failures WHERE target_user_id=?",
            (int(target_user_id),),
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
                   last_error=?,
                   failure_reason=?,
                   failure_at=?,
                   updated_at=?
             WHERE target_user_id=?
            """,
            (STATUS_CANCELLED, reason, reason, now, now, int(target_user_id)),
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
      (send may never have happened - do NOT mark sent)
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
            outbox = conn.execute(
                "SELECT status FROM first_dm_outbox WHERE target_user_id=?",
                (tid,),
            ).fetchone()
            outbox_status = str(outbox["status"]) if outbox else None
            if outbox_status == "prepared":
                # Delivery is ambiguous and must be reconciled against Telegram.
                # Never release it blindly: that could create a duplicate First DM.
                continue
            if cstatus in {"in_progress", "completed"} or outbox_status == "sent":
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
                # 'sending' or no contact - safe to retry as pending.
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




def count_first_dm_total() -> int:
    """Durable all-time number of confirmed First-DM sends."""
    conn = get_connection()
    row = conn.execute("SELECT COUNT(*) AS c FROM first_dm_events").fetchone()
    return int(row["c"] if row else 0)


def source_label(lead: dict[str, Any]) -> str:
    """Readable source for admin notifications and queue screens."""
    chat_id = lead.get("source_chat_id")
    account_id = lead.get("source_account_user_id")
    if chat_id is not None:
        conn = get_connection()
        row = conn.execute(
            """
            SELECT title, username
              FROM account_discovered_chats
             WHERE account_user_id=? AND chat_id=?
            """,
            (int(account_id or 0), int(chat_id)),
        ).fetchone()
        if row:
            title = str(row["title"] or "").strip()
            username = str(row["username"] or "").strip().lstrip("@")
            if title:
                return title
            if username:
                return f"@{username}"
        return f"чат {chat_id}"
    return "импорт" if not account_id else "активность в группе"


def count_first_dm_today() -> int:
    """Confirmed First-DM sends since midnight in admin time (UTC+3)."""
    import datetime as dt

    admin_tz = dt.timezone(dt.timedelta(hours=3))
    local_now = dt.datetime.now(admin_tz)
    local_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_utc = local_start.astimezone(dt.timezone.utc).isoformat()
    end_utc = (local_start + dt.timedelta(days=1)).astimezone(dt.timezone.utc).isoformat()
    conn = get_connection()
    row = conn.execute(
        """
        SELECT COUNT(*) AS c
          FROM first_dm_events
         WHERE sent_at >= ? AND sent_at < ?
        """,
        (start_utc, end_utc),
    ).fetchone()
    return int(row["c"] if row else 0)


MAX_SEND_ATTEMPTS = 5


def mark_sending(target_user_id: int, account_user_id: int) -> None:
    """Mark contact as 'sending' BEFORE network send.

    Only mark_sent upgrades to in_progress (real success).
    Stale 'sending' is released back to pending - no false sent.
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
        # already claimed - reassign sender
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


def bump_send_attempts(target_user_id: int, error: str | None = None) -> int:
    """Increment and return retry count, keeping a concise diagnostic."""
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE leads
               SET send_attempts=COALESCE(send_attempts, 0) + 1,
                   last_error=COALESCE(?, last_error),
                   updated_at=?
             WHERE target_user_id=?
            """,
            ((str(error)[:500] if error else None), now, int(target_user_id)),
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




def reopen_entity_failures_for_new_account(account_user_id: int) -> int:
    """Reopen terminal no-entity leads when a genuinely untried sender joins."""
    uid = int(account_user_id)
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        cur = conn.execute(
            """
            UPDATE leads
               SET status=?, eligible_at=?, claimed_by_account=NULL, claimed_at=NULL,
                   failure_reason=NULL, failure_at=NULL,
                   last_error='new_sender_account_available', updated_at=?
             WHERE status=?
               AND failure_reason='no_entity_all_accounts'
               AND NOT EXISTS (
                   SELECT 1 FROM lead_account_failures f
                    WHERE f.target_user_id=leads.target_user_id
                      AND f.account_user_id=?
                      AND f.failure_kind='no_entity'
               )
            """,
            (STATUS_PENDING, now, now, STATUS_CANCELLED, uid),
        )
        return int(cur.rowcount or 0)


def get_last_error(target_user_id: int) -> str | None:
    conn = get_connection()
    row = conn.execute(
        "SELECT last_error FROM leads WHERE target_user_id=?",
        (int(target_user_id),),
    ).fetchone()
    if row is None or row["last_error"] is None:
        return None
    return str(row["last_error"])

def set_last_error(target_user_id: int, error: str | None) -> None:
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            "UPDATE leads SET last_error=?, updated_at=? WHERE target_user_id=?",
            ((str(error)[:500] if error else None), now, int(target_user_id)),
        )


def defer_claim(target_user_id: int, *, seconds: int, reason: str) -> None:
    """Release a claimed lead for a short operational retry without a tight loop."""
    eligible = (
        dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=max(1, int(seconds)))
    ).isoformat()
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE leads
               SET status=?, claimed_by_account=NULL, claimed_at=NULL,
                   eligible_at=?, last_error=COALESCE(last_error, ?), updated_at=?
             WHERE target_user_id=? AND status IN (?, ?)
            """,
            (STATUS_PENDING, eligible, str(reason)[:500], now,
             int(target_user_id), STATUS_CLAIMED, STATUS_PENDING),
        )


def record_account_failure(
    target_user_id: int, account_user_id: int, failure_kind: str, error: str | None = None
) -> None:
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            INSERT INTO lead_account_failures (
                target_user_id, account_user_id, failure_kind, error, attempted_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(target_user_id, account_user_id, failure_kind) DO UPDATE SET
                error=excluded.error, attempted_at=excluded.attempted_at
            """,
            (int(target_user_id), int(account_user_id), str(failure_kind),
             (str(error)[:500] if error else None), now),
        )
        conn.execute(
            "UPDATE leads SET last_error=?, updated_at=? WHERE target_user_id=?",
            ((str(error)[:500] if error else str(failure_kind)), now, int(target_user_id)),
        )


def failed_account_ids(target_user_id: int, failure_kind: str) -> set[int]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT account_user_id FROM lead_account_failures
         WHERE target_user_id=? AND failure_kind=?
        """,
        (int(target_user_id), str(failure_kind)),
    ).fetchall()
    return {int(row["account_user_id"]) for row in rows}



def clear_account_failures(target_user_id: int) -> None:
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            "DELETE FROM lead_account_failures WHERE target_user_id=?",
            (int(target_user_id),),
        )


def identity_snapshot_changed(target_user_id: int, lead: dict[str, Any]) -> bool:
    """Detect better entity evidence arriving while a lead was being attempted."""
    conn = get_connection()
    row = conn.execute(
        """
        SELECT username, access_hash, source_account_user_id
          FROM leads WHERE target_user_id=?
        """,
        (int(target_user_id),),
    ).fetchone()
    if row is None:
        return False
    def _norm(value):
        return None if value is None else str(value)
    return any(
        _norm(row[key]) != _norm(lead.get(key))
        for key in ("username", "access_hash", "source_account_user_id")
    )

def mark_terminal_failure(target_user_id: int, reason: str, error: str | None = None) -> None:
    """Stop automatic retries while allowing fresh future activity/import to reopen it."""
    now = _now_iso()
    detail = str(error or reason)[:500]
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE leads
               SET status=?, claimed_by_account=NULL, claimed_at=NULL,
                   last_error=?, failure_reason=?, failure_at=?, updated_at=?
             WHERE target_user_id=?
            """,
            (STATUS_CANCELLED, detail, str(reason)[:120], now, now, int(target_user_id)),
        )
        conn.execute(
            "DELETE FROM contacts WHERE target_user_id=? AND status='sending'",
            (int(target_user_id),),
        )


def list_recent_failures(limit: int = 10) -> list[dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT target_user_id, username, first_name, last_name, failure_reason,
               failure_at, last_error, send_attempts
          FROM leads
         WHERE status=? AND failure_reason IS NOT NULL
         ORDER BY COALESCE(failure_at, updated_at) DESC
         LIMIT ?
        """,
        (STATUS_CANCELLED, int(limit)),
    ).fetchall()
    return [dict(row) for row in rows]

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


def force_requeue(
    *,
    target_user_id: int,
    username: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
    access_hash: int | None = None,
    source_chat_id: int | None = None,
    source_account_user_id: int | None = None,
) -> bool:
    """
    Put target into pending for a new first DM even if previously sent.
    Opt-out still blocks. Clears contact row so claim path allows send.
    """
    target_user_id = int(target_user_id)
    if opt_out_svc.is_opted_out(target_user_id):
        return False
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        # Preserve the previous attempt before opening a new campaign cycle.
        # The archive keeps its own 30/180-day retention schedule and statistics.
        from services import dialog_archive

        dialog_archive.archive_current_attempt(
            target_user_id, reason="explicit_import_requeue", conn=conn
        )
        conn.execute(
            "DELETE FROM contacts WHERE target_user_id=?",
            (target_user_id,),
        )
        # The operational tables must contain only the new/current attempt.
        conn.execute(
            "DELETE FROM first_dm_outbox WHERE target_user_id=?",
            (target_user_id,),
        )
        conn.execute(
            "DELETE FROM dialog_outbox WHERE target_user_id=?",
            (target_user_id,),
        )
        conn.execute(
            "DELETE FROM dialog_inbox WHERE target_user_id=?",
            (target_user_id,),
        )
        conn.execute(
            "DELETE FROM dialogs WHERE target_user_id=?",
            (target_user_id,),
        )
        conn.execute(
            "DELETE FROM lead_account_failures WHERE target_user_id=?",
            (target_user_id,),
        )
        existing = conn.execute(
            "SELECT status FROM leads WHERE target_user_id=?",
            (target_user_id,),
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE leads
                   SET username=COALESCE(?, username),
                       first_name=COALESCE(?, first_name),
                       last_name=COALESCE(?, last_name),
                       access_hash=COALESCE(?, access_hash),
                       source_chat_id=COALESCE(?, source_chat_id),
                       source_account_user_id=COALESCE(?, source_account_user_id),
                       status=?,
                       eligible_at=?,
                       claimed_by_account=NULL,
                       claimed_at=NULL,
                       send_attempts=0,
                       last_error=NULL,
                       failure_reason=NULL,
                       failure_at=NULL,
                       last_seen_at=?,
                       updated_at=?
                 WHERE target_user_id=?
                """,
                (
                    username,
                    first_name,
                    last_name,
                    access_hash,
                    source_chat_id,
                    source_account_user_id,
                    STATUS_PENDING,
                    now,
                    now,
                    now,
                    target_user_id,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO leads (
                    target_user_id, username, first_name, last_name, access_hash,
                    source_chat_id, source_account_user_id, status,
                    eligible_at, claimed_by_account, claimed_at,
                    last_seen_at, created_at, updated_at, send_attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, 0)
                """,
                (
                    target_user_id,
                    username,
                    first_name,
                    last_name,
                    access_hash,
                    source_chat_id,
                    source_account_user_id,
                    STATUS_PENDING,
                    now,
                    now,
                    now,
                    now,
                ),
            )
    return True
