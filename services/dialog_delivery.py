"""Durable outbox for every post-First-DM message in a dialog."""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

from config import LOCAL_DIALOG_TEXT_RETENTION_DAYS
from db.schema import db_lock, get_connection
from services import phrases as phrases_svc

KIND_AUTO_LINK = "auto_link"
KIND_FOLLOWUP = "followup"
KIND_ENGAGE = "engage"
KIND_EXPLAIN = "explain"
KIND_CONTEXTUAL = "contextual"
KIND_DIRECT_LINK = "direct_link"
KIND_CLOSE = "close"
KIND_APOLOGY = "apology"
KIND_PROMO = "promo"
KIND_SMOOTH_APOLOGY = "smooth_apology"
KIND_LINK_HELP = "link_help"
KIND_QNA = "qna"
KIND_STOP_CLOSE = "stop_close"

STATUS_PREPARED = "prepared"
STATUS_SENT = "sent"
STATUS_FAILED = "failed"


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def inbox_action_key(message_kind: str, inbox_id: int) -> str:
    return f"{message_kind}:inbox:{int(inbox_id)}"


def _default_transition(message_kind: str) -> dict[str, Any]:
    if message_kind == KIND_AUTO_LINK:
        return {
            "stage": "link_sent",
            "bump_outgoing": True,
            "link_sent": True,
            "clear_auto_link": True,
            "append_history": True,
        }
    if message_kind == KIND_FOLLOWUP:
        return {
            "stage": "followup_sent",
            "bump_outgoing": True,
            "clear_auto_link": True,
            "append_history": True,
        }
    if message_kind == KIND_SMOOTH_APOLOGY:
        return {
            "stage": "apology_sent",
            "bump_outgoing": True,
            "clear_auto_link": True,
            "append_history": True,
        }
    if message_kind == KIND_LINK_HELP:
        return {
            "stage": "link_help_sent",
            "bump_outgoing": True,
            "clear_auto_link": True,
            "append_history": True,
        }
    return {"bump_outgoing": True, "append_history": True}


def _base_kind(action_kind: str, message_kind: str | None) -> str:
    if message_kind:
        return str(message_kind)
    return str(action_kind).split(":", 1)[0]


def _phrase_kind_for_message(message_kind: str) -> str | None:
    kind = str(message_kind or "")
    if kind in {KIND_PROMO, KIND_AUTO_LINK}:
        return phrases_svc.KIND_PROMO
    if kind == KIND_SMOOTH_APOLOGY:
        return phrases_svc.KIND_APOLOGY
    if kind == KIND_LINK_HELP:
        return phrases_svc.KIND_LINK_HELP
    return None


def prepare(
    target_user_id: int,
    account_user_id: int,
    action_kind: str,
    text: str,
    *,
    message_kind: str | None = None,
    transition: dict[str, Any] | None = None,
    source_inbox_id: int | None = None,
    allow_opt_out: bool = False,
) -> bool:
    """Persist one outbound dialog message before touching Telegram.

    ``action_kind`` is the unique per-dialog delivery key. Scheduled legacy keys
    remain ``auto_link`` and ``followup``; user-triggered messages use keys tied to
    the durable inbox row, e.g. ``explain:inbox:42``.
    """
    target = int(target_user_id)
    account = int(account_user_id)
    action_key = str(action_kind)
    kind = _base_kind(action_key, message_kind)
    transition_data = dict(transition or _default_transition(kind))
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        dialog = conn.execute(
            "SELECT stage, link_sent FROM dialogs WHERE target_user_id=?",
            (target,),
        ).fetchone()
        if not dialog:
            return False
        stage = str(dialog["stage"])
        if stage == "closed" and not allow_opt_out:
            return False
        if action_key == KIND_AUTO_LINK:
            if stage != "explained" or int(dialog["link_sent"] or 0):
                return False
        elif action_key == KIND_SMOOTH_APOLOGY:
            if stage != "promo_sent" or not int(dialog["link_sent"] or 0):
                return False
        elif action_key == KIND_LINK_HELP:
            allow_legacy_skip = bool(transition_data.get("allow_skip_apology", False))
            valid_stage = stage == "apology_sent" or (
                allow_legacy_skip and stage == "promo_sent"
            )
            if not valid_stage or not int(dialog["link_sent"] or 0):
                return False
        elif action_key == KIND_FOLLOWUP and stage != "waiting_reply":
            return False

        existing = conn.execute(
            """
            SELECT status, recovery_next_at FROM dialog_outbox
             WHERE target_user_id=? AND action_kind=?
            """,
            (target, action_key),
        ).fetchone()
        if existing and str(existing["status"]) in {STATUS_PREPARED, STATUS_SENT}:
            return False
        if existing and str(existing["status"]) == STATUS_FAILED:
            retry_at = str(existing["recovery_next_at"] or "")
            if retry_at and retry_at > now:
                return False

        purge_due = (
            _now() + dt.timedelta(days=LOCAL_DIALOG_TEXT_RETENTION_DAYS)
        ).isoformat()
        conn.execute(
            """
            INSERT INTO dialog_outbox (
                target_user_id, action_kind, account_user_id, text, status,
                prepared_at, telegram_message_id, sent_at, last_error, updated_at,
                message_kind, transition_json, source_inbox_id, allow_opt_out,
                recovery_attempts, recovery_next_at, recovery_last_error
            ) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?, ?, ?, ?, 0, NULL, NULL)
            ON CONFLICT(target_user_id, action_kind) DO UPDATE SET
                account_user_id=excluded.account_user_id,
                text=excluded.text,
                status=excluded.status,
                prepared_at=excluded.prepared_at,
                telegram_message_id=NULL,
                sent_at=NULL,
                last_error=NULL,
                updated_at=excluded.updated_at,
                message_kind=excluded.message_kind,
                transition_json=excluded.transition_json,
                source_inbox_id=excluded.source_inbox_id,
                allow_opt_out=excluded.allow_opt_out,
                recovery_attempts=0,
                recovery_next_at=NULL,
                recovery_last_error=NULL
            """,
            (
                target,
                action_key,
                account,
                str(text),
                STATUS_PREPARED,
                now,
                now,
                kind,
                json.dumps(transition_data, ensure_ascii=False),
                int(source_inbox_id) if source_inbox_id is not None else None,
                1 if allow_opt_out else 0,
            ),
        )
        conn.execute(
            """
            UPDATE dialogs
               SET history_purge_at=CASE
                       WHEN history_purge_at IS NULL OR history_purge_at>? THEN ?
                       ELSE history_purge_at
                   END,
                   history_purged_at=NULL
             WHERE target_user_id=?
            """,
            (purge_due, purge_due, target),
        )
        phrase_kind = _phrase_kind_for_message(kind)
        if phrase_kind:
            phrases_svc.remember(
                phrase_kind,
                str(text),
                delivery_key=f"dialog:{target}:{action_key}",
                conn=conn,
                created_at=now,
            )
    return True


def replace_prepared_text(
    target_user_id: int,
    action_kind: str,
    text: str,
) -> bool:
    """Replace text of a PREPARED row before Telegram delivery.

    Used by final safety guards when an older prepared message contains a repeated
    greeting. The outbox, dialog history source and phrase journal stay consistent.
    """
    target = int(target_user_id)
    action_key = str(action_kind)
    value = str(text or "").strip()
    if not value:
        return False
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        row = conn.execute(
            """
            SELECT status, message_kind FROM dialog_outbox
             WHERE target_user_id=? AND action_kind=?
            """,
            (target, action_key),
        ).fetchone()
        if not row or str(row["status"]) != STATUS_PREPARED:
            return False
        conn.execute(
            """
            UPDATE dialog_outbox
               SET text=?, updated_at=?
             WHERE target_user_id=? AND action_kind=? AND status=?
            """,
            (value, now, target, action_key, STATUS_PREPARED),
        )
        kind = str(row["message_kind"] or _base_kind(action_key, None))
        phrase_kind = _phrase_kind_for_message(kind)
        if phrase_kind:
            phrases_svc.remember(
                phrase_kind,
                value,
                delivery_key=f"dialog:{target}:{action_key}",
                conn=conn,
                created_at=now,
            )
    return True


def _load_history(raw: str | None) -> list[dict[str, Any]]:
    try:
        history = json.loads(raw or "[]")
        if not isinstance(history, list):
            return []
        return history
    except (TypeError, json.JSONDecodeError):
        return []


def _parse_transition(raw: str | None, message_kind: str) -> dict[str, Any]:
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                return data
        except (TypeError, json.JSONDecodeError):
            pass
    return _default_transition(message_kind)


def commit_sent(
    target_user_id: int,
    action_kind: str,
    *,
    telegram_message_id: int | None = None,
    sent_at: str | None = None,
) -> bool:
    """Atomically confirm Telegram delivery and its dialog transition."""
    target = int(target_user_id)
    action_key = str(action_kind)
    now = _now_iso()
    sent = sent_at or now
    conn = get_connection()
    with db_lock(), conn:
        row = conn.execute(
            """
            SELECT account_user_id, text, status, message_kind, transition_json,
                   allow_opt_out, source_inbox_id, prepared_at
              FROM dialog_outbox
             WHERE target_user_id=? AND action_kind=?
            """,
            (target, action_key),
        ).fetchone()
        if not row:
            return False
        if str(row["status"]) == STATUS_SENT:
            return True
        dialog = conn.execute(
            """
            SELECT stage, outgoing_count, link_sent, auto_link_at, history_json,
                   lifecycle_completed_at
              FROM dialogs WHERE target_user_id=?
            """,
            (target,),
        ).fetchone()
        if not dialog:
            return False
        allow_opt_out = bool(int(row["allow_opt_out"] or 0))
        if str(dialog["stage"]) == "closed" and not allow_opt_out:
            conn.execute(
                """
                UPDATE dialog_outbox
                   SET status=?, last_error=?, updated_at=?
                 WHERE target_user_id=? AND action_kind=?
                """,
                (STATUS_FAILED, "dialog_closed_before_commit", now, target, action_key),
            )
            return False

        kind = str(row["message_kind"] or _base_kind(action_key, None))
        transition = _parse_transition(row["transition_json"], kind)
        text = str(row["text"] or "")
        history = _load_history(dialog["history_json"])
        if bool(transition.get("append_history", True)):
            history.append({"role": "assistant", "text": text, "at": sent})
            history = history[-20:]

        outgoing = int(dialog["outgoing_count"] or 0)
        if bool(transition.get("bump_outgoing", True)):
            outgoing += 1
        stage = str(transition.get("stage") or dialog["stage"])
        link_sent = int(dialog["link_sent"] or 0)
        if transition.get("link_sent") is not None:
            link_sent = 1 if bool(transition.get("link_sent")) else 0
        auto_link_at = dialog["auto_link_at"]
        if bool(transition.get("clear_auto_link", False)):
            auto_link_at = None
        elif "auto_link_at" in transition:
            auto_link_at = transition.get("auto_link_at")

        from services import dialog_store as dialog_store_svc

        completed_at = dialog["lifecycle_completed_at"]
        if dialog_store_svc.is_active_stage(stage):
            completed_at = None
        elif dialog_store_svc.is_terminal_stage(stage) and not completed_at:
            completed_at = sent

        conn.execute(
            """
            UPDATE dialog_outbox
               SET status=?, telegram_message_id=COALESCE(?, telegram_message_id),
                   sent_at=COALESCE(sent_at, ?), last_error=NULL, updated_at=?,
                   recovery_attempts=0, recovery_next_at=NULL,
                   recovery_last_error=NULL
             WHERE target_user_id=? AND action_kind=?
            """,
            (STATUS_SENT, telegram_message_id, sent, now, target, action_key),
        )
        conn.execute(
            """
            UPDATE dialogs
               SET stage=?, outgoing_count=?, link_sent=?, auto_link_at=?,
                   history_json=?, lifecycle_completed_at=?, updated_at=?
             WHERE target_user_id=?
            """,
            (
                stage,
                outgoing,
                link_sent,
                auto_link_at,
                json.dumps(history, ensure_ascii=False),
                completed_at,
                now,
                target,
            ),
        )
        if bool(transition.get("mark_contact_completed", False)):
            conn.execute(
                """
                UPDATE contacts SET status='completed', updated_at=?
                 WHERE target_user_id=?
                """,
                (now, target),
            )
        source_inbox_id = row["source_inbox_id"]
        if source_inbox_id is not None:
            conn.execute(
                """
                UPDATE dialog_inbox
                   SET status='done', processed_at=COALESCE(processed_at, ?),
                       updated_at=?, last_error=NULL
                 WHERE id=? AND status IN ('pending', 'processing')
                """,
                (now, now, int(source_inbox_id)),
            )
        # Idempotent recovery backfill for messages prepared by older versions.
        phrase_kind = _phrase_kind_for_message(kind)
        if phrase_kind:
            phrases_svc.remember(
                phrase_kind,
                text,
                delivery_key=f"dialog:{target}:{action_key}",
                conn=conn,
                created_at=sent,
            )
    return True


def mark_failed(target_user_id: int, action_kind: str, error: str) -> None:
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE dialog_outbox
               SET status=?, last_error=?, updated_at=?,
                   recovery_next_at=NULL, recovery_last_error=NULL
             WHERE target_user_id=? AND action_kind=? AND status=?
            """,
            (
                STATUS_FAILED,
                str(error)[:500],
                _now_iso(),
                int(target_user_id),
                str(action_kind),
                STATUS_PREPARED,
            ),
        )


def mark_failed_with_backoff(
    target_user_id: int,
    action_kind: str,
    error: str,
    *,
    delay_seconds: int,
) -> str:
    """Fail one prepared action and persist a retry-not-before timestamp."""
    now = _now()
    retry_at = (now + dt.timedelta(seconds=max(1, int(delay_seconds)))).isoformat()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE dialog_outbox
               SET status=?, last_error=?, updated_at=?,
                   recovery_attempts=COALESCE(recovery_attempts, 0) + 1,
                   recovery_next_at=?, recovery_last_error=?
             WHERE target_user_id=? AND action_kind=? AND status=?
            """,
            (
                STATUS_FAILED,
                str(error)[:500],
                now.isoformat(),
                retry_at,
                str(error)[:500],
                int(target_user_id),
                str(action_kind),
                STATUS_PREPARED,
            ),
        )
    return retry_at


def retry_not_before(target_user_id: int, action_kind: str) -> str | None:
    row = get(target_user_id, action_kind)
    value = str((row or {}).get("recovery_next_at") or "")
    return value or None


def get(target_user_id: int, action_kind: str) -> dict[str, Any] | None:
    conn = get_connection()
    row = conn.execute(
        """
        SELECT target_user_id, action_kind, account_user_id, text, status,
               prepared_at, telegram_message_id, sent_at, last_error, updated_at,
               message_kind, transition_json, source_inbox_id, allow_opt_out,
               recovery_attempts, recovery_next_at, recovery_last_error
          FROM dialog_outbox
         WHERE target_user_id=? AND action_kind=?
        """,
        (int(target_user_id), str(action_kind)),
    ).fetchone()
    return dict(row) if row else None


def get_prepared(target_user_id: int, action_kind: str) -> dict[str, Any] | None:
    row = get(target_user_id, action_kind)
    if row and str(row.get("status")) == STATUS_PREPARED:
        return row
    return None


def list_prepared_for_target(target_user_id: int) -> list[dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT target_user_id, action_kind, account_user_id, text, status,
               prepared_at, telegram_message_id, sent_at, last_error, updated_at,
               message_kind, transition_json, source_inbox_id, allow_opt_out,
               recovery_attempts, recovery_next_at, recovery_last_error
          FROM dialog_outbox
         WHERE target_user_id=? AND status=?
         ORDER BY prepared_at ASC
        """,
        (int(target_user_id), STATUS_PREPARED),
    ).fetchall()
    return [dict(r) for r in rows]


def list_stale_prepared(
    *, older_than_seconds: int = 90, limit: int = 100
) -> list[dict[str, Any]]:
    cutoff = (_now() - dt.timedelta(seconds=max(1, int(older_than_seconds)))).isoformat()
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT o.target_user_id, o.action_kind, o.account_user_id, o.text,
               o.prepared_at, o.message_kind, o.transition_json,
               o.source_inbox_id, o.allow_opt_out, o.recovery_attempts,
               o.recovery_next_at, o.recovery_last_error,
               a.username, a.access_hash, a.source_account_user_id
          FROM dialog_outbox o
          LEFT JOIN audience a ON a.user_id=o.target_user_id
         WHERE o.status=? AND o.prepared_at <= ?
           AND (o.recovery_next_at IS NULL OR o.recovery_next_at <= ?)
         ORDER BY COALESCE(o.recovery_next_at, o.prepared_at) ASC
         LIMIT ?
        """,
        (STATUS_PREPARED, cutoff, _now_iso(), int(limit)),
    ).fetchall()
    return [dict(r) for r in rows]


def defer_recovery(
    target_user_id: int,
    action_kind: str,
    error: str,
    *,
    delay_seconds: int,
) -> int:
    """Persist a bounded recovery backoff for one ambiguous dialog message."""
    now = _now()
    retry_at = (now + dt.timedelta(seconds=max(1, int(delay_seconds)))).isoformat()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE dialog_outbox
               SET recovery_attempts=COALESCE(recovery_attempts, 0) + 1,
                   recovery_next_at=?, recovery_last_error=?, updated_at=?
             WHERE target_user_id=? AND action_kind=? AND status=?
            """,
            (
                retry_at,
                str(error)[:500],
                now.isoformat(),
                int(target_user_id),
                str(action_kind),
                STATUS_PREPARED,
            ),
        )
        row = conn.execute(
            """
            SELECT COALESCE(recovery_attempts, 0) AS c
              FROM dialog_outbox
             WHERE target_user_id=? AND action_kind=?
            """,
            (int(target_user_id), str(action_kind)),
        ).fetchone()
        return int(row["c"] if row else 0)


def abandon_recovery(target_user_id: int, action_kind: str, error: str) -> None:
    """Stop an unrecoverable ambiguous action without risking a duplicate send."""
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE dialog_outbox
               SET status=?, last_error=?, updated_at=?, recovery_next_at=NULL,
                   recovery_last_error=?
             WHERE target_user_id=? AND action_kind=? AND status=?
            """,
            (
                STATUS_FAILED,
                str(error)[:500],
                now,
                str(error)[:500],
                int(target_user_id),
                str(action_kind),
                STATUS_PREPARED,
            ),
        )
        conn.execute(
            """
            UPDATE dialogs
               SET stage='closed', auto_link_at=NULL,
                   lifecycle_completed_at=COALESCE(lifecycle_completed_at, ?),
                   updated_at=?
             WHERE target_user_id=? AND stage!='closed'
            """,
            (now, now, int(target_user_id)),
        )
        conn.execute(
            """
            UPDATE contacts SET status='completed', updated_at=?
             WHERE target_user_id=?
            """,
            (now, int(target_user_id)),
        )


def clear_for_target(target_user_id: int, *, preserve_opt_out_allowed: bool = False) -> None:
    conn = get_connection()
    with db_lock(), conn:
        if preserve_opt_out_allowed:
            conn.execute(
                """
                DELETE FROM dialog_outbox
                 WHERE target_user_id=? AND COALESCE(allow_opt_out, 0)=0
                """,
                (int(target_user_id),),
            )
        else:
            conn.execute(
                "DELETE FROM dialog_outbox WHERE target_user_id=?",
                (int(target_user_id),),
            )
