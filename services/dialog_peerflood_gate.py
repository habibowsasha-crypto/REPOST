"""Optional account-wide gate for dialog sends after Telegram PeerFlood."""

from __future__ import annotations

import datetime as dt
from typing import Any

import config as app_config
from db.schema import db_lock, get_connection

_GATE_SECONDS = 5 * 60
_PROBE_LEASE_SECONDS = 45


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def is_enabled() -> bool:
    """Return whether the account gate and the base dialog guard are enabled."""
    return bool(
        getattr(app_config, "DIALOG_PEERFLOOD_GUARD_ENABLED", False)
        and getattr(app_config, "DIALOG_PEERFLOOD_ACCOUNT_GATE_ENABLED", False)
    )


def activate(
    account_user_id: int,
    *,
    now: dt.datetime | None = None,
    delay_seconds: int = _GATE_SECONDS,
) -> str | None:
    """Close dialog sending for one account for at least one safe window."""
    if not is_enabled():
        return None
    current = now or _now()
    blocked_until = current + dt.timedelta(
        seconds=max(_GATE_SECONDS, int(delay_seconds))
    )
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            INSERT INTO dialog_peerflood_account_gates(
                account_user_id, blocked_until, probe_claim_until, updated_at
            ) VALUES (?, ?, NULL, ?)
            ON CONFLICT(account_user_id) DO UPDATE SET
                blocked_until=excluded.blocked_until,
                probe_claim_until=NULL,
                updated_at=excluded.updated_at
            """,
            (int(account_user_id), blocked_until.isoformat(), current.isoformat()),
        )
    return blocked_until.isoformat()


def wait_retry_at(
    account_user_id: int,
    *,
    now: dt.datetime | None = None,
) -> str | None:
    """Return the active wait deadline without claiming the probe."""
    if not is_enabled():
        return None
    current = now or _now()
    conn = get_connection()
    row = conn.execute(
        """
        SELECT blocked_until, probe_claim_until
          FROM dialog_peerflood_account_gates
         WHERE account_user_id=?
        """,
        (int(account_user_id),),
    ).fetchone()
    if not row:
        return None
    blocked_until = _parse_iso(row["blocked_until"])
    probe_claim_until = _parse_iso(row["probe_claim_until"])
    if blocked_until is not None and blocked_until > current:
        return blocked_until.isoformat()
    if probe_claim_until is not None and probe_claim_until > current:
        return probe_claim_until.isoformat()
    return None


def before_send(
    account_user_id: int,
    *,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    """Return normal, wait or probe before one established-dialog send.

    The first caller after the five-minute window atomically claims one short
    probe lease. Other dialog sends for the same account remain deferred until
    that probe finishes or its crash-safe lease expires.
    """
    if not is_enabled():
        return {"mode": "normal", "retry_at": None}

    current = now or _now()
    now_iso = current.isoformat()
    conn = get_connection()
    with db_lock(), conn:
        row = conn.execute(
            """
            SELECT blocked_until, probe_claim_until
              FROM dialog_peerflood_account_gates
             WHERE account_user_id=?
            """,
            (int(account_user_id),),
        ).fetchone()
        if not row:
            return {"mode": "normal", "retry_at": None}

        blocked_until = _parse_iso(row["blocked_until"])
        probe_claim_until = _parse_iso(row["probe_claim_until"])
        if blocked_until is None:
            conn.execute(
                "DELETE FROM dialog_peerflood_account_gates WHERE account_user_id=?",
                (int(account_user_id),),
            )
            return {"mode": "normal", "retry_at": None}
        if blocked_until > current:
            return {"mode": "wait", "retry_at": blocked_until.isoformat()}
        if probe_claim_until is not None and probe_claim_until > current:
            return {"mode": "wait", "retry_at": probe_claim_until.isoformat()}

        lease_until = current + dt.timedelta(seconds=_PROBE_LEASE_SECONDS)
        cur = conn.execute(
            """
            UPDATE dialog_peerflood_account_gates
               SET probe_claim_until=?, updated_at=?
             WHERE account_user_id=?
               AND blocked_until<=?
               AND (probe_claim_until IS NULL OR probe_claim_until<=?)
            """,
            (
                lease_until.isoformat(),
                now_iso,
                int(account_user_id),
                now_iso,
                now_iso,
            ),
        )
        if int(cur.rowcount or 0) == 1:
            return {
                "mode": "probe",
                "retry_at": lease_until.isoformat(),
            }

        latest = conn.execute(
            """
            SELECT blocked_until, probe_claim_until
              FROM dialog_peerflood_account_gates
             WHERE account_user_id=?
            """,
            (int(account_user_id),),
        ).fetchone()
        if not latest:
            return {"mode": "normal", "retry_at": None}
        retry = _parse_iso(latest["probe_claim_until"]) or _parse_iso(
            latest["blocked_until"]
        )
        return {
            "mode": "wait",
            "retry_at": (retry or (current + dt.timedelta(seconds=1))).isoformat(),
        }


def mark_probe_success(account_user_id: int) -> bool:
    """Open the account gate after a non-PeerFlood probe succeeds."""
    if not is_enabled():
        return False
    conn = get_connection()
    with db_lock(), conn:
        cur = conn.execute(
            "DELETE FROM dialog_peerflood_account_gates WHERE account_user_id=?",
            (int(account_user_id),),
        )
    return bool(int(cur.rowcount or 0))


def release_probe(account_user_id: int) -> bool:
    """Release a probe lease without opening or extending the gate."""
    if not is_enabled():
        return False
    now = _now().isoformat()
    conn = get_connection()
    with db_lock(), conn:
        cur = conn.execute(
            """
            UPDATE dialog_peerflood_account_gates
               SET probe_claim_until=NULL, updated_at=?
             WHERE account_user_id=?
            """,
            (now, int(account_user_id)),
        )
    return bool(int(cur.rowcount or 0))


def get_state(account_user_id: int) -> dict[str, Any] | None:
    """Return one persisted gate row for diagnostics and tests."""
    conn = get_connection()
    row = conn.execute(
        """
        SELECT account_user_id, blocked_until, probe_claim_until, updated_at
          FROM dialog_peerflood_account_gates
         WHERE account_user_id=?
        """,
        (int(account_user_id),),
    ).fetchone()
    return dict(row) if row else None


def _parse_iso(value: Any) -> dt.datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)
