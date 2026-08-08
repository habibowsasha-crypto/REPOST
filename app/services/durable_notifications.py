from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

from app.database import db
from app.services.notification_style import ensure_visual_card, strip_footer

log = logging.getLogger(__name__)
NotifyFn = Callable[..., Awaitable[object] | object]
_LOCKS: dict[str, asyncio.Lock] = {}
_LOCKS_GUARD = asyncio.Lock()
_NOTIFICATION_EVENT_KEY: ContextVar[str] = ContextVar(
    "antilud_notification_event_key", default=""
)
# A Telegram request must finish well before its durable claim can be reclaimed by
# another Railway replica.  The outbox intentionally claims one row at a time;
# this lease therefore belongs to the row currently being delivered, not to a
# serial batch whose tail could expire before it is attempted.
_NOTIFICATION_CLAIM_LEASE_SEC = 90.0
_NOTIFICATION_SEND_TIMEOUT_SEC = 45.0


def set_notification_event_key(value: str | None) -> None:
    """Set stable per-execution identity for notifications in this async task."""
    _NOTIFICATION_EVENT_KEY.set(str(value or "").strip())


def _delivery_confirmed(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    delivered = getattr(value, "delivered", None)
    if delivered is not None:
        return bool(delivered)
    if isinstance(value, dict) and "delivered" in value:
        return bool(value.get("delivered"))
    return False


def _delivery_error(value: Any) -> str:
    code = getattr(value, "code", None)
    error = getattr(value, "error", None)
    if isinstance(value, dict):
        code = value.get("code", code)
        error = value.get("error", error)
    text = ": ".join(
        part for part in (str(code or "").strip(), str(error or "").strip()) if part
    )
    return text[:1000]


def _retry_delay(attempts: int) -> float:
    normalized = max(1, int(attempts or 1))
    return float(min(900, 5 * (2 ** min(normalized - 1, 8))))


def _parse_dt(value: Any) -> datetime | None:
    try:
        text = str(value or "").strip().replace("Z", "+00:00")
        if not text:
            return None
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return None


def notification_key(
    user_id: int,
    text: str,
    source: str,
    *,
    event_key: str | None = None,
    reply_markup_spec: dict[str, Any] | None = None,
) -> str:
    # v1.6.18: hash content with the volatile per-minute footer stripped, so
    # the SAME unresolved condition rebuilt into a fresh card() on a later
    # monitor pass produces the SAME dedup key instead of a new one every
    # time the wall-clock minute changes. The displayed/stored text passed by
    # the caller (with the visible footer) is unaffected -- only the key
    # derivation changes.
    content = strip_footer(str(text))
    stable_event = str(
        event_key if event_key is not None else _NOTIFICATION_EVENT_KEY.get()
    ).strip()
    raw_text = f"{int(user_id)}\n{str(source or 'monitor')}\n{stable_event}\n{content}"
    # Preserve the exact pre-v1.6.37 key for notifications without buttons, so
    # deployment cannot resend previously delivered durable events.
    if reply_markup_spec:
        markup_text = json.dumps(
            reply_markup_spec,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        raw_text = f"{raw_text}\n{markup_text}"
    raw = raw_text.encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _notify_accepts_markup(notify: NotifyFn) -> bool:
    try:
        signature = inspect.signature(notify)
    except (TypeError, ValueError):
        return False
    positional = 0
    for parameter in signature.parameters.values():
        if parameter.kind == inspect.Parameter.VAR_POSITIONAL:
            return True
        if parameter.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            positional += 1
    return positional >= 3


async def _lock_for(key: str) -> asyncio.Lock:
    async with _LOCKS_GUARD:
        lock = _LOCKS.get(key)
        if lock is None:
            lock = asyncio.Lock()
            _LOCKS[key] = lock
        if len(_LOCKS) > 5000:
            for old_key, old_lock in list(_LOCKS.items()):
                if old_key != key and not old_lock.locked():
                    _LOCKS.pop(old_key, None)
                    if len(_LOCKS) <= 4000:
                        break
        return lock


async def _attempt_claimed_delivery(
    notify: NotifyFn | None,
    row: dict[str, Any],
    *,
    reply_markup_spec: dict[str, Any] | None = None,
) -> bool:
    """Deliver one DB-claimed row and CAS-complete its distributed lease."""

    delivered = False
    last_error = "notification callback unavailable"
    if notify is not None:
        try:
            if reply_markup_spec and _notify_accepts_markup(notify):
                result = notify(
                    int(row.get("user_id") or 0),
                    str(row.get("message_text") or ""),
                    reply_markup_spec,
                )
            else:
                result = notify(
                    int(row.get("user_id") or 0),
                    str(row.get("message_text") or ""),
                )
            if hasattr(result, "__await__"):
                result = await asyncio.wait_for(  # type: ignore[misc]
                    result,
                    timeout=_NOTIFICATION_SEND_TIMEOUT_SEC,
                )
            delivered = _delivery_confirmed(result)
            last_error = _delivery_error(result) or (
                ""
                if delivered
                else f"unconfirmed result type={type(result).__name__}"
            )
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"[:1000]
            log.exception(
                "critical notification send failed source=%s user_id=%s",
                str(row.get("source") or "monitor"),
                int(row.get("user_id") or 0),
            )

    attempts = int(row.get("attempts") or 0) + 1
    next_attempt = None
    if not delivered:
        next_attempt = (
            datetime.now(timezone.utc)
            + timedelta(seconds=_retry_delay(attempts))
        ).isoformat()
    completed = await db.complete_durable_notification_claim(
        int(row.get("id") or 0),
        claim_token=str(row.get("claim_token") or ""),
        claim_generation=int(row.get("claim_generation") or 0),
        delivered=delivered,
        attempts=attempts,
        next_attempt_at=next_attempt,
        last_error=last_error,
    )
    if not completed:
        # Delivery may already have happened, but this worker no longer owns the
        # row.  Do not overwrite a newer replica's state.  The outbox remains
        # at-least-once after a crash between Telegram acceptance and CAS commit.
        log.error(
            "DURABLE_NOTIFICATION_LEASE_CONFLICT id=%s key=%s delivered=%s",
            int(row.get("id") or 0),
            str(row.get("dedup_key") or ""),
            int(delivered),
        )
        return False
    if not delivered:
        log.warning(
            "critical notification queued source=%s user_id=%s attempts=%s next=%s error=%s",
            str(row.get("source") or "monitor"),
            int(row.get("user_id") or 0),
            attempts,
            next_attempt,
            last_error,
        )
    return delivered


async def send_or_enqueue(
    notify: NotifyFn | None,
    user_id: int,
    text: str,
    *,
    source: str,
    event_key: str | None = None,
    dedup_key_override: str | None = None,
    reply_markup_spec: dict[str, Any] | None = None,
) -> bool:
    """Persist, atomically claim and deliver one critical notification.

    The database claim is the cross-replica authority.  The local lock only
    avoids redundant work inside one process; it is not relied upon for safety.
    """

    visual = ensure_visual_card(text)
    stable_event = str(
        event_key if event_key is not None else _NOTIFICATION_EVENT_KEY.get()
    ).strip()
    key = str(dedup_key_override or "").strip() or notification_key(
        user_id,
        visual,
        source,
        event_key=stable_event,
        reply_markup_spec=reply_markup_spec,
    )
    lock = await _lock_for(key)
    async with lock:
        existing = await db.get_durable_notification(key)
        now = datetime.now(timezone.utc)
        if existing and str(existing.get("status") or "") == "delivered":
            if stable_event:
                return True
            delivered_at = _parse_dt(existing.get("delivered_at"))
            if delivered_at is not None and (now - delivered_at).total_seconds() < 600:
                return True
            existing = None
        if existing:
            status = str(existing.get("status") or "pending")
            claim_expiry = _parse_dt(existing.get("claim_expires_at"))
            if status == "processing" and (
                claim_expiry is None or now < claim_expiry
            ):
                return False
            next_attempt = _parse_dt(existing.get("next_attempt_at"))
            if status == "pending" and next_attempt is not None and now < next_attempt:
                return False
            attempts = int(existing.get("attempts") or 0)
        else:
            attempts = 0

        await db.upsert_durable_notification(
            dedup_key=key,
            user_id=int(user_id),
            message_text=visual,
            source=str(source or "monitor"),
            attempts=attempts,
            next_attempt_at=now.isoformat(),
            last_error="",
            delivered=False,
            reply_markup_json=(
                json.dumps(
                    reply_markup_spec,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                if reply_markup_spec
                else None
            ),
        )
        claimed = await db.claim_durable_notification_by_key(
            key, lease_seconds=_NOTIFICATION_CLAIM_LEASE_SEC
        )
        if not claimed:
            return False
        return await _attempt_claimed_delivery(
            notify, claimed, reply_markup_spec=reply_markup_spec
        )


async def process_due_notifications_once(
    notify: NotifyFn | None,
    *,
    limit: int = 100,
) -> int:
    if notify is None:
        return 0
    # Do not lease a large serial batch.  A slow Telegram response on row one
    # used to let the common lease of rows 2..N expire before this worker reached
    # them, allowing another replica to send the same alert.  Claiming immediately
    # before each attempt keeps lease age bounded and preserves cross-replica CAS.
    remaining = max(1, min(int(limit or 100), 1000))
    processed = 0
    for _ in range(remaining):
        rows = await db.claim_due_durable_notifications(
            limit=1,
            lease_seconds=_NOTIFICATION_CLAIM_LEASE_SEC,
        )
        if not rows:
            break
        row = rows[0]
        markup_spec = None
        try:
            raw_markup = str(row.get("reply_markup_json") or "").strip()
            parsed_markup = json.loads(raw_markup) if raw_markup else None
            if isinstance(parsed_markup, dict):
                markup_spec = parsed_markup
        except (TypeError, ValueError, json.JSONDecodeError):
            markup_spec = None
        if await _attempt_claimed_delivery(
            notify, row, reply_markup_spec=markup_spec
        ):
            processed += 1
    return processed


async def durable_notification_worker_loop(notify: NotifyFn | None) -> None:
    """Retry claimed notifications and prune old delivered rows periodically."""

    next_prune_at = 0.0
    while True:
        try:
            await process_due_notifications_once(notify, limit=100)
            now_mono = asyncio.get_running_loop().time()
            if now_mono >= next_prune_at:
                pruned = await db.prune_durable_notifications(
                    delivered_retention_days=30, limit=1000
                )
                if pruned:
                    log.info("DURABLE_NOTIFICATION_RETENTION_PRUNED rows=%s", pruned)
                next_prune_at = now_mono + 6 * 60 * 60
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("durable notification worker iteration failed")
        await asyncio.sleep(2.0)
