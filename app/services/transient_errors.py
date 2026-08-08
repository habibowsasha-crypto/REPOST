from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def is_transient_exchange_error(exc: BaseException) -> bool:
    """Return True for exchange errors that are usually temporary and should be retried.

    BingX/network temporary errors are retried.
    We also treat HTTP 429/5xx and common timeout/server-unavailable texts as transient.
    """
    text = f"{type(exc).__name__}: {exc}".lower()
    transient_markers = (
        "mexcnetworkambiguouserror",
        "networkambiguouserror",
        "system error. please try again later",
        "http 429",
        "http 500",
        "http 502",
        "http 503",
        "http 504",
        "too many requests",
        "rate limit",
        "temporarily unavailable",
        "service unavailable",
        "timeout",
        "timed out",
    )
    return any(marker in text for marker in transient_markers)


def transient_error_message(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {str(exc)[:500]}"


def record_transient_error(
    payload: dict[str, Any], area: str, exc: BaseException
) -> int:
    """Record a retryable error in exchange_order_ids_json payload and return attempts count."""
    if not isinstance(payload, dict):
        return 1
    bucket = payload.get("transient_errors")
    if not isinstance(bucket, dict):
        bucket = {}
        payload["transient_errors"] = bucket
    item = bucket.get(area)
    if not isinstance(item, dict):
        item = {"count": 0}
    item["count"] = int(item.get("count") or 0) + 1
    item["last_error"] = transient_error_message(exc)
    item["last_at"] = datetime.now(timezone.utc).isoformat()
    bucket[area] = item
    return int(item["count"])


def should_notify_transient(attempts: int, *, every: int = 3) -> bool:
    """Notify on first retryable error and then every N attempts to avoid Telegram spam."""
    try:
        every = max(1, int(every))
    except Exception:
        every = 3
    return attempts == 1 or attempts % every == 0


def max_transient_retries(value: int | None = None) -> int:
    """Normalize max retry value. 0 disables the cap."""
    try:
        n = int(value or 0)
    except Exception:
        n = 30
    return max(0, n)


def transient_retry_exhausted(attempts: int, *, max_retries: int | None = None) -> bool:
    """Return True when retryable exchange errors should stop auto-retrying."""
    cap = max_transient_retries(max_retries)
    return cap > 0 and int(attempts or 0) >= cap
