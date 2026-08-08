from __future__ import annotations

import asyncio
import html
import logging
import re
import time
from dataclasses import dataclass
from typing import Any

from app.services.admin_only_mode import admin_only_user_allowed

log = logging.getLogger(__name__)

# Telegram's official service-notification account is not a user chat a bot can
# message. Keeping it out also prevents accidental preview attempts when old DB
# rows contain this id.
_INVALID_PRIVATE_RECIPIENT_IDS = {777000}

# Process-local readiness cache. Positive checks are cached longer; failures are
# retried after a short delay so /start can recover without redeploy.
_DM_READY_UNTIL: dict[int, float] = {}
_DM_BLOCKED_UNTIL: dict[int, float] = {}
_DM_READY_TTL_SEC = 600.0
_DM_BLOCKED_TTL_SEC = 30.0


@dataclass(frozen=True)
class DeliveryOutcome:
    delivered: bool
    code: str = ""
    error: str = ""
    attempts: int = 0


def mark_private_chat_ready(user_id: int) -> None:
    uid = int(user_id)
    _DM_BLOCKED_UNTIL.pop(uid, None)
    _DM_READY_UNTIL[uid] = time.monotonic() + _DM_READY_TTL_SEC


def mark_private_chat_unavailable(user_id: int) -> None:
    uid = int(user_id)
    _DM_READY_UNTIL.pop(uid, None)
    _DM_BLOCKED_UNTIL[uid] = time.monotonic() + _DM_BLOCKED_TTL_SEC


def _plain_text(text: str) -> str:
    # Telegram HTML cards only use simple tags; stripping them is a safe fallback.
    return re.sub(r"<[^>]+>", "", html.unescape(str(text)))


def _classify(exc: BaseException) -> tuple[str, bool, bool]:
    """Return (code, transient, html_parse_error)."""
    name = type(exc).__name__.lower()
    text = str(exc).lower()

    if "user_bot_to_bot_disabled" in text:
        return "recipient_is_bot", False, False
    if "forbidden" in name or any(
        token in text
        for token in (
            "bot can't initiate conversation",
            "bot was blocked",
            "user is deactivated",
            "chat not found",
        )
    ):
        return "dm_forbidden", False, False
    if any(
        token in text
        for token in (
            "can't parse entities",
            "unsupported start tag",
            "unsupported end tag",
            "can't find end tag",
        )
    ):
        return "html_parse_error", False, True
    if any(
        token in name or token in text
        for token in (
            "network",
            "timeout",
            "server",
            "tempor",
            "retryafter",
            "too many requests",
            "flood",
            "connection",
        )
    ):
        return "transient", True, False
    return "telegram_error", False, False


async def probe_private_chat(bot: Any, user_id: int) -> DeliveryOutcome:
    """Verify that Telegram allows this bot to reach the user's private chat.

    The probe is performed once per cache window with a harmless typing action.
    A group signal is not executed for a user whose private chat is unavailable,
    preventing a live position from being opened without any direct alert path.
    """
    uid = int(user_id)
    if uid <= 0 or uid in _INVALID_PRIVATE_RECIPIENT_IDS:
        return DeliveryOutcome(False, "invalid_recipient", "not a private user chat", 0)
    if not admin_only_user_allowed(uid):
        return DeliveryOutcome(False, "admin_only_suppressed", "ADMIN_ONLY_MODE", 0)

    now = time.monotonic()
    if _DM_READY_UNTIL.get(uid, 0.0) > now:
        return DeliveryOutcome(True, "cached_ready", "", 0)
    if _DM_BLOCKED_UNTIL.get(uid, 0.0) > now:
        return DeliveryOutcome(
            False, "cached_unavailable", "private chat unavailable", 0
        )

    try:
        await bot.send_chat_action(chat_id=uid, action="typing")
        mark_private_chat_ready(uid)
        return DeliveryOutcome(True, "probe_ok", "", 1)
    except Exception as exc:  # aiogram exception classes vary across versions
        code, transient, _ = _classify(exc)
        if code in {"dm_forbidden", "recipient_is_bot", "invalid_recipient"}:
            mark_private_chat_unavailable(uid)
        # A transient probe failure should not be treated as permanent, but for
        # this signal we still skip execution: opening without an alert channel
        # is less safe than waiting for the next signal.
        log.warning("private chat probe failed uid=%s code=%s error=%s", uid, code, exc)
        return DeliveryOutcome(
            False,
            code if not transient else "probe_transient",
            f"{type(exc).__name__}: {exc}",
            1,
        )


async def send_private_message(
    bot: Any,
    user_id: int,
    text: str,
    *,
    parse_mode: str | None = "HTML",
    attempts: int = 3,
    log_context: str = "notification",
    reply_markup: Any = None,
) -> DeliveryOutcome:
    """Send a private notification with bounded retries and exact classification."""
    uid = int(user_id)
    if uid <= 0 or uid in _INVALID_PRIVATE_RECIPIENT_IDS:
        return DeliveryOutcome(False, "invalid_recipient", "not a private user chat", 0)
    if not admin_only_user_allowed(uid):
        # Intentional policy suppression is terminal, not a delivery failure.
        # Report it as handled so durable/lifecycle notification producers do
        # not retry forever for legacy non-admin executions. No Telegram API
        # call is made.
        log.info("%s suppressed by ADMIN_ONLY_MODE uid=%s", log_context, uid)
        return DeliveryOutcome(True, "admin_only_suppressed", "", 0)

    max_attempts = max(1, min(3, int(attempts)))
    last_error = ""
    last_code = "telegram_error"

    for attempt in range(1, max_attempts + 1):
        try:
            await bot.send_message(
                uid, text, parse_mode=parse_mode, reply_markup=reply_markup
            )
            mark_private_chat_ready(uid)
            log.info("%s delivered uid=%s attempt=%s", log_context, uid, attempt)
            return DeliveryOutcome(True, "delivered", "", attempt)
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            code, transient, html_error = _classify(exc)
            last_code = code

            if code in {"dm_forbidden", "recipient_is_bot"}:
                mark_private_chat_unavailable(uid)
                unavailable_log = (
                    log.info
                    if str(log_context) == "preview notification"
                    else log.warning
                )
                unavailable_log(
                    "%s unavailable uid=%s code=%s error=%s",
                    log_context,
                    uid,
                    code,
                    last_error,
                )
                return DeliveryOutcome(False, code, last_error, attempt)

            if html_error and parse_mode:
                try:
                    await bot.send_message(
                        uid,
                        _plain_text(text),
                        parse_mode=None,
                        reply_markup=reply_markup,
                    )
                    mark_private_chat_ready(uid)
                    log.warning("%s delivered as plain text uid=%s", log_context, uid)
                    return DeliveryOutcome(True, "plain_text_fallback", "", attempt)
                except Exception as fallback_exc:
                    f_code, f_transient, _ = _classify(fallback_exc)
                    last_code = f_code
                    last_error = f"{type(fallback_exc).__name__}: {fallback_exc}"
                    if f_code in {"dm_forbidden", "recipient_is_bot"}:
                        mark_private_chat_unavailable(uid)
                        return DeliveryOutcome(False, f_code, last_error, attempt)
                    transient = f_transient

            log.warning(
                "%s attempt failed uid=%s attempt=%s/%s code=%s error=%s",
                log_context,
                uid,
                attempt,
                max_attempts,
                last_code,
                last_error,
            )
            if attempt >= max_attempts or not transient:
                break
            retry_after = getattr(exc, "retry_after", None)
            delay = float(retry_after or (0.5 * (2 ** (attempt - 1))))
            await asyncio.sleep(min(max(delay, 0.25), 5.0))

    log.error(
        "%s permanently failed uid=%s code=%s error=%s",
        log_context,
        uid,
        last_code,
        last_error,
    )
    return DeliveryOutcome(False, last_code, last_error, max_attempts)
