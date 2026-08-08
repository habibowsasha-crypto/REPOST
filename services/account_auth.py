"""Persistent Telegram account authorization state and admin alerts."""

from __future__ import annotations

import datetime as dt
from typing import Any

from loguru import logger
from telethon import Button

from config import ADMIN_ID_LIST, bot
from db.schema import db_lock, get_connection
from services import accounts as accounts_svc

AUTH_UNKNOWN = "unknown"
AUTH_AUTHORIZED = "authorized"
AUTH_REAUTH_REQUIRED = "reauth_required"

_AUTH_LOSS_CLASS_NAMES = {
    "UnauthorizedError",
    "AuthKeyUnregisteredError",
    "SessionRevokedError",
    "AuthKeyDuplicatedError",
    "UserDeactivatedError",
    "UserDeactivatedBanError",
}

_AUTH_LOSS_TEXT_MARKERS = (
    "session not authorized",
    "session_not_authorized",
    "account_session_not_authorized",
    "auth key unregistered",
    "auth key duplicated",
    "session revoked",
    "authorization key",
    "user deactivated",
)


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def is_auth_loss_error(exc: BaseException | None) -> bool:
    """Return True only for durable Telegram authorization loss signals."""
    if exc is None:
        return False
    if type(exc).__name__ in _AUTH_LOSS_CLASS_NAMES:
        return True
    text = f"{type(exc).__name__}: {exc}".lower()
    return any(marker in text for marker in _AUTH_LOSS_TEXT_MARKERS)


def mark_authorized(account_user_id: int) -> bool:
    """Mark a session healthy and re-arm the next loss notification."""
    uid = int(account_user_id)
    conn = get_connection()
    now = _now_iso()
    with db_lock(), conn:
        cur = conn.execute(
            """
            UPDATE accounts
               SET auth_status=?, auth_error=NULL, auth_lost_at=NULL,
                   auth_notified_at=NULL, updated_at=?
             WHERE user_id=?
               AND (
                    COALESCE(auth_status, 'unknown') != ?
                    OR auth_error IS NOT NULL
                    OR auth_lost_at IS NOT NULL
                    OR auth_notified_at IS NOT NULL
               )
            """,
            (AUTH_AUTHORIZED, now, uid, AUTH_AUTHORIZED),
        )
        return int(cur.rowcount or 0) == 1


def mark_reauth_required(account_user_id: int, reason: str) -> dict[str, Any]:
    """Persist a durable auth-loss state without deleting the account or dialogs."""
    uid = int(account_user_id)
    now = _now_iso()
    detail = str(reason or "session_not_authorized")[:500]
    conn = get_connection()
    with db_lock(), conn:
        row = conn.execute(
            "SELECT auth_status, auth_notified_at FROM accounts WHERE user_id=?",
            (uid,),
        ).fetchone()
        if not row:
            return {"found": False, "transitioned": False, "notify": False}
        previous = str(row["auth_status"] or AUTH_UNKNOWN)
        transitioned = previous != AUTH_REAUTH_REQUIRED
        conn.execute(
            """
            UPDATE accounts
               SET auth_status=?, auth_error=?,
                   auth_lost_at=CASE WHEN ? THEN ? ELSE COALESCE(auth_lost_at, ?) END,
                   auth_notified_at=CASE WHEN ? THEN NULL ELSE auth_notified_at END,
                   updated_at=?
             WHERE user_id=?
            """,
            (
                AUTH_REAUTH_REQUIRED,
                detail,
                1 if transitioned else 0,
                now,
                now,
                1 if transitioned else 0,
                now,
                uid,
            ),
        )
        notify = transitioned or not bool(row["auth_notified_at"])
    return {"found": True, "transitioned": transitioned, "notify": notify}


def mark_notification_sent(account_user_id: int) -> None:
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE accounts
               SET auth_notified_at=?, updated_at=?
             WHERE user_id=? AND auth_status=?
            """,
            (_now_iso(), _now_iso(), int(account_user_id), AUTH_REAUTH_REQUIRED),
        )


def notification_text(account_user_id: int) -> str:
    acc = accounts_svc.get_account(int(account_user_id)) or {
        "user_id": int(account_user_id)
    }
    label = accounts_svc.format_account_label(acc, include_id=False)
    active_dialogs = 0
    try:
        from services import dialog_store as dialog_store_svc

        active_dialogs = dialog_store_svc.count_open_for_account(int(account_user_id))
    except Exception as exc:
        logger.debug("Account auth notification dialog count failed: {}", exc)
        active_dialogs = 0
    return "\n".join(
        [
            "🚨 **АККАУНТ ПОТЕРЯЛ ВХОД**",
            "━━━━━━━━━━━━━━━━━━",
            "",
            f"Аккаунт: **{label}**",
            f"ID: `{int(account_user_id)}`",
            "",
            "Telegram-сессия больше не действует.",
            "Новые First DM и ответы с этого аккаунта остановлены.",
            f"Активных диалогов: **{active_dialogs}**",
            "",
            "Выберите действие:",
        ]
    )


def notification_buttons(account_user_id: int):
    uid = int(account_user_id)
    return [
        [Button.inline("🔑 ПЕРЕЗАЙТИ", f"acc_relogin_{uid}".encode())],
        [Button.inline("🗑 УДАЛИТЬ АККАУНТ", f"acc_del_ask_{uid}".encode())],
        [Button.inline("👤 ОТКРЫТЬ АККАУНТ", f"acc_card_{uid}".encode())],
    ]


async def notify_reauth_required(account_user_id: int, *, force: bool = False) -> bool:
    """Send one persistent alert per auth-loss incident."""
    uid = int(account_user_id)
    acc = accounts_svc.get_account(uid)
    if not acc or not accounts_svc.is_reauth_required(acc):
        return False
    if acc.get("auth_notified_at") and not force:
        return False
    if not ADMIN_ID_LIST:
        return False

    sent = 0
    text = notification_text(uid)
    buttons = notification_buttons(uid)
    for admin_id in ADMIN_ID_LIST:
        try:
            await bot.send_message(
                int(admin_id),
                text,
                buttons=buttons,
                link_preview=False,
            )
            sent += 1
        except Exception as exc:
            logger.warning(
                "Account auth alert failed admin={} account={} error={}",
                admin_id,
                uid,
                type(exc).__name__,
            )
    if sent:
        mark_notification_sent(uid)
        logger.warning(
            "Account authorization lost account={} admins_notified={}", uid, sent
        )
        return True
    return False


async def notify_pending_reauth_required() -> int:
    """Retry alerts that were persisted but never delivered to an admin."""
    sent = 0
    for acc in accounts_svc.list_reauth_required():
        if acc.get("auth_notified_at"):
            continue
        if await notify_reauth_required(int(acc["user_id"])):
            sent += 1
    return sent


async def register_auth_loss(
    account_user_id: int,
    reason: str | BaseException,
    *,
    notify: bool = True,
) -> dict[str, Any]:
    detail = (
        f"{type(reason).__name__}: {reason}"
        if isinstance(reason, BaseException)
        else str(reason)
    )
    state = mark_reauth_required(int(account_user_id), detail)
    if notify and state.get("notify"):
        await notify_reauth_required(int(account_user_id))
    return state
