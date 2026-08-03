"""@SpamBot check flow after PeerFlood + scheduled auto-resume."""

from __future__ import annotations

import datetime as dt
import re
from typing import Any, Optional

from loguru import logger
from telethon import TelegramClient

from config import (
    ADMIN_ID_LIST,
    SPAMBOT_AUTO_RESUME,
    bot,
)
from db.schema import db_lock, get_connection
from services import accounts as accounts_svc
from services import monitor as monitor_svc
from services import pacing

STATUS_IDLE = "idle"
STATUS_CHECKING = "checking"
STATUS_LIMITED = "limited"
STATUS_FREE_PENDING = "free_pending_resume"
STATUS_ERROR = "error"

_SPAMBOT = "SpamBot"

def _account_label(account_user_id: int) -> str:
    """Human label: @username or name · id."""
    from services import accounts as accounts_svc

    acc = accounts_svc.get_account(int(account_user_id))
    if not acc:
        return f"id `{account_user_id}`"
    return accounts_svc.format_account_label(acc, include_id=True)


def _notify_peerflood(label: str, seconds: int) -> str:
    from services import runtime as runtime_svc
    pause = runtime_svc.format_duration(int(seconds))
    rng = runtime_svc.format_peer_flood_range()
    line1 = f"⚠️ Аккаунт **{label}** словил PeerFlood."
    line2 = f"Пауза **{pause}** (рандом {rng}). Спрашиваю @SpamBot."
    return line1 + "\n" + line2

def _notify_free_manual(label: str) -> str:
    return (
        f"✅ @SpamBot: **{label}** свободен.\n"
        f"Auto-resume выкл — сними паузу вручную."
    )


def _notify_limited(label: str, until: str, next_check: str) -> str:
    return (
        f"⏳ @SpamBot пишет, что **{label}** ещё ограничен\n"
        f"примерно до `{until}`.\n"
        f"Проверю снова после `{next_check}`."
    )


def _notify_unknown(label: str, reply: str) -> str:
    return (
        f"❔ Неясный ответ @SpamBot по **{label}**.\n"
        f"{reply}"
    )


def _notify_resumed(label: str, source: str) -> str:
    src = {"manual": "вручную", "auto": "auto-resume", "spambot_free": "auto-resume"}.get(
        source, source
    )
    return f"✅ **{label}** снова можно использовать в рассылке ({src})."


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def _parse_iso(value: str | None) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        raw = value.replace("Z", "+00:00")
        parsed = dt.datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed
    except Exception:
        return None


def get_state(account_user_id: int) -> dict[str, Any]:
    conn = get_connection()
    row = conn.execute(
        """
        SELECT account_user_id, status, last_reply, next_check_at,
               limited_until, updated_at
          FROM spambot_state
         WHERE account_user_id=?
        """,
        (int(account_user_id),),
    ).fetchone()
    if not row:
        return {
            "account_user_id": int(account_user_id),
            "status": STATUS_IDLE,
            "last_reply": None,
            "next_check_at": None,
            "limited_until": None,
            "updated_at": None,
        }
    return dict(row)


def _upsert_state(
    account_user_id: int,
    *,
    status: str,
    last_reply: str | None = None,
    next_check_at: str | None = None,
    limited_until: str | None = None,
) -> None:
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            INSERT INTO spambot_state (
                account_user_id, status, last_reply, next_check_at,
                limited_until, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_user_id) DO UPDATE SET
                status=excluded.status,
                last_reply=COALESCE(excluded.last_reply, spambot_state.last_reply),
                next_check_at=excluded.next_check_at,
                limited_until=excluded.limited_until,
                updated_at=excluded.updated_at
            """,
            (
                int(account_user_id),
                status,
                last_reply,
                next_check_at,
                limited_until,
                _now_iso(),
            ),
        )


async def on_peer_flood(account_user_id: int) -> None:
    """
    Called when PeerFlood is detected.
    Pauses account, sets min cooldown, schedules SpamBot check.
    """
    account_user_id = int(account_user_id)
    from services import runtime as runtime_svc
    seconds = int(runtime_svc.pick_peer_flood_seconds())
    min_until = _now() + dt.timedelta(seconds=seconds)
    pacing.set_paused(account_user_id, "PeerFlood", paused=True)
    # Also set cooldown so resume cannot happen before min window.
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE accounts
               SET cooldown_until=?,
                   pause_reason=?,
                   is_paused=1,
                   updated_at=?
             WHERE user_id=?
            """,
            (
                min_until.isoformat(),
                "PeerFlood",
                _now_iso(),
                account_user_id,
            ),
        )

    _upsert_state(
        account_user_id,
        status=STATUS_CHECKING,
        next_check_at=_now_iso(),  # check ASAP
        limited_until=None,
    )
    label = _account_label(account_user_id)
    await notify_admins(
        _notify_peerflood(label, seconds)
    )
    # Immediate check attempt
    await check_account(account_user_id, force=True)


def parse_spambot_reply(text: str) -> dict[str, Any]:
    """
    Heuristic parse of SpamBot reply.
    Returns: {result: free|limited|unknown, limited_until: iso|None, summary: str}
    """
    raw = (text or "").strip()
    lower = raw.lower()

    free_markers = [
        "no limits are currently applied",
        "free from spam",
        "your account is free",
        "нет ограничений",
        "ограничений нет",
        "аккаунт свободен",
        "не огранич",
        "good news",
    ]
    limited_markers = [
        "limited until",
        "restricted until",
        "your account was blocked",
        "your account is limited",
        "ограничен до",
        "заблокирован до",
        "ограничения действуют",
        "временно ограничен",
    ]

    if any(m in lower for m in free_markers) and not any(
        m in lower for m in limited_markers
    ):
        return {"result": "free", "limited_until": None, "summary": raw[:300]}

    if any(m in lower for m in limited_markers):
        until = _extract_until(raw)
        return {"result": "limited", "limited_until": until, "summary": raw[:300]}

    return {"result": "unknown", "limited_until": None, "summary": raw[:300]}


def _extract_until(text: str) -> Optional[str]:
    """Best-effort datetime extraction; falls back to +24h if limited but unparsed."""
    # ISO-like
    m = re.search(r"(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?)", text)
    if m:
        try:
            parsed = dt.datetime.fromisoformat(m.group(1).replace(" ", "T"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            return parsed.isoformat()
        except Exception:
            pass
    # dd.mm.yyyy hh:mm
    m = re.search(r"(\d{1,2})[./](\d{1,2})[./](\d{4})\s+(\d{1,2}):(\d{2})", text)
    if m:
        d, mo, y, h, mi = map(int, m.groups())
        try:
            parsed = dt.datetime(y, mo, d, h, mi, tzinfo=dt.timezone.utc)
            return parsed.isoformat()
        except Exception:
            pass
    return (_now() + dt.timedelta(hours=24)).isoformat()


async def check_account(account_user_id: int, *, force: bool = False) -> dict[str, Any]:
    """
    Write /start to @SpamBot, read latest reply, update state.
    Returns parse result + status.
    """
    account_user_id = int(account_user_id)
    state = get_state(account_user_id)
    if not force and state.get("next_check_at"):
        nxt = _parse_iso(state.get("next_check_at"))
        if nxt and nxt > _now():
            return {"result": "skipped_not_due", "status": state.get("status")}

    client = monitor_svc.get_client(account_user_id)
    if client is None or not client.is_connected():
        # Try temporary connect
        acc = accounts_svc.get_account(account_user_id)
        if not acc or not acc.get("session_string"):
            _upsert_state(
                account_user_id,
                status=STATUS_ERROR,
                last_reply="no session",
                next_check_at=(_now() + dt.timedelta(minutes=15)).isoformat(),
            )
            return {"result": "error", "status": STATUS_ERROR, "detail": "no_session"}

        from telethon.sessions import StringSession
        from config import API_ID, API_HASH

        client = TelegramClient(
            StringSession(acc["session_string"]), API_ID, API_HASH
        )
        temp = True
        try:
            await client.connect()
            if not await client.is_user_authorized():
                raise RuntimeError("not_authorized")
            return await _run_spambot_dialog(client, account_user_id)
        except Exception as exc:
            logger.exception("SpamBot temp client failed {}: {}", account_user_id, exc)
            _upsert_state(
                account_user_id,
                status=STATUS_ERROR,
                last_reply=str(type(exc).__name__),
                next_check_at=(_now() + dt.timedelta(minutes=15)).isoformat(),
            )
            return {"result": "error", "status": STATUS_ERROR, "detail": str(exc)}
        finally:
            if temp:
                try:
                    await client.disconnect()
                except Exception:
                    pass
    else:
        return await _run_spambot_dialog(client, account_user_id)


async def _run_spambot_dialog(
    client: TelegramClient, account_user_id: int
) -> dict[str, Any]:
    try:
        entity = await client.get_entity(_SPAMBOT)
        await client.send_message(entity, "/start")
        await _sleep(2.5)
        # Read last incoming from SpamBot
        reply_text = ""
        async for msg in client.iter_messages(entity, limit=5):
            if msg.out:
                continue
            reply_text = (msg.message or "").strip()
            if reply_text:
                break
        if not reply_text:
            _upsert_state(
                account_user_id,
                status=STATUS_ERROR,
                last_reply="(empty reply)",
                next_check_at=(_now() + dt.timedelta(minutes=15)).isoformat(),
            )
            return {"result": "unknown", "status": STATUS_ERROR, "summary": "empty"}

        parsed = parse_spambot_reply(reply_text)
        await _apply_parse(account_user_id, parsed, reply_text)
        return {**parsed, "status": get_state(account_user_id).get("status")}
    except Exception as exc:
        logger.exception("SpamBot dialog failed account={}: {}", account_user_id, exc)
        _upsert_state(
            account_user_id,
            status=STATUS_ERROR,
            last_reply=str(type(exc).__name__),
            next_check_at=(_now() + dt.timedelta(minutes=15)).isoformat(),
        )
        return {"result": "error", "status": STATUS_ERROR, "detail": str(exc)}


async def _apply_parse(
    account_user_id: int, parsed: dict[str, Any], reply_text: str
) -> None:
    result = parsed.get("result")
    if result == "free":
        # Resume only after min cooldown (cooldown_until on account).
        acc = accounts_svc.get_account(account_user_id)
        cooldown = _parse_iso((acc or {}).get("cooldown_until"))
        if cooldown and cooldown > _now():
            _upsert_state(
                account_user_id,
                status=STATUS_FREE_PENDING,
                last_reply=reply_text[:500],
                next_check_at=cooldown.isoformat(),
                limited_until=None,
            )
            logger.info(
                "SpamBot free for {} - wait min cooldown until {}",
                account_user_id,
                cooldown.isoformat(),
            )
            return

        if SPAMBOT_AUTO_RESUME:
            await resume_account(account_user_id, source="spambot_free")
        else:
            _upsert_state(
                account_user_id,
                status=STATUS_FREE_PENDING,
                last_reply=reply_text[:500],
                next_check_at=None,
            )
            label = _account_label(account_user_id)
            await notify_admins(_notify_free_manual(label))
        return

    if result == "limited":
        until = parsed.get("limited_until")
        next_check = until or (_now() + dt.timedelta(hours=6)).isoformat()
        # Push next_check a bit after reported limit
        nxt = _parse_iso(next_check)
        if nxt:
            nxt = nxt + dt.timedelta(minutes=1)
            next_check = nxt.isoformat()
        _upsert_state(
            account_user_id,
            status=STATUS_LIMITED,
            last_reply=reply_text[:500],
            next_check_at=next_check,
            limited_until=until,
        )
        label = _account_label(account_user_id)
        until_short = (until or "неизвестно")[:19]
        next_short = (next_check or "")[:19]
        await notify_admins(
            _notify_limited(label, until_short, next_short)
        )
        return

    # unknown
    _upsert_state(
        account_user_id,
        status=STATUS_ERROR,
        last_reply=reply_text[:500],
        next_check_at=(_now() + dt.timedelta(minutes=15)).isoformat(),
    )
    label = _account_label(account_user_id)
    await notify_admins(
        _notify_unknown(label, (reply_text or "")[:200] or "(пусто)")
    )


async def resume_account(account_user_id: int, *, source: str = "manual") -> None:
    """Clear PeerFlood pause and mark SpamBot idle."""
    account_user_id = int(account_user_id)
    pacing.set_paused(account_user_id, "", paused=False)
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE accounts
               SET is_paused=0,
                   pause_reason=NULL,
                   cooldown_until=NULL,
                   updated_at=?
             WHERE user_id=?
            """,
            (_now_iso(), account_user_id),
        )
    _upsert_state(
        account_user_id,
        status=STATUS_IDLE,
        last_reply=f"resumed:{source}",
        next_check_at=None,
        limited_until=None,
    )
    logger.info("Account {} resumed ({})", account_user_id, source)
    label = _account_label(account_user_id)
    await notify_admins(_notify_resumed(label, source))
    try:
        await monitor_svc.refresh_monitor()
    except Exception as exc:
        logger.warning("monitor refresh after resume: {}", exc)


async def process_due_checks() -> int:
    """Run due SpamBot checks / pending resumes. Returns actions count."""
    conn = get_connection()
    now = _now_iso()
    rows = conn.execute(
        """
        SELECT account_user_id, status, next_check_at
          FROM spambot_state
         WHERE next_check_at IS NOT NULL
           AND next_check_at <= ?
        """,
        (now,),
    ).fetchall()
    actions = 0
    for row in rows:
        uid = int(row["account_user_id"])
        status = str(row["status"] or "")
        if status == STATUS_FREE_PENDING and SPAMBOT_AUTO_RESUME:
            acc = accounts_svc.get_account(uid)
            cooldown = _parse_iso((acc or {}).get("cooldown_until"))
            if cooldown and cooldown > _now():
                continue
            await resume_account(uid, source="spambot_auto")
            actions += 1
            continue
        await check_account(uid, force=True)
        actions += 1
    return actions


async def notify_admins(text: str) -> None:
    if not ADMIN_ID_LIST:
        return
    for admin_id in ADMIN_ID_LIST:
        try:
            await bot.send_message(admin_id, text, link_preview=False)
        except Exception as exc:
            logger.debug("notify admin {} failed: {}", admin_id, exc)


async def _sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)
