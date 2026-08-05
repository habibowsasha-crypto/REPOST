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
from services import account_auth
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



_MSK = dt.timezone(dt.timedelta(hours=3))


def _format_admin_time(iso_or_dt) -> str:
    """Show time in Moscow (UTC+3) for admin notifications."""
    if iso_or_dt is None:
        return "неизвестно"
    if isinstance(iso_or_dt, dt.datetime):
        d = iso_or_dt
    else:
        d = _parse_iso(str(iso_or_dt))
        if not d:
            return str(iso_or_dt)[:19]
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    local = d.astimezone(_MSK)
    return local.strftime("%d.%m.%Y %H:%M МСК")


def _notify_peerflood(
    label: str,
    seconds: int,
    *,
    streak: int = 1,
    burst_triggered: bool = False,
    extra_seconds: int = 0,
) -> str:
    from services import runtime as runtime_svc

    pause = runtime_svc.format_duration(int(seconds))
    if burst_triggered:
        burst = (
            "\n🔥 Серия: **5 PeerFlood за 10 минут**"
            f"\n➕ Дополнительная пауза: **{runtime_svc.format_duration(int(extra_seconds))}**"
        )
    else:
        burst = f"\n🔁 Серия за 10 минут: **{streak}/5**"
    return (
        "🚨 **ОБНАРУЖЕН PEERFLOOD**\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 Аккаунт: **{label}**\n"
        "⏸ First DM: **остановлены**\n"
        f"⏳ Общая пауза: **{pause}**"
        f"{burst}\n"
        "🤖 SpamBot: **проверка запущена**"
    )


def _notify_free_manual(label: str) -> str:
    return (
        "🟢 **SPAMBOT: ОГРАНИЧЕНИЙ НЕТ**\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 Аккаунт: **{label}**\n"
        "✅ Telegram не подтвердил ограничение.\n"
        "⏸ Автовозобновление выключено - сними паузу вручную."
    )


def _notify_limited(label: str, until: str, next_check: str) -> str:
    until_h = _format_admin_time(until)
    next_h = _format_admin_time(next_check)
    return (
        "🔴 **SPAMBOT: АККАУНТ ОГРАНИЧЕН**\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 Аккаунт: **{label}**\n"
        f"⏳ Ограничение примерно до: **{until_h}**\n"
        f"🔄 Следующая проверка: **{next_h}**\n"
        "📨 First DM: **остановлены**"
    )


def _notify_unknown(label: str, reply: str) -> str:
    snippet = (reply or "").replace("\n", " ").strip()
    if len(snippet) > 240:
        snippet = snippet[:237] + "…"
    return (
        "⚠️ **НЕЯСНЫЙ ОТВЕТ SPAMBOT**\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 Аккаунт: **{label}**\n"
        f"💬 Ответ: {snippet}\n"
        "📌 Аккаунт останется на паузе до следующей проверки."
    )


def _notify_resumed(label: str, source: str) -> str:
    src = {
        "manual": "вручную",
        "auto": "автоматически",
        "spambot_free": "после проверки SpamBot",
        "spambot_auto": "после автоматической проверки SpamBot",
    }.get(source, source)
    return (
        "▶️ **FIRST DM АККАУНТА ВОЗОБНОВЛЕНЫ**\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 Аккаунт: **{label}**\n"
        f"🔄 Возобновление: **{src}**\n"
        "✅ Аккаунт снова участвует в отправке First DM."
    )


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
    except (TypeError, ValueError):
        return None


def _timezone_from_text(text: str) -> dt.tzinfo:
    """Extract UTC/GMT offset or Moscow time marker from SpamBot text."""
    lower = (text or "").lower()
    offset = re.search(r"(?:utc|gmt)\s*([+-])\s*(\d{1,2})(?::?(\d{2}))?", lower)
    if offset:
        sign = 1 if offset.group(1) == "+" else -1
        hours = int(offset.group(2))
        minutes = int(offset.group(3) or 0)
        if hours <= 23 and minutes <= 59:
            return dt.timezone(sign * dt.timedelta(hours=hours, minutes=minutes))
    if re.search(r"\b(?:мск|msk|moscow)\b", lower):
        return _MSK
    return dt.timezone.utc


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
    PeerFlood from Telegram on DM send.

    Important: @SpamBot "free" != no PeerFlood. PeerFlood is a cold-DM rate limit.
    Every real exception is counted. Repeated events during an active ordinary
    cooldown are debounced, except the fifth event extends that cooldown by the
    separately configured extra time.
    """
    account_user_id = int(account_user_id)
    from services import runtime as runtime_svc

    repair = accounts_svc.clamp_peerflood_cooldown(account_user_id)
    if repair.get("changed"):
        logger.warning(
            "PeerFlood cooldown clamped account={} old_until={} safe_until={} cleared={}",
            account_user_id,
            repair.get("old_until"),
            repair.get("safe_until"),
            repair.get("cleared"),
        )
    acc = accounts_svc.get_account(account_user_id)
    st = get_state(account_user_id) or {}
    st_status = str(st.get("status") or "")
    active_local_cooldown = False
    if acc and acc.get("is_paused") and str(acc.get("pause_reason") or "").lower() == "peerflood":
        current_cd = _parse_iso(acc.get("cooldown_until"))
        active_local_cooldown = bool(current_cd and current_cd > _now())
    if not active_local_cooldown:
        accounts_svc.clear_peerflood_burst_marker(account_user_id)

    # Count every real PeerFlood exception, including concurrent dialog sends
    # received while the account is already inside its ordinary cooldown.
    info = accounts_svc.register_peerflood_hit(account_user_id)
    streak = int(info.get("streak") or 1)
    burst_triggered = bool(info.get("burst_triggered"))
    burst_suppressed = bool(info.get("burst_suppressed"))
    extra_seconds = int(info.get("extra_pause_seconds") or 0)

    if burst_suppressed:
        logger.warning(
            "PeerFlood 5-in-10 extra suppressed account={} reason=already_applied_in_active_pause",
            account_user_id,
        )

    if acc and acc.get("is_paused"):
        cd = _parse_iso(acc.get("cooldown_until"))
        if cd and cd > _now():
            if burst_triggered and extra_seconds > 0:
                _, ordinary_hi = runtime_svc.get_peer_flood_range_seconds()
                hard_cap = _now() + dt.timedelta(
                    seconds=int(ordinary_hi) + int(extra_seconds)
                )
                extended_until = min(
                    cd + dt.timedelta(seconds=extra_seconds),
                    hard_cap,
                )
                conn = get_connection()
                with db_lock(), conn:
                    conn.execute(
                        """
                        UPDATE accounts
                           SET cooldown_until=?,
                               updated_at=?
                         WHERE user_id=?
                        """,
                        (
                            extended_until.isoformat(),
                            _now_iso(),
                            account_user_id,
                        ),
                    )
                label = _account_label(account_user_id)
                await notify_admins(
                    _notify_peerflood(
                        label,
                        max(1, int((extended_until - _now()).total_seconds())),
                        streak=streak,
                        burst_triggered=True,
                        extra_seconds=extra_seconds,
                    )
                )
                logger.warning(
                    "PeerFlood burst extra cooldown added account={} extra_sec={} until={}",
                    account_user_id,
                    extra_seconds,
                    extended_until.isoformat(),
                )
            else:
                logger.info(
                    "PeerFlood counted and debounced account={} series={}/5 cooldown_until={}",
                    account_user_id,
                    streak,
                    cd.isoformat(),
                )
            return
        # Cooldown already over: only debounce if SpamBot still mid-check/limited.
        if st_status == STATUS_LIMITED:
            logger.info(
                "PeerFlood counted and debounced (still limited) account={} series={}/5",
                account_user_id,
                streak,
            )
            return
        if st_status == STATUS_CHECKING:
            # Allow re-entry if check is older than 10 min (stuck).
            pass

    if info.get("pause_seconds") is not None:
        seconds = int(info["pause_seconds"])
    else:
        seconds = int(runtime_svc.pick_peer_flood_seconds())
    min_until = _now() + dt.timedelta(seconds=seconds)
    pacing.set_paused(account_user_id, "PeerFlood", paused=True)
    # PeerFlood controls only cooldown_until. A pre-existing First DM interval
    # remains independent, but no new 2-7 minute interval is added here.
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

    # One check now; next_check only as safety net (not immediate re-queue)
    _upsert_state(
        account_user_id,
        status=STATUS_CHECKING,
        next_check_at=(_now() + dt.timedelta(hours=12)).isoformat(),
        limited_until=None,
    )
    label = _account_label(account_user_id)
    await notify_admins(
        _notify_peerflood(
            label,
            seconds,
            streak=streak,
            burst_triggered=burst_triggered,
            extra_seconds=extra_seconds,
        )
    )
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
    """Best-effort datetime extraction from @SpamBot limited messages.

    Supports:
      - 2026-08-03 23:42 / 2026-08-03T23:42:00
      - 03.08.2026 23:42
      - 3 Aug 2026, 23:42 UTC  (English months)
      - 3 августа 2026, 23:42 МСК  (Russian months)
      - UTC/GMT offsets such as UTC+3
    Parsed values are normalized to UTC. Fallback +24h only if nothing matched.
    """
    raw = text or ""

    # ISO-like: 2026-08-03 23:42[:ss]
    m = re.search(r"(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?)", raw)
    if m:
        try:
            parsed = dt.datetime.fromisoformat(m.group(1).replace(" ", "T"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=_timezone_from_text(raw))
            return parsed.astimezone(dt.timezone.utc).isoformat()
        except (TypeError, ValueError, OverflowError) as exc:
            logger.debug("SpamBot ISO date parse failed: {}", exc)

    # dd.mm.yyyy hh:mm
    m = re.search(r"(\d{1,2})[./](\d{1,2})[./](\d{4})\s+(\d{1,2}):(\d{2})", raw)
    if m:
        d, mo, y, h, mi = map(int, m.groups())
        try:
            return dt.datetime(y, mo, d, h, mi, tzinfo=_timezone_from_text(raw)).astimezone(dt.timezone.utc).isoformat()
        except (TypeError, ValueError, OverflowError) as exc:
            logger.debug("SpamBot numeric date parse failed: {}", exc)

    months_en = {
        "jan": 1, "january": 1,
        "feb": 2, "february": 2,
        "mar": 3, "march": 3,
        "apr": 4, "april": 4,
        "may": 5,
        "jun": 6, "june": 6,
        "jul": 7, "july": 7,
        "aug": 8, "august": 8,
        "sep": 9, "sept": 9, "september": 9,
        "oct": 10, "october": 10,
        "nov": 11, "november": 11,
        "dec": 12, "december": 12,
    }
    months_ru = {
        "январ": 1, "феврал": 2, "март": 3, "апрел": 4,
        "ма": 5, "июн": 6, "июл": 7, "август": 8,
        "сентябр": 9, "октябр": 10, "ноябр": 11, "декабр": 12,
    }

    # 3 Aug 2026, 23:42 UTC  |  3 August 2026 23:42
    m = re.search(
        r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\s*,?\s*(\d{1,2}):(\d{2})",
        raw,
    )
    if m:
        d = int(m.group(1))
        mon_s = m.group(2).lower()
        y, h, mi = int(m.group(3)), int(m.group(4)), int(m.group(5))
        mo = months_en.get(mon_s) or months_en.get(mon_s[:3])
        if mo:
            try:
                return dt.datetime(y, mo, d, h, mi, tzinfo=_timezone_from_text(raw)).astimezone(dt.timezone.utc).isoformat()
            except (TypeError, ValueError, OverflowError) as exc:
                logger.debug("SpamBot English date parse failed: {}", exc)

    # 3 августа 2026, 23:42  (optional UTC/МСК words ignored)
    m = re.search(
        r"(\d{1,2})\s+([А-Яа-яЁё]+)\s+(\d{4})\s*,?\s*(\d{1,2}):(\d{2})",
        raw,
    )
    if m:
        d = int(m.group(1))
        mon_s = m.group(2).lower()
        y, h, mi = int(m.group(3)), int(m.group(4)), int(m.group(5))
        mo = None
        for stem, num in months_ru.items():
            if mon_s.startswith(stem):
                # avoid "ма" matching "март" wrongly - already ordered; "мая" starts with ма
                if stem == "ма" and not mon_s.startswith("ма"):
                    continue
                if stem == "ма" and mon_s.startswith("март"):
                    mo = 3
                    break
                if stem == "ма" and (mon_s.startswith("мая") or mon_s == "май"):
                    mo = 5
                    break
                if stem != "ма":
                    mo = num
                    break
        if mo:
            try:
                return dt.datetime(y, mo, d, h, mi, tzinfo=_timezone_from_text(raw)).astimezone(dt.timezone.utc).isoformat()
            except (TypeError, ValueError, OverflowError) as exc:
                logger.debug("SpamBot Russian date parse failed: {}", exc)

    logger.warning("SpamBot until date unparsed, fallback +24h. snippet={!r}", raw[:180])
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
                await account_auth.register_auth_loss(
                    account_user_id, "session_not_authorized", notify=True
                )
                raise RuntimeError("session_not_authorized")
            return await _run_spambot_dialog(client, account_user_id)
        except Exception as exc:
            if account_auth.is_auth_loss_error(exc):
                await account_auth.register_auth_loss(
                    account_user_id, exc, notify=True
                )
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
                except Exception as exc:
                    logger.debug("Temporary SpamBot client disconnect failed: {}", exc)
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
        if account_auth.is_auth_loss_error(exc):
            await account_auth.register_auth_loss(
                account_user_id, exc, notify=True
            )
            await monitor_svc.disconnect_account(
                account_user_id, cancel_tasks=True
            )
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
        # Resume only after account cooldown_until (PeerFlood pause window).
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
        await notify_admins(
            _notify_limited(label, until or "", next_check or "")
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
    """Clear PeerFlood pause and mark SpamBot idle.

    Manual resume also clears next_send_at so admin override takes effect now.
    Auto resume (spambot_*) keeps next_send_at to avoid sticky same-account loops.
    """
    account_user_id = int(account_user_id)
    pacing.set_paused(account_user_id, "", paused=False)
    clear_next = source == "manual"
    conn = get_connection()
    with db_lock(), conn:
        if clear_next:
            conn.execute(
                """
                UPDATE accounts
                   SET is_paused=0,
                       pause_reason=NULL,
                       cooldown_until=NULL,
                       next_send_at=NULL,
                       peerflood_burst_applied_at=NULL,
                       updated_at=?
                 WHERE user_id=?
                """,
                (_now_iso(), account_user_id),
            )
        else:
            conn.execute(
                """
                UPDATE accounts
                   SET is_paused=0,
                       pause_reason=NULL,
                       cooldown_until=NULL,
                       peerflood_burst_applied_at=NULL,
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
        if status == STATUS_FREE_PENDING:
            if not SPAMBOT_AUTO_RESUME:
                continue
            acc = accounts_svc.get_account(uid)
            cooldown = _parse_iso((acc or {}).get("cooldown_until"))
            if cooldown and cooldown > _now():
                # push next_check to cooldown so we do not spin
                _upsert_state(
                    uid,
                    status=STATUS_FREE_PENDING,
                    next_check_at=cooldown.isoformat(),
                )
                continue
            await resume_account(uid, source="spambot_auto")
            actions += 1
            continue
        if status == STATUS_CHECKING:
            # Stuck CHECKING past next_check_at → re-check once (prior run may have crashed).
            await check_account(uid, force=True)
            actions += 1
            continue
        if status in {STATUS_LIMITED, STATUS_ERROR}:
            await check_account(uid, force=True)
            actions += 1
            continue
        # idle or unknown: clear stale next_check
        _upsert_state(uid, status=status or STATUS_IDLE, next_check_at=None)
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
