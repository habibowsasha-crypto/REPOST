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


def _notify_peerflood(label: str, seconds: int, *, streak: int = 1, interval_bumped: bool = False) -> str:
    from services import runtime as runtime_svc
    pause = runtime_svc.format_duration(int(seconds))
    rng = runtime_svc.format_peer_flood_range()
    line1 = f"⚠️ Аккаунт **{label}** словил PeerFlood."
    if streak >= 2 or seconds >= 25 * 60:
        line2 = (
            f"Повтор быстрее 10–15 мин → пауза **{pause}** "
            f"(обычно {rng}). Спрашиваю @SpamBot."
        )
        extra = "\n🔁 Временный cooldown, потом как в настройках."
    else:
        line2 = f"Пауза **{pause}** (настройки {rng}). Спрашиваю @SpamBot."
        extra = ""
    if interval_bumped:
        extra += "\n⏱ На 30 мин увеличена задержка first DM на этом аккаунте."
    return line1 + "\n" + line2 + extra

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
    PeerFlood from Telegram on DM send.

    Important: @SpamBot "free" != no PeerFlood. PeerFlood is a cold-DM rate limit.
    Debounce: if already paused with active cooldown, do not re-notify or re-/start SpamBot.
    """
    account_user_id = int(account_user_id)
    from services import runtime as runtime_svc

    acc = accounts_svc.get_account(account_user_id)
    st = get_state(account_user_id) or {}
    st_status = str(st.get("status") or "")

    if acc and acc.get("is_paused"):
        cd = _parse_iso(acc.get("cooldown_until"))
        if cd and cd > _now():
            logger.info(
                "PeerFlood debounced (cooldown until {}) account={}",
                cd.isoformat(),
                account_user_id,
            )
            return
        # Cooldown already over: only debounce if SpamBot still mid-check/limited.
        if st_status == STATUS_LIMITED:
            logger.info(
                "PeerFlood debounced (still limited) account={}",
                account_user_id,
            )
            return
        if st_status == STATUS_CHECKING:
            # Allow re-entry if check is older than 10 min (stuck)
            pass

    info = accounts_svc.register_peerflood_hit(account_user_id)
    streak = int(info.get("streak") or 1)
    interval_bumped = bool(info.get("interval_bumped"))
    rapid = bool(info.get("rapid"))
    if info.get("pause_seconds") is not None:
        seconds = int(info["pause_seconds"])
    else:
        seconds = int(runtime_svc.pick_peer_flood_seconds())
    min_until = _now() + dt.timedelta(seconds=seconds)
    pacing.set_paused(account_user_id, "PeerFlood", paused=True)
    # Push next_send_at past cooldown so this account is not the only
    # "ready" sender the moment pause lifts (others may still be in 10–15m window).
    acc_row = accounts_svc.get_account(account_user_id) or {}
    lo = acc_row.get("dm_interval_min_sec")
    hi = acc_row.get("dm_interval_max_sec")
    if lo is not None and hi is not None:
        import random as _rnd
        a, b = int(lo), int(hi)
        if a > b:
            a, b = b, a
        gap = _rnd.randint(max(60, a), max(60, b))
    else:
        gap = max(60, int(runtime_svc.get_account_interval_range()[0]))
    next_send = min_until + dt.timedelta(seconds=int(gap))
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE accounts
               SET cooldown_until=?,
                   pause_reason=?,
                   is_paused=1,
                   next_send_at=?,
                   updated_at=?
             WHERE user_id=?
            """,
            (
                min_until.isoformat(),
                "PeerFlood",
                next_send.isoformat(),
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
    await notify_admins(_notify_peerflood(label, seconds, streak=streak, interval_bumped=interval_bumped))
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
      - 3 августа 2026, 23:42  (Russian months)
    Fallback +24h only if nothing matched.
    """
    raw = text or ""

    # ISO-like: 2026-08-03 23:42[:ss]
    m = re.search(r"(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?)", raw)
    if m:
        try:
            parsed = dt.datetime.fromisoformat(m.group(1).replace(" ", "T"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            return parsed.isoformat()
        except Exception:
            pass

    # dd.mm.yyyy hh:mm
    m = re.search(r"(\d{1,2})[./](\d{1,2})[./](\d{4})\s+(\d{1,2}):(\d{2})", raw)
    if m:
        d, mo, y, h, mi = map(int, m.groups())
        try:
            return dt.datetime(y, mo, d, h, mi, tzinfo=dt.timezone.utc).isoformat()
        except Exception:
            pass

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
                return dt.datetime(y, mo, d, h, mi, tzinfo=dt.timezone.utc).isoformat()
            except Exception:
                pass

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
                # avoid "ма" matching "март" wrongly — already ordered; "мая" starts with ма
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
                return dt.datetime(y, mo, d, h, mi, tzinfo=dt.timezone.utc).isoformat()
            except Exception:
                pass

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
            # Already checked inline in on_peer_flood; do not /start again
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
