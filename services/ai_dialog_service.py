"""AI dialog layer for TgBlaster DM.

The old DM module creates the first contact. This service handles only follow-up
incoming private messages and keeps short dialog history in SQLite.
"""

from __future__ import annotations

import asyncio
import datetime
import random
import sqlite3
from dataclasses import dataclass
from typing import Any, Optional
from weakref import WeakValueDictionary

from decouple import config
from loguru import logger
from telethon import TelegramClient
from telethon.tl.types import User

from config import conn


def _now_iso() -> str:
    return datetime.datetime.utcnow().isoformat()


def _truthy(name: str, default: str = "false") -> bool:
    value = config(name, default=default).strip().lower()
    return value in {"1", "true", "yes", "on", "y", "да"}


def ai_enabled() -> bool:
    return _truthy("AI_DM_ENABLED", "false")


def ai_dry_run() -> bool:
    return _truthy("AI_DM_DRY_RUN", "false")


def _csv_words(name: str, default: str) -> list[str]:
    raw = config(name, default=default)
    return [w.strip().lower() for w in raw.split(",") if w.strip()]


def _contains_any(text: str, words: list[str]) -> bool:
    lower = (text or "").lower()
    return any(word in lower for word in words)


def _safe_int(name: str, default: int, min_value: int | None = None, max_value: int | None = None) -> int:
    try:
        value = int(config(name, default=str(default)))
    except Exception:
        value = default
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def _claim_incoming_message(account_user_id: int, target_user_id: int, telegram_message_id: int | None) -> bool:
    """Returns False when this exact Telegram PM was already handled.

    This prevents duplicate AI replies when several active DM tasks are running
    under the same sender account and each client receives the same private msg.
    """
    if telegram_message_id is None:
        return True
    cursor = conn.cursor()
    try:
        cursor.execute(
            """INSERT INTO ai_processed_messages
               (account_user_id, target_user_id, telegram_message_id, processed_at)
               VALUES (?,?,?,?)""",
            (account_user_id, target_user_id, telegram_message_id, _now_iso()),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    except Exception as exc:
        logger.error(f"[AI DM] incoming-message de-duplication failed: {exc}")
        # Do not drop a real user reply because of a transient DB issue.
        return True
    finally:
        cursor.close()


def _daily_dialog_limit_reached() -> bool:
    limit = _safe_int("AI_DAILY_DIALOG_LIMIT", 0, min_value=0)
    if limit <= 0:
        return False
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM ai_dialogs WHERE date(created_at) = date('now')")
        count = int(cursor.fetchone()[0] or 0)
        return count >= limit
    finally:
        cursor.close()


async def _safe_send_message(client: TelegramClient, target: User, text: str, context: str) -> bool:
    target_id = int(getattr(target, "id", 0) or 0)
    try:
        await client.send_message(target, text)
        return True
    except Exception as exc:
        logger.error(f"[AI DM] send failed ({context}) user={target_id}: {exc}")
        return False


STOP_WORDS_DEFAULT = "стоп,не пиши,отстань,не интересно,не надо,удали,заблокирую,жалоба,спам"
HUMAN_WORDS_DEFAULT = "админ,оператор,человек,менеджер,живой"

# Per-user async locks protect against duplicate/parallel AI replies when several
# DM tasks for the same account receive the same private message.
_dialog_locks: WeakValueDictionary[tuple[int, int], asyncio.Lock] = WeakValueDictionary()


def _get_dialog_lock(account_user_id: int, target_user_id: int) -> asyncio.Lock:
    key = (account_user_id, target_user_id)
    lock = _dialog_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _dialog_locks[key] = lock
    return lock


@dataclass
class DialogRow:
    id: int
    dm_task_id: int
    account_user_id: int
    target_user_id: int
    username: Optional[str]
    first_name: Optional[str]
    stage: str
    status: str
    message_count: int
    stopped_reason: Optional[str]


def create_ai_tables() -> None:
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ai_dialogs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dm_task_id INTEGER,
            account_user_id INTEGER NOT NULL,
            target_user_id INTEGER NOT NULL,
            username TEXT,
            first_name TEXT,
            stage TEXT DEFAULT 'new_contact',
            status TEXT DEFAULT 'active',
            message_count INTEGER DEFAULT 0,
            last_incoming_at TEXT,
            last_outgoing_at TEXT,
            created_at TEXT,
            updated_at TEXT,
            stopped_reason TEXT,
            UNIQUE(account_user_id, target_user_id)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ai_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dialog_id INTEGER NOT NULL,
            direction TEXT NOT NULL,
            message_text TEXT,
            created_at TEXT,
            provider TEXT,
            model TEXT,
            tokens_used INTEGER DEFAULT 0
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ai_processed_messages (
            account_user_id INTEGER NOT NULL,
            target_user_id INTEGER NOT NULL,
            telegram_message_id INTEGER NOT NULL,
            processed_at TEXT NOT NULL,
            PRIMARY KEY (account_user_id, target_user_id, telegram_message_id)
        )
    """)
    # Older installations may have been created before the UNIQUE constraint.
    # Merge duplicate dialog rows without losing their message history.
    duplicates = cursor.execute(
        """
        SELECT account_user_id, target_user_id, MAX(id) AS keep_id
        FROM ai_dialogs
        GROUP BY account_user_id, target_user_id
        HAVING COUNT(*) > 1
        """
    ).fetchall()
    for account_user_id, target_user_id, keep_id in duplicates:
        duplicate_ids = [
            int(row[0])
            for row in cursor.execute(
                """
                SELECT id FROM ai_dialogs
                WHERE account_user_id = ? AND target_user_id = ? AND id <> ?
                """,
                (account_user_id, target_user_id, keep_id),
            ).fetchall()
        ]
        for duplicate_id in duplicate_ids:
            cursor.execute(
                "UPDATE ai_messages SET dialog_id = ? WHERE dialog_id = ?",
                (keep_id, duplicate_id),
            )
            cursor.execute("DELETE FROM ai_dialogs WHERE id = ?", (duplicate_id,))

    cursor.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_ai_dialogs_account_target "
        "ON ai_dialogs(account_user_id, target_user_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_ai_dialogs_status_updated ON ai_dialogs(status, updated_at)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_ai_messages_dialog_id ON ai_messages(dialog_id, id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_ai_processed_at ON ai_processed_messages(processed_at)"
    )

    # Лёгкие миграции для старых БД.
    for table, col, ddl in [
        ("ai_dialogs", "stopped_reason", "ALTER TABLE ai_dialogs ADD COLUMN stopped_reason TEXT"),
        ("ai_dialogs", "message_count", "ALTER TABLE ai_dialogs ADD COLUMN message_count INTEGER DEFAULT 0"),
        ("ai_messages", "tokens_used", "ALTER TABLE ai_messages ADD COLUMN tokens_used INTEGER DEFAULT 0"),
    ]:
        try:
            cursor.execute(ddl)
            conn.commit()
        except Exception:
            pass
    conn.commit()
    cursor.close()


def _row_to_dialog(row: tuple | None) -> Optional[DialogRow]:
    if not row:
        return None
    return DialogRow(*row)


def _get_dialog_by_target(account_user_id: int, target_user_id: int) -> Optional[DialogRow]:
    cursor = conn.cursor()
    cursor.execute(
        """SELECT id, dm_task_id, account_user_id, target_user_id, username, first_name,
                  stage, status, message_count, stopped_reason
           FROM ai_dialogs WHERE account_user_id = ? AND target_user_id = ?""",
        (account_user_id, target_user_id),
    )
    row = cursor.fetchone()
    cursor.close()
    return _row_to_dialog(row)


def _get_dialog_by_id(dialog_id: int) -> Optional[DialogRow]:
    cursor = conn.cursor()
    cursor.execute(
        """SELECT id, dm_task_id, account_user_id, target_user_id, username, first_name,
                  stage, status, message_count, stopped_reason
           FROM ai_dialogs WHERE id = ?""",
        (dialog_id,),
    )
    row = cursor.fetchone()
    cursor.close()
    return _row_to_dialog(row)


def _upsert_dialog(
    *,
    dm_task_id: int,
    account_user_id: int,
    target_user_id: int,
    username: Optional[str],
    first_name: Optional[str],
) -> DialogRow:
    existing = _get_dialog_by_target(account_user_id, target_user_id)
    now = _now_iso()
    cursor = conn.cursor()
    if existing:
        cursor.execute(
            """UPDATE ai_dialogs SET dm_task_id = ?, username = COALESCE(?, username),
                      first_name = COALESCE(?, first_name), updated_at = ?
               WHERE id = ?""",
            (dm_task_id, username, first_name, now, existing.id),
        )
        conn.commit()
        cursor.close()
        return _get_dialog_by_id(existing.id) or existing

    cursor.execute(
        """INSERT INTO ai_dialogs
           (dm_task_id, account_user_id, target_user_id, username, first_name, stage,
            status, message_count, created_at, updated_at)
           VALUES (?,?,?,?,?,'new_contact','active',0,?,?)""",
        (dm_task_id, account_user_id, target_user_id, username, first_name, now, now),
    )
    dialog_id = cursor.lastrowid
    conn.commit()
    cursor.close()
    return _get_dialog_by_id(dialog_id)  # type: ignore[return-value]


def _save_message(
    dialog_id: int,
    direction: str,
    text: str,
    provider: str = "local",
    model: str = "",
    tokens_used: int = 0,
) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO ai_messages
           (dialog_id, direction, message_text, created_at, provider, model, tokens_used)
           VALUES (?,?,?,?,?,?,?)""",
        (dialog_id, direction, text, _now_iso(), provider, model, tokens_used),
    )
    conn.commit()
    cursor.close()


def _set_dialog_status(dialog_id: int, status: str, reason: str = "", stage: Optional[str] = None) -> None:
    cursor = conn.cursor()
    if stage:
        cursor.execute(
            """UPDATE ai_dialogs SET status = ?, stopped_reason = ?, stage = ?, updated_at = ?
               WHERE id = ?""",
            (status, reason, stage, _now_iso(), dialog_id),
        )
    else:
        cursor.execute(
            """UPDATE ai_dialogs SET status = ?, stopped_reason = ?, updated_at = ? WHERE id = ?""",
            (status, reason, _now_iso(), dialog_id),
        )
    conn.commit()
    cursor.close()


def _set_stage(dialog_id: int, stage: str) -> None:
    cursor = conn.cursor()
    cursor.execute("UPDATE ai_dialogs SET stage = ?, updated_at = ? WHERE id = ?", (stage, _now_iso(), dialog_id))
    conn.commit()
    cursor.close()


def _mark_incoming(dialog_id: int) -> None:
    cursor = conn.cursor()
    cursor.execute("UPDATE ai_dialogs SET last_incoming_at = ?, updated_at = ? WHERE id = ?", (_now_iso(), _now_iso(), dialog_id))
    conn.commit()
    cursor.close()


def _mark_outgoing(dialog_id: int) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """UPDATE ai_dialogs
           SET last_outgoing_at = ?, updated_at = ?, message_count = COALESCE(message_count,0) + 1
           WHERE id = ?""",
        (_now_iso(), _now_iso(), dialog_id),
    )
    conn.commit()
    cursor.close()


def record_first_dm(
    *,
    dm_task_id: int,
    account_user_id: int,
    target: User,
    text: str,
) -> None:
    """Create a new AI response cycle after a real first DM was delivered.

    A dialog row is reused per sender/recipient pair. Older versions only updated
    metadata for an existing row, so a recipient whose previous cycle was already
    ``completed`` stayed completed and the AI silently ignored the new reply.

    A newly delivered first DM re-opens ordinary completed/error cycles. Explicit
    opt-out, admin stop and human-takeover states remain closed as before.
    """
    if not ai_enabled():
        return
    create_ai_tables()
    existing = _get_dialog_by_target(account_user_id, target.id)
    if existing is None and _daily_dialog_limit_reached():
        logger.warning(f"[AI DM] daily dialog limit reached; skip dialog for user={target.id}")
        return
    dialog = _upsert_dialog(
        dm_task_id=dm_task_id,
        account_user_id=account_user_id,
        target_user_id=target.id,
        username=getattr(target, "username", None),
        first_name=getattr(target, "first_name", None),
    )
    _save_message(dialog.id, "outgoing", text, provider="dm_first", model="first_dm")

    protected_closed_statuses = {"closed_negative", "admin_stopped", "human_needed"}
    cursor = conn.cursor()
    try:
        if dialog.status in protected_closed_statuses:
            cursor.execute(
                "UPDATE ai_dialogs SET last_outgoing_at = ?, updated_at = ? WHERE id = ?",
                (_now_iso(), _now_iso(), dialog.id),
            )
            logger.info(
                f"[AI DM] first DM recorded without reopening protected state: "
                f"dialog={dialog.id}, user={target.id}, status={dialog.status}"
            )
        else:
            cursor.execute(
                """UPDATE ai_dialogs
                   SET status = 'active', stage = 'first_dm_sent', stopped_reason = NULL,
                       last_outgoing_at = ?, updated_at = ?
                   WHERE id = ?""",
                (_now_iso(), _now_iso(), dialog.id),
            )
            logger.info(
                f"[AI DM] response cycle opened after first DM: "
                f"dialog={dialog.id}, user={target.id}, previous_status={dialog.status}"
            )
        conn.commit()
    finally:
        cursor.close()


PIRATE_VIP_LINK = "https://telegram.me/+pvPjmt2KW_QyZTAy"
PIRATE_VIP_LINK_TOKEN = "pvPjmt2KW_QyZTAy"
PIRATE_VIP_SOURCES = (
    "2Trade [PRIVATE]",
    "Sancho DT [Premium]",
    "CryptoMan [PREMIUM]",
    "Trader 80/20 [Premium]",
    "Дмитрий Аниськевич [Premium]",
)


def _extract_output_text(response: Any) -> str:
    text = getattr(response, "output_text", None)
    if text:
        return str(text).strip()
    try:
        parts = []
        for item in getattr(response, "output", []) or []:
            for content in getattr(item, "content", []) or []:
                value = getattr(content, "text", None)
                if value:
                    parts.append(value)
        return "\n".join(parts).strip()
    except Exception:
        return ""


def _offer_intro_fallback() -> str:
    variants = (
        "Смотри, расскажу быстро 👇",
        "Да, к делу — вот что хотел показать 👇",
        "Короче, не буду тянуть — есть такая подборка 👇",
        "Расскажу сразу по сути 👇",
    )
    return random.choice(variants)


async def _generate_offer_intro(user_text: str) -> tuple[str, int, str]:
    """Generate only one short contextual lead-in for the fixed offer block.

    The model cannot alter the mandatory offer facts or link because those are
    appended locally after this sentence.
    """
    api_key = config("OPENAI_API_KEY", default="").strip()
    if not api_key:
        return _offer_intro_fallback(), 0, "local_intro"

    model = config("AI_MODEL", default="gpt-4o-mini").strip()
    from openai import AsyncOpenAI

    client = AsyncOpenAI(api_key=api_key)
    response = await client.responses.create(
        model=model,
        instructions=(
            "Ответь по-русски одной короткой естественной фразой-вступлением, максимум 12 слов. "
            "Не задавай вопросов, не используй ссылки, не перечисляй каналы, не обещай прибыль. "
            "Фраза должна мягко перейти к краткому объяснению предложения."
        ),
        input=[{"role": "user", "content": user_text}],
        max_output_tokens=_safe_int("AI_OFFER_INTRO_MAX_TOKENS", 48, min_value=16, max_value=96),
    )
    usage = getattr(response, "usage", None)
    tokens = int(getattr(usage, "total_tokens", 0) or 0) if usage is not None else 0
    intro = _extract_output_text(response).replace("\n", " ").strip()
    if not intro:
        raise RuntimeError("OpenAI returned empty offer intro")

    lower_intro = intro.lower()
    forbidden = ("http://", "https://", "t.me", "telegram.me", "2trade", "sancho", "cryptoman", "80/20", "аниськевич")
    if "?" in intro or any(token in lower_intro for token in forbidden):
        raise RuntimeError("OpenAI intro violated compact-funnel format")

    # Defensive cap: even a disobedient model cannot create a long preamble.
    words = intro.split()
    if len(words) > 16:
        intro = " ".join(words[:16]).rstrip(" ,;:-") + "."
    return intro, tokens, model


def _canonical_offer_reply(intro: str) -> str:
    sources = "\n".join(f"🔹 {name}" for name in PIRATE_VIP_SOURCES)
    return (
        f"{intro.strip()}\n\n"
        "Есть 🏴‍☠️ PIRATE VIP FREE — бесплатная подборка постов сразу из 5 VIP-каналов:\n\n"
        f"{sources}\n\n"
        "Новые посты из этих VIP-каналов дублируются с задержкой до 0.5 секунды. "
        "Совокупная стоимость отдельных доступов — более 1000$, а здесь всё бесплатно.\n\n"
        f"Вот ссылка: {PIRATE_VIP_LINK}\n\n"
        "Заходи и спокойно посмотри. Не спеши выходить — просто глянь, подходит ли тебе 🙂"
    )


def _latest_first_dm_message_id(dialog_id: int) -> int:
    """Return the message boundary for the current first-DM response cycle."""
    cursor = conn.cursor()
    try:
        row = cursor.execute(
            """
            SELECT MAX(id) FROM ai_messages
            WHERE dialog_id = ? AND provider = 'dm_first'
            """,
            (dialog_id,),
        ).fetchone()
        return int((row or [0])[0] or 0)
    finally:
        cursor.close()


def _dialog_has_sent_offer(dialog_id: int) -> bool:
    """Detect an offer sent after the latest first DM in the current cycle.

    Older offers must not suppress the reply to a newly delivered first DM.
    """
    cycle_start_id = _latest_first_dm_message_id(dialog_id)
    cursor = conn.cursor()
    try:
        row = cursor.execute(
            """
            SELECT 1 FROM ai_messages
            WHERE dialog_id = ?
              AND id > ?
              AND direction = 'outgoing'
              AND instr(COALESCE(message_text, ''), ?) > 0
            LIMIT 1
            """,
            (dialog_id, cycle_start_id, PIRATE_VIP_LINK_TOKEN),
        ).fetchone()
        return row is not None
    finally:
        cursor.close()


def _dialog_has_post_offer_apology(dialog_id: int) -> bool:
    cycle_start_id = _latest_first_dm_message_id(dialog_id)
    cursor = conn.cursor()
    try:
        row = cursor.execute(
            """
            SELECT 1 FROM ai_messages
            WHERE dialog_id = ?
              AND id > ?
              AND direction = 'outgoing'
              AND model = 'post_offer_apology'
            LIMIT 1
            """,
            (dialog_id, cycle_start_id),
        ).fetchone()
        return row is not None
    finally:
        cursor.close()


def _post_offer_apology() -> str:
    default = "Понял, извини за беспокойство. Больше писать не буду 🙌"
    return config("AI_POST_OFFER_APOLOGY", default=default).strip() or default


async def _send_post_offer_apology(
    *, dialog: DialogRow, client: TelegramClient, sender: User
) -> None:
    """Send at most one courtesy apology after the link, then close forever."""
    if _dialog_has_post_offer_apology(dialog.id):
        _set_dialog_status(dialog.id, "closed_negative", "post_offer_apology_already_sent", stage="closed_negative")
        return

    reply = _post_offer_apology()
    if ai_dry_run():
        logger.info(f"[AI DM DRY RUN post-offer apology] user={sender.id}: {reply}")
        _save_message(
            dialog.id,
            "system",
            f"[DRY RUN post-offer apology] {reply}",
            provider="dry_run",
            model="post_offer_apology",
        )
        return

    sent = await _safe_send_message(client, sender, reply, "post_offer_apology")
    if not sent:
        _set_dialog_status(dialog.id, "send_error", "telegram_send_failed", stage="send_error")
        return
    _save_message(dialog.id, "outgoing", reply, provider="local", model="post_offer_apology")
    _mark_outgoing(dialog.id)
    _set_dialog_status(dialog.id, "closed_negative", "post_offer_rejection", stage="closed_negative")
    logger.info(f"[AI DM] one-time post-offer apology sent: dialog={dialog.id}, user={sender.id}")


async def handle_private_incoming(
    *,
    dm_task_id: int,
    account_user_id: int,
    client: TelegramClient,
    sender: User,
    text: str,
    message_id: int | None = None,
) -> None:
    """Handle a private reply with a strict two-step compact funnel.

    Outgoing sequence:
      1. Existing random first DM (sent by the DM worker).
      2. One concise offer containing all mandatory facts and the exact link.
      3. Optional one-time apology only when the user explicitly rejects/opts out.

    After the offer link has been sent, all other incoming messages are ignored.
    """
    if not ai_enabled():
        return
    create_ai_tables()

    if not _claim_incoming_message(account_user_id, sender.id, message_id):
        logger.debug(
            f"[AI DM] duplicate private message ignored: "
            f"account={account_user_id}, user={sender.id}, message_id={message_id}"
        )
        return

    lock = _get_dialog_lock(account_user_id, sender.id)
    async with lock:
        dialog = _get_dialog_by_target(account_user_id, sender.id)
        if not dialog:
            if _truthy("AI_REPLY_ONLY_KNOWN_DIALOGS", "true"):
                return
            if _daily_dialog_limit_reached():
                logger.warning(f"[AI DM] daily dialog limit reached; skip incoming user={sender.id}")
                return
            dialog = _upsert_dialog(
                dm_task_id=dm_task_id,
                account_user_id=account_user_id,
                target_user_id=sender.id,
                username=getattr(sender, "username", None),
                first_name=getattr(sender, "first_name", None),
            )

        text = (text or "").strip()
        if not text:
            return

        stop_words = _csv_words("AI_STOP_WORDS", STOP_WORDS_DEFAULT)
        human_words = _csv_words("AI_HUMAN_TAKEOVER_WORDS", HUMAN_WORDS_DEFAULT)
        offer_already_sent = _dialog_has_sent_offer(dialog.id)

        # Existing conversations from older versions may still be marked active
        # even though the link was already sent. Close them on the next incoming
        # message and never restart the long conversation.
        if offer_already_sent:
            _save_message(dialog.id, "incoming", text, provider="telegram")
            _mark_incoming(dialog.id)
            if _contains_any(text, stop_words):
                await _send_post_offer_apology(dialog=dialog, client=client, sender=sender)
            else:
                _set_dialog_status(dialog.id, "completed", "offer_already_sent", stage="completed")
                logger.info(
                    f"[AI DM] post-offer message ignored: dialog={dialog.id}, user={sender.id}"
                )
            return

        if dialog.status != "active":
            return

        _save_message(dialog.id, "incoming", text, provider="telegram")
        _mark_incoming(dialog.id)

        if _contains_any(text, stop_words):
            reply = config("AI_STOP_REPLY", default="Понял, больше писать не буду 🙌").strip()
            if ai_dry_run():
                logger.info(f"[AI DM DRY RUN stop] user={sender.id}: {reply}")
                _save_message(
                    dialog.id,
                    "system",
                    f"[DRY RUN stop draft] {reply}",
                    provider="dry_run",
                    model="stop_reply",
                )
                return
            sent = await _safe_send_message(client, sender, reply, "stop_reply")
            if not sent:
                _set_dialog_status(dialog.id, "send_error", "telegram_send_failed")
                return
            _save_message(dialog.id, "outgoing", reply, provider="local", model="stop_reply")
            _mark_outgoing(dialog.id)
            _set_dialog_status(dialog.id, "closed_negative", "stop_word", stage="closed_negative")
            logger.info(f"[AI DM] stop-word: dialog={dialog.id}, user={sender.id}")
            return

        if _contains_any(text, human_words):
            reply = config(
                "AI_HUMAN_TAKEOVER_REPLY",
                default="Понял, лучше передам человеку, чтобы ответили точнее 🙌",
            ).strip()
            if ai_dry_run():
                logger.info(f"[AI DM DRY RUN human] user={sender.id}: {reply}")
                _save_message(
                    dialog.id,
                    "system",
                    f"[DRY RUN human draft] {reply}",
                    provider="dry_run",
                    model="human_takeover",
                )
                return
            sent = await _safe_send_message(client, sender, reply, "human_takeover")
            if not sent:
                _set_dialog_status(dialog.id, "send_error", "telegram_send_failed")
                return
            _save_message(dialog.id, "outgoing", reply, provider="local", model="human_takeover")
            _mark_outgoing(dialog.id)
            _set_dialog_status(dialog.id, "human_needed", "human_takeover", stage="human_needed")
            logger.info(f"[AI DM] human takeover: dialog={dialog.id}, user={sender.id}")
            return

        dmin = _safe_int("AI_REPLY_DELAY_MIN_SECONDS", 15, min_value=0, max_value=3600)
        dmax = _safe_int("AI_REPLY_DELAY_MAX_SECONDS", 60, min_value=0, max_value=3600)
        if dmax < dmin:
            dmax = dmin
        delay = random.randint(dmin, dmax)
        if delay:
            await asyncio.sleep(delay)

        # The dialog may have been stopped during the delay.
        dialog = _get_dialog_by_id(dialog.id) or dialog
        if dialog.status != "active" or _dialog_has_sent_offer(dialog.id):
            return

        try:
            intro, tokens, model = await _generate_offer_intro(text)
        except Exception as exc:
            logger.error(f"[AI DM] OpenAI intro error for dialog={dialog.id}, user={sender.id}: {exc}")
            intro, tokens, model = _offer_intro_fallback(), 0, "local_intro"

        reply = _canonical_offer_reply(intro)

        if ai_dry_run():
            logger.info(f"[AI DM DRY RUN compact offer] user={sender.id}: {reply}")
            _save_message(
                dialog.id,
                "system",
                f"[DRY RUN compact offer] {reply}",
                provider="dry_run",
                model=model,
                tokens_used=tokens,
            )
            return

        sent = await _safe_send_message(client, sender, reply, "compact_offer")
        if not sent:
            _set_dialog_status(dialog.id, "send_error", "telegram_send_failed", stage="send_error")
            return

        _save_message(
            dialog.id,
            "outgoing",
            reply,
            provider="openai+local" if model != "local_intro" else "local",
            model=model,
            tokens_used=tokens,
        )
        _mark_outgoing(dialog.id)
        _set_dialog_status(dialog.id, "completed", "offer_sent", stage="completed")
        logger.info(
            f"[AI DM] compact offer sent and dialog completed: dialog={dialog.id}, user={sender.id}"
        )


def stop_dialog_by_user(target_user_id: int, reason: str = "admin_stop") -> bool:
    cursor = conn.cursor()
    cursor.execute(
        """UPDATE ai_dialogs SET status='admin_stopped', stopped_reason=?, updated_at=?
           WHERE target_user_id=? AND status='active'""",
        (reason, _now_iso(), target_user_id),
    )
    affected = cursor.rowcount
    conn.commit()
    cursor.close()
    return affected > 0


def resume_dialog_by_user(target_user_id: int) -> bool:
    cursor = conn.cursor()
    cursor.execute(
        """UPDATE ai_dialogs SET status='active', stopped_reason=NULL, updated_at=?
           WHERE target_user_id=?""",
        (_now_iso(), target_user_id),
    )
    affected = cursor.rowcount
    conn.commit()
    cursor.close()
    return affected > 0


def ai_stats() -> dict[str, Any]:
    create_ai_tables()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM ai_dialogs")
    total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM ai_dialogs WHERE status='active'")
    active = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM ai_messages WHERE date(created_at) = date('now')")
    messages_today = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM ai_dialogs WHERE date(created_at) = date('now')")
    dialogs_today = cursor.fetchone()[0]
    cursor.close()
    return {
        "enabled": ai_enabled(),
        "dry_run": ai_dry_run(),
        "model": config("AI_MODEL", default="gpt-4o-mini"),
        "total_dialogs": total,
        "active_dialogs": active,
        "messages_today": messages_today,
        "dialogs_today": dialogs_today,
        "daily_dialog_limit": _safe_int("AI_DAILY_DIALOG_LIMIT", 0, min_value=0),
        "funnel_mode": "compact_offer_once",
        "close_after_offer": True,
        "post_offer_apology": True,
    }


def recent_dialogs(limit: int = 10) -> list[tuple]:
    create_ai_tables()
    cursor = conn.cursor()
    cursor.execute(
        """SELECT target_user_id, username, first_name, stage, status, message_count, updated_at
           FROM ai_dialogs ORDER BY updated_at DESC LIMIT ?""",
        (limit,),
    )
    rows = cursor.fetchall()
    cursor.close()
    return rows


def export_dialogs_text(limit: int = 200) -> str:
    create_ai_tables()
    cursor = conn.cursor()
    cursor.execute(
        """SELECT d.target_user_id, d.username, d.stage, d.status, m.direction, m.message_text, m.created_at
           FROM ai_messages m
           JOIN ai_dialogs d ON d.id = m.dialog_id
           ORDER BY m.id DESC LIMIT ?""",
        (limit,),
    )
    rows = list(reversed(cursor.fetchall()))
    cursor.close()
    lines = ["AI dialogs export"]
    for uid, username, stage, status, direction, text, created in rows:
        lines.append(f"[{created}] user={uid} @{username or '-'} stage={stage} status={status} {direction}: {text}")
    return "\n".join(lines)
