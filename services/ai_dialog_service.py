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


def _history(dialog_id: int, limit: int = 12) -> list[dict[str, str]]:
    cursor = conn.cursor()
    cursor.execute(
        """SELECT direction, message_text FROM ai_messages
           WHERE dialog_id = ? ORDER BY id DESC LIMIT ?""",
        (dialog_id, limit),
    )
    rows = list(reversed(cursor.fetchall()))
    cursor.close()
    result = []
    for direction, text in rows:
        role = "assistant" if direction == "outgoing" else "user"
        if direction == "system":
            role = "system"
        result.append({"role": role, "content": text or ""})
    return result


def record_first_dm(
    *,
    dm_task_id: int,
    account_user_id: int,
    target: User,
    text: str,
) -> None:
    """Creates/updates an active dialog after the first DM has actually been sent."""
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
    cursor = conn.cursor()
    cursor.execute("UPDATE ai_dialogs SET last_outgoing_at = ?, updated_at = ? WHERE id = ?", (_now_iso(), _now_iso(), dialog.id))
    conn.commit()
    cursor.close()


def _project_context() -> str:
    name = config("VIP_PROJECT_NAME", default="VIP-проект")
    link = config("VIP_CHANNEL_LINK", default="").strip()
    description = config(
        "VIP_DESCRIPTION",
        default="закрытый канал с торговыми идеями, разбором рынка и сопровождением",
    )
    price = config("VIP_PRICE_TEXT", default="").strip()
    return (
        f"Название проекта: {name}\n"
        f"Описание: {description}\n"
        f"Ссылка на VIP: {link or '[ссылка не задана]'}\n"
        f"Условия/цена: {price or '[не указано]'}"
    )


def _system_prompt(dialog: DialogRow) -> str:
    custom = config("AI_SYSTEM_PROMPT", default="").strip()
    if custom:
        return custom

    return f"""
Ты ассистент крипто/VIP-проекта. Общайся живо, коротко и по-человечески, на русском языке.

Главная задача: продолжить диалог после первого личного сообщения, понять интерес человека и мягко подвести к VIP-каналу, если это уместно.

Правила стиля:
- не пиши длинные полотна;
- 1 сообщение = 1-3 коротких абзаца;
- не начинай сразу с продажи;
- сначала задай простой вопрос;
- не обещай гарантированную прибыль, иксы, 100% сигналов или безрисковый доход;
- не выдумывай статистику, отзывы, результаты, цены и условия;
- если человек сомневается, отвечай спокойно;
- если человек просит не писать, не спорь;
- не утверждай, что ты реальный человек, если тебя прямо спрашивают;
- ссылку на VIP давай только если человек проявил интерес, спросил условия, ссылку, цену, доступ или сказал, что ему интересно.

Текущая стадия диалога: {dialog.stage}
Количество AI-ответов этому человеку: {dialog.message_count}

Данные проекта:
{_project_context()}
""".strip()


def _extract_output_text(response: Any) -> str:
    text = getattr(response, "output_text", None)
    if text:
        return str(text).strip()
    # Fallback на случай старой/нестандартной версии SDK.
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


async def _generate_ai_reply(dialog: DialogRow) -> tuple[str, int]:
    api_key = config("OPENAI_API_KEY", default="").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is empty")

    model = config("AI_MODEL", default="gpt-4o-mini").strip()
    max_output_tokens = _safe_int("AI_MAX_OUTPUT_TOKENS", 240, min_value=32, max_value=2000)

    from openai import AsyncOpenAI

    client = AsyncOpenAI(api_key=api_key)
    messages = _history(dialog.id, limit=_safe_int("AI_HISTORY_LIMIT", 12, min_value=2, max_value=30))
    input_messages = []
    for msg in messages:
        if msg["role"] == "system":
            continue
        input_messages.append({"role": msg["role"], "content": msg["content"]})

    response = await client.responses.create(
        model=model,
        instructions=_system_prompt(dialog),
        input=input_messages,
        max_output_tokens=max_output_tokens,
    )
    usage = getattr(response, "usage", None)
    tokens = 0
    if usage is not None:
        tokens = int(getattr(usage, "total_tokens", 0) or 0)
    text = _extract_output_text(response)
    if not text:
        raise RuntimeError("OpenAI returned empty response")
    return text, tokens


def _guess_next_stage(user_text: str, ai_text: str, current_stage: str) -> str:
    lower = f"{user_text}\n{ai_text}".lower()
    if any(x in lower for x in ["ссылка", "услов", "цена", "сколько", "доступ", "как попасть", "хочу", "интересно"]):
        return "offer"
    if any(x in lower for x in ["торг", "крипт", "вип", "vip", "сигнал"]):
        return "qualification"
    if current_stage == "new_contact":
        return "small_talk"
    return current_stage


async def handle_private_incoming(
    *,
    dm_task_id: int,
    account_user_id: int,
    client: TelegramClient,
    sender: User,
    text: str,
    message_id: int | None = None,
) -> None:
    """Handles a user's private reply to the userbot account."""
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

        if dialog.status != "active":
            return

        text = (text or "").strip()
        if not text:
            return

        stop_words = _csv_words("AI_STOP_WORDS", STOP_WORDS_DEFAULT)
        human_words = _csv_words("AI_HUMAN_TAKEOVER_WORDS", HUMAN_WORDS_DEFAULT)
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
            reply = config("AI_HUMAN_TAKEOVER_REPLY", default="Понял, лучше передам человеку, чтобы ответили точнее 🙌").strip()
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

        max_messages = _safe_int("AI_MAX_MESSAGES_PER_USER", 8, min_value=1, max_value=100)
        if dialog.message_count >= max_messages:
            _set_dialog_status(dialog.id, "closed_limit", "max_messages", stage="closed_limit")
            logger.info(f"[AI DM] max messages reached: dialog={dialog.id}, user={sender.id}")
            return

        dmin = _safe_int("AI_REPLY_DELAY_MIN_SECONDS", 15, min_value=0, max_value=3600)
        dmax = _safe_int("AI_REPLY_DELAY_MAX_SECONDS", 60, min_value=0, max_value=3600)
        if dmax < dmin:
            dmax = dmin
        delay = random.randint(dmin, dmax)
        if delay:
            await asyncio.sleep(delay)

        # Диалог мог быть остановлен за время задержки.
        dialog = _get_dialog_by_id(dialog.id) or dialog
        if dialog.status != "active":
            return

        try:
            reply, tokens = await _generate_ai_reply(dialog)
            model = config("AI_MODEL", default="gpt-4o-mini").strip()
        except Exception as exc:
            logger.error(f"[AI DM] OpenAI error for dialog={dialog.id}, user={sender.id}: {exc}")
            if _truthy("AI_FALLBACK_REPLY_ENABLED", "true"):
                reply = config(
                    "AI_FALLBACK_REPLY",
                    default="Понял тебя. А ты сам торгуешь или больше смотришь идеи со стороны?",
                ).strip()
                tokens = 0
                model = "fallback"
            else:
                return

        if ai_dry_run():
            logger.info(f"[AI DM DRY RUN] user={sender.id}: {reply}")
            _save_message(
                dialog.id,
                "system",
                f"[DRY RUN draft] {reply}",
                provider="dry_run",
                model=model,
                tokens_used=tokens,
            )
            return

        sent = await _safe_send_message(client, sender, reply, "ai_reply")
        if not sent:
            _set_dialog_status(dialog.id, "send_error", "telegram_send_failed")
            return

        _save_message(
            dialog.id,
            "outgoing",
            reply,
            provider="openai" if model != "fallback" else "local",
            model=model,
            tokens_used=tokens,
        )
        _mark_outgoing(dialog.id)
        next_stage = _guess_next_stage(text, reply, dialog.stage)
        _set_stage(dialog.id, next_stage)
        logger.info(f"[AI DM] reply sent: dialog={dialog.id}, user={sender.id}, stage={next_stage}")


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
