"""Selectable first-DM module: AI generates a unique opening message.

This module only chooses the text of the first private message. Recipient
selection, queueing, pacing and PeerFlood handling stay outside this file.

Rules:
- one short message;
- no invite links;
- no profit / winrate promises;
- anti-repeat against recent openings;
- local template fallback when OpenAI is unavailable or rejects validation.
"""

from __future__ import annotations

import datetime as dt
import random
import re
import threading
from typing import Any, Optional

from decouple import config
from loguru import logger

from config import conn
from services.first_message import SOFT_TEMPLATES, TRADING_TEMPLATES

MODULE_ID = "ai_first_dm"
MODULE_LABEL = "🤖 AI Первый DM"

RECENT_WINDOW = 40
MAX_ATTEMPTS = 3
MAX_CHARS = 180
MIN_CHARS = 12

_db_lock = threading.RLock()

_FORBIDDEN = (
    "http://",
    "https://",
    "t.me/",
    "telegram.me/",
    "заработ",
    "прибыл",
    "винрейт",
    "winrate",
    "гарант",
    "100%",
    "100 %",
    "без риска",
    "легкие деньги",
    "лёгкие деньги",
    "подписывай",
    "жми на ссыл",
    "vip бесплат",
    "слив",
)

_LOCAL_FALLBACK = tuple(
    dict.fromkeys(
        list(SOFT_TEMPLATES)
        + list(TRADING_TEMPLATES)
        + [
            "Привет 👋 Увидел тебя в чате — можно один быстрый вопрос?",
            "Салют. Не отвлекаю? Хотел спросить кое-что по теме чата.",
            "Привет. Видел твоё сообщение — интересно твое мнение на секунду.",
            "Здорова 👋 Ты по теме чата или просто читаешь ленту?",
            "Привет, можно коротко? Есть мысль по тому, что в чате обсуждают.",
            "Эй, привет. Ты сейчас в теме чата — можно уточнить одну вещь?",
            "Привет 👋 Не для спама: вопрос на 10 секунд по чату.",
            "Салют. Если не занят — хотел спросить, ты сам торгуешь или больше смотришь?",
            "Привет. Увидел тебя в чате и подумал, что тебе может быть близка одна тема.",
            "Здорова. Можно на минуту? Без воды, просто короткий вопрос.",
        ]
    )
)


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _normalize(text: str) -> str:
    value = (text or "").lower().replace("ё", "е")
    value = re.sub(r"https?://\S+", " ", value)
    value = re.sub(r"[^a-zа-я0-9 ]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def ensure_ai_first_dm_history_table() -> None:
    with _db_lock, conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_first_dm_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_text TEXT NOT NULL,
                normalized TEXT NOT NULL,
                source TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_ai_first_dm_history_created
            ON ai_first_dm_history(created_at DESC)
            """
        )


def _recent_normalized(limit: int = RECENT_WINDOW) -> list[str]:
    ensure_ai_first_dm_history_table()
    rows = conn.execute(
        """
        SELECT normalized FROM ai_first_dm_history
         ORDER BY id DESC LIMIT ?
        """,
        (max(1, int(limit)),),
    ).fetchall()
    return [str(row[0]) for row in rows if row and row[0]]


def _remember(message: str, *, source: str) -> None:
    ensure_ai_first_dm_history_table()
    normalized = _normalize(message)
    if not normalized:
        return
    with _db_lock, conn:
        conn.execute(
            """
            INSERT INTO ai_first_dm_history (message_text, normalized, source, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (message.strip(), normalized, source, _now_iso()),
        )
        # Keep table bounded.
        conn.execute(
            """
            DELETE FROM ai_first_dm_history
             WHERE id NOT IN (
                SELECT id FROM (
                    SELECT id FROM ai_first_dm_history ORDER BY id DESC LIMIT 500
                )
             )
            """
        )


def _validate(message: str, recent: list[str]) -> tuple[bool, str]:
    text = " ".join((message or "").split()).strip()
    if len(text) < MIN_CHARS:
        return False, "слишком коротко"
    if len(text) > MAX_CHARS:
        return False, "слишком длинно"
    if "\n" in text:
        return False, "должен быть один абзац"
    lower = text.lower()
    if any(marker in lower for marker in _FORBIDDEN):
        return False, "запрещённая формулировка или ссылка"
    if re.search(r"https?://|t\.me/|telegram\.me/", text, re.IGNORECASE):
        return False, "содержит ссылку"
    normalized = _normalize(text)
    if not normalized:
        return False, "пусто после нормализации"
    for previous in recent:
        if normalized == previous:
            return False, "точный повтор"
        # crude similarity: shared token ratio
        a = set(normalized.split())
        b = set(previous.split())
        if a and b:
            overlap = len(a & b) / max(len(a | b), 1)
            if overlap >= 0.9 and abs(len(a) - len(b)) <= 2:
                return False, "слишком похоже на недавнее"
    return True, "ok"


def _local_fallback(recent: list[str]) -> str:
    shuffled = list(_LOCAL_FALLBACK)
    random.shuffle(shuffled)
    for candidate in shuffled:
        ok, _reason = _validate(candidate, recent)
        if ok:
            return candidate
    return random.choice(list(_LOCAL_FALLBACK))


def _build_prompt(
    *,
    source_chat_title: Optional[str],
    target_first_name: Optional[str],
    recent: list[str],
) -> tuple[str, str]:
    chat = " ".join(str(source_chat_title or "").split())[:80] or "чат по трейдингу/крипте"
    name = " ".join(str(target_first_name or "").split())[:40]
    name_hint = f"Имя собеседника (можно не использовать): {name}." if name else ""
    recent_block = "\n".join(f"- {item}" for item in recent[:12]) or "- (пусто)"
    instructions = (
        "Ты пишешь первое личное сообщение незнакомцу в Telegram от имени обычного человека. "
        "Цель — чтобы человек ЗАХОТЕЛ ответить: любопытство, лёгкий крючок, живой тон. "
        "Ровно ОДНО короткое сообщение на русском, 1–2 предложения, максимум ~160 символов. "
        "Без ссылок, без рекламы, без обещаний прибыли/винрейта, без VIP, без «подпишись», "
        "без давления и без представления «я бот/менеджер». "
        "Можно опереться на то, что видел человека в чате. Вопрос лучше открытый и лёгкий."
    )
    user = (
        f"Контекст: человек писал в чате «{chat}». {name_hint}\n"
        "Задача: максимально завлекающее первое сообщение — чтобы хотелось ответить. Формула: короткое приветствие + намёк на общую тему чата + лёгкий вопрос.\n"
        "Не повторяй эти недавние начала (смысл):\n"
        f"{recent_block}\n"
        "Верни только текст сообщения, без кавычек и пояснений."
    )
    return instructions, user


async def _openai_generate(instructions: str, user_input: str) -> str:
    import asyncio

    api_key = config("OPENAI_API_KEY", default="").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not configured")
    model = config("AI_MODEL", default="gpt-4o-mini").strip() or "gpt-4o-mini"
    max_tokens = int(config("AI_FIRST_DM_MAX_TOKENS", default="120") or 120)
    max_tokens = max(40, min(max_tokens, 300))
    timeout_seconds = float(config("AI_FIRST_DM_TIMEOUT_SECONDS", default="20") or 20)
    timeout_seconds = max(5.0, min(timeout_seconds, 60.0))

    from openai import AsyncOpenAI

    client = AsyncOpenAI(api_key=api_key)

    async def _call():
        return await client.responses.create(
            model=model,
            instructions=instructions,
            input=[{"role": "user", "content": user_input}],
            max_output_tokens=max_tokens,
        )

    try:
        response = await asyncio.wait_for(_call(), timeout=timeout_seconds)
    finally:
        try:
            await client.close()
        except Exception:
            pass
    raw = getattr(response, "output_text", None)
    if not raw:
        parts: list[str] = []
        for item in getattr(response, "output", []) or []:
            for content in getattr(item, "content", []) or []:
                value = getattr(content, "text", None)
                if value:
                    parts.append(str(value))
        raw = "\n".join(parts)
    text = " ".join(str(raw or "").split()).strip().strip('"“”«»')
    return text


async def choose_ai_generated_first_dm_text(
    *,
    source_chat_title: Optional[str] = None,
    target_first_name: Optional[str] = None,
) -> str:
    """Return one validated first-DM text; never raises to the send pipeline."""
    recent = _recent_normalized()
    last_error = "unknown"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            instructions, user_input = _build_prompt(
                source_chat_title=source_chat_title,
                target_first_name=target_first_name,
                recent=recent,
            )
            generated = await _openai_generate(instructions, user_input)
            ok, reason = _validate(generated, recent)
            if ok:
                _remember(generated, source="openai")
                logger.info(
                    f"[AI first DM] generated ok attempt={attempt} chars={len(generated)}"
                )
                return generated
            last_error = reason
            logger.info(
                f"[AI first DM] rejected attempt={attempt}: {reason}"
            )
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            logger.warning(f"[AI first DM] generation failed attempt={attempt}: {exc}")

    fallback = _local_fallback(recent)
    if not (fallback or "").strip():
        fallback = "Привет 👋 Можно короткий вопрос?"
    _remember(fallback, source="local_fallback")
    logger.warning(
        f"[AI first DM] using local fallback after failures ({last_error})"
    )
    return fallback


def choose_ai_generated_first_dm_text_sync(**kwargs: Any) -> str:
    """Sync helper for tests: local fallback only."""
    recent = _recent_normalized()
    text = _local_fallback(recent)
    _remember(text, source="local_sync")
    return text


__all__ = [
    "MODULE_ID",
    "MODULE_LABEL",
    "choose_ai_generated_first_dm_text",
    "choose_ai_generated_first_dm_text_sync",
    "ensure_ai_first_dm_history_table",
]
