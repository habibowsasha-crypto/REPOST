"""Local first-DM templates (no links). Fallback when AI is off or fails."""

from __future__ import annotations

import random
import re

# Human-sounding mix. Soft hooks get weight for reply rate.
FIRST_DM_TEMPLATES: list[str] = [
    # soft — easy to answer
    "Можно спросить?",
    "Не отвлеку?",
    "Есть секунда?",
    "Можно на минуту?",
    "Привет, не занят?",
    "Хотел спросить одну вещь",
    "Можно коротко?",
    "Не помешаю?",
    # signals / self
    "Ты сам торгуешь или по сигналам?",
    "А ты по сигналам или сам смотришь?",
    "На сигналах сидишь или сам?",
    "Сам анализ или сигналы ловишь?",
    # active
    "А ты щас торгуешь?",
    "В рынке сейчас или на паузе?",
    "Щас торгуешь ещё?",
    "Ты вообще сейчас в рынке?",
    # newbie / help (people answer to help)
    "Новичку не подскажешь?",
    "Можно глупый вопрос по трейду?",
    "Подскажи одну вещь по рынку?",
    "А не подскажешь новичку?",
]

_BUCKETS: dict[str, re.Pattern[str]] = {
    "signals": re.compile(r"сигнал|сам\s+торг|сам\s+смотр|сам\s+анализ", re.I),
    "losses": re.compile(r"убыт|потеря|слил|минус", re.I),
    "strategy": re.compile(r"стратег", re.I),
    "active": re.compile(r"торгу|рынк|паузе|отош", re.I),
    "newbie": re.compile(r"новичк|подскаж|помо", re.I),
    "soft": re.compile(
        r"можно\s+(спросить|коротко|на\s+минут)|есть\s+(секунд|минут)|"
        r"не\s+отвлек|не\s+помеш|не\s+занят|хотел\s+спросить",
        re.I,
    ),
}


def _bucket(text: str) -> str:
    for name, rx in _BUCKETS.items():
        if rx.search(text or ""):
            return name
    return "other"


def _opener(text: str) -> str:
    t = (text or "").strip().lower()
    for p in ("слушай", "эй", "привет", "салют", "кстати"):
        if t.startswith(p):
            return p
    return (t.split() or [""])[0]


def pick_first_dm(*, recent: list[str] | None = None) -> str:
    """Pick template avoiding recent exact text, buckets and same opener."""
    recent = recent or []
    recent_set = set(recent)
    recent_buckets = {_bucket(t) for t in recent[:8]}
    recent_openers = {_opener(t) for t in recent[:6]}

    pool = [t for t in FIRST_DM_TEMPLATES if t not in recent_set]
    if not pool:
        pool = list(FIRST_DM_TEMPLATES)

    diversified = [t for t in pool if _bucket(t) not in recent_buckets]
    if diversified:
        pool = diversified

    alt = [t for t in pool if _opener(t) not in recent_openers]
    if alt:
        pool = alt
    elif "слушай" in recent_openers:
        alt2 = [t for t in pool if _opener(t) != "слушай"]
        if alt2:
            pool = alt2

    # mild bias to soft if none in last 3 (reply rate)
    last_b = {_bucket(x) for x in recent[:3]}
    if "soft" not in last_b:
        soft_pool = [t for t in pool if _bucket(t) == "soft"]
        if soft_pool and random.random() < 0.45:
            return random.choice(soft_pool)

    return random.choice(pool)
