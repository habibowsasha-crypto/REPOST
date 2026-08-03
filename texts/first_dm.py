"""Local first-DM templates (no links). Fallback when AI is off or fails."""

from __future__ import annotations

import random
import re

# Mix: soft permission + light trading. No channel link, no hard pitch.
FIRST_DM_TEMPLATES: list[str] = [
    # soft / simple
    "Можно спросить?",
    "Привет, можно на секунду?",
    "Есть минутка? Хотел спросить",
    "Не отвлекаю? Можно вопрос",
    "А можешь помочь новичку?",
    "Есть вопрос про трейдинг, можно?",
    "Можно коротко спросить?",
    "Привет, не помешаю?",
    "Салют, есть секунда?",
    # trading light
    "Слушай, а ты щас торгуешь?",
    "А ты сам торгуешь или по сигналам?",
    "Новичку не подскажешь по трейду?",
    "По какой стратегии ты торгуешь?",
    "Ты сейчас в рынке или на паузе?",
    "Щас торгуешь или уже отошёл?",
    "Слушай, ты вообще в трейдинг ещё?",
    "Есть минута? Хотел спросить по трейду",
]

# Semantic buckets — avoid repeating same bucket in a row when possible
_BUCKETS: dict[str, re.Pattern[str]] = {
    "signals": re.compile(r"сигнал", re.I),
    "losses": re.compile(r"убыт|потеря|слил|минус", re.I),
    "strategy": re.compile(r"стратег", re.I),
    "active": re.compile(r"торгу|рынк|паузе|отош", re.I),
    "newbie": re.compile(r"новичк|помо", re.I),
    "soft": re.compile(r"можно\s+спросить|есть\s+(секунд|минут)|не\s+отвлек|не\s+помеш", re.I),
}


def _bucket(text: str) -> str:
    for name, rx in _BUCKETS.items():
        if rx.search(text or ""):
            return name
    return "other"


def pick_first_dm(*, recent: list[str] | None = None) -> str:
    """Pick template avoiding recent exact text and recent semantic buckets."""
    recent = recent or []
    recent_set = set(recent)
    recent_buckets = {_bucket(t) for t in recent[:8]}

    pool = [t for t in FIRST_DM_TEMPLATES if t not in recent_set]
    if not pool:
        pool = list(FIRST_DM_TEMPLATES)

    diversified = [t for t in pool if _bucket(t) not in recent_buckets]
    if diversified:
        pool = diversified
    return random.choice(pool)
