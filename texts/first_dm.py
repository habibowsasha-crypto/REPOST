"""Local first-DM templates (no links). Fallback when AI is off or fails."""

from __future__ import annotations

import random

# Short hooks only - user should answer easily. No channel link.
FIRST_DM_TEMPLATES: list[str] = [
    "Привет, можно спросить?",
    "Салют, не отвлекаю?",
    "Привет, есть секунда?",
    "Можно короткий вопрос?",
    "Привет, не помешаю?",
    "Хей, можно спросить кое-что?",
    "Привет, ты не против если спрошу?",
    "Салют, есть минутка?",
    "Привет, хотел спросить одну вещь",
    "Можно уточнить кое-что?",
    "Привет, не отвлекаю сильно?",
    "Салют, можно на секунду?",
]


def pick_first_dm(*, recent: list[str] | None = None) -> str:
    """Pick a random template, avoiding recent exact matches when possible."""
    recent = recent or []
    recent_set = set(recent)
    pool = [t for t in FIRST_DM_TEMPLATES if t not in recent_set]
    if not pool:
        pool = list(FIRST_DM_TEMPLATES)
    return random.choice(pool)
