"""Local first-DM templates (no links). Fallback when AI is off or fails."""

from __future__ import annotations

import random

# Short trading / soft hooks. No channel link, no hard pitch.
FIRST_DM_TEMPLATES: list[str] = [
    "Слушай, а ты щас торгуешь?",
    "А ты сам торгуешь или по сигналам?",
    "А не подскажешь новичку?",
    "По какой стратегии ты торгуешь?",
    "Слушай, а ты много потерял в трейдинге? Можно спросить",
    "Ты сейчас в рынке или на паузе?",
    "Сам сделки открываешь или по сигналам идёшь?",
    "Есть минута? Хотел спросить по трейду",
    "Слушай, ты вообще в трейдинг ещё?",
    "А ты по своей стратегии или чужие сигналы берёшь?",
    "Новичку не подскажешь по трейду?",
    "Щас торгуешь или уже отошёл?",
]


def pick_first_dm(*, recent: list[str] | None = None) -> str:
    """Pick a random template, avoiding recent exact matches when possible."""
    recent = recent or []
    recent_set = set(recent)
    pool = [t for t in FIRST_DM_TEMPLATES if t not in recent_set]
    if not pool:
        pool = list(FIRST_DM_TEMPLATES)
    return random.choice(pool)
