"""First-message selector for the optional reactive AI quick-offer module."""

from __future__ import annotations

import random

from services.first_message import SOFT_TEMPLATES, TRADING_TEMPLATES, VIP_TEMPLATES


MODULE_ID = "ai_quick_offer"
MODULE_LABEL = "🤖 AI Быстрый оффер"

AI_QUICK_OFFER_FIRST_DM_TEMPLATES: tuple[str, ...] = tuple(
    SOFT_TEMPLATES + TRADING_TEMPLATES + VIP_TEMPLATES
)


def choose_ai_quick_offer_first_dm_text() -> str:
    """Reuse the approved first-DM phrase pool without changing its module."""
    return random.choice(AI_QUICK_OFFER_FIRST_DM_TEMPLATES)
