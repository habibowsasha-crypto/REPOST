"""First-DM templates for the optional «VIP Кирилла» module.

This module only chooses text. Recipient eligibility, queueing, pacing and the
Telegram send path remain in the existing DM runtime.
"""

from __future__ import annotations

import random


MODULE_ID = "kirill_vip"
MODULE_LABEL = "👑 VIP Кирилла"

KIRILL_VIP_TEMPLATES: tuple[str, ...] = (
    "Слушай, у тебя VIP Кирилла есть?",
    "Ты VIPку Кирилла покупал?",
    "А ты сидишь в платном канале Кирилла?",
    "У тебя есть доступ в VIP Кирилла?",
    "Слушай, ты VIP Кирилла когда-нибудь брал?",
    "Ты только бесплатный канал Кирилла смотришь или VIP тоже есть?",
    "Ты покупал закрытый VIP-канал Кирилла?",
    "Как тебе вообще VIP Кирилла, есть доступ?",
    "Ты давно за Кириллом следишь? Его VIP брал?",
    "У тебя платка Кирилла есть?",
    "Ты сейчас пользуешься VIP Кирилла?",
    "Слушай, хотел спросить: VIP Кирилла у тебя есть?",
    "Ты случайно не покупал VIP Кирилла?",
    "Ты больше по бесплатному каналу Кирилла или по VIP?",
    "А сколько сейчас VIP Кирилла стоит, не знаешь?",
    "Слушай, а VIP Кирилла своих денег стоит?",
    "Ты когда-нибудь торговал по VIP-сигналам Кирилла?",
    "У Кирилла VIP реально отличается от бесплатного канала?",
    "Ты платный канал Кирилла давно смотришь?",
    "Слушай, у тебя случайно VIPки Кирилла нет?",
    "Ты фанат Кирилла или просто сигналы смотришь? 😄",
    "Ты в VIP Кирилла когда-нибудь вступал?",
    "А ты бы стал покупать VIP Кирилла?",
    "Слушай, VIPку Кирилла не брал?",
)


def choose_kirill_vip_first_dm_text() -> str:
    """Return one natural first question from the dedicated template pool."""
    return random.choice(KIRILL_VIP_TEMPLATES)


def get_kirill_vip_templates_preview(limit: int = 25) -> list[str]:
    return list(KIRILL_VIP_TEMPLATES[: max(0, int(limit))])
