"""Deterministic dialog tree for the optional «VIP Кирилла» first-DM module.

The module deliberately keeps the first follow-up tightly connected to the
opening question: it explains that Максим noticed the user in Кирилл's chat,
then offers the free channel where software copies posts from Кирилл's VIP and
other paid VIP sources. Explicit opt-out is handled earlier by ai_dialog_service.
"""

from __future__ import annotations

import re
from typing import Sequence

from services.maxim_sales_funnel import (
    DEFAULT_FREE_SOURCE_COUNT,
    DEFAULT_PAID_SOURCE_COUNT,
    FunnelPlan,
    PIRATE_VIP_LINK,
    analyze_history,
    classify_intent,
    post_link_final_messages,
)


_KIRILL_SOURCE = "Я тебя в чате Кирилла увидел, поэтому и спросил."


def _normalize(text: str) -> str:
    value = (text or "").lower().replace("ё", "е")
    value = re.sub(r"https?://\S+", " ", value)
    value = re.sub(r"[^a-zа-я0-9 ]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def _looks_like_yes(text: str) -> bool:
    normalized = _normalize(text)
    if not normalized:
        return False
    negative = (
        "нет", "не покупал", "не брал", "нету", "не состою", "не сидел",
        "не было", "никогда не", "только бесплат",
    )
    if any(marker in normalized for marker in negative):
        return False
    return bool(
        re.search(
            r"\b(да|есть|покупал|купил|брал|состою|сижу|пользуюсь|имеется|был доступ)\b",
            normalized,
        )
    )


def _looks_like_no(text: str) -> bool:
    normalized = _normalize(text)
    return any(
        marker in normalized
        for marker in (
            "нет", "не покупал", "не брал", "нету", "не состою", "не сидел",
            "не было", "никогда не", "только бесплат", "дорого", "жалко денег",
            "не хочу покупать", "раньше был", "подписка закончилась", "перестал продлевать",
        )
    )


def _source_prefix() -> str:
    return _KIRILL_SOURCE


def _offer_for_owner() -> list[str]:
    other_count = max(1, DEFAULT_FREE_SOURCE_COUNT - 1)
    return [
        f"{_source_prefix()} Дорого обошлась? А ведь отдельно покупать её было необязательно — "
        f"посты из VIP Кирилла можно смотреть бесплатно. Софт моментально копирует их "
        f"в Telegram-канал вместе с постами ещё из {other_count} платных закрытых VIP-каналов.",
        f"Вот, глянь: {PIRATE_VIP_LINK}",
    ]


def _offer_for_non_owner() -> list[str]:
    other_count = max(1, DEFAULT_FREE_SOURCE_COUNT - 1)
    return [
        f"{_source_prefix()} Тогда отдельно покупать его VIP не нужно — софт моментально "
        f"копирует новые посты Кирилла в бесплатный Telegram-канал вместе с постами ещё "
        f"из {other_count} платных закрытых VIP-каналов.",
        f"Вот, глянь: {PIRATE_VIP_LINK}",
    ]


def _generic_offer() -> list[str]:
    other_count = max(1, DEFAULT_FREE_SOURCE_COUNT - 1)
    return [
        f"{_source_prefix()} Есть бесплатный Telegram-канал, куда софт моментально копирует "
        f"новые посты из VIP Кирилла и ещё {other_count} платных закрытых VIP-каналов. "
        "Каждый доступ отдельно покупать не нужно.",
        f"Вот ссылка: {PIRATE_VIP_LINK}",
    ]


def build_kirill_vip_plan(
    *,
    stage: str,
    history: Sequence[tuple[str, str]],
    source_chat_title: str | None,
    followup_count: int,
    max_followups: int,
) -> FunnelPlan:
    """Choose the pre-link branch for the dedicated Кирилл module."""
    state = analyze_history(history)
    text = state.last_user_text
    intent = classify_intent(text)

    if intent == "soft_decline":
        return FunnelPlan(
            "soft_decline", "completed", True,
            ["Понял, без проблем. Не буду навязывать."],
            model="local_kirill_vip",
        )
    if intent == "scam_suspicion":
        return FunnelPlan(
            "scam_reassurance", "scam_reassured", False,
            [
                "Тебя никто не заставляет. Можешь глянуть или просто забить. "
                "Если не хочешь, чтобы я писал, скажи — больше не напишу."
            ],
            model="local_kirill_vip",
        )
    if intent == "source_question":
        return FunnelPlan(
            "source_answer", stage or "first_dm_sent", False,
            [f"{_source_prefix()} Хотел узнать, у тебя его VIP есть или только бесплатный канал смотришь?"],
            model="local_kirill_vip",
        )
    if intent == "identity_question":
        return FunnelPlan(
            "identity_answer", stage or "first_dm_sent", False,
            [f"Я Максим. {_source_prefix()} Хотел спросить насчёт его VIP."],
            model="local_kirill_vip",
        )
    if intent == "bot_question":
        return FunnelPlan(
            "bot_answer", stage or "first_dm_sent", False,
            [
                "Часть ответов автоматизирована. Я Максим, занимаюсь привлечением людей "
                "в бесплатный Telegram-канал со сливами VIP-каналов."
            ],
            model="local_kirill_vip",
        )
    if intent == "benefit_question":
        return FunnelPlan(
            "business_model_link", "post_link_active", False,
            [
                "Я привлекаю людей в бесплатный канал и получаю за это зарплату. "
                f"Создатель позже может предложить расширенный доступ почти к {DEFAULT_PAID_SOURCE_COUNT} VIP-каналам, "
                "но покупать ничего не обязательно.",
                f"Вот бесплатный канал: {PIRATE_VIP_LINK}",
            ],
            model="local_kirill_vip",
        )
    if intent in {"ask_link", "link_access_issue"}:
        return FunnelPlan(
            "concise_link", "post_link_active", False,
            [f"Вот ссылка: {PIRATE_VIP_LINK}"],
            model="local_kirill_vip",
        )
    if intent in {"what_is_it", "payment_question", "reaction"}:
        return FunnelPlan(
            "kirill_offer", "post_link_active", False, _generic_offer(),
            model="local_kirill_vip",
        )
    if _looks_like_yes(text):
        return FunnelPlan(
            "kirill_has_vip", "post_link_active", False, _offer_for_owner(),
            model="local_kirill_vip",
        )
    if _looks_like_no(text):
        return FunnelPlan(
            "kirill_no_vip", "post_link_active", False, _offer_for_non_owner(),
            model="local_kirill_vip",
        )

    # Any ordinary first reply still advances the Кирилл funnel instead of
    # falling back to the generic VIP question tree.
    return FunnelPlan(
        "kirill_offer", "post_link_active", False, _generic_offer(),
        model="local_kirill_vip",
    )


def build_kirill_vip_post_link_plan(
    *, history: Sequence[tuple[str, str]], source_chat_title: str | None
) -> FunnelPlan:
    """Give one final Кирилл-aware answer after the invitation link."""
    state = analyze_history(history)
    text = state.last_user_text
    normalized = _normalize(text)
    intent = classify_intent(text)

    if intent == "link_access_issue":
        messages = [
            "Сверху над чатом закрой крестиком плашку «Заблокировать / Добавить». "
            "Потом нажми ссылку ещё раз. Если всё равно не откроется — скопируй "
            "ссылку и вставь её в Telegram."
        ]
    elif intent == "ask_link" or any(
        marker in normalized for marker in ("повтори ссылку", "скинь еще раз", "скинь ещё раз", "потерял ссылку")
    ):
        messages = [f"Вот ссылка ещё раз: {PIRATE_VIP_LINK}"]
    elif "официаль" in normalized or "оригиналь" in normalized:
        messages = [
            "Нет, это не официальный доступ в платный канал Кирилла. "
            "Софт копирует его новые VIP-посты в отдельный бесплатный Telegram-канал."
        ]
    elif "только кирилл" in normalized or "только кирил" in normalized:
        messages = [
            f"Нет. Там посты из VIP Кирилла и ещё {max(1, DEFAULT_FREE_SOURCE_COUNT - 1)} платных закрытых VIP-каналов."
        ]
    elif "сколько" in normalized and ("канал" in normalized or "вип" in normalized or "vip" in normalized):
        messages = [
            f"В бесплатном канале сейчас {DEFAULT_FREE_SOURCE_COUNT} VIP-источников, включая Кирилла. "
            f"В расширенной версии почти {DEFAULT_PAID_SOURCE_COUNT} русскоязычных и зарубежных каналов."
        ]
    else:
        messages = post_link_final_messages(text, source_chat_title=source_chat_title)

    return FunnelPlan(
        "post_link_final", "completed", True, messages,
        model="local_kirill_vip_post_link",
    )
