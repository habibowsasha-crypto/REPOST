"""Random first DM message templates.

This module does not decide who receives a DM. It only chooses the text used
for the first message when an existing DM task is about to send a message.
"""

from __future__ import annotations

import os
import random
from functools import lru_cache
from typing import Iterable, List

from decouple import config
from loguru import logger


SOFT_TEMPLATES: list[str] = [
    "Привет 👋 Ты сейчас не занят?",
    "Привет, можно короткий вопрос?",
    "Салют 👋 Ты по крипте давно?",
    "Привет, не отвлекаю?",
    "Здорова, можно пару слов?",
    "Привет 👋 Ты сейчас на связи?",
    "Привет. Можно вопрос по теме чата?",
    "Слушай, привет 👋 Можно на минуту?",
]

TRADING_TEMPLATES: list[str] = [
    "Привет 👋 Ты сам торгуешь или просто наблюдаешь?",
    "Слушай, ты сам трейдишь или больше изучаешь?",
    "Привет. Ты по сигналам торгуешь или чисто сам?",
    "Слушай, интересно - ты сам анализируешь рынок или смотришь идеи из каналов?",
    "Привет 👋 Ты криптой давно занимаешься?",
    "Слушай, ты больше фьючи смотришь или спот?",
]

VIP_TEMPLATES: list[str] = [
    "Привет. Ты через VIP-каналы когда-нибудь торговал?",
    "Слушай, а ты торговал через VIP-каналы?",
    "Привет 👋 У тебя вообще есть какие-нибудь VIP-каналы по трейдингу?",
    "Ты торгуешь через ВИПки или сам анализируешь?",
    "Слушай, что думаешь насчёт VIP-каналов?",
    "Привет. А ты знаешь, что такое ВИПки в трейдинге?",
    "Ты когда-нибудь покупал доступ в VIP-канал?",
    "Слушай, а у тебя есть проверенные ВИПки или не пользуешься таким?",
    "А ты вообще доверяешь VIP-каналам или скептически относишься?",
    "Слушай, интересно твоё мнение - ВИПки по крипте реально помогают или больше мусор?",
    "Ты пробовал когда-нибудь торговать по сигналам из закрытых каналов?",
    "Слушай, а тебе вообще интересны закрытые крипто-каналы или не твоя тема?",
]

BUILTIN_BY_CATEGORY: dict[str, list[str]] = {
    "soft": SOFT_TEMPLATES,
    "trading": TRADING_TEMPLATES,
    "vip": VIP_TEMPLATES,
}


def _split_templates(raw: str | None) -> list[str]:
    if not raw:
        return []
    # Поддерживаем два формата: через | или построчно.
    raw = raw.replace("\r\n", "\n")
    chunks: Iterable[str]
    if "|" in raw:
        chunks = raw.split("|")
    else:
        chunks = raw.split("\n")
    return [x.strip() for x in chunks if x.strip() and not x.strip().startswith("#")]


@lru_cache(maxsize=1)
def _load_file_templates() -> list[str]:
    path = config("FIRST_DM_TEMPLATES_FILE", default="").strip()
    if not path:
        return []
    if not os.path.exists(path):
        logger.warning(f"FIRST_DM_TEMPLATES_FILE задан, но файл не найден: {path}")
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return _split_templates(f.read())
    except Exception as exc:
        logger.error(f"Не удалось прочитать FIRST_DM_TEMPLATES_FILE={path}: {exc}")
        return []


def reload_first_dm_templates_cache() -> None:
    """Позволяет перечитать файл шаблонов без рестарта процесса."""
    _load_file_templates.cache_clear()


def _env_templates() -> list[str]:
    return _split_templates(config("FIRST_DM_TEMPLATES", default=""))


def _builtin_templates(category: str) -> list[str]:
    category = (category or "all").strip().lower()
    if category in BUILTIN_BY_CATEGORY:
        return list(BUILTIN_BY_CATEGORY[category])

    # category=all: мягкие + трейдинг + VIP с настраиваемой долей VIP.
    try:
        vip_weight = int(config("FIRST_DM_VIP_WEIGHT_PERCENT", default="70"))
    except (TypeError, ValueError):
        vip_weight = 70
    vip_weight = max(0, min(100, vip_weight))
    if random.randint(1, 100) <= vip_weight:
        return list(VIP_TEMPLATES)
    return list(SOFT_TEMPLATES + TRADING_TEMPLATES)


def is_random_first_dm_enabled() -> bool:
    mode = config("FIRST_DM_MODE", default="").strip().lower()
    flag = config("DM_FIRST_MESSAGE_RANDOM", default="false").strip().lower()
    return mode == "random" or flag in {"1", "true", "yes", "on", "y", "да"}


def choose_first_dm_text(fallback_text: str = "") -> str:
    """Выбирает первое сообщение.

    Приоритет:
    1) FIRST_DM_TEMPLATES_FILE
    2) FIRST_DM_TEMPLATES
    3) встроенные шаблоны по FIRST_DM_CATEGORY
    4) fallback_text из старого /dm_post
    """
    if not is_random_first_dm_enabled():
        return fallback_text

    category = config("FIRST_DM_CATEGORY", default="all").strip().lower()
    templates: List[str] = []
    templates.extend(_load_file_templates())
    templates.extend(_env_templates())

    if not templates:
        templates = _builtin_templates(category)

    if not templates:
        return fallback_text

    return random.choice(templates).strip() or fallback_text


def get_templates_preview(limit: int = 25) -> list[str]:
    templates = _load_file_templates() or _env_templates()
    if not templates:
        category = config("FIRST_DM_CATEGORY", default="all").strip().lower()
        if category == "all":
            templates = SOFT_TEMPLATES + TRADING_TEMPLATES + VIP_TEMPLATES
        else:
            templates = _builtin_templates(category)
    return templates[:limit]
