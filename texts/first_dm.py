"""Local first-DM templates (no links). Fallback when AI is off or fails."""

from __future__ import annotations

import random
import re

# Human-sounding mix. Soft hooks get weight for reply rate.
FIRST_DM_TEMPLATES: list[str] = [
    # soft
    "Не отвлеку коротким вопросом?",
    "Можно одну вещь спросить?",
    "Не помешаю на секунду?",
    "Можно коротко по рынку?",
    # signals and activity
    "Ты сам торгуешь или по сигналам?",
    "Сам смотришь график или по сигналам?",
    "Сейчас торгуешь или пока наблюдаешь?",
    "Ты сейчас в рынке или на паузе?",
    # market view and watchlist
    "Как рынок сейчас видишь?",
    "Что сейчас больше смотришь - биток или альты?",
    "На что сейчас смотришь по рынку?",
    "Сейчас больше крипту или золото смотришь?",
    # analysis source and format
    "Сам график разбираешь или идеи смотришь?",
    "Сам анализируешь или чужие идеи смотришь?",
    "Короткий разбор или подробный удобнее?",
    "Какой формат разбора тебе удобнее?",
    # timeframe
    "Какой таймфрейм чаще смотришь?",
    "На каком ТФ обычно рынок смотришь?",
    "Больше младший ТФ смотришь или старший?",
    "На каком таймфрейме чаще анализируешь?",
]

_BUCKETS: dict[str, re.Pattern[str]] = {
    "signals": re.compile(r"сигнал|сам\s+торг|сам\s+смотр|сам\s+анализ", re.I),
    "losses": re.compile(r"убыт|потеря|слил|минус", re.I),
    "strategy": re.compile(r"стратег", re.I),
    "active": re.compile(r"торгу|рынк|паузе|отош", re.I),
    "market_view": re.compile(r"как\s+рынок|рынок\s+сейчас\s+вид", re.I),
    "watchlist": re.compile(r"биток|альт|крипт|золот|что\s+сейчас\s+больше", re.I),
    "analysis_source": re.compile(r"сам\s+(график|анализ)|идеи\s+смотр", re.I),
    "format": re.compile(r"формат\s+разбора|короткий\s+разбор|подроб", re.I),
    "timeframe": re.compile(r"таймфрейм|\bтф\b", re.I),
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
