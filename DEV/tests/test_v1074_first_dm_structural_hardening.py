"""v1.0.74 structural First DM hardening."""

from __future__ import annotations

import asyncio
import importlib
import os

import pytest


BAD_MAGNETS = [
    "Привет, сигнал часто вход или цена поздно?",
    "Привет, твоя помощь по сигналам часто нужна?",
    "Привет, ты торг часто или вход бывает?",
    "Привет, ты сам или сигналы чаще вход?",
    "Привет, канал часто или вход уже поздно?",
    "Привет, движение вход вручную отслеживаешь?",
    "Привет, помощь по сигналам тебе нужна?",
    "Привет, ты канал или сигнал чаще?",
    "Привет, цена или помощь по входу нужна?",
    "Привет, можешь помочь с сигналами часто?",
    "Привет, нужна помощь или вход поздно?",
    "Привет, сигналы помощь вручную или софт?",
    "Привет, вход уведомление часто или канал?",
    "Привет, торговля сигнал цена бывает часто?",
    "Привет, ты сам вход или движение сигнал?",
    "Привет, канал вручную цена или фьючи?",
    "Привет, стоп сигнал помощь часто нужна?",
    "Привет, можно помощь по входу спросить?",
    "Привет, сигнал или твоя помощь чаще?",
    "Привет, фьючи помощь или спот нужен?",
    "Привет, вход бывает сигнал вручную часто?",
    "Привет, ты уведомление или цена поздно?",
    "Привет, помощь в крипте тебе часто нужна?",
    "Привет, можешь сигнал помощь показать?",
    "Привет, анализ вход цена или часто?",
    "Привет, движение вручную или помощь нужна?",
    "Привет, ты торгуешь помощь или сигналы?",
    "Привет, цена ушла или помощь пришла?",
    "Привет, уведомление помощь часто приходит?",
    "Привет, вход сигнал или канал вручную?",
    "Привет, крипта помощь или фьючи чаще?",
    "Привет, ты часто цена сигнал вход?",
    "Привет, бывает помощь после движения?",
    "Привет, канал сигнал помощь или цена?",
    "Привет, сигнал часто помощь замечаешь?",
    "Привет, вход поздно помощь нужна тебе?",
    "Привет, сам анализ или помощь сигналы?",
    "Привет, уведомления помощь вовремя приходят?",
    "Привет, спот помощь или фьючи выбираешь?",
    "Привет, входы помощь вручную отслеживаешь?",
    "Привет, цена сигнал часто проверяешь или?",
    "Привет, движение или вход помощь бывает?",
    "Привет, сигналов помощь много или мало?",
    "Привет, ты помощь по крипте отслеживаешь?",
    "Привет, канал поздно или вход помощь?",
    "Привет, анализ помощь или сигнал готовый?",
    "Привет, стоп помощь или вход пропускаешь?",
    "Привет, импульс помощь или цена поздно?",
    "Привет, торговые уведомления помощь проверяешь?",
    "Привет, сигнал помощь после движения видишь?",
]


def test_all_adversarial_keyword_soup_is_rejected(app_env):
    from services.ai_first_dm import validate_first_dm

    assert len(BAD_MAGNETS) >= 50
    for text in BAD_MAGNETS:
        ok, reason = validate_first_dm(text, style="magnet")
        assert not ok, (reason, text)


def test_only_reviewed_magnet_structures_are_accepted(app_env):
    from services.ai_first_dm import validate_first_dm
    from texts.first_dm import MAGNET_FIRST_DM_TEMPLATES

    approved = set(MAGNET_FIRST_DM_TEMPLATES)
    for text in approved:
        assert validate_first_dm(text, style="magnet")[0], text
    assert not validate_first_dm(
        "Привет, часто видишь хороший сигнал уже после движения?",
        style="magnet",
    )[0]


def test_help_is_forbidden_in_every_form(app_env):
    from services.ai_first_dm import validate_first_dm

    variants = [
        "Привет, помощь по сигналам тебе нужна?",
        "Привет, поможешь выбрать спот или фьючи?",
        "Привет, можешь помочь с поздним входом?",
        "Привет, помоги понять, сигнал уже поздно?",
    ]
    for text in variants:
        assert validate_first_dm(text, style="magnet")[1] == "help_topic_forbidden"


def test_local_fallback_never_weakens_similarity(app_env, monkeypatch):
    from services import ai_first_dm
    from texts.first_dm import MAGNET_FIRST_DM_TEMPLATES

    monkeypatch.setattr(ai_first_dm.random, "shuffle", lambda seq: None)
    monkeypatch.setattr(ai_first_dm, "_too_similar_recent", lambda *args, **kwargs: True)
    with pytest.raises(ai_first_dm.FirstDMUnavailableError):
        ai_first_dm._local_first_dm(MAGNET_FIRST_DM_TEMPLATES[:20], style="magnet")


def test_invalid_first_dm_style_fails_closed(app_env, monkeypatch):
    monkeypatch.setenv("FIRST_DM_STYLE", "legasy")
    import config

    with pytest.raises(RuntimeError, match="FIRST_DM_STYLE"):
        importlib.reload(config)
    monkeypatch.setenv("FIRST_DM_STYLE", "magnet")
    importlib.reload(config)


def test_dashboard_displays_active_first_dm_style(app_env):
    from handlers import menu

    text = menu._dashboard_text()
    assert "First DM: `magnet`" in text
