"""First DM validator and fallback."""

from __future__ import annotations

import asyncio


def test_validator_rules(app_env):
    from services.ai_first_dm import sanitize_dashes, validate_first_dm

    assert validate_first_dm("Привет, часто сигнал замечаешь уже после движения?")[0]
    assert validate_first_dm("Привет, можно спросить?", style="legacy")[0]
    assert not validate_first_dm("Смотри https://t.me/x")[0]
    assert not validate_first_dm("Привет \u2014 можно?")[0]
    ok, _ = validate_first_dm(sanitize_dashes("Привет \u2014 можно спросить?"), style="legacy")
    assert ok
    assert not validate_first_dm("Какие фильмы любишь?")[0]
    assert not validate_first_dm("")[0]


def test_fallback_generate(app_env):
    from services.ai_first_dm import generate_first_dm, validate_first_dm

    text = asyncio.run(generate_first_dm())
    assert validate_first_dm(text)[0]
    assert "http" not in text.lower()
