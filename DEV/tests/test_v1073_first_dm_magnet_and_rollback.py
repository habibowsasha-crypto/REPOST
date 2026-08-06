"""v1.0.73 First DM magnet system with preserved legacy rollback."""

from __future__ import annotations

import asyncio


def test_default_mode_is_magnet(app_env):
    import config

    assert config.FIRST_DM_STYLE == "magnet"


def test_magnet_examples_are_concrete_and_answerable(app_env):
    from services.ai_first_dm import validate_first_dm
    from texts.first_dm import MAGNET_FIRST_DM_TEMPLATES

    assert len(MAGNET_FIRST_DM_TEMPLATES) >= 30
    for text in MAGNET_FIRST_DM_TEMPLATES:
        ok, reason = validate_first_dm(text, style="magnet")
        assert ok, (reason, text)
        assert text.startswith("Привет,")
        assert text.endswith("?")
        assert "\u2014" not in text and "\u2013" not in text


def test_magnet_rejects_empty_and_broken_hooks(app_env):
    from services.ai_first_dm import validate_first_dm

    rejected = [
        "Привет, нужна твоя помощь?",
        "Привет, можно один вопрос?",
        "Привет, можно немного поговорить?",
        "Привет, можно пару слов?",
        "Привет, могу спросить что-то?",
        "Привет, нужна моя помощь?",
    ]
    for text in rejected:
        assert not validate_first_dm(text, style="magnet")[0], text


def test_legacy_pool_is_preserved_and_valid(app_env):
    from services.ai_first_dm import validate_first_dm
    from texts.first_dm import LEGACY_FIRST_DM_TEMPLATES

    assert len(LEGACY_FIRST_DM_TEMPLATES) >= 50
    assert "Привет, можно один вопрос?" in LEGACY_FIRST_DM_TEMPLATES
    assert "Привет, можно спросить?" in LEGACY_FIRST_DM_TEMPLATES
    for text in LEGACY_FIRST_DM_TEMPLATES:
        assert validate_first_dm(text, style="legacy")[0], text


def test_local_generation_uses_magnet_by_default(app_env):
    from services.ai_first_dm import generate_first_dm, validate_first_dm

    text = asyncio.run(generate_first_dm())
    assert validate_first_dm(text, style="magnet")[0]


def test_pick_can_rollback_without_old_zip(app_env):
    from texts.first_dm import pick_first_dm

    text = pick_first_dm(style="legacy")
    assert text
    assert "сигнал" not in text.casefold()
    assert "вход" not in text.casefold()
