"""v1.0.76 selectable short_hook First DM mode."""

from __future__ import annotations

from pathlib import Path


def test_short_hook_mode_contract(app_env):
    from services import ai_first_dm
    from texts.first_dm import SHORT_HOOK_FIRST_DM_TEMPLATES, templates_for_style

    random_state = ai_first_dm.random.getstate()
    try:
        assert len(SHORT_HOOK_FIRST_DM_TEMPLATES) >= 40
        assert templates_for_style("short_hook") is SHORT_HOOK_FIRST_DM_TEMPLATES
        assert "Привет, можешь помочь с вопросом?" in SHORT_HOOK_FIRST_DM_TEMPLATES

        help_roots = ("помощ", "помог", "помож", "выруч", "подскаж")
        for text in SHORT_HOOK_FIRST_DM_TEMPLATES:
            ok, reason = ai_first_dm.validate_first_dm(text, style="short_hook")
            assert ok, (reason, text)
            assert "\u2014" not in text and "\u2013" not in text
            if any(root in text.casefold() for root in help_roots):
                assert "вопрос" in text.casefold(), text

        rejected = [
            "Привет, можешь помочь?",
            "Слушай, поможешь?",
            "Салам, можешь подсказать?",
            "Здарова, выручишь?",
            "Привет, нужна помощь",
            "Слушай, можно твою помощь?",
            "Привет, можешь помочь с одной задачей?",
        ]
        for text in rejected:
            assert not ai_first_dm.validate_first_dm(text, style="short_hook")[0], text

        recent = SHORT_HOOK_FIRST_DM_TEMPLATES[:20]
        next_text = ai_first_dm._local_first_dm(recent, style="short_hook")
        assert next_text not in recent
        assert not ai_first_dm._too_similar_recent(next_text, recent)

        test_file = Path(__file__).resolve()
        project_root = test_file.parents[1]
        if not (project_root / "config.py").exists():
            project_root = test_file.parents[2]
        config_source = (project_root / "config.py").read_text(encoding="utf-8")
        assert '{"magnet", "short_hook", "legacy"}' in config_source
    finally:
        ai_first_dm.random.setstate(random_state)
