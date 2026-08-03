"""Dialog stop detection and store stages."""

from __future__ import annotations

import datetime as dt


def test_stop_detection(app_env):
    from services import ai_dialog

    assert ai_dialog.is_hard_stop("Не пиши мне больше")
    assert ai_dialog.is_hard_stop("отстань")
    assert ai_dialog.is_soft_decline("неинтересно")
    assert not ai_dialog.is_hard_stop("ну давай")


def test_dialog_stages_and_due(app_env):
    from services import dialog_store as ds

    ds.create_after_first_dm(100, 1, "Можно спросить?")
    d = ds.get_dialog(100)
    assert d["stage"] == ds.STAGE_WAITING_REPLY
    assert int(d["outgoing_count"]) == 1

    past = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=10)).isoformat()
    ds.set_stage(100, ds.STAGE_EXPLAINED, auto_link_at=past)
    due = ds.list_due_auto_links()
    assert any(int(x["target_user_id"]) == 100 for x in due)

    assert ds.count_active() >= 1


def test_explain_without_link_fallback(app_env):
    import asyncio

    from services import ai_dialog

    text = asyncio.run(
        ai_dialog.generate_explain(
            [
                {"role": "assistant", "text": "Можно спросить?"},
                {"role": "user", "text": "ну"},
            ]
        )
    )
    assert "t.me" not in text.lower()
    assert "http" not in text.lower()


def test_link_wrap_contains_channel(app_env):
    import asyncio

    from services import ai_dialog

    text = asyncio.run(
        ai_dialog.generate_link_wrap([{"role": "assistant", "text": "explain"}])
    )
    assert "t.me" in text


def test_explain_branches(app_env):
    from services.ai_dialog import classify_user_reply, _EXPLAIN_BY_BRANCH

    assert classify_user_reply("сам торгую") == "self"
    assert classify_user_reply("иду по сигналам") == "signals"
    assert classify_user_reply("я новичок еще") == "newbie"
    assert classify_user_reply("хз") == "other"
    for b, texts in _EXPLAIN_BY_BRANCH.items():
        assert texts
        for x in texts:
            assert "http" not in x.lower()
            assert "t.me" not in x.lower()


def test_followup_due_after_silence(app_env):
    import datetime as dt
    from services import dialog_store as ds

    ds.create_after_first_dm(200, 1, "Можно спросить?")
    d = ds.get_dialog(200)
    assert d["stage"] == ds.STAGE_WAITING_REPLY
    assert d.get("auto_link_at")  # follow-up deadline
    # Force due
    past = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=1)).isoformat()
    ds.set_stage(200, ds.STAGE_WAITING_REPLY, auto_link_at=past)
    due = ds.list_due_followups()
    assert any(int(x["target_user_id"]) == 200 for x in due)
    assert ds.MAX_OUTGOING == 5


def test_followup_text_no_channel(app_env):
    from services import ai_dialog

    for _ in range(10):
        text = ai_dialog.followup_silence_text()
        assert "t.me" not in text.lower()
        assert "http" not in text.lower()
        assert "канал" not in text.lower()


def test_followup_stage_in_engine_source(app_env):
    from pathlib import Path
    src = Path("services/dialog_engine.py").read_text()
    assert "STAGE_FOLLOWUP_SENT" in src
    assert "process_due_followups" in src
