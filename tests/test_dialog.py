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
