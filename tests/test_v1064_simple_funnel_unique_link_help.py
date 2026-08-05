"""v1.0.64 simple First DM, 20-message uniqueness and link-help stage."""

from __future__ import annotations

import asyncio
import datetime as dt
from types import SimpleNamespace


class _FakeClient:
    def __init__(self):
        self.sent: list[str] = []

    def is_connected(self):
        return True

    async def get_input_entity(self, target):
        return target

    async def send_message(self, entity, text):
        self.sent.append(text)
        return SimpleNamespace(
            id=5000 + len(self.sent),
            message=text,
            out=True,
            date=dt.datetime.now(dt.timezone.utc),
        )


async def _value(value):
    return value


def _patch_delivery(monkeypatch, client):
    from services import dialog_engine, monitor

    monkeypatch.setattr(monitor, "get_client", lambda account: client)
    monkeypatch.setattr(
        monitor,
        "maybe_disconnect_inactive_account",
        lambda account: _value(None),
    )
    monkeypatch.setattr(dialog_engine, "_delay_reply", lambda: 0.0)
    monkeypatch.setattr(dialog_engine, "_auto_link_delay", lambda: 60)


def _force_due(target: int, stage: str):
    from services import dialog_store

    past = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)).isoformat()
    dialog_store.set_stage(target, stage, auto_link_at=past)


def test_first_dm_rejects_old_market_questions(app_env):
    from services import ai_first_dm

    assert ai_first_dm.validate_first_dm("Привет, можно один вопрос?")[0]
    assert ai_first_dm.validate_first_dm("Привет, ты занят?")[0]
    assert not ai_first_dm.validate_first_dm("Что думаешь о движении рынка?")[0]
    assert not ai_first_dm.validate_first_dm("У тебя сейчас есть позиция?")[0]
    assert not ai_first_dm.validate_first_dm("Какой таймфрейм чаще смотришь?")[0]


def test_local_first_dm_has_no_exact_repeat_inside_twenty(app_env):
    from services import ai_first_dm, phrases

    generated: list[str] = []
    for _ in range(20):
        text = asyncio.run(ai_first_dm.generate_first_dm())
        assert ai_first_dm.validate_first_dm(text)[0]
        assert text not in generated
        generated.append(text)
        phrases.remember(phrases.KIND_FIRST_DM, text)


def test_all_four_phrase_types_keep_separate_last_twenty(app_env):
    from services import phrases

    kinds = (
        phrases.KIND_FIRST_DM,
        phrases.KIND_PROMO,
        phrases.KIND_APOLOGY,
        phrases.KIND_LINK_HELP,
    )
    for kind in kinds:
        for index in range(25):
            phrases.remember(kind, f"{kind}-{index}")
        recent = phrases.recent_texts(kind, limit=100)
        assert len(recent) == 20
        assert recent[0] == f"{kind}-24"
        assert recent[-1] == f"{kind}-5"


def test_link_help_contains_all_required_steps(app_env):
    from services import ai_dialog

    text = asyncio.run(ai_dialog.generate_link_open_help([]))
    lower = text.lower()
    assert "заблокировать" in lower
    assert "добавить" in lower
    assert "крест" in lower or "закрой" in lower or "убери" in lower
    assert "ссыл" in lower
    assert "скопируй" in lower
    assert ai_dialog._link_help_ok(text)
    assert "http" not in lower and "t.me" not in lower



def test_local_generators_avoid_exact_repeat_inside_twenty(app_env):
    from services import ai_dialog, phrases

    cases = (
        (
            phrases.KIND_PROMO,
            lambda: ai_dialog.generate_promo([], category=ai_dialog.CATEGORY_NORMAL),
            lambda value: ai_dialog._promo_ok(value.rsplit("\n", 1)[0]),
        ),
        (
            phrases.KIND_APOLOGY,
            lambda: ai_dialog.generate_smoothing_apology([]),
            ai_dialog._apology_ok,
        ),
        (
            phrases.KIND_LINK_HELP,
            lambda: ai_dialog.generate_link_open_help([]),
            ai_dialog._link_help_ok,
        ),
    )

    for kind, generator, validator in cases:
        generated: list[str] = []
        for _ in range(20):
            value = asyncio.run(generator())
            assert validator(value)
            assert value not in generated
            generated.append(value)
            phrases.remember(kind, value)

def test_full_automatic_path_sends_apology_then_link_help(app_env, monkeypatch):
    from services import ai_dialog, dialog_engine, dialog_store, phrases

    target = 16401
    account = 6401
    dialog_store.create_after_first_dm(target, account, "Привет, можно спросить?")
    dialog_store.set_stage(
        target,
        dialog_store.STAGE_PROMO_SENT,
        bump_outgoing=True,
        link_sent=True,
    )
    _force_due(target, dialog_store.STAGE_PROMO_SENT)

    client = _FakeClient()
    _patch_delivery(monkeypatch, client)
    apology = "Сорян, не хотел навязываться. Просто поделился, вдруг пригодится."
    help_text = (
        "Закрой крестиком панель «Заблокировать / Добавить» над чатом и нажми "
        "ссылку ещё раз. Если Telegram не пускает - скопируй ссылку вручную."
    )
    monkeypatch.setattr(
        ai_dialog,
        "generate_smoothing_apology",
        lambda history: _value(apology),
    )
    monkeypatch.setattr(
        ai_dialog,
        "generate_link_open_help",
        lambda history: _value(help_text),
    )

    assert asyncio.run(dialog_engine.process_due_auto_links()) == 1
    after_apology = dialog_store.get_dialog(target)
    assert after_apology["stage"] == dialog_store.STAGE_APOLOGY_SENT
    assert int(after_apology["outgoing_count"]) == 3
    assert after_apology["auto_link_at"] is not None

    _force_due(target, dialog_store.STAGE_APOLOGY_SENT)
    assert asyncio.run(dialog_engine.process_due_auto_links()) == 1
    final = dialog_store.get_dialog(target)
    assert final["stage"] == dialog_store.STAGE_LINK_HELP_SENT
    assert int(final["outgoing_count"]) == 4
    assert final["auto_link_at"] is None
    assert client.sent == [apology, help_text]
    assert phrases.recent_texts(phrases.KIND_APOLOGY, limit=1) == [apology]
    assert phrases.recent_texts(phrases.KIND_LINK_HELP, limit=1) == [help_text]


def test_pending_user_reply_does_not_erase_automatic_sequence(app_env, monkeypatch):
    from services import ai_dialog, dialog_engine, dialog_store

    target = 16402
    account = 6402
    dialog_store.create_after_first_dm(target, account, "Привет, можно один вопрос?")
    future = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5)).isoformat()
    dialog_store.set_stage(
        target,
        dialog_store.STAGE_PROMO_SENT,
        bump_outgoing=True,
        link_sent=True,
        auto_link_at=future,
    )
    client = _FakeClient()
    _patch_delivery(monkeypatch, client)
    monkeypatch.setattr(
        ai_dialog,
        "generate_qna_reply",
        lambda history, **kwargs: _value("да, можешь глянуть ссылку выше"),
    )

    asyncio.run(
        dialog_engine.handle_incoming_private(
            account,
            target,
            "а что там?",
            telegram_message_id=77,
        )
    )
    current = dialog_store.get_dialog(target)
    assert current["stage"] == dialog_store.STAGE_PROMO_SENT
    assert current["auto_link_at"] == future
    assert int(current["outgoing_count"]) == 3


def test_link_help_prepare_requires_apology_stage(app_env):
    from services import dialog_delivery, dialog_store

    target = 16403
    account = 6403
    dialog_store.create_after_first_dm(target, account, "Привет, не занят?")
    assert not dialog_delivery.prepare(
        target,
        account,
        dialog_delivery.KIND_LINK_HELP,
        "Закрой крестиком панель «Заблокировать / Добавить», нажми ссылку ещё раз, затем скопируй её вручную.",
    )
    dialog_store.set_stage(
        target,
        dialog_store.STAGE_APOLOGY_SENT,
        bump_outgoing=True,
        link_sent=True,
    )
    assert dialog_delivery.prepare(
        target,
        account,
        dialog_delivery.KIND_LINK_HELP,
        "Закрой крестиком панель «Заблокировать / Добавить», нажми ссылку ещё раз, затем скопируй её вручную.",
    )
