"""v1.0.82 selectable dialog Variant 3."""

from __future__ import annotations

import asyncio
import datetime as dt
from types import SimpleNamespace


class _Client:
    def __init__(self):
        self.sent: list[str] = []

    def is_connected(self):
        return True

    async def get_input_entity(self, value):
        return value

    async def send_message(self, entity, text):
        self.sent.append(str(text))
        return SimpleNamespace(
            id=82000 + len(self.sent),
            message=str(text),
            out=True,
            date=dt.datetime.now(dt.timezone.utc),
        )


async def _value(value):
    return value


def _patch_variant3(monkeypatch, client: _Client) -> None:
    from services import ai_dialog, dialog_engine, monitor

    monkeypatch.setattr(ai_dialog, "DIALOG_FLOW_VARIANT", 3)
    monkeypatch.setattr(dialog_engine, "DIALOG_FLOW_VARIANT", 3)
    monkeypatch.setattr(monitor, "get_client", lambda account: client)
    monkeypatch.setattr(
        monitor,
        "maybe_disconnect_inactive_account",
        lambda account: _value(None),
    )
    monkeypatch.setattr(dialog_engine, "_delay_reply", lambda: 0.0)
    monkeypatch.setattr(dialog_engine, "_auto_link_delay", lambda: 60)


def _force_due(target: int, stage: str) -> None:
    from services import dialog_store

    past = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)
    dialog_store.set_stage(target, stage, auto_link_at=past.isoformat())


def test_variant3_variable_and_exact_inline_hint(app_env, monkeypatch):
    import config
    from services import ai_dialog

    assert config.DIALOG_FLOW_VARIANT == 1
    monkeypatch.setattr(ai_dialog, "DIALOG_FLOW_VARIANT", 3)

    text = asyncio.run(
        ai_dialog.generate_promo([], category=ai_dialog.CATEGORY_NORMAL)
    )
    expected = (
        "Если ссылка не откроется - закрой крестиком панель "
        "«Заблокировать / Добавить» сверху над чатом и нажми на ссылку ещё раз."
    )
    assert text.endswith(f"https://t.me/+testhash\n\n{expected}")
    assert "\u2014" not in text and "\u2013" not in text
    assert ai_dialog.inline_link_help_text(1) == ""
    assert ai_dialog.inline_link_help_text(2).startswith("Не открылась ссылка?")
    assert ai_dialog.inline_link_help_text(3) == expected


def test_variant3_automatic_path_stops_after_apology(app_env, monkeypatch):
    from services import ai_dialog, dialog_engine, dialog_store

    account, target = 108201, 180201
    dialog_store.create_after_first_dm(target, account, "Привет, можно вопрос?")
    dialog_store.append_history(target, "user", "Да")
    dialog_store.set_stage(
        target,
        dialog_store.STAGE_PROMO_SENT,
        bump_outgoing=True,
        link_sent=True,
    )
    _force_due(target, dialog_store.STAGE_PROMO_SENT)

    client = _Client()
    _patch_variant3(monkeypatch, client)
    apology = "Сорян, не хотел навязываться. Просто поделился, вдруг пригодится."
    monkeypatch.setattr(
        ai_dialog,
        "generate_smoothing_apology",
        lambda history: _value(apology),
    )

    assert asyncio.run(dialog_engine.process_due_auto_links()) == 1
    current = dialog_store.get_dialog(target)
    assert client.sent == [apology]
    assert current["stage"] == dialog_store.STAGE_APOLOGY_SENT
    assert int(current["outgoing_count"]) == 3
    assert current["auto_link_at"] is None

    assert asyncio.run(dialog_engine.process_due_auto_links()) == 0
    assert client.sent == [apology]


def test_variant3_detailed_help_only_after_real_link_problem(app_env, monkeypatch):
    from services import ai_dialog, dialog_engine, dialog_store

    account, target = 108202, 180202
    dialog_store.create_after_first_dm(target, account, "Привет, можно вопрос?")
    dialog_store.set_stage(
        target,
        dialog_store.STAGE_APOLOGY_SENT,
        bump_outgoing=True,
        link_sent=True,
        clear_auto_link=True,
    )

    client = _Client()
    _patch_variant3(monkeypatch, client)
    help_text = (
        "Закрой крестиком панель «Заблокировать / Добавить» над чатом, "
        "нажми ссылку ещё раз, а если не получится - скопируй её вручную."
    )

    async def classify(*args, **kwargs):
        return ai_dialog.CATEGORY_NORMAL

    monkeypatch.setattr(ai_dialog, "classify_user_message", classify)
    monkeypatch.setattr(
        ai_dialog,
        "generate_link_open_help",
        lambda history: _value(help_text),
    )

    asyncio.run(
        dialog_engine.handle_incoming_private(
            account,
            target,
            "Ссылка не открывается",
            telegram_message_id=18020201,
        )
    )

    current = dialog_store.get_dialog(target)
    assert client.sent == [help_text]
    assert current["stage"] == dialog_store.STAGE_LINK_HELP_SENT
    assert int(current["outgoing_count"]) == 3
    assert current["auto_link_at"] is None


def test_link_problem_detector_does_not_treat_plain_request_as_failure(app_env):
    from services import ai_dialog

    assert ai_dialog.is_link_open_problem("Ссылка не открывается")
    assert ai_dialog.is_link_open_problem("Не могу перейти по ссылке")
    assert ai_dialog.is_link_open_problem("Как открыть ссылку?")
    assert not ai_dialog.is_link_open_problem("Скинь ссылку ещё раз")
    assert not ai_dialog.is_link_open_problem("Ок, посмотрю")
    assert not ai_dialog.is_link_open_problem("🎤", content_kind="voice")


def test_variant3_reserves_only_apology_inside_five_message_cap(app_env, monkeypatch):
    from services import dialog_engine, dialog_store

    monkeypatch.setattr(dialog_engine, "DIALOG_FLOW_VARIANT", 3)
    assert dialog_engine._reserved_automatic_slots(dialog_store.STAGE_PROMO_SENT) == 1
    assert dialog_engine._reserved_automatic_slots(dialog_store.STAGE_APOLOGY_SENT) == 0

    monkeypatch.setattr(dialog_engine, "DIALOG_FLOW_VARIANT", 1)
    assert dialog_engine._reserved_automatic_slots(dialog_store.STAGE_PROMO_SENT) == 2
    assert dialog_engine._reserved_automatic_slots(dialog_store.STAGE_APOLOGY_SENT) == 1
