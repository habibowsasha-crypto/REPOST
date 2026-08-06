"""v1.0.75 prevents every repeated greeting after the First DM."""

from __future__ import annotations

import asyncio
import datetime as dt
from types import SimpleNamespace

import pytest


@pytest.mark.parametrize(
    "text",
    [
        "Привет, извини за вмешательство",
        "Здравствуйте! Просто хотел уточнить",
        "Добрый день, ссылка выше",
        "Добрый вечер. Не хотел навязываться",
        "Хай, можешь посмотреть позже",
        "Здарова, просто оставил ссылку",
        "Салют! Всё уже скинул выше",
        "Hello, link is above",
        "Hi! Просто оставил информацию",
        "Привет, привет, извини что отвлёк",
    ],
)
def test_post_first_dm_greeting_detection(app_env, text):
    from services import ai_dialog

    assert ai_dialog.starts_with_post_first_dm_greeting(text)


def test_post_first_dm_sanitizer_removes_only_leading_greeting(app_env):
    from services import ai_dialog

    assert (
        ai_dialog.sanitize_post_first_dm_text(
            "Привет, извини за вмешательство. Просто хотел поделиться"
        )
        == "Извини за вмешательство. Просто хотел поделиться"
    )
    assert (
        ai_dialog.sanitize_post_first_dm_text("сорян что отвлёк, просто поделился")
        == "сорян что отвлёк, просто поделился"
    )
    assert ai_dialog.sanitize_post_first_dm_text("Привет!") == ""
    assert not ai_dialog.starts_with_post_first_dm_greeting(
        "Здорово, что ссылка открылась"
    )


def test_all_post_first_dm_validators_reject_new_greeting(app_env):
    from services import ai_dialog

    promo = (
        "Привет, просто хотел оставить бесплатный канал. Там софт почти сразу "
        "копирует посты из закрытых VIP-каналов. Платить за каждую випку отдельно "
        "не нужно, может найдёшь полезную идею для сделок"
    )
    apology = "Привет, извини за внезапное сообщение. Просто решил поделиться."
    link_help = (
        "Привет, закрой крестиком панель «Заблокировать / Добавить» над чатом, "
        "нажми ссылку ещё раз, а если не откроется - скопируй её вручную."
    )
    qna = "Привет, ссылка уже выше, можешь спокойно посмотреть"

    assert "repeated_greeting" in ai_dialog._promo_validation_errors(promo)
    assert not ai_dialog._apology_ok(apology)
    assert not ai_dialog._link_help_ok(link_help)
    assert not ai_dialog._qna_ok(qna)


def test_apology_ai_retries_and_never_returns_greeting(app_env, monkeypatch):
    from services import ai_dialog

    calls = 0

    async def bad_reply(*args, **kwargs):
        nonlocal calls
        calls += 1
        return "Привет, извини за вмешательство. Просто хотел поделиться."

    monkeypatch.setattr(ai_dialog, "AI_DM_ENABLED", True)
    monkeypatch.setattr(ai_dialog, "OPENAI_API_KEY", "test")
    monkeypatch.setattr(ai_dialog, "_openai_reply", bad_reply)

    result = asyncio.run(ai_dialog.generate_smoothing_apology([]))
    assert calls == ai_dialog.MAX_GENERATION_ATTEMPTS
    assert not ai_dialog.starts_with_post_first_dm_greeting(result)
    assert ai_dialog._apology_ok(result)


def test_qna_ai_greeting_is_rejected_for_safe_fallback(app_env, monkeypatch):
    from services import ai_dialog

    async def bad_reply(*args, **kwargs):
        return "Привет, ссылка уже выше, можешь спокойно посмотреть"

    monkeypatch.setattr(ai_dialog, "AI_DM_ENABLED", True)
    monkeypatch.setattr(ai_dialog, "OPENAI_API_KEY", "test")
    monkeypatch.setattr(ai_dialog, "_openai_reply", bad_reply)

    result = asyncio.run(ai_dialog.generate_qna_reply([]))
    assert not ai_dialog.starts_with_post_first_dm_greeting(result)
    assert ai_dialog._qna_ok(result)


def test_final_delivery_guard_repairs_legacy_prepared_apology(
    app_env, monkeypatch
):
    from db.schema import get_connection
    from services import dialog_delivery, dialog_engine, dialog_store, monitor

    target = 1075001
    account = 1075002
    old_text = "Привет, извини за вмешательство. Просто хотел поделиться."
    clean_text = "Извини за вмешательство. Просто хотел поделиться."

    dialog_store.create_after_first_dm(target, account, "Привет, часто сигнал опаздывает?")
    dialog_store.set_stage(
        target,
        dialog_store.STAGE_PROMO_SENT,
        bump_outgoing=True,
        link_sent=True,
    )
    assert dialog_delivery.prepare(
        target,
        account,
        dialog_delivery.KIND_SMOOTH_APOLOGY,
        old_text,
        message_kind=dialog_delivery.KIND_SMOOTH_APOLOGY,
        transition={
            "stage": dialog_store.STAGE_APOLOGY_SENT,
            "bump_outgoing": True,
            "link_sent": True,
            "clear_auto_link": True,
            "append_history": True,
        },
    )

    class Client:
        def __init__(self):
            self.sent = []

        def is_connected(self):
            return True

        async def get_input_entity(self, value):
            return value

        async def send_message(self, entity, text):
            self.sent.append(text)
            return SimpleNamespace(
                id=75,
                date=dt.datetime.now(dt.timezone.utc),
            )

    client = Client()
    monkeypatch.setattr(monitor, "get_client", lambda value: client)
    monkeypatch.setattr(
        monitor,
        "maybe_disconnect_inactive_account",
        lambda value: asyncio.sleep(0),
    )

    result = asyncio.run(
        dialog_engine._send_prepared_action(
            account,
            target,
            dialog_delivery.KIND_SMOOTH_APOLOGY,
            old_text,
        )
    )
    assert result == "sent"
    assert client.sent == [clean_text]

    row = get_connection().execute(
        "SELECT text, status FROM dialog_outbox WHERE target_user_id=? AND action_kind=?",
        (target, dialog_delivery.KIND_SMOOTH_APOLOGY),
    ).fetchone()
    assert row["text"] == clean_text
    assert row["status"] == dialog_delivery.STATUS_SENT
    history = dialog_store.get_dialog(target)["history"]
    assert history[-1]["text"] == clean_text


def test_every_fixed_post_first_dm_fallback_has_no_greeting(app_env):
    from services import ai_dialog

    pools = (
        ai_dialog._SMOOTHING_FALLBACKS,
        ai_dialog._LINK_HELP_FALLBACKS,
        ai_dialog._FIRST_DM_SILENCE_FALLBACKS,
        ai_dialog._AGGRESSIVE_CLOSE_FALLBACKS,
        ai_dialog._STOP_CLOSE_FALLBACKS,
        ai_dialog._SOFT_CLOSE_FALLBACKS,
        ai_dialog._QNA_FALLBACKS,
    )
    for pool in pools:
        for text in pool:
            assert not ai_dialog.starts_with_post_first_dm_greeting(text), text
