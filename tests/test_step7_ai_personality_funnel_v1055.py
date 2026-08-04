"""Approved AI personality and 5-message funnel for v1.0.55."""

from __future__ import annotations

import asyncio
import datetime as dt
from types import SimpleNamespace


def _message(msg_id: int, text: str):
    return SimpleNamespace(
        id=msg_id,
        message=text,
        out=True,
        date=dt.datetime.now(dt.timezone.utc),
    )


class _FakeClient:
    def __init__(self):
        self.sent: list[str] = []

    def is_connected(self):
        return True

    async def get_input_entity(self, target):
        return target

    async def send_message(self, entity, text):
        self.sent.append(text)
        return _message(1000 + len(self.sent), text)


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
    monkeypatch.setattr(dialog_engine, "_auto_link_delay", lambda: 5)


def test_default_apology_delay_is_5_to_60_seconds(app_env):
    from services import runtime

    assert runtime.get_auto_link_delay_range() == (5, 60)


def test_emoji_only_is_always_non_text_reaction(app_env):
    from services import ai_dialog

    assert ai_dialog.is_emoji_only("👍")
    assert ai_dialog.is_emoji_only("🖕")
    assert ai_dialog.is_non_text_reaction("🖕", "text")
    assert asyncio.run(
        ai_dialog.classify_user_message([], text="🖕", content_kind="text")
    ) == ai_dialog.CATEGORY_NORMAL


def test_voice_sticker_and_media_are_always_neutral(app_env):
    from services import ai_dialog

    for kind in ("voice", "sticker", "gif", "photo", "video", "document"):
        category = asyncio.run(
            ai_dialog.classify_user_message(
                [],
                text="отъебись",
                content_kind=kind,
            )
        )
        assert category == ai_dialog.CATEGORY_NORMAL


def test_voice_reaction_sends_complete_promo_with_link(app_env, monkeypatch):
    from services import ai_dialog, dialog_engine, dialog_store

    dialog_store.create_after_first_dm(9101, 301, "ты торгуешь?")
    client = _FakeClient()
    _patch_delivery(monkeypatch, client)
    monkeypatch.setattr(
        ai_dialog,
        "generate_promo",
        lambda history, **kwargs: _value(
            "понял 👍 бесплатный канал, там софт почти сразу копирует посты из "
            "закрытых випок. отдельные доступы покупать не надо, вдруг пригодится\n"
            "https://t.me/+testhash"
        ),
    )

    asyncio.run(
        dialog_engine.handle_incoming_private(
            301,
            9101,
            "[голосовое сообщение]",
            telegram_message_id=1,
            content_kind="voice",
        )
    )

    dialog = dialog_store.get_dialog(9101)
    assert dialog["stage"] == dialog_store.STAGE_PROMO_SENT
    assert int(dialog["outgoing_count"]) == 2
    assert int(dialog["link_sent"]) == 1
    assert dialog["auto_link_at"] is not None
    assert len(client.sent) == 1
    assert "https://t.me/+testhash" in client.sent[0]


def test_any_sticker_content_still_continues_promo(app_env, monkeypatch):
    from services import ai_dialog, dialog_engine, dialog_store, opt_out

    dialog_store.create_after_first_dm(9102, 302, "ты в рынке?")
    client = _FakeClient()
    _patch_delivery(monkeypatch, client)
    monkeypatch.setattr(
        ai_dialog,
        "generate_promo",
        lambda history, **kwargs: _value(
            "понял) бесплатный канал, программа почти сразу копирует посты из "
            "закрытых випок. без покупки доступов, может пригодится для анализа\n"
            "https://t.me/+testhash"
        ),
    )

    asyncio.run(
        dialog_engine.handle_incoming_private(
            302,
            9102,
            "отъебись",
            telegram_message_id=2,
            content_kind="sticker",
        )
    )

    assert not opt_out.is_opted_out(9102)
    assert dialog_store.get_dialog(9102)["stage"] == dialog_store.STAGE_PROMO_SENT
    assert len(client.sent) == 1


def test_due_action_after_promo_is_apology_not_second_link(app_env, monkeypatch):
    from services import ai_dialog, dialog_engine, dialog_store

    dialog_store.create_after_first_dm(9103, 303, "ты торгуешь?")
    past = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)).isoformat()
    dialog_store.set_stage(
        9103,
        dialog_store.STAGE_PROMO_SENT,
        bump_outgoing=True,
        link_sent=True,
        auto_link_at=past,
    )
    client = _FakeClient()
    _patch_delivery(monkeypatch, client)
    monkeypatch.setattr(
        ai_dialog,
        "generate_smoothing_apology",
        lambda history: _value("сорян что отвлёк, просто решил поделиться"),
    )

    assert asyncio.run(dialog_engine.process_due_auto_links()) == 1
    dialog = dialog_store.get_dialog(9103)
    assert dialog["stage"] == dialog_store.STAGE_APOLOGY_SENT
    assert int(dialog["outgoing_count"]) == 3
    assert dialog["auto_link_at"] is None
    assert client.sent == ["сорян что отвлёк, просто решил поделиться"]


def test_pending_user_reply_cancels_due_apology(app_env, monkeypatch):
    from services import dialog_engine, dialog_inbox, dialog_store

    dialog_store.create_after_first_dm(9104, 304, "ты торгуешь?")
    past = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)).isoformat()
    dialog_store.set_stage(
        9104,
        dialog_store.STAGE_PROMO_SENT,
        bump_outgoing=True,
        link_sent=True,
        auto_link_at=past,
    )
    dialog_inbox.enqueue(304, 9104, "а что там?", telegram_message_id=4)
    client = _FakeClient()
    _patch_delivery(monkeypatch, client)

    assert asyncio.run(dialog_engine.process_due_auto_links()) == 0
    assert client.sent == []
    assert dialog_store.get_dialog(9104)["stage"] == dialog_store.STAGE_PROMO_SENT


def test_promo_store_keeps_exactly_last_30(app_env):
    from db.schema import get_connection
    from services import phrases

    for index in range(35):
        phrases.remember(phrases.KIND_PROMO, f"promo {index}")

    recent = phrases.recent_texts(phrases.KIND_PROMO, limit=100)
    assert len(recent) == 30
    assert recent[0] == "promo 34"
    assert recent[-1] == "promo 5"
    count = get_connection().execute(
        "SELECT COUNT(*) AS c FROM sent_phrases WHERE kind=?",
        (phrases.KIND_PROMO,),
    ).fetchone()["c"]
    assert int(count) == 30


def test_similarity_detects_near_duplicate(app_env):
    from services import ai_dialog

    old = (
        "понял тебя. бесплатный канал, там софт почти сразу копирует посты из "
        "закрытых випок. глянь, вдруг пригодится"
    )
    new = (
        "понял тебя, бесплатный канал где софт почти сразу копирует посты из "
        "закрытых випок. глянь вдруг пригодится"
    )
    assert ai_dialog.is_too_similar(new, [old])
    assert not ai_dialog.is_too_similar(
        "да, держи. программа переносит публикации с минимальной задержкой, "
        "может найдёшь полезный разбор",
        [old],
    )


def test_local_promo_has_required_facts_and_exact_link(app_env):
    from services import ai_dialog

    text = asyncio.run(
        ai_dialog.generate_promo(
            [{"role": "user", "text": "да"}],
            category=ai_dialog.CATEGORY_NORMAL,
        )
    )
    lower = text.lower()
    assert "https://t.me/+testhash" in text
    assert text.count("https://t.me/+testhash") == 1
    assert "софт" in lower or "программ" in lower
    assert "копир" in lower or "перенос" in lower or "подхватыва" in lower
    assert "вип" in lower or "vip" in lower
    assert "бесплат" in lower
    assert "—" not in text and "–" not in text


def test_stop_request_leaves_final_link_and_global_optout(app_env, monkeypatch):
    from services import dialog_engine, dialog_store, opt_out

    dialog_store.create_after_first_dm(9105, 305, "ты торгуешь?")
    client = _FakeClient()
    _patch_delivery(monkeypatch, client)

    asyncio.run(
        dialog_engine.handle_incoming_private(
            305,
            9105,
            "больше не пиши мне",
            telegram_message_id=5,
        )
    )

    assert opt_out.is_opted_out(9105)
    assert dialog_store.get_dialog(9105)["stage"] == dialog_store.STAGE_CLOSED
    assert len(client.sent) == 1
    assert "https://t.me/+testhash" in client.sent[0]
    assert "больше" in client.sent[0].lower()


def test_aggressive_refusal_has_no_link(app_env, monkeypatch):
    from services import dialog_engine, dialog_store, opt_out

    dialog_store.create_after_first_dm(9106, 306, "ты торгуешь?")
    client = _FakeClient()
    _patch_delivery(monkeypatch, client)

    asyncio.run(
        dialog_engine.handle_incoming_private(
            306,
            9106,
            "отъебись",
            telegram_message_id=6,
        )
    )

    assert opt_out.is_opted_out(9106)
    assert dialog_store.get_dialog(9106)["stage"] == dialog_store.STAGE_CLOSED
    assert len(client.sent) == 1
    assert "https://" not in client.sent[0]
    assert "извин" in client.sent[0].lower() or "понял" in client.sent[0].lower()


def test_five_outgoing_messages_are_absolute_limit(app_env, monkeypatch):
    from services import ai_dialog, dialog_engine, dialog_store

    dialog_store.create_after_first_dm(9107, 307, "ты торгуешь?")
    client = _FakeClient()
    _patch_delivery(monkeypatch, client)
    monkeypatch.setattr(
        ai_dialog,
        "generate_promo",
        lambda history, **kwargs: _value(
            "понял. бесплатный канал, софт почти сразу копирует посты из закрытых "
            "випок. доступы покупать не надо, вдруг пригодится\n"
            "https://t.me/+testhash"
        ),
    )
    monkeypatch.setattr(
        ai_dialog,
        "generate_smoothing_apology",
        lambda history: _value("сорян что отвлёк, просто решил поделиться"),
    )
    monkeypatch.setattr(
        ai_dialog,
        "generate_qna_reply",
        lambda history, **kwargs: _value("короткий ответ"),
    )

    asyncio.run(
        dialog_engine.handle_incoming_private(
            307, 9107, "да", telegram_message_id=70
        )
    )
    past = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)).isoformat()
    dialog_store.set_stage(9107, dialog_store.STAGE_PROMO_SENT, auto_link_at=past)
    asyncio.run(dialog_engine.process_due_auto_links())
    asyncio.run(
        dialog_engine.handle_incoming_private(
            307, 9107, "вопрос один", telegram_message_id=71
        )
    )
    asyncio.run(
        dialog_engine.handle_incoming_private(
            307, 9107, "вопрос два", telegram_message_id=72
        )
    )

    dialog = dialog_store.get_dialog(9107)
    assert int(dialog["outgoing_count"]) == 5
    assert dialog["stage"] == dialog_store.STAGE_CLOSED
    assert len(client.sent) == 4  # First DM was sent before this fake client was attached.

    asyncio.run(
        dialog_engine.handle_incoming_private(
            307, 9107, "ещё вопрос", telegram_message_id=73
        )
    )
    assert len(client.sent) == 4
