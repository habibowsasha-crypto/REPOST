"""Regression tests for Step 2: durable delivery for every dialog message."""

from __future__ import annotations

import asyncio
import datetime as dt
from types import SimpleNamespace


def _message(msg_id: int, text: str, *, minutes_ago: int = 0):
    return SimpleNamespace(
        id=msg_id,
        message=text,
        out=True,
        date=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=minutes_ago),
    )


def test_ambiguous_ai_reply_is_reconciled_without_duplicate(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import ai_dialog, dialog_delivery, dialog_engine, dialog_inbox
    from services import dialog_store, monitor

    dialog_store.create_after_first_dm(8101, 201, "Ты торгуешь?")
    sent: list[object] = []

    class FakeClient:
        def is_connected(self):
            return True

        async def get_input_entity(self, target):
            return target

        async def send_message(self, entity, text):
            msg = _message(5001, text)
            sent.append(msg)
            raise RuntimeError("response_lost_after_telegram_accept")

        async def iter_messages(self, entity):
            for msg in reversed(sent):
                yield msg

    client = FakeClient()
    monkeypatch.setattr(monitor, "get_client", lambda account: client)
    monkeypatch.setattr(ai_dialog, "first_bot_was_soft", lambda history: False)
    monkeypatch.setattr(ai_dialog, "generate_promo", lambda history, **kwargs: _async_value("ответ AI\nhttps://t.me/+testhash"))
    monkeypatch.setattr(dialog_engine, "_delay_reply", lambda: 0.0)

    asyncio.run(
        dialog_engine.handle_incoming_private(
            201, 8101, "да", telegram_message_id=9001
        )
    )
    assert len(sent) == 1
    assert dialog_inbox.count_by_status(dialog_inbox.STATUS_PENDING) == 1

    conn = get_connection()
    row = conn.execute(
        "SELECT action_kind FROM dialog_outbox WHERE target_user_id=8101 AND status='prepared'"
    ).fetchone()
    assert row is not None
    old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)).isoformat()
    with db_lock(), conn:
        conn.execute(
            "UPDATE dialog_outbox SET prepared_at=? WHERE target_user_id=8101",
            (old,),
        )

    assert asyncio.run(dialog_engine.recover_ambiguous_dialog_messages()) == 1
    assert dialog_inbox.count_by_status(dialog_inbox.STATUS_DONE) == 1
    assert asyncio.run(dialog_engine.recover_pending_incoming_messages()) == 0
    assert len(sent) == 1

    dialog = dialog_store.get_dialog(8101)
    assert dialog["stage"] == dialog_store.STAGE_PROMO_SENT
    assert [x["text"] for x in dialog["history"] if x["role"] == "user"] == ["да"]
    assert [x["text"] for x in dialog["history"] if x["role"] == "assistant"].count(
        "ответ AI\nhttps://t.me/+testhash"
    ) == 1
    outbox = dialog_delivery.get(8101, str(row["action_kind"]))
    assert outbox["status"] == dialog_delivery.STATUS_SENT
    assert int(outbox["telegram_message_id"]) == 5001


def test_direct_link_uses_same_durable_outbox(app_env, monkeypatch):
    from db.schema import get_connection
    from services import ai_dialog, dialog_delivery, dialog_engine, dialog_store, monitor

    dialog_store.create_after_first_dm(8102, 202, "Можно спросить?")
    dialog_store.set_stage(8102, dialog_store.STAGE_ENGAGED, clear_auto_link=True)

    class FakeClient:
        def is_connected(self):
            return True

        async def get_input_entity(self, target):
            return target

        async def send_message(self, entity, text):
            return _message(5002, text)

    monkeypatch.setattr(monitor, "get_client", lambda account: FakeClient())
    monkeypatch.setattr(ai_dialog, "is_link_request", lambda text: True)
    monkeypatch.setattr(ai_dialog, "generate_promo", lambda history, **kwargs: _async_value("ссылка\nhttps://t.me/+testhash"))
    monkeypatch.setattr(dialog_engine, "_delay_reply", lambda: 0.0)

    asyncio.run(
        dialog_engine.handle_incoming_private(
            202, 8102, "дай ссылку", telegram_message_id=9002
        )
    )

    dialog = dialog_store.get_dialog(8102)
    assert dialog["stage"] == dialog_store.STAGE_PROMO_SENT
    assert int(dialog["link_sent"]) == 1
    row = get_connection().execute(
        """
        SELECT message_kind, status, telegram_message_id, source_inbox_id
          FROM dialog_outbox WHERE target_user_id=8102
        """
    ).fetchone()
    assert row["message_kind"] == dialog_delivery.KIND_PROMO
    assert row["status"] == dialog_delivery.STATUS_SENT
    assert int(row["telegram_message_id"]) == 5002
    assert row["source_inbox_id"] is not None


def test_ambiguous_apology_can_commit_after_optout_closed_dialog(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import dialog_delivery, dialog_engine, dialog_inbox, dialog_store, monitor

    dialog_store.create_after_first_dm(8103, 203, "Можно вопрос?")
    sent: list[object] = []

    class FakeClient:
        def is_connected(self):
            return True

        async def get_input_entity(self, target):
            return target

        async def send_message(self, entity, text):
            msg = _message(5003, text)
            sent.append(msg)
            raise RuntimeError("response_lost")

        async def iter_messages(self, entity):
            for msg in reversed(sent):
                yield msg

    monkeypatch.setattr(monitor, "get_client", lambda account: FakeClient())
    monkeypatch.setattr(monitor, "maybe_disconnect_inactive_account", lambda account: _async_value(None))
    monkeypatch.setattr(dialog_engine, "_delay_reply", lambda: 0.0)

    asyncio.run(
        dialog_engine.handle_incoming_private(
            203, 8103, "не пиши мне", telegram_message_id=9003
        )
    )
    assert len(sent) == 1
    assert dialog_inbox.count_by_status(dialog_inbox.STATUS_PENDING) == 1

    conn = get_connection()
    row = conn.execute(
        "SELECT action_kind, allow_opt_out FROM dialog_outbox WHERE target_user_id=8103"
    ).fetchone()
    assert int(row["allow_opt_out"]) == 1
    with db_lock(), conn:
        conn.execute(
            "UPDATE dialog_outbox SET prepared_at=? WHERE target_user_id=8103",
            ((dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)).isoformat(),),
        )

    assert asyncio.run(dialog_engine.recover_ambiguous_dialog_messages()) == 1
    assert dialog_inbox.count_by_status(dialog_inbox.STATUS_DONE) == 1
    assert dialog_store.get_dialog(8103)["stage"] == dialog_store.STAGE_CLOSED
    outbox = dialog_delivery.get(8103, str(row["action_kind"]))
    assert outbox["status"] == dialog_delivery.STATUS_SENT
    assert int(outbox["telegram_message_id"]) == 5003


def test_history_reconciliation_has_no_numeric_message_cap(app_env):
    from services import telegram_history

    expected = _message(42, "нужный текст", minutes_ago=1)
    noise = [_message(1000 + i, f"шум {i}") for i in range(150)]

    class FakeClient:
        async def iter_messages(self, entity):
            for msg in noise:
                yield msg
            yield expected

        async def get_messages(self, entity, limit=None):
            raise AssertionError("iter_messages should be used in production path")

    found = asyncio.run(
        telegram_history.find_outgoing_text_since(
            FakeClient(),
            1,
            "нужный текст",
            since=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=2),
        )
    )
    assert found is expected


async def _async_value(value):
    return value
