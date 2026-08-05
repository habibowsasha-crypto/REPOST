"""v1.0.66 regression tests for calm and terminal textual refusals."""

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
            id=9600 + len(self.sent),
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


def test_calm_refusal_after_promo_keeps_automatic_sequence(app_env, monkeypatch):
    from services import dialog_engine, dialog_store, opt_out

    target = 16601
    account = 6601
    dialog_store.create_after_first_dm(target, account, "Привет, можно спросить?")
    due = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5)).isoformat()
    dialog_store.set_stage(
        target,
        dialog_store.STAGE_PROMO_SENT,
        bump_outgoing=True,
        link_sent=True,
        auto_link_at=due,
    )
    client = _FakeClient()
    _patch_delivery(monkeypatch, client)

    asyncio.run(
        dialog_engine.handle_incoming_private(
            account,
            target,
            "нет, спасибо",
            telegram_message_id=66001,
        )
    )

    dialog = dialog_store.get_dialog(target)
    assert dialog["stage"] == dialog_store.STAGE_PROMO_SENT
    assert dialog["auto_link_at"] == due
    assert client.sent == []
    assert not opt_out.is_opted_out(target)


def test_terminal_refusals_still_stop_without_promo(app_env, monkeypatch):
    from services import dialog_engine, dialog_store, opt_out

    for offset, text in enumerate(("не пиши мне больше", "иди нахуй"), start=1):
        target = 16610 + offset
        account = 6610 + offset
        dialog_store.create_after_first_dm(target, account, "Привет, есть минутка?")
        client = _FakeClient()
        _patch_delivery(monkeypatch, client)

        asyncio.run(
            dialog_engine.handle_incoming_private(
                account,
                target,
                text,
                telegram_message_id=66100 + offset,
            )
        )

        assert opt_out.is_opted_out(target)
        assert dialog_store.get_dialog(target)["stage"] == dialog_store.STAGE_CLOSED
        assert len(client.sent) == 1
        assert "https://" not in client.sent[0]
        assert "t.me/" not in client.sent[0]


def test_migration_cancels_prepared_v1065_soft_close(app_env):
    from db import schema
    from services import dialog_delivery, dialog_inbox, dialog_store

    target = 16620
    account = 6620
    dialog_store.create_after_first_dm(target, account, "Привет, можно вопрос?")
    inbox_id = dialog_inbox.enqueue(
        account,
        target,
        "нет, спасибо, не надо",
        telegram_message_id=66200,
        is_hard_stop=False,
        content_kind="text",
    )
    assert inbox_id is not None
    action_key = f"close:inbox:{inbox_id}"
    assert dialog_delivery.prepare(
        target,
        account,
        action_key,
        "Понял, не буду отвлекать.",
        message_kind=dialog_delivery.KIND_CLOSE,
        transition={
            "stage": dialog_store.STAGE_CLOSED,
            "bump_outgoing": True,
            "clear_auto_link": True,
            "append_history": True,
            "mark_contact_completed": True,
        },
        source_inbox_id=inbox_id,
    )

    schema.init_db()
    row = dialog_delivery.get(target, action_key)
    assert row["status"] == dialog_delivery.STATUS_FAILED
    assert row["last_error"] == "v1.0.66_soft_refusal_close_cancelled"
