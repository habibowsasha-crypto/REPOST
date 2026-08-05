"""Regression tests for Step 1: durable sequential incoming messages."""

from __future__ import annotations

import asyncio


def test_parallel_messages_are_queued_and_processed_in_order(app_env, monkeypatch):
    from services import dialog_engine
    from services import dialog_inbox
    from services import dialog_store

    dialog_store.create_after_first_dm(7001, 101, "Ты торгуешь?")
    started = asyncio.Event()
    release = asyncio.Event()
    processed: list[str] = []

    async def fake_body(account, target, text, *, history_already_appended=False):
        processed.append(text)
        if text == "первое":
            started.set()
            await release.wait()

    monkeypatch.setattr(dialog_engine, "_handle_incoming_private_body", fake_body)

    async def scenario():
        first = asyncio.create_task(
            dialog_engine.handle_incoming_private(
                101, 7001, "первое", telegram_message_id=1
            )
        )
        await started.wait()
        second = asyncio.create_task(
            dialog_engine.handle_incoming_private(
                101, 7001, "второе", telegram_message_id=2
            )
        )
        await asyncio.sleep(0)
        release.set()
        await asyncio.gather(first, second)

    asyncio.run(scenario())

    assert processed == ["первое", "второе"]
    assert dialog_inbox.count_by_status(dialog_inbox.STATUS_DONE) == 2
    history = dialog_store.get_dialog(7001)["history"]
    assert [x["text"] for x in history if x["role"] == "user"] == [
        "первое",
        "второе",
    ]


def test_direct_refusal_preempts_pending_ai_reply(app_env, monkeypatch):
    from services import ai_dialog
    from services import dialog_engine
    from services import dialog_store
    from services import monitor
    from services import opt_out

    dialog_store.create_after_first_dm(7002, 102, "Ты сам торгуешь?")
    generation_started = asyncio.Event()
    release_generation = asyncio.Event()
    sent: list[str] = []

    async def slow_promo(history, **kwargs):
        generation_started.set()
        await release_generation.wait()
        return "обычное продолжение воронки\nhttps://t.me/+testhash"

    class FakeClient:
        def is_connected(self):
            return True

        async def get_input_entity(self, target):
            return target

        async def send_message(self, entity, text):
            sent.append(text)
            return object()

    monkeypatch.setattr(ai_dialog, "first_bot_was_soft", lambda history: False)
    monkeypatch.setattr(ai_dialog, "generate_promo", slow_promo)
    monkeypatch.setattr(dialog_engine, "_delay_reply", lambda: 0.0)
    monkeypatch.setattr(monitor, "get_client", lambda account: FakeClient())
    monkeypatch.setattr(monitor, "maybe_disconnect_inactive_account", lambda account: _noop())

    async def scenario():
        normal = asyncio.create_task(
            dialog_engine.handle_incoming_private(
                102, 7002, "да, торгую", telegram_message_id=10
            )
        )
        await generation_started.wait()
        refusal = asyncio.create_task(
            dialog_engine.handle_incoming_private(
                102, 7002, "не пиши мне больше", telegram_message_id=11
            )
        )
        await asyncio.sleep(0)
        assert opt_out.is_opted_out(7002)
        release_generation.set()
        await asyncio.gather(normal, refusal)

    asyncio.run(scenario())

    assert len(sent) == 1
    assert "не буду" in sent[0].lower() or "не напишу" in sent[0].lower()
    assert "https://" not in sent[0]
    assert opt_out.is_opted_out(7002)
    assert dialog_store.get_dialog(7002)["stage"] == dialog_store.STAGE_CLOSED
    history = dialog_store.get_dialog(7002)["history"]
    user_texts = [x["text"] for x in history if x["role"] == "user"]
    assert user_texts == ["да, торгую", "не пиши мне больше"]


def test_duplicate_telegram_event_is_processed_once(app_env, monkeypatch):
    from services import dialog_engine
    from services import dialog_inbox
    from services import dialog_store

    dialog_store.create_after_first_dm(7003, 103, "Можно спросить?")
    processed: list[str] = []

    async def fake_body(account, target, text, *, history_already_appended=False):
        processed.append(text)

    monkeypatch.setattr(dialog_engine, "_handle_incoming_private_body", fake_body)

    async def scenario():
        await dialog_engine.handle_incoming_private(
            103, 7003, "ответ", telegram_message_id=77
        )
        await dialog_engine.handle_incoming_private(
            103, 7003, "ответ", telegram_message_id=77
        )

    asyncio.run(scenario())

    assert processed == ["ответ"]
    assert dialog_inbox.count_by_status(dialog_inbox.STATUS_DONE) == 1


async def _noop():
    return None


def test_cancelled_hard_stop_is_requeued_for_restart(app_env, monkeypatch):
    from services import dialog_engine
    from services import dialog_inbox
    from services import dialog_store

    dialog_store.create_after_first_dm(7004, 104, "Можно спросить?")
    started = asyncio.Event()

    async def blocked_hard_stop(account, target, text, **kwargs):
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(dialog_engine, "_process_hard_stop", blocked_hard_stop)

    async def scenario():
        task = asyncio.create_task(
            dialog_engine.handle_incoming_private(
                104, 7004, "не пиши", telegram_message_id=88
            )
        )
        await started.wait()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(scenario())

    assert dialog_inbox.count_by_status(dialog_inbox.STATUS_PENDING) == 1
