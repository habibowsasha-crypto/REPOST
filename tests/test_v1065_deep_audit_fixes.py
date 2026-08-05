"""v1.0.65 deep-audit regression tests."""

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
            id=9000 + len(self.sent),
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


def test_first_reply_soft_refusal_continues_to_promo(app_env, monkeypatch):
    from services import ai_dialog, dialog_engine, dialog_store, opt_out

    target = 16501
    account = 6501
    dialog_store.create_after_first_dm(target, account, "Привет, можно спросить?")
    client = _FakeClient()
    _patch_delivery(monkeypatch, client)
    promo = (
        "понял тебя, просто оставлю на всякий случай. бесплатный канал - "
        "там софт почти сразу переносит публикации из закрытых випок, платить "
        "за каждую отдельно не нужно\nhttps://t.me/+testhash"
    )
    monkeypatch.setattr(ai_dialog, "generate_promo", lambda *args, **kwargs: _value(promo))

    asyncio.run(
        dialog_engine.handle_incoming_private(
            account,
            target,
            "нет, спасибо, не надо",
            telegram_message_id=1,
        )
    )

    dialog = dialog_store.get_dialog(target)
    assert dialog["stage"] == dialog_store.STAGE_PROMO_SENT
    assert len(client.sent) == 1
    assert client.sent[0] == promo
    assert not opt_out.is_opted_out(target)


def test_stop_request_never_sends_link(app_env, monkeypatch):
    from services import dialog_engine, dialog_store, opt_out

    target = 16502
    account = 6502
    dialog_store.create_after_first_dm(target, account, "Привет, есть минутка?")
    client = _FakeClient()
    _patch_delivery(monkeypatch, client)

    asyncio.run(
        dialog_engine.handle_incoming_private(
            account,
            target,
            "больше не пиши мне",
            telegram_message_id=2,
        )
    )

    assert opt_out.is_opted_out(target)
    assert len(client.sent) == 1
    assert "https://" not in client.sent[0]
    assert "t.me/" not in client.sent[0]


def test_qna_budget_reserves_mandatory_link_help(app_env, monkeypatch):
    from services import ai_dialog, dialog_engine, dialog_store

    target = 16503
    account = 6503
    dialog_store.create_after_first_dm(target, account, "Привет, можно один вопрос?")
    dialog_store.set_stage(
        target,
        dialog_store.STAGE_PROMO_SENT,
        bump_outgoing=True,
        link_sent=True,
        auto_link_at=(dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5)).isoformat(),
    )
    client = _FakeClient()
    _patch_delivery(monkeypatch, client)
    monkeypatch.setattr(
        ai_dialog,
        "generate_qna_reply",
        lambda history, **kwargs: _value("короткий ответ"),
    )
    monkeypatch.setattr(
        ai_dialog,
        "generate_smoothing_apology",
        lambda history: _value("Сорян, не хотел навязываться. Просто поделился."),
    )
    help_text = (
        "Закрой крестиком панель «Заблокировать / Добавить» над чатом, нажми "
        "ссылку ещё раз, а если Telegram не пустит - скопируй её вручную."
    )
    monkeypatch.setattr(
        ai_dialog,
        "generate_link_open_help",
        lambda history: _value(help_text),
    )

    asyncio.run(
        dialog_engine.handle_incoming_private(
            account, target, "а что там?", telegram_message_id=3
        )
    )
    asyncio.run(
        dialog_engine.handle_incoming_private(
            account, target, "и ещё вопрос", telegram_message_id=4
        )
    )
    assert len(client.sent) == 1
    assert int(dialog_store.get_dialog(target)["outgoing_count"]) == 3

    _force_due(target, dialog_store.STAGE_PROMO_SENT)
    assert asyncio.run(dialog_engine.process_due_auto_links()) == 1
    _force_due(target, dialog_store.STAGE_APOLOGY_SENT)
    assert asyncio.run(dialog_engine.process_due_auto_links()) == 1

    final = dialog_store.get_dialog(target)
    assert int(final["outgoing_count"]) == 5
    assert final["stage"] == dialog_store.STAGE_CLOSED
    assert client.sent[-1] == help_text


def test_legacy_overspent_dialog_prioritizes_link_help(app_env, monkeypatch):
    from services import ai_dialog, dialog_engine, dialog_store

    target = 16504
    account = 6504
    dialog_store.create_after_first_dm(target, account, "Привет, не занят?")
    # Simulate v1.0.64: first DM + promo + two Q&A = four outgoing messages,
    # while the dialog is still waiting for apology.
    dialog_store.set_stage(
        target,
        dialog_store.STAGE_PROMO_SENT,
        bump_outgoing=True,
        link_sent=True,
    )
    dialog_store.set_stage(target, dialog_store.STAGE_PROMO_SENT, bump_outgoing=True)
    dialog_store.set_stage(target, dialog_store.STAGE_PROMO_SENT, bump_outgoing=True)
    _force_due(target, dialog_store.STAGE_PROMO_SENT)

    client = _FakeClient()
    _patch_delivery(monkeypatch, client)
    help_text = (
        "Закрой крестиком панель «Заблокировать / Добавить» над чатом и снова "
        "нажми ссылку. Если Telegram не пускает - скопируй её вручную."
    )
    monkeypatch.setattr(
        ai_dialog,
        "generate_link_open_help",
        lambda history: _value(help_text),
    )

    assert asyncio.run(dialog_engine.process_due_auto_links()) == 1
    final = dialog_store.get_dialog(target)
    assert final["stage"] == dialog_store.STAGE_CLOSED
    assert int(final["outgoing_count"]) == 5
    assert client.sent == [help_text]


def test_phrase_journal_is_written_during_durable_prepare(app_env):
    from services import dialog_delivery, dialog_store, phrases

    target = 16505
    account = 6505
    dialog_store.create_after_first_dm(target, account, "Привет, можно спросить?")
    promo = (
        "ок, понял. просто хотел оставить бесплатный канал - там софт почти сразу "
        "копирует публикации из закрытых випок, платить отдельно не нужно\n"
        "https://t.me/+testhash"
    )
    assert dialog_delivery.prepare(
        target,
        account,
        "promo:inbox:500",
        promo,
        message_kind=dialog_delivery.KIND_PROMO,
        transition={
            "stage": dialog_store.STAGE_PROMO_SENT,
            "bump_outgoing": True,
            "link_sent": True,
            "append_history": True,
        },
        source_inbox_id=500,
    )
    assert phrases.recent_texts(phrases.KIND_PROMO, limit=1) == [promo]

    # Commit backfill is idempotent and does not create a second history row.
    assert dialog_delivery.commit_sent(target, "promo:inbox:500")
    assert phrases.recent_texts(phrases.KIND_PROMO, limit=10).count(promo) == 1


def test_recent_ai_block_redacts_admin_link_and_handle(app_env):
    from services import ai_dialog

    block = ai_dialog._recent_block(
        ["текст https://t.me/+privatehash и @SomeUser"]
    )
    assert "privatehash" not in block
    assert "@SomeUser" not in block
    assert "[ссылка]" in block
    assert "[username]" in block


def test_first_dm_phrase_is_journaled_during_prepare(app_env):
    from services import accounts, first_dm_delivery, phrases, queue

    account = 6506
    target = 16506
    text = "Привет, можно один вопрос?"
    accounts.upsert_account(
        user_id=account,
        session_string="session",
        username="sender6506",
    )
    accounts.set_participates(account, True)
    queue.upsert_from_activity(
        target_user_id=target,
        username="lead16506",
        access_hash=123456,
        source_account_user_id=account,
    )
    assert queue.claim_random_pending(account)
    assert first_dm_delivery.prepare(target, account, text)
    assert phrases.recent_texts(phrases.KIND_FIRST_DM, limit=1) == [text]


def test_parallel_first_dm_generation_uses_updated_last20_window(app_env, monkeypatch):
    from services import accounts, dispatcher, monitor, phrases, queue

    class _EntityClient(_FakeClient):
        def __init__(self, target_id: int):
            super().__init__()
            self.target_id = target_id

        async def get_input_entity(self, target):
            return SimpleNamespace(user_id=self.target_id)

    account_a, target_a = 6507, 16507
    account_b, target_b = 6508, 16508
    clients = {
        account_a: _EntityClient(target_a),
        account_b: _EntityClient(target_b),
    }
    leads = []
    account_rows = []
    for account, target in ((account_a, target_a), (account_b, target_b)):
        accounts.upsert_account(
            user_id=account,
            session_string=f"session-{account}",
            username=f"sender{account}",
        )
        accounts.set_participates(account, True)
        queue.upsert_from_activity(
            target_user_id=target,
            username=f"lead{target}",
            access_hash=target * 10,
            source_account_user_id=account,
        )
        leads.append(
            {
                "target_user_id": target,
                "username": f"lead{target}",
                "access_hash": target * 10,
                "source_account_user_id": account,
            }
        )
        account_rows.append(accounts.get_account(account))

    monkeypatch.setattr(monitor, "get_client", lambda account: clients[int(account)])
    monkeypatch.setattr(dispatcher, "_notify_admins_first_dm", lambda *args: _value(None))

    async def generated_from_current_window():
        recent = phrases.recent_texts(phrases.KIND_FIRST_DM, limit=20)
        # Make the race reproducible if generation and prepare are not serialized.
        await asyncio.sleep(0.02)
        if "Привет, можно спросить?" not in recent:
            return "Привет, можно спросить?"
        return "Привет, есть минутка?"

    monkeypatch.setattr(dispatcher, "generate_first_dm", generated_from_current_window)

    async def run_both():
        return await asyncio.gather(
            dispatcher._attempt_lead_across_accounts(leads[0], [account_rows[0]]),
            dispatcher._attempt_lead_across_accounts(leads[1], [account_rows[1]]),
        )

    assert asyncio.run(run_both()) == [True, True]
    sent = clients[account_a].sent + clients[account_b].sent
    assert len(sent) == 2
    assert len(set(sent)) == 2


def test_ai_cannot_override_explicit_local_refusal(app_env, monkeypatch):
    from services import ai_dialog

    monkeypatch.setattr(ai_dialog, "AI_DM_ENABLED", True)
    monkeypatch.setattr(ai_dialog, "OPENAI_API_KEY", "test-key")

    async def misleading_model(*args, **kwargs):
        return "normal"

    monkeypatch.setattr(ai_dialog, "_openai_reply", misleading_model)
    category = asyncio.run(
        ai_dialog.classify_user_message([], text="нет, спасибо, не надо")
    )
    assert category == ai_dialog.CATEGORY_SOFT_REFUSAL
