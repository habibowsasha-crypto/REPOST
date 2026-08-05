"""v1.0.67 deep-audit fixes: intent safety, privacy and cooldown delivery."""

from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path
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
            id=9700 + len(self.sent),
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


def _add_account_with_cooldown(account: int, seconds: int = 600):
    from db.schema import db_lock, get_connection
    from services import accounts

    accounts.upsert_account(
        user_id=account,
        session_string="session",
        username=f"sender{account}",
    )
    accounts.set_participates(account, True)
    until = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=seconds)).isoformat()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE accounts
               SET is_paused=1, pause_reason='PeerFlood', cooldown_until=?
             WHERE user_id=?
            """,
            (until, account),
        )
    return until


def test_reassuring_phrases_are_not_false_stop_requests(app_env):
    from services import ai_dialog

    for text in (
        "не беспокойся, всё нормально",
        "не отвлекайся, говори",
        "не беспокойтесь, я отвечу позже",
    ):
        assert ai_dialog.local_category(text) == ai_dialog.CATEGORY_NORMAL
        assert not ai_dialog.is_hard_stop(text)


def test_common_direct_aggressive_refusals_are_terminal(app_env):
    from services import ai_dialog

    for text in (
        "отвали",
        "проваливай",
        "иди лесом",
        "заткнись",
        "достал уже",
        "пошёл ты",
    ):
        assert ai_dialog.local_category(text) == ai_dialog.CATEGORY_AGGRESSIVE_REFUSAL
        assert ai_dialog.is_hard_stop(text)

    for text in (
        "не беспокой меня больше",
        "не отвлекайте больше",
        "оставь меня в покое",
    ):
        assert ai_dialog.local_category(text) == ai_dialog.CATEGORY_STOP_REQUEST
        assert ai_dialog.is_hard_stop(text)


def test_reassurance_continues_to_promo_but_aggression_does_not(app_env, monkeypatch):
    from services import ai_dialog, dialog_engine, dialog_store, opt_out

    account = 6701
    client = _FakeClient()
    _patch_delivery(monkeypatch, client)
    promo = (
        "ок, понял. просто хотел оставить бесплатный канал - там софт почти сразу "
        "копирует публикации из закрытых випок, платить отдельно не нужно\n"
        "https://t.me/+testhash"
    )
    monkeypatch.setattr(ai_dialog, "generate_promo", lambda *args, **kwargs: _value(promo))

    calm_target = 16701
    dialog_store.create_after_first_dm(calm_target, account, "Привет, можно спросить?")
    asyncio.run(
        dialog_engine.handle_incoming_private(
            account,
            calm_target,
            "не беспокойся, всё нормально",
            telegram_message_id=1,
        )
    )
    assert dialog_store.get_dialog(calm_target)["stage"] == dialog_store.STAGE_PROMO_SENT
    assert client.sent == [promo]
    assert not opt_out.is_opted_out(calm_target)

    aggressive_target = 16702
    dialog_store.create_after_first_dm(aggressive_target, account, "Привет, можно вопрос?")
    asyncio.run(
        dialog_engine.handle_incoming_private(
            account,
            aggressive_target,
            "отвали",
            telegram_message_id=2,
        )
    )
    assert dialog_store.get_dialog(aggressive_target)["stage"] == dialog_store.STAGE_CLOSED
    assert opt_out.is_opted_out(aggressive_target)
    assert len(client.sent) == 2
    assert "https://" not in client.sent[-1]


def test_all_openai_history_is_redacted(app_env):
    from services import ai_dialog

    secret = "https://t.me/+privateInviteHash"
    history = [
        {"role": "assistant", "text": f"держи {secret} и @SomePrivateUser"},
        {"role": "user", "text": "мой сайт https://example.com/path и @OtherUser"},
    ]
    messages = ai_dialog._history_messages_for_ai(history)
    joined = "\n".join(item["content"] for item in messages)
    assert "privateInviteHash" not in joined
    assert "example.com" not in joined
    assert "@SomePrivateUser" not in joined
    assert "@OtherUser" not in joined
    assert "[ссылка]" in joined
    assert "[username]" in joined


def test_scheduled_dialog_waits_for_peerflood_cooldown(app_env, monkeypatch):
    from services import ai_dialog, dialog_engine, dialog_store

    account = 6702
    target = 16703
    until = _add_account_with_cooldown(account, seconds=600)
    dialog_store.create_after_first_dm(target, account, "Привет, есть минутка?")
    past = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)).isoformat()
    dialog_store.set_stage(
        target,
        dialog_store.STAGE_PROMO_SENT,
        bump_outgoing=True,
        link_sent=True,
        auto_link_at=past,
    )
    client = _FakeClient()
    _patch_delivery(monkeypatch, client)
    generated = {"count": 0}

    async def _apology(history):
        generated["count"] += 1
        return "Сорян, не хотел навязываться. Просто поделился."

    monkeypatch.setattr(ai_dialog, "generate_smoothing_apology", _apology)
    assert asyncio.run(dialog_engine.process_due_auto_links()) == 0
    current = dialog_store.get_dialog(target)
    assert current["stage"] == dialog_store.STAGE_PROMO_SENT
    assert current["auto_link_at"] > until
    assert generated["count"] == 0
    assert client.sent == []


def test_incoming_dialog_waits_for_peerflood_cooldown(app_env, monkeypatch):
    from services import ai_dialog, dialog_engine, dialog_inbox, dialog_store

    account = 6703
    target = 16704
    _add_account_with_cooldown(account, seconds=600)
    dialog_store.create_after_first_dm(target, account, "Привет, можно один вопрос?")
    client = _FakeClient()
    _patch_delivery(monkeypatch, client)
    called = {"promo": 0}

    async def _promo(*args, **kwargs):
        called["promo"] += 1
        return "promo"

    monkeypatch.setattr(ai_dialog, "generate_promo", _promo)
    asyncio.run(
        dialog_engine.handle_incoming_private(
            account,
            target,
            "да",
            telegram_message_id=4,
        )
    )
    assert dialog_inbox.count_by_status(dialog_inbox.STATUS_PENDING) == 1
    assert called["promo"] == 0
    assert client.sent == []


def test_due_queries_are_bounded_and_oldest_first(app_env):
    from services import dialog_store

    account = 6704
    base = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=10)
    expected: list[int] = []
    for index in range(6):
        target = 16720 + index
        dialog_store.create_after_first_dm(target, account, "Привет, можно спросить?")
        due = (base + dt.timedelta(seconds=index)).isoformat()
        dialog_store.set_stage(
            target,
            dialog_store.STAGE_PROMO_SENT,
            bump_outgoing=True,
            link_sent=True,
            auto_link_at=due,
        )
        expected.append(target)

    rows = dialog_store.list_due_auto_links(limit=3)
    assert [int(row["target_user_id"]) for row in rows] == expected[:3]


def test_startup_log_does_not_print_exact_channel_link(app_env):
    source = Path(__file__).resolve().parents[1].joinpath("main.py").read_text()
    assert 'logger.info("Exact CHANNEL_LINK configured: {}"' not in source
    assert 'logger.info("CHANNEL_LINK configured and validated")' in source


def test_floodwait_does_not_retry_every_scheduler_tick(app_env, monkeypatch):
    from telethon.errors import FloodWaitError
    from services import accounts, ai_dialog, dialog_engine, dialog_store

    class _FloodClient(_FakeClient):
        def __init__(self):
            super().__init__()
            self.attempts = 0

        async def send_message(self, entity, text):
            self.attempts += 1
            raise FloodWaitError(seconds=300)

    account = 6705
    target = 16740
    accounts.upsert_account(
        user_id=account,
        session_string="session",
        username="sender6705",
    )
    accounts.set_participates(account, True)
    dialog_store.create_after_first_dm(target, account, "Привет, можно спросить?")
    past = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)).isoformat()
    dialog_store.set_stage(
        target,
        dialog_store.STAGE_PROMO_SENT,
        bump_outgoing=True,
        link_sent=True,
        auto_link_at=past,
    )
    client = _FloodClient()
    _patch_delivery(monkeypatch, client)
    monkeypatch.setattr(
        ai_dialog,
        "generate_smoothing_apology",
        lambda history: _value("Сорян, не хотел навязываться. Просто поделился."),
    )

    assert asyncio.run(dialog_engine.process_due_auto_links()) == 0
    assert client.attempts == 1
    first_retry_at = dialog_store.get_dialog(target)["auto_link_at"]
    assert first_retry_at > dt.datetime.now(dt.timezone.utc).isoformat()

    assert asyncio.run(dialog_engine.process_due_auto_links()) == 0
    assert client.attempts == 1
