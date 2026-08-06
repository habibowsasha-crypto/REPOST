"""v1.0.69 First DM admin notification includes the exact delivered text."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace


def _seed_account_and_claimed_lead(account_id: int, target_id: int):
    from services import accounts, queue

    accounts.upsert_account(
        user_id=account_id,
        session_string=f"session-{account_id}",
        username=f"sender{account_id}",
        first_name="Sender",
    )
    accounts.set_participates(account_id, True)
    queue.upsert_from_activity(
        target_user_id=target_id,
        username=f"target{target_id}",
        first_name="Target",
        source_chat_id=-100123456,
        source_account_user_id=account_id,
        access_hash=target_id * 10,
    )
    lead = queue.claim_random_pending(account_id)
    assert lead is not None
    return lead


def test_notification_contains_exact_delivered_first_dm(app_env, monkeypatch):
    from services import dispatcher, queue

    account_id = 6901
    target_id = 7901
    lead = _seed_account_and_claimed_lead(account_id, target_id)
    exact_text = "Привет, можно один вопрос?"

    monkeypatch.setattr(queue, "count_first_dm_today", lambda: 88)
    monkeypatch.setattr(queue, "count_first_dm_total", lambda: 245)

    notification = dispatcher._notify_first_dm(account_id, lead, exact_text)

    assert "📨 **FIRST DM ОТПРАВЛЕН**" in notification
    assert "💬 **Отправленный First DM:**" in notification
    assert exact_text in notification
    assert notification.count(exact_text) == 1
    assert "📬 Сегодня: **88**" in notification
    assert "📊 Всего: **245**" in notification


def test_success_notification_receives_the_same_text_sent_to_telegram(app_env, monkeypatch):
    from services import dispatcher

    account_id = 6902
    target_id = 7902
    lead = _seed_account_and_claimed_lead(account_id, target_id)
    exact_text = "Привет, не отвлекаю?"
    sent_texts: list[str] = []
    notified: list[tuple[int, dict, str]] = []

    class Client:
        async def send_message(self, entity, text):
            sent_texts.append(text)
            return SimpleNamespace(id=12345, date=None)

    async def notify(account, current_lead, text):
        notified.append((account, current_lead, text))

    monkeypatch.setattr(dispatcher, "_notify_admins_first_dm", notify)

    result = asyncio.run(
        dispatcher._send_first_dm(
            Client(), account_id, lead, exact_text, entity=object()
        )
    )

    assert result == "sent"
    assert sent_texts == [exact_text]
    assert len(notified) == 1
    assert notified[0][0] == account_id
    assert int(notified[0][1]["target_user_id"]) == target_id
    assert notified[0][2] == sent_texts[0]


def test_failed_first_dm_does_not_create_success_notification(app_env, monkeypatch):
    from services import dispatcher

    account_id = 6903
    target_id = 7903
    lead = _seed_account_and_claimed_lead(account_id, target_id)
    notified: list[str] = []

    class FakePeerIdInvalidError(Exception):
        pass

    class Client:
        async def send_message(self, entity, text):
            raise FakePeerIdInvalidError("invalid peer")

    async def notify(account, current_lead, text):
        notified.append(text)

    monkeypatch.setattr(dispatcher, "PeerIdInvalidError", FakePeerIdInvalidError)
    monkeypatch.setattr(dispatcher, "_notify_admins_first_dm", notify)

    result = asyncio.run(
        dispatcher._send_first_dm(
            Client(), account_id, lead, "Привет, можно спросить?", entity=object()
        )
    )

    assert result == "peer_invalid"
    assert notified == []


def test_later_dialog_delivery_has_no_routine_admin_message_notification():
    """Only First DM keeps the routine success notification."""
    from pathlib import Path

    test_file = Path(__file__).resolve()
    root = test_file.parents[1]
    if not (root / "services").exists():
        root = test_file.parents[2]
    for relative in (
        "services/dialog_delivery.py",
        "services/dialog_engine.py",
        "services/dialog_inbox.py",
    ):
        source = (root / relative).read_text(encoding="utf-8")
        assert "_notify_admins_first_dm" not in source
        assert "FIRST DM ОТПРАВЛЕН" not in source
