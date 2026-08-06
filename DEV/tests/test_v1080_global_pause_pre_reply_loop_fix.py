"""v1.0.80 regressions for global pause, PeerFlood and durable dialogs."""

from __future__ import annotations

import asyncio
import datetime as dt
from types import SimpleNamespace


def _seed_account(accounts, user_id: int):
    accounts.upsert_account(
        user_id=user_id,
        session_string=f"session-{user_id}",
        username=f"sender{user_id}",
    )
    accounts.set_participates(user_id, True)
    return accounts.get_account(user_id)


def _seed_due_followup(dialog_store, target: int, account: int) -> None:
    dialog_store.create_after_first_dm(target, account, "Привет, можно вопрос?")
    past = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)).isoformat()
    dialog_store.set_stage(
        target,
        dialog_store.STAGE_WAITING_REPLY,
        auto_link_at=past,
    )


class _Client:
    def __init__(self, *, error: BaseException | None = None):
        self.error = error
        self.sent: list[str] = []

    def is_connected(self):
        return True

    async def get_input_entity(self, value):
        return value

    async def send_message(self, entity, text):
        self.sent.append(str(text))
        if self.error is not None:
            raise self.error
        return SimpleNamespace(
            id=9000 + len(self.sent),
            date=dt.datetime.now(dt.timezone.utc),
        )


async def _none(*args, **kwargs):
    return None


def _patch_dialog_delivery(monkeypatch, client: _Client) -> None:
    from services import dialog_engine

    monkeypatch.setattr(dialog_engine.monitor_svc, "get_client", lambda account: client)
    monkeypatch.setattr(
        dialog_engine.monitor_svc,
        "maybe_disconnect_inactive_account",
        _none,
    )
    monkeypatch.setattr(dialog_engine, "_delay_reply", lambda: 0.0)
    monkeypatch.setattr(dialog_engine, "_auto_link_delay", lambda: 60)


def _outbox_count() -> int:
    from db.schema import get_connection

    row = get_connection().execute("SELECT COUNT(*) AS c FROM dialog_outbox").fetchone()
    return int(row["c"])


def test_global_pause_one_due_followup_sends_and_prepares_nothing(app_env, monkeypatch):
    from services import dialog_engine, dialog_store, runtime

    _seed_due_followup(dialog_store, 18001, 10801)
    runtime.set_worker_enabled(False)
    client = _Client()
    _patch_dialog_delivery(monkeypatch, client)

    assert asyncio.run(dialog_engine.process_due_followups()) == 0
    assert client.sent == []
    assert _outbox_count() == 0


def test_global_pause_one_hundred_due_followups_prepare_nothing(app_env, monkeypatch):
    from services import dialog_engine, dialog_store, runtime

    for index in range(100):
        _seed_due_followup(dialog_store, 18100 + index, 10802)
    runtime.set_worker_enabled(False)
    client = _Client()
    _patch_dialog_delivery(monkeypatch, client)

    assert asyncio.run(dialog_engine.process_due_followups(limit=200)) == 0
    assert client.sent == []
    assert _outbox_count() == 0


def test_global_pause_restart_holds_old_prepared_followup(app_env, monkeypatch):
    from db.schema import close_connection, db_lock, get_connection, init_db
    from services import dialog_delivery, dialog_engine, dialog_store, runtime

    target, account = 18003, 10803
    _seed_due_followup(dialog_store, target, account)
    runtime.set_worker_enabled(False)
    assert dialog_delivery.prepare(
        target,
        account,
        dialog_delivery.KIND_FOLLOWUP,
        "Старый follow-up",
    )
    with db_lock(), get_connection() as conn:
        conn.execute(
            "DELETE FROM schema_migrations WHERE name='v1_0_80_global_pause_pre_reply_loop_fix'"
        )
    close_connection()
    init_db()

    row = dialog_delivery.get(target, dialog_delivery.KIND_FOLLOWUP)
    assert row["status"] == dialog_delivery.STATUS_FAILED
    assert row["last_error"] == "v1.0.80_global_pause_pre_reply_hold"
    client = _Client()
    _patch_dialog_delivery(monkeypatch, client)
    assert asyncio.run(dialog_engine.process_due_followups()) == 0
    assert client.sent == []


def test_global_pause_real_incoming_text_sends_promo(app_env, monkeypatch):
    from services import ai_dialog, dialog_engine, dialog_store, runtime

    target, account = 18004, 10804
    dialog_store.create_after_first_dm(target, account, "Привет, можно вопрос?")
    runtime.set_worker_enabled(False)
    client = _Client()
    _patch_dialog_delivery(monkeypatch, client)

    async def classify(*args, **kwargs):
        return ai_dialog.CATEGORY_NORMAL

    async def promo(*args, **kwargs):
        return "Вот канал, который хотел показать: https://t.me/+testhash"

    monkeypatch.setattr(ai_dialog, "classify_user_message", classify)
    monkeypatch.setattr(ai_dialog, "generate_promo", promo)
    asyncio.run(
        dialog_engine.handle_incoming_private(
            account,
            target,
            "Да, слушаю",
            telegram_message_id=1800401,
        )
    )

    assert len(client.sent) == 1
    assert "https://t.me/+testhash" in client.sent[0]
    assert dialog_store.get_dialog(target)["stage"] == dialog_store.STAGE_PROMO_SENT


def test_global_pause_real_incoming_emoji_sends_promo(app_env, monkeypatch):
    from services import ai_dialog, dialog_engine, dialog_store, runtime

    target, account = 18005, 10805
    dialog_store.create_after_first_dm(target, account, "Привет, можно вопрос?")
    runtime.set_worker_enabled(False)
    client = _Client()
    _patch_dialog_delivery(monkeypatch, client)

    async def classify(*args, **kwargs):
        return ai_dialog.CATEGORY_NORMAL

    async def promo(*args, **kwargs):
        return "Глянь, может пригодиться: https://t.me/+testhash"

    monkeypatch.setattr(ai_dialog, "classify_user_message", classify)
    monkeypatch.setattr(ai_dialog, "generate_promo", promo)
    asyncio.run(
        dialog_engine.handle_incoming_private(
            account,
            target,
            "🙂",
            telegram_message_id=1800501,
            content_kind="emoji",
        )
    )

    assert len(client.sent) == 1
    assert dialog_store.has_incoming_reply(target, account)


def test_real_reply_promo_continues_to_apology_and_link_help_on_pause(app_env, monkeypatch):
    from services import ai_dialog, dialog_engine, dialog_store, runtime

    target, account = 18006, 10806
    dialog_store.create_after_first_dm(target, account, "Привет, можно вопрос?")
    runtime.set_worker_enabled(False)
    client = _Client()
    _patch_dialog_delivery(monkeypatch, client)

    async def classify(*args, **kwargs):
        return ai_dialog.CATEGORY_NORMAL

    async def promo(*args, **kwargs):
        return "Вот ссылка: https://t.me/+testhash"

    async def apology(*args, **kwargs):
        return "Сорян, не хотел навязываться."

    async def link_help(*args, **kwargs):
        return "Закрой панель крестиком и нажми ссылку ещё раз."

    monkeypatch.setattr(ai_dialog, "classify_user_message", classify)
    monkeypatch.setattr(ai_dialog, "generate_promo", promo)
    monkeypatch.setattr(ai_dialog, "generate_smoothing_apology", apology)
    monkeypatch.setattr(ai_dialog, "generate_link_open_help", link_help)

    asyncio.run(
        dialog_engine.handle_incoming_private(
            account,
            target,
            "Ок",
            telegram_message_id=1800601,
        )
    )
    past = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)).isoformat()
    dialog_store.set_stage(target, dialog_store.STAGE_PROMO_SENT, auto_link_at=past)
    assert asyncio.run(dialog_engine.process_due_auto_links()) == 1
    dialog_store.set_stage(target, dialog_store.STAGE_APOLOGY_SENT, auto_link_at=past)
    assert asyncio.run(dialog_engine.process_due_auto_links()) == 1

    assert len(client.sent) == 3
    assert dialog_store.get_dialog(target)["stage"] == dialog_store.STAGE_LINK_HELP_SENT


def test_new_queue_lead_stays_unsent_during_global_pause(app_env, monkeypatch):
    from services import accounts, dispatcher, queue, runtime

    account, target = 10807, 18007
    acc = _seed_account(accounts, account)
    queue.upsert_from_activity(
        target_user_id=target,
        username="target18007",
        first_name="Target",
        access_hash=12345,
        source_chat_id=-10018007,
        source_account_user_id=account,
    )
    lead = queue.claim_random_pending(account)
    runtime.set_worker_enabled(False)
    sends: list[int] = []

    async def send(*args, **kwargs):
        sends.append(1)
        return "sent"

    monkeypatch.setattr(dispatcher, "_send_first_dm", send)
    assert asyncio.run(
        dispatcher._attempt_lead_across_accounts(
            lead,
            [acc],
            text="Привет",
            enforce_global_pause=True,
        )
    ) is False
    assert sends == []
    assert queue.get_lead(target)["status"] == queue.STATUS_PENDING


def test_peerflood_one_main_notification_per_active_incident(app_env, monkeypatch):
    from services import accounts, runtime, spambot

    account = 10808
    _seed_account(accounts, account)
    runtime.set_peer_flood_range_seconds(60, 60)
    now = dt.datetime(2026, 8, 6, 6, 0, tzinfo=dt.timezone.utc)
    monkeypatch.setattr(spambot, "_now", lambda: now)
    monkeypatch.setattr(accounts, "_now", lambda: now)
    monkeypatch.setattr(accounts, "_now_iso", lambda: now.isoformat())
    notices: list[str] = []

    async def notify(text: str):
        notices.append(text)

    async def check(*args, **kwargs):
        return None

    monkeypatch.setattr(spambot, "notify_admins", notify)
    monkeypatch.setattr(spambot, "check_account", check)
    asyncio.run(spambot.on_peer_flood(account))
    asyncio.run(spambot.on_peer_flood(account))

    assert len(notices) == 1
    assert "ОБНАРУЖЕН PEERFLOOD" in notices[0]


def test_spambot_free_notifies_resume_once(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import accounts, pacing, runtime, spambot

    account = 10809
    _seed_account(accounts, account)
    runtime.set_worker_enabled(False)
    now = dt.datetime(2026, 8, 6, 6, 10, tzinfo=dt.timezone.utc)
    monkeypatch.setattr(spambot, "_now", lambda: now)
    monkeypatch.setattr(pacing, "random_account_interval_seconds", lambda acc=None: 180)
    with db_lock(), get_connection() as conn:
        conn.execute(
            "UPDATE accounts SET is_paused=1, pause_reason='PeerFlood', cooldown_until=? WHERE user_id=?",
            ((now - dt.timedelta(seconds=1)).isoformat(), account),
        )
    spambot._upsert_state(
        account,
        status=spambot.STATUS_FREE_PENDING,
        next_check_at=now.isoformat(),
        last_reply="free",
    )
    notices: list[str] = []

    async def notify(text: str):
        notices.append(text)

    monkeypatch.setattr(spambot, "notify_admins", notify)
    monkeypatch.setattr(spambot.monitor_svc, "refresh_monitor", _none)

    assert asyncio.run(spambot.process_due_checks()) == 1
    assert asyncio.run(spambot.process_due_checks()) == 0
    assert len(notices) == 1
    assert "ТРАНСПОРТНАЯ ПАУЗА" in notices[0]


def test_reprocessing_stale_free_pending_creates_no_notification(app_env, monkeypatch):
    from services import accounts, spambot

    account = 10810
    _seed_account(accounts, account)
    now = dt.datetime(2026, 8, 6, 6, 20, tzinfo=dt.timezone.utc)
    monkeypatch.setattr(spambot, "_now", lambda: now)
    spambot._upsert_state(
        account,
        status=spambot.STATUS_FREE_PENDING,
        next_check_at=now.isoformat(),
        last_reply="stale free",
    )
    notices: list[str] = []

    async def notify(text: str):
        notices.append(text)

    monkeypatch.setattr(spambot, "notify_admins", notify)
    monkeypatch.setattr(spambot.monitor_svc, "refresh_monitor", _none)
    assert asyncio.run(spambot.process_due_checks()) == 0
    assert notices == []
    assert spambot.get_state(account)["status"] == spambot.STATUS_IDLE


def test_same_followup_peerflood_gets_persistent_six_hour_hold(app_env, monkeypatch):
    from services import dialog_delivery, dialog_engine, dialog_store, runtime, spambot

    class FakePeerFlood(Exception):
        pass

    target, account = 18011, 10811
    _seed_due_followup(dialog_store, target, account)
    runtime.set_worker_enabled(True)
    client = _Client(error=FakePeerFlood("peer flood"))
    _patch_dialog_delivery(monkeypatch, client)
    monkeypatch.setattr(dialog_engine, "PeerFloodError", FakePeerFlood)
    monkeypatch.setattr(spambot, "on_peer_flood", _none)

    assert asyncio.run(dialog_engine.process_due_followups()) == 0
    row = dialog_delivery.get(target, dialog_delivery.KIND_FOLLOWUP)
    retry_at = dt.datetime.fromisoformat(str(row["recovery_next_at"]).replace("Z", "+00:00"))
    assert row["status"] == dialog_delivery.STATUS_FAILED
    assert retry_at > dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5)
    assert asyncio.run(dialog_engine.process_due_followups()) == 0
    assert len(client.sent) == 1


def test_fifth_peerflood_adds_extension_once(app_env, monkeypatch):
    from services import accounts, runtime, spambot

    account = 10812
    _seed_account(accounts, account)
    runtime.set_peer_flood_range_seconds(60, 60)
    runtime.set_peer_flood_burst_extra_seconds(600)
    now = dt.datetime(2026, 8, 6, 6, 30, tzinfo=dt.timezone.utc)
    monkeypatch.setattr(spambot, "_now", lambda: now)
    monkeypatch.setattr(accounts, "_now", lambda: now)
    monkeypatch.setattr(accounts, "_now_iso", lambda: now.isoformat())
    notices: list[str] = []

    async def notify(text: str):
        notices.append(text)

    async def check(*args, **kwargs):
        return None

    monkeypatch.setattr(spambot, "notify_admins", notify)
    monkeypatch.setattr(spambot, "check_account", check)
    for _ in range(5):
        asyncio.run(spambot.on_peer_flood(account))
    after_fifth = accounts.get_account(account)["cooldown_until"]
    asyncio.run(spambot.on_peer_flood(account))
    after_sixth = accounts.get_account(account)["cooldown_until"]

    assert after_sixth == after_fifth
    assert len([n for n in notices if "5 PeerFlood за 10 минут" in n]) == 1


def test_manual_resume_still_clears_transport_pause(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import accounts, spambot

    account = 10813
    _seed_account(accounts, account)
    future = dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5)
    with db_lock(), get_connection() as conn:
        conn.execute(
            "UPDATE accounts SET is_paused=1, pause_reason='PeerFlood', cooldown_until=?, next_send_at=? WHERE user_id=?",
            (future.isoformat(), future.isoformat(), account),
        )
    monkeypatch.setattr(spambot, "notify_admins", _none)
    monkeypatch.setattr(spambot.monitor_svc, "refresh_monitor", _none)
    assert asyncio.run(spambot.resume_account(account, source="manual")) is True
    row = accounts.get_account(account)
    assert row["is_paused"] == 0
    assert row["cooldown_until"] is None
    assert row["next_send_at"] is None


def test_authorization_lost_account_is_not_auto_resumed(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import account_auth, accounts, spambot

    account = 10814
    _seed_account(accounts, account)
    now = dt.datetime(2026, 8, 6, 6, 40, tzinfo=dt.timezone.utc)
    monkeypatch.setattr(spambot, "_now", lambda: now)
    with db_lock(), get_connection() as conn:
        conn.execute(
            "UPDATE accounts SET auth_status=?, is_paused=1, pause_reason='PeerFlood', cooldown_until=? WHERE user_id=?",
            (account_auth.AUTH_REAUTH_REQUIRED, (now - dt.timedelta(seconds=1)).isoformat(), account),
        )
    spambot._upsert_state(
        account,
        status=spambot.STATUS_FREE_PENDING,
        next_check_at=now.isoformat(),
        last_reply="free",
    )
    notices: list[str] = []

    async def notify(text: str):
        notices.append(text)

    monkeypatch.setattr(spambot, "notify_admins", notify)
    assert asyncio.run(spambot.process_due_checks()) == 0
    assert accounts.get_account(account)["is_paused"] == 1
    assert notices == []


def test_pending_incoming_ignores_first_dm_cooldown_and_processes_once(
    app_env, monkeypatch
):
    from db.schema import db_lock, get_connection
    from services import accounts, dialog_engine, dialog_inbox, dialog_store

    account, target = 10815, 18015
    _seed_account(accounts, account)
    dialog_store.create_after_first_dm(target, account, "Привет, можно вопрос?")
    future = dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=2)
    with db_lock(), get_connection() as conn:
        conn.execute(
            "UPDATE accounts SET is_paused=1, pause_reason='PeerFlood', cooldown_until=? WHERE user_id=?",
            (future.isoformat(), account),
        )
    monkeypatch.setattr(dialog_engine.monitor_svc, "get_client", lambda uid: object())
    processed: list[str] = []

    async def body(account_id, target_id, text, **kwargs):
        processed.append(text)

    monkeypatch.setattr(dialog_engine, "_handle_incoming_private_body", body)
    asyncio.run(
        dialog_engine.handle_incoming_private(
            account,
            target,
            "Сообщение",
            telegram_message_id=1801501,
        )
    )
    assert dialog_inbox.count_by_status(dialog_inbox.STATUS_PENDING) == 0
    assert dialog_inbox.count_by_status(dialog_inbox.STATUS_DONE) == 1
    assert processed == ["Сообщение"]
    assert asyncio.run(dialog_engine.recover_pending_incoming_messages()) == 0
    row = accounts.get_account(account)
    assert row["is_paused"] == 1
    assert row["pause_reason"] == "PeerFlood"


def test_crash_recovery_commits_one_outbox_without_duplicate_send(app_env, monkeypatch):
    from services import dialog_delivery, dialog_engine, dialog_inbox, dialog_store

    account, target = 10816, 18016
    dialog_store.create_after_first_dm(target, account, "Привет, можно вопрос?")
    inbox_id = dialog_inbox.enqueue(
        account,
        target,
        "Да",
        telegram_message_id=1801601,
    )
    action = dialog_delivery.inbox_action_key(dialog_delivery.KIND_PROMO, inbox_id)
    text = "Вот ссылка: https://t.me/+testhash"
    assert dialog_delivery.prepare(
        target,
        account,
        action,
        text,
        message_kind=dialog_delivery.KIND_PROMO,
        transition={
            "stage": dialog_store.STAGE_PROMO_SENT,
            "bump_outgoing": True,
            "link_sent": True,
            "append_history": True,
        },
        source_inbox_id=inbox_id,
    )
    client = _Client()
    _patch_dialog_delivery(monkeypatch, client)

    async def found(*args, **kwargs):
        return SimpleNamespace(id=777, date=dt.datetime.now(dt.timezone.utc))

    monkeypatch.setattr(dialog_engine.telegram_history, "find_outgoing_text_since", found)
    result = asyncio.run(
        dialog_engine._deliver_inbox_message(
            account,
            target,
            text,
            message_kind=dialog_delivery.KIND_PROMO,
            source_inbox_id=inbox_id,
            transition={
                "stage": dialog_store.STAGE_PROMO_SENT,
                "bump_outgoing": True,
                "link_sent": True,
                "append_history": True,
            },
        )
    )
    assert result == "sent"
    assert client.sent == []
    assert _outbox_count() == 1


def test_dialog_stays_pinned_to_original_account(app_env):
    from services import dialog_engine, dialog_inbox, dialog_store

    owner, wrong, target = 10817, 10818, 18017
    dialog_store.create_after_first_dm(target, owner, "Привет, можно вопрос?")
    asyncio.run(
        dialog_engine.handle_incoming_private(
            wrong,
            target,
            "Ответ",
            telegram_message_id=1801701,
        )
    )
    assert dialog_inbox.count_by_status(dialog_inbox.STATUS_PENDING) == 0
    assert dialog_store.get_dialog(target)["account_user_id"] == owner


def test_aggressive_text_refusal_still_opts_out_during_pause(app_env, monkeypatch):
    from services import ai_dialog, dialog_engine, dialog_store, opt_out, runtime

    account, target = 10819, 18019
    dialog_store.create_after_first_dm(target, account, "Привет, можно вопрос?")
    runtime.set_worker_enabled(False)
    client = _Client()
    _patch_dialog_delivery(monkeypatch, client)
    monkeypatch.setattr(ai_dialog, "is_hard_stop", lambda text: True)
    asyncio.run(
        dialog_engine.handle_incoming_private(
            account,
            target,
            "Не пиши мне больше",
            telegram_message_id=1801901,
        )
    )
    assert opt_out.is_opted_out(target)
    assert dialog_store.get_dialog(target)["stage"] == dialog_store.STAGE_CLOSED
    assert all("t.me/" not in text for text in client.sent)


def test_calm_text_refusal_keeps_soft_promo_branch_during_pause(app_env, monkeypatch):
    from services import ai_dialog, dialog_engine, dialog_store, opt_out, runtime

    account, target = 10820, 18020
    dialog_store.create_after_first_dm(target, account, "Привет, можно вопрос?")
    runtime.set_worker_enabled(False)
    client = _Client()
    _patch_dialog_delivery(monkeypatch, client)

    async def classify(*args, **kwargs):
        return ai_dialog.CATEGORY_SOFT_REFUSAL

    async def promo(*args, **kwargs):
        return "Понимаю. Просто оставлю ссылку: https://t.me/+testhash"

    monkeypatch.setattr(ai_dialog, "classify_user_message", classify)
    monkeypatch.setattr(ai_dialog, "generate_promo", promo)
    asyncio.run(
        dialog_engine.handle_incoming_private(
            account,
            target,
            "Нет, спасибо",
            telegram_message_id=1802001,
        )
    )
    assert not opt_out.is_opted_out(target)
    assert len(client.sent) == 1
    assert "https://t.me/+testhash" in client.sent[0]


def test_non_text_incoming_continues_promo_branch_during_pause(app_env, monkeypatch):
    from services import ai_dialog, dialog_engine, dialog_store, runtime

    account, target = 10821, 18021
    dialog_store.create_after_first_dm(target, account, "Привет, можно вопрос?")
    runtime.set_worker_enabled(False)
    client = _Client()
    _patch_dialog_delivery(monkeypatch, client)

    async def classify(*args, **kwargs):
        assert kwargs["content_kind"] == "sticker"
        return ai_dialog.CATEGORY_NORMAL

    async def promo(*args, **kwargs):
        return "Можешь посмотреть здесь: https://t.me/+testhash"

    monkeypatch.setattr(ai_dialog, "classify_user_message", classify)
    monkeypatch.setattr(ai_dialog, "generate_promo", promo)
    asyncio.run(
        dialog_engine.handle_incoming_private(
            account,
            target,
            "",
            telegram_message_id=1802101,
            content_kind="sticker",
        )
    )
    assert len(client.sent) == 1
    assert dialog_store.has_incoming_reply(target, account)
