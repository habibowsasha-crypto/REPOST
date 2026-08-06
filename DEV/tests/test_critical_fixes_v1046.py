"""Regression tests for the v1.0.46 critical fixes."""

from __future__ import annotations

import asyncio
from pathlib import Path


def test_direct_refusal_is_apologetic_and_stops(app_env):
    from services import ai_dialog

    assert ai_dialog.is_hard_stop("Больше не пиши мне")
    assert ai_dialog.is_hard_stop("не надо писать")
    for _ in range(10):
        text = ai_dialog.soft_close_text().lower()
        assert "извини" in text


def test_optout_deactivates_scheduled_dialog(app_env):
    from services import accounts as accounts_svc
    from services import dialog_store as dialogs
    from services import opt_out
    from services import queue

    accounts_svc.upsert_account(user_id=1, session_string="s")
    queue.upsert_from_activity(target_user_id=100, source_account_user_id=1)
    queue.claim_random_pending(1)
    queue.mark_sending(100, 1)
    queue.mark_sent(100, 1)
    dialogs.create_after_first_dm(100, 1, "Можно спросить?")
    dialogs.set_stage(100, dialogs.STAGE_EXPLAINED, auto_link_at="2000-01-01T00:00:00+00:00")

    opt_out.add(100, "user_stop")
    dialog = dialogs.get_dialog(100)
    assert dialog["stage"] == dialogs.STAGE_CLOSED
    assert dialog["auto_link_at"] is None
    assert dialogs.list_due_auto_links() == []


def test_detailed_import_export_preserves_entity_data(app_env):
    from services import audience
    from services import queue

    records = audience.parse_import_text(
        "user_id,username,access_hash,first_name,last_name,source_chat_id,source_account_user_id\n"
        "777,trader777,987654321,Ivan,Ivanov,-100123,42\n"
    )
    stats = audience.import_records(records)
    assert stats["queued"] == 1
    assert stats["with_username"] == 1
    assert stats["with_access_hash"] == 1
    assert stats["with_source_account"] == 1

    lead = queue.claim_random_pending(42)
    assert int(lead["target_user_id"]) == 777
    assert int(lead["access_hash"]) == 987654321
    assert int(lead["source_account_user_id"]) == 42

    exported = audience.export_csv_text(only_with_dm=False)
    assert "trader777" in exported
    assert "987654321" in exported
    assert "source_account_user_id" in exported.splitlines()[0]


def test_source_account_is_always_first(app_env):
    from services.dispatcher import _order_accounts_for_lead

    ready = [{"user_id": 1}, {"user_id": 2}, {"user_id": 3}]
    for _ in range(30):
        ordered = _order_accounts_for_lead({"source_account_user_id": 2}, ready)
        assert int(ordered[0]["user_id"]) == 2


def test_only_exact_admin_link_survives(app_env):
    from services import ai_dialog

    result = ai_dialog._enforce_admin_link(
        "Глянь https://evil.example и https://t.me/+testhash", include_link=True
    )
    assert "evil.example" not in result
    assert result.count("https://t.me/+testhash") == 1

    generated = asyncio.run(ai_dialog.generate_link_wrap([]))
    assert generated.count("https://t.me/+testhash") == 1
    assert "ссылка не задана" not in generated.lower()


def test_delete_archives_history_but_deactivates_dialog(app_env):
    from services import accounts as accounts_svc
    from services import dialog_store as dialogs
    from services import queue

    accounts_svc.upsert_account(user_id=9, session_string="secret")
    queue.upsert_from_activity(target_user_id=909, source_account_user_id=9)
    queue.claim_random_pending(9)
    queue.mark_sending(909, 9)
    queue.mark_sent(909, 9)
    dialogs.create_after_first_dm(909, 9, "Первое сообщение")
    dialogs.append_history(909, "user", "Ответ")

    assert accounts_svc.delete_account(9)
    assert accounts_svc.get_account(9) is None
    dialog = dialogs.get_dialog(909)
    assert dialog is not None
    assert dialog["stage"] == dialogs.STAGE_CLOSED
    assert dialog["auto_link_at"] is None
    assert any(item["text"] == "Ответ" for item in dialog["history"])


def test_pause_gate_blocks_autonomous_pre_reply_workers(app_env):
    src = Path("services/dialog_engine.py").read_text(encoding="utf-8")
    assert "_global_pre_reply_blocked" in src
    followup_part = src.split("async def process_due_followups", 1)[1]
    assert "if not runtime_svc.is_worker_enabled():" in followup_part
    assert "Main-menu pause stops only new First DM" not in src


def test_disabled_account_stays_connected_until_dialog_closes(app_env, monkeypatch):
    from services import accounts as accounts_svc
    from services import dialog_store as dialogs
    from services import monitor

    class FakeClient:
        def __init__(self, *args, **kwargs):
            self.connected = False

        async def connect(self):
            self.connected = True

        async def is_user_authorized(self):
            return True

        def on(self, *args, **kwargs):
            return lambda func: func

        def is_connected(self):
            return self.connected

        async def disconnect(self):
            self.connected = False

    monkeypatch.setattr(monitor, "TelegramClient", FakeClient)
    accounts_svc.upsert_account(user_id=123, session_string="session")
    accounts_svc.set_participates(123, False)
    dialogs.create_after_first_dm(555, 123, "Первое")

    asyncio.run(monitor.start_monitor())
    assert 123 in monitor.connected_account_ids()

    dialogs.set_stage(555, dialogs.STAGE_CLOSED, clear_auto_link=True)
    disconnected = asyncio.run(monitor.maybe_disconnect_inactive_account(123))
    assert disconnected is True
    assert 123 not in monitor.connected_account_ids()
