from __future__ import annotations

import pytest


def _add_account(user_id: int = 100, *, participates: bool = True):
    from services import accounts

    accounts.upsert_account(
        user_id=user_id,
        session_string=f"session-{user_id}",
        phone="+79000000000",
        username=f"user{user_id}",
    )
    accounts.set_participates(user_id, participates)
    return accounts.get_account(user_id)


def test_auth_columns_and_reauth_state_are_persistent(app_env):
    from db.schema import get_connection
    from services import account_auth, accounts

    _add_account(100)
    state = account_auth.mark_reauth_required(100, "session_not_authorized")

    assert state["transitioned"] is True
    row = accounts.get_account(100)
    assert row["auth_status"] == account_auth.AUTH_REAUTH_REQUIRED
    assert row["auth_error"] == "session_not_authorized"
    assert row["auth_lost_at"]
    assert accounts.count_reauth_required() == 1
    assert accounts.count_participating() == 0

    columns = {
        row[1] for row in get_connection().execute("PRAGMA table_info(accounts)")
    }
    assert {"auth_status", "auth_error", "auth_lost_at", "auth_notified_at"} <= columns


def test_auth_loss_notification_has_action_buttons_and_is_deduplicated(
    app_env, monkeypatch
):
    import config
    from services import account_auth, accounts

    _add_account(101)
    sent = []

    async def fake_send_message(admin_id, text, **kwargs):
        sent.append((admin_id, text, kwargs))

    monkeypatch.setattr(config.bot, "send_message", fake_send_message, raising=False)

    account_auth.mark_reauth_required(101, "SessionRevokedError")

    import asyncio

    assert asyncio.run(account_auth.notify_reauth_required(101)) is True
    assert asyncio.run(account_auth.notify_reauth_required(101)) is False
    assert len(sent) == 1
    text = sent[0][1]
    buttons = sent[0][2]["buttons"]
    flat = [row[0][1] for row in buttons]
    assert "АККАУНТ ПОТЕРЯЛ ВХОД" in text
    assert flat == ["🔑 ПЕРЕЗАЙТИ", "🗑 УДАЛИТЬ АККАУНТ", "👤 ОТКРЫТЬ АККАУНТ"]
    assert accounts.get_account(101)["auth_notified_at"]


def test_relogin_session_refresh_preserves_first_dm_setting(app_env):
    from services import account_auth, accounts

    _add_account(102, participates=True)
    account_auth.mark_reauth_required(102, "session_not_authorized")

    accounts.upsert_account(
        user_id=102,
        session_string="new-session",
        phone="+79000000000",
        username="user102",
    )
    row = accounts.get_account(102)
    assert row["session_string"] == "new-session"
    assert row["participates"] == 1
    assert row["auth_status"] == account_auth.AUTH_AUTHORIZED
    assert accounts.count_participating() == 1


def test_dashboard_and_main_menu_surface_problem_account(app_env):
    from services import account_auth, accounts

    _add_account(103)
    account_auth.mark_reauth_required(103, "session_not_authorized")

    line = accounts.dashboard_account_line(accounts.get_account(103))
    assert line.startswith("🔴")
    assert "Требуется повторный вход" in line

    from handlers import menu

    rows = menu._main_menu_buttons()
    labels = [button[1] for row in rows for button in row]
    assert any("НУЖЕН ВХОД" in label for label in labels)


@pytest.mark.asyncio
async def test_health_check_quarantines_unauthorized_connected_client(
    app_env, monkeypatch
):
    import config
    from services import account_auth, accounts, monitor

    _add_account(104)
    sent = []

    async def fake_send_message(admin_id, text, **kwargs):
        sent.append((admin_id, text, kwargs))

    monkeypatch.setattr(config.bot, "send_message", fake_send_message, raising=False)

    class FakeClient:
        def is_connected(self):
            return True

        async def is_user_authorized(self):
            return False

        async def disconnect(self):
            return None

    monitor._clients.clear()
    monitor._clients[104] = FakeClient()
    lost = await monitor.check_authorization_health(force=True)

    assert lost == 1
    assert 104 not in monitor.connected_account_ids()
    assert accounts.get_account(104)["auth_status"] == account_auth.AUTH_REAUTH_REQUIRED
    assert len(sent) == 1


def test_auth_loss_error_classifier_is_strict(app_env):
    from services import account_auth

    SessionRevokedError = type("SessionRevokedError", (Exception,), {})
    TimeoutErrorX = type("TimeoutError", (Exception,), {})

    assert account_auth.is_auth_loss_error(SessionRevokedError("revoked")) is True
    assert account_auth.is_auth_loss_error(TimeoutErrorX("network timeout")) is False
