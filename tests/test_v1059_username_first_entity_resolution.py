"""Regression coverage for v1.0.59 username-first Telegram entity resolution."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace


class _EntityClient:
    def __init__(self, responses):
        self.responses = dict(responses)
        self.calls: list[object] = []

    async def get_input_entity(self, value):
        self.calls.append(value)
        response = self.responses[value]
        if isinstance(response, BaseException):
            raise response
        return response


def test_username_is_resolved_before_id_and_source_access_hash(app_env):
    from services import dispatcher

    client = _EntityClient(
        {
            "leadname": SimpleNamespace(user_id=5001, route="username"),
            5001: SimpleNamespace(user_id=5001, route="id"),
        }
    )
    lead = {
        "target_user_id": 5001,
        "username": "@leadname",
        "access_hash": 123456,
        "source_account_user_id": 6001,
    }

    entity = asyncio.run(dispatcher._resolve_target_entity(client, 6001, lead))

    assert entity.route == "username"
    assert client.calls == ["leadname"]


def test_changed_or_recycled_username_is_rejected_before_id_fallback(app_env):
    from services import dispatcher

    client = _EntityClient(
        {
            "oldname": SimpleNamespace(user_id=9999, route="wrong_username"),
            5002: SimpleNamespace(user_id=5002, route="id"),
        }
    )
    lead = {
        "target_user_id": 5002,
        "username": "oldname",
        "access_hash": 222222,
        "source_account_user_id": 6002,
    }

    entity = asyncio.run(dispatcher._resolve_target_entity(client, 6002, lead))

    assert entity.route == "id"
    assert client.calls == ["oldname", 5002]


def test_unavailable_username_falls_back_to_account_cached_id(app_env):
    from services import dispatcher

    client = _EntityClient(
        {
            "missingname": ValueError("username unavailable"),
            5003: SimpleNamespace(user_id=5003, route="id"),
        }
    )
    lead = {
        "target_user_id": 5003,
        "username": "missingname",
        "access_hash": 333333,
        "source_account_user_id": 6003,
    }

    entity = asyncio.run(dispatcher._resolve_target_entity(client, 6003, lead))

    assert entity.route == "id"
    assert client.calls == ["missingname", 5003]


def test_source_access_hash_is_used_only_after_username_and_id_fail(app_env):
    from services import dispatcher

    client = _EntityClient(
        {
            "sourcelead": ValueError("username unavailable"),
            5004: ValueError("id unavailable"),
        }
    )
    lead = {
        "target_user_id": 5004,
        "username": "sourcelead",
        "access_hash": 444444,
        "source_account_user_id": 6004,
    }

    entity = asyncio.run(dispatcher._resolve_target_entity(client, 6004, lead))

    assert int(entity.user_id) == 5004
    assert int(entity.access_hash) == 444444
    assert client.calls == ["sourcelead", 5004]


def test_foreign_account_never_reuses_source_account_access_hash(app_env):
    from services import dispatcher

    client = _EntityClient(
        {
            "foreignlead": ValueError("username unavailable"),
            5005: ValueError("id unavailable"),
        }
    )
    lead = {
        "target_user_id": 5005,
        "username": "foreignlead",
        "access_hash": 555555,
        "source_account_user_id": 6005,
    }

    entity = asyncio.run(dispatcher._resolve_target_entity(client, 6006, lead))

    assert entity is None
    assert client.calls == ["foreignlead", 5005]


def test_real_lookup_failure_is_not_hidden_as_no_entity(app_env):
    from services import dispatcher

    class FloodLikeError(Exception):
        pass

    client = _EntityClient(
        {
            "leaderror": FloodLikeError("temporary Telegram failure"),
            5006: SimpleNamespace(user_id=5006),
        }
    )
    lead = {
        "target_user_id": 5006,
        "username": "leaderror",
        "access_hash": 666666,
        "source_account_user_id": 6006,
    }

    try:
        asyncio.run(dispatcher._resolve_target_entity(client, 6006, lead))
    except FloodLikeError:
        pass
    else:
        raise AssertionError("Temporary Telegram lookup failures must propagate")

    assert client.calls == ["leaderror"]


def test_non_source_account_can_send_shared_queue_lead_via_username(app_env, monkeypatch):
    import datetime as dt

    from services import accounts, dispatcher, monitor, queue

    accounts.upsert_account(
        user_id=7002,
        session_string="test-session-7002",
        username="sender7002",
    )
    accounts.set_participates(7002, True)
    queue.upsert_from_activity(
        target_user_id=8001,
        username="sharedlead",
        access_hash=888888,
        source_account_user_id=7001,
    )
    lead = queue.claim_random_pending(7002)
    assert lead is not None

    calls: list[object] = []
    sent: list[tuple[object, str]] = []

    class Client:
        def is_connected(self):
            return True

        async def get_input_entity(self, value):
            calls.append(value)
            assert value == "sharedlead"
            return SimpleNamespace(user_id=8001, route="username")

        async def send_message(self, entity, text):
            sent.append((entity, text))
            return SimpleNamespace(id=91, date=dt.datetime.now(dt.timezone.utc))

    async def generated():
        return "Как рынок сегодня видишь?"

    async def notify(*args, **kwargs):
        return None

    client = Client()
    monkeypatch.setattr(monitor, "get_client", lambda account_id: client)
    monkeypatch.setattr(dispatcher, "generate_first_dm", generated)
    monkeypatch.setattr(dispatcher, "_notify_admins_first_dm", notify)

    result = asyncio.run(
        dispatcher._attempt_lead_across_accounts(
            lead,
            [{"user_id": 7002, "participates": 1, "session_string": "s2"}],
        )
    )

    assert result is True
    assert calls == ["sharedlead"]
    assert len(sent) == 1
    assert sent[0][0].route == "username"
    assert sent[0][1] == "Как рынок сегодня видишь?"
    from db.schema import get_connection

    current = get_connection().execute(
        "SELECT status, claimed_by_account FROM leads WHERE target_user_id=?",
        (8001,),
    ).fetchone()
    assert current is not None
    assert current["status"] == queue.STATUS_SENT
    assert int(current["claimed_by_account"]) == 7002
