"""v1.0.77 entity-first dispatch and local short_hook hardening."""

from __future__ import annotations

import asyncio


def _account(user_id: int, *, participates: bool = True):
    from services import accounts

    accounts.upsert_account(
        user_id=user_id,
        session_string=f"session-{user_id}",
        username=f"sender{user_id}",
    )
    accounts.set_participates(user_id, participates)
    return accounts.get_account(user_id)


def test_short_hook_never_calls_ai(app_env, monkeypatch):
    from services import ai_first_dm
    from texts.first_dm import SHORT_HOOK_FIRST_DM_TEMPLATES

    monkeypatch.setattr(ai_first_dm, "FIRST_DM_STYLE", "short_hook")
    monkeypatch.setattr(ai_first_dm, "AI_DM_ENABLED", True)
    monkeypatch.setattr(ai_first_dm, "OPENAI_API_KEY", "configured-but-unused")

    async def forbidden(*args, **kwargs):
        raise AssertionError("short_hook must not call AI")

    monkeypatch.setattr(ai_first_dm, "_openai_first_dm", forbidden)
    text = asyncio.run(ai_first_dm.generate_first_dm())
    assert text in SHORT_HOOK_FIRST_DM_TEMPLATES


def test_ai_list_marker_is_removed_before_use(app_env):
    from services import ai_first_dm

    raw = "- Привет, ты больше по споту или по фьючам?"
    clean = ai_first_dm.sanitize_ai_output(raw)
    assert clean == "Привет, ты больше по споту или по фьючам?"
    assert ai_first_dm.validate_first_dm(raw, style="magnet") == (
        False,
        "leading_list_marker",
    )
    assert ai_first_dm.validate_first_dm(clean, style="magnet")[0]


def test_entity_evidence_is_saved_per_account(app_env):
    from services import queue

    queue.upsert_from_activity(
        target_user_id=91001,
        username="target91001",
        access_hash=111111,
        source_chat_id=-1001,
        source_account_user_id=71001,
    )
    queue.upsert_from_activity(
        target_user_id=91001,
        username="target91001",
        access_hash=222222,
        source_chat_id=-1001,
        source_account_user_id=71002,
    )

    first = queue.get_account_entity(91001, 71001)
    second = queue.get_account_entity(91001, 71002)
    assert first and int(first["access_hash"]) == 111111
    assert second and int(second["access_hash"]) == 222222
    assert queue.known_entity_account_ids(91001) == {71001, 71002}


def test_identical_activity_does_not_reopen_negative_cache(app_env):
    from services import queue

    queue.upsert_from_activity(
        target_user_id=91002,
        username="target91002",
        access_hash=333333,
        source_chat_id=-1002,
        source_account_user_id=71003,
    )
    queue.record_account_failure(91002, 71003, "no_entity", "stale")
    queue.upsert_from_activity(
        target_user_id=91002,
        username="target91002",
        access_hash=333333,
        source_chat_id=-1002,
        source_account_user_id=71003,
    )
    assert queue.failed_account_ids(91002, "no_entity") == {71003}

    queue.upsert_from_activity(
        target_user_id=91002,
        username="target91002",
        access_hash=444444,
        source_chat_id=-1002,
        source_account_user_id=71003,
    )
    assert queue.failed_account_ids(91002, "no_entity") == set()


def test_automatic_resolver_uses_owned_hash_without_username_search(app_env):
    from services import dispatcher, queue

    queue.upsert_from_activity(
        target_user_id=91003,
        username="target91003",
        access_hash=555555,
        source_chat_id=-1003,
        source_account_user_id=71004,
    )
    lead = queue.get_lead(91003)
    assert lead

    class Client:
        async def get_input_entity(self, value):
            raise AssertionError(f"unexpected Telegram lookup: {value!r}")

    entity = asyncio.run(
        dispatcher._resolve_target_entity(
            Client(),
            71004,
            lead,
            allow_remote_username_lookup=False,
        )
    )
    assert int(entity.user_id) == 91003
    assert int(entity.access_hash) == 555555


def test_automatic_resolver_never_searches_username_without_owned_hash(app_env):
    from services import dispatcher

    calls: list[object] = []

    class Client:
        async def get_input_entity(self, value):
            calls.append(value)
            raise ValueError("not in local cache")

    lead = {
        "target_user_id": 91004,
        "username": "target91004",
        "access_hash": None,
        "source_account_user_id": None,
    }
    entity = asyncio.run(
        dispatcher._resolve_target_entity(
            Client(),
            71005,
            lead,
            allow_remote_username_lookup=False,
        )
    )
    assert entity is None
    assert calls == [91004]
    assert "target91004" not in calls


def test_only_accounts_that_saw_target_are_automatic_candidates(app_env):
    from services import dispatcher, queue

    _account(71006)
    _account(71007)
    _account(71008)
    queue.upsert_from_activity(
        target_user_id=91005,
        access_hash=666666,
        source_chat_id=-1004,
        source_account_user_id=71007,
    )
    lead = queue.get_lead(91005)
    assert lead
    ready = [
        {"user_id": 71006},
        {"user_id": 71007},
        {"user_id": 71008},
    ]
    ordered = dispatcher._owned_ready_accounts_for_lead(lead, ready)
    assert [int(item["user_id"]) for item in ordered] == [71007]


def test_reauth_account_is_not_considered_entity_owner(app_env):
    from services import account_auth, dispatcher, queue

    _account(71009)
    queue.upsert_from_activity(
        target_user_id=91006,
        access_hash=777777,
        source_chat_id=-1005,
        source_account_user_id=71009,
    )
    lead = queue.get_lead(91006)
    assert lead
    assert dispatcher._owned_possible_sender_ids(lead) == {71009}

    account_auth.mark_reauth_required(71009, "session_not_authorized")
    assert dispatcher._owned_possible_sender_ids(lead) == set()


def test_no_active_entity_evidence_becomes_terminal_and_can_reopen(app_env):
    from services import dispatcher, queue

    queue.upsert_from_activity(
        target_user_id=91007,
        username="target91007",
        source_account_user_id=None,
        access_hash=None,
    )
    lead = queue.get_lead(91007)
    assert lead
    assert dispatcher._finish_or_defer_unresolvable(
        91007,
        lead,
        possible_account_ids=set(),
    )
    failed = queue.get_lead(91007)
    assert failed and failed["failure_reason"] == "no_active_entity_evidence"

    queue.upsert_from_activity(
        target_user_id=91007,
        username="target91007",
        access_hash=888888,
        source_chat_id=-1006,
        source_account_user_id=71010,
    )
    reopened = queue.get_lead(91007)
    assert reopened and reopened["status"] == queue.STATUS_PENDING


def test_recent_history_sync_backfills_only_existing_leads(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import chats, dispatcher, monitor, queue

    _account(71011)
    queue.upsert_from_activity(
        target_user_id=91008,
        username="target91008",
        access_hash=999001,
        source_chat_id=-1007,
        source_account_user_id=71012,
    )
    lead = queue.get_lead(91008)
    assert lead
    assert dispatcher._finish_or_defer_unresolvable(
        91008,
        lead,
        possible_account_ids=set(),
    )
    terminal = queue.get_lead(91008)
    assert terminal and terminal["status"] == queue.STATUS_CANCELLED

    now = "2026-08-05T16:00:00+00:00"
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            INSERT INTO account_discovered_chats (
                account_user_id, chat_id, title, username, peer_type, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (71011, -1007, "Test chat", None, "channel", now),
        )
        conn.execute(
            """
            INSERT INTO account_selected_chats(account_user_id, chat_id)
            VALUES (?, ?)
            """,
            (71011, -1007),
        )

    class FakeUser:
        def __init__(self, user_id: int, access_hash: int, username: str):
            self.id = user_id
            self.access_hash = access_hash
            self.username = username
            self.bot = False
            self.is_self = False

    class Message:
        def __init__(self, sender):
            self.sender = sender

        async def get_sender(self):
            return self.sender

    class Client:
        def iter_messages(self, chat_id, limit):
            assert chat_id == -1007
            assert limit == monitor._ENTITY_SYNC_HISTORY_LIMIT

            async def iterator():
                yield Message(FakeUser(91008, 999008, "target91008"))
                yield Message(FakeUser(91009, 999009, "not-a-lead"))

            return iterator()

    monkeypatch.setattr(monitor, "User", FakeUser)
    monkeypatch.setattr(monitor, "_ENTITY_SYNC_CHAT_DELAY_SECONDS", 0)
    matched = asyncio.run(monitor._sync_recent_entity_evidence(71011, Client()))
    assert matched == 1
    evidence = queue.get_account_entity(91008, 71011)
    assert evidence and int(evidence["access_hash"]) == 999008
    reopened = queue.get_lead(91008)
    assert reopened and reopened["status"] == queue.STATUS_PENDING
    assert int(reopened["source_account_user_id"]) == 71011
    assert queue.get_lead(91009) is None
    assert chats.entity_sync_due(71011, -1007) is False


def test_production_entity_failure_does_not_call_ai(app_env, monkeypatch):
    from services import dispatcher, monitor, queue

    _account(71013)
    queue.upsert_from_activity(
        target_user_id=91010,
        username="target91010",
        source_account_user_id=71013,
        access_hash=None,
    )
    lead = queue.claim_random_pending(71013)
    assert lead

    class Client:
        def is_connected(self):
            return True

        async def get_input_entity(self, value):
            assert value == 91010
            raise ValueError("not cached")

    async def forbidden_generation():
        raise AssertionError("AI must not run without entity evidence")

    monkeypatch.setattr(monitor, "get_client", lambda account_id: Client())
    monkeypatch.setattr(dispatcher, "generate_first_dm", forbidden_generation)
    result = asyncio.run(
        dispatcher._attempt_lead_across_accounts(
            lead,
            [{"user_id": 71013, "participates": 1, "session_string": "s"}],
            possible_account_ids={71013},
        )
    )
    assert result is True
    failed = queue.get_lead(91010)
    assert failed and failed["failure_reason"] == "no_entity_all_accounts"
