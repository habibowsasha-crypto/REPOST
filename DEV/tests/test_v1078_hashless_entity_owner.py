"""v1.0.78 hashless entity-owner and stable no-entity terminal tests."""

from __future__ import annotations

import asyncio


def _account(user_id: int):
    from services import accounts

    accounts.upsert_account(
        user_id=user_id,
        session_string=f"session-{user_id}",
        username=f"sender{user_id}",
    )
    accounts.set_participates(user_id, True)
    return accounts.get_account(user_id)


def test_hashless_observer_is_still_candidate_for_one_local_cache_lookup(app_env):
    from services import dispatcher, queue

    _account(72001)
    queue.upsert_from_activity(
        target_user_id=92001,
        username="target92001",
        access_hash=None,
        source_chat_id=-2001,
        source_account_user_id=72001,
    )
    lead = queue.get_lead(92001)
    assert lead
    assert dispatcher._owned_possible_sender_ids(lead) == {72001}



def test_hashless_owner_uses_numeric_local_cache_without_username_search(app_env):
    from services import dispatcher, queue

    _account(72002)
    queue.upsert_from_activity(
        target_user_id=92002,
        username="target92002",
        access_hash=None,
        source_chat_id=-2002,
        source_account_user_id=72002,
    )
    lead = queue.get_lead(92002)
    assert lead
    calls: list[object] = []

    class Entity:
        user_id = 92002
        access_hash = 123456789

    class Client:
        async def get_input_entity(self, value):
            calls.append(value)
            if value == 92002:
                return Entity()
            raise AssertionError(f"remote username lookup attempted: {value!r}")

    entity = asyncio.run(
        dispatcher._resolve_target_entity(
            Client(),
            72002,
            lead,
            allow_remote_username_lookup=False,
        )
    )
    assert entity is not None
    assert calls == [92002]
    saved = queue.get_account_entity(92002, 72002)
    assert saved and int(saved["access_hash"]) == 123456789



def test_identical_hashless_activity_does_not_reopen_terminal_lead(app_env):
    from services import queue

    queue.upsert_from_activity(
        target_user_id=92003,
        username="target92003",
        access_hash=None,
        source_chat_id=-2003,
        source_account_user_id=72003,
    )
    queue.mark_terminal_failure(
        92003,
        "no_entity_all_accounts",
        "local cache miss",
    )
    action = queue.upsert_from_activity(
        target_user_id=92003,
        username="target92003",
        access_hash=None,
        source_chat_id=-2003,
        source_account_user_id=72003,
    )
    lead = queue.get_lead(92003)
    assert action == "skipped_status_cancelled"
    assert lead and lead["status"] == queue.STATUS_CANCELLED
    assert lead["failure_reason"] == "no_entity_all_accounts"



def test_improved_hash_reopens_previous_no_entity_terminal(app_env):
    from services import queue

    queue.upsert_from_activity(
        target_user_id=92004,
        username="target92004",
        access_hash=None,
        source_chat_id=-2004,
        source_account_user_id=72004,
    )
    queue.mark_terminal_failure(
        92004,
        "no_entity_all_accounts",
        "local cache miss",
    )
    action = queue.upsert_from_activity(
        target_user_id=92004,
        username="target92004",
        access_hash=444444444,
        source_chat_id=-2004,
        source_account_user_id=72004,
    )
    lead = queue.get_lead(92004)
    assert action == "refreshed"
    assert lead and lead["status"] == queue.STATUS_PENDING
    assert lead["failure_reason"] is None
    assert int(queue.get_account_entity(92004, 72004)["access_hash"]) == 444444444



def test_identical_record_account_entity_does_not_reopen_terminal(app_env):
    from services import queue

    queue.upsert_from_activity(
        target_user_id=92005,
        username="target92005",
        access_hash=555555555,
        source_chat_id=-2005,
        source_account_user_id=72005,
    )
    queue.mark_terminal_failure(
        92005,
        "no_entity_all_accounts",
        "send rejected",
    )
    queue.record_account_entity(
        target_user_id=92005,
        account_user_id=72005,
        access_hash=555555555,
        username="target92005",
        source_chat_id=-2005,
        reopen_no_entity=True,
    )
    lead = queue.get_lead(92005)
    assert lead and lead["status"] == queue.STATUS_CANCELLED
    assert lead["failure_reason"] == "no_entity_all_accounts"


def test_alternating_identical_hashless_activity_does_not_reopen_terminal(app_env):
    from services import queue

    queue.upsert_from_activity(
        target_user_id=92006,
        username="target92006",
        access_hash=None,
        source_chat_id=-2006,
        source_account_user_id=72006,
    )
    queue.upsert_from_activity(
        target_user_id=92006,
        username="target92006",
        access_hash=None,
        source_chat_id=-2006,
        source_account_user_id=72007,
    )
    queue.mark_terminal_failure(
        92006,
        "no_entity_all_accounts",
        "both local caches missed",
    )

    for account_id in (72006, 72007, 72006, 72007):
        action = queue.upsert_from_activity(
            target_user_id=92006,
            username="target92006",
            access_hash=None,
            source_chat_id=-2006,
            source_account_user_id=account_id,
        )
        lead = queue.get_lead(92006)
        assert action == "skipped_status_cancelled"
        assert lead and lead["status"] == queue.STATUS_CANCELLED
        assert lead["failure_reason"] == "no_entity_all_accounts"
