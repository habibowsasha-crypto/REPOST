from __future__ import annotations

from aiogram.fsm.state import State, StatesGroup


class AICommentsUI(StatesGroup):
    """Isolated UI-only states for the AI Comments menu scaffold."""

    menu = State()
    channels = State()
    channel = State()
    channel_memory = State()
    channel_memory_edit = State()
    channel_scenario_edit = State()
    channel_scenario_confirm = State()
    account_profiles = State()
    account_profile = State()
    account_profile_edit = State()
    account_profile_confirm = State()
    account_profile_history = State()
    recent_posts = State()
    comment_history = State()
    test_comment = State()
    draft_generation = State()
    draft_generation_confirm = State()
    dialogue = State()
    dialogue_post = State()
    dialogue_profiles = State()
    dialogue_thread = State()
    dialogue_generation_confirm = State()
    dialogue_review = State()
    drafts = State()
    settings = State()
    statistics = State()


AI_COMMENTS_STATE_PREFIX = f"{AICommentsUI.__name__}:"


def is_ai_comments_state(value: str | None) -> bool:
    """Return True only for states owned by the isolated comments UI."""

    return bool(value and value.startswith(AI_COMMENTS_STATE_PREFIX))
