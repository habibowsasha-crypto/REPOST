"""Transient admin wizard state (login, future text inputs)."""

from __future__ import annotations

from typing import Any

# admin_id -> state dict
_admin_states: dict[int, dict[str, Any]] = {}


def get_state(admin_id: int) -> dict[str, Any] | None:
    return _admin_states.get(int(admin_id))


def set_state(admin_id: int, **kwargs: Any) -> dict[str, Any]:
    admin_id = int(admin_id)
    current = _admin_states.get(admin_id) or {}
    current.update(kwargs)
    _admin_states[admin_id] = current
    return current


def clear_state(admin_id: int) -> None:
    _admin_states.pop(int(admin_id), None)


def is_in_flow(admin_id: int, flow: str) -> bool:
    state = get_state(admin_id)
    return bool(state and state.get("flow") == flow)
