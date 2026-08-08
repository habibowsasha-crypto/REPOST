"""Explicit notification classification for pre-entry trade skips.

The user's optional preference may hide only routine, expected decisions.  Any
configuration, queue, exchange, sizing or safety warning stays mandatory even
when the underlying executor uses ``status='skipped'`` for fail-closed control
flow.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from app.services.models import ExecutionResult

OPTIONAL_TRADE_SKIP_NOTIFICATION_KIND = "optional_trade_skip"
MANDATORY_TRADE_WARNING_NOTIFICATION_KIND = "trade_warning"

OPTIONAL_TRADE_SKIP_KINDS = frozenset(
    {
        "user_mode_off",
        "duplicate_signal",
        "risk_limit",
        "same_symbol_active",
        "live_position_guard",
        "live_entry_order_guard",
    }
)


def _payload_copy(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    return dict(payload or {})


def optional_trade_skip_payload(
    skip_kind: str,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Attach the exact allowlisted category for a suppressible routine skip."""

    normalized = str(skip_kind or "").strip().lower()
    if normalized not in OPTIONAL_TRADE_SKIP_KINDS:
        raise ValueError(f"unknown optional trade skip kind: {skip_kind!r}")
    result = _payload_copy(payload)
    result["notification_kind"] = OPTIONAL_TRADE_SKIP_NOTIFICATION_KIND
    result["trade_skip_kind"] = normalized
    return result


def mandatory_trade_warning_payload(
    warning_kind: str,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Mark a fail-closed skip as a mandatory warning.

    Warning kinds intentionally remain open-ended because exchange/API error
    codes evolve.  The classifier never suppresses this notification kind.
    """

    normalized = str(warning_kind or "").strip().lower()
    if not normalized:
        raise ValueError("mandatory trade warning kind is required")
    result = _payload_copy(payload)
    result["notification_kind"] = MANDATORY_TRADE_WARNING_NOTIFICATION_KIND
    result["trade_warning_kind"] = normalized
    return result


def is_optional_trade_skip_result(result: ExecutionResult) -> bool:
    """Return True only for a valid, explicitly allowlisted routine skip."""

    if str(result.status or "").strip().lower() != "skipped":
        return False
    payload = dict(result.payload or {})
    if payload.get("notification_kind") != OPTIONAL_TRADE_SKIP_NOTIFICATION_KIND:
        return False
    skip_kind = str(payload.get("trade_skip_kind") or "").strip().lower()
    return skip_kind in OPTIONAL_TRADE_SKIP_KINDS
