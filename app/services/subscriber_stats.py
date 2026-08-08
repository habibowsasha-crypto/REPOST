"""Admin-facing subscriber counts derived from authoritative user rows.

This module is deliberately read-only.  It summarizes the same persisted mode,
BingX API and whitelist fields used by signal fan-out, without probing private
exchange endpoints or changing trading state.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


def _normalized_tokens(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        items = value.replace(";", ",").split(",")
    else:
        try:
            items = list(value)
        except TypeError:
            items = [value]
    return {str(item).strip().lower() for item in items if str(item).strip()}


def _has_mexc_api(row: Mapping[str, Any]) -> bool:
    tokens = _normalized_tokens(row.get("connected_exchanges"))
    return "bingx" in tokens or "mexc" in tokens


def _has_mexc_whitelist(row: Mapping[str, Any]) -> bool:
    grants = _normalized_tokens(row.get("whitelist_exchanges"))
    if grants:
        return "all" in grants or "bingx" in grants or "mexc" in grants
    # Backward-compatible fallback for legacy rows.  The current DB reader
    # normally converts this flag into a grant set, but the summary remains
    # correct for direct/test callers and partially migrated records.
    return bool(row.get("whitelisted"))


def summarize_subscribers(rows: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    """Return conservative subscriber/admin counters.

    ``ready_for_attempt`` is intentionally not called "will open".  It means a
    non-admin subscriber is in AUTO mode, has a BingX whitelist grant and has an
    enabled BingX API record.  Balance, risk limits, open positions, symbol
    conflicts and live BingX state are checked only when a concrete signal is
    executed and can still reduce the final number of opened trades.
    """

    users = [dict(row) for row in rows]
    admins = [row for row in users if bool(row.get("is_admin"))]
    subscribers = [row for row in users if not bool(row.get("is_admin"))]

    mode_auto = 0
    mode_preview = 0
    mode_off = 0
    mode_unknown = 0
    api_connected = 0
    whitelisted = 0
    routed_for_execution = 0
    ready_for_attempt = 0
    auto_without_api = 0
    auto_without_whitelist = 0

    for row in subscribers:
        mode = str(row.get("mode") or "preview").strip().lower()
        has_api = _has_mexc_api(row)
        has_whitelist = _has_mexc_whitelist(row)

        if has_api:
            api_connected += 1
        if has_whitelist:
            whitelisted += 1

        if mode == "auto":
            mode_auto += 1
            if has_whitelist:
                routed_for_execution += 1
            if has_whitelist and has_api:
                ready_for_attempt += 1
            if not has_api:
                auto_without_api += 1
            if not has_whitelist:
                auto_without_whitelist += 1
        elif mode == "preview":
            mode_preview += 1
        elif mode == "off":
            mode_off += 1
        else:
            mode_unknown += 1

    return {
        "total_accounts": len(users),
        "admins": len(admins),
        "subscribers": len(subscribers),
        "api_connected": api_connected,
        "whitelisted": whitelisted,
        "mode_auto": mode_auto,
        "mode_preview": mode_preview,
        "mode_off": mode_off,
        "mode_unknown": mode_unknown,
        "routed_for_execution": routed_for_execution,
        "ready_for_attempt": ready_for_attempt,
        "auto_without_api": auto_without_api,
        "auto_without_whitelist": auto_without_whitelist,
    }
