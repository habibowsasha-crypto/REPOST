from __future__ import annotations

from typing import Any, Iterable


def account_local_full_pass_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group a fetched full-reconcile page by user without changing membership.

    Signal fan-out inserts executions in signal order, so an id-ordered page is
    typically interleaved across accounts.  Lifecycle and BE pre-read caches are
    account-scoped and freshness-bounded; processing one account's rows together
    lets those existing caches coalesce private position reads before they expire.

    The helper is deliberately narrow:
    - it never filters or deduplicates rows;
    - it preserves original order inside one user;
    - malformed/missing user ids stay in their original relative order at the end;
    - pagination cursors are computed by the callers from row ids and are unchanged.
    """

    indexed = list(enumerate(rows))

    def sort_key(item: tuple[int, dict[str, Any]]) -> tuple[int, int, int]:
        original_index, row = item
        try:
            raw_user_id = row.get("user_id") if isinstance(row, dict) else None
            if isinstance(raw_user_id, bool):
                raise ValueError("boolean user id")
            user_id = int(raw_user_id or 0)
        except (TypeError, ValueError, OverflowError):
            user_id = 0
        if user_id > 0:
            return (0, user_id, original_index)
        return (1, 0, original_index)

    indexed.sort(key=sort_key)
    return [row for _, row in indexed]
