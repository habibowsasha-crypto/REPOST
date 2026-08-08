"""Pure worker-number contract for bounded financial coordinator tasks."""

from __future__ import annotations


def is_primary_financial_worker(worker_number: int) -> bool:
    """Return True only for the existing 1-based coordinator worker.

    ``start_financial_reconciliation_dispatcher`` creates workers numbered
    ``1..N`` for task names and logs.  The bounded G54/G55 coordinator work
    must therefore run on worker 1 and never on a non-existent worker 0.
    """

    return int(worker_number) == 1
