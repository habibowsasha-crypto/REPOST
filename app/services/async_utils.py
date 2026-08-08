from __future__ import annotations


class StaleExecutionPass(BaseException):
    """Abort one monitor batch after an execution status CAS is rejected.

    This is an internal control-flow sentinel, deliberately derived from
    ``BaseException`` so broad ``except Exception`` recovery branches cannot
    misclassify a stale worker pass as an exchange error and continue sending
    orders or notifications. Monitor entrypoints catch it explicitly, close
    adapters in ``finally`` and resume on the next polling cycle.
    """

    def __init__(
        self,
        *,
        source: str,
        execution_id: int,
        expected_status: str,
        attempted_status: str,
    ) -> None:
        self.source = str(source)
        self.execution_id = int(execution_id)
        self.expected_status = str(expected_status)
        self.attempted_status = str(attempted_status)
        super().__init__(
            f"{self.source}: stale execution #{self.execution_id}; "
            f"expected={self.expected_status}, attempted={self.attempted_status}"
        )


class NullAsyncContext:
    """Small async no-op context manager used for targeted monitor calls."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


def null_async_context() -> NullAsyncContext:
    return NullAsyncContext()
