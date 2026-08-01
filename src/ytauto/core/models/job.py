"""Lifecycle vocabulary for jobs and their stages."""

from __future__ import annotations

from enum import StrEnum


class JobState(StrEnum):
    """Where a job sits in its lifecycle. Persisted as TEXT."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        """True when no further work will happen without explicit requeueing."""
        return self in _TERMINAL_JOB_STATES


class StageStatus(StrEnum):
    """Where one stage of one job sits. Persisted as TEXT."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    SKIPPED = "skipped"
    FAILED = "failed"

    @property
    def is_done(self) -> bool:
        """True when a resume must NOT rerun this stage.

        SKIPPED means a fingerprint cache hit - the artifact already exists, so
        it is as done as SUCCEEDED. FAILED is deliberately not done: resuming a
        crashed batch must retry it.
        """
        return self in _DONE_STAGE_STATUSES


_TERMINAL_JOB_STATES = frozenset({JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED})
_DONE_STAGE_STATUSES = frozenset({StageStatus.SUCCEEDED, StageStatus.SKIPPED})
