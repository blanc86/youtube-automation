import pytest

from ytauto.core.models.job import JobState, StageStatus


@pytest.mark.parametrize(
    ("state", "terminal"),
    [
        (JobState.QUEUED, False),
        (JobState.RUNNING, False),
        (JobState.SUCCEEDED, True),
        (JobState.FAILED, True),
        (JobState.CANCELLED, True),
    ],
)
def test_job_terminality(state: JobState, terminal: bool) -> None:
    assert state.is_terminal is terminal


@pytest.mark.parametrize(
    ("status", "done"),
    [
        (StageStatus.PENDING, False),
        (StageStatus.RUNNING, False),
        (StageStatus.SUCCEEDED, True),
        (StageStatus.SKIPPED, True),
        (StageStatus.FAILED, False),
    ],
)
def test_stage_doneness(status: StageStatus, done: bool) -> None:
    """SKIPPED is a fingerprint cache hit - done, and not to be rerun.
    FAILED is NOT done: resume must retry it."""
    assert status.is_done is done


def test_states_serialise_as_plain_strings() -> None:
    """These values are persisted in SQLite TEXT columns."""
    assert f"{JobState.QUEUED}" == "queued"
    assert f"{StageStatus.SKIPPED}" == "skipped"
