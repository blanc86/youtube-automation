"""The measured blocker Phase 2a's ffmpeg stages cannot ship without.

60 KB of worker stderr deadlocked ``_pump`` permanently: nothing drained the
stderr pipe concurrently with the stdout read loop, so a worker that filled
the OS pipe buffer writing to stderr blocked forever on that write, while the
dispatcher blocked forever reading stdout - and ``proc.wait(timeout=30)``
never even got a chance to fire, sitting as it does after the stdout loop.
ffmpeg clears 60 KB of stderr in seconds under ordinary logging, so no stage
that shells out to it could ever complete. See ``dispatcher._spawn`` and
``dispatcher._pump`` for the fix: stderr goes to a per-attempt file instead of
a pipe.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from .conftest import DispatcherEnv

pytestmark = pytest.mark.integration


def test_a_worker_that_floods_stderr_still_reports_its_result(
    dispatcher_env: Callable[..., DispatcherEnv],
) -> None:
    """200 KB of stderr must not deadlock the pump. 60 KB was measured as fatal.

    The bounded deadline (60s, not the 1800s default) is load-bearing for the
    guard-pin recorded in this task's report, not just for this run: with the
    default 1800s, reverting the stderr fix would hang for half an hour
    instead of failing within a reasonable test timeout.
    """
    env = dispatcher_env(pipeline_id="it-flood", pump_deadline_s=60.0)

    report = env.dispatcher.tick()

    assert report.spawned == ("flood",), "the flooding stage must complete, not hang"
    assert env.stage_status("flood") == "succeeded"
