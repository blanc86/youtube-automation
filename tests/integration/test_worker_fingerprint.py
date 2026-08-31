"""The worker refuses a stage it fingerprints differently to its dispatcher.

One stage execution builds the stage object twice, in two processes: the
dispatcher builds its pipeline once per process (``registry.build_pipeline``)
and records the fingerprint *its* copy computes, while the worker builds the
stage again per job from that job's real settings. Nothing reconciled the two
until this check - ``run_stage`` verifies that every artifact a stage claims
exists in the CAS, which says nothing about the digest it is indexed under.

A disagreement is therefore invisible at the moment it happens and permanent
afterwards: the artifacts land under the dispatcher's digest, every later run
recomputes that same digest, misses, re-runs, and records again. Nothing
fails, the cache simply stops working for that stage forever.

Driven through a real worker subprocess rather than the dispatcher, and with
the assignment hand-built, because the point is what the worker does with an
assignment whose ``fingerprint`` field disagrees with the stage it resolves -
which a correctly-behaving dispatcher will never produce.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from .stages import FixedFingerprint, Unfingerprintable

pytestmark = pytest.mark.integration

_TESTS_ROOT = Path(__file__).resolve().parent.parent
_PIPELINE_ID = "it-fingerprint"


def _assignment(
    tmp_path: Path, *, fingerprint: str, stage_id: str = FixedFingerprint.id
) -> dict[str, object]:
    """The shape ``dispatcher._build_assignment`` emits, with ``fingerprint``
    under the test's control."""
    return {
        "job_id": "fp-job",
        "stage_id": stage_id,
        "project_id": "proj-1",
        "pipeline_id": _PIPELINE_ID,
        "correlation_id": f"fp-job:{stage_id}:1",
        "cas_root": str(tmp_path / "cas"),
        "workdir": str(tmp_path / "work"),
        "settings": {},
        "fingerprint": fingerprint,
        "inputs": {},
    }


def _run_worker(assignment: dict[str, object]) -> subprocess.CompletedProcess[bytes]:
    env = dict(os.environ)
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        os.pathsep.join([str(_TESTS_ROOT), existing]) if existing else str(_TESTS_ROOT)
    )
    return subprocess.run(
        [sys.executable, "-m", "ytauto.app.worker"],
        input=json.dumps(assignment).encode("ascii"),
        capture_output=True,
        env=env,
        check=False,
        timeout=60,
    )


def _messages(stdout: bytes) -> list[dict[str, object]]:
    return [json.loads(line) for line in stdout.decode("utf-8").splitlines() if line.strip()]


def test_a_worker_refuses_a_stage_it_fingerprints_differently(tmp_path: Path) -> None:
    """A disagreement must fail the job loudly, naming both digests.

    FATAL rather than RETRYABLE: a retry rebuilds the same two disagreeing
    stage objects from the same two sets of settings and reaches the same
    conclusion, having burned an attempt to do it.
    """
    assigned = "b" * 64
    completed = _run_worker(_assignment(tmp_path, fingerprint=assigned))

    messages = _messages(completed.stdout)
    assert [m["type"] for m in messages] == ["error"], (
        f"expected exactly one terminal error, got {messages!r}"
    )
    error = messages[0]
    assert error["kind"] == "fatal"
    assert error["stage_id"] == FixedFingerprint.id
    assert FixedFingerprint.FINGERPRINT in str(error["message"]), "the computed digest"
    assert assigned in str(error["message"]), "the assigned digest"
    assert completed.returncode == 1


def test_a_stage_that_cannot_fingerprint_itself_fails_fatally(tmp_path: Path) -> None:
    """A raising ``fingerprint`` is stage code failing, and stage code failing
    is a FATAL protocol error - the same contract ``run_stage`` applies to
    ``stage.run``.

    Letting it escape instead would kill the worker with no terminal message,
    which the dispatcher can only read as "died for an unknown reason": one
    attempt charged, 5/10/20/40/80 s of backoff, and only then a failed job.
    Every one of those retries is guaranteed to fail identically, because the
    settings arrive over the pipe byte-for-byte the same each time.

    The traceback is the cost of catching, so the assertion below is that the
    exception *type* survives into the message - that is what reaches
    ``jobs.last_error`` and is all an operator gets.
    """
    completed = _run_worker(
        _assignment(tmp_path, fingerprint="c" * 64, stage_id=Unfingerprintable.id)
    )

    messages = _messages(completed.stdout)
    assert [m["type"] for m in messages] == ["error"], (
        f"a raising fingerprint must be reported, not crash the worker; got {messages!r}"
    )
    error = messages[0]
    assert error["kind"] == "fatal"
    assert error["stage_id"] == Unfingerprintable.id
    assert "ValidationError" in str(error["message"]), "the exception type must survive"
    assert "never-ran" in str(error["message"]), "and so must what it said"
    assert b"Traceback" not in completed.stderr, "the worker must not have crashed"
    assert completed.returncode == 1


def test_a_worker_runs_the_stage_when_the_fingerprints_agree(tmp_path: Path) -> None:
    """The positive control, and the thing that keeps the check from being a
    stage-runner that never runs anything: the identical assignment with the
    matching digest succeeds."""
    completed = _run_worker(_assignment(tmp_path, fingerprint=FixedFingerprint.FINGERPRINT))

    assert completed.returncode == 0, completed.stderr.decode("utf-8", errors="replace")
    assert [m["type"] for m in _messages(completed.stdout)] == ["result"]
