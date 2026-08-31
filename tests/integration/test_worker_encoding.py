"""The worker's end of the pipe writes UTF-8, not the host's locale codec.

``print`` in a subprocess encodes with the locale codec - measured cp1252 on
this project's Windows box - and ``sys.stdout``'s error handler for encoding
is strict, so one character that codec cannot represent raises
``UnicodeEncodeError`` at the write.

That is invisible through the worker *protocol*: ``worker_protocol.encode``
serialises with ``json.dumps``, whose ``ensure_ascii`` default escapes every
non-ASCII character to ``\\uXXXX``, so every protocol line is pure ASCII by
construction. It is entirely visible for anything else the process writes to
stdout - a stage that logs, or a library a stage calls that logs, which is
the normal behaviour of exactly the ffmpeg/provider code Phase 2 adds. The
stage below stands in for that.

The worker is driven directly here rather than through the dispatcher, and
its stdout is read as raw bytes, because the assertion is about the bytes on
the wire. Going through the dispatcher would prove less: a stray non-protocol
line fails the stage either way (``decode`` rejects it), so the outcome would
be identical with and without the fix and the test would pin nothing.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path

import pytest

from ytauto.core.pipeline.stage import JobContext, ProgressFn, StageResult
from ytauto.infra.cas.store import CasStore

pytestmark = pytest.mark.integration

_TESTS_ROOT = Path(__file__).resolve().parent.parent
_ARROW = "→"
"""U+2192 RIGHTWARDS ARROW - deliberately outside cp1252, unlike an accented
Latin letter, which cp1252 encodes happily and which would prove nothing on
this host."""


class ChattyStage:
    """A stage that logs one non-ASCII line to its own stdout, then succeeds.

    At module scope, and registered as the ``it-encoding:chatty`` entry point
    below, because ``app/worker.py`` resolves stages through
    ``app/registry.py`` - see ``tests/integration/test_resume.py``'s module
    docstring for the full cross-process plumbing note.
    """

    id = "chatty"
    version = 1
    depends_on: tuple[str, ...] = ()
    settings_keys: tuple[str, ...] = ()
    gpu_pool = "gpu_compute"

    def fingerprint(self, ctx: JobContext) -> str:
        return "d" * 64

    def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult:
        print(f"provider log line {_ARROW} done", flush=True)
        return StageResult(artifacts=())


def make_chatty(*, cas: CasStore, settings: Mapping[str, object]) -> ChattyStage:
    """Entry point ``it-encoding:chatty``. Produces no artifacts, so it never
    needs the store it is handed."""
    return ChattyStage()


_PIPELINE_ID = "it-encoding"


def _assignment(tmp_path: Path) -> dict[str, object]:
    """The exact shape ``dispatcher._build_assignment`` emits.

    Hand-built rather than produced by the dispatcher because this test drives
    the worker directly (see the module docstring); if the two ever disagree,
    ``tests/unit/app/test_dispatcher.py`` is what notices, since it asserts on
    the real builder's output.
    """
    return {
        "job_id": "enc-job",
        "stage_id": ChattyStage.id,
        "project_id": "proj-1",
        "pipeline_id": _PIPELINE_ID,
        "correlation_id": "enc-job:chatty:1",
        "cas_root": str(tmp_path / "cas"),
        "workdir": str(tmp_path / "work"),
        "settings": {},
        "fingerprint": "d" * 64,
        "inputs": {},
    }


def test_the_worker_writes_utf8_to_stdout_whatever_the_locale_says(tmp_path: Path) -> None:
    """One non-ASCII character must not be able to kill a worker.

    Without an explicit encoding the child raises UnicodeEncodeError inside
    ``run()``, which ``run_stage`` turns into a FATAL stage error - so the
    stage fails, the worker exits 1, and the character never reaches the
    wire. With it, the line is written as UTF-8 and the stage succeeds.
    """
    env = dict(os.environ)
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        os.pathsep.join([str(_TESTS_ROOT), existing]) if existing else str(_TESTS_ROOT)
    )

    completed = subprocess.run(
        [sys.executable, "-m", "ytauto.app.worker"],
        input=json.dumps(_assignment(tmp_path)).encode("ascii"),
        capture_output=True,
        env=env,
        check=False,
        timeout=60,
    )

    assert completed.returncode == 0, completed.stderr.decode("utf-8", errors="replace")
    assert _ARROW.encode("utf-8") in completed.stdout, (
        "the worker's stdout must carry the character as UTF-8"
    )
    assert b'"type":"result"' in completed.stdout, "the stage itself must still have succeeded"
