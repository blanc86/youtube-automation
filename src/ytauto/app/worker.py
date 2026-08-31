"""Subprocess entry point for one pipeline stage.

Invoked as ``python -m ytauto.app.worker`` with a JSON assignment on stdin
(see ``dispatcher._build_assignment`` for its shape). Runs the assigned stage
through ``run_stage``, writes one worker-protocol line per event to stdout -
a ``staged`` message per produced artifact, then a terminal ``result`` or
``error`` - and exits non-zero on ``error`` so a process supervisor sees
failure without parsing stdout.

The stage itself is resolved through ``app/registry.py`` from the
assignment's ``pipeline_id`` and ``stage_id`` - an entry point named
``"<pipeline_id>:<stage_id>"`` - and handed the CAS store opened below plus
the job's settings. It is never imported by name: the dispatcher used to
send a ``"module:QualName"`` string resolved here by reflection, which could
only zero-arg construct a class and so could not give a stage the store it
writes its output through.

Must never import Qt and must never touch SQLite - the dispatcher is the
only component that writes job state (``app/scheduler/dispatcher.py``). The
Qt half is proven by an import-linter ``forbidden`` contract (see
``pyproject.toml``, demonstrated failing before being trusted - see this
task's report). There is no equivalent mechanical proof for SQLite:
``CasStore`` structurally requires a ``sqlite3.Connection`` to construct
(Task 8 split its writer-only methods out but not its constructor), so this
module opens a throwaway ``:memory:`` connection purely to satisfy that
signature. Nothing on this module's call path - ``CasStore.stage_file`` and
``.exists()``/``.path_for()``, the only ``CasStore`` methods ``run_stage``
and this module use - ever executes a statement against it; both are
filesystem-only, and ``build_stage`` only constructs. The discipline is
structural (grep this file and ``runner.run_stage`` for ``.execute(``: zero
matches), not linter-enforced.

``Staged`` messages are derived here, never from the stage itself: once
``run_stage`` returns a ``Result``, every artifact it names has already been
verified to exist in the CAS (Task 12's whole point - a stage cannot lie
about what it produced without the parent finding out). That verification is
what makes ``kind`` (off the ``ArtifactLine``) and ``size_bytes`` (a
``stat()`` of ``cas.path_for(digest)``) safe to trust here.
"""

from __future__ import annotations

import io
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

from ytauto.app.registry import build_stage
from ytauto.app.scheduler.runner import RunnerContext, run_stage
from ytauto.app.scheduler.worker_protocol import Error, Message, Result, Staged, encode
from ytauto.core.errors import ErrorKind
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.content_hash import ContentHash
from ytauto.core.pipeline.stage import JobContext, Stage
from ytauto.infra.cas.store import CasStore


def _build_context(assignment: dict[str, Any]) -> JobContext:
    """Reconstruct the ``JobContext`` a dispatcher-side assignment describes."""
    inputs: dict[str, tuple[ArtifactRef, ...]] = {
        stage_id: tuple(
            ArtifactRef(name=a["name"], kind=a["kind"], digest=ContentHash(a["digest"]))
            for a in artifacts
        )
        for stage_id, artifacts in assignment["inputs"].items()
    }
    return JobContext(
        job_id=assignment["job_id"],
        project_id=assignment["project_id"],
        settings=assignment["settings"],
        inputs=inputs,
        workdir=Path(assignment["workdir"]),
    )


def _use_utf8_stdout() -> None:
    """Pin this worker's stdout to UTF-8, whatever the host's locale says.

    ``print`` here and ``Popen(..., text=True)`` in the parent both default
    to the locale codec - cp1252 on a typical Windows box, utf-8 on macOS -
    so the same worker line means different things on different machines.
    Every protocol line survives that by luck rather than design:
    ``worker_protocol.encode`` goes through ``json.dumps``, whose
    ``ensure_ascii`` default escapes non-ASCII to ``\\uXXXX``. Nothing else
    the process writes is so lucky. One character outside cp1252 printed by a
    stage, or by a library a stage calls, raises ``UnicodeEncodeError`` at
    the write - which becomes a FATAL stage error at best, and at worst kills
    the worker before it emits anything, with the traceback on a stderr the
    parent never reads.

    ``sys.stdout`` is a ``TextIOWrapper`` whenever this module is run the way
    it is meant to be (``python -m ytauto.app.worker`` with a pipe); the
    isinstance check is there so an embedded caller that replaced it with
    something else is left alone rather than crashed.
    """
    if isinstance(sys.stdout, io.TextIOWrapper):
        sys.stdout.reconfigure(encoding="utf-8")


def _emit(message: Message) -> None:
    print(encode(message), flush=True)


def _fingerprint_disagreement(
    stage: Stage, ctx: JobContext, assignment: dict[str, Any], correlation_id: str
) -> Error | None:
    """Refuse to run a stage this worker fingerprints differently to its parent.

    The dispatcher computes a stage's fingerprint from its *own* copy of that
    stage - built once per process, from whatever settings its caller passed -
    and records the stage's artifacts under it. This worker builds the stage
    again, per job, from that job's real settings (``registry.build_stage``).
    Nothing reconciled the two: ``run_stage`` verifies that every artifact a
    stage claims exists in the CAS, which says nothing about the digest it
    gets indexed under.

    So a stage whose ``fingerprint`` depends on anything decided at
    construction time - a provider chosen from the settings its factory was
    handed being the obvious one - would have its output recorded under a
    digest the executed configuration never reproduces. Every later run
    recomputes the parent's digest, misses, re-runs, and records again:
    silent, permanent cache poisoning, with nothing failing anywhere.

    ``FATAL`` rather than ``RETRYABLE`` because a retry rebuilds the same two
    disagreeing stage objects from the same two sets of settings and reaches
    the same conclusion, having burned an attempt to do it. Both digests are
    named because the disagreement, not either value, is the bug.

    A ``fingerprint`` that *raises* is translated the same way, for the same
    reason ``run_stage`` translates everything ``stage.run`` raises into a
    ``FATAL`` error (``runner.py``): ``fingerprint`` is stage code by the same
    definition, and catching one stage method while letting the other kill the
    process is the inconsistency, not the catch. Letting it escape would kill
    the worker with no terminal message, which lands in ``_pump``'s death
    branch - one attempt charged, 5/10/20/40/80 s of backoff, and only then a
    failed job. That is roughly two and a half minutes of *guaranteed
    identical* failures before an operator sees anything, because a raising
    fingerprint is deterministic: the settings arrive over the pipe byte-for-
    byte the same on every attempt. The traceback is the real cost of
    catching, so the message keeps ``f"{type(exc).__name__}: {exc}"`` - the
    same shape ``run_stage`` uses - and the exception type survives into
    ``jobs.last_error`` even though the traceback does not.

    ``main``'s carve-out for a malformed assignment (crash rather than emit)
    does not reach here: that exists because there is no trustworthy
    job/stage/correlation id to stamp a message with, and by this point all
    three have been read.

    Returns None when the two agree, which is every correctly-written stage.

    Raises:
        Nothing.
    """
    try:
        computed = stage.fingerprint(ctx)
    except Exception as exc:
        return Error(
            job_id=assignment["job_id"],
            stage_id=stage.id,
            correlation_id=correlation_id,
            message=(
                f"stage {stage.id!r} could not fingerprint itself: {type(exc).__name__}: {exc}"
            ),
            kind=ErrorKind.FATAL,
        )
    expected = assignment["fingerprint"]
    if computed == expected:
        return None
    return Error(
        job_id=assignment["job_id"],
        stage_id=stage.id,
        correlation_id=correlation_id,
        message=(
            f"stage {stage.id!r} fingerprints as {computed} in this worker but the "
            f"dispatcher assigned it {expected}; the two processes built different "
            f"stages, and running would record artifacts under a digest this "
            f"configuration never reproduces"
        ),
        kind=ErrorKind.FATAL,
    )


def main() -> int:
    """Run one stage assignment read from stdin. Returns the process exit code.

    A stage is only run once this worker's own fingerprint for it agrees with
    the one the dispatcher assigned - see ``_fingerprint_disagreement``, which
    is checked after the context is built (so there is something to
    fingerprint) and before ``run_stage`` (so nothing is written under a
    digest that will never be looked up again).

    Raises:
        Nothing from a well-formed assignment: every exception a stage can
        produce is translated into an ``Error`` message and reported via the
        exit code rather than raised here - ``run_stage`` does that for
        ``stage.run``, and ``_fingerprint_disagreement`` for
        ``stage.fingerprint``. A malformed assignment (missing JSON keys, a
        ``pipeline_id`` and ``stage_id`` pair no entry point is registered
        under) is deliberately NOT caught - there is no trustworthy
        job/stage/correlation id to stamp a protocol ``error`` message with in
        that case, so the process crashes with a traceback on stderr and a
        nonzero exit instead of emitting a message that could be wrong. Both
        stage-code translations sit *after* those three ids have been read,
        which is exactly why they emit rather than crash.
    """
    _use_utf8_stdout()
    assignment: dict[str, Any] = json.loads(sys.stdin.read())
    job_id = assignment["job_id"]
    stage_id = assignment["stage_id"]
    correlation_id = assignment["correlation_id"]

    ctx = _build_context(assignment)
    runner_ctx = RunnerContext(job=ctx, job_id=job_id, correlation_id=correlation_id)

    # CasStore's constructor requires a connection (see the module
    # docstring); this one is never queried or written to. The stage is built
    # inside this block, not before it, because a stage is handed the store to
    # write its own output through - which is the whole reason the registry
    # takes a factory rather than a class.
    memory_conn = sqlite3.connect(":memory:")
    try:
        cas = CasStore(root=Path(assignment["cas_root"]), conn=memory_conn)
        stage = build_stage(assignment["pipeline_id"], stage_id, cas, assignment["settings"])
        # Before running anything: the stage this worker rebuilt must agree
        # with the one the dispatcher fingerprinted, or its output would be
        # indexed under a digest nothing reproduces.
        disagreement = _fingerprint_disagreement(stage, ctx, assignment, correlation_id)
        message: Message = (
            disagreement if disagreement is not None else run_stage(stage, runner_ctx, cas)
        )

        if isinstance(message, Result):
            for artifact in message.artifacts:
                digest = ContentHash(artifact.digest)
                size_bytes = cas.path_for(digest).stat().st_size
                _emit(
                    Staged(
                        job_id=job_id,
                        stage_id=stage_id,
                        correlation_id=correlation_id,
                        digest=artifact.digest,
                        kind=artifact.kind,
                        size_bytes=size_bytes,
                    )
                )
    finally:
        memory_conn.close()

    _emit(message)
    return 0 if isinstance(message, Result) else 1


if __name__ == "__main__":
    raise SystemExit(main())
