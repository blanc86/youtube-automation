"""Phase 1b's two exit criteria.

**Criterion 1 (resume).** A synthetic three-stage job (``fetch -> tts ->
render``) runs under the dispatcher, is killed mid-flight by genuinely
terminating the worker subprocess (``Popen.kill()`` - never a faked death),
and on restart resumes from its last completed stage without re-running
completed work.

**Criterion 2 (the cache actually caches).** The same job, run twice, must
hit the cache on *every* stage the second time, with no downstream stage
re-running. This is deliberately a separate test from criterion 1: killed in
stage 2 and resumed at stage 2, stage 3 in the resume test only ever runs
once - so an artifact-ordering bug that silently disables caching for stage 3
would be invisible to criterion 1 alone. See Step 4 of this task's report for
the experiment that proves the two criteria really do catch different bugs.

Both tests spawn real ``python -m ytauto.app.worker`` subprocesses, so they
are integration tests, not unit tests - marked accordingly and run as the
gate's separate integration step.

**Cross-process plumbing.** ``app/worker.py`` resolves a stage via reflection
off ``"module:QualName"`` and zero-arg-constructs it (Task 13's placeholder
for the provider registry Phase 2 will build - see that module's
docstring), so the three synthetic ``Stage`` classes below live at module
scope in *this* file and take no constructor arguments. Two consequences
follow:

1. The worker subprocess must be able to ``importlib.import_module`` this
   module. Pytest's own import makes it reachable as ``integration.test_resume``
   from ``tests/`` (rootdir-insertion: ``tests/integration/__init__.py``
   exists, ``tests/__init__.py`` does not, so pytest treats ``tests/`` as the
   package root) - but that only puts ``tests/`` on *this* process's
   ``sys.path``, not the subprocess's. The ``env`` fixture below propagates it
   via the ``PYTHONPATH`` environment variable, which ``Popen`` (no explicit
   ``env=`` in ``dispatcher._spawn``) inherits from this process at call time.
2. A zero-arg stage has no constructor-injected ``CasStore`` to write its own
   output through (a real ``Stage`` gets one at construction - see
   ``runner.py``'s module docstring). The synthetic stages recover the CAS
   root the same out-of-band way: an environment variable the ``env`` fixture
   sets before any dispatcher tick runs.

Each stage's execution counter (Step 2 of the brief) lives in a third
environment-variable-addressed location: a directory of one append-only file
per stage id, deliberately keyed by stage id alone rather than job id, so the
twice-run test's assertion ("still 1" after a second job) is checking the
*same* file both times.
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from ytauto.app.scheduler.dispatcher import Dispatcher
from ytauto.app.scheduler.governor import Governor
from ytauto.app.scheduler.queue import JobQueue
from ytauto.app.scheduler.runner import build_spec
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.content_hash import ContentHash
from ytauto.core.models.job import JobState, StageStatus
from ytauto.core.pipeline.fingerprint import compute_fingerprint
from ytauto.core.pipeline.graph import Pipeline
from ytauto.core.pipeline.stage import JobContext, ProgressFn, Stage, StageResult
from ytauto.infra.artifacts import ArtifactStore
from ytauto.infra.cas.store import CasStore
from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import apply_migrations

pytestmark = pytest.mark.integration

# -- cross-process plumbing (see module docstring) --------------------------

_TESTS_ROOT = Path(__file__).resolve().parent.parent
_CAS_ROOT_ENV = "YTAUTO_T14_CAS_ROOT"
_COUNTER_DIR_ENV = "YTAUTO_T14_COUNTER_DIR"
_KILL_STAGE_ENV = "YTAUTO_T14_KILL_STAGE"
_PIPELINE_ID = "t14-three-stage"
_PAUSE_S = 20.0
"""Upper bound on how long a to-be-killed stage waits before giving up and
running anyway. Bounds a test bug (the kill never arrives) to a slow failure
instead of an indefinite hang."""


def _write_blob(data: bytes, *, kind: str) -> ContentHash:
    """Write ``data`` into the CAS root the ``env`` fixture published.

    Filesystem-only, mirroring ``app/worker.py``'s own throwaway ``:memory:``
    connection - see that module's docstring for why ``CasStore``'s
    constructor needs a connection at all despite never executing a
    statement against it here.
    """
    conn = sqlite3.connect(":memory:")
    try:
        cas = CasStore(root=Path(os.environ[_CAS_ROOT_ENV]), conn=conn)
        return cas.stage_file(data, kind=kind)
    finally:
        conn.close()


def _record_run(stage_id: str) -> None:
    """Append one line to this stage's execution-counter file.

    Keyed by stage id alone, not job id - the twice-run test relies on a
    second job's cache hit leaving the *same* file untouched to prove the
    stage did not re-execute.
    """
    counter_dir = Path(os.environ[_COUNTER_DIR_ENV])
    counter_dir.mkdir(parents=True, exist_ok=True)
    with (counter_dir / f"{stage_id}.count").open("a", encoding="utf-8") as handle:
        handle.write("1\n")


def _maybe_pause_for_kill(ctx: JobContext, stage_id: str) -> None:
    """Give the resume test a real process to kill.

    A no-op unless ``_KILL_STAGE_ENV`` names this exact stage. On the first
    attempt (no marker file yet) it writes the marker the test polls for -
    proof a real subprocess is inside ``run()`` - and pauses so the test has
    time to call ``Popen.kill()`` for real. The resumed attempt (marker
    already present, because ``ctx.workdir`` is job+stage scoped and survives
    the kill) returns immediately and runs normally.
    """
    if os.environ.get(_KILL_STAGE_ENV) != stage_id:
        return
    marker = ctx.workdir / "started.marker"
    if marker.exists():
        return
    marker.write_text("started", encoding="utf-8")
    deadline = time.monotonic() + _PAUSE_S
    while time.monotonic() < deadline:
        time.sleep(0.05)


def _fingerprint(stage: Stage, ctx: JobContext) -> str:
    """The sanctioned pattern (``runner.build_spec`` + ``compute_fingerprint``),
    the same one a real Stage implementation is expected to use."""
    spec = build_spec(stage, "synthetic", "1", ctx.inputs, ctx.settings)
    return compute_fingerprint(spec)


class FetchStage:
    """Stage 1: no inputs, one deterministic output blob."""

    id = "fetch"
    version = 1
    depends_on: tuple[str, ...] = ()

    def fingerprint(self, ctx: JobContext) -> str:
        return _fingerprint(self, ctx)

    def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult:
        _record_run(self.id)
        _maybe_pause_for_kill(ctx, self.id)
        digest = _write_blob(b"fetched script bytes", kind="blob")
        return StageResult(artifacts=(ArtifactRef(name="script", kind="blob", digest=digest),))


class TtsStage:
    """Stage 2: consumes fetch's output, produces *two* deterministic blobs.

    Declared here in reverse-alphabetical order (``timings`` before
    ``narration``) on purpose - a single-artifact stage can never exercise
    ``StageResult.__post_init__``'s sort at all, since a one-element tuple is
    already "sorted" no matter what. Step 4 of this task's report needs a
    declaration order that disagrees with name order to have any chance of
    showing something when that sort is reverted.
    """

    id = "tts"
    version = 1
    depends_on: tuple[str, ...] = ("fetch",)

    def fingerprint(self, ctx: JobContext) -> str:
        return _fingerprint(self, ctx)

    def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult:
        _record_run(self.id)
        _maybe_pause_for_kill(ctx, self.id)
        script = ctx.input("fetch", "script")
        timings_digest = _write_blob(b"timings:" + script.digest.encode("ascii"), kind="blob")
        narration_digest = _write_blob(b"narrated:" + script.digest.encode("ascii"), kind="blob")
        return StageResult(
            artifacts=(
                ArtifactRef(name="timings", kind="blob", digest=timings_digest),
                ArtifactRef(name="narration", kind="blob", digest=narration_digest),
            )
        )


class RenderStage:
    """Stage 3: consumes tts's two outputs in declaration order, one output blob.

    Concatenating ``ctx.inputs["tts"]`` in whatever order it arrives - rather
    than looking each artifact up by name - is deliberate: it is what makes
    this stage's own fingerprint and output content sensitive to upstream
    order at all, mirroring the "concatenating clips the other way round is a
    different video" scenario ``StageResult``'s docstring warns about.
    """

    id = "render"
    version = 1
    depends_on: tuple[str, ...] = ("tts",)

    def fingerprint(self, ctx: JobContext) -> str:
        return _fingerprint(self, ctx)

    def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult:
        _record_run(self.id)
        _maybe_pause_for_kill(ctx, self.id)
        upstream = ctx.inputs.get("tts", ())
        payload = b"rendered:" + b",".join(a.digest.encode("ascii") for a in upstream)
        digest = _write_blob(payload, kind="blob")
        return StageResult(artifacts=(ArtifactRef(name="video", kind="blob", digest=digest),))


def _pipeline() -> Pipeline:
    return Pipeline(id=_PIPELINE_ID, stages=(FetchStage(), TtsStage(), RenderStage()))


# -- fixtures -----------------------------------------------------------


@pytest.fixture()
def db_conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """A migrated database. Closed on teardown so Windows can delete tmp_path."""
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture()
def cas(tmp_path: Path, db_conn: sqlite3.Connection) -> CasStore:
    return CasStore(root=tmp_path / "cas", conn=db_conn)


@pytest.fixture()
def artifacts(cas: CasStore, db_conn: sqlite3.Connection) -> ArtifactStore:
    return ArtifactStore(cas, db_conn)


@pytest.fixture()
def queue(db_conn: sqlite3.Connection) -> JobQueue:
    return JobQueue(db_conn)


@pytest.fixture()
def env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, cas: CasStore) -> Path:
    """Wire the synthetic stages' three out-of-band channels (see module
    docstring) and return the counter directory.

    ``PYTHONPATH`` is extended, not replaced, in case the host environment
    already sets one.
    """
    counters = tmp_path / "counters"
    existing = os.environ.get("PYTHONPATH")
    pythonpath = os.pathsep.join([str(_TESTS_ROOT), existing]) if existing else str(_TESTS_ROOT)
    monkeypatch.setenv("PYTHONPATH", pythonpath)
    monkeypatch.setenv(_CAS_ROOT_ENV, str(cas.root))
    monkeypatch.setenv(_COUNTER_DIR_ENV, str(counters))
    monkeypatch.delenv(_KILL_STAGE_ENV, raising=False)
    return counters


# -- helpers --------------------------------------------------------------


def _stage_status(conn: sqlite3.Connection, job_id: str, stage_id: str) -> str | None:
    row = conn.execute(
        "SELECT status FROM job_stages WHERE job_id = ? AND stage_id = ?", (job_id, stage_id)
    ).fetchone()
    return str(row["status"]) if row is not None else None


def _stage_fingerprint(conn: sqlite3.Connection, job_id: str, stage_id: str) -> str:
    row = conn.execute(
        "SELECT fingerprint FROM job_stages WHERE job_id = ? AND stage_id = ?", (job_id, stage_id)
    ).fetchone()
    assert row is not None and row["fingerprint"] is not None
    return str(row["fingerprint"])


def _job_state(conn: sqlite3.Connection, job_id: str) -> str:
    row = conn.execute("SELECT state FROM jobs WHERE id = ?", (job_id,)).fetchone()
    assert row is not None
    return str(row["state"])


def _run_count(counters: Path, stage_id: str) -> int:
    path = counters / f"{stage_id}.count"
    if not path.is_file():
        return 0
    return len(path.read_text(encoding="utf-8").splitlines())


def _wait_for(path: Path, *, timeout_s: float = 20.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.05)
    raise TimeoutError(f"timed out waiting for {path}")


# -- criterion 1: resume ----------------------------------------------------


def test_a_killed_worker_resumes_from_its_last_completed_stage(
    db_conn: sqlite3.Connection,
    cas: CasStore,
    artifacts: ArtifactStore,
    queue: JobQueue,
    env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The dispatcher's process-isolation contract: a genuinely killed worker
    loses no completed work, and the job finishes on resume."""
    monkeypatch.setenv(_KILL_STAGE_ENV, "tts")
    pipeline = _pipeline()
    job_id = "resume-job"
    queue.enqueue(job_id, "proj-1", _PIPELINE_ID)

    dispatcher = Dispatcher(
        db_conn, cas, artifacts, Governor(), queue, pipelines={_PIPELINE_ID: pipeline}
    )

    # Stage 1 (fetch): runs to completion normally, through a real worker
    # subprocess, no killing involved.
    report = dispatcher.tick()
    assert report.spawned == ("fetch",)
    assert _stage_status(db_conn, job_id, "fetch") == StageStatus.SUCCEEDED.value

    # Stage 2 (tts): tick() blocks reading the worker's stdout until it
    # closes, so the spawn+kill dance needs a second thread. The main thread
    # waits for proof a real subprocess is inside run() (the marker file),
    # then kills that process for real.
    outcome: dict[str, object] = {}

    def _drive_stage_two() -> None:
        outcome["report"] = dispatcher.tick()

    driver = threading.Thread(target=_drive_stage_two)
    driver.start()

    marker = cas.root.parent / "work" / job_id / "tts" / "started.marker"
    _wait_for(marker)

    owner = f"{job_id}:tts"
    proc = dispatcher._running[owner]
    proc.kill()

    driver.join(timeout=30)
    assert not driver.is_alive(), "dispatcher.tick() never returned after the worker was killed"

    # The killed attempt must not be recorded as done, and completed work
    # must survive the kill.
    assert _stage_status(db_conn, job_id, "tts") != StageStatus.SUCCEEDED.value
    assert _stage_status(db_conn, job_id, "fetch") == StageStatus.SUCCEEDED.value
    assert _run_count(env, "tts") == 1, "the killed attempt should have run exactly once so far"

    # "Restart": a brand-new Dispatcher and Governor, holding none of the
    # original's in-flight bookkeeping (no _running entry, no lease), driven
    # only by what the database recorded. reap() is what a real restart runs
    # first; here it is a no-op, since dispatcher.tick()'s own pump already
    # detected the dead worker (stdout closed with no terminal message) and
    # reset+requeued synchronously, before this line ever runs.
    resumed = Dispatcher(
        db_conn, cas, artifacts, Governor(), queue, pipelines={_PIPELINE_ID: pipeline}
    )
    resumed.reap()
    final = resumed.run_until_idle(max_ticks=10)
    assert final.idle

    assert _job_state(db_conn, job_id) == JobState.SUCCEEDED.value
    for stage_id in ("fetch", "tts", "render"):
        assert _stage_status(db_conn, job_id, stage_id) == StageStatus.SUCCEEDED.value

    # Stage 1 was not re-executed - an execution counter, not a timing guess.
    assert _run_count(env, "fetch") == 1
    # Stage 2 ran twice: the killed attempt, then the resumed one.
    assert _run_count(env, "tts") == 2
    # Stage 3 only ever ran once, cleanly, after the resume.
    assert _run_count(env, "render") == 1

    # Every blob the job produced is present.
    for stage_id in ("fetch", "tts", "render"):
        fingerprint = _stage_fingerprint(db_conn, job_id, stage_id)
        produced = artifacts.lookup(fingerprint)
        assert produced is not None, f"{stage_id}'s artifacts were not recorded"
        for artifact in produced:
            assert cas.exists(artifact.digest), f"{stage_id}'s blob is missing from the CAS"


# -- criterion 2: the cache actually caches ----------------------------------


def test_running_the_same_job_twice_hits_the_cache_on_every_stage(
    db_conn: sqlite3.Connection,
    cas: CasStore,
    artifacts: ArtifactStore,
    queue: JobQueue,
    env: Path,
) -> None:
    """Criterion 1 cannot catch artifact-order drift: killed in stage 2 and
    resuming at stage 2, stage 3 was never cached, so the drift is invisible.
    Run the whole job twice and every stage must be a cache hit, with no
    downstream stage re-running."""
    pipeline = _pipeline()
    dispatcher = Dispatcher(
        db_conn, cas, artifacts, Governor(), queue, pipelines={_PIPELINE_ID: pipeline}
    )

    queue.enqueue("job-one", "proj-1", _PIPELINE_ID)
    first = dispatcher.run_until_idle(max_ticks=10)
    assert set(first.spawned) == {"fetch", "tts", "render"}
    assert first.skipped == ()
    assert _job_state(db_conn, "job-one") == JobState.SUCCEEDED.value
    for stage_id in ("fetch", "tts", "render"):
        assert _run_count(env, stage_id) == 1

    queue.enqueue("job-two", "proj-1", _PIPELINE_ID)
    second = dispatcher.run_until_idle(max_ticks=10)
    assert second.spawned == ()
    assert set(second.skipped) == {"fetch", "tts", "render"}
    assert _job_state(db_conn, "job-two") == JobState.SUCCEEDED.value

    for stage_id in ("fetch", "tts", "render"):
        assert _stage_status(db_conn, "job-two", stage_id) == StageStatus.SKIPPED.value
        assert _run_count(env, stage_id) == 1, f"{stage_id} re-executed on the second run"

    # The second job's artifacts are literally the first job's - the point
    # of content-addressed, cross-job dedup.
    for stage_id in ("fetch", "tts", "render"):
        fp_one = _stage_fingerprint(db_conn, "job-one", stage_id)
        fp_two = _stage_fingerprint(db_conn, "job-two", stage_id)
        assert fp_one == fp_two
        assert artifacts.lookup(fp_one) == artifacts.lookup(fp_two)
