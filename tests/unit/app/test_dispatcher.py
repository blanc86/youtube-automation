import io
import os
import sqlite3
import subprocess
import threading
from collections.abc import Iterator, Sequence
from contextlib import suppress
from pathlib import Path

import pytest

import ytauto.app.scheduler.dispatcher as dispatcher_module
from ytauto.app.scheduler.dispatcher import _MAX_STAGE_ATTEMPTS, Dispatcher, StagedArtifact
from ytauto.app.scheduler.governor import Governor
from ytauto.app.scheduler.queue import JobQueue
from ytauto.app.scheduler.worker_protocol import Error
from ytauto.core.errors import ErrorKind
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.content_hash import ContentHash
from ytauto.core.pipeline.graph import Pipeline
from ytauto.core.pipeline.stage import JobContext, ProgressFn, StageResult
from ytauto.infra.artifacts import ArtifactStore
from ytauto.infra.cas.eviction import EvictionPolicy, Evictor
from ytauto.infra.cas.store import CasStore
from ytauto.infra.clock import utc_now_iso
from ytauto.infra.db.engine import connect, transaction
from ytauto.infra.db.migrations import apply_migrations

# db_conn is defined in tests/unit/conftest.py.

_FETCH_FINGERPRINT = "a" * 64
_TTS_FINGERPRINT = "b" * 64
_SOLO_FINGERPRINT = "c" * 64
_TEST_PIPELINE_ID = "test-pipeline"
_SOLO_PIPELINE_ID = "solo-pipeline"

_FINGERPRINTS = {"fetch": _FETCH_FINGERPRINT, "tts": _TTS_FINGERPRINT, "only": _SOLO_FINGERPRINT}


class _FixedStage:
    """A minimal Stage double whose fingerprint is a fixed constant.

    Real stages compute their fingerprint from inputs and settings (see
    test_runner.py's ``_FakeStage``). These tests instead need the
    dispatcher's own probe to land on a fingerprint the test can predict in
    advance, without replicating JobContext/build_spec/compute_fingerprint
    here - so it is a fixed constant per stage_id.
    """

    def __init__(self, stage_id: str, depends_on: tuple[str, ...] = ()) -> None:
        self.id = stage_id
        self.version = 1
        self.depends_on = depends_on

    def fingerprint(self, ctx: JobContext) -> str:
        return _FINGERPRINTS[self.id]

    def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult:
        raise NotImplementedError(
            "not exercised: none of these unit tests let a real worker run a "
            "stage - spawn_spy replaces subprocess.Popen; the real spawn is "
            "exercised in Task 14"
        )


def _test_pipeline() -> Pipeline:
    """fetch -> tts."""
    return Pipeline(
        id=_TEST_PIPELINE_ID,
        stages=(_FixedStage("fetch"), _FixedStage("tts", depends_on=("fetch",))),
    )


def _solo_pipeline() -> Pipeline:
    return Pipeline(id=_SOLO_PIPELINE_ID, stages=(_FixedStage("only"),))


class _FakeProcess:
    """A minimal subprocess.Popen double: already exited, empty stdout.

    None of the seven tests below exercise a genuine spawn-and-pump round
    trip (Task 14 does that with a real subprocess); this only has to be
    safe to construct and read from if a test's dispatcher does reach
    _spawn.
    """

    def __init__(self) -> None:
        self.pid = -1
        self.returncode = 0
        self.stdin: io.StringIO = io.StringIO()
        self.stdout: io.StringIO | None = io.StringIO("")
        self.stderr: io.StringIO | None = io.StringIO("")

    def wait(self, timeout: float | None = None) -> int:
        return self.returncode

    def poll(self) -> int | None:
        return self.returncode

    def kill(self) -> None:
        pass

    def terminate(self) -> None:
        pass


class SpawnSpy:
    """Replaces subprocess.Popen so unit tests never spawn a real process.

    The real spawn is exercised in Task 14. Here it is only ever necessary
    to prove a worker WOULD have been started - and, for the cache-hit test,
    that it was NOT.
    """

    def __init__(self) -> None:
        self.calls = 0
        self.argv: list[tuple[str, ...]] = []
        self.kwargs: list[dict[str, object]] = []

    def __call__(self, argv: Sequence[str], **kwargs: object) -> _FakeProcess:
        self.calls += 1
        self.argv.append(tuple(argv))
        self.kwargs.append(dict(kwargs))
        return _FakeProcess()


class _ClosedPipe(io.StringIO):
    """The stdin of a worker that exited before the parent could write to it."""

    def write(self, s: str, /) -> int:
        raise BrokenPipeError(32, "Broken pipe")


class DeadOnArrivalSpawnSpy(SpawnSpy):
    """A spawn whose process is already gone by the time stdin is written."""

    def __call__(self, argv: Sequence[str], **kwargs: object) -> _FakeProcess:
        proc = super().__call__(argv, **kwargs)
        proc.stdin = _ClosedPipe()
        return proc


class _NeverExitingProcess:
    """A subprocess.Popen double whose stdout blocks forever until killed.

    Backed by a genuine OS pipe (``os.pipe()``), not ``io.StringIO``: an
    empty ``StringIO`` reaches EOF immediately, which cannot exercise the
    blocking read the pump deadline (Step 5's test) needs to interrupt. The
    pipe's write end is never closed by anything except ``kill()`` - a silent,
    immortal worker - so ``_pump``'s ``for raw_line in stdout`` loop stays
    blocked until something calls it, exactly the way killing a real worker
    closes its real stdout pipe and ends the loop through its ordinary EOF
    path.
    """

    def __init__(self) -> None:
        self.pid = -1
        self.returncode: int | None = None
        read_fd, self._write_fd = os.pipe()
        self.stdin: io.StringIO = io.StringIO()
        self.stdout: io.TextIOWrapper | None = io.TextIOWrapper(
            io.FileIO(read_fd, "rb"), encoding="utf-8"
        )
        self.stderr: io.StringIO | None = io.StringIO("")
        self._exited = threading.Event()

    def wait(self, timeout: float | None = None) -> int:
        if not self._exited.wait(timeout=timeout):
            raise subprocess.TimeoutExpired(cmd="never-exiting", timeout=timeout or 0)
        assert self.returncode is not None
        return self.returncode

    def poll(self) -> int | None:
        return self.returncode

    def kill(self) -> None:
        if self._exited.is_set():
            return
        self.returncode = -9
        os.close(self._write_fd)
        self._exited.set()

    def terminate(self) -> None:
        self.kill()


class NeverExitingSpawnSpy:
    """Replaces subprocess.Popen with a worker that writes nothing and never
    exits - the case the pump deadline (not the stderr-file fix) bounds.

    Not a SpawnSpy subclass: SpawnSpy.__call__ is typed to return
    _FakeProcess, and this needs to return an unrelated double.
    """

    def __init__(self) -> None:
        self.calls = 0
        self.processes: list[_NeverExitingProcess] = []

    def __call__(self, argv: Sequence[str], **kwargs: object) -> _NeverExitingProcess:
        self.calls += 1
        proc = _NeverExitingProcess()
        self.processes.append(proc)
        return proc


@pytest.fixture()
def store(tmp_path: Path, db_conn: sqlite3.Connection) -> CasStore:
    """A CasStore sharing the migrated connection from ``db_conn``."""
    return CasStore(root=tmp_path / "cas", conn=db_conn)


@pytest.fixture()
def artifacts(store: CasStore, db_conn: sqlite3.Connection) -> ArtifactStore:
    return ArtifactStore(store, db_conn)


@pytest.fixture()
def queue(db_conn: sqlite3.Connection) -> JobQueue:
    return JobQueue(db_conn)


@pytest.fixture()
def governor() -> Governor:
    return Governor()


@pytest.fixture()
def dispatcher(
    db_conn: sqlite3.Connection,
    store: CasStore,
    artifacts: ArtifactStore,
    governor: Governor,
    queue: JobQueue,
) -> Dispatcher:
    """A Dispatcher over a fresh database, with job "j1" already claimed and
    its "tts" stage marked running - simulating a worker mid-flight. That is
    the baseline every reap()-focused test below needs (a job whose lease has
    already expired, with a stage still 'running' for reap() to find and
    reset). Tests that instead need "j1" freshly claimable
    (the cache-hit test) requeue it themselves via _prerecord_stage_output.
    """
    d = Dispatcher(
        db_conn,
        store,
        artifacts,
        governor,
        queue,
        pipelines={_TEST_PIPELINE_ID: _test_pipeline(), _SOLO_PIPELINE_ID: _solo_pipeline()},
    )
    queue.enqueue("j1", "p1", _TEST_PIPELINE_ID)
    queue.claim("baseline-owner", lease_s=-1)  # already expired
    _mark_stage(db_conn, "j1", "tts", "running")
    return d


@pytest.fixture()
def spawn_spy(monkeypatch: pytest.MonkeyPatch) -> SpawnSpy:
    spy = SpawnSpy()
    monkeypatch.setattr(dispatcher_module, "Popen", spy)
    return spy


def _mark_stage(conn: sqlite3.Connection, job_id: str, stage_id: str, status: str) -> None:
    """Upsert one job_stages row. Assumes the job row already exists - every
    test using this reaches it through the ``dispatcher`` fixture's baseline
    or its own explicit enqueue first."""
    now = utc_now_iso()
    with transaction(conn):
        conn.execute(
            "INSERT INTO job_stages (job_id, stage_id, status, started_at) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(job_id, stage_id) DO UPDATE SET status = excluded.status",
            (job_id, stage_id, status, now),
        )


def _status(conn: sqlite3.Connection, job_id: str, stage_id: str) -> str | None:
    row = conn.execute(
        "SELECT status FROM job_stages WHERE job_id = ? AND stage_id = ?", (job_id, stage_id)
    ).fetchone()
    return str(row["status"]) if row is not None else None


def _dispatcher(
    conn: sqlite3.Connection, tmp_path: Path, *, pump_deadline_s: float = 1800.0
) -> Dispatcher:
    """Build a fresh Dispatcher over the two-stage test pipeline, with nothing
    enqueued yet.

    Unlike the ``dispatcher`` fixture (pre-wired with "j1" already claimed and
    its "tts" stage running - the baseline every reap()-focused test needs),
    this starts from an empty queue so a test can control both what gets
    enqueued and pump_deadline_s, which the fixture has no way to override.
    """
    store = CasStore(root=tmp_path / "cas", conn=conn)
    artifacts = ArtifactStore(store, conn)
    governor = Governor()
    queue = JobQueue(conn)
    return Dispatcher(
        conn,
        store,
        artifacts,
        governor,
        queue,
        pipelines={_TEST_PIPELINE_ID: _test_pipeline(), _SOLO_PIPELINE_ID: _solo_pipeline()},
        pump_deadline_s=pump_deadline_s,
    )


def _enqueue(
    conn: sqlite3.Connection, job_id: str, *, stages: tuple[str, ...] = ("fetch", "tts")
) -> None:
    """Enqueue a fresh job against ``_test_pipeline()`` (fetch -> tts).

    ``stages`` documents which stage(s) the calling test actually drives; the
    pipeline itself is always the fixed fetch->tts one, so passing a
    different tuple does not change what runs - it exists for readability at
    call sites.
    """
    JobQueue(conn).enqueue(job_id, "p1", _TEST_PIPELINE_ID)


def _job_last_error(conn: sqlite3.Connection, job_id: str) -> str:
    row = conn.execute("SELECT last_error FROM jobs WHERE id = ?", (job_id,)).fetchone()
    last_error = row["last_error"]
    return str(last_error) if last_error is not None else ""


def _job_state(conn: sqlite3.Connection, job_id: str) -> str:
    row = conn.execute("SELECT state FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return str(row["state"])


def _stage_attempts(conn: sqlite3.Connection, job_id: str, stage_id: str) -> int:
    row = conn.execute(
        "SELECT attempts FROM job_stages WHERE job_id = ? AND stage_id = ?", (job_id, stage_id)
    ).fetchone()
    return int(row["attempts"])


def _prerecord_stage_output(dispatcher: Dispatcher, *, job_id: str, stage_id: str) -> None:
    """Make ``stage_id`` a cache hit before ``tick()`` runs.

    Requeues the job (undoing the ``dispatcher`` fixture's baseline claim) so
    tick()'s own claim() can pick it up fresh, then records an artifact under
    exactly the fingerprint _FixedStage.fingerprint() reports for this
    stage_id - the same one tick()'s probe will look up.
    """
    dispatcher._queue.requeue(job_id, available_in_s=-1)
    data = f"{stage_id}-cached-output".encode()
    digest = dispatcher._cas.stage_file(data, kind="blob")
    dispatcher._cas.record_blob(digest, kind="blob", size_bytes=len(data))
    dispatcher._artifacts.record(
        _FINGERPRINTS[stage_id], stage_id, [ArtifactRef(name="out", kind="blob", digest=digest)]
    )


def _fail_the_job_stages_update(dispatcher: Dispatcher) -> None:
    """Monkeypatch commit_stage's final step to raise, without touching the
    connection at all - proving the other three steps (record_blob, retain,
    ArtifactStore.record) roll back together with it rather than partially
    surviving."""

    def _boom(job_id: str, stage_id: str, fingerprint: str) -> None:
        raise sqlite3.OperationalError("simulated job_stages update failure")

    dispatcher._mark_stage_succeeded = _boom  # type: ignore[method-assign]


def _staged(
    digest: ContentHash, *, name: str = "out", kind: str = "blob", size_bytes: int = 0
) -> StagedArtifact:
    return StagedArtifact(name=name, kind=kind, digest=digest, size_bytes=size_bytes)


def _error(job_id: str, stage_id: str, kind: ErrorKind, *, retry_after_s: float | None) -> Error:
    return Error(
        job_id=job_id,
        stage_id=stage_id,
        correlation_id="c1",
        message=f"{stage_id} failed: {kind.value}",
        kind=kind,
        retry_after_s=retry_after_s,
    )


def _complete_a_one_stage_job(dispatcher: Dispatcher, store: CasStore) -> ContentHash:
    """Run a fresh one-stage job to completion via commit_stage, returning its
    output digest so the caller can assert on the CAS's post-completion state."""
    job_id = "solo"
    dispatcher._queue.enqueue(job_id, "p1", _SOLO_PIPELINE_ID)
    dispatcher._queue.claim("solo-owner", lease_s=60)
    _mark_stage(dispatcher._conn, job_id, "only", "running")
    data = b"solo output"
    digest = store.stage_file(data, kind="blob")
    dispatcher.commit_stage(
        job_id, "only", _SOLO_FINGERPRINT, [_staged(digest, size_bytes=len(data))]
    )
    return digest


def test_a_cache_hit_marks_the_stage_skipped_without_spawning_a_worker(
    dispatcher: Dispatcher, spawn_spy: SpawnSpy
) -> None:
    """The single probe that delivers crash-resume, cheap iteration and
    cross-project dedup."""
    _prerecord_stage_output(dispatcher, job_id="j1", stage_id="fetch")

    report = dispatcher.tick()

    assert report.skipped == ("fetch",)
    assert spawn_spy.calls == 0, "a cache hit must not spawn a worker"


def test_a_claimed_job_with_nothing_ready_goes_back_to_the_queue(
    dispatcher: Dispatcher, queue: JobQueue, db_conn: sqlite3.Connection, spawn_spy: SpawnSpy
) -> None:
    """Claiming and then abandoning a job parks it for the whole lease.

    ``tick()`` is the only thing that claims, and it only ever claims
    ``state = 'queued'``, so a job left ``running`` with nothing ready is
    invisible to every dispatcher for the full 300 s lease - while the report
    says ``idle``, i.e. "nothing to do", rather than "I just hid a job".
    """
    _mark_stage(db_conn, "j1", "fetch", "running")  # the only ready stage, already in flight
    queue.requeue("j1", available_in_s=-1)

    report = dispatcher.tick()

    assert report.idle
    assert spawn_spy.calls == 0
    assert _job_state(db_conn, "j1") == "queued", "an unadvanceable claim must be released"


def test_a_refused_lease_is_not_reported_as_a_spawn(
    dispatcher: Dispatcher,
    governor: Governor,
    queue: JobQueue,
    db_conn: sqlite3.Connection,
    spawn_spy: SpawnSpy,
) -> None:
    """The refused-lease path claimed a spawn that never happened - and left
    the job claimed, so the "later tick" it defers to could never see it."""
    queue.requeue("j1", available_in_s=-1)
    governor.lease("gpu_compute", "somebody-else").__enter__()  # pool now full

    report = dispatcher.tick()

    assert spawn_spy.calls == 0, "no worker can start while the pool is full"
    assert report.spawned == (), "a refused lease is not a spawn"
    assert report.idle
    assert _job_state(db_conn, "j1") == "queued"


def test_the_worker_pipe_is_opened_with_an_explicit_encoding(
    dispatcher: Dispatcher, spawn_spy: SpawnSpy, queue: JobQueue
) -> None:
    """``text=True`` alone decodes with the host's locale codec - cp1252 on a
    typical Windows box, utf-8 on macOS - so a worker line would mean
    different things on different machines, and a byte cp1252 has no mapping
    for (0x81, 0x8d, 0x8f, 0x90, 0x9d - all of them ordinary continuation
    bytes in UTF-8) raises UnicodeDecodeError straight out of the pump's read
    loop. The worker pins the same encoding on its end; this pins the
    parent's, which no end-to-end test can reach today because every protocol
    line is pure ASCII (``json.dumps`` escapes non-ASCII by default)."""
    queue.requeue("j1", available_in_s=-1)

    dispatcher.tick()

    assert spawn_spy.kwargs[0]["encoding"] == "utf-8"


def test_a_worker_that_died_before_reading_its_assignment_does_not_strand_the_lease(
    dispatcher: Dispatcher,
    governor: Governor,
    queue: JobQueue,
    db_conn: sqlite3.Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_spawn``'s ``finally`` released the lease only ``if not started``,
    and ``started`` was set *before* the writes to ``proc.stdin``. A worker
    that had already exited made ``write`` raise ``BrokenPipeError`` out of
    ``tick()`` past that handler, leaving ``gpu_compute`` at 0 for the life
    of the process - one bad venv and the dispatcher never runs anything
    again."""
    spy = DeadOnArrivalSpawnSpy()
    monkeypatch.setattr(dispatcher_module, "Popen", spy)
    queue.requeue("j1", available_in_s=-1)

    report = dispatcher.tick()

    assert spy.calls == 1
    assert governor.available("gpu_compute") == 1, "the lease must not outlive the failed spawn"
    assert report.spawned == ("fetch",), "a worker was started; it just died immediately"
    assert _job_state(db_conn, "j1") == "queued"
    assert _stage_attempts(db_conn, "j1", "fetch") == 1, "a dead-on-arrival worker costs an attempt"


def test_a_stage_commit_is_atomic(
    dispatcher: Dispatcher, db_conn: sqlite3.Connection, store: CasStore
) -> None:
    """Blob rows, retains, the artifact record and the job_stages update land
    together or not at all."""
    digest = store.stage_file(b"audio", kind="blob")
    _fail_the_job_stages_update(dispatcher)  # monkeypatched to raise

    with pytest.raises(sqlite3.OperationalError):
        dispatcher.commit_stage("j1", "tts", "f" * 64, [_staged(digest)])

    assert db_conn.execute("SELECT count(*) FROM cas_objects").fetchone()[0] == 0
    assert db_conn.execute("SELECT count(*) FROM artifacts").fetchone()[0] == 0
    assert (
        db_conn.execute(
            "SELECT status FROM job_stages WHERE job_id='j1' AND stage_id='tts'"
        ).fetchone()["status"]
        != "succeeded"
    )


def test_a_dead_worker_releases_its_governor_leases(
    dispatcher: Dispatcher, governor: Governor
) -> None:
    """A worker cannot release what it held when it died; the reaper must."""
    governor.lease("gpu_compute", "j1:tts").__enter__()
    assert governor.available("gpu_compute") == 0

    dispatcher.reap()

    assert governor.available("gpu_compute") == 1


def test_a_dead_worker_s_job_returns_to_the_queue_at_its_last_completed_stage(
    dispatcher: Dispatcher, queue: JobQueue, db_conn: sqlite3.Connection
) -> None:
    _mark_stage(db_conn, "j1", "fetch", "succeeded")
    _mark_stage(db_conn, "j1", "tts", "running")

    dispatcher.reap()

    assert queue.claim("w2", lease_s=60) is not None, "the job must be claimable again"
    assert _status(db_conn, "j1", "fetch") == "succeeded", "completed work must survive"
    assert _status(db_conn, "j1", "tts") != "running", "the killed stage must be reset"


def test_a_RATE_LIMITED_error_defers_the_job_by_retry_after_s(
    dispatcher: Dispatcher, db_conn: sqlite3.Connection
) -> None:
    """Without available_at this could not be honoured at all - the claim query
    had no way to exclude a job that must not run until T."""
    dispatcher.handle_error(_error("j1", "tts", ErrorKind.RATE_LIMITED, retry_after_s=3600))

    row = db_conn.execute("SELECT state, available_at FROM jobs WHERE id='j1'").fetchone()
    assert row["state"] == "queued"
    assert row["available_at"] > utc_now_iso()


def test_a_rate_limited_stage_backs_off_and_eventually_fails_the_job(
    dispatcher: Dispatcher, db_conn: sqlite3.Connection
) -> None:
    """A provider stuck at 429 must not defer the same job forever.

    RATE_LIMITED used to requeue without touching ``job_stages.attempts`` at
    all, so ``_MAX_STAGE_ATTEMPTS`` could never be reached down this path -
    and a ``retry_after_s`` of 0 requeued the job as immediately claimable,
    which is a spin, not a retry.
    """
    for attempt in range(1, _MAX_STAGE_ATTEMPTS + 1):
        dispatcher.handle_error(_error("j1", "tts", ErrorKind.RATE_LIMITED, retry_after_s=0.0))
        row = db_conn.execute("SELECT state, available_at FROM jobs WHERE id='j1'").fetchone()
        assert row["state"] == "queued"
        assert row["available_at"] > utc_now_iso(), "a retry must back off, never requeue at zero"
        assert _stage_attempts(db_conn, "j1", "tts") == attempt

    dispatcher.handle_error(_error("j1", "tts", ErrorKind.RATE_LIMITED, retry_after_s=0.0))

    assert _job_state(db_conn, "j1") == "failed"


def test_a_worker_that_dies_without_a_terminal_message_is_not_respawned_forever(
    dispatcher: Dispatcher,
    spawn_spy: SpawnSpy,
    queue: JobQueue,
    db_conn: sqlite3.Connection,
) -> None:
    """The hot loop: a worker that crashes before saying anything.

    ``_pump``'s "no terminal message" branch reset the stage and requeued at
    ``available_in_s=0`` without ever charging an attempt, and
    ``_MAX_STAGE_ATTEMPTS`` was checked only in ``handle_error``'s RETRYABLE
    branch. Measured against a worker that ``os._exit(9)``s inside ``run()``:
    six spawns, the job still ``queued``, ``job_stages.attempts`` still 0 -
    an unbounded fork loop at 100% CPU for any broken provider, OOM kill,
    native segfault or bad venv.

    ``SpawnSpy`` hands back a process that has already exited with empty
    stdout, which is exactly what the pump sees in that case. The backoff is
    fast-forwarded between ticks so the test does not have to sleep through
    it; the assertion is that the spawns are *bounded*, not that they are
    fast.
    """
    queue.requeue("j1", available_in_s=-1)  # undo the fixture's baseline claim
    _mark_stage(db_conn, "j1", "tts", "pending")

    for _ in range(_MAX_STAGE_ATTEMPTS + 3):
        dispatcher.tick()
        if _job_state(db_conn, "j1") == "queued":
            queue.requeue("j1", available_in_s=-1)

    assert spawn_spy.calls == _MAX_STAGE_ATTEMPTS + 1, "the respawn loop must be bounded"
    assert _job_state(db_conn, "j1") == "failed"
    assert _stage_attempts(db_conn, "j1", "fetch") == _MAX_STAGE_ATTEMPTS + 1
    assert db_conn.execute("SELECT last_error FROM jobs WHERE id='j1'").fetchone()["last_error"]


def test_a_worker_that_never_exits_is_killed_by_the_pump_deadline(
    db_conn: sqlite3.Connection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A silent, immortal worker must not hang the dispatcher forever.

    The stderr-to-file fix (this task's other half) does nothing for this
    case: a worker that writes nothing at all to either pipe and never exits
    still blocks ``_pump``'s ``for raw_line in stdout`` forever, since that
    loop has no timeout of its own. Only the watchdog bounds it.

    ``NeverExitingSpawnSpy`` hands back a process backed by a genuine OS pipe
    whose write end nothing closes except its own ``kill()`` - see
    ``_NeverExitingProcess`` for why ``io.StringIO`` (the rest of this file's
    doubles) cannot exercise a blocking read at all.
    """
    spy = NeverExitingSpawnSpy()
    monkeypatch.setattr(dispatcher_module, "Popen", spy)
    dispatcher = _dispatcher(db_conn, tmp_path, pump_deadline_s=0.2)
    _enqueue(db_conn, "j1", stages=("fetch",))

    report = dispatcher.tick()

    assert report.spawned == ("fetch",)
    assert _status(db_conn, "j1", "fetch") == "pending", "a timed-out stage must reset"
    assert _stage_attempts(db_conn, "j1", "fetch") == 1, "a deadline kill must charge an attempt"
    assert "deadline" in _job_last_error(db_conn, "j1")


def test_a_FATAL_error_fails_the_job_without_requeueing(
    dispatcher: Dispatcher, db_conn: sqlite3.Connection
) -> None:
    dispatcher.handle_error(_error("j1", "tts", ErrorKind.FATAL, retry_after_s=None))

    row = db_conn.execute("SELECT state, last_error FROM jobs WHERE id='j1'").fetchone()
    assert row["state"] == "failed"
    assert row["last_error"]


def test_job_completion_releases_every_job_level_retain(
    dispatcher: Dispatcher, store: CasStore
) -> None:
    """After completion the outputs become LRU-evictable, which is the intended
    end state - the cache does not pin them (Task 3)."""
    digest = _complete_a_one_stage_job(dispatcher, store)
    assert store.refcount(digest) == 0
    assert digest in [d for d, _ in store.iter_evictable()]


def test_a_terminally_failed_job_releases_every_pin_its_completed_stages_took(
    dispatcher: Dispatcher, store: CasStore, db_conn: sqlite3.Connection
) -> None:
    """The mirror of the test above, and the case that actually happens.

    ``queue.fail`` is terminal and ``claim()`` matches only ``state =
    'queued'``, so a job that fails at stage 2 can never reach
    ``_maybe_complete_job`` again - stage 1's pin would be held by nothing,
    forever, with no repair path anywhere in the system. That is Phase 1a's
    "the 40 GiB ceiling is decorative" defect arriving through the job-pin
    door, and for a provider pipeline a failed job is the normal case.
    """
    _mark_stage(db_conn, "j1", "fetch", "running")
    data = b"fetch output the failed job will never use"
    digest = store.stage_file(data, kind="blob")
    dispatcher.commit_stage(
        "j1", "fetch", _FETCH_FINGERPRINT, [_staged(digest, size_bytes=len(data))]
    )
    assert store.refcount(digest) == 1, "a committed stage takes the job's in-flight pin"

    dispatcher.handle_error(_error("j1", "tts", ErrorKind.FATAL, retry_after_s=None))

    assert db_conn.execute("SELECT state FROM jobs WHERE id='j1'").fetchone()["state"] == "failed"
    assert store.refcount(digest) == 0, "a terminally failed job must not strand its pins"
    assert digest in [d for d, _ in store.iter_evictable()]


def test_a_stage_pins_what_its_fingerprint_resolves_to_not_what_it_staged(
    dispatcher: Dispatcher,
    store: CasStore,
    artifacts: ArtifactStore,
    db_conn: sqlite3.Connection,
) -> None:
    """C3: the retain and the release must name the same digests.

    ``ArtifactStore.record`` returns False when the fingerprint already has
    rows, so a re-run of a non-deterministic stage produces bytes that are
    NOT what the ``artifacts`` table names. Pinning the staged digests while
    releasing the recorded ones leaves the recorded blob - the one
    ``gather_inputs`` will hand the downstream stage - unpinned and
    evictable, and leaves the staged blob pinned forever.

    Only reachable once the ``Evictor`` has a production caller (it has none
    in Phase 1b, and there is one dispatcher), which is why this pins the
    symmetry rather than the eviction scenario itself.
    """
    _mark_stage(db_conn, "j1", "fetch", "running")
    recorded = store.put_bytes(b"a previous run's fetch output", kind="blob")
    artifacts.record(
        _FETCH_FINGERPRINT, "fetch", [ArtifactRef(name="out", kind="blob", digest=recorded)]
    )
    data = b"this run's fetch output, byte-for-byte different"
    restaged = store.stage_file(data, kind="blob")

    dispatcher.commit_stage(
        "j1", "fetch", _FETCH_FINGERPRINT, [_staged(restaged, size_bytes=len(data))]
    )

    assert store.refcount(recorded) == 1, (
        "the job must pin what its downstream stage will actually be given"
    )
    assert store.refcount(restaged) == 0, "a re-staged blob nothing references must not be pinned"

    dispatcher.handle_error(_error("j1", "tts", ErrorKind.FATAL, retry_after_s=None))

    assert store.refcount(recorded) == 0, "the release must name the digests the retain took"
    assert store.refcount(restaged) == 0


def test_a_worker_that_staged_then_died_leaves_only_reclaimable_orphans(
    dispatcher: Dispatcher, store: CasStore, db_conn: sqlite3.Connection
) -> None:
    """The staged/result split's new failure surface, named in the design's
    risks. A blob file with no row is exactly what sweep_orphans reclaims, and
    the single-transaction commit means no partial rows are ever written."""
    digest = store.stage_file(b"half a stage", kind="blob")
    dispatcher.reap()  # worker died before emitting result

    assert db_conn.execute("SELECT count(*) FROM cas_objects").fetchone()[0] == 0
    assert store.path_for(digest).is_file(), "the file is an orphan, not yet reclaimed"

    report = Evictor(store, EvictionPolicy(max_bytes=1)).sweep_orphans(min_age_s=0)
    assert report.orphan_blobs == 1
    assert not store.path_for(digest).is_file()


# ---------------------------------------------------------------------------
# The job-completion release is a read-then-write (_stage_state and lookup
# SELECT, then release UPDATEs), so it needs the same immediate=True the
# queue's claim() needed and for the same reason - see queue.py's module
# docstring, and tests/unit/app/test_queue.py for the two-connection prior
# art this fixture copies. Every other test in this file uses one connection
# and so cannot exercise the race at all.
# ---------------------------------------------------------------------------


@pytest.fixture()
def two_connections(tmp_path: Path) -> Iterator[tuple[sqlite3.Connection, sqlite3.Connection]]:
    """Two live connections to the same migrated database file.

    The contender's busy_timeout=0 makes it fail fast rather than mask the
    result by waiting.
    """
    db = tmp_path / "two.db"
    conn_a = connect(db)
    apply_migrations(conn_a)
    conn_b = connect(db)
    conn_b.execute("PRAGMA busy_timeout=0")
    try:
        yield conn_a, conn_b
    finally:
        conn_b.close()
        conn_a.close()


def _record_succeeded_stage(
    conn: sqlite3.Connection, job_id: str, stage_id: str, fingerprint: str
) -> None:
    now = utc_now_iso()
    with transaction(conn):
        conn.execute(
            "INSERT INTO job_stages "
            "(job_id, stage_id, status, fingerprint, started_at, finished_at) "
            "VALUES (?, ?, 'succeeded', ?, ?, ?)",
            (job_id, stage_id, fingerprint, now, now),
        )


def test_completing_a_job_survives_a_writer_committing_mid_release(
    two_connections: tuple[sqlite3.Connection, sqlite3.Connection], tmp_path: Path
) -> None:
    """A deferred transaction takes its read snapshot at the first SELECT.

    If another connection commits before the release's UPDATE runs, SQLite
    refuses to upgrade that stale snapshot to a writer -
    SQLITE_BUSY_SNAPSHOT, returned immediately without the busy handler ever
    running, so busy_timeout does not help and the caller sees "database is
    locked". immediate=True takes the write lock at BEGIN, before any read,
    so there is no snapshot to invalidate and the contender simply loses.

    The interleave is forced rather than raced: the contender fires from
    inside the first ``lookup`` the release performs, which is after
    ``_stage_state``'s SELECT and before ``release``'s UPDATE.
    """
    conn_a, conn_b = two_connections
    cas = CasStore(root=tmp_path / "cas-two", conn=conn_a)
    artifacts = ArtifactStore(cas, conn_a)
    queue = JobQueue(conn_a)
    dispatcher = Dispatcher(
        conn_a,
        cas,
        artifacts,
        Governor(),
        queue,
        pipelines={_SOLO_PIPELINE_ID: _solo_pipeline()},
    )

    queue.enqueue("solo", "p1", _SOLO_PIPELINE_ID)
    queue.claim("owner", lease_s=60)
    digest = cas.put_bytes(b"solo output", kind="blob")
    artifacts.record(
        _SOLO_FINGERPRINT, "only", [ArtifactRef(name="out", kind="blob", digest=digest)]
    )
    cas.retain(digest)  # the job's in-flight pin, as commit_stage would have taken it
    _record_succeeded_stage(conn_a, "solo", "only", _SOLO_FINGERPRINT)

    real_lookup = artifacts.lookup
    fired: list[str] = []

    def racing_lookup(fingerprint: str) -> tuple[ArtifactRef, ...] | None:
        found = real_lookup(fingerprint)
        if not fired:
            fired.append(fingerprint)
            with suppress(sqlite3.OperationalError):
                conn_b.execute("BEGIN IMMEDIATE")
                conn_b.execute("UPDATE jobs SET priority = priority + 1 WHERE id = 'solo'")
                conn_b.execute("COMMIT")
        return found

    artifacts.lookup = racing_lookup  # type: ignore[method-assign]

    dispatcher._maybe_complete_job("solo")

    assert fired, "the contender never ran; the interleave did not happen"
    assert _job_state(conn_a, "solo") == "succeeded"
    assert cas.refcount(digest) == 0
