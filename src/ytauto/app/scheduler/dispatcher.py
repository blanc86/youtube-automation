"""The dispatcher: the only component that writes job state.

Everything earlier in Phase 1b - the queue, the governor, the worker
protocol, the stage runner - exists to be assembled here. ``tick()`` does one
unit of work (claim a job, advance it by one stage: a cache-hit skip or a
worker spawn) so tests can drive it deterministically instead of racing a
background loop. A real dispatch loop is just ``while True: tick()`` with a
sleep on ``idle``; nothing in this module builds that loop, since a test
harness needs to stop it, not run it forever.

**The atomic commit is the point of this module.** On a worker's ``result``
message, ``commit_stage`` does all of this as one transaction, which Task 1's
savepoint re-entrancy is what makes possible:

1. ``CasStore.record_blob`` for each staged digest - the row for a file a
   worker already wrote.
2. ``CasStore.retain`` each one - the **job's** in-flight pin, not the
   cache's. ``ArtifactStore.record`` stopped retaining in Task 3, so this is
   not a duplicate: the cache says "this fingerprint produced these bytes",
   the job says "I am using these bytes right now, do not evict them".
3. ``ArtifactStore.record`` - index the fingerprint. Composes inside this
   transaction because of a fix this task made to ``infra/artifacts.py``: see
   that module's ``record()`` docstring for why its ``immediate=True`` had to
   become conditional on whether a transaction is already open.
4. The ``job_stages`` row flips to ``succeeded``.

Steps 2 and 3 are not duplicates, and the job's retain (step 2) is released
only when the job completes (every stage done) or is reaped (the job's
worker died) - never by the cache, which is what keeps a cached artifact
LRU-evictable once nothing is actively using it.

**Governor lease ownership.** Every ``gpu_compute`` lease this module
acquires is owned by the string ``f"{job_id}:{stage_id}"`` - one stage
attempt, one owner. ``reap()`` reconstructs that same string from
``job_stages`` rows still marked ``running`` under a job whose lease expired,
so it can call ``Governor.release_all`` for a worker that died holding a
lease it can never release itself.

**One connection per thread.** Every object this class touches - ``conn``,
``cas``, ``artifacts``, ``queue`` - must share the exact same
``sqlite3.Connection``, and that connection must never be used by more than
one thread while a transaction may be open on it (see
``infra/db/engine.py``'s module docstring for why: savepoints are a
per-connection LIFO stack that two threads cannot nest independently, and no
lock over the depth counter can fix that). This module is written
single-threaded and synchronous on purpose - ``tick()`` blocks until a
spawned worker's stdout closes - specifically so a caller never has a reason
to reach for a second thread against the same connection.
"""

from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from subprocess import Popen

from ytauto.app.scheduler.governor import Governor
from ytauto.app.scheduler.queue import ClaimedJob, JobQueue
from ytauto.app.scheduler.runner import gather_inputs
from ytauto.app.scheduler.worker_protocol import (
    Error,
    Message,
    Result,
    Staged,
    decode,
)
from ytauto.core.errors import ErrorKind, ValidationError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.content_hash import ContentHash
from ytauto.core.models.job import StageStatus
from ytauto.core.pipeline.graph import Pipeline
from ytauto.core.pipeline.stage import JobContext, Stage
from ytauto.infra.artifacts import ArtifactStore
from ytauto.infra.cas.store import CasStore
from ytauto.infra.clock import utc_now_iso
from ytauto.infra.db.engine import transaction

_DEFAULT_JOB_LEASE_S = 300.0
_DEFAULT_RETRY_AFTER_S = 60.0
_BASE_BACKOFF_S = 5.0
_MAX_BACKOFF_S = 3600.0
_MAX_STAGE_ATTEMPTS = 5
_GPU_POOL = "gpu_compute"


@dataclass(frozen=True)
class StagedArtifact:
    """One produced artifact, ready to commit: a named, sized CAS blob.

    ``commit_stage`` needs both what ``Staged`` carries (kind, digest,
    size_bytes - enough to record the blob row) and what a worker's
    ``Result.artifacts`` carries (a name - what a downstream stage will look
    it up by). The wire protocol keeps those two messages separate (a stage
    can stage a blob before it knows the final ``Result`` shape), so
    ``_resolve_staged_artifacts`` pairs each ``Result`` artifact with the
    ``Staged`` message reporting the same digest to build this. Tests
    construct one directly via the ``_staged`` helper instead of replaying
    that pairing.
    """

    name: str
    kind: str
    digest: ContentHash
    size_bytes: int


@dataclass(frozen=True)
class TickReport:
    """What one ``tick()`` call did.

    ``idle`` is True when there was nothing to do this tick: no claimable
    job, or a claimed job with no stage ready to advance (every remaining
    stage is already running, blocked on a dependency, or done).
    """

    skipped: tuple[str, ...] = ()
    spawned: tuple[str, ...] = ()
    idle: bool = False


def _build_assignment(
    claimed: ClaimedJob, stage: Stage, ctx: JobContext, fingerprint: str, cas_root: str
) -> dict[str, object]:
    """Serialise everything ``app/worker.py`` needs to run one stage.

    ``stage_import`` (``"module:QualName"``, resolved by reflection off the
    stage's own class) is a placeholder for the provider/stage registry
    Phase 2 will build: Phase 1b ships no concrete ``Stage`` implementations
    to register, so the worker imports and zero-arg-constructs the same
    class the dispatcher already holds a reference to. It only works for a
    module-level class with a no-argument constructor - true of every
    synthetic test stage in this phase, not guaranteed once real provider
    parameters exist.
    """
    stage_type = type(stage)
    return {
        "job_id": claimed.job_id,
        "stage_id": stage.id,
        "project_id": claimed.project_id,
        "correlation_id": f"{claimed.job_id}:{stage.id}:{claimed.attempts}",
        "cas_root": cas_root,
        "workdir": str(ctx.workdir),
        "settings": dict(ctx.settings),
        "fingerprint": fingerprint,
        "stage_import": f"{stage_type.__module__}:{stage_type.__qualname__}",
        "inputs": {
            stage_id: [
                {"name": artifact.name, "kind": artifact.kind, "digest": str(artifact.digest)}
                for artifact in artifacts
            ]
            for stage_id, artifacts in ctx.inputs.items()
        },
    }


def _resolve_staged_artifacts(result: Result, staged: Sequence[Staged]) -> list[StagedArtifact]:
    """Pair each named ``Result`` artifact with the ``Staged`` message for its digest.

    A digest ``Result.artifacts`` names but no ``Staged`` message ever
    reported is exactly the failure ``run_stage``'s own verification exists
    to prevent (Task 12) - unreachable from a worker that actually ran
    ``run_stage``, but ``size_bytes`` still needs a value, so a missing match
    falls back to 0 rather than raising and losing the rest of the batch.
    """
    by_digest = {item.digest: item for item in staged}
    resolved: list[StagedArtifact] = []
    for artifact in result.artifacts:
        match = by_digest.get(artifact.digest)
        size_bytes = match.size_bytes if match is not None else 0
        resolved.append(
            StagedArtifact(
                name=artifact.name,
                kind=artifact.kind,
                digest=ContentHash(artifact.digest),
                size_bytes=size_bytes,
            )
        )
    return resolved


class Dispatcher:
    """Claims jobs, plans stages, spawns workers, reaps the dead, commits results.

    ``pipelines`` maps ``jobs.pipeline_id`` to the ``Pipeline`` it runs.
    Nothing in the schema stores a serialised pipeline - a job's DAG is code,
    constructed once at process start and handed in here - so the caller
    that assembles a ``Dispatcher`` is also the one place a pipeline
    registry needs to exist.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        cas: CasStore,
        artifacts: ArtifactStore,
        governor: Governor,
        queue: JobQueue,
        *,
        pipelines: Mapping[str, Pipeline],
        owner: str = "dispatcher",
        job_lease_s: float = _DEFAULT_JOB_LEASE_S,
    ) -> None:
        # Must be the exact connection `cas`/`artifacts`/`queue` were
        # themselves constructed with - the module docstring's "one
        # connection per thread" requirement. transaction()'s re-entrancy is
        # keyed by id(conn), so composing record_blob/retain/record/UPDATE
        # into one atomic step only works if they all execute against the
        # same underlying sqlite3.Connection object.
        self._conn = conn
        self._cas = cas
        self._artifacts = artifacts
        self._governor = governor
        self._queue = queue
        self._pipelines: dict[str, Pipeline] = dict(pipelines)
        self._owner = owner
        self._job_lease_s = job_lease_s
        self._running: dict[str, Popen[str]] = {}

    # -- planning -----------------------------------------------------

    def _pipeline_for(self, pipeline_id: str) -> Pipeline:
        """Raises:
        ValidationError: no pipeline is registered under this id.
        """
        try:
            return self._pipelines[pipeline_id]
        except KeyError:
            raise ValidationError(f"unknown pipeline: {pipeline_id!r}") from None

    def _stage_state(self, job_id: str) -> tuple[dict[str, StageStatus], dict[str, str]]:
        """Current status and recorded fingerprint per stage of ``job_id``."""
        rows = self._conn.execute(
            "SELECT stage_id, status, fingerprint FROM job_stages WHERE job_id = ?",
            (job_id,),
        ).fetchall()
        statuses = {row["stage_id"]: StageStatus(row["status"]) for row in rows}
        fingerprints = {
            row["stage_id"]: row["fingerprint"] for row in rows if row["fingerprint"] is not None
        }
        return statuses, fingerprints

    def _workdir_for(self, job_id: str, stage_id: str) -> Path:
        """Where this stage may write scratch files. Pure - no directory is created."""
        return self._cas.root.parent / "work" / job_id / stage_id

    # -- the loop -------------------------------------------------------

    def tick(self) -> TickReport:
        """Do one unit of work: claim a job, advance it by one stage.

        Claims the highest-priority claimable job, computes its done-set from
        ``job_stages``, and picks the first stage ``Pipeline.ready_stages``
        returns that is not already ``running``. A cache hit (``lookup``
        finds the fingerprint) marks the stage skipped without spawning
        anything - the single probe that delivers crash-resume, cheap
        iteration and cross-project dedup. A miss spawns a worker and blocks
        until it reports a terminal message (see the module docstring for why
        this is deliberately synchronous rather than threaded).

        Raises:
            ValidationError: the claimed job's ``pipeline_id`` names no
                registered pipeline, or an upstream fingerprint recorded in
                ``job_stages`` is malformed (from ``gather_inputs``).
            sqlite3.Error: a claim, read or write fails - includes
                ``sqlite3.OperationalError`` for lock contention and
                ``sqlite3.IntegrityError`` propagated from
                ``ArtifactStore.record`` via ``commit_stage``.
            OSError: a cache miss's worker subprocess cannot be started
                (from ``_spawn``).
        """
        claimed = self._queue.claim(self._owner, lease_s=self._job_lease_s)
        if claimed is None:
            return TickReport(idle=True)

        pipeline = self._pipeline_for(claimed.pipeline_id)
        statuses, fingerprints = self._stage_state(claimed.job_id)
        done = frozenset(sid for sid, status in statuses.items() if status.is_done)
        ready = [
            stage
            for stage in pipeline.ready_stages(done)
            if statuses.get(stage.id) is not StageStatus.RUNNING
        ]
        if not ready:
            return TickReport(idle=True)

        stage = ready[0]
        inputs = gather_inputs(pipeline, stage.id, fingerprints, self._artifacts)
        ctx = JobContext(
            job_id=claimed.job_id,
            project_id=claimed.project_id,
            settings={},
            inputs=inputs,
            workdir=self._workdir_for(claimed.job_id, stage.id),
        )
        fingerprint = stage.fingerprint(ctx)
        cached = self._artifacts.lookup(fingerprint)
        if cached is not None:
            self._mark_skipped(claimed.job_id, stage.id, fingerprint, cached)
            return TickReport(skipped=(stage.id,))

        self._spawn(claimed, stage, ctx, fingerprint)
        return TickReport(spawned=(stage.id,))

    def run_until_idle(self, *, max_ticks: int) -> TickReport:
        """Call ``tick()`` until it reports idle or ``max_ticks`` is reached.

        Returns the union of every tick's ``skipped``/``spawned`` stages, in
        the order they happened. ``idle`` on the result reflects only the
        LAST tick: True means the loop stopped because there was nothing left
        to do, False means it stopped because ``max_ticks`` ran out while
        work potentially remained.

        Raises:
            ValidationError, sqlite3.Error, OSError: propagated from
                ``tick()`` - see its own ``Raises:`` section.
        """
        skipped: list[str] = []
        spawned: list[str] = []
        idle = False
        for _ in range(max_ticks):
            report = self.tick()
            skipped.extend(report.skipped)
            spawned.extend(report.spawned)
            idle = report.idle
            if idle:
                break
        return TickReport(skipped=tuple(skipped), spawned=tuple(spawned), idle=idle)

    # -- committing a stage ----------------------------------------------

    def _mark_skipped(
        self,
        job_id: str,
        stage_id: str,
        fingerprint: str,
        artifacts: tuple[ArtifactRef, ...],
    ) -> None:
        """Record a cache hit: retain the job's pin on every artifact it will
        consume, and mark the stage done without ever spawning a worker.

        An upsert, not a bare update: a stage that has never been attempted
        before has no ``job_stages`` row yet.
        """
        now = utc_now_iso()
        with transaction(self._conn):
            for artifact in artifacts:
                self._cas.retain(artifact.digest)
            self._conn.execute(
                "INSERT INTO job_stages "
                "(job_id, stage_id, status, fingerprint, started_at, finished_at) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(job_id, stage_id) DO UPDATE SET "
                "status = excluded.status, fingerprint = excluded.fingerprint, "
                "started_at = excluded.started_at, finished_at = excluded.finished_at",
                (job_id, stage_id, StageStatus.SKIPPED.value, fingerprint, now, now),
            )
        self._maybe_complete_job(job_id)

    def _mark_stage_succeeded(self, job_id: str, stage_id: str, fingerprint: str) -> None:
        """The fourth step of the atomic commit. A bare update: by the time a
        stage can succeed, spawning it already created its ``job_stages`` row."""
        self._conn.execute(
            "UPDATE job_stages SET status = ?, fingerprint = ?, finished_at = ? "
            "WHERE job_id = ? AND stage_id = ?",
            (StageStatus.SUCCEEDED.value, fingerprint, utc_now_iso(), job_id, stage_id),
        )

    def commit_stage(
        self, job_id: str, stage_id: str, fingerprint: str, staged: Sequence[StagedArtifact]
    ) -> None:
        """Record a stage's output as one atomic transaction.

        ``record_blob``, ``retain``, ``ArtifactStore.record`` and the
        ``job_stages`` update land together or not at all - see the module
        docstring for the full rationale and Step 5 of the task report for
        the guard-pinning proof (remove the ``transaction()`` wrapper and
        watch ``cas_objects`` rows survive a failure at the last step).

        Raises:
            ValidationError: a digest is malformed, a staged file is missing
                from the CAS, two artifacts in ``staged`` share a name, or
                (propagated from ``ArtifactStore.record``) some other
                pre-flight check fails.
            sqlite3.OperationalError: the write lock could not be acquired
                within ``busy_timeout``, or any statement inside the
                transaction fails for another reason - either way, the whole
                transaction rolls back.
            sqlite3.IntegrityError: propagated from ``ArtifactStore.record``
                if an insert violates a constraint other than the
                primary-key collision it already handles internally - today
                unreachable, since the primary key is that table's only
                constraint.
        """
        with transaction(self._conn, immediate=True):
            for item in staged:
                self._cas.record_blob(item.digest, kind=item.kind, size_bytes=item.size_bytes)
                self._cas.retain(item.digest)
            if staged:
                artifact_refs = [
                    ArtifactRef(name=item.name, kind=item.kind, digest=item.digest)
                    for item in staged
                ]
                self._artifacts.record(fingerprint, stage_id, artifact_refs)
            self._mark_stage_succeeded(job_id, stage_id, fingerprint)

        owner = f"{job_id}:{stage_id}"
        self._governor.release_all(owner)
        self._running.pop(owner, None)
        self._maybe_complete_job(job_id)

    def _maybe_complete_job(self, job_id: str) -> None:
        """Mark the job succeeded and release every job-level retain once
        every one of its stages is done.

        Reads the job's own pipeline to know the full stage set - a job with
        a subset of its stages done is not finished, and a done-set that
        somehow contained an id outside the pipeline (should never happen)
        must not be mistaken for completion either, hence exact equality
        rather than a subset check.
        """
        row = self._conn.execute("SELECT pipeline_id FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if row is None:
            return
        pipeline = self._pipeline_for(row["pipeline_id"])
        statuses, fingerprints = self._stage_state(job_id)
        all_ids = {stage.id for stage in pipeline.stages}
        done_ids = {sid for sid, status in statuses.items() if status.is_done}
        if done_ids != all_ids:
            return

        with transaction(self._conn):
            for fingerprint in fingerprints.values():
                artifacts = self._artifacts.lookup(fingerprint)
                if artifacts is None:
                    continue
                for artifact in artifacts:
                    self._cas.release(artifact.digest)
            self._queue.complete(job_id)

    # -- reaping the dead --------------------------------------------------

    def _reset_stage(self, job_id: str, stage_id: str) -> None:
        self._conn.execute(
            "UPDATE job_stages SET status = ?, started_at = NULL WHERE job_id = ? AND stage_id = ?",
            (StageStatus.PENDING.value, job_id, stage_id),
        )

    def reap(self) -> None:
        """Return every job whose lease expired to the queue at its last
        completed stage, releasing what a dead worker held.

        For each job ``JobQueue.reap_expired`` returns, every one of its
        stages still marked ``running`` is reset to ``pending`` (completed
        stages are untouched - the whole point of crash-resume) and its
        governor lease is released via ``Governor.release_all`` keyed by
        ``f"{job_id}:{stage_id}"`` - the one convention ``_spawn`` also uses,
        so a lease acquired before this dispatcher instance even existed
        (a worker outliving a dispatcher restart) is still found and freed.

        Does not touch the filesystem: a worker that staged a blob and died
        before reporting it leaves an orphan file with no row, which is
        ``Evictor.sweep_orphans``'s job, not this one's - see this task's
        report for why that split is deliberate.

        Raises:
            sqlite3.Error: a query or update fails.
        """
        for job_id in self._queue.reap_expired():
            rows = self._conn.execute(
                "SELECT stage_id FROM job_stages WHERE job_id = ? AND status = ?",
                (job_id, StageStatus.RUNNING.value),
            ).fetchall()
            for row in rows:
                stage_id = row["stage_id"]
                owner = f"{job_id}:{stage_id}"
                self._governor.release_all(owner)
                self._running.pop(owner, None)
                with transaction(self._conn):
                    self._reset_stage(job_id, stage_id)

    # -- error handling ----------------------------------------------------

    def _bump_stage_attempts(self, job_id: str, stage_id: str) -> int:
        with transaction(self._conn):
            self._conn.execute(
                "UPDATE job_stages SET attempts = attempts + 1 WHERE job_id = ? AND stage_id = ?",
                (job_id, stage_id),
            )
            row = self._conn.execute(
                "SELECT attempts FROM job_stages WHERE job_id = ? AND stage_id = ?",
                (job_id, stage_id),
            ).fetchone()
        return int(row["attempts"]) if row is not None else 1

    def handle_error(self, error: Error) -> None:
        """Route a worker's ``error`` message to the right job-level outcome.

        ``RATE_LIMITED`` defers the job by exactly ``error.retry_after_s`` -
        the reason ``jobs.available_at`` exists at all (Migration 003):
        without it, a rate limit could not be honoured, only ignored.
        ``RETRYABLE`` backs off exponentially on the stage's own attempt
        count and fails the job once ``_MAX_STAGE_ATTEMPTS`` is exceeded, so
        one poison stage cannot spin forever. ``FATAL`` and
        ``QUOTA_EXCEEDED`` fail the job immediately - retrying either only
        delays a failure an operator needs to see, and retrying spent quota
        cannot succeed.

        Raises:
            sqlite3.Error: a query or update fails.
        """
        job_id, stage_id = error.job_id, error.stage_id
        owner = f"{job_id}:{stage_id}"
        self._governor.release_all(owner)
        self._running.pop(owner, None)
        with transaction(self._conn):
            self._reset_stage(job_id, stage_id)

        if error.kind is ErrorKind.RATE_LIMITED:
            delay = (
                error.retry_after_s if error.retry_after_s is not None else _DEFAULT_RETRY_AFTER_S
            )
            self._queue.requeue(job_id, available_in_s=delay, error=error.message)
        elif error.kind is ErrorKind.RETRYABLE:
            attempts = self._bump_stage_attempts(job_id, stage_id)
            if attempts > _MAX_STAGE_ATTEMPTS:
                self._queue.fail(job_id, error.message)
            else:
                delay = min(_MAX_BACKOFF_S, _BASE_BACKOFF_S * (2 ** max(0, attempts - 1)))
                self._queue.requeue(job_id, available_in_s=delay, error=error.message)
        else:  # FATAL, QUOTA_EXCEEDED
            self._queue.fail(job_id, error.message)

    # -- spawning and pumping a worker --------------------------------------

    def _spawn(self, claimed: ClaimedJob, stage: Stage, ctx: JobContext, fingerprint: str) -> None:
        """Acquire a ``gpu_compute`` lease, spawn a worker, and block until it
        reports a terminal message.

        Only ``gpu_compute`` exists to lease against in this phase (§3.5 of
        the design defers ``gpu_encode``/``cpu_heavy``/``net_api`` until
        something needs them), so every spawn takes it - conservative for a
        stage that does not actually need the GPU, but correct: nothing in
        the current ``Stage`` protocol carries a capability descriptor to
        decide otherwise, and a real ``requires_gpu`` gate is Phase 2's job
        once providers exist. A refused lease defers to a later tick rather
        than blocking.

        Raises:
            OSError: the worker subprocess cannot be started.
        """
        owner = f"{claimed.job_id}:{stage.id}"
        lease = self._governor.lease(_GPU_POOL, owner)
        granted = lease.__enter__()
        if not granted:
            lease.__exit__(None, None, None)
            return

        started = False
        try:
            ctx.workdir.mkdir(parents=True, exist_ok=True)
            now = utc_now_iso()
            with transaction(self._conn):
                self._conn.execute(
                    "INSERT INTO job_stages (job_id, stage_id, status, started_at) "
                    "VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(job_id, stage_id) DO UPDATE SET "
                    "status = excluded.status, started_at = excluded.started_at",
                    (claimed.job_id, stage.id, StageStatus.RUNNING.value, now),
                )

            assignment = _build_assignment(claimed, stage, ctx, fingerprint, str(self._cas.root))
            proc = Popen(
                [sys.executable, "-m", "ytauto.app.worker"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self._running[owner] = proc
            started = True
        finally:
            # A failure between acquiring the lease and successfully tracking
            # the process (mkdir, the DB write, or Popen itself) must not
            # leak the lease - nothing else would ever release it, since
            # _running never got an entry for reap() to find later.
            if not started:
                self._governor.release_all(owner)

        if proc.stdin is not None:
            proc.stdin.write(json.dumps(assignment))
            proc.stdin.close()

        self._pump(claimed.job_id, stage.id, fingerprint, proc, owner)

    def _pump(
        self, job_id: str, stage_id: str, fingerprint: str, proc: Popen[str], owner: str
    ) -> None:
        """Read one worker's stdout until a terminal message, then settle it.

        Reading blocks until the process closes stdout - see the module
        docstring for why this dispatcher is deliberately synchronous. A
        malformed known-message-type line (``decode`` raising
        ``ValidationError``) is treated as a fatal stage error rather than
        left to crash the whole dispatcher: one worker's bug or corrupted
        pipe should not take every other job down with it. An unknown
        type/version (``decode`` returning ``None``) is silently skipped -
        version skew, not a bug.

        Raises:
            Nothing itself. Exceptions from ``commit_stage``/``handle_error``
            propagate.
        """
        staged: list[Staged] = []
        result: Result | None = None
        error: Error | None = None
        try:
            stdout = proc.stdout
            if stdout is not None:
                for raw_line in stdout:
                    line = raw_line.strip()
                    if not line:
                        continue
                    try:
                        message: Message | None = decode(line)
                    except ValidationError as exc:
                        error = Error(
                            job_id=job_id,
                            stage_id=stage_id,
                            correlation_id="-",
                            message=f"malformed worker output: {exc}",
                            kind=ErrorKind.FATAL,
                        )
                        break
                    if message is None:
                        continue
                    if isinstance(message, Staged):
                        staged.append(message)
                    elif isinstance(message, Result):
                        result = message
                        break
                    elif isinstance(message, Error):
                        error = message
                        break
                    # Progress and LogLine are advisory only; nothing to commit.
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        finally:
            self._governor.release_all(owner)
            self._running.pop(owner, None)

        if result is not None:
            self.commit_stage(
                job_id, stage_id, fingerprint, _resolve_staged_artifacts(result, staged)
            )
        elif error is not None:
            self.handle_error(error)
        else:
            # stdout closed with no terminal message: the worker died without
            # reporting anything. Reset now rather than waiting for the job
            # lease to expire and reap() to notice.
            with transaction(self._conn):
                self._reset_stage(job_id, stage_id)
            self._queue.requeue(job_id)
