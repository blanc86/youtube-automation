"""Driving one project to two rendered masters: the whole render sequence, once.

``ytauto run`` (``cli/__main__.py``) and the local web UI (``ytauto.ui``) are
two front ends onto exactly the same operation. Everything between "this
project's settings are refreshed" and "the masters are sitting in a folder a
person can open" lives here, so there is one implementation of it rather than
two that drift: build the pipeline, enqueue one job, drain the queue while
respecting retry backoff, and - on success - export the two masters out of
the content-addressed store.

This module was extracted from ``cli/__main__.py``'s ``_run``/``_export_masters``
when the web UI arrived; the reasoning preserved in the docstrings below is
that function's, not new. What deliberately did *not* move is anything
front-end shaped: resolving the output directory (the CLI has ``--output-dir``,
the UI does not), formatting diagnostics, and choosing process exit codes.
``render_project`` returns a ``RenderOutcome`` describing what happened and
lets each front end say it in its own words - which is what keeps ``ytauto
run``'s stderr contract (exit codes, the exact wording an operator greps for)
a property of the CLI rather than something the UI could break by accident.

**One connection per thread.** ``conn`` here is used for transactions
(``JobQueue``, ``Dispatcher``, ``CasStore``), and ``infra.db.engine``'s own
module docstring is explicit that a connection carrying transactions must not
be shared across threads. The UI calls ``render_project`` on a background
thread, so that thread opens - and closes - a connection of its own rather
than borrowing the request thread's.
"""

from __future__ import annotations

import shutil
import sqlite3
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from ytauto.app.registry import build_pipeline
from ytauto.app.scheduler.dispatcher import Dispatcher
from ytauto.app.scheduler.governor import Governor
from ytauto.app.scheduler.queue import JobQueue
from ytauto.core.errors import RenderError
from ytauto.core.models.job import JobState, StageStatus
from ytauto.infra.artifacts import ArtifactStore
from ytauto.infra.cas.store import CasStore

PIPELINE_ID = "story_video"
"""The only pipeline either front end drives. Not exposed as a flag or a
form field: Phase 2a ships exactly one pipeline, and a selector with one
legal value would be dead surface area until a second pipeline exists."""

DEFAULT_MAX_TICKS = 100
"""``tick()`` advances a job by exactly one stage, so a clean run of the
seven-stage ``story_video`` pipeline needs a minimum of seven ticks (one per
stage; ``_maybe_complete_job`` requeues the job immediately after each
non-terminal stage commit, so no idle ticks intervene on the happy path).
100 leaves generous headroom within *one* ``run_until_idle`` call for a
handful of stages that fail once cache-cold before succeeding on their own
first retry - but see ``_WALL_CLOCK_BUDGET_S`` below for the budget that
actually governs surviving a real retry backoff: a tick budget alone cannot,
because a deferred job makes ``tick()`` report idle on the very next call
regardless of how many ticks remain."""

_POLL_INTERVAL_S = 1.0
"""How long ``render_project`` sleeps between polls while the job is merely
deferred (on a ``_retry_stage`` backoff), not finished and not stuck.
``run_until_idle`` stops the instant ``tick()`` reports idle, and a job whose
``jobs.available_at`` was pushed into the future by a RETRYABLE or
RATE_LIMITED failure makes the *next* ``tick()`` report idle immediately -
nowhere near exhausting the tick budget - because nothing is currently
claimable. Called once, a render would report "did not reach a terminal
state" for a job that was in fact healthy and simply waiting out its backoff.
Polling at a fixed, modest interval (rather than parsing ``jobs.available_at``
to sleep the exact remaining backoff) keeps this simple and correct at the
cost of up to one second of added latency per retry - negligible next to a
video render."""

_WALL_CLOCK_BUDGET_S = 600.0
"""How long ``render_project`` will keep waiting through a *continuous
stretch with no progress* before giving up on a deferred-but-not-terminal
job. This is a budget on idle waiting, not on the call's total elapsed time:
it is measured from the most recent tick that actually did something (a
cache-hit skip or a spawn, even one that itself re-deferred via a RETRYABLE/
RATE_LIMITED failure), not from when the render started. A real
``story_video`` run can legitimately spend several minutes doing genuine work
(TTS, B-roll selection, two ffmpeg composes) before it ever needs a retry -
counting that busy time against this budget would report a healthy, merely
slow run as a timeout, which is exactly the bug an earlier version of this
had before ``waiting_since`` was made to reset on every busy tick.

Backoff for one stage tops out at
``_BASE_BACKOFF_S * 2 ** (_MAX_STAGE_ATTEMPTS - 2)`` = 5 * 2**3 = 40s between
its last two attempts (75s total across all four waits before a fifth, final,
terminal attempt) - so 600s of *continuous* silence leaves generous headroom
for one stage to exhaust its own retries without blocking a caller
indefinitely, while never penalising time spent on work that is actually
happening. Distinct from ``max_ticks``, which bounds *ticks spent doing real
work in one ``run_until_idle`` call*; this bounds *wall-clock time spent
waiting with zero progress* - the two failure modes are reported as two
different ``RenderState`` values so a front end can say two different
things."""

MASTER_STAGE_IDS = ("compose_landscape", "compose_vertical")
"""The only two stages whose output is exported. Keyed by stage id, not by
scanning ``artifacts`` for any row whose name ends in ``.mp4`` -
``artifacts`` is keyed by fingerprint and shared across projects by design
(cross-project dedup), so a name-based scan could just as easily return a
different project's masters. Going through THIS job's own
``job_stages.fingerprint`` for exactly these two stage ids is what keeps
exports project-scoped."""


class RenderState(StrEnum):
    """How a render ended. ``SUCCEEDED`` is the only outcome that produced files."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"
    EXPORT_FAILED = "export_failed"
    TICK_BUDGET_EXHAUSTED = "tick_budget_exhausted"
    NO_PROGRESS_TIMEOUT = "no_progress_timeout"


@dataclass(frozen=True)
class RenderOutcome:
    """What one ``render_project`` call did.

    ``export_dir`` is set only for ``SUCCEEDED``: a failed job exports
    nothing, so a partial or absent render never leaves stale files that look
    like output. ``detail`` carries the underlying message for
    ``EXPORT_FAILED`` (which names the offending file and both sizes) and the
    job's last recorded state for the two non-terminal outcomes; it is empty
    otherwise. ``job_id`` is always set - a caller diagnosing a failure from
    the database needs the row to look at.
    """

    job_id: str
    state: RenderState
    export_dir: Path | None = None
    detail: str = ""
    max_ticks: int = DEFAULT_MAX_TICKS
    no_progress_budget_s: float = _WALL_CLOCK_BUDGET_S

    @property
    def ok(self) -> bool:
        return self.state is RenderState.SUCCEEDED


def job_state(conn: sqlite3.Connection, job_id: str) -> str | None:
    """The ``jobs.state`` of one job, or ``None`` if no such row exists."""
    row = conn.execute("SELECT state FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return str(row["state"]) if row is not None else None


def export_masters(
    conn: sqlite3.Connection,
    cas: CasStore,
    artifacts: ArtifactStore,
    *,
    job_id: str,
    slug: str,
    output_dir: Path,
) -> Path:
    """Copy this succeeded job's two rendered masters out of the CAS into
    ``output_dir / slug``, keeping their artifact names. Returns the export
    directory. Existing files of the same name are overwritten - a re-run
    refreshes a project's output rather than accumulating copies.

    ``output_dir`` is the user-facing location resolved by
    ``infra.paths.resolve_output_dir`` (the platform's Videos folder, falling
    back to Downloads) or an explicit override - never ``AppPaths.exports``,
    which lives under application data and is not somewhere a person looks
    for their own files.

    **Every copy is verified before this function returns successfully.**
    ``shutil.copyfile`` can itself return having written a short file - a
    disk that fills up mid-write, or an interrupted network/cloud-synced
    destination, does not reliably surface as a raised exception. So after
    each copy, the destination's size on disk is compared against the source
    blob's own size (``CasStore.path_for(digest).stat().st_size``); anything
    else - missing, unreadable, or a size mismatch - raises ``RenderError``
    naming the file, rather than letting a truncated file be reported as a
    successful export.

    Called identically for a fresh render and for a fully-cached re-run: a
    stage recorded ``skipped`` (cache hit) has exactly as valid a
    ``fingerprint`` as one recorded ``succeeded`` (``StageStatus.is_done`` -
    see ``core.models.job`` - treats them the same), and the blob it names is
    exactly as real. The operator wants the video, not a report that no work
    was done this time.

    Only ``.mp4`` artifacts are copied. Both compose stages also record a
    ``captions.ass`` artifact under the same name; copying it too would let
    the second stage's copy silently clobber the first's, and it is a
    debugging aid, not a deliverable.

    Copies rather than moves or hardlinks: the CAS blob must stay intact and
    refcounted, and the export is a convenience copy the operator may delete
    or move freely without touching the store.

    A stage id with no ``job_stages`` row at all is skipped rather than
    treated as an error: both front ends only ever drive one real pipeline
    (``PIPELINE_ID``, which always contains both compose stages), but a
    handful of unit tests substitute a smaller fake pipeline that has neither
    - a legitimate difference in what pipeline the job ran, not a broken
    invariant.

    Raises:
        AssertionError: a compose stage this job DID run has a ``job_stages``
            row that is not done, or a done row whose fingerprint's artifacts
            are missing from the store. Either would mean the job reported
            ``succeeded`` while an invariant the dispatcher is supposed to
            guarantee - every stage a succeeded job actually ran is done,
            with its artifacts intact - was already broken. Not a
            documented, recoverable failure mode; a bug elsewhere, surfaced
            loudly rather than silently exporting an incomplete set of files.
        RenderError: a copied master failed verification - missing, unreadable,
            or a size mismatch against its source blob. Names the destination
            file and both sizes.
    """
    export_dir = output_dir / slug
    export_dir.mkdir(parents=True, exist_ok=True)
    for stage_id in MASTER_STAGE_IDS:
        row = conn.execute(
            "SELECT status, fingerprint FROM job_stages WHERE job_id = ? AND stage_id = ?",
            (job_id, stage_id),
        ).fetchone()
        if row is None:
            continue
        if row["fingerprint"] is None or not StageStatus(row["status"]).is_done:
            raise AssertionError(
                f"job {job_id} succeeded but stage {stage_id!r} has a job_stages "
                f"row that is not done (status={row['status']!r}) - cannot export"
            )
        found = artifacts.lookup(row["fingerprint"])
        if found is None:
            raise AssertionError(
                f"job {job_id} succeeded but stage {stage_id!r}'s fingerprint "
                f"{row['fingerprint']!r} has no recorded artifacts - cannot export"
            )
        for artifact in found:
            if artifact.name.endswith(".mp4"):
                source = cas.path_for(artifact.digest)
                source_size = source.stat().st_size
                dest = export_dir / artifact.name
                shutil.copyfile(source, dest)
                try:
                    dest_size = dest.stat().st_size
                except OSError as exc:
                    raise RenderError(
                        f"export verification failed for {dest}: could not stat "
                        f"the file just written ({exc})"
                    ) from exc
                if dest_size != source_size:
                    raise RenderError(
                        f"export verification failed for {dest}: wrote "
                        f"{dest_size} bytes but the source is {source_size} "
                        "bytes - the file on disk is truncated or corrupt"
                    )
    return export_dir


def render_project(
    conn: sqlite3.Connection,
    cas: CasStore,
    artifacts: ArtifactStore,
    *,
    project_id: str,
    slug: str,
    settings: Mapping[str, object],
    output_dir: Path,
    max_ticks: int = DEFAULT_MAX_TICKS,
    job_id: str | None = None,
) -> RenderOutcome:
    """Enqueue one job for this project and drain the queue until it ends.

    ``settings`` must already be the mapping ``refresh_run_settings``
    returned - the two derived keys (``story_digest``,
    ``broll_manifest_digest``) go stale on their own, so re-deriving them is
    the caller's first step, and doing it here would hide a failure that
    belongs before the job is enqueued rather than after minutes of
    rendering. Likewise ``output_dir``: an unwritable destination is bad
    environment state, and both front ends resolve and check it up front.

    **Retries need more than one ``run_until_idle`` call.** ``tick()``
    reports idle whenever nothing is *currently* claimable, and a job a
    RETRYABLE or RATE_LIMITED worker failure just deferred
    (``jobs.available_at`` pushed into the future by ``_retry_stage``) is
    exactly that - not claimable *yet*, not stuck. Since ``run_until_idle``
    stops the instant one ``tick()`` reports idle, a single call made right
    after such a failure returns almost immediately, nowhere near
    ``max_ticks``, with the job neither ``succeeded`` nor ``failed``. Calling
    it once and reporting whatever state the job is in afterwards would
    misreport a healthy, still-retrying job as "did not finish"
    indistinguishably from a genuinely stuck one. So this loops: after each
    ``run_until_idle`` call, if the job is not yet terminal *and* the call
    actually went idle (as opposed to running out of ``max_ticks`` while
    genuinely busy - ``report.idle`` tells them apart), it sleeps briefly and
    calls again. The wait is bounded by ``_WALL_CLOCK_BUDGET_S``, but that
    budget measures a *continuous stretch with no progress* (tracked as
    ``waiting_since``, reset whenever a round's
    ``report.skipped``/``report.spawned`` is non-empty - real work happened,
    even if that attempt itself re-deferred), never total elapsed time.
    Anchoring it to start instead was tried and rejected: a real multi-minute
    render doing genuine work would then get charged against the same budget
    as time spent idle-waiting, and could report a false timeout on its first
    retry purely because the busy phase before it ran long.

    **Poison-job policy.** ``Dispatcher.tick()`` can raise ``ValidationError``
    for a job that has nothing to do with this call's own job - one left
    behind by earlier breakage, whose ``project_id`` no longer names a row in
    ``projects``, or whose ``job_stages`` carries a fingerprint
    ``gather_inputs`` cannot parse. A queue can carry more than one job, and
    ``claim()`` takes the highest-priority claimable job, which is not
    necessarily the one just enqueued - so a stale poison job can and does
    intercept a healthy run. The policy is to let ``ValidationError``
    propagate: it is a bug class two separate reviews flagged as fatal, and
    swallowing it here would let the dispatcher grind through a database
    whose invariants are already broken. The job ``tick()`` was mid-claim on
    stays claimed until its lease (300 s) expires and then becomes
    reclaimable, so a poison job surfaces identically on the next attempt -
    the intended, visible signal to go fix the underlying data.

    Args:
        job_id: the id to enqueue under. Generated when omitted; supplied by
            a caller (the UI) that wants to record the id before the render
            starts so a status page can name it while the job is still
            running.

    Returns:
        A ``RenderOutcome``. Only ``RenderState.SUCCEEDED`` carries an
        ``export_dir``.

    Raises:
        ValidationError: the dispatcher hit an unrecoverable inconsistency -
            see the poison-job policy above.
        sqlite3.Error: a query or write fails.
    """
    resolved_job_id = job_id if job_id is not None else uuid.uuid4().hex
    queue = JobQueue(conn)
    pipeline = build_pipeline(PIPELINE_ID, cas, settings)
    queue.enqueue(resolved_job_id, project_id, PIPELINE_ID)
    dispatcher = Dispatcher(
        conn, cas, artifacts, Governor(), queue, pipelines={PIPELINE_ID: pipeline}
    )

    gave_up_waiting_on_backoff = False
    waiting_since: float | None = None
    while True:
        report = dispatcher.run_until_idle(max_ticks=max_ticks)
        state = job_state(conn, resolved_job_id)
        if state in (JobState.SUCCEEDED.value, JobState.FAILED.value):
            break
        if not report.idle:
            # The tick budget ran out while genuinely busy - that is the
            # budget this argument exists to bound; stop now rather than
            # silently retrying past what the caller asked for.
            break
        now = time.monotonic()
        # A tick that actually did something this round - a cache-hit skip,
        # or a spawn (even one that ultimately re-deferred via a RETRYABLE/
        # RATE_LIMITED failure) - is a busy tick, not an idle one, even
        # though run_until_idle's *last* tick this round found nothing
        # claimable and so reported idle=True overall. Real work happened, so
        # the waiting clock restarts from now rather than accumulating
        # against however long this render has been running - see
        # _WALL_CLOCK_BUDGET_S.
        made_progress = bool(report.skipped or report.spawned)
        if made_progress or waiting_since is None:
            waiting_since = now
        elapsed_waiting = now - waiting_since
        if elapsed_waiting >= _WALL_CLOCK_BUDGET_S:
            gave_up_waiting_on_backoff = True
            break
        time.sleep(min(_POLL_INTERVAL_S, _WALL_CLOCK_BUDGET_S - elapsed_waiting))

    final_state = job_state(conn, resolved_job_id)
    if final_state == JobState.SUCCEEDED.value:
        try:
            export_dir = export_masters(
                conn, cas, artifacts, job_id=resolved_job_id, slug=slug, output_dir=output_dir
            )
        except RenderError as exc:
            return RenderOutcome(
                job_id=resolved_job_id,
                state=RenderState.EXPORT_FAILED,
                detail=str(exc),
                max_ticks=max_ticks,
                no_progress_budget_s=_WALL_CLOCK_BUDGET_S,
            )
        return RenderOutcome(
            job_id=resolved_job_id,
            state=RenderState.SUCCEEDED,
            export_dir=export_dir,
            max_ticks=max_ticks,
            no_progress_budget_s=_WALL_CLOCK_BUDGET_S,
        )
    if final_state == JobState.FAILED.value:
        return RenderOutcome(
            job_id=resolved_job_id,
            state=RenderState.FAILED,
            detail=_last_error(conn, resolved_job_id),
            max_ticks=max_ticks,
            no_progress_budget_s=_WALL_CLOCK_BUDGET_S,
        )
    return RenderOutcome(
        job_id=resolved_job_id,
        state=(
            RenderState.NO_PROGRESS_TIMEOUT
            if gave_up_waiting_on_backoff
            else RenderState.TICK_BUDGET_EXHAUSTED
        ),
        detail=repr(final_state),
        max_ticks=max_ticks,
        no_progress_budget_s=_WALL_CLOCK_BUDGET_S,
    )


def _last_error(conn: sqlite3.Connection, job_id: str) -> str:
    """``jobs.last_error`` for a failed job, or an empty string.

    The CLI never showed this (an operator has the log file and the job id);
    the web UI has neither open in front of them, so a failed render needs
    something to say beyond "it failed".
    """
    row = conn.execute("SELECT last_error FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if row is None or row["last_error"] is None:
        return ""
    return str(row["last_error"])
