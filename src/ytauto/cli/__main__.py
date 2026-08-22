"""Command-line entry point."""

from __future__ import annotations

import argparse
import contextlib
import shutil
import sqlite3
import sys
import time
import uuid
from pathlib import Path

from ytauto import __version__
from ytauto.app.registry import build_pipeline
from ytauto.app.scheduler.dispatcher import Dispatcher
from ytauto.app.scheduler.governor import Governor
from ytauto.app.scheduler.queue import JobQueue
from ytauto.app.services.enqueue import create_project, refresh_run_settings, resolve_project_id
from ytauto.cli.doctor import exit_code, format_report, run_checks
from ytauto.core.errors import ConfigurationError, RenderError, ValidationError
from ytauto.core.models.job import JobState, StageStatus
from ytauto.infra.artifacts import ArtifactStore
from ytauto.infra.broll import BrollLibrary
from ytauto.infra.cas.store import CasStore
from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import apply_migrations
from ytauto.infra.logging import bind_correlation_id, configure_logging
from ytauto.infra.paths import AppPaths, ensure_writable_dir, resolve_output_dir

_PIPELINE_ID = "story_video"
"""The only pipeline this CLI drives. Not exposed as a flag: Phase 2a ships
exactly one pipeline, and a --pipeline flag with one legal value would be
dead surface area until a second pipeline actually exists."""

_DEFAULT_MAX_TICKS = 100
"""``tick()`` advances a job by exactly one stage, so a clean run of the
seven-stage ``story_video`` pipeline needs a minimum of seven ticks (one per
stage; ``_maybe_complete_job`` requeues the job immediately after each
non-terminal stage commit, so no idle ticks intervene on the happy path).
100 leaves generous headroom within *one* ``run_until_idle`` call for a
handful of stages that fail once cache-cold before succeeding on their own
first retry - but see ``_RUN_WALL_CLOCK_BUDGET_S`` below for the budget that
actually governs surviving a real retry backoff: ``--max-ticks`` alone
cannot, because a deferred job makes ``tick()`` report idle on the very next
call regardless of how many ticks remain."""

_RUN_POLL_INTERVAL_S = 1.0
"""How long ``_run`` sleeps between polls while the job is merely deferred
(on a ``_retry_stage`` backoff), not finished and not stuck. Review finding
(Important #3): ``run_until_idle`` stops the instant ``tick()`` reports
idle, and a job whose ``jobs.available_at`` was pushed into the future by a
RETRYABLE or RATE_LIMITED failure makes the *next* ``tick()`` report idle
immediately - nowhere near exhausting ``--max-ticks`` - because nothing is
currently claimable. Called once, ``ytauto run`` would report "did not reach
a terminal state" for a job that was in fact healthy and simply waiting out
its backoff. Polling at a fixed, modest interval (rather than parsing
``jobs.available_at`` to sleep the exact remaining backoff) keeps this
simple and correct at the cost of up to one second of added latency per
retry - negligible next to a video render."""

_RUN_WALL_CLOCK_BUDGET_S = 600.0
"""How long ``_run`` will keep waiting through a *continuous stretch with no
progress* before giving up on a deferred-but-not-terminal job. This is a
budget on idle waiting, not on the invocation's total elapsed time: it is
measured from the most recent tick that actually did something (a cache-hit
skip or a spawn, even one that itself re-deferred via a RETRYABLE/
RATE_LIMITED failure), not from when ``_run`` started. A real
``story_video`` run can legitimately spend several minutes doing genuine
work (TTS, B-roll selection, two ffmpeg composes) before it ever needs a
retry - counting that busy time against this budget would report a healthy,
merely slow run as a timeout, which is exactly the bug an earlier version of
this fix had (review round 3) before ``waiting_since`` was made to reset on
every busy tick.

Backoff for one stage tops out at
``_BASE_BACKOFF_S * 2 ** (_MAX_STAGE_ATTEMPTS - 2)`` = 5 * 2**3 = 40s between
its last two attempts (75s total across all four waits before a fifth,
final, terminal attempt) - so 600s of *continuous* silence leaves generous
headroom for one stage to exhaust its own retries without ``ytauto run``
blocking an operator's terminal indefinitely, while never penalising time
spent on work that is actually happening. Distinct from ``--max-ticks``,
which bounds *ticks spent doing real work in one ``run_until_idle`` call*;
this bounds *wall-clock time spent waiting with zero progress* - the two
failure modes get two different diagnostic messages (see ``_run``)."""


def _add_broll_subcommand(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    broll = subparsers.add_parser("broll", help="manage the B-roll library")
    broll_subparsers = broll.add_subparsers(dest="broll_command", required=True)

    add = broll_subparsers.add_parser(
        "add", help="ingest a source clip: probe, normalise to both canvases, record provenance"
    )
    add.add_argument("path", type=Path, help="path to the source video file")
    # --source-url and --licence are required, not optional: the provenance
    # record is the point of this command, and an optional licence would be
    # blank on every clip within a week.
    add.add_argument("--source-url", required=True, help="where the clip came from")
    add.add_argument("--licence", required=True, help="the clip's licence")
    add.add_argument("--attribution", default="", help="attribution text, if the licence needs one")
    add.add_argument("--notes", default="", help="free-form notes")


def _broll_add(paths: AppPaths, args: argparse.Namespace) -> int:
    """Ingest one clip and rewrite the manifest. Returns the process exit code.

    The manifest is rewritten after every successful add - Task 10's clip
    selection and the compose stages both read it as a single CAS blob, so it
    must never describe a library older than the row that was just committed.
    """
    paths.ensure()
    conn = connect(paths.db_file)
    try:
        apply_migrations(conn)
        cas = CasStore(root=paths.cas, conn=conn)
        library = BrollLibrary(conn, cas)
        clip_id = library.add(
            args.path,
            source_url=args.source_url,
            licence=args.licence,
            attribution=args.attribution,
            notes=args.notes,
        )
        library.write_manifest()
    finally:
        conn.close()
    print(f"added B-roll clip {clip_id}")
    return 0


def _add_project_subcommand(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    project = subparsers.add_parser("project", help="manage projects")
    project_subparsers = project.add_subparsers(dest="project_command", required=True)

    create = project_subparsers.add_parser("create", help="create a project from a story text file")
    create.add_argument("--slug", required=True, help="url-safe, unique project identifier")
    create.add_argument("--title", required=True, help="human-readable project title")
    create.add_argument("--story", required=True, type=Path, help="path to the story text file")


def _project_create(paths: AppPaths, args: argparse.Namespace) -> int:
    """Create a project from a story file. Returns the process exit code.

    All the interesting behaviour - hashing the story's normalised text,
    staging it into the CAS, writing the human-editable copy to the project's
    own directory, and recording both the digest and the on-disk path in
    settings - lives in ``app.services.enqueue.create_project``; see its
    docstring.

    ``create_project``'s own docstring documents ``OSError`` and
    ``UnicodeDecodeError`` alongside ``ValidationError`` - a non-UTF-8 story
    file (an ordinary Windows-1252 save is entirely plausible) or an
    unreadable/unwritable path are both bad input in exactly the same sense
    a missing story file is, and all three must report the documented exit-2
    contract rather than letting an undocumented exception type slip past
    this ``except`` clause and crash out of ``main()`` as a raw traceback.
    """
    paths.ensure()
    conn = connect(paths.db_file)
    try:
        apply_migrations(conn)
        cas = CasStore(root=paths.cas, conn=conn)
        project_dir = paths.projects / args.slug
        try:
            project_id = create_project(
                conn,
                cas,
                project_dir,
                slug=args.slug,
                title=args.title,
                story_path=args.story,
            )
        except (ValidationError, OSError, UnicodeDecodeError) as exc:
            print(f"ytauto project create: {exc}", file=sys.stderr)
            return 2
    finally:
        conn.close()
    print(f"created project {args.slug!r} ({project_id})")
    return 0


def _add_run_subcommand(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    run = subparsers.add_parser("run", help="enqueue and drain one job for a project")
    run.add_argument("--project", dest="slug", required=True, help="project slug to run")
    run.add_argument(
        "--max-ticks",
        type=int,
        default=_DEFAULT_MAX_TICKS,
        help=(
            "stop after this many dispatcher ticks in any one poll round - a "
            "per-round budget, not a cumulative limit on the whole invocation "
            f"(default: {_DEFAULT_MAX_TICKS})"
        ),
    )
    run.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help=(
            "write rendered masters here instead of the auto-detected Videos "
            "(falling back to Downloads) folder"
        ),
    )


def _job_state(conn: sqlite3.Connection, job_id: str) -> str | None:
    row = conn.execute("SELECT state FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return str(row["state"]) if row is not None else None


_MASTER_STAGE_IDS = ("compose_landscape", "compose_vertical")
"""The only two stages whose output is exported. Keyed by stage id, not by
scanning ``artifacts`` for any row whose name ends in ``.mp4`` -
``artifacts`` is keyed by fingerprint and shared across projects by design
(cross-project dedup), so a name-based scan could just as easily return a
different project's masters. Going through THIS job's own
``job_stages.fingerprint`` for exactly these two stage ids is what keeps
exports project-scoped."""


def _export_masters(
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
    back to Downloads) or an explicit ``--output-dir`` - never
    ``AppPaths.exports``, which lives under application data and is not
    somewhere a person looks for their own files.

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
    treated as an error: ``ytauto run`` only ever drives one real pipeline
    (``_PIPELINE_ID``, which always contains both compose stages), but a
    handful of this module's own unit tests substitute a smaller fake
    pipeline that has neither - a legitimate difference in what pipeline the
    job ran, not a broken invariant.

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
    for stage_id in _MASTER_STAGE_IDS:
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


def _run(paths: AppPaths, args: argparse.Namespace) -> int:
    """Enqueue one job against ``--project`` and drain the queue. Returns the
    process exit code.

    **Two settings are re-derived before anything is enqueued.**
    ``refresh_run_settings`` (see its own docstring for the full account)
    recomputes ``story_digest`` from whatever is on disk at
    ``settings["story_path"]`` right now, and rewrites the B-roll manifest to
    bind ``broll_manifest_digest`` to the library's current state. Both are
    derived values that go stale on their own: a story is edited in place by
    design, and the B-roll library is global and mutable. Doing it here, in
    the one place that turns a project into a job, is what makes an edited
    story invalidate the cache instead of being silently ignored - and what
    means a project created by ``ytauto project create`` is runnable with no
    further configuration, which it was not before (there is still no
    ``ytauto project set-setting`` verb).

    **The output location is resolved before anything is enqueued.**
    ``infra.paths.resolve_output_dir`` picks the user's platform Videos
    folder (falling back to Downloads if that cannot be created or proven
    writable), or ``--output-dir`` overrides it explicitly - either way this
    happens before the job is enqueued, so a broken output location fails
    fast rather than after minutes of rendering.

    **On success, the two rendered masters are exported.** ``_export_masters``
    copies them out of the content-addressed store - where they live as
    hash-named blobs nobody could find on their own - into
    ``<output_dir> / <slug>``, verifies each copy actually landed at its full
    size, and the export directory is the last thing this command prints.
    This runs identically whether the job just rendered for real or the run
    was a fully-cached no-op (every stage a cache hit): either way the
    operator wants the video files, not a report that nothing happened. A
    failed job exports nothing, so a partial or absent render never leaves
    stale files that look like output. A verification failure (a truncated or
    missing copy) is reported as a command failure naming the file, never as
    success.

    Exit codes: 0 once the enqueued job reaches ``succeeded``; 2 if
    ``--project`` names no project, the project's settings are missing or
    malformed, or the output location (auto-detected or ``--output-dir``)
    could not be created or proven writable - all bad input or bad
    environment, checked before anything is enqueued; 1 for every other way
    this can fail to end in success - the job reaches ``failed``, it burns
    through ``--max-ticks`` while genuinely busy, it is still waiting out a
    retry backoff once ``_RUN_WALL_CLOCK_BUDGET_S`` elapses, the dispatcher
    itself raises, or the successful job's masters fail export verification.
    The last three are reported with different stderr messages (see below) -
    sharing one exit code is fine within this brief's ``{0,1,2}`` contract,
    but "ran out of tick budget", "made no progress for N seconds" and "the
    render actually failed" send an operator to different places and must
    not read the same.

    **Retries need more than one ``run_until_idle`` call.** ``tick()``
    reports idle whenever nothing is *currently* claimable, and a job a
    RETRYABLE or RATE_LIMITED worker failure just deferred
    (``jobs.available_at`` pushed into the future by ``_retry_stage``) is
    exactly that - not claimable *yet*, not stuck. Since ``run_until_idle``
    stops the instant one ``tick()`` reports idle, a single call made right
    after such a failure returns almost immediately, nowhere near
    ``--max-ticks``, with the job neither ``succeeded`` nor ``failed`` (review
    finding, Important #3). Calling it once and reporting whatever state the
    job is in afterwards would misreport a healthy, still-retrying job as
    "did not finish" indistinguishably from a genuinely stuck one. So this
    loops: after each ``run_until_idle`` call, if the job is not yet terminal
    *and* the call actually went idle (as opposed to running out of
    ``--max-ticks`` while genuinely busy - ``report.idle`` tells them apart),
    it sleeps briefly and calls again. The wait is bounded by
    ``_RUN_WALL_CLOCK_BUDGET_S``, but that budget measures a *continuous
    stretch with no progress* (tracked as ``waiting_since``, reset whenever a
    round's ``report.skipped``/``report.spawned`` is non-empty - real work
    happened, even if that attempt itself re-deferred), never the
    invocation's total elapsed time. Anchoring it to invocation start instead
    was tried and rejected (review round 3): a real multi-minute render doing
    genuine work would then get charged against the same budget as time spent
    idle-waiting, and could report a false timeout on its first retry purely
    because the busy phase before it ran long. This stays entirely inside the
    CLI - no change to ``Dispatcher``/``tick()`` was needed or made.

    **Poison-job policy.** ``Dispatcher.tick()`` can raise ``ValidationError``
    for a job that has nothing to do with this invocation's own job - one
    left behind by earlier breakage, whose ``project_id`` no longer names a
    row in ``projects``, or whose ``job_stages`` carries a fingerprint
    ``gather_inputs`` cannot parse (see ``tick()``'s own ``Raises:`` section).
    Both paths were already latent defects (Task 3's review found the first,
    Task 11's the second); ``ytauto run`` is the first thing that ever drains
    a real queue; a queue can carry more than one job; and ``claim()`` takes
    the highest-priority claimable job, which is not necessarily the one this
    invocation just enqueued - so a stale poison job can and does intercept a
    healthy run.

    The policy here is to let it propagate out of ``run_until_idle`` on
    whichever call raises it - the retry loop above calls it more than once,
    but never catches ``ValidationError`` around any individual call, so this
    is unchanged from a single-call design in the one way that matters: no
    iteration of the loop ever intervenes to fail just the offending job and
    keep going. It is reported as a clear, non-zero failure naming the
    offending row, rather than swallowing it and letting the dispatcher grind
    through a database whose invariants a bug has already broken. The
    alternative - catching it per-tick inside the dispatcher and failing just
    that one job before continuing - was considered and rejected: it would
    need a change to ``app/scheduler/dispatcher.py``, which is out of this
    task's scope, and it would turn a bug class two separate reviews flagged
    as fatal into something this command quietly tolerates. The job
    ``tick()`` was mid-claim on when this happens stays claimed until its
    lease (300 s) expires and then becomes reclaimable again; a poison job
    left in that state will surface identically on the next invocation, which
    is the intended, visible signal to go fix the underlying data rather than
    a crash this command should paper over.
    """
    paths.ensure()
    conn = connect(paths.db_file)
    try:
        apply_migrations(conn)
        cas = CasStore(root=paths.cas, conn=conn)
        artifacts = ArtifactStore(cas, conn)
        queue = JobQueue(conn)

        try:
            project_id = resolve_project_id(conn, args.slug)
            settings = refresh_run_settings(conn, cas, project_id)
        except (ValidationError, OSError, UnicodeDecodeError) as exc:
            print(f"ytauto run: {exc}", file=sys.stderr)
            return 2

        # Resolved before enqueueing anything: an unwritable output location
        # is bad environment state exactly like an invalid project setting,
        # and there is no point spending minutes rendering a video with
        # nowhere to put it.
        if args.output_dir is not None:
            output_dir = Path(args.output_dir).expanduser().resolve()
            if not ensure_writable_dir(output_dir):
                print(
                    f"ytauto run: --output-dir {output_dir} could not be created "
                    "or is not writable",
                    file=sys.stderr,
                )
                return 2
        else:
            try:
                output_dir = resolve_output_dir()
            except ConfigurationError as exc:
                print(f"ytauto run: {exc}", file=sys.stderr)
                return 2

        job_id = uuid.uuid4().hex
        gave_up_waiting_on_backoff = False
        try:
            pipeline = build_pipeline(_PIPELINE_ID, cas, settings)
            queue.enqueue(job_id, project_id, _PIPELINE_ID)
            dispatcher = Dispatcher(
                conn, cas, artifacts, Governor(), queue, pipelines={_PIPELINE_ID: pipeline}
            )
            waiting_since: float | None = None
            while True:
                report = dispatcher.run_until_idle(max_ticks=args.max_ticks)
                state = _job_state(conn, job_id)
                if state in (JobState.SUCCEEDED.value, JobState.FAILED.value):
                    break
                if not report.idle:
                    # --max-ticks ran out while genuinely busy - that is the
                    # budget this flag exists to bound; stop now rather than
                    # silently retrying past what the operator asked for.
                    break
                now = time.monotonic()
                # A tick that actually did something this round - a cache-hit
                # skip, or a spawn (even one that ultimately re-deferred via a
                # RETRYABLE/RATE_LIMITED failure) - is a busy tick, not an idle
                # one, even though run_until_idle's *last* tick this round
                # found nothing claimable and so reported idle=True overall.
                # Real work happened, so the waiting clock restarts from now
                # rather than accumulating against however long this whole
                # invocation has been running - see _RUN_WALL_CLOCK_BUDGET_S.
                made_progress = bool(report.skipped or report.spawned)
                if made_progress or waiting_since is None:
                    waiting_since = now
                elapsed_waiting = now - waiting_since
                if elapsed_waiting >= _RUN_WALL_CLOCK_BUDGET_S:
                    gave_up_waiting_on_backoff = True
                    break
                time.sleep(min(_RUN_POLL_INTERVAL_S, _RUN_WALL_CLOCK_BUDGET_S - elapsed_waiting))
        except ValidationError as exc:
            print(
                f"ytauto run: the queue stopped on an unrecoverable error: {exc}",
                file=sys.stderr,
            )
            return 1

        state = _job_state(conn, job_id)
        if state == JobState.SUCCEEDED.value:
            try:
                export_dir = _export_masters(
                    conn, cas, artifacts, job_id=job_id, slug=args.slug, output_dir=output_dir
                )
            except RenderError as exc:
                print(f"ytauto run: {exc}", file=sys.stderr)
                return 1
            print(f"ytauto run: job {job_id} succeeded; masters written to {export_dir}")
            return 0
        if state == JobState.FAILED.value:
            print(f"ytauto run: job {job_id} failed", file=sys.stderr)
            return 1
        if gave_up_waiting_on_backoff:
            print(
                f"ytauto run: job {job_id} made no progress for "
                f"{_RUN_WALL_CLOCK_BUDGET_S:.0f}s and this invocation gave up "
                f"waiting (currently {state!r}) - this is a retry-budget "
                "timeout, not a render failure. The job itself is untouched "
                "and will resume on its own once its retry backoff elapses; "
                "running `ytauto run` again now enqueues a SEPARATE new job "
                "for this project rather than resuming this one",
                file=sys.stderr,
            )
        else:
            print(
                f"ytauto run: job {job_id} did not reach a terminal state within "
                f"{args.max_ticks} ticks (currently {state!r})",
                file=sys.stderr,
            )
        return 1
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ytauto", description="Faceless video automation")
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--data-dir", type=Path, default=None, help="override the data directory")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("doctor", help="check that the environment is usable")
    _add_broll_subcommand(subparsers)
    _add_project_subcommand(subparsers)
    _add_run_subcommand(subparsers)

    args = parser.parse_args(argv)
    paths = AppPaths.resolve(override=args.data_dir)

    # Deliberately non-fatal. An unwritable data root is precisely the condition
    # `doctor` exists to report. Crashing here would show a traceback instead of
    # the diagnosis, and would make the careful error handling in
    # _check_paths/_check_disk unreachable on the real CLI path. File logging is
    # simply unavailable for such a run; _check_paths surfaces the cause.
    #
    # BOTH exception types are required. paths.ensure() raises ConfigurationError,
    # but it is not what fails first: Path.mkdir(parents=True, exist_ok=True) on an
    # *existing* directory succeeds regardless of write permission, so ensure()
    # returns cleanly and configure_logging goes on to construct a
    # RotatingFileHandler - whose __init__ opens the log file and raises a raw
    # OSError (PermissionError [Errno 13] on the reproduction).
    with contextlib.suppress(ConfigurationError, OSError):
        configure_logging(paths)
    bind_correlation_id()

    if args.command == "doctor":
        results = run_checks(paths)
        print(format_report(results))
        return exit_code(results)

    if args.command == "broll":
        if args.broll_command == "add":
            return _broll_add(paths, args)
        parser.error(f"unknown broll command: {args.broll_command}")
        return 2

    if args.command == "project":
        if args.project_command == "create":
            return _project_create(paths, args)
        parser.error(f"unknown project command: {args.project_command}")
        return 2

    if args.command == "run":
        return _run(paths, args)

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
