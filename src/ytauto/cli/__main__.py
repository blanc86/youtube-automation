"""Command-line entry point."""

from __future__ import annotations

import argparse
import contextlib
import sqlite3
import sys
import uuid
from pathlib import Path

from ytauto import __version__
from ytauto.app.registry import build_pipeline
from ytauto.app.scheduler.dispatcher import Dispatcher
from ytauto.app.scheduler.governor import Governor
from ytauto.app.scheduler.queue import JobQueue
from ytauto.app.services.enqueue import create_project, resolve_project_id
from ytauto.app.services.projects import ProjectService
from ytauto.cli.doctor import exit_code, format_report, run_checks
from ytauto.core.errors import ConfigurationError, ValidationError
from ytauto.core.models.job import JobState
from ytauto.infra.artifacts import ArtifactStore
from ytauto.infra.broll import BrollLibrary
from ytauto.infra.cas.store import CasStore
from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import apply_migrations
from ytauto.infra.logging import bind_correlation_id, configure_logging
from ytauto.infra.paths import AppPaths

_PIPELINE_ID = "story_video"
"""The only pipeline this CLI drives. Not exposed as a flag: Phase 2a ships
exactly one pipeline, and a --pipeline flag with one legal value would be
dead surface area until a second pipeline actually exists."""

_DEFAULT_MAX_TICKS = 100
"""``tick()`` advances a job by exactly one stage, so a clean run of the
seven-stage ``story_video`` pipeline needs a minimum of seven ticks (one per
stage; ``_maybe_complete_job`` requeues the job immediately after each
non-terminal stage commit, so no idle ticks intervene on the happy path).
100 leaves generous headroom for a handful of transient-failure retries
(``_MAX_STAGE_ATTEMPTS = 5`` per stage, exponential backoff) without either
truncating a healthy run or letting a single invocation spin unboundedly."""


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
        except ValidationError as exc:
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
        help=f"stop after this many dispatcher ticks (default: {_DEFAULT_MAX_TICKS})",
    )


def _job_state(conn: sqlite3.Connection, job_id: str) -> str | None:
    row = conn.execute("SELECT state FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return str(row["state"]) if row is not None else None


def _run(paths: AppPaths, args: argparse.Namespace) -> int:
    """Enqueue one job against ``--project`` and drain the queue. Returns the
    process exit code.

    Exit codes: 0 once the enqueued job reaches ``succeeded``; 2 if
    ``--project`` names no project - bad input, checked before anything is
    enqueued; 1 for every other way this can fail to end in success - the
    job reaches ``failed``, it has not reached a terminal state once
    ``--max-ticks`` runs out, or the dispatcher itself raises.

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

    The policy here is to let it propagate out of ``run_until_idle`` (called
    once, exactly as Step 3 of this task's brief specifies - not looped by
    hand so this command could catch and retry around it) and report it as a
    clear, non-zero failure naming the offending row, rather than swallowing
    it and letting the dispatcher grind through a database whose invariants a
    bug has already broken. The alternative - catching it per-tick inside the
    dispatcher and failing just that one job before continuing - was
    considered and rejected: it would need a change to
    ``app/scheduler/dispatcher.py``, which is out of this task's scope, and it
    would turn a bug class two separate reviews flagged as fatal into
    something this command quietly tolerates. The job ``tick()`` was
    mid-claim on when this happens stays claimed until its lease (300 s)
    expires and then becomes reclaimable again; a poison job left in that
    state will surface identically on the next invocation, which is the
    intended, visible signal to go fix the underlying data rather than a
    crash this command should paper over.
    """
    paths.ensure()
    conn = connect(paths.db_file)
    try:
        apply_migrations(conn)
        cas = CasStore(root=paths.cas, conn=conn)
        artifacts = ArtifactStore(cas, conn)
        queue = JobQueue(conn)
        projects = ProjectService(conn)

        try:
            project_id = resolve_project_id(conn, args.slug)
        except ValidationError as exc:
            print(f"ytauto run: {exc}", file=sys.stderr)
            return 2

        job_id = uuid.uuid4().hex
        try:
            settings = projects.settings_for(project_id)
            pipeline = build_pipeline(_PIPELINE_ID, cas, settings)
            queue.enqueue(job_id, project_id, _PIPELINE_ID)
            dispatcher = Dispatcher(
                conn, cas, artifacts, Governor(), queue, pipelines={_PIPELINE_ID: pipeline}
            )
            dispatcher.run_until_idle(max_ticks=args.max_ticks)
        except ValidationError as exc:
            print(
                f"ytauto run: the queue stopped on an unrecoverable error: {exc}",
                file=sys.stderr,
            )
            return 1

        state = _job_state(conn, job_id)
        if state == JobState.SUCCEEDED.value:
            return 0
        if state == JobState.FAILED.value:
            print(f"ytauto run: job {job_id} failed", file=sys.stderr)
            return 1
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
