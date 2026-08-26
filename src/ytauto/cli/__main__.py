"""Command-line entry point."""

from __future__ import annotations

import argparse
import contextlib
import sys
from pathlib import Path

from ytauto import __version__
from ytauto.app.services.enqueue import create_project, refresh_run_settings, resolve_project_id
from ytauto.app.services.render import (
    DEFAULT_MAX_TICKS,
    PIPELINE_ID,
    RenderState,
    render_project,
)
from ytauto.cli.doctor import exit_code, format_report, run_checks
from ytauto.core.errors import ConfigurationError, ValidationError
from ytauto.infra.artifacts import ArtifactStore
from ytauto.infra.broll import BrollLibrary
from ytauto.infra.cas.store import CasStore
from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import apply_migrations
from ytauto.infra.logging import bind_correlation_id, configure_logging
from ytauto.infra.music import MusicLibrary
from ytauto.infra.paths import AppPaths, ensure_writable_dir, resolve_output_dir
from ytauto.ui import DEFAULT_PORT, HOST

_PIPELINE_ID = PIPELINE_ID
"""Re-exported from ``app.services.render``, where the whole render sequence
now lives so ``ytauto ui`` drives the identical one. Kept as a module name
here because this module's own tests and the integration suite reference it."""

_DEFAULT_MAX_TICKS = DEFAULT_MAX_TICKS
"""Also re-exported: it is the default of ``--max-ticks``, whose help text
must name a number."""


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


def _add_music_subcommand(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    music = subparsers.add_parser("music", help="manage the music library")
    music_subparsers = music.add_subparsers(dest="music_command", required=True)

    add = music_subparsers.add_parser(
        "add", help="ingest a music track: probe, store, record provenance"
    )
    add.add_argument("path", type=Path, help="path to the audio file")
    # Required here for the same reason as on `broll add`, and with less room
    # for argument: a Content ID match on the bed claims the whole video.
    add.add_argument("--source-url", required=True, help="where the track came from")
    add.add_argument("--licence", required=True, help="the track's licence")
    add.add_argument("--title", default="", help="display name (defaults to the filename)")
    add.add_argument("--attribution", default="", help="attribution text, if the licence needs one")
    add.add_argument("--notes", default="", help="free-form notes")

    music_subparsers.add_parser("list", help="list every track in the library")


def _music_add(paths: AppPaths, args: argparse.Namespace) -> int:
    """Ingest one track. Returns the process exit code.

    No manifest rewrite, unlike ``_broll_add``: a project names one track by
    id and ``refresh_run_settings`` resolves it per run, so there is no
    library-wide blob for a stage to read.
    """
    paths.ensure()
    conn = connect(paths.db_file)
    try:
        apply_migrations(conn)
        cas = CasStore(root=paths.cas, conn=conn)
        track_id = MusicLibrary(conn, cas).add(
            args.path,
            source_url=args.source_url,
            licence=args.licence,
            title=args.title,
            attribution=args.attribution,
            notes=args.notes,
        )
    finally:
        conn.close()
    print(f"added music track {track_id}")
    return 0


def _music_list(paths: AppPaths, args: argparse.Namespace) -> int:
    """Print the library, one track per line. Returns the process exit code."""
    paths.ensure()
    conn = connect(paths.db_file)
    try:
        apply_migrations(conn)
        cas = CasStore(root=paths.cas, conn=conn)
        tracks = MusicLibrary(conn, cas).list_tracks()
    finally:
        conn.close()

    if not tracks:
        print("no music tracks yet - add one with `ytauto music add`")
        return 0
    for track in tracks:
        print(f"{track.id}  {track.duration_s:7.1f}s  {track.licence:<12}  {track.title}")
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


def _run(paths: AppPaths, args: argparse.Namespace) -> int:
    """Enqueue one job against ``--project`` and drain the queue. Returns the
    process exit code.

    Thin wiring, deliberately. Everything between "refresh this project's
    settings" and "the masters are in a folder" lives in
    ``app.services.render.render_project`` so that ``ytauto ui`` drives the
    identical sequence rather than a second copy of it - see that module's
    docstring. What stays here is what is genuinely CLI-shaped: the
    ``--output-dir`` flag, the exit-code contract, and the wording of every
    diagnostic.

    **Two settings are re-derived before anything is enqueued.**
    ``refresh_run_settings`` (see its own docstring for the full account)
    recomputes ``story_digest`` from whatever is on disk at
    ``settings["story_path"]`` right now, and rewrites the B-roll manifest to
    bind ``broll_manifest_digest`` to the library's current state. Both are
    derived values that go stale on their own: a story is edited in place by
    design, and the B-roll library is global and mutable. Doing it here, in
    the one place that turns a project into a job, is what makes an edited
    story invalidate the cache instead of being silently ignored.

    **The output location is resolved before anything is enqueued.**
    ``infra.paths.resolve_output_dir`` picks the user's platform Videos
    folder (falling back to Downloads if that cannot be created or proven
    writable), or ``--output-dir`` overrides it explicitly - either way this
    happens before the job is enqueued, so a broken output location fails
    fast rather than after minutes of rendering.

    **On success, the two rendered masters are exported** into
    ``<output_dir> / <slug>`` and the export directory is the last thing this
    command prints - it is the one thing an operator most needs to see.

    Exit codes: 0 once the enqueued job reaches ``succeeded``; 2 if
    ``--project`` names no project, the project's settings are missing or
    malformed, or the output location (auto-detected or ``--output-dir``)
    could not be created or proven writable - all bad input or bad
    environment, checked before anything is enqueued; 1 for every other way
    this can fail to end in success - the job reaches ``failed``, it burns
    through ``--max-ticks`` while genuinely busy, it is still waiting out a
    retry backoff once the no-progress budget elapses, the dispatcher itself
    raises, or the successful job's masters fail export verification. The
    last three are reported with different stderr messages - sharing one exit
    code is fine within this brief's ``{0,1,2}`` contract, but "ran out of
    tick budget", "made no progress for N seconds" and "the render actually
    failed" send an operator to different places and must not read the same.
    """
    paths.ensure()
    conn = connect(paths.db_file)
    try:
        apply_migrations(conn)
        cas = CasStore(root=paths.cas, conn=conn)
        artifacts = ArtifactStore(cas, conn)

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

        try:
            outcome = render_project(
                conn,
                cas,
                artifacts,
                project_id=project_id,
                slug=args.slug,
                settings=settings,
                output_dir=output_dir,
                max_ticks=args.max_ticks,
            )
        except ValidationError as exc:
            print(
                f"ytauto run: the queue stopped on an unrecoverable error: {exc}",
                file=sys.stderr,
            )
            return 1

        if outcome.state is RenderState.SUCCEEDED:
            print(
                f"ytauto run: job {outcome.job_id} succeeded; "
                f"masters written to {outcome.export_dir}"
            )
            return 0
        if outcome.state is RenderState.EXPORT_FAILED:
            print(f"ytauto run: {outcome.detail}", file=sys.stderr)
            return 1
        if outcome.state is RenderState.FAILED:
            print(f"ytauto run: job {outcome.job_id} failed", file=sys.stderr)
            return 1
        if outcome.state is RenderState.NO_PROGRESS_TIMEOUT:
            print(
                f"ytauto run: job {outcome.job_id} made no progress for "
                f"{outcome.no_progress_budget_s:.0f}s and this invocation gave up "
                f"waiting (currently {outcome.detail}) - this is a retry-budget "
                "timeout, not a render failure. The job itself is untouched "
                "and will resume on its own once its retry backoff elapses; "
                "running `ytauto run` again now enqueues a SEPARATE new job "
                "for this project rather than resuming this one",
                file=sys.stderr,
            )
        else:
            print(
                f"ytauto run: job {outcome.job_id} did not reach a terminal state within "
                f"{outcome.max_ticks} ticks (currently {outcome.detail})",
                file=sys.stderr,
            )
        return 1
    finally:
        conn.close()


def _add_ui_subcommand(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    ui = subparsers.add_parser("ui", help="serve the local web UI")
    ui.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"port to listen on (default: {DEFAULT_PORT})",
    )
    # There is deliberately no --host. The UI has no authentication of any
    # kind - it creates projects, edits stories and starts renders on this
    # machine - so the listen address is not a decision to delegate to a
    # flag someone might set to 0.0.0.0 "just to test from my phone". It is
    # pinned to loopback in ytauto.ui.server.


def _ui(paths: AppPaths, args: argparse.Namespace) -> int:
    """Serve the local web UI until interrupted. Returns the process exit code.

    Exit codes match the rest of this CLI: 2 for bad environment (the data
    directory cannot be created, or the port is already taken - both things
    the user must fix), 0 for a clean shutdown on Ctrl-C.
    """
    try:
        paths.ensure()
    except ConfigurationError as exc:
        print(f"ytauto ui: {exc}", file=sys.stderr)
        return 2
    # Imported here, not at module scope: this is the only subcommand that
    # needs Flask, and paying its import cost on every `ytauto doctor` would
    # be a tax on the front end that does not use it. ``ytauto.ui``'s own
    # package module carries HOST/DEFAULT_PORT precisely so the argument
    # parser can name them without dragging the framework in.
    from ytauto.ui.server import serve

    url = f"http://{HOST}:{args.port}/"
    print(f"ytauto ui: serving {url}  (Ctrl-C to stop)")
    try:
        serve(paths, port=args.port)
    except OSError as exc:
        print(f"ytauto ui: cannot listen on port {args.port}: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("\nytauto ui: stopped")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ytauto", description="Faceless video automation")
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--data-dir", type=Path, default=None, help="override the data directory")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("doctor", help="check that the environment is usable")
    _add_broll_subcommand(subparsers)
    _add_music_subcommand(subparsers)
    _add_project_subcommand(subparsers)
    _add_run_subcommand(subparsers)
    _add_ui_subcommand(subparsers)

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

    if args.command == "music":
        if args.music_command == "add":
            return _music_add(paths, args)
        if args.music_command == "list":
            return _music_list(paths, args)
        parser.error(f"unknown music command: {args.music_command}")
        return 2

    if args.command == "project":
        if args.project_command == "create":
            return _project_create(paths, args)
        parser.error(f"unknown project command: {args.project_command}")
        return 2

    if args.command == "run":
        return _run(paths, args)

    if args.command == "ui":
        return _ui(paths, args)

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
