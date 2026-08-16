"""Command-line entry point."""

from __future__ import annotations

import argparse
import contextlib
from pathlib import Path

from ytauto import __version__
from ytauto.cli.doctor import exit_code, format_report, run_checks
from ytauto.core.errors import ConfigurationError
from ytauto.infra.broll import BrollLibrary
from ytauto.infra.cas.store import CasStore
from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import apply_migrations
from ytauto.infra.logging import bind_correlation_id, configure_logging
from ytauto.infra.paths import AppPaths


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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ytauto", description="Faceless video automation")
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--data-dir", type=Path, default=None, help="override the data directory")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("doctor", help="check that the environment is usable")
    _add_broll_subcommand(subparsers)

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

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
