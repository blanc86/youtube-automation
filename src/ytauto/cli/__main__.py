"""Command-line entry point."""

from __future__ import annotations

import argparse
import contextlib
from pathlib import Path

from ytauto import __version__
from ytauto.cli.doctor import exit_code, format_report, run_checks
from ytauto.core.errors import ConfigurationError
from ytauto.infra.logging import bind_correlation_id, configure_logging
from ytauto.infra.paths import AppPaths


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ytauto", description="Faceless video automation")
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--data-dir", type=Path, default=None, help="override the data directory")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("doctor", help="check that the environment is usable")

    args = parser.parse_args(argv)
    paths = AppPaths.resolve(override=args.data_dir)

    # Deliberately non-fatal. configure_logging calls paths.ensure(), which
    # raises ConfigurationError on an unwritable data root - precisely the
    # condition `doctor` exists to report. Crashing here would show a traceback
    # instead of the diagnosis, and would make the careful error handling in
    # _check_paths/_check_disk unreachable on the real CLI path. File logging
    # is simply unavailable for such a run; _check_paths surfaces the cause.
    with contextlib.suppress(ConfigurationError):
        configure_logging(paths)
    bind_correlation_id()

    if args.command == "doctor":
        results = run_checks(paths)
        print(format_report(results))
        return exit_code(results)

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
