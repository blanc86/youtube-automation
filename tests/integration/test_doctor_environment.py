"""Integration tests for ``ytauto doctor`` against the real environment.

These tests require a real ffmpeg (with ffprobe alongside it) on PATH or
resolvable via ``YTAUTO_FFMPEG_DIR`` — unlike the hermetic tests in
``tests/unit/cli/test_doctor.py``, they shell out to the actual ffmpeg
binary (and, where present, ``nvidia-smi``) rather than monkeypatching it.
"""

from pathlib import Path

from ytauto.cli.doctor import Severity, run_checks
from ytauto.infra.paths import AppPaths


def test_run_checks_covers_the_expected_surface(tmp_path: Path) -> None:
    results = run_checks(AppPaths.resolve(override=tmp_path))
    names = {r.name for r in results}
    assert {
        "python",
        "data directories",
        "database",
        "cache ceiling",
        "ffmpeg",
        "h264 encoder",
        "subtitle burn-in",
        "gpu",
        "free disk",
    } <= names


def test_run_checks_creates_a_usable_database(tmp_path: Path) -> None:
    paths = AppPaths.resolve(override=tmp_path)
    run_checks(paths)
    assert paths.db_file.exists()


def test_python_check_passes_on_the_supported_interpreter(tmp_path: Path) -> None:
    results = run_checks(AppPaths.resolve(override=tmp_path))
    python = next(r for r in results if r.name == "python")
    assert python.severity is Severity.OK
