import sqlite3
import subprocess
from pathlib import Path

import pytest

from ytauto.cli.doctor import (
    CheckResult,
    Severity,
    exit_code,
    format_report,
    run_checks,
)
from ytauto.core.errors import ConfigurationError
from ytauto.infra.db.migrations import HEAD_VERSION
from ytauto.infra.paths import AppPaths


def test_exit_code_is_zero_when_all_ok() -> None:
    results = [CheckResult("a", Severity.OK, "fine"), CheckResult("b", Severity.WARN, "meh")]
    assert exit_code(results) == 0


def test_exit_code_is_one_when_anything_failed() -> None:
    results = [CheckResult("a", Severity.OK, "fine"), CheckResult("b", Severity.FAIL, "broken")]
    assert exit_code(results) == 1


def test_warnings_alone_do_not_fail_the_run() -> None:
    """A machine without an NVIDIA GPU is degraded, not broken."""
    assert exit_code([CheckResult("gpu", Severity.WARN, "none found")]) == 0


def test_format_report_includes_every_check_name_and_detail() -> None:
    text = format_report(
        [CheckResult("python", Severity.OK, "3.12.1"), CheckResult("gpu", Severity.WARN, "absent")]
    )
    assert "python" in text
    assert "3.12.1" in text
    assert "gpu" in text
    assert "absent" in text


def test_missing_ffmpeg_is_reported_as_a_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ytauto.infra.ffmpeg.locator import FfmpegNotFound

    def _boom(*_args: object, **_kwargs: object) -> None:
        raise FfmpegNotFound("simulated absence")

    monkeypatch.setattr("ytauto.cli.doctor.locate", _boom)
    # Hermetic: without this, run_checks still calls the real gpu.detect(),
    # which shells out to nvidia-smi on any machine that has one.
    monkeypatch.setattr("ytauto.cli.doctor.gpu.detect", lambda: None)
    results = run_checks(AppPaths.resolve(override=tmp_path))
    ffmpeg = next(r for r in results if r.name == "ffmpeg")
    assert ffmpeg.severity is Severity.FAIL
    assert exit_code(results) == 1


def test_probe_timeout_is_reported_as_a_failure_not_a_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A present-but-broken ffmpeg (hangs, corrupt binary) must not crash doctor.

    probe() runs subprocess.run(..., timeout=30) twice; a hung or corrupt
    binary raises subprocess.TimeoutExpired, which is a SubprocessError and
    NOT an OSError. A green run can never exercise this path.
    """
    from ytauto.infra.ffmpeg.locator import FfmpegBinaries

    def _fake_locate(*_args: object, **_kwargs: object) -> FfmpegBinaries:
        return FfmpegBinaries(
            ffmpeg=tmp_path / "ffmpeg.exe", ffprobe=tmp_path / "ffprobe.exe", version="0.0"
        )

    def _boom_probe(*_args: object, **_kwargs: object) -> None:
        raise subprocess.TimeoutExpired(cmd="ffmpeg -encoders", timeout=30)

    monkeypatch.setattr("ytauto.cli.doctor.locate", _fake_locate)
    monkeypatch.setattr("ytauto.cli.doctor.probe", _boom_probe)
    monkeypatch.setattr("ytauto.cli.doctor.gpu.detect", lambda: None)

    results = run_checks(AppPaths.resolve(override=tmp_path))

    h264 = next(r for r in results if r.name == "h264 encoder")
    subtitle = next(r for r in results if r.name == "subtitle burn-in")
    assert h264.severity is Severity.FAIL
    assert subtitle.severity is Severity.FAIL


@pytest.mark.parametrize(
    "failure",
    [
        subprocess.TimeoutExpired(cmd="ffmpeg -version", timeout=15),
        OSError(8, "Exec format error"),
    ],
    ids=["timeout", "oserror"],
)
def test_locate_failure_is_reported_as_a_failure_not_a_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: Exception
) -> None:
    """locate() calls _read_version(), which runs a subprocess with timeout=15.

    A present-but-hanging ffmpeg raises TimeoutExpired (a SubprocessError and
    NOT an OSError); a non-executable one raises OSError. Neither is
    FfmpegNotFound, so before this guard both escaped `except FfmpegNotFound`
    and crashed doctor with a traceback instead of printing the diagnosis.
    """

    def _boom_locate(*_args: object, **_kwargs: object) -> None:
        raise failure

    monkeypatch.setattr("ytauto.cli.doctor.locate", _boom_locate)
    monkeypatch.setattr("ytauto.cli.doctor.gpu.detect", lambda: None)

    results = run_checks(AppPaths.resolve(override=tmp_path))

    for name in ("ffmpeg", "h264 encoder", "subtitle burn-in"):
        assert next(r for r in results if r.name == name).severity is Severity.FAIL
    # The detail must name the real cause, not just "not found".
    ffmpeg = next(r for r in results if r.name == "ffmpeg")
    assert type(failure).__name__ in ffmpeg.detail
    assert exit_code(results) == 1


def test_run_checks_reports_every_check_even_when_paths_ensure_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Nothing aborts the report - every check still gets a row."""
    from ytauto.infra.ffmpeg.locator import FfmpegNotFound

    def _boom_locate(*_args: object, **_kwargs: object) -> None:
        raise FfmpegNotFound("simulated absence")

    def _boom_ensure(self: AppPaths) -> None:
        raise ConfigurationError("simulated unwritable root")

    monkeypatch.setattr("ytauto.cli.doctor.locate", _boom_locate)
    monkeypatch.setattr("ytauto.cli.doctor.gpu.detect", lambda: None)
    monkeypatch.setattr(AppPaths, "ensure", _boom_ensure)

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

    # Names alone are not enough: a _check_paths that swallowed the error and
    # returned OK would still satisfy the assertion above.
    paths_row = next(r for r in results if r.name == "data directories")
    assert paths_row.severity is Severity.FAIL
    assert exit_code(results) == 1


def _fake_binaries(tmp_path: Path) -> object:
    from ytauto.infra.ffmpeg.locator import FfmpegBinaries

    return FfmpegBinaries(
        ffmpeg=tmp_path / "ffmpeg.exe", ffprobe=tmp_path / "ffprobe.exe", version="7.1.1"
    )


def _patch_ffmpeg(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capabilities: object) -> None:
    monkeypatch.setattr("ytauto.cli.doctor.locate", lambda *_a, **_k: _fake_binaries(tmp_path))
    monkeypatch.setattr("ytauto.cli.doctor.probe", lambda *_a, **_k: capabilities)
    monkeypatch.setattr("ytauto.cli.doctor.gpu.detect", lambda: None)


def test_a_build_with_no_h264_encoder_is_a_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """best_h264_encoder() raises ConfigurationError; doctor must report it."""
    from ytauto.infra.ffmpeg.probe import FfmpegCapabilities

    _patch_ffmpeg(
        monkeypatch,
        tmp_path,
        FfmpegCapabilities(encoders=frozenset({"libx265"}), filters=frozenset({"ass"})),
    )

    results = run_checks(AppPaths.resolve(override=tmp_path))

    h264 = next(r for r in results if r.name == "h264 encoder")
    assert h264.severity is Severity.FAIL
    assert "h264" in h264.detail
    # libass is present, so that row must stay OK - the failure is scoped.
    assert next(r for r in results if r.name == "subtitle burn-in").severity is Severity.OK
    assert exit_code(results) == 1


def test_a_build_without_libass_is_a_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No `ass` filter means no video can ever be subtitled - FAIL, not WARN."""
    from ytauto.infra.ffmpeg.probe import FfmpegCapabilities

    _patch_ffmpeg(
        monkeypatch,
        tmp_path,
        FfmpegCapabilities(encoders=frozenset({"libx264"}), filters=frozenset({"scale"})),
    )

    results = run_checks(AppPaths.resolve(override=tmp_path))

    subtitle = next(r for r in results if r.name == "subtitle burn-in")
    assert subtitle.severity is Severity.FAIL
    assert "ass" in subtitle.detail
    # Software-only encoding is degraded, not broken.
    assert next(r for r in results if r.name == "h264 encoder").severity is Severity.WARN
    assert exit_code(results) == 1


def test_a_broken_database_fails_both_database_and_cache_ceiling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The _check_database handler had no test at all."""
    from ytauto.infra.ffmpeg.locator import FfmpegNotFound

    def _boom_connect(*_args: object, **_kwargs: object) -> None:
        raise sqlite3.OperationalError("unable to open database file")

    def _boom_locate(*_args: object, **_kwargs: object) -> None:
        raise FfmpegNotFound("simulated absence")

    monkeypatch.setattr("ytauto.cli.doctor.connect", _boom_connect)
    monkeypatch.setattr("ytauto.cli.doctor.locate", _boom_locate)
    monkeypatch.setattr("ytauto.cli.doctor.gpu.detect", lambda: None)

    results = run_checks(AppPaths.resolve(override=tmp_path))

    database = [r for r in results if r.name == "database"]
    ceiling = [r for r in results if r.name == "cache ceiling"]
    # Exactly one row each - the handler must not emit a second, contradictory
    # "database" row on top of one already appended.
    assert len(database) == 1
    assert len(ceiling) == 1
    assert database[0].severity is Severity.FAIL
    assert ceiling[0].severity is Severity.FAIL
    assert "unable to open database file" in database[0].detail
    assert exit_code(results) == 1


def test_a_database_stuck_below_head_is_a_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A schema behind HEAD must FAIL: the app would read tables that do not
    match the code that queries them.
    """
    from ytauto.infra.ffmpeg.locator import FfmpegNotFound

    def _boom_locate(*_args: object, **_kwargs: object) -> None:
        raise FfmpegNotFound("simulated absence")

    monkeypatch.setattr("ytauto.cli.doctor.apply_migrations", lambda _conn: HEAD_VERSION - 1)
    monkeypatch.setattr("ytauto.cli.doctor.locate", _boom_locate)
    monkeypatch.setattr("ytauto.cli.doctor.gpu.detect", lambda: None)

    results = run_checks(AppPaths.resolve(override=tmp_path))

    assert next(r for r in results if r.name == "database").severity is Severity.FAIL


@pytest.mark.parametrize(
    "failure",
    [
        ConfigurationError("simulated unwritable root"),
        PermissionError(13, "Permission denied"),
    ],
    ids=["configuration-error", "oserror"],
)
def test_main_does_not_crash_when_configure_logging_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: Exception
) -> None:
    """A green run cannot exercise this: an unwritable data root makes
    configure_logging() fail before any check runs.

    Two *different* failures reach here, and suppressing only the first is the
    bug this parametrisation exists to catch. paths.ensure() raises
    ConfigurationError - but it is not what fails first, because
    Path.mkdir(parents=True, exist_ok=True) on an *existing* directory succeeds
    regardless of write permission. ensure() therefore returns cleanly and the
    RotatingFileHandler constructor is what fails, opening the log file and
    raising a raw OSError.
    """
    from ytauto.cli.__main__ import main
    from ytauto.infra.ffmpeg.locator import FfmpegNotFound

    def _boom_locate(*_args: object, **_kwargs: object) -> None:
        raise FfmpegNotFound("simulated absence")

    def _boom_configure_logging(*_args: object, **_kwargs: object) -> None:
        raise failure

    monkeypatch.setattr("ytauto.cli.doctor.locate", _boom_locate)
    monkeypatch.setattr("ytauto.cli.doctor.gpu.detect", lambda: None)
    monkeypatch.setattr("ytauto.cli.__main__.configure_logging", _boom_configure_logging)

    # --data-dir is a top-level option; it must precede the subcommand name
    # for argparse's subparsers to accept it.
    result = main(["--data-dir", str(tmp_path), "doctor"])

    # Not `isinstance(result, int)`: that is vacuously true of any return value
    # and is the banned pattern. The mocked ffmpeg absence guarantees a FAIL
    # row, so 1 is the only correct exit code.
    assert result == 1
