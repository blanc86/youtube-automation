from pathlib import Path

import pytest

from ytauto.cli.doctor import (
    CheckResult,
    Severity,
    exit_code,
    format_report,
    run_checks,
)
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
    results = run_checks(AppPaths.resolve(override=tmp_path))
    ffmpeg = next(r for r in results if r.name == "ffmpeg")
    assert ffmpeg.severity is Severity.FAIL
    assert exit_code(results) == 1
