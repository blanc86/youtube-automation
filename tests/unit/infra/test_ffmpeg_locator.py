import subprocess
from pathlib import Path

import pytest

from ytauto.infra.ffmpeg.locator import _EXE, FfmpegNotFound, locate, parse_version

BANNER = (
    "ffmpeg version 7.1.1-essentials_build-www.gyan.dev Copyright (c) 2000-2025 the FFmpeg "
    "developers\nbuilt with gcc 14.2.0 (Rev1, Built by MSYS2 project)\n"
)


def test_parse_version_extracts_the_version_token() -> None:
    assert parse_version(BANNER) == "7.1.1-essentials_build-www.gyan.dev"


def test_parse_version_handles_a_plain_release_banner() -> None:
    assert parse_version("ffmpeg version 6.0 Copyright (c) 2000-2023") == "6.0"


def test_parse_version_returns_unknown_when_unparseable() -> None:
    assert parse_version("not an ffmpeg banner at all") == "unknown"


def _fake_pair(directory: Path) -> None:
    """Create a stub ffmpeg/ffprobe pair named the way THIS platform names them.

    The suffix comes from the locator's own ``_EXE`` rather than being hardcoded.
    Spelling it ".exe" here made every one of these tests silently Windows-only:
    on POSIX the locator looks for an extensionless ``ffmpeg``, found nothing,
    and each test collapsed into the generic not-found error - including the two
    that assert on a *specific* message, which then passed their raises() check
    for entirely the wrong reason.
    """
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"ffmpeg{_EXE}").write_text("stub")
    (directory / f"ffprobe{_EXE}").write_text("stub")


def test_configured_directory_is_preferred(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_pair(tmp_path / "bin")
    monkeypatch.setattr("ytauto.infra.ffmpeg.locator._read_version", lambda _: "7.1.1")
    found = locate(configured_dir=tmp_path / "bin")
    assert found.ffmpeg.parent == tmp_path / "bin"
    assert found.ffprobe.exists()
    assert found.version == "7.1.1"


def test_env_var_is_used_when_no_argument(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_pair(tmp_path / "envbin")
    monkeypatch.setenv("YTAUTO_FFMPEG_DIR", str(tmp_path / "envbin"))
    monkeypatch.setattr("ytauto.infra.ffmpeg.locator._read_version", lambda _: "7.1.1")
    assert locate().ffmpeg.parent == tmp_path / "envbin"


def test_missing_ffprobe_beside_ffmpeg_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lonely = tmp_path / "lonely"
    lonely.mkdir()
    (lonely / f"ffmpeg{_EXE}").write_text("stub")
    monkeypatch.delenv("YTAUTO_FFMPEG_DIR", raising=False)
    monkeypatch.setattr("shutil.which", lambda *_a, **_k: None)
    # Match a phrase unique to the mismatched-pair message. Matching on
    # "ffprobe" alone would also match the generic not-found message, so this
    # test would still pass if _pair_in's error were caught inside the loop
    # and the search fell through to exhaust every candidate.
    with pytest.raises(FfmpegNotFound, match="beside it"):
        locate(configured_dir=lonely)


def test_a_binary_that_exits_non_zero_is_rejected_not_reported_as_unknown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A corrupt build exits non-zero and prints nothing.

    Parsing that empty stdout yields version "unknown", which reads as a
    successful probe of a working install. The exit status must be believed.
    """
    _fake_pair(tmp_path / "bin")

    def _fake_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", _fake_run)

    with pytest.raises(FfmpegNotFound, match="corrupt"):
        locate(configured_dir=tmp_path / "bin")


def test_raises_when_nothing_is_found(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("YTAUTO_FFMPEG_DIR", raising=False)
    monkeypatch.setattr("shutil.which", lambda *_a, **_k: None)
    with pytest.raises(FfmpegNotFound):
        locate(configured_dir=tmp_path / "nonexistent")
