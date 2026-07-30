"""Locate the ffmpeg/ffprobe binaries.

Version parsing is a pure function so it is testable without executing
anything. Only ``_read_version`` touches a subprocess.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from ytauto.core.errors import ConfigurationError

_ENV_VAR = "YTAUTO_FFMPEG_DIR"
_VERSION_RE = re.compile(r"^ffmpeg version (\S+)")
_EXE = ".exe" if os.name == "nt" else ""


class FfmpegNotFound(ConfigurationError):
    """Neither a configured, environment, nor PATH installation was usable."""


@dataclass(frozen=True)
class FfmpegBinaries:
    ffmpeg: Path
    ffprobe: Path
    version: str


def parse_version(banner: str) -> str:
    """Extract the version token from an ffmpeg banner. Pure."""
    match = _VERSION_RE.search(banner.strip())
    return match.group(1) if match else "unknown"


def _read_version(ffmpeg: Path) -> str:
    result = subprocess.run(
        [str(ffmpeg), "-hide_banner", "-version"],
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=15,
        check=False,
    )
    if result.returncode != 0:
        # A non-zero exit means the binary is there but did not answer. Parsing
        # its (likely empty) stdout would silently report version "unknown" for
        # a corrupt build, which reads as a successful probe.
        raise FfmpegNotFound(
            f"{ffmpeg} exited {result.returncode} when asked for its version; "
            "the installation looks corrupt"
        )
    return parse_version(result.stdout)


def _pair_in(directory: Path) -> tuple[Path, Path] | None:
    ffmpeg = directory / f"ffmpeg{_EXE}"
    if not ffmpeg.is_file():
        return None
    ffprobe = directory / f"ffprobe{_EXE}"
    if not ffprobe.is_file():
        raise FfmpegNotFound(
            f"found ffmpeg at {ffmpeg} but no ffprobe beside it; "
            "install a complete build so the pair stays version-matched"
        )
    return ffmpeg, ffprobe


def locate(configured_dir: Path | None = None) -> FfmpegBinaries:
    """Find a matched ffmpeg/ffprobe pair.

    Note this does more than look at the filesystem: it executes the binary to
    read its version, so it can fail in every way a subprocess can. Callers that
    only catch FfmpegNotFound will be surprised - `ytauto doctor` was.

    Raises:
        FfmpegNotFound: no candidate directory held a usable pair; an ffmpeg was
            found with no ffprobe beside it; or the binary exited non-zero when
            asked for its version.
        subprocess.TimeoutExpired: the binary did not respond within 15s.
        OSError: the binary exists but cannot be executed (wrong architecture,
            missing loader, permission denied).
    """
    candidates: list[Path] = []
    if configured_dir is not None:
        candidates.append(Path(configured_dir))
    if from_env := os.environ.get(_ENV_VAR):
        candidates.append(Path(from_env))
    if on_path := shutil.which("ffmpeg"):
        candidates.append(Path(on_path).parent)
    candidates.append(Path(__file__).resolve().parents[3] / "bin")

    for directory in candidates:
        if not directory.is_dir():
            continue
        if (pair := _pair_in(directory)) is not None:
            ffmpeg, ffprobe = pair
            return FfmpegBinaries(ffmpeg=ffmpeg, ffprobe=ffprobe, version=_read_version(ffmpeg))

    raise FfmpegNotFound(
        "ffmpeg was not found. Install it and put it on PATH, or set "
        f"{_ENV_VAR} to the directory containing ffmpeg and ffprobe."
    )
