"""Probe a media file's own dimensions and duration via ffprobe.

Distinct from ``ytauto.infra.ffmpeg.probe``, which probes the *ffmpeg build's*
own capabilities (which encoders and filters it supports). This module probes
a *media file's* content - the question B-roll ingest needs answered before it
can normalise a clip: how big is it, and how long does it run. It lives
alongside ``locator.py``/``probe.py`` under ``infra/ffmpeg`` because all three
wrap a subprocess call into the ffmpeg/ffprobe pair and none of them are
reachable from ``ytauto.core``.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ytauto.core.errors import ValidationError


@dataclass(frozen=True)
class MediaInfo:
    width: int
    height: int
    duration_s: float


def _duration(format_block: dict[str, Any], video_stream: dict[str, Any]) -> float | None:
    """Prefer the container-level duration, falling back to the stream's own.

    ``format.duration`` is what ffprobe fills in for essentially every
    container ffmpeg can mux, so it is tried first. Some inputs (raw /
    elementary streams, oddities from other tools) omit the format block's
    duration but still carry one on the video stream, hence the fallback
    rather than treating format-only as authoritative.

    A duration of zero or less is treated the same as an absent one: a
    zero-duration clip is not a valid answer, it is a probe failure that
    happened not to raise, and passing it through would let a black gap reach
    a rendered segment instead of failing loudly here where it is cheap to
    fix.
    """
    for block in (format_block, video_stream):
        raw = block.get("duration")
        if raw is None:
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    return None


def probe_media(path: Path, *, ffprobe: Path) -> MediaInfo:
    """Probe ``path``'s video stream for width, height and duration.

    Runs ``ffprobe -v error -print_format json -show_streams -show_format``
    and parses the result. ``ffprobe`` is taken explicitly rather than
    resolved internally - callers get it from ``infra.ffmpeg.locator.locate``,
    mirroring how ``infra.ffmpeg.probe.probe`` takes a resolved
    ``FfmpegBinaries`` rather than re-locating one itself.

    Raises:
        ValidationError: ``path`` does not exist, ffprobe exited non-zero,
            its stdout was not parseable JSON, the file has no video stream,
            the video stream is missing width or height, or no positive
            duration could be found on either the format block or the video
            stream.
        subprocess.TimeoutExpired: ffprobe did not respond within 30s.
        OSError: ``ffprobe`` cannot be executed.
    """
    if not path.is_file():
        raise ValidationError(f"source file does not exist: {path}")

    result = subprocess.run(
        [
            str(ffprobe),
            "-v",
            "error",
            "-print_format",
            "json",
            "-show_streams",
            "-show_format",
            str(path),
        ],
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        check=False,
    )
    if result.returncode != 0:
        raise ValidationError(
            f"ffprobe exited {result.returncode} probing {path}: {result.stderr.strip()}"
        )

    try:
        payload: dict[str, Any] = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise ValidationError(f"ffprobe produced unparseable output for {path}: {exc}") from exc

    streams: list[dict[str, Any]] = payload.get("streams", [])
    video_stream = next((s for s in streams if s.get("codec_type") == "video"), None)
    if video_stream is None:
        raise ValidationError(f"no video stream found in {path}")

    width = video_stream.get("width")
    height = video_stream.get("height")
    if width is None or height is None:
        raise ValidationError(f"video stream in {path} is missing width/height")

    duration = _duration(payload.get("format", {}), video_stream)
    if duration is None:
        raise ValidationError(
            f"could not determine a positive duration for {path}: absent from both "
            "format.duration and the video stream's own duration"
        )

    return MediaInfo(width=int(width), height=int(height), duration_s=duration)


def probe_audio_duration(path: Path, *, ffprobe: Path) -> float:
    """Probe ``path``'s *audio* stream and return its duration in seconds.

    Separate from ``probe_media`` rather than a flag on it, because the two
    disagree about what a valid file is. ``probe_media`` requires a video
    stream and reads width/height off it; a music file has no video stream at
    all, so it would be rejected outright - and an MP3 that carries embedded
    cover art is worse than rejected, because the artwork *is* a video stream
    (mjpeg, one frame) and ``probe_media`` would happily return the cover
    image's dimensions and, on some files, its frame duration. This asks the
    audio stream directly.

    Raises:
        ValidationError: ``path`` does not exist, ffprobe exited non-zero, its
            stdout was not parseable JSON, the file has no audio stream, or no
            positive duration could be found on either the format block or the
            audio stream.
        subprocess.TimeoutExpired: ffprobe did not respond within 30s.
        OSError: ``ffprobe`` cannot be executed.
    """
    if not path.is_file():
        raise ValidationError(f"source file does not exist: {path}")

    result = subprocess.run(
        [
            str(ffprobe),
            "-v",
            "error",
            "-print_format",
            "json",
            "-show_streams",
            "-show_format",
            str(path),
        ],
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        check=False,
    )
    if result.returncode != 0:
        raise ValidationError(
            f"ffprobe exited {result.returncode} probing {path}: {result.stderr.strip()}"
        )

    try:
        payload: dict[str, Any] = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise ValidationError(f"ffprobe produced unparseable output for {path}: {exc}") from exc

    streams: list[dict[str, Any]] = payload.get("streams", [])
    audio_stream = next((s for s in streams if s.get("codec_type") == "audio"), None)
    if audio_stream is None:
        raise ValidationError(f"no audio stream found in {path}")

    duration = _duration(payload.get("format", {}), audio_stream)
    if duration is None:
        raise ValidationError(
            f"could not determine a positive duration for {path}: absent from both "
            "format.duration and the audio stream's own duration"
        )
    return duration


def probe_dimensions(path: Path, *, ffprobe: Path) -> tuple[int, int]:
    """Convenience wrapper over ``probe_media`` for callers that only need size.

    Raises:
        Same as ``probe_media``.
    """
    info = probe_media(path, ffprobe=ffprobe)
    return (info.width, info.height)
