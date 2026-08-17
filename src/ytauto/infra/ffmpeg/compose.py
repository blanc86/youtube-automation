"""Build the ffmpeg argument vector for one full-video compose pass.

This is the whole point of Task 11's brief: trim every B-roll segment,
concatenate them, burn captions in, and mux the narration - **one ffmpeg
invocation, no intermediate files**. Writing an intermediate per-segment file
and re-reading it is the dominant cause of slow render pipelines, and this
project's design says so explicitly; a single ``-filter_complex`` graph is
what avoids it.

``compose_args`` is pure: it returns the argument vector (excluding the
``ffmpeg`` binary itself, which the caller - ``app/stages/compose.py`` -
prepends once it has resolved one via ``infra.ffmpeg.locator.locate``) and
executes nothing. That split is what makes the four graph-shape tests in
``tests/unit/infra/test_compose_args.py`` run in milliseconds with no
subprocess and no real ffmpeg binary anywhere on the machine.

**The Windows drive-letter colon.** ffmpeg's filter-graph syntax treats ``:``
as an argument separator inside one filter's own option list (e.g.
``ass=filename:original_size=...``), so an absolute Windows path like
``C:\\work\\captions.ass`` interpolated into ``ass=C:\\work\\captions.ass``
reads as filename ``C`` with a stray ``\\work\\captions.ass`` option - a
broken graph, not a missing-file error, and a confusing one to debug from the
error text alone. ``compose_args`` refuses an absolute ``ass_path`` outright
rather than trying to escape it: the sanctioned fix is for the caller to run
ffmpeg with ``cwd`` set to the directory containing the ``.ass`` file and
pass just its bare filename, which sidesteps the colon entirely instead of
fighting ffmpeg's own escaping rules.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

_AUDIO_CODEC = "aac"
_PIX_FMT = "yuv420p"


@dataclass(frozen=True)
class ComposeClip:
    """One trimmed B-roll segment to concatenate into the master.

    ``path`` is expected to already be normalised to the target canvas (see
    Task 9's ``normalise_clip``) - the ``scale``/``pad`` chain
    ``compose_args`` still applies per clip is a defensive guard against a
    stale or hand-edited manifest entry, not the primary mechanism for
    getting the dimensions right.
    """

    path: Path
    in_point_s: float
    duration_s: float


def _scale_pad(width: int, height: int) -> str:
    """The same scale-then-pad chain ``infra.broll.normalise_clip`` uses.

    Every clip named in the B-roll manifest was already normalised to this
    exact canvas at ingest time, so this is a defensive second application,
    not the primary source of correct dimensions: ffmpeg's ``concat`` filter
    requires every input stream feeding it to agree on width and height
    exactly, and a filter chain that enforces that itself turns a corrupted
    or hand-edited manifest entry into a clear scale/pad no-op instead of an
    opaque ``concat`` failure deep in ffmpeg's own diagnostics.
    """
    return (
        f"scale={width}:{height}:force_original_aspect_ratio=decrease,"
        f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2"
    )


def compose_args(
    *,
    clips: Sequence[ComposeClip],
    ass_path: Path,
    audio_path: Path,
    out_path: Path,
    width: int,
    height: int,
    encoder: str,
) -> list[str]:
    """Build the full ffmpeg argument vector for one compose pass. Pure.

    Shape, in order: one ``-ss``/``-t``/``-i`` triple per clip (input-side
    seeking, so each clip is read starting at its own ``in_point_s`` for
    exactly ``duration_s``), then one more ``-i`` for the narration track.
    ``-filter_complex`` scales and pads each clip stream to
    ``width``x``height``, concatenates them in order, then burns
    ``ass_path`` onto the concatenated result - concat before ass, always,
    so captions land on the assembled timeline rather than on one lone
    segment. The filtered video and the narration's own audio stream are
    mapped to the output, encoded with ``encoder`` (already resolved by the
    caller - this function makes no encoder-availability decision of its
    own) and ``aac`` respectively, with ``-shortest`` so the output ends at
    the shorter of the two streams. Per this task's brief, that is an
    accepted Phase 2a limitation, not a bug this function works around: the
    video track is derived from the last word's end (Task 7), narration.mp3
    may carry trailing silence past it, and ``-shortest`` is what makes the
    mux end at the video rather than the audio - deliberately left in place.

    Raises:
        ValueError: ``clips`` is empty - there is nothing to concatenate.
        ValueError: ``ass_path`` is absolute. See the module docstring for
            why: an absolute Windows path breaks the filter graph's own
            ``:``-as-separator parsing. Callers must pass a bare filename and
            run ffmpeg with ``cwd`` set to the directory that contains it.
    """
    if not clips:
        raise ValueError("compose_args needs at least one clip to concatenate")
    if ass_path.is_absolute():
        raise ValueError(
            f"ass_path must be relative, not {ass_path!r}: an absolute Windows path "
            "puts a drive-letter colon inside the filter graph, which ffmpeg's own "
            "argument-separator parsing reads as breaking the graph rather than as "
            "part of a filename. Pass a bare filename and run ffmpeg with cwd set to "
            "the directory that contains it."
        )

    args: list[str] = ["-hide_banner", "-y"]
    for clip in clips:
        args += [
            "-ss",
            str(float(clip.in_point_s)),
            "-t",
            str(float(clip.duration_s)),
            "-i",
            str(clip.path),
        ]
    args += ["-i", str(audio_path)]

    clip_count = len(clips)
    scale_pad = _scale_pad(width, height)
    per_clip = ";".join(f"[{i}:v]{scale_pad}[v{i}]" for i in range(clip_count))
    concat_inputs = "".join(f"[v{i}]" for i in range(clip_count))
    graph = (
        f"{per_clip};{concat_inputs}concat=n={clip_count}:v=1:a=0[vcat];[vcat]ass={ass_path}[vout]"
    )

    args += [
        "-filter_complex",
        graph,
        "-map",
        "[vout]",
        "-map",
        f"{clip_count}:a",
        "-c:v",
        encoder,
        "-pix_fmt",
        _PIX_FMT,
        "-c:a",
        _AUDIO_CODEC,
        "-shortest",
        str(out_path),
    ]
    return args
