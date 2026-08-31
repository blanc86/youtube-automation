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


BED_TARGET_LUFS = -32.0
"""The level every bed is normalised to before the operator's trim.

Chosen by measurement, not taste. Three legitimate tracks spanning 24 dB of
input level (-39.5, -21.5 and -15.5 dBFS mean) all land within 0.1 dB of
-31.4 dBFS mean after ``loudnorm=I=-32``. Narration from edge-tts sits around
-21.8 dBFS mean in a finished master, so a normalised bed arrives about 10 dB
under the voice - present, and not competing with it.

That spread is the whole reason this constant exists. Before it, ``gain_db``
attenuated a track whose own level was unknown, so the same setting produced
an inaudible bed for one track and a reasonable one for another. Nothing in
the plumbing was wrong; the control simply did not mean anything."""

_BED_TRUE_PEAK = -3.0
"""No format pin on either branch, deliberately, and this was measured the
hard way. Pinning both to ``aformat=...:channel_layouts=stereo`` to make
``amix``'s inputs agree also pushes a mono narration through ffmpeg's
mono-to-stereo rematrix, which applies its own attenuation: the finished
master came back 2.5 dB *quieter* than the same render with no music at all.
The voice must never be touched, so libavfilter is left to negotiate the
sample-rate difference between 24 kHz narration and loudnorm's 48 kHz output
- which it already did correctly for the 44.1 kHz beds that preceded
loudnorm."""


@dataclass(frozen=True)
class MusicBed:
    """An optional music track to mix in under the narration.

    ``trim_db`` adjusts the bed *after* it has been normalised to
    ``BED_TARGET_LUFS``, so 0 means "the standard bed level" and the number is
    a statement about this video rather than about the file. It is applied to
    the music alone - the narration is never touched - which is what keeps the
    volume independent of the voice.

    ``fade_out_s`` is the tail fade applied at the end of the mix. Music that
    stops dead on the last frame is the single most noticeable artefact of a
    naively muxed bed, and the fix is one filter, so it is not optional.
    """

    path: Path
    trim_db: float
    total_duration_s: float
    fade_out_s: float = 1.5


def _audio_graph(narration_index: int, music: MusicBed | None) -> tuple[str, str]:
    """Build the audio half of the filter graph, and name its output pad.

    With no music this is empty and the caller maps the narration stream
    directly, exactly as it did before music existed - a project with no bed
    produces the identical argument vector it always has.

    With music, three details are load-bearing and each is silent when wrong:

    ``normalize=0`` on ``amix``. ffmpeg's default is ``normalize=1``, which
    divides every input by the number of inputs - so simply adding a bed
    would halve the narration's volume, and the operator would hear a quieter
    voice and reasonably conclude the *music* setting was too loud. With
    normalisation off, ``volume`` is the only thing that changes a level, and
    it is applied to the music alone.

    ``duration=first``. The narration is the first input, so the mix ends
    when the voice ends rather than running to the end of a bed that may be
    minutes longer. This is what makes the caller's ``-stream_loop -1`` safe:
    the music repeats indefinitely so a short track still covers a long
    video, and this is the thing that stops it. Nothing here may pad the
    narration - ``apad`` would make the first input infinite too, and then
    the mix has no defined end at all and only ``-shortest`` saves it.

    The tail fade is computed against the *video's* duration, not the
    narration's. Those differ by design: the video ends at the last word
    (Task 7), while narration.mp3 can carry trailing silence past it, and
    ``-shortest`` cuts the mux at the video. Fading against the video is
    therefore fading against what someone actually watches.
    """
    if music is None:
        return "", f"{narration_index}:a"

    music_index = narration_index + 1
    fade_start = max(0.0, music.total_duration_s - music.fade_out_s)
    graph = (
        # loudnorm FIRST, then the trim. Normalising after the trim would put
        # the level back exactly where loudnorm wants it and the setting would
        # do nothing at all.
        f"[{music_index}:a]loudnorm=I={BED_TARGET_LUFS}:TP={_BED_TRUE_PEAK}:LRA=11,"
        f"volume={music.trim_db}dB,"
        f"afade=t=out:st={fade_start:.3f}:d={music.fade_out_s}[bed];"
        f"[{narration_index}:a][bed]"
        "amix=inputs=2:duration=first:dropout_transition=0:normalize=0[aout]"
    )
    return graph, "[aout]"


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
    music: MusicBed | None = None,
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
    if music is not None:
        # -stream_loop applies to the input that follows it, and must precede
        # that input's own -i. -1 is "repeat forever": a 30-second bed under a
        # three-minute video is the ordinary case, and the mix's
        # duration=first is what bounds it again.
        args += ["-stream_loop", "-1", "-i", str(music.path)]

    clip_count = len(clips)
    scale_pad = _scale_pad(width, height)
    per_clip = ";".join(f"[{i}:v]{scale_pad}[v{i}]" for i in range(clip_count))
    concat_inputs = "".join(f"[v{i}]" for i in range(clip_count))
    graph = (
        f"{per_clip};{concat_inputs}concat=n={clip_count}:v=1:a=0[vcat];[vcat]ass={ass_path}[vout]"
    )
    audio_graph, audio_map = _audio_graph(clip_count, music)
    if audio_graph:
        graph = f"{graph};{audio_graph}"

    args += [
        "-filter_complex",
        graph,
        "-map",
        "[vout]",
        "-map",
        audio_map,
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
