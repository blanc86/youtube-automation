"""Unit tests for ``infra.ffmpeg.compose.compose_args``.

Pure argument-vector construction, tested with no real ffmpeg binary and no
subprocess anywhere - the graph-shape assertions below are the same four the
task brief pins verbatim, plus a guard-pin (Step 8) proving the ``-ss``
mutation fails for the *predicted* reason, and two of my own covering the
``ass_path``/empty-``clips`` guards ``compose_args`` raises on directly.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ytauto.infra.ffmpeg.compose import ComposeClip, compose_args


def _c(path: str, in_point_s: float, duration_s: float) -> ComposeClip:
    return ComposeClip(path=Path(path), in_point_s=in_point_s, duration_s=duration_s)


def test_the_graph_concatenates_then_burns_captions_in_one_pass() -> None:
    """Writing an intermediate file is the dominant cause of slow renders."""
    args = compose_args(
        clips=[_c("a.mp4", 0, 3), _c("b.mp4", 2, 3)],
        ass_path=Path("c.ass"),
        audio_path=Path("n.mp3"),
        out_path=Path("o.mp4"),
        width=1920,
        height=1080,
        encoder="h264_nvenc",
    )
    graph = args[args.index("-filter_complex") + 1]
    assert "concat=n=2:v=1:a=0" in graph
    assert "ass=" in graph
    assert graph.index("concat") < graph.index("ass"), "captions burn after the concat"


def test_each_segment_is_trimmed_at_its_own_in_point() -> None:
    args = compose_args(
        clips=[_c("a.mp4", 4.5, 3.0)],
        ass_path=Path("c.ass"),
        audio_path=Path("n.mp3"),
        out_path=Path("o.mp4"),
        width=1920,
        height=1080,
        encoder="h264_nvenc",
    )
    assert args[args.index("-ss") + 1] == "4.5"
    assert args[args.index("-t") + 1] == "3.0"


def test_segment_k_gets_segment_k_s_in_point_not_the_first_segment_s() -> None:
    """The exact off-by-one the brief cared most about (Task 11 review,
    Important #1). The single-clip test above can only ever find the
    *first* "-ss" via ``args.index(...)``, so nothing in the suite
    distinguishes "each clip trimmed at its own in-point" from "every clip
    trimmed at the first clip's" - the code is correct, but was unpinned.

    Three clips at distinct in-points and durations, parsed into
    ``(-ss, -t, -i)`` triples in order and matched against their own clip -
    the stronger assertion the brief's own note already called for, rather
    than a positional ``.index()`` lookup.
    """
    clips = [
        _c("a.mp4", 1.0, 2.0),
        _c("b.mp4", 5.5, 3.25),
        _c("c.mp4", 10.0, 1.5),
    ]
    args = compose_args(
        clips=clips,
        ass_path=Path("c.ass"),
        audio_path=Path("n.mp3"),
        out_path=Path("o.mp4"),
        width=1920,
        height=1080,
        encoder="h264_nvenc",
    )

    triples: list[tuple[str, str, str]] = []
    i = args.index("-ss")
    while i < len(args) and args[i] == "-ss":
        assert args[i + 2] == "-t"
        assert args[i + 4] == "-i"
        triples.append((args[i + 1], args[i + 3], args[i + 5]))
        i += 6

    assert triples == [
        ("1.0", "2.0", "a.mp4"),
        ("5.5", "3.25", "b.mp4"),
        ("10.0", "1.5", "c.mp4"),
    ], "each clip must be trimmed at its own in-point/duration, in its own order"


def test_the_ass_path_is_relative_so_a_windows_drive_letter_cannot_break_the_filter() -> None:
    """ffmpeg filter syntax treats ':' as an argument separator, so 'C:\\x' breaks the graph."""
    args = compose_args(
        clips=[_c("a.mp4", 0, 3)],
        ass_path=Path("captions.ass"),
        audio_path=Path("n.mp3"),
        out_path=Path("o.mp4"),
        width=1920,
        height=1080,
        encoder="h264_nvenc",
    )
    graph = args[args.index("-filter_complex") + 1]
    assert ":" not in graph.split("ass=")[1].split("[")[0]


def test_the_output_is_cut_to_the_narration_length() -> None:
    args = compose_args(
        clips=[_c("a.mp4", 0, 3)],
        ass_path=Path("c.ass"),
        audio_path=Path("n.mp3"),
        out_path=Path("o.mp4"),
        width=1920,
        height=1080,
        encoder="h264_nvenc",
    )
    assert "-shortest" in args


# -- coverage beyond the brief's four: the two guards compose_args raises on ------


def test_an_absolute_ass_path_is_refused_outright() -> None:
    """The defensive twin of the relative-path test above: compose_args does
    not merely happen to produce a colon-free graph when handed a relative
    path, it refuses an absolute one outright, so a caller that regresses
    this (passes ctx.workdir / "captions.ass" instead of a bare filename)
    fails loudly here rather than producing a silently broken filter graph."""
    with pytest.raises(ValueError, match="relative"):
        compose_args(
            clips=[_c("a.mp4", 0, 3)],
            ass_path=Path("C:/work/captions.ass"),
            audio_path=Path("n.mp3"),
            out_path=Path("o.mp4"),
            width=1920,
            height=1080,
            encoder="h264_nvenc",
        )


def test_no_clips_is_refused_rather_than_building_an_empty_concat() -> None:
    with pytest.raises(ValueError, match="clip"):
        compose_args(
            clips=[],
            ass_path=Path("c.ass"),
            audio_path=Path("n.mp3"),
            out_path=Path("o.mp4"),
            width=1920,
            height=1080,
            encoder="h264_nvenc",
        )


# -- Step 8: guard-pin the trim ---------------------------------------------
#
# Not an automated test: pytest has no way to mutate production source and
# assert on the *next* test run from inside the same run. This was performed
# by hand - see the task report for the exact patch applied to compose_args
# (deleting the "-ss", str(float(clip.in_point_s)) pair from the per-clip
# loop) and the verbatim failure it produced. Recorded here as a comment so
# the guard's existence and its predicted failure mode are next to the test
# it protects, per this task's "beware tests that look like coverage" note.
