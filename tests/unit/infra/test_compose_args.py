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

from ytauto.infra.ffmpeg.compose import ComposeClip, MusicBed, compose_args


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


def test_an_absolute_ass_path_is_refused_outright(tmp_path: Path) -> None:
    """The defensive twin of the relative-path test above: compose_args does
    not merely happen to produce a colon-free graph when handed a relative
    path, it refuses an absolute one outright, so a caller that regresses
    this (passes ctx.workdir / "captions.ass" instead of a bare filename)
    fails loudly here rather than producing a silently broken filter graph.

    ``tmp_path / "captions.ass"`` rather than a literal ``Path("C:/work/...")``:
    the guard being tested is ``Path.is_absolute()``, and a hardcoded
    Windows-style string is only absolute *on Windows* -
    ``PureWindowsPath("C:/work/captions.ass").is_absolute()`` is ``True`` but
    ``PurePosixPath("C:/work/captions.ass").is_absolute()`` is ``False``, so on
    POSIX that string is an ordinary relative filename and the guard correctly
    does not fire, failing this test with ``DID NOT RAISE`` for a reason that
    has nothing to do with ``compose_args``. ``tmp_path`` is absolute on every
    platform pytest runs on, so this test now exercises the same property the
    production guard actually checks, everywhere."""
    with pytest.raises(ValueError, match="relative"):
        compose_args(
            clips=[_c("a.mp4", 0, 3)],
            ass_path=tmp_path / "captions.ass",
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


# -- the music bed ------------------------------------------------------------


def _with_music(gain_db: float = -18.0, total: float = 20.0, fade: float = 1.5) -> list[str]:
    return compose_args(
        clips=[_c("a.mp4", 0, 10), _c("b.mp4", 0, 10)],
        ass_path=Path("c.ass"),
        audio_path=Path("n.mp3"),
        out_path=Path("o.mp4"),
        width=1920,
        height=1080,
        encoder="libx264",
        music=MusicBed(
            path=Path("bed.mp3"), gain_db=gain_db, total_duration_s=total, fade_out_s=fade
        ),
    )


def test_no_music_produces_the_vector_that_existed_before_music() -> None:
    """The overwhelmingly common case must not pay for the feature: with no
    bed there is no audio filter graph at all and the narration stream is
    mapped straight through, exactly as it was."""
    args = compose_args(
        clips=[_c("a.mp4", 0, 3)],
        ass_path=Path("c.ass"),
        audio_path=Path("n.mp3"),
        out_path=Path("o.mp4"),
        width=1920,
        height=1080,
        encoder="libx264",
    )
    graph = args[args.index("-filter_complex") + 1]
    assert "amix" not in graph
    assert "volume=" not in graph
    assert "-stream_loop" not in args
    # One clip, so the narration is input 1 and is mapped as a bare stream
    # specifier rather than a filter pad.
    assert args[args.index("-map") + 3] == "1:a"


def test_the_mix_does_not_normalise_so_the_narration_keeps_its_level() -> None:
    """ffmpeg's amix defaults to normalize=1, which divides every input by the
    number of inputs - adding a bed would halve the voice, and the operator
    would blame the music setting. This is the assertion that pins it."""
    graph = _with_music()[_with_music().index("-filter_complex") + 1]
    assert "normalize=0" in graph


def test_the_bed_is_looped_and_the_mix_ends_with_the_narration() -> None:
    """A 30-second track under a three-minute video is ordinary, so the input
    repeats forever; duration=first is the only thing that bounds it."""
    args = _with_music()
    loop_at = args.index("-stream_loop")
    assert args[loop_at + 1] == "-1"
    # -stream_loop is an input option: it must precede the -i it applies to,
    # and that -i must be the music, not a clip.
    assert args[loop_at + 2] == "-i"
    assert args[loop_at + 3] == "bed.mp3"
    graph = args[args.index("-filter_complex") + 1]
    assert "duration=first" in graph


def test_gain_applies_to_the_bed_alone() -> None:
    """'Independent volume' means the voice is never touched."""
    graph = _with_music(gain_db=-24.0)[_with_music(gain_db=-24.0).index("-filter_complex") + 1]
    bed = graph.split("[bed]")[0]
    assert "volume=-24.0dB" in bed
    # The narration pad is consumed by amix directly, with no volume filter of
    # its own anywhere in the graph.
    assert graph.count("volume=") == 1


def test_the_tail_fade_is_timed_against_the_video_not_the_track() -> None:
    """The video ends at the last word and -shortest cuts there, so fading
    against anything else fades at a moment nobody sees."""
    graph = _with_music(total=20.0, fade=1.5)[
        _with_music(total=20.0, fade=1.5).index("-filter_complex") + 1
    ]
    assert "afade=t=out:st=18.500:d=1.5" in graph


def test_a_video_shorter_than_the_fade_does_not_seek_backwards() -> None:
    """A five-second short with a 1.5s fade is fine; a one-second one must not
    produce a negative start time, which ffmpeg rejects outright."""
    graph = _with_music(total=1.0, fade=1.5)[
        _with_music(total=1.0, fade=1.5).index("-filter_complex") + 1
    ]
    assert "st=0.000" in graph


def test_the_mapped_audio_is_the_mix_when_there_is_a_bed() -> None:
    args = _with_music()
    maps = [args[i + 1] for i, a in enumerate(args) if a == "-map"]
    assert maps == ["[vout]", "[aout]"]
