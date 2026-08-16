"""``plan_timeline``: the pure caption-grouping and B-roll segmentation core.

Zero dependencies, so this earns the densest tests in the phase and no
integration test at all (see ``core.pipeline.timeline``'s own module
docstring). Every test here calls ``plan_timeline`` directly - no ``CasStore``,
no ``JobContext``, no fake provider - because there is nothing to fake.

``_template`` below is this file's own construction, not given verbatim by
the brief: the brief's pinned test snippets call it as
``_template(words_max=5)``, ``_template(seg_min=3.0, seg_max=5.0)``, and
``_template()``, so its keyword names and defaults had to be chosen to make
every one of those calls produce a ``template`` mapping carrying exactly the
keys ``plan_timeline`` reads: ``words_per_group_max``, ``segment_seconds_min``,
``segment_seconds_max`` (``words_per_group_min`` is included too, for
fidelity to the real settings shape, even though ``plan_timeline`` never
reads it - see that module's docstring for why). Defaults were chosen loose
enough (``words_max=8``, ``seg_min=4.0``, ``seg_max=8.0``) that no pinned
test's expected grouping/segmentation is driven by a default the brief never
specified.
"""

from __future__ import annotations

from collections.abc import Mapping

import pytest

from ytauto.core.pipeline.timeline import plan_timeline


def _template(
    *,
    words_min: int = 2,
    words_max: int = 8,
    seg_min: float = 4.0,
    seg_max: float = 8.0,
) -> Mapping[str, object]:
    return {
        "words_per_group_min": words_min,
        "words_per_group_max": words_max,
        "segment_seconds_min": seg_min,
        "segment_seconds_max": seg_max,
    }


# --- Step 1: grouping -------------------------------------------------------


def test_a_group_closes_at_the_maximum_word_count() -> None:
    words = [(f"w{i}", i * 1.0, i * 1.0 + 0.5) for i in range(10)]
    tl = plan_timeline(words, 10.0, _template(words_max=5), seed=1)
    assert [len(g.words) for g in tl.groups] == [5, 5]


def test_a_group_closes_early_on_sentence_ending_punctuation() -> None:
    """A caption must not run across a full stop even when it is under the word cap."""
    words = [("The", 0.0, 0.3), ("train.", 0.3, 0.8), ("It", 0.8, 1.0), ("left.", 1.0, 1.4)]
    tl = plan_timeline(words, 1.4, _template(words_max=5), seed=1)
    assert [len(g.words) for g in tl.groups] == [2, 2]


def test_a_group_spans_its_first_word_start_to_its_last_word_end() -> None:
    words = [("a", 0.2, 0.4), ("b", 0.5, 0.9)]
    tl = plan_timeline(words, 1.0, _template(words_max=5), seed=1)
    assert (tl.groups[0].start_s, tl.groups[0].end_s) == (0.2, 0.9)


def test_every_word_appears_exactly_once_across_all_groups() -> None:
    """Grouping must partition, not sample - a dropped word is a missing caption."""
    words = [(f"w{i}", i * 0.5, i * 0.5 + 0.4) for i in range(23)]
    tl = plan_timeline(words, 12.0, _template(words_max=4), seed=1)
    flat = [w[0] for g in tl.groups for w in g.words]
    assert flat == [w[0] for w in words]


# --- Step 5: segmentation ---------------------------------------------------


def test_a_segment_boundary_always_lands_on_a_group_boundary() -> None:
    """A B-roll cut mid-phrase is the artefact this rule exists to prevent."""
    words = [(f"w{i}", i * 0.4, i * 0.4 + 0.35) for i in range(60)]
    tl = plan_timeline(words, 24.0, _template(words_max=4, seg_min=3.0, seg_max=5.0), seed=1)
    group_edges = {g.start_s for g in tl.groups} | {g.end_s for g in tl.groups}
    for seg in tl.segments:
        assert seg.start_s in group_edges or seg.start_s == 0.0
        assert seg.end_s in group_edges or seg.end_s == tl.duration_s


def test_segments_tile_the_whole_duration_without_gap_or_overlap() -> None:
    """A gap is a black frame; an overlap is a dropped clip."""
    words = [(f"w{i}", i * 0.4, i * 0.4 + 0.35) for i in range(60)]
    tl = plan_timeline(words, 24.0, _template(seg_min=3.0, seg_max=5.0), seed=1)
    assert tl.segments[0].start_s == 0.0
    assert tl.segments[-1].end_s == pytest.approx(24.0)
    for prev, nxt in zip(tl.segments, tl.segments[1:], strict=False):
        assert prev.end_s == nxt.start_s


def test_the_same_seed_produces_an_identical_timeline() -> None:
    """An unstable timeline silently disables every downstream cache."""
    words = [(f"w{i}", i * 0.4, i * 0.4 + 0.35) for i in range(40)]
    a = plan_timeline(words, 16.0, _template(), seed=99)
    b = plan_timeline(words, 16.0, _template(), seed=99)
    assert a == b


# --- Step 8: degenerate inputs ----------------------------------------------


def test_a_single_word_produces_one_group_and_one_segment() -> None:
    tl = plan_timeline([("alone", 0.0, 0.8)], 0.8, _template(), seed=1)
    assert len(tl.groups) == 1
    assert len(tl.segments) == 1
    assert tl.segments[0].end_s == pytest.approx(0.8)


def test_no_words_produces_no_groups_but_still_covers_the_duration() -> None:
    """Silence is legal input. A segment list that does not reach duration_s
    leaves the tail of the video black."""
    tl = plan_timeline([], 4.0, _template(), seed=1)
    assert tl.groups == ()
    assert tl.segments[0].start_s == 0.0
    assert tl.segments[-1].end_s == pytest.approx(4.0)


def test_audio_longer_than_the_last_word_still_tiles_to_the_end() -> None:
    """Pins the pure function against an ``audio_duration_s`` longer than the
    last word's own end - which is exactly what edge-tts's trailing silence
    would look like *if* something supplied it.

    Nothing in the current pipeline does: ``PlanTimeline.run`` derives
    ``audio_duration_s`` as the last word's own ``end_s`` (see
    ``app/stages/plan_timeline.py``'s ``PlanTimeline`` docstring and
    ``tests/unit/app/stages/test_plan_timeline.py::test_run_derives_audio_duration_from_the_last_words_end``,
    which pins that derivation), so the stage can never actually pass a
    longer value in production, and the render step composites with
    ``-shortest`` besides, which fails toward a truncated narration tail
    rather than a black frame either way. This test exists so the day a real
    probed duration replaces that stand-in, `plan_timeline` is already
    proven correct for it - not because trailing silence is handled
    end-to-end today; it is not."""
    tl = plan_timeline([("word", 0.0, 0.5)], 6.0, _template(), seed=1)
    assert tl.segments[-1].end_s == pytest.approx(6.0)


def test_a_zero_length_word_does_not_produce_an_inverted_group() -> None:
    """A group whose end precedes its start makes ffmpeg's ass filter drop the
    event silently - a caption that never appears, with nothing failing."""
    tl = plan_timeline([("a", 1.0, 1.0), ("b", 1.0, 1.4)], 2.0, _template(), seed=1)
    for group in tl.groups:
        assert group.end_s >= group.start_s


# --- review fix round: duration_s shorter than the word timings imply ------


def test_a_short_duration_cannot_invert_or_overrun_the_final_segment() -> None:
    """Unreachable today - ``PlanTimeline.run`` derives ``audio_duration_s``
    as the last word's own ``end_s``, so the stage can never pass a shorter
    ``duration_s`` than the words it read (see ``PlanTimeline``'s own
    docstring and ``test_audio_longer_than_the_last_word_still_tiles_to_the_end``
    above). But this guards the pure function itself: a probed duration
    that eventually replaces that stand-in can legitimately come back
    shorter than what the word timings imply, and every other pinned test's
    ``duration_s`` is at least as long as its words describe, so none of
    them could have caught this.

    Before the fix, ``boundaries[-1] = duration_s`` naively overwrote only
    the literal last boundary: with ``duration_s`` shorter than several
    groups' own ends, that produced an inverted final segment (``start_s``
    greater than ``end_s``) while every segment between the true duration
    and the old final boundary silently overran it - both wrong, and both
    checked here directly rather than only through the tiling invariant
    ``test_segments_tile_the_whole_duration_without_gap_or_overlap`` already
    covers for well-behaved input.

    ``inverted``/``overrun`` are collected over the *whole* segment list
    before either is asserted on, rather than checked inside one
    short-circuiting loop: boundaries are monotonic, so in this specific
    scenario an inversion at the tail is never reachable without an earlier
    segment already overrunning - a per-segment loop that asserted both
    conditions together would always fail on the earlier overrun first and
    never actually exercise the inversion check. Keeping them as two
    independent whole-list assertions means both invariants are genuinely
    checked, not just the one that happens to trip first."""
    words = [(f"w{i}", i * 0.4, i * 0.4 + 0.35) for i in range(60)]
    tl = plan_timeline(words, 2.0, _template(seg_min=3.0, seg_max=5.0), seed=1)

    inverted = [s for s in tl.segments if s.start_s > s.end_s]
    overrun = [s for s in tl.segments if s.end_s > 2.0]
    assert inverted == [], f"inverted segment(s): {inverted}"
    assert overrun == [], f"segment(s) overran duration_s: {overrun}"
    assert tl.segments[0].start_s == 0.0
    assert tl.segments[-1].end_s == pytest.approx(2.0)
    for prev, nxt in zip(tl.segments, tl.segments[1:], strict=False):
        assert prev.end_s == nxt.start_s


# --- words_per_group_min is advisory: additional pin ------------------------


def test_words_per_group_min_never_forces_a_merge_across_a_sentence_end() -> None:
    """The brief resolves this ambiguity explicitly: a trailing group of one
    word at the end of a sentence is correct output, not a bug to smooth
    over - even when words_per_group_min asks for more than that.

    ``words_max`` is set high (10) so the word cap can never explain the
    early close; only the sentence-ending period on "Stop." can.
    """
    words = [("Wait.", 0.0, 0.3), ("Stop.", 0.3, 0.6), ("Go", 0.6, 0.8)]
    tl = plan_timeline(words, 0.8, _template(words_min=5, words_max=10), seed=1)
    assert [len(g.words) for g in tl.groups] == [1, 1, 1]


# --- sentence-ending punctuation: quotes and brackets ------------------------


def test_sentence_end_is_recognised_after_a_trailing_quote() -> None:
    """'he said."' ends a sentence - the brief's own example, verbatim."""
    words = [("he", 0.0, 0.2), ('said."', 0.2, 0.5), ("Next", 0.5, 0.7)]
    tl = plan_timeline(words, 0.7, _template(words_max=10), seed=1)
    assert [len(g.words) for g in tl.groups] == [2, 1]
