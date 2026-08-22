"""``render_ass``: the pure ``Timeline`` -> ASS document writer.

Zero dependencies beyond Task 7's own ``Timeline``/``CaptionGroup``/``Segment``
dataclasses, so - matching ``test_timeline.py``'s own reasoning - this earns
dense unit tests and no integration test: there is nothing here that needs a
real filesystem, a real ffmpeg, or a real clock.

``_group``/``_timeline``/``_style`` below are this file's own construction,
not given verbatim by the brief (the brief's pinned snippets call them as
``_timeline()``, ``_timeline(groups=[_group(words=[...])])``, ``_style()``,
and ``_style(accent="&H0000FFFF")``, so their keyword names and defaults had
to be chosen to make every one of those calls produce sensible input).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from ytauto.core.captions.ass import render_ass
from ytauto.core.pipeline.timeline import CaptionGroup, Segment, Timeline


def _group(*, words: Sequence[tuple[str, float, float]] | None = None) -> CaptionGroup:
    words = list(words) if words is not None else [("hello", 0.0, 0.5), ("world", 0.5, 1.0)]
    return CaptionGroup(start_s=words[0][1], end_s=words[-1][2], words=tuple(words))


def _timeline(*, groups: Sequence[CaptionGroup] | None = None) -> Timeline:
    group_list = list(groups) if groups is not None else [_group()]
    duration = group_list[-1].end_s if group_list else 0.0
    return Timeline(
        duration_s=duration,
        groups=tuple(group_list),
        segments=(Segment(start_s=0.0, end_s=duration),),
    )


def _style(*, accent: str = "&H0000FFFF") -> Mapping[str, object]:
    return {"accent_colour": accent}


# --- Step 1: the brief's own pinned tests, verbatim -------------------------


def test_the_header_carries_the_canvas_resolution() -> None:
    """PlayResX/Y wrong means every caption is mis-scaled on one of the two canvases."""
    ass = render_ass(_timeline(), width=1080, height=1920, style=_style())
    assert "PlayResX: 1080" in ass
    assert "PlayResY: 1920" in ass


def test_one_dialogue_event_per_word_not_per_group() -> None:
    tl = _timeline(groups=[_group(words=[("a", 0.0, 0.5), ("b", 0.5, 1.0), ("c", 1.0, 1.5)])])
    ass = render_ass(tl, width=1080, height=1920, style=_style())
    assert ass.count("\nDialogue:") == 3


def test_each_event_accents_exactly_one_word_and_shows_them_all() -> None:
    tl = _timeline(groups=[_group(words=[("alpha", 0.0, 0.5), ("beta", 0.5, 1.0)])])
    ass = render_ass(tl, width=1080, height=1920, style=_style(accent="&H0000FFFF"))
    lines = [ln for ln in ass.splitlines() if ln.startswith("Dialogue:")]

    assert len(lines) == 2
    for line in lines:
        assert line.count("&H0000FFFF") == 1, "exactly one word is accented per event"
        assert "alpha" in line and "beta" in line, "the whole group stays on screen"
    # The accent must move: event 0 accents alpha, event 1 accents beta.
    assert lines[0].index("&H0000FFFF") < lines[0].index("beta")
    assert lines[1].index("&H0000FFFF") > lines[1].index("alpha")


def test_event_timings_use_ass_centisecond_format() -> None:
    tl = _timeline(groups=[_group(words=[("a", 1.5, 2.25)])])
    line = [
        ln
        for ln in render_ass(tl, width=1080, height=1920, style=_style()).splitlines()
        if ln.startswith("Dialogue:")
    ][0]
    assert "0:00:01.50" in line
    assert "0:00:02.25" in line


# --- Step 5: the escaping guard - adapted from the brief, see the docstring -


def test_a_brace_in_the_narration_cannot_inject_an_override_tag() -> None:
    """An unescaped ``{`` turns caption text into an ASS override block.

    **Deviates from the brief's own Step-1 snippet for this guard.** The
    brief's snippet checks that the literal substring made of *two* raw
    backslashes immediately followed by the tag body does not survive into
    the output. That check is unsatisfiable by any *correctly* escaping
    implementation, not only a broken one: escaping a literal backslash for
    ASS the standard, information-preserving way is to double it, and
    doubling a source run of ``k >= 1`` backslashes always produces a run of
    ``2k >= 2`` backslashes - which itself always *contains* "two
    backslashes immediately before the tag body" as its own trailing
    substring. Confirmed directly (not just reasoned about) against this
    module's real ``_escape``, both against the brief's literal payload and
    a single-backslash payload, before concluding this is a defect in the
    brief rather than in the implementation - see this task's report for the
    reproduction.

    This test instead pins the actual safety property the brief's own prose
    describes: a raw, *unescaped* ASS colour-override (``{`` + one backslash
    + tag body + ``}``) must not survive verbatim into the rendered
    ``Dialogue`` text - the narration's own text is neutralised, not passed
    through unchanged.
    """
    tl = _timeline(groups=[_group(words=[("{\\c&HFF0000&}gotcha", 0.0, 0.5)])])
    ass = render_ass(tl, width=1080, height=1920, style=_style())
    dialogue_text = ass.split("Dialogue:")[1]
    assert "{\\c&HFF0000&}" not in dialogue_text, "the raw override block survived unescaped"
    assert "gotcha" in dialogue_text, "the word's own text must still render"


def test_a_lone_backslash_is_doubled_so_it_cannot_form_an_ass_escape() -> None:
    """A narrated word containing a literal backslash immediately followed
    by "N" - "back\\Nslash" - must not be misread as ASS's own "\\N"
    hard-newline escape, which is recognised even in plain text outside any
    override block. Narrower than the brace test above: this pins backslash
    escaping in isolation, with no brace involved at all. Escaping doubles
    the one original backslash, so the text always has an *even* number of
    backslashes immediately before "Nslash" in the output, never the single
    backslash that would trigger the newline escape.
    """
    tl = _timeline(groups=[_group(words=[("back\\Nslash", 0.0, 0.5)])])
    ass = render_ass(tl, width=1080, height=1920, style=_style())
    dialogue_text = ass.split("Dialogue:")[1]
    assert "back\\\\Nslash" in dialogue_text, "the one original backslash must survive, doubled"
    assert "back\\Nslash" not in dialogue_text, "the raw single-backslash form must not survive"


# --- extra coverage: style defaults ------------------------------------------


def test_missing_style_keys_fall_back_to_defaults_without_raising() -> None:
    """style is a rendering concern, not a validation boundary - an empty
    mapping must still produce a complete, valid-looking document."""
    ass = render_ass(_timeline(), width=1080, height=1920, style={})
    assert "[V4+ Styles]" in ass
    assert "Style: Default," in ass


def test_the_accent_colour_defaults_when_absent() -> None:
    """No caller-supplied accent still highlights exactly one word per event."""
    tl = _timeline(groups=[_group(words=[("alpha", 0.0, 0.5), ("beta", 0.5, 1.0)])])
    ass = render_ass(tl, width=1080, height=1920, style={})
    lines = [ln for ln in ass.splitlines() if ln.startswith("Dialogue:")]
    assert lines[0].count("\\c") == 2, "one accent-open and one reset-to-primary override"


def test_a_style_line_uses_the_same_style_name_the_dialogue_lines_reference() -> None:
    tl = _timeline(groups=[_group(words=[("a", 0.0, 0.5), ("b", 0.5, 1.0)])])
    ass = render_ass(tl, width=1080, height=1920, style=_style())
    assert "Style: Default," in ass
    dialogue_lines = [ln for ln in ass.splitlines() if ln.startswith("Dialogue:")]
    for line in dialogue_lines:
        # Format: Layer,Start,End,Style,Name,MarginL,MarginR,MarginV,Effect,Text
        assert line.split(",")[3] == "Default"


# --- extra coverage: timestamp formatting ------------------------------------


def test_a_float_noise_near_integer_centisecond_does_not_truncate_short() -> None:
    """0.29 * 100 == 28.999999999999996 in float64 - a bare int() truncates
    that to 28 centiseconds, one short of the intended 29. This is float
    representation noise, not a genuine fractional value, and must not
    silently shift every caption a centisecond early."""
    tl = _timeline(groups=[_group(words=[("w", 0.29, 0.29)])])
    line = [
        ln
        for ln in render_ass(tl, width=1080, height=1920, style=_style()).splitlines()
        if ln.startswith("Dialogue:")
    ][0]
    assert "0:00:00.29" in line


def test_a_genuinely_fractional_centisecond_still_truncates_down() -> None:
    """0.297s is 29.7 centiseconds - genuinely fractional, not float noise -
    and must truncate to 29, not round up to 30."""
    tl = _timeline(groups=[_group(words=[("w", 0.297, 0.297)])])
    line = [
        ln
        for ln in render_ass(tl, width=1080, height=1920, style=_style()).splitlines()
        if ln.startswith("Dialogue:")
    ][0]
    assert "0:00:00.29" in line
    assert "0:00:00.30" not in line


def test_a_genuine_value_near_a_centisecond_boundary_does_not_round_up() -> None:
    """1.499999996s is genuinely 0.000000004s short of 1.50s - not float64
    noise (that's ~1e-13, this gap is ~4e-9) - and must still truncate down
    to 0:00:01.49, not up to 0:00:01.50. A too-wide epsilon (this guard used
    to use ``round(seconds * 100, 6)``, which snaps anything within 5e-7 of
    an integer) would round this up, landing exactly on the
    late-and-clips-the-word failure the truncation rule exists to prevent."""
    tl = _timeline(groups=[_group(words=[("w", 1.499999996, 1.499999996)])])
    line = [
        ln
        for ln in render_ass(tl, width=1080, height=1920, style=_style()).splitlines()
        if ln.startswith("Dialogue:")
    ][0]
    assert "0:00:01.49" in line
    assert "0:00:01.50" not in line


def test_timestamps_past_a_minute_carry_into_minutes_and_hours() -> None:
    tl = _timeline(groups=[_group(words=[("w", 3661.5, 3661.5)])])
    line = [
        ln
        for ln in render_ass(tl, width=1080, height=1920, style=_style()).splitlines()
        if ln.startswith("Dialogue:")
    ][0]
    assert "1:01:01.50" in line


# --- extra coverage: multiple groups ------------------------------------------


def test_dialogue_events_accumulate_across_multiple_groups() -> None:
    tl = _timeline(
        groups=[
            _group(words=[("a", 0.0, 0.5), ("b", 0.5, 1.0)]),
            _group(words=[("c", 1.0, 1.5), ("d", 1.5, 2.0), ("e", 2.0, 2.5)]),
        ]
    )
    ass = render_ass(tl, width=1080, height=1920, style=_style())
    assert ass.count("\nDialogue:") == 5


# --- Defect 1: captions must never go blank between words -------------------
#
# Real narration has pauses: a word's own end_s is well short of the next
# word's start_s (silence, breath, sentence break). The original
# implementation gave every event exactly its own word's [start_s, end_s) -
# so every such pause was a frame with no caption at all. The fix tiles each
# event's end to the *next* event's own start, so consecutive events abut
# with zero gap, and the last event of the whole timeline reaches
# timeline.duration_s.


def _dialogue_lines(ass: str) -> list[str]:
    return [ln for ln in ass.splitlines() if ln.startswith("Dialogue:")]


def _start_end(line: str) -> tuple[str, str]:
    # Format: Layer,Start,End,Style,Name,MarginL,MarginR,MarginV,Effect,Text
    fields = line.split(",")
    return fields[1], fields[2]


def _gappy_timeline() -> Timeline:
    """Two groups, each with an internal pause between its words, plus a
    pause between the two groups, plus trailing silence after the last word
    out to duration_s - every kind of gap Defect 1 covers, in one timeline.
    """
    groups = (
        CaptionGroup(
            start_s=0.0,
            end_s=0.9,
            words=(("hello", 0.0, 0.4), ("there", 0.6, 0.9)),  # 0.4->0.6 gap
        ),
        CaptionGroup(
            start_s=1.5,
            end_s=2.4,
            words=(("my", 1.5, 1.8), ("friend", 2.0, 2.4)),  # 1.8->2.0 gap
        ),  # 0.9 -> 1.5 is the inter-group gap
    )
    return Timeline(
        duration_s=3.0,  # trailing silence: 2.4 -> 3.0
        groups=groups,
        segments=(Segment(start_s=0.0, end_s=3.0),),
    )


def test_no_blank_interval_anywhere_consecutive_events_abut() -> None:
    """For a multi-group timeline riddled with pauses, every event's end
    must equal the next event's start - no gap, anywhere - and the very
    last event must reach duration_s exactly."""
    tl = _gappy_timeline()
    lines = _dialogue_lines(render_ass(tl, width=1080, height=1920, style=_style()))
    assert len(lines) == 4  # one event per word: hello, there, my, friend

    pairs = [_start_end(line) for line in lines]
    for (_start, end), (next_start, _next_end) in zip(pairs, pairs[1:], strict=False):
        assert end == next_start, "an event's end must equal the next event's start"

    # The last event reaches the timeline's own duration, not its word's end_s.
    assert pairs[-1][1] == "0:00:03.00"


def test_the_accent_still_advances_on_each_words_own_start() -> None:
    """Tiling changes only event *ends* - the accent must still switch onto
    each word at that word's own start_s, unaffected by how far the event's
    end was stretched."""
    tl = _gappy_timeline()
    lines = _dialogue_lines(render_ass(tl, width=1080, height=1920, style=_style()))
    starts = [_start_end(line)[0] for line in lines]
    assert starts == ["0:00:00.00", "0:00:00.60", "0:00:01.50", "0:00:02.00"]


def test_guard_pin_word_tight_ends_reintroduce_the_blank_gap() -> None:
    """Guard-pin: revert render_ass to the original word-tight event ends
    (event end = that word's own end_s) and confirm the no-blank test fails
    for the *right* reason - a specific gap between "hello"'s event (ending
    0:00:00.40) and "there"'s event (starting 0:00:00.60), not some
    unrelated error.

    Prediction made before running: with word-tight ends, event 0 ("hello")
    ends at 0:00:00.40 while event 1 ("there") starts at 0:00:00.60 - the
    two do not abut, so the ``end == next_start`` assertion fails on that
    exact pair.
    """
    tl = _gappy_timeline()
    accent_colour = "&H0000FFFF"
    primary_colour = "&H00FFFFFF"

    # Reimplements the pre-fix behaviour directly (does not call render_ass,
    # which no longer has it) so the guard exercises the same tiling logic
    # this file's other tests assert against, on the same input.
    from ytauto.core.captions.ass import _render_words

    lines = []
    for group in tl.groups:
        for index, (_text, start_s, end_s) in enumerate(group.words):
            text = _render_words(group.words, index, accent_colour, primary_colour)
            lines.append(f"Dialogue: 0,{start_s:.2f},{end_s:.2f},Default,,0,0,0,,{text}")

    pairs = [(ln.split(",")[1], ln.split(",")[2]) for ln in lines]
    failures = [
        (end, next_start)
        for (_start, end), (next_start, _next_end) in zip(pairs, pairs[1:], strict=False)
        if end != next_start
    ]
    assert failures, "word-tight ends were expected to reintroduce at least one blank gap"
    # The specific gap predicted above: hello's event ends 0.40, there's starts 0.60.
    assert failures[0] == ("0.40", "0.60")


# --- Defect 2: alignment ------------------------------------------------------


def test_alignment_defaults_to_middle_centre() -> None:
    ass = render_ass(_timeline(), width=1080, height=1920, style=_style())
    style_line = [ln for ln in ass.splitlines() if ln.startswith("Style: Default,")][0]
    fields = style_line.removeprefix("Style: ").split(",")
    # Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour,
    # OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX,
    # ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, ...
    alignment_index = 18
    assert fields[alignment_index] == "5"


def test_alignment_is_overridable_via_caption_style() -> None:
    style: Mapping[str, object] = {"accent_colour": "&H0000FFFF", "alignment": 2}
    ass = render_ass(_timeline(), width=1080, height=1920, style=style)
    style_line = [ln for ln in ass.splitlines() if ln.startswith("Style: Default,")][0]
    fields = style_line.removeprefix("Style: ").split(",")
    assert fields[18] == "2"
