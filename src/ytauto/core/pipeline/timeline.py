"""``plan_timeline``: the pure caption-grouping and B-roll segmentation core.

Every visible timing decision in the finished video traces back to this
module: which words share one on-screen caption, and where a B-roll cut
lands. Deliberately **stdlib-only** - not one import from elsewhere in
``ytauto`` - because a pure function of its own arguments (no I/O, no clock,
no randomness that actually matters; see ``seed``'s note on ``plan_timeline``
below) is exhaustively testable in milliseconds, which is exactly what this
task's test file leans on. ``app/stages/plan_timeline.py`` is the thin
artifact-reading/writing wrapper around this; nothing here knows a
``CasStore`` or a ``JobContext`` exists.

Two decisions happen in sequence, never mixed:

1. **Grouping** (``_group_words``) turns the flat ``word_timings`` sequence
   into ``CaptionGroup`` objects, one caption's worth of words each. A group
   closes when it reaches ``words_per_group_max`` words, or when its latest
   word ends a sentence - whichever comes first. ``words_per_group_min`` is
   read nowhere in this module: see its own paragraph below.
2. **Segmentation** (``_plan_segments``) walks the finished groups and packs
   them into ``Segment`` spans for B-roll cuts, targeting
   ``segment_seconds_min``/``_max``. A segment boundary is always a group's
   ``end_s`` (or the timeline's own ``0.0``/``duration_s`` at the very
   ends) - never a timestamp that falls inside a caption - which is what
   keeps a B-roll cut from ever landing mid-phrase.

**On ``words_per_group_min``:** it is part of ``PlanTimeline.settings_keys``
(so changing it still invalidates the cache, per that stage's own contract)
but this module never reads it. The brief's own resolved ambiguity says
plainly that it must never force a sentence to split, and must never merge
groups to reach it - and Step 3's grouping rule (close on
max-reached-or-sentence-end, nothing else) leaves no other decision in the
algorithm it could influence without violating one of those two rules. This
was checked, not assumed - ``test_a_group_closes_at_the_maximum_word_count``
passes a ``words_per_group_max`` of 5 against a default ``words_per_group_min``
and still gets full 5-word groups, which would be wrong were ``min`` doing
anything. See this task's report for the full note.

**On ``seed``:** likewise threaded through ``plan_timeline``'s signature and
into ``PlanTimeline.settings_keys`` (a seed change must still invalidate the
cache), but not consulted anywhere below. Grouping closes deterministically
on the *first* qualifying condition; segmentation closes at the *first*
group boundary that reaches ``segment_seconds_min``, or backs off to the
*previous* one if that would exceed ``segment_seconds_max`` - both fully
determined by ``word_timings``/``template`` alone, with no point in either
algorithm that is genuinely a coin flip. Manufacturing an artificial use of
``random.Random(seed)`` that could not change the output would be worse than
leaving it unconsumed: see this task's report.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

_SENTENCE_TERMINAL: tuple[str, ...] = (".", "!", "?", "…")
"""Terminal punctuation, per the brief - literally ``.``, ``!``, ``?``, and
the single-character ellipsis ``…``. No abbreviation detection (``Mr.``
etc.): the brief calls that out of scope, and it would need a dictionary."""

_TRAILING_STRIP = "\"'‘’“”)]}"
"""Trailing quote/bracket characters stripped before the terminal check, so
``he said."`` (a straight double quote after the period) is recognised as
ending a sentence: stripping the trailing ``"`` leaves ``he said.``, whose
last character is a terminal. Covers straight and curly quotes and every
common closing bracket - the brief names the category ("quotes and closing
brackets"), not an exhaustive character list."""


def _ends_sentence(word: str) -> bool:
    """Whether ``word`` ends a sentence, per the brief's Step 3 rule: check
    after stripping trailing whitespace, then trailing quote/bracket
    characters."""
    stripped = word.rstrip().rstrip(_TRAILING_STRIP)
    return stripped.endswith(_SENTENCE_TERMINAL)


@dataclass(frozen=True)
class CaptionGroup:
    """One on-screen caption's words, spanning its first word's start to its
    last word's end."""

    start_s: float
    end_s: float
    words: tuple[tuple[str, float, float], ...]


@dataclass(frozen=True)
class Segment:
    """One B-roll cut's span. Its boundaries always land on a
    ``CaptionGroup`` edge (or the timeline's own start/end) - never mid-caption."""

    start_s: float
    end_s: float


@dataclass(frozen=True)
class Timeline:
    """The complete edit: every caption group and every B-roll segment. The
    segments tile ``[0.0, duration_s]`` exactly - no gap, no overlap."""

    duration_s: float
    groups: tuple[CaptionGroup, ...]
    segments: tuple[Segment, ...]


def _close_group(words: list[tuple[str, float, float]]) -> CaptionGroup:
    """Build a ``CaptionGroup`` from an in-progress word buffer.

    ``start_s``/``end_s`` come from the first and last word's own timestamps,
    never from summing durations - so a zero-length word in the middle of a
    group (a real edge-tts artefact; see
    ``test_a_zero_length_word_does_not_produce_an_inverted_group``) can never
    push ``end_s`` below ``start_s``, as long as the words arrive in
    chronological order, which ``word_timings.json`` always does.
    """
    return CaptionGroup(start_s=words[0][1], end_s=words[-1][2], words=tuple(words))


def _group_words(
    word_timings: Sequence[tuple[str, float, float]], *, words_per_group_max: int
) -> tuple[CaptionGroup, ...]:
    """Partition every word into ``CaptionGroup``s, in order. Never drops,
    duplicates, or reorders a word - see
    ``test_every_word_appears_exactly_once_across_all_groups``.

    A group closes - and a new one starts - the moment it reaches
    ``words_per_group_max`` words, or its most recently added word ends a
    sentence. ``words_per_group_min`` plays no part; see this module's
    docstring.
    """
    groups: list[CaptionGroup] = []
    current: list[tuple[str, float, float]] = []
    for word in word_timings:
        current.append(word)
        if len(current) >= words_per_group_max or _ends_sentence(word[0]):
            groups.append(_close_group(current))
            current = []
    if current:
        groups.append(_close_group(current))
    return tuple(groups)


def _find_segment_close(
    groups: tuple[CaptionGroup, ...],
    start_index: int,
    seg_start: float,
    segment_seconds_min: float,
    segment_seconds_max: float,
) -> int:
    """The index of the group the current segment closes at.

    Scans forward from ``start_index``, accumulating span (``seg_start`` to
    each candidate group's own ``end_s``) until it reaches
    ``segment_seconds_min``, then closes there - unless doing so would
    exceed ``segment_seconds_max``, in which case it backs off to the
    *previous* group's boundary instead (Step 6's rule, verbatim). If
    backing off would leave the segment empty - the very first group already
    exceeds ``segment_seconds_max`` on its own - there is no better choice
    than including it anyway; an empty segment is not a legal output.

    If no remaining group's cumulative span ever reaches
    ``segment_seconds_min`` (the tail of the input), closes at the last
    available group instead; the caller extends that boundary out to
    ``duration_s``.
    """
    total = len(groups)
    index = start_index
    while index < total:
        span = groups[index].end_s - seg_start
        if span >= segment_seconds_min:
            if span <= segment_seconds_max:
                return index
            return index - 1 if index > start_index else index
        index += 1
    return total - 1


def _plan_segments(
    groups: tuple[CaptionGroup, ...],
    duration_s: float,
    *,
    segment_seconds_min: float,
    segment_seconds_max: float,
) -> tuple[Segment, ...]:
    """Pack ``groups`` into contiguous ``Segment``s covering
    ``[0.0, duration_s]`` with no gap or overlap. See ``_find_segment_close``
    for the per-segment closing rule.

    Silence (no groups at all) is legal input: it still produces exactly one
    segment spanning the whole duration, so the tail of the video is never
    left uncovered.
    """
    if not groups:
        return (Segment(start_s=0.0, end_s=duration_s),)

    boundaries: list[float] = []
    seg_start = 0.0
    index = 0
    total = len(groups)
    while index < total:
        close_at = _find_segment_close(
            groups, index, seg_start, segment_seconds_min, segment_seconds_max
        )
        boundary = groups[close_at].end_s
        boundaries.append(boundary)
        seg_start = boundary
        index = close_at + 1
    # The last group's own end may fall short of duration_s - edge-tts pads
    # trailing silence past the last word - so the final segment always
    # extends to cover it, or the last B-roll clip would end early and the
    # video would go black before the audio does.
    #
    # A naive `boundaries[-1] = duration_s` is only correct when every
    # existing boundary is already <= duration_s. It is not correct in
    # general: a duration_s shorter than an earlier group's own end -
    # unreachable while audio_duration_s is derived from the last word's
    # end (see PlanTimeline's own docstring), but live the moment a real
    # probed duration replaces that stand-in - would silently invert the
    # final segment (its start_s, an earlier group's end, would exceed the
    # overwritten end_s) while every segment between duration_s and the
    # old final boundary quietly overran the declared duration. Instead:
    # find the first boundary that would reach or exceed duration_s, drop
    # it and everything after, and close there at duration_s exactly. When
    # no boundary reaches duration_s (the ordinary case, including trailing
    # silence past the last word), this reduces to replacing the last
    # element, so segment count is unaffected by this guard.
    overrun_at = next((i for i, b in enumerate(boundaries) if b >= duration_s), None)
    if overrun_at is None:
        boundaries[-1] = duration_s
    else:
        boundaries = [*boundaries[:overrun_at], duration_s]

    segments: list[Segment] = []
    start = 0.0
    for end in boundaries:
        segments.append(Segment(start_s=start, end_s=end))
        start = end
    return tuple(segments)


def _require_int(template: Mapping[str, object], key: str) -> int:
    """Narrow one ``template`` value to ``int``, ``bool`` excluded.

    ``bool`` is a subclass of ``int`` in Python, so ``isinstance(True, int)``
    is true; excluding it explicitly stops a stray ``True``/``False`` from
    silently acting as a word-count cap of ``1``/``0``.

    Raises:
        TypeError: ``template[key]`` is missing or not an ``int``.
    """
    value = template[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"template[{key!r}] must be an int, got {type(value).__name__}")
    return value


def _require_float(template: Mapping[str, object], key: str) -> float:
    """Narrow one ``template`` value to ``float``. An ``int`` is accepted and
    widened, since JSON round-tripping (and hand-written test templates)
    commonly hand back whole numbers for what is conceptually a float.

    Raises:
        TypeError: ``template[key]`` is missing or neither ``float`` nor ``int``.
    """
    value = template[key]
    if isinstance(value, bool):
        raise TypeError(f"template[{key!r}] must be a float, got bool")
    if isinstance(value, int):
        return float(value)
    if not isinstance(value, float):
        raise TypeError(f"template[{key!r}] must be a float, got {type(value).__name__}")
    return value


def plan_timeline(
    word_timings: Sequence[tuple[str, float, float]],
    audio_duration_s: float,
    template: Mapping[str, object],
    seed: int,
) -> Timeline:
    """Plan the caption groups and B-roll segments for one narration.

    ``template`` must carry ``words_per_group_max``, ``segment_seconds_min``,
    and ``segment_seconds_max``. ``words_per_group_min`` and ``seed`` are
    accepted (matching ``PlanTimeline.settings_keys``) but not read - see
    this module's own docstring for why both are genuinely inert here rather
    than silently ignored oversights.

    Raises:
        KeyError: ``template`` is missing ``words_per_group_max``,
            ``segment_seconds_min``, or ``segment_seconds_max``.
        TypeError: one of those values is present but not the expected type.
    """
    words_per_group_max = _require_int(template, "words_per_group_max")
    segment_seconds_min = _require_float(template, "segment_seconds_min")
    segment_seconds_max = _require_float(template, "segment_seconds_max")

    groups = _group_words(word_timings, words_per_group_max=words_per_group_max)
    segments = _plan_segments(
        groups,
        audio_duration_s,
        segment_seconds_min=segment_seconds_min,
        segment_seconds_max=segment_seconds_max,
    )
    return Timeline(duration_s=audio_duration_s, groups=groups, segments=segments)
