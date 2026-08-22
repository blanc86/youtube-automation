"""``render_ass``: turns a planned ``Timeline`` (Task 7) into an ASS (Advanced
SubStation Alpha) subtitle document, ready for an ``ass`` filter burn-in.

The signature look of this genre - and the entire reason the pipeline
captures per-word timings at all - is the moving word-by-word highlight: the
currently-spoken word changes colour, and *only* the currently-spoken word.
ASS's own karaoke tag (``\\k``) looks tempting for this but is wrong: karaoke
timing paints each word's colour in turn and leaves it painted, so by the
last word of a group every earlier word is *also* the accent colour - the
highlight accumulates instead of moving.

The technique used instead: **one ``Dialogue`` event per word.** For an
N-word ``CaptionGroup``, this module emits N events, rendering all N words of
the group, with only that event's word wrapped in an accent-colour override
(``{\\c<accent>}word{\\c<primary>}``). Only one word is ever coloured
differently per event, and which word that is changes every event - which is
what makes the accent *move* rather than accumulate. N is 3-5 words (the
group-size cap from Task 7's ``words_per_group_max``), so this is cheap: a
handful of near-identical ``Dialogue`` lines per caption, not a performance
concern.

**Event ends are tiled, not word-tight.** A first attempt gave each event
exactly its own word's ``[start_s, end_s)`` - which is correct for *when the
accent should be on that word*, but wrong for *when the caption should be on
screen*: every inter-word pause became a frame with no caption at all (on a
real narration, dozens of times, totalling several seconds). The fix keeps
the accent timing exactly as before - it still starts on the word's own
``start_s`` - but stretches each event's *end* to the next event's own
start, so consecutive events abut with zero gap. The last event of a group
therefore stays on screen through the pause before the next group starts
(matching "stays until the line has finished being spoken"), and the very
last event of the whole timeline ends at ``timeline.duration_s``. This is
computed from the flattened, chronological sequence of every word across
every group - not from ``CaptionGroup.end_s``, which stays exactly what
``core.pipeline.timeline`` documents it as ("when the last word stops being
spoken") because segment cut points are pinned to it; widening it here would
silently move where B-roll cuts land.

Deliberately **stdlib-only** beyond the one import of Task 7's own
``Timeline`` - the type this module exists to consume. No I/O, no clock, no
randomness: ``render_ass`` is a pure function of its arguments, matching
``core.pipeline.timeline.plan_timeline``'s own house style (see that
module's docstring). There is no stage and no entry point here - Tasks 11
and 12's compose stages call ``render_ass`` directly, which is what stops
"burn captions for canvas A" and "burn captions for canvas B" from
duplicating this module's logic.
"""

from __future__ import annotations

from collections.abc import Mapping

from ytauto.core.pipeline.timeline import Timeline

_STYLE_NAME = "Default"
"""The one ``[V4+ Styles]`` style every ``Dialogue`` event references (the
brief's Step 5: "using the same ``Style`` name throughout")."""

# --- style defaults ----------------------------------------------------------
#
# The brief names exactly six things ``style`` may carry: font name, size,
# primary colour, accent colour, outline, and shadow, plus (as of this fix)
# a seventh: alignment - captions were pinned to bottom-centre with no way
# for a caller to change it, and the brief now calls for a middle-centre
# default. ``alignment`` is read from ``style`` the same way as the other
# six, via ``_style_field``. Every other field the ASS ``[V4+ Styles]`` line
# requires (SecondaryColour, OutlineColour, BackColour, Bold, Italic, ...,
# the three margins, Encoding) is not part of that enumerated set, so it
# stays a fixed constant here rather than a second, unnamed style contract
# this module would be inventing on Tasks 11/12's behalf. See this task's
# report for the full list and the reasoning.

_DEFAULT_FONT_NAME = "Arial"
_DEFAULT_FONT_SIZE = 96
_DEFAULT_PRIMARY_COLOUR = "&H00FFFFFF"  # opaque white
_DEFAULT_ACCENT_COLOUR = "&H0000FFFF"  # opaque yellow - &HAABBGGRR, not RGB
_DEFAULT_OUTLINE = 3
_DEFAULT_SHADOW = 0
_DEFAULT_ALIGNMENT = 5  # middle-centre, numpad-style

# Fixed [V4+ Styles] fields - not caller-configurable; see the note above.
_SECONDARY_COLOUR = "&H000000FF"  # unused: no \k karaoke tag is ever emitted
_OUTLINE_COLOUR = "&H00000000"  # opaque black
_BACK_COLOUR = "&H00000000"  # opaque black
_BOLD = "-1"  # ASS boolean true
_ITALIC = "0"
_UNDERLINE = "0"
_STRIKE_OUT = "0"
_SCALE_X = "100"
_SCALE_Y = "100"
_SPACING = "0"
_ANGLE = "0"
_BORDER_STYLE = "1"  # outline + drop shadow, no opaque box
_MARGIN_L = "40"
_MARGIN_R = "40"
_MARGIN_V = "80"
_ENCODING = "1"


def _style_field(style: Mapping[str, object], key: str, default: object) -> str:
    """One of the seven style-driven ``[V4+ Styles]``/override fields:
    ``style[key]`` if present, ``default`` otherwise.

    Never raises. This is a rendering concern, not a validation boundary - a
    caller that hands a nonsensical value (the wrong type, a malformed ASS
    colour) gets that value reflected straight into the output file rather
    than a rejected render; Tasks 11/12 are responsible for passing
    ASS-format strings, per this task's own resolved ambiguity on colours.
    """
    return str(style.get(key, default))


def _format_timestamp(seconds: float) -> str:
    """Render ``seconds`` as ASS's own ``H:MM:SS.cc`` timestamp: one-digit
    hour, two-digit minutes and seconds, two-digit **centiseconds** - ASS
    timestamps are centisecond-precision, not millisecond.

    Truncated, not rounded, per this task's resolved ambiguity: a caption
    appearing one centisecond early is invisible; one late can clip the
    word's own first frame.

    The ``+ 1e-9`` before truncating exists only to absorb float64
    representation noise - ``0.29 * 100`` is ``28.999999999999996`` in
    float64, which a bare ``int()`` truncates to ``28`` instead of the
    intended ``29``. ``1e-9`` is sized to that noise specifically (float64
    noise from a multiply-by-100 is on the order of 1e-13, so 1e-9 is
    still four orders of magnitude of headroom above it) rather than a
    coarser fix like ``round(seconds * 100, 6)``: rounding to 6 decimal
    places snaps anything within 5e-7 of an integer, which is six orders of
    magnitude more permissive than the noise it exists to absorb - wide
    enough to catch a *genuine* value like ``1.499999996`` and truncate it
    up to ``150`` instead of down to the correct ``149``, exactly the
    late-and-clips-the-word failure this function's own truncation rule
    exists to prevent. The narrow epsilon absorbs the noise case without
    ever promoting a genuine near-boundary value. Verified directly: see
    ``test_a_float_noise_near_integer_centisecond_does_not_truncate_short``
    and ``test_a_genuine_value_near_a_centisecond_boundary_does_not_round_up``.
    """
    total_centiseconds = int(seconds * 100 + 1e-9)
    centiseconds = total_centiseconds % 100
    total_seconds = total_centiseconds // 100
    secs = total_seconds % 60
    total_minutes = total_seconds // 60
    minutes = total_minutes % 60
    hours = total_minutes // 60
    return f"{hours}:{minutes:02d}:{secs:02d}.{centiseconds:02d}"


def _escape(text: str) -> str:
    """Escape ``text`` for safe interpolation into an ASS ``Text`` field.

    ``{`` and ``}`` delimit an override block, and ``\\`` starts a tag inside
    one - left unescaped, a narrated word containing any of the three could
    inject an override tag (change colour, move position, ...) into every
    caption downstream of it. Order matters: backslashes are doubled
    *before* the brace-escaping backslashes are added, so the two escaping
    backslashes just inserted are never themselves re-escaped by the first
    step.
    """
    return text.replace("\\", "\\\\").replace("{", "\\{").replace("}", "\\}")


def _style_line(style: Mapping[str, object]) -> str:
    """The single ``Style:`` line of the ``[V4+ Styles]`` section, built from
    ``style``'s six style-line fields (font name, size, primary colour,
    outline, shadow, alignment - ``accent_colour`` is not a ``[V4+ Styles]``
    field; it only ever appears in a per-word override, see
    ``_render_words``) plus the fixed fields listed at module scope.
    """
    fields = ",".join(
        [
            _STYLE_NAME,
            _style_field(style, "font_name", _DEFAULT_FONT_NAME),
            _style_field(style, "font_size", _DEFAULT_FONT_SIZE),
            _style_field(style, "primary_colour", _DEFAULT_PRIMARY_COLOUR),
            _SECONDARY_COLOUR,
            _OUTLINE_COLOUR,
            _BACK_COLOUR,
            _BOLD,
            _ITALIC,
            _UNDERLINE,
            _STRIKE_OUT,
            _SCALE_X,
            _SCALE_Y,
            _SPACING,
            _ANGLE,
            _BORDER_STYLE,
            _style_field(style, "outline", _DEFAULT_OUTLINE),
            _style_field(style, "shadow", _DEFAULT_SHADOW),
            _style_field(style, "alignment", _DEFAULT_ALIGNMENT),
            _MARGIN_L,
            _MARGIN_R,
            _MARGIN_V,
            _ENCODING,
        ]
    )
    return f"Style: {fields}"


def _render_words(
    words: tuple[tuple[str, float, float], ...],
    accent_index: int,
    accent_colour: str,
    primary_colour: str,
) -> str:
    """The ``Text`` field for the ``Dialogue`` event that accents
    ``words[accent_index]``: every word of the group, space-joined and
    individually escaped, with only the accented word wrapped in
    ``{\\c<accent>}...{\\c<primary>}``.

    The closing override resets colour back to ``primary_colour`` rather
    than leaving the accent colour open - an ASS override persists to the
    end of the event (or the next override) otherwise, which would leave
    every word *after* the accented one also accent-coloured instead of
    only the one word this technique is meant to isolate.
    """
    rendered = []
    for index, (text, _start_s, _end_s) in enumerate(words):
        escaped = _escape(text)
        if index == accent_index:
            escaped = f"{{\\c{accent_colour}}}{escaped}{{\\c{primary_colour}}}"
        rendered.append(escaped)
    return " ".join(rendered)


def render_ass(timeline: Timeline, *, width: int, height: int, style: Mapping[str, object]) -> str:
    """Render ``timeline`` as a complete ASS document.

    ``width``/``height`` become ``PlayResX``/``PlayResY`` - get these wrong
    and every caption is mis-scaled on whichever canvas doesn't match (the
    two portrait/landscape canvases Tasks 11/12 render for). ``style``
    supplies the seven caller-configurable fields (see the module-level
    defaults above); ``timeline.segments`` is not read at all - B-roll
    segmentation is a different concern this module has no part in.

    One ``Dialogue`` event per word, all on ``Layer`` 0, all against
    ``_STYLE_NAME`` - see the module docstring for why the per-word event is
    the whole technique, and for why each event's *end* is tiled to the next
    event's *start* rather than the word's own ``end_s``.
    """
    accent_colour = _style_field(style, "accent_colour", _DEFAULT_ACCENT_COLOUR)
    primary_colour = _style_field(style, "primary_colour", _DEFAULT_PRIMARY_COLOUR)

    lines: list[str] = [
        "[Script Info]",
        "ScriptType: v4.00+",
        "WrapStyle: 0",
        "ScaledBorderAndShadow: yes",
        "YCbCr Matrix: None",
        f"PlayResX: {width}",
        f"PlayResY: {height}",
        "",
        "[V4+ Styles]",
        "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, "
        "BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, "
        "BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding",
        _style_line(style),
        "",
        "[Events]",
        "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text",
    ]

    # Every word's own start_s, in chronological order across every group -
    # groups themselves are already chronological and non-overlapping (see
    # core.pipeline.timeline), so this single flattened list is enough to
    # tile every event: event i's end is starts[i + 1], and the very last
    # event's end is timeline.duration_s. This is what keeps a caption on
    # screen through the pause *between* words and *between* groups - the
    # blank-gap defect this module used to have - without touching
    # CaptionGroup.end_s (see the module docstring for why that field is
    # left alone).
    starts: list[float] = [word[1] for group in timeline.groups for word in group.words]

    position = 0
    for group in timeline.groups:
        for index, (_text, start_s, _own_end_s) in enumerate(group.words):
            end_s = starts[position + 1] if position + 1 < len(starts) else timeline.duration_s
            text = _render_words(group.words, index, accent_colour, primary_colour)
            lines.append(
                f"Dialogue: 0,{_format_timestamp(start_s)},{_format_timestamp(end_s)},"
                f"{_STYLE_NAME},,0,0,0,,{text}"
            )
            position += 1

    return "\n".join(lines) + "\n"
