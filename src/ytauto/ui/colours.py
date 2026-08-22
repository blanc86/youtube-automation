r"""Converting between an HTML colour input and an ASS colour literal.

``<input type="color">` speaks exactly one dialect: ``#rrggbb``. ASS speaks
``&HAABBGGRR`` - **alpha first, then blue, green, red**. The byte order is
reversed relative to HTML, and getting it wrong does not fail: it renders,
and the captions simply come out with red and blue swapped. ``core.captions.ass``
already carries the warning in a comment on its own default
(``_DEFAULT_ACCENT_COLOUR = "&H0000FFFF"  # opaque yellow - &HAABBGGRR, not RGB``);
this module is where that knowledge has to be executable rather than a note.

Worked example, in both directions:

    #ffff00  (yellow: R=ff G=ff B=00)  <->  &H0000FFFF  (A=00 B=00 G=ff R=ff)
    #ff0000  (pure red)                <->  &H000000FF
    #0000ff  (pure blue)               <->  &H00FF0000

**Alpha is preserved, never invented.** ASS alpha is inverted relative to
every other convention here - ``00`` is fully opaque, ``FF`` fully
transparent - and there is no HTML control that maps onto it cleanly. The UI
therefore does not expose alpha: it reads whatever alpha byte the stored
value already carried and writes the same one back, so a caption style
hand-edited to be semi-transparent survives a trip through the settings form
instead of being silently forced opaque. A value with no alpha to preserve
(a new project, whose ``caption_style`` is ``{}``) gets ``00``.
"""

from __future__ import annotations

import re

OPAQUE = "00"
"""ASS's fully-opaque alpha byte. Inverted relative to CSS/RGBA: ``FF`` is
fully *transparent* in ASS, not fully opaque."""

_ASS_COLOUR = re.compile(r"^&H([0-9A-Fa-f]{2})([0-9A-Fa-f]{2})([0-9A-Fa-f]{2})([0-9A-Fa-f]{2})$")
_HEX_COLOUR = re.compile(r"^#?([0-9A-Fa-f]{6})$")


def ass_to_hex(value: object, *, default: str = "#ffffff") -> str:
    """The ``#rrggbb`` an ``<input type="color">`` can display for an ASS colour.

    Returns ``default`` for anything unparseable, rather than raising: this
    feeds a form field, and a ``caption_style`` hand-edited to something odd
    should still render a usable settings page (where the user can then fix
    it) instead of a 500. The odd value is not silently written back either -
    it is only overwritten if the user actually submits the form.
    """
    match = _ASS_COLOUR.match(value) if isinstance(value, str) else None
    if match is None:
        return default
    _alpha, blue, green, red = match.groups()
    return f"#{red}{green}{blue}".lower()


def alpha_of(value: object, *, default: str = OPAQUE) -> str:
    """The two-character alpha byte of an ASS colour, for round-tripping.

    See the module docstring: the UI does not offer an alpha control, so the
    only correct thing to do with an existing alpha is give it back.
    """
    match = _ASS_COLOUR.match(value) if isinstance(value, str) else None
    return match.group(1).upper() if match else default


def hex_to_ass(value: str, *, alpha: str = OPAQUE) -> str:
    """The ``&HAABBGGRR`` literal for an ``#rrggbb`` colour input.

    Raises:
        ValueError: ``value`` is not six hex digits, with or without a
            leading ``#``. A browser's colour input cannot produce anything
            else, so this only fires on a hand-crafted request - which is
            bad input, and is reported as a form error rather than accepted
            into a settings value that would reach ffmpeg.
    """
    match = _HEX_COLOUR.match(value.strip())
    if match is None:
        raise ValueError(f"not a #rrggbb colour: {value!r}")
    digits = match.group(1).upper()
    red, green, blue = digits[0:2], digits[2:4], digits[4:6]
    return f"&H{alpha.upper()}{blue}{green}{red}"
