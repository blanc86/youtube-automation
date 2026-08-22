"""Turning the per-project settings form into a settings mapping, and back.

The authority on what a settings value may be is
``app.services.enqueue.validate_settings`` - the same function
``create_project`` and ``refresh_run_settings`` already run. Nothing here
re-decides any of that. This module's whole job is the part
``validate_settings`` cannot do: HTML form fields arrive as strings, and a
string is not an ``int``, a ``float``, or an ASS colour literal. So
``parse_form`` coerces, and then hands the result to the real validator.

Two conversions are more than coercion and are documented where they live:
colours (``ui.colours`` - ASS is ``&HAABBGGRR``, not RGB) and ``font_size``
(below - blank means *automatic*, and automatic is not a number).
"""

from __future__ import annotations

from collections.abc import Mapping

from ytauto.app.services.enqueue import validate_settings
from ytauto.ui.colours import alpha_of, ass_to_hex, hex_to_ass

ALIGNMENTS: tuple[tuple[int, str], ...] = (
    (7, "Top left"),
    (8, "Top centre"),
    (9, "Top right"),
    (4, "Middle left"),
    (5, "Middle centre"),
    (6, "Middle right"),
    (1, "Bottom left"),
    (2, "Bottom centre"),
    (3, "Bottom right"),
)
"""ASS alignment codes, laid out here in reading order rather than numeric
order because the numbering is numpad-shaped (1 is bottom-left, 7 is
top-left) and a dropdown listing 1-9 reads as nonsense. 5 - middle centre -
is ``core.captions.ass``'s own default."""

DEFAULT_PRIMARY_HEX = "#ffffff"
DEFAULT_ACCENT_HEX = "#ffff00"
"""What the two colour pickers show for a project whose ``caption_style`` does
not name a colour. These mirror ``core.captions.ass``'s ``_DEFAULT_PRIMARY_COLOUR``
(``&H00FFFFFF``, opaque white) and ``_DEFAULT_ACCENT_COLOUR`` (``&H0000FFFF``,
opaque yellow) - a colour input has no concept of "unset", so it has to show
*something*, and showing anything other than what the renderer would actually
use would be a lie the user only discovers after a render."""

DEFAULT_ALIGNMENT = 5

_NUMBER_FIELDS: tuple[str, ...] = ("segment_seconds_min", "segment_seconds_max")
_INT_FIELDS: tuple[str, ...] = ("seed", "words_per_group_min", "words_per_group_max")
_TEXT_FIELDS: tuple[str, ...] = ("voice", "rate", "encoder")


class FormError(Exception):
    """A form value could not be read as the type its setting requires.

    Distinct from ``ValidationError``, which means the value parsed fine and
    is still not legal (``words_per_group_max`` below its own min, say). Both
    end up in the same red banner; keeping them apart means a genuine
    ``ValidationError`` raised deeper in the stack is never mistaken for a
    typo in a number field.
    """


def _int_field(form: Mapping[str, str], key: str) -> int:
    raw = form.get(key, "").strip()
    try:
        return int(raw)
    except ValueError:
        raise FormError(f"{_label(key)} must be a whole number, got {raw!r}") from None


def _float_field(form: Mapping[str, str], key: str) -> float:
    raw = form.get(key, "").strip()
    try:
        return float(raw)
    except ValueError:
        raise FormError(f"{_label(key)} must be a number, got {raw!r}") from None


def _label(key: str) -> str:
    return key.replace("_", " ")


def parse_form(form: Mapping[str, str], *, current: Mapping[str, object]) -> dict[str, object]:
    """Read the settings form into the settings keys it covers.

    ``current`` is the project's existing settings. It is read for exactly
    one thing: the alpha byte of each caption colour, which the form does not
    expose and must therefore preserve rather than reset - see ``ui.colours``.
    Every other value in the returned mapping comes from ``form``.

    The result covers only the keys the form has fields for. Settings the UI
    deliberately does not manage - ``story_digest``, ``story_path``,
    ``broll_manifest_digest``, all three derived or owned elsewhere - are not
    in it, so a caller merging this over the stored settings leaves them
    untouched.

    **A blank font size means automatic, and automatic is the absence of the
    key.** ``ComposeStage.run`` does ``style.setdefault("font_size", height //
    20)``, which resolves to 54 on the landscape canvas and 96 on the
    vertical one - the same apparent size on both. Writing an explicit number
    here would pin *both* canvases to it, so the field is optional and a
    blank one omits the key entirely rather than substituting either
    canvas's number.

    Raises:
        FormError: a field could not be read as its setting's type.
        ValidationError: a parsed value is not legal - raised by
            ``validate_settings``, unchanged and unwrapped, so the message
            the user sees is the one the engine would have produced.
    """
    parsed: dict[str, object] = {}
    for key in _TEXT_FIELDS:
        parsed[key] = form.get(key, "").strip()
    for key in _INT_FIELDS:
        parsed[key] = _int_field(form, key)
    for key in _NUMBER_FIELDS:
        parsed[key] = _float_field(form, key)

    style: dict[str, object] = dict(_current_style(current))
    for key, default_hex in (
        ("primary_colour", DEFAULT_PRIMARY_HEX),
        ("accent_colour", DEFAULT_ACCENT_HEX),
    ):
        supplied = form.get(key, default_hex)
        try:
            style[key] = hex_to_ass(supplied, alpha=alpha_of(style.get(key)))
        except ValueError as exc:
            raise FormError(f"{_label(key)}: {exc}") from None

    alignment = _int_field(form, "alignment")
    if alignment not in {code for code, _ in ALIGNMENTS}:
        raise FormError(f"alignment must be one of 1-9, got {alignment}")
    style["alignment"] = alignment

    font_size_raw = form.get("font_size", "").strip()
    if font_size_raw:
        font_size = _int_field(form, "font_size")
        if font_size < 1:
            raise FormError(f"font size must be at least 1, got {font_size}")
        style["font_size"] = font_size
    else:
        style.pop("font_size", None)

    parsed["caption_style"] = style

    # The real validator, on the real merged mapping - not on `parsed` alone.
    # `validate_settings` checks pairs of bounds against each other, and it
    # can only do that for a mapping that has both halves. Merging first also
    # means a value the form does not manage cannot be made illegal by one
    # that it does.
    validate_settings({**current, **parsed})
    return parsed


def _current_style(settings: Mapping[str, object]) -> Mapping[str, object]:
    value = settings.get("caption_style")
    return value if isinstance(value, Mapping) else {}


def form_values(settings: Mapping[str, object]) -> dict[str, object]:
    """The values to render into the form's fields for a stored settings mapping.

    Colours come back as ``#rrggbb`` for ``<input type="color">``; everything
    else is passed through as-is and rendered by Jinja. ``font_size`` is
    ``""`` when unset, which is what makes the field show its "automatic"
    placeholder.
    """
    style = _current_style(settings)
    values: dict[str, object] = {
        key: settings.get(key, "") for key in (*_TEXT_FIELDS, *_INT_FIELDS, *_NUMBER_FIELDS)
    }
    values["primary_colour"] = ass_to_hex(style.get("primary_colour"), default=DEFAULT_PRIMARY_HEX)
    values["accent_colour"] = ass_to_hex(style.get("accent_colour"), default=DEFAULT_ACCENT_HEX)
    alignment = style.get("alignment", DEFAULT_ALIGNMENT)
    values["alignment"] = alignment if isinstance(alignment, int) else DEFAULT_ALIGNMENT
    font_size = style.get("font_size")
    values["font_size"] = font_size if isinstance(font_size, int) else ""
    return values


__all__ = [
    "ALIGNMENTS",
    "DEFAULT_ACCENT_HEX",
    "DEFAULT_ALIGNMENT",
    "DEFAULT_PRIMARY_HEX",
    "FormError",
    "form_values",
    "parse_form",
]
