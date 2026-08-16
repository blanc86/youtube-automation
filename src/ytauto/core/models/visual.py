"""What a ``VisualStrategy`` draws from, and what it hands back.

Exists because ``VisualStrategy.plan`` could not carry what ``select_broll``
needs through its original shape - see ``core.ports.providers``' own
docstring above the ``VisualStrategy`` Protocol for the full argument. In
short: a single ``duration_s`` cannot express a timeline's independently-sized
segments, a bare ``tuple[str, ...]`` of asset references cannot carry an
in-point, and there was nowhere in the old signature to hand the strategy a
candidate library to draw from at all. These two frozen dataclasses are the
fix, mirroring ``core.models.narration``'s ``WordBoundary``/``Narration``
pair: small, validated, and reused by every ``VisualStrategy`` implementation
rather than each provider inventing its own shape.
"""

from __future__ import annotations

from dataclasses import dataclass

from ytauto.core.errors import ValidationError


@dataclass(frozen=True)
class VisualCandidate:
    """One visual asset a ``VisualStrategy`` may draw from.

    ``asset_id`` is opaque to the port itself - ``LibraryVisualStrategy``
    populates it with a B-roll ``clip_id`` (from Task 9's manifest); a future
    strategy backed by ``ImageGenerator`` might populate it with a generated
    image's identifier instead. ``plan`` never interprets it beyond identity
    and never mutates it - it only ever returns one back, unchanged, inside a
    ``VisualPlacement``.

    Raises:
        ValidationError: ``asset_id`` is empty, or ``duration_s`` is not
            positive - a zero- or negative-length asset could never fill any
            segment, so admitting one here would only defer a confusing
            failure to ``plan``.
    """

    asset_id: str
    duration_s: float

    def __post_init__(self) -> None:
        if not self.asset_id:
            raise ValidationError("VisualCandidate.asset_id must not be empty")
        if self.duration_s <= 0:
            raise ValidationError(f"VisualCandidate.duration_s must be positive: {self.duration_s}")


@dataclass(frozen=True)
class VisualPlacement:
    """One planned segment's visual: which asset, from what in-point, for how long.

    ``duration_s`` here is how much of the *segment* this placement fills -
    always the segment's own duration, never the source asset's total
    ``duration_s`` - so a clip longer than the segment it was drawn for is
    simply used in part, starting at ``in_point_s``.

    Raises:
        ValidationError: ``asset_id`` is empty, ``in_point_s`` is negative, or
            ``duration_s`` is not positive.
    """

    asset_id: str
    in_point_s: float
    duration_s: float

    def __post_init__(self) -> None:
        if not self.asset_id:
            raise ValidationError("VisualPlacement.asset_id must not be empty")
        if self.in_point_s < 0:
            raise ValidationError(
                f"VisualPlacement.in_point_s must not be negative: {self.in_point_s}"
            )
        if self.duration_s <= 0:
            raise ValidationError(f"VisualPlacement.duration_s must be positive: {self.duration_s}")
