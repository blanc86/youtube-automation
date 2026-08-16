import pytest

from ytauto.core.errors import ValidationError
from ytauto.core.models.visual import VisualCandidate, VisualPlacement


def _candidate(**overrides: object) -> VisualCandidate:
    base: dict[str, object] = {"asset_id": "clip-1", "duration_s": 12.0}
    base.update(overrides)
    return VisualCandidate(**base)  # type: ignore[arg-type]


def _placement(**overrides: object) -> VisualPlacement:
    base: dict[str, object] = {"asset_id": "clip-1", "in_point_s": 1.0, "duration_s": 5.0}
    base.update(overrides)
    return VisualPlacement(**base)  # type: ignore[arg-type]


def test_an_empty_candidate_asset_id_is_rejected() -> None:
    with pytest.raises(ValidationError, match="asset_id"):
        _candidate(asset_id="")


def test_a_non_positive_candidate_duration_is_rejected() -> None:
    """A zero- or negative-length asset could never fill any segment."""
    with pytest.raises(ValidationError, match="duration_s"):
        _candidate(duration_s=0.0)


def test_a_negative_candidate_duration_is_rejected() -> None:
    with pytest.raises(ValidationError, match="duration_s"):
        _candidate(duration_s=-1.0)


def test_a_candidate_is_frozen() -> None:
    with pytest.raises(AttributeError):
        _candidate().asset_id = "other"  # type: ignore[misc]


def test_an_empty_placement_asset_id_is_rejected() -> None:
    with pytest.raises(ValidationError, match="asset_id"):
        _placement(asset_id="")


def test_a_negative_placement_in_point_is_rejected() -> None:
    with pytest.raises(ValidationError, match="in_point_s"):
        _placement(in_point_s=-0.1)


def test_a_zero_placement_in_point_is_accepted() -> None:
    """The boundary case, pinned positively: an in-point of exactly 0.0 is
    the ordinary case when a clip exactly matches its segment's length."""
    assert _placement(in_point_s=0.0).in_point_s == 0.0


def test_a_non_positive_placement_duration_is_rejected() -> None:
    with pytest.raises(ValidationError, match="duration_s"):
        _placement(duration_s=0.0)


def test_a_placement_is_frozen() -> None:
    with pytest.raises(AttributeError):
        _placement().in_point_s = 2.0  # type: ignore[misc]
