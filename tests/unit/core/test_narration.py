import pytest

from ytauto.core.errors import ValidationError
from ytauto.core.models.narration import Narration, WordBoundary


def _boundary(**overrides: object) -> WordBoundary:
    base: dict[str, object] = {"text": "hello", "start_s": 0.5, "duration_s": 0.25}
    base.update(overrides)
    return WordBoundary(**base)  # type: ignore[arg-type]


def test_a_boundary_reports_its_end() -> None:
    assert _boundary(start_s=1.0, duration_s=0.5).end_s == 1.5


def test_an_empty_word_is_rejected() -> None:
    """A blank word would render as an empty caption cue that still consumes
    time on screen."""
    with pytest.raises(ValidationError, match="text"):
        _boundary(text="")


def test_a_negative_duration_is_rejected() -> None:
    """A word cannot end before it starts; the renderer would emit a cue with
    a reversed range and ffmpeg would reject the whole subtitle file."""
    with pytest.raises(ValidationError, match="duration_s"):
        _boundary(duration_s=-0.1)


def test_a_zero_duration_is_accepted() -> None:
    """The boundary case, pinned positively: engines do emit zero-length
    boundaries for elisions, and rejecting them would fail real narration."""
    assert _boundary(duration_s=0.0).duration_s == 0.0


def test_a_boundary_is_frozen() -> None:
    with pytest.raises(AttributeError):
        _boundary().text = "other"  # type: ignore[misc]


def test_narration_carries_no_boundaries_for_an_audio_only_engine() -> None:
    """None, not an empty tuple: "this engine reports nothing" and "this
    engine reported no words" must be distinguishable, because the first
    forces ASR and the second is a bug."""
    assert Narration(audio=b"\x00", boundaries=None).boundaries is None


def test_narration_carries_boundaries_when_the_engine_emits_them() -> None:
    narration = Narration(audio=b"\x00", boundaries=(_boundary(),))
    assert narration.boundaries is not None
    assert narration.boundaries[0].text == "hello"


def test_narration_is_frozen() -> None:
    with pytest.raises(AttributeError):
        Narration(audio=b"", boundaries=None).audio = b"other"  # type: ignore[misc]
