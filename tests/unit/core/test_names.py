import pytest

from ytauto.core.errors import ValidationError
from ytauto.core.models.names import assert_unique_names


def test_unique_names_pass() -> None:
    assert_unique_names(["a", "b", "c"], what="stage", context="pipeline 'p'")


def test_a_duplicate_is_named_in_the_message() -> None:
    with pytest.raises(ValidationError, match="duplicate stage name in pipeline 'p': 'b'"):
        assert_unique_names(["a", "b", "b"], what="stage", context="pipeline 'p'")


def test_the_first_duplicate_is_reported_not_the_last() -> None:
    """Deterministic messages matter: a test asserting on the message must not
    depend on which duplicate happens to be found."""
    with pytest.raises(ValidationError, match="'b'"):
        assert_unique_names(["a", "b", "b", "c", "c"], what="stage", context="p")


def test_an_empty_iterable_is_fine() -> None:
    assert_unique_names([], what="artifact", context="stage 'tts'")
