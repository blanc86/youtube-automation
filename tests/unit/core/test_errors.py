import pytest

from ytauto.core.errors import (
    ErrorKind,
    ProviderError,
    RenderError,
    ValidationError,
    YtautoError,
)


def test_all_errors_share_one_base() -> None:
    for cls in (ValidationError, RenderError, ProviderError):
        assert issubclass(cls, YtautoError)


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        (ErrorKind.RETRYABLE, True),
        (ErrorKind.RATE_LIMITED, True),
        (ErrorKind.FATAL, False),
        (ErrorKind.QUOTA_EXCEEDED, False),
    ],
)
def test_retryability_is_derived_from_kind(kind: ErrorKind, expected: bool) -> None:
    err = ProviderError("boom", provider_id="gemini", kind=kind)
    assert err.is_retryable is expected


def test_quota_exceeded_is_not_retryable() -> None:
    """Retrying a quota breach burns money and never succeeds."""
    err = ProviderError("over budget", provider_id="openai", kind=ErrorKind.QUOTA_EXCEEDED)
    assert err.is_retryable is False


def test_provider_error_carries_context_for_diagnostics() -> None:
    err = ProviderError(
        "429 slow down",
        provider_id="elevenlabs",
        kind=ErrorKind.RATE_LIMITED,
        retry_after_s=12.5,
    )
    assert err.provider_id == "elevenlabs"
    assert err.retry_after_s == 12.5
    assert "elevenlabs" in str(err)
