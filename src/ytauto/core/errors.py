"""Typed error taxonomy.

Every error raised anywhere in the application derives from ``YtautoError``.
Retry behaviour is derived from ``ErrorKind`` rather than decided at each call
site, so a provider cannot accidentally make a fatal error look retryable.
"""

from __future__ import annotations

from enum import StrEnum


class YtautoError(Exception):
    """Base class for every application error."""


class ConfigurationError(YtautoError):
    """The application is misconfigured; user action is required."""


class ValidationError(YtautoError):
    """Input failed a domain invariant."""


class ResourceExhausted(YtautoError):
    """A finite local resource (disk, VRAM, lease) was unavailable."""


class RenderError(YtautoError):
    """Video composition or export failed."""


class ErrorKind(StrEnum):
    """How the scheduler should treat a provider failure."""

    RETRYABLE = "retryable"
    FATAL = "fatal"
    RATE_LIMITED = "rate_limited"
    QUOTA_EXCEEDED = "quota_exceeded"


_RETRYABLE_KINDS = frozenset({ErrorKind.RETRYABLE, ErrorKind.RATE_LIMITED})


class ProviderError(YtautoError):
    """A provider call failed.

    ``kind`` drives scheduler behaviour. ``QUOTA_EXCEEDED`` is deliberately
    not retryable: retrying spends money and cannot succeed.
    """

    def __init__(
        self,
        message: str,
        *,
        provider_id: str,
        kind: ErrorKind,
        retry_after_s: float | None = None,
    ) -> None:
        super().__init__(f"[{provider_id}/{kind}] {message}")
        self.provider_id = provider_id
        self.kind = kind
        self.retry_after_s = retry_after_s

    @property
    def is_retryable(self) -> bool:
        return self.kind in _RETRYABLE_KINDS
