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


class TransactionError(YtautoError):
    """``immediate=True`` was requested for a transaction nested inside another.

    ``transaction()`` is re-entrant: a nested call opens a SAVEPOINT instead of
    a new BEGIN. But a nested call cannot honour ``immediate=True`` - the write
    lock's timing was already decided by the outer BEGIN - so that combination
    is refused instead of silently downgraded.

    Deliberately distinct from ``sqlite3.OperationalError``, which the same
    helper raises for legitimate lock contention. This one always means a
    programming error and must never be retried. A scheduler claiming a job
    needs to tell "my code is broken, crash" from "someone else holds the
    lock, back off" without string-matching an error message.
    """


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
