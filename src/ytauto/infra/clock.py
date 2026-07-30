"""The single source of timestamps.

Every stored timestamp goes through here: UTC, ISO-8601, explicit offset.
Lexicographic string ordering of these values matches chronological ordering,
which the CAS evictor relies on when it sorts by ``last_accessed_at``.
"""

from __future__ import annotations

from datetime import UTC, datetime


def utc_now_iso() -> str:
    """Current UTC time as an ISO-8601 string ending in '+00:00'."""
    return datetime.now(tz=UTC).isoformat()
