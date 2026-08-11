"""The wire protocol between the dispatcher and its worker subprocesses.

One JSON object per line on the worker's stdout - never binary, never a
length prefix, so any tool that can read a pipe line by line can inspect a
live worker without decoding anything first.

Every message carries ``v`` (the protocol version), ``type``, ``job_id``,
``stage_id`` and ``correlation_id`` - the last as an *explicit field* on
every dataclass rather than pulled from a ``ContextVar`` at encode time.
Phase 0's carry-forward Sec 1.3 found that reading it from context instead
lets a relayed log line get stamped with the *parent's* correlation ID
rather than the job's, quietly destroying the per-job trail the mechanism
exists to provide.

``decode`` returns ``None`` for an unknown ``type`` or an unknown ``v``
rather than raising: a newer worker must never be able to wedge an older
parent by emitting a message shape the parent does not recognise. A line
that is not JSON at all is different - that means the pipe itself is
corrupt, which is not survivable, so ``decode`` raises for that case instead
of silently skipping it.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, assert_never

from ytauto.core.errors import ErrorKind, ValidationError

PROTOCOL_VERSION: int = 1


@dataclass(frozen=True)
class ArtifactLine:
    """One artifact entry inside a ``Result`` message's wire payload.

    Deliberately distinct from ``core.models.artifact.ArtifactRef``: this is
    the shape a worker subprocess puts on the wire, not the validated domain
    object the parent later builds once the artifact has been recorded.
    """

    name: str
    kind: str
    digest: str


@dataclass(frozen=True)
class Progress:
    """Advisory progress. Never persisted as state - a lost line changes nothing."""

    job_id: str
    stage_id: str
    correlation_id: str
    fraction: float
    note: str


@dataclass(frozen=True)
class Staged:
    """A blob file is on disk under the CAS root; the parent owns the row.

    Workers may write blob files but never touch SQLite - single-writer
    discipline, see ``CasStore.stage_file`` / ``CasStore.record_blob``. This
    message is what carries a digest from the process that staged the file
    to the process that is allowed to record it.
    """

    job_id: str
    stage_id: str
    correlation_id: str
    digest: str
    kind: str
    size_bytes: int


@dataclass(frozen=True)
class Result:
    """The stage succeeded; here is everything it produced."""

    job_id: str
    stage_id: str
    correlation_id: str
    artifacts: tuple[ArtifactLine, ...]
    meta: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class Error:
    """The stage failed. ``kind`` drives retry-versus-fail, not the message text."""

    job_id: str
    stage_id: str
    correlation_id: str
    message: str
    kind: ErrorKind
    retry_after_s: float | None = None


@dataclass(frozen=True)
class LogLine:
    """A log record relayed into the parent's logger."""

    job_id: str
    stage_id: str
    correlation_id: str
    level: str
    message: str
    exc: str | None = None


Message = Progress | Staged | Result | Error | LogLine

_TYPE_NAMES: dict[type[Message], str] = {
    Progress: "progress",
    Staged: "staged",
    Result: "result",
    Error: "error",
    LogLine: "log",
}


def encode(msg: Message) -> str:
    """Serialise ``msg`` to exactly one line of JSON.

    Uses ``json.dumps(..., separators=(",", ":"))``. ``json.dumps`` escapes
    control characters - including an embedded ``\\n`` - by default, which is
    what makes "exactly one line" a guarantee rather than a convention: the
    transport is line-delimited, so an unescaped newline inside a log message
    would split one message into two unparseable halves.
    """
    common: dict[str, Any] = {
        "v": PROTOCOL_VERSION,
        "type": _TYPE_NAMES[type(msg)],
        "job_id": msg.job_id,
        "stage_id": msg.stage_id,
        "correlation_id": msg.correlation_id,
    }
    payload: dict[str, Any]
    if isinstance(msg, Progress):
        payload = {"fraction": msg.fraction, "note": msg.note}
    elif isinstance(msg, Staged):
        payload = {"digest": msg.digest, "kind": msg.kind, "size_bytes": msg.size_bytes}
    elif isinstance(msg, Result):
        payload = {
            "artifacts": [
                {"name": a.name, "kind": a.kind, "digest": a.digest} for a in msg.artifacts
            ],
            "meta": dict(msg.meta),
        }
    elif isinstance(msg, Error):
        payload = {
            "message": msg.message,
            "kind": msg.kind.value,
            "retry_after_s": msg.retry_after_s,
        }
    elif isinstance(msg, LogLine):
        payload = {"level": msg.level, "message": msg.message, "exc": msg.exc}
    else:
        assert_never(msg)
    return json.dumps({**common, **payload}, separators=(",", ":"))


def decode(line: str) -> Message | None:
    """Parse one line of the wire protocol.

    Returns ``None`` for a ``type`` or ``v`` this parent does not recognise,
    and for a recognised ``type`` whose payload is missing a field it
    requires - the same forward-compatibility reasoning covers both: a
    message this parent cannot fully make sense of is skipped, not fatal.

    Raises:
        ValidationError: ``line`` is not valid JSON. That means the pipe
            itself is corrupt, which is not survivable and must not be
            silently skipped.
    """
    try:
        raw = json.loads(line)
    except json.JSONDecodeError as exc:
        raise ValidationError(f"not valid JSON: {line!r}") from exc

    if not isinstance(raw, dict):
        return None
    if raw.get("v") != PROTOCOL_VERSION:
        return None

    try:
        return _decode_payload(raw)
    except (KeyError, TypeError, ValueError):
        return None


def _decode_payload(raw: dict[str, Any]) -> Message | None:
    """Dispatch on ``type``. Returns ``None`` for a ``type`` this parent lacks.

    Raises:
        KeyError: a field the named type requires is absent from ``raw``.
        ValueError: a field is present but malformed - e.g. ``kind`` is not a
            valid ``ErrorKind`` value.
    """
    common: dict[str, Any] = {
        "job_id": raw["job_id"],
        "stage_id": raw["stage_id"],
        "correlation_id": raw["correlation_id"],
    }
    msg_type = raw.get("type")
    if msg_type == "progress":
        return Progress(**common, fraction=raw["fraction"], note=raw["note"])
    if msg_type == "staged":
        return Staged(
            **common, digest=raw["digest"], kind=raw["kind"], size_bytes=raw["size_bytes"]
        )
    if msg_type == "result":
        artifacts = tuple(
            ArtifactLine(name=a["name"], kind=a["kind"], digest=a["digest"])
            for a in raw["artifacts"]
        )
        return Result(**common, artifacts=artifacts, meta=raw.get("meta", {}))
    if msg_type == "error":
        return Error(
            **common,
            message=raw["message"],
            kind=ErrorKind(raw["kind"]),
            retry_after_s=raw.get("retry_after_s"),
        )
    if msg_type == "log":
        return LogLine(**common, level=raw["level"], message=raw["message"], exc=raw.get("exc"))
    return None
