"""Content-addressed stage fingerprinting.

A stage whose fingerprint already has stored artifacts is skipped. One
mechanism therefore delivers crash-resume, cheap iteration, and cross-project
dedup - and an unstable fingerprint silently disables all three while failing
nothing. Canonicalisation here is deliberately strict for that reason.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from pathlib import PurePath
from typing import Any

from ytauto.core.errors import ValidationError
from ytauto.core.models.content_hash import ContentHash

FINGERPRINT_SCHEMA_VERSION = 1
"""Bump when canonicalisation changes, so old artifacts invalidate rather than
colliding with differently-computed new ones."""


def _encode(obj: object) -> Any:
    """Convert a value json.dumps cannot handle into one it can.

    Raises:
        ValidationError: for paths, non-finite floats, and unsupported types.
    """
    if isinstance(obj, PurePath):
        raise ValidationError(
            f"a path may not appear in a fingerprint: {obj!r} is machine-specific"
        )
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, (set, frozenset)):
        return sorted(obj, key=canonical_json)
    if isinstance(obj, bytes):
        return obj.hex()
    raise ValidationError(f"cannot fingerprint a value of type {type(obj).__name__}: {obj!r}")


def canonical_json(value: object) -> str:
    """Encode a value as deterministic JSON: sorted keys, no whitespace.

    ``allow_nan=False`` is what rejects NaN and Infinity: they are not valid
    JSON and are meaningless as a cache key. It is load-bearing, not decorative
    - without it json.dumps happily emits bare ``NaN``, which no other JSON
    reader would accept and which never compares equal to itself.

    Raises:
        ValidationError: if the value contains a path, a non-finite float, a
            circular reference, or an unsupported type.
    """
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
            default=_encode,
        )
    except ValueError as exc:  # non-finite float, or a circular reference
        raise ValidationError(f"value is not canonicalisable: {exc}") from exc


@dataclass(frozen=True)
class FingerprintSpec:
    """Everything that determines a stage's output.

    ``input_digests`` is ordered because order changes the result - concatenating
    two clips the other way round produces a different video.
    """

    stage_id: str
    stage_version: int
    provider_id: str
    provider_version: str
    input_digests: tuple[ContentHash, ...]
    settings: Mapping[str, object]


def compute_fingerprint(spec: FingerprintSpec) -> str:
    """Return the 64-char lowercase hex fingerprint for a stage execution.

    Raises:
        ValidationError: if ``settings`` contains a path, a non-finite float, or
            an unsupported type.
    """
    payload = {
        "schema": FINGERPRINT_SCHEMA_VERSION,
        "stage_id": spec.stage_id,
        "stage_version": spec.stage_version,
        "provider_id": spec.provider_id,
        "provider_version": spec.provider_version,
        "input_digests": list(spec.input_digests),
        "settings": dict(spec.settings),
    }
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
