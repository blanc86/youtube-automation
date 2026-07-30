"""Content hashing: the identity of every artifact the pipeline produces.

This lives in ``core`` rather than beside the CAS because it is the project's
central naming abstraction and depends on nothing but ``hashlib``. Phase 1's
``Stage``, ``StageResult``, ``fingerprint`` and ``core/models/job`` all have to
name artifact content hashes, and ``core`` cannot import ``infra`` - so keeping
``ContentHash`` in ``infra.cas.store`` would force those to duplicate the
``NewType`` or degrade to a bare ``str``.

A digest is always the full 64-character lowercase hex SHA-256 of the content.
Truncated or uppercase forms are rejected rather than normalised: silently
accepting them would let two spellings of one digest address one object.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import NewType

from ytauto.core.errors import ValidationError

ContentHash = NewType("ContentHash", str)

# 1 MiB. Large enough that hashing a multi-GB render is not syscall-bound,
# small enough that it never dominates a worker's resident set.
_CHUNK = 1024 * 1024
_HEX = frozenset("0123456789abcdef")


def validate_digest(digest: str) -> ContentHash:
    """Return ``digest`` as a ``ContentHash``, rejecting anything malformed.

    Raises:
        ValidationError: ``digest`` is not exactly 64 lowercase hex characters.
    """
    if len(digest) != 64 or not set(digest) <= _HEX:
        raise ValidationError(f"not a valid sha256 hex digest: {digest!r}")
    return ContentHash(digest)


def hash_bytes(data: bytes) -> ContentHash:
    """Hash an in-memory buffer. Pure."""
    return ContentHash(hashlib.sha256(data).hexdigest())


def hash_file(path: Path) -> ContentHash:
    """Hash a file's contents, streaming so arbitrarily large files are safe.

    Raises:
        OSError: ``path`` cannot be opened or read.
    """
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_CHUNK):
            digest.update(chunk)
    return ContentHash(digest.hexdigest())
