"""Content-addressed blob storage.

Objects are named by the SHA-256 of their contents, so identical bytes are
stored exactly once regardless of how many projects reference them. Refcounts
protect in-use objects from the evictor.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import sqlite3
from pathlib import Path
from typing import NewType

from ytauto.core.errors import ValidationError
from ytauto.infra.clock import utc_now_iso
from ytauto.infra.db.engine import transaction

ContentHash = NewType("ContentHash", str)

_CHUNK = 1024 * 1024
_HEX = frozenset("0123456789abcdef")


def _validate(digest: str) -> ContentHash:
    if len(digest) != 64 or not set(digest) <= _HEX:
        raise ValidationError(f"not a valid sha256 hex digest: {digest!r}")
    return ContentHash(digest)


def hash_bytes(data: bytes) -> ContentHash:
    return ContentHash(hashlib.sha256(data).hexdigest())


def hash_file(path: Path) -> ContentHash:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_CHUNK):
            digest.update(chunk)
    return ContentHash(digest.hexdigest())


class CasStore:
    """Blob storage addressed by content hash, with refcounts held in SQLite."""

    def __init__(self, root: Path, conn: sqlite3.Connection) -> None:
        self._root = root
        self._conn = conn
        self._root.mkdir(parents=True, exist_ok=True)

    def path_for(self, digest: ContentHash) -> Path:
        valid = _validate(digest)
        return self._root / valid[0:2] / valid[2:4] / valid

    def exists(self, digest: ContentHash) -> bool:
        return self.path_for(digest).is_file()

    def _staging_path(self, target: Path) -> Path:
        """A tmp name unique per process.

        Phase 1 runs several worker subprocesses against this store; a shared
        ``.tmp`` name would let two of them corrupt each other's partial write.
        """
        return target.with_name(f"{target.name}.{os.getpid()}.tmp")

    def put_bytes(self, data: bytes, *, kind: str) -> ContentHash:
        digest = hash_bytes(data)
        target = self.path_for(digest)
        if not target.is_file():
            target.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._staging_path(target)
            tmp.write_bytes(data)
            tmp.replace(target)
        self._record(digest, kind=kind, size=len(data))
        return digest

    def put_file(self, src: Path, *, kind: str, move: bool = False) -> ContentHash:
        if not src.is_file():
            raise ValidationError(f"source file does not exist: {src}")
        digest = hash_file(src)
        target = self.path_for(digest)
        size = src.stat().st_size
        if target.is_file():
            if move:
                src.unlink()
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._staging_path(target)
            if move:
                shutil.move(str(src), tmp)
            else:
                shutil.copyfile(src, tmp)
            tmp.replace(target)
        self._record(digest, kind=kind, size=size)
        return digest

    def read_bytes(self, digest: ContentHash) -> bytes:
        path = self.path_for(digest)
        if not path.is_file():
            raise ValidationError(f"no such object in store: {digest}")
        return path.read_bytes()

    def _record(self, digest: ContentHash, *, kind: str, size: int) -> None:
        with transaction(self._conn):
            self._conn.execute(
                """
                INSERT INTO cas_objects (hash, kind, size_bytes, created_at, last_accessed_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(hash) DO UPDATE SET last_accessed_at = excluded.last_accessed_at
                """,
                (digest, kind, size, utc_now_iso(), utc_now_iso()),
            )

    def touch(self, digest: ContentHash) -> None:
        self.set_last_accessed(digest, utc_now_iso())

    def set_last_accessed(self, digest: ContentHash, timestamp: str) -> None:
        """Set the access time explicitly.

        Public because the evictor's LRU ordering must be assertable in tests,
        and because restore-from-backup needs to preserve original access times.
        """
        with transaction(self._conn):
            self._conn.execute(
                "UPDATE cas_objects SET last_accessed_at = ? WHERE hash = ?", (timestamp, digest)
            )

    def retain(self, digest: ContentHash) -> None:
        with transaction(self._conn):
            self._conn.execute(
                "UPDATE cas_objects SET refcount = refcount + 1 WHERE hash = ?", (digest,)
            )

    def release(self, digest: ContentHash) -> None:
        with transaction(self._conn):
            self._conn.execute(
                "UPDATE cas_objects SET refcount = max(0, refcount - 1) WHERE hash = ?", (digest,)
            )

    def refcount(self, digest: ContentHash) -> int:
        row = self._conn.execute(
            "SELECT refcount FROM cas_objects WHERE hash = ?", (digest,)
        ).fetchone()
        if row is None:
            raise ValidationError(f"no such object in store: {digest}")
        return int(row["refcount"])

    def size_of(self, digest: ContentHash) -> int:
        row = self._conn.execute(
            "SELECT size_bytes FROM cas_objects WHERE hash = ?", (digest,)
        ).fetchone()
        if row is None:
            raise ValidationError(f"no such object in store: {digest}")
        return int(row["size_bytes"])

    def total_size(self) -> int:
        row = self._conn.execute("SELECT coalesce(sum(size_bytes), 0) AS s FROM cas_objects")
        return int(row.fetchone()["s"])

    def iter_evictable(self) -> list[tuple[ContentHash, int]]:
        """Unreferenced objects as (hash, size_bytes), least-recently-accessed first.

        Objects with refcount > 0 are excluded: they belong to a project or an
        in-flight job and must survive eviction.
        """
        rows = self._conn.execute(
            """
            SELECT hash, size_bytes FROM cas_objects
            WHERE refcount = 0
            ORDER BY last_accessed_at ASC
            """
        ).fetchall()
        return [(ContentHash(row["hash"]), int(row["size_bytes"])) for row in rows]

    def forget(self, digest: ContentHash) -> None:
        """Delete the object's file and its row. Idempotent."""
        self.path_for(digest).unlink(missing_ok=True)
        with transaction(self._conn):
            self._conn.execute("DELETE FROM cas_objects WHERE hash = ?", (digest,))
