"""Content-addressed blob storage.

Objects are named by the SHA-256 of their contents, so identical bytes are
stored exactly once regardless of how many projects reference them. Refcounts
protect in-use objects from the evictor.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
from pathlib import Path

from ytauto.core.errors import ValidationError
from ytauto.core.models.content_hash import (
    ContentHash,
    hash_bytes,
    hash_file,
    validate_digest,
)
from ytauto.infra.clock import utc_now_iso
from ytauto.infra.db.engine import transaction

# Re-exported: the hashing primitives moved to core (Phase 1 needs them there,
# and core cannot import infra), but the store remains their natural entry
# point for callers that are already talking to the CAS.
__all__ = ["CasStore", "ContentHash", "hash_bytes", "hash_file"]


class CasStore:
    """Blob storage addressed by content hash, with refcounts held in SQLite."""

    def __init__(self, root: Path, conn: sqlite3.Connection) -> None:
        """Open a store rooted at ``root``, creating the directory if needed.

        Raises:
            OSError: ``root`` cannot be created.
        """
        self._root = root
        self._conn = conn
        self._root.mkdir(parents=True, exist_ok=True)

    @property
    def root(self) -> Path:
        """The content-addressed store's base directory."""
        return self._root

    def path_for(self, digest: ContentHash) -> Path:
        """Compute the sharded on-disk path for a digest. Does not touch disk.

        Raises:
            ValidationError: ``digest`` is not a valid sha256 hex digest.
        """
        valid = validate_digest(digest)
        return self._root / valid[0:2] / valid[2:4] / valid

    def exists(self, digest: ContentHash) -> bool:
        """Whether the object's file is present.

        Raises:
            ValidationError: ``digest`` is not a valid sha256 hex digest.
        """
        return self.path_for(digest).is_file()

    def _staging_path(self, target: Path) -> Path:
        """A tmp name unique per process.

        Phase 1 runs several worker subprocesses against this store; a shared
        ``.tmp`` name would let two of them corrupt each other's partial write.
        """
        return target.with_name(f"{target.name}.{os.getpid()}.tmp")

    def put_bytes(self, data: bytes, *, kind: str) -> ContentHash:
        """Store an in-memory buffer, deduplicating on content. Idempotent.

        Raises:
            OSError: the staging file cannot be written, or the atomic replace
                into place fails.
            sqlite3.Error: the refcount row cannot be recorded.
        """
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
        """Store a file's contents. With ``move=True`` the source is consumed.

        Raises:
            ValidationError: ``src`` does not exist or is not a regular file.
            OSError: ``src`` cannot be read, or the copy/move/replace fails.
            sqlite3.Error: the refcount row cannot be recorded.
        """
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
        """Read an object's full contents into memory.

        Raises:
            ValidationError: ``digest`` is malformed, or no such object is
                stored.
            OSError: the object's file exists but cannot be read.
        """
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
        """Mark the object as accessed now, deferring the eviction that follows.

        Raises:
            ValidationError: ``digest`` is malformed, or names no stored object.
        """
        self.set_last_accessed(digest, utc_now_iso())

    def set_last_accessed(self, digest: ContentHash, timestamp: str) -> None:
        """Set the access time explicitly.

        Public because the evictor's LRU ordering must be assertable in tests,
        and because restore-from-backup needs to preserve original access times.

        Raises:
            ValidationError: ``digest`` is malformed, or names no stored object.
        """
        self._update_one(
            "UPDATE cas_objects SET last_accessed_at = ? WHERE hash = ?",
            (timestamp, validate_digest(digest)),
            digest,
        )

    def retain(self, digest: ContentHash) -> None:
        """Pin the object against eviction. Every retain needs a later release.

        Raises:
            ValidationError: ``digest`` is malformed, or names no stored object.
        """
        self._update_one(
            "UPDATE cas_objects SET refcount = refcount + 1 WHERE hash = ?",
            (validate_digest(digest),),
            digest,
        )

    def release(self, digest: ContentHash) -> None:
        """Drop one pin. Clamped at zero, so an extra release cannot go negative.

        Raises:
            ValidationError: ``digest`` is malformed, or names no stored object.
        """
        self._update_one(
            "UPDATE cas_objects SET refcount = max(0, refcount - 1) WHERE hash = ?",
            (validate_digest(digest),),
            digest,
        )

    def _update_one(self, sql: str, params: tuple[object, ...], digest: str) -> None:
        """Run an UPDATE that must match exactly one row.

        Without the rowcount check these mutators silently no-op on an unknown
        hash. That matters most for retain(): it is the only thing protecting an
        in-flight job's assets from the evictor, so a retain against a wrong or
        already-evicted digest would report success, the job would proceed, and
        the evictor would delete an asset a running render depends on - silent
        data loss surfacing much later as a mid-render missing file.
        """
        with transaction(self._conn):
            cursor = self._conn.execute(sql, params)
            if cursor.rowcount == 0:
                raise ValidationError(f"no such object in store: {digest}")

    def refcount(self, digest: ContentHash) -> int:
        """How many holders currently pin this object.

        Raises:
            ValidationError: no such object is stored.
            sqlite3.Error: the query fails.
        """
        row = self._conn.execute(
            "SELECT refcount FROM cas_objects WHERE hash = ?", (digest,)
        ).fetchone()
        if row is None:
            raise ValidationError(f"no such object in store: {digest}")
        return int(row["refcount"])

    def size_of(self, digest: ContentHash) -> int:
        """The object's recorded size in bytes.

        Raises:
            ValidationError: no such object is stored.
            sqlite3.Error: the query fails.
        """
        row = self._conn.execute(
            "SELECT size_bytes FROM cas_objects WHERE hash = ?", (digest,)
        ).fetchone()
        if row is None:
            raise ValidationError(f"no such object in store: {digest}")
        return int(row["size_bytes"])

    def total_size(self) -> int:
        """Total recorded bytes across every stored object.

        Raises:
            sqlite3.Error: the query fails.
        """
        row = self._conn.execute("SELECT coalesce(sum(size_bytes), 0) AS s FROM cas_objects")
        return int(row.fetchone()["s"])

    def iter_evictable(self) -> list[tuple[ContentHash, int]]:
        """Unreferenced objects as (hash, size_bytes), least-recently-accessed first.

        Objects with refcount > 0 are excluded: they belong to a project or an
        in-flight job and must survive eviction.

        Raises:
            sqlite3.Error: the query fails.
        """
        rows = self._conn.execute(
            """
            SELECT hash, size_bytes FROM cas_objects
            WHERE refcount = 0
            ORDER BY last_accessed_at ASC
            """
        ).fetchall()
        return [(ContentHash(row["hash"]), int(row["size_bytes"])) for row in rows]

    def known_digests(self) -> frozenset[ContentHash]:
        """Every digest with a row in ``cas_objects``.

        Used by the evictor's orphan sweep to tell recorded blobs from garbage.

        Raises:
            sqlite3.Error: the query fails.
        """
        rows = self._conn.execute("SELECT hash FROM cas_objects").fetchall()
        return frozenset(ContentHash(row["hash"]) for row in rows)

    def forget(self, digest: ContentHash) -> None:
        """Delete the object's row and then its file. Idempotent.

        The row goes first on purpose: a crash between the two steps leaves a
        file with no row, which the orphan sweep reclaims. The reverse order
        would leave a row with no file - which makes total_size() overcount and
        read_bytes() fail for a digest size_of() still answers.

        Raises:
            ValidationError: if ``digest`` is not a valid sha256 hex digest.
            OSError: if the file exists but cannot be removed.
        """
        path = self.path_for(digest)
        with transaction(self._conn):
            self._conn.execute("DELETE FROM cas_objects WHERE hash = ?", (digest,))
        path.unlink(missing_ok=True)
