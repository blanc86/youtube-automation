"""LRU eviction for the content-addressed store.

The target machine has ~84 GB free. Without a ceiling and an evictor, batch
operation fills the disk within weeks. Objects with refcount > 0 belong to a
project or an in-flight job and are never evicted.
"""

from __future__ import annotations

import shutil
import time
from dataclasses import dataclass
from pathlib import Path

from ytauto.infra.cas.store import CasStore

MAX_CEILING_BYTES = 40 * 1024**3
_FREE_FRACTION = 0.40
_STAGING_SUFFIX = ".tmp"
_DEFAULT_MIN_AGE_S = 900.0


@dataclass(frozen=True)
class EvictionPolicy:
    max_bytes: int

    @classmethod
    def compute(cls, cas_root: Path, current_size: int) -> EvictionPolicy:
        """Ceiling = min(40 GiB, 40% of (free space + what the cache already holds)).

        Including ``current_size`` keeps the ceiling stable as the cache grows;
        computing against raw free space alone makes it shrink on every run.

        Raises:
            OSError: ``cas_root`` cannot be created, or its free space cannot
                be queried.
        """
        cas_root.mkdir(parents=True, exist_ok=True)
        free = shutil.disk_usage(cas_root).free
        budget = int((free + current_size) * _FREE_FRACTION)
        return cls(max_bytes=max(1, min(MAX_CEILING_BYTES, budget)))


@dataclass(frozen=True)
class EvictionReport:
    evicted: int
    bytes_freed: int
    bytes_remaining: int


@dataclass(frozen=True)
class SweepReport:
    orphan_blobs: int
    orphan_bytes: int
    stale_staging: int
    stale_staging_bytes: int
    phantom_rows: int


class Evictor:
    def __init__(self, store: CasStore, policy: EvictionPolicy) -> None:
        self._store = store
        self._policy = policy

    def run(self) -> EvictionReport:
        """Evict least-recently-used unreferenced objects until under the ceiling.

        Raises:
            OSError: an object's file cannot be removed.
            sqlite3.Error: the store cannot be queried, or a row cannot be
                deleted.
        """
        total = self._store.total_size()
        if total <= self._policy.max_bytes:
            return EvictionReport(evicted=0, bytes_freed=0, bytes_remaining=total)

        freed = 0
        evicted = 0
        for digest, size in self._store.iter_evictable():
            if total - freed <= self._policy.max_bytes:
                break
            self._store.forget(digest)
            freed += size
            evicted += 1

        return EvictionReport(evicted=evicted, bytes_freed=freed, bytes_remaining=total - freed)

    def sweep_orphans(self, *, min_age_s: float = _DEFAULT_MIN_AGE_S) -> SweepReport:
        """Reclaim blobs with no row and staging files from dead workers.

        ``min_age_s`` is a correctness requirement, not tuning: put_bytes writes
        the file before recording the row, so a blob written moments ago is
        indistinguishable from an orphan. Only files older than the threshold
        are touched, which makes this safe to run while workers are writing.

        ``known`` is a snapshot taken once at the start of the walk. Because
        put_bytes is idempotent, a second writer can record a row for a file
        already on disk after the snapshot was taken but before the loop
        reaches that file - so each orphan candidate is re-checked against the
        database immediately before it is unlinked, closing the window a stale
        snapshot alone would leave open. Even that re-check has its own
        (much smaller) window, so any row that still ends up pointing at a
        missing file is cleaned up afterwards by forget_rows_without_files().

        Raises:
            OSError: if the cache directory cannot be walked, or a file exists
                but cannot be removed (on Windows, typically because another
                process holds it open).
        """
        known = self._store.known_digests()
        cutoff = time.time() - min_age_s
        orphans = orphan_bytes = staging = staging_bytes = 0

        for path in self._store.root.glob("*/*/*"):
            if not path.is_file():
                continue
            try:
                stat = path.stat()
            except FileNotFoundError:
                continue  # a concurrent sweep or writer got there first
            if stat.st_mtime > cutoff:
                continue

            if path.name.endswith(_STAGING_SUFFIX):
                path.unlink(missing_ok=True)
                staging += 1
                staging_bytes += stat.st_size
            elif path.name not in known and not self._store.has_row(path.name):
                path.unlink(missing_ok=True)
                orphans += 1
                orphan_bytes += stat.st_size

        phantom_rows = self._store.forget_rows_without_files()

        return SweepReport(
            orphan_blobs=orphans,
            orphan_bytes=orphan_bytes,
            stale_staging=staging,
            stale_staging_bytes=staging_bytes,
            phantom_rows=phantom_rows,
        )
