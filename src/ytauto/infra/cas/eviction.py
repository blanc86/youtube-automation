"""LRU eviction for the content-addressed store.

The target machine has ~84 GB free. Without a ceiling and an evictor, batch
operation fills the disk within weeks. Objects with refcount > 0 belong to a
project or an in-flight job and are never evicted.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

from ytauto.infra.cas.store import CasStore

MAX_CEILING_BYTES = 40 * 1024**3
_FREE_FRACTION = 0.40


@dataclass(frozen=True)
class EvictionPolicy:
    max_bytes: int

    @classmethod
    def compute(cls, cas_root: Path, current_size: int) -> EvictionPolicy:
        """Ceiling = min(40 GiB, 40% of (free space + what the cache already holds)).

        Including ``current_size`` keeps the ceiling stable as the cache grows;
        computing against raw free space alone makes it shrink on every run.
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


class Evictor:
    def __init__(self, store: CasStore, policy: EvictionPolicy) -> None:
        self._store = store
        self._policy = policy

    def run(self) -> EvictionReport:
        """Evict least-recently-used unreferenced objects until under the ceiling."""
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
