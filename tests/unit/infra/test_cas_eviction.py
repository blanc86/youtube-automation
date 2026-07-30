from pathlib import Path

import pytest

from ytauto.core.errors import ValidationError
from ytauto.infra.cas.eviction import MAX_CEILING_BYTES, EvictionPolicy, Evictor
from ytauto.infra.cas.store import CasStore

# `store` comes from tests/unit/infra/conftest.py — do NOT redefine it here.

OLD = "2020-01-01T00:00:00+00:00"
NEW = "2026-01-01T00:00:00+00:00"


def test_ceiling_is_capped_at_40_gib(tmp_path: Path) -> None:
    policy = EvictionPolicy.compute(tmp_path, current_size=0)
    assert policy.max_bytes <= MAX_CEILING_BYTES


def test_ceiling_is_positive(tmp_path: Path) -> None:
    assert EvictionPolicy.compute(tmp_path, current_size=0).max_bytes > 0


def test_nothing_evicted_when_under_ceiling(store: CasStore) -> None:
    store.put_bytes(b"small", kind="blob")
    report = Evictor(store, EvictionPolicy(max_bytes=1_000_000)).run()
    assert report.evicted == 0
    assert report.bytes_freed == 0


def test_evicts_least_recently_used_first(store: CasStore) -> None:
    old = store.put_bytes(b"0123456789", kind="blob")  # 10 bytes
    new = store.put_bytes(b"abcdefghij", kind="blob")  # 10 bytes
    store.set_last_accessed(old, OLD)
    store.set_last_accessed(new, NEW)

    report = Evictor(store, EvictionPolicy(max_bytes=10)).run()

    assert report.evicted == 1
    assert not store.exists(old)
    assert store.exists(new)


def test_retained_objects_are_never_evicted(store: CasStore) -> None:
    pinned = store.put_bytes(b"0123456789", kind="blob")
    store.retain(pinned)
    store.set_last_accessed(pinned, OLD)

    report = Evictor(store, EvictionPolicy(max_bytes=0)).run()

    assert report.evicted == 0
    assert store.exists(pinned)


def test_stops_as_soon_as_it_is_under_the_ceiling(store: CasStore) -> None:
    """Eviction must free just enough, not empty the cache."""
    oldest = store.put_bytes(b"0123456789", kind="blob")
    middle = store.put_bytes(b"abcdefghij", kind="blob")
    newest = store.put_bytes(b"klmnopqrst", kind="blob")
    store.set_last_accessed(oldest, "2020-01-01T00:00:00+00:00")
    store.set_last_accessed(middle, "2023-01-01T00:00:00+00:00")
    store.set_last_accessed(newest, NEW)

    report = Evictor(store, EvictionPolicy(max_bytes=20)).run()

    assert report.evicted == 1
    assert not store.exists(oldest)
    assert store.exists(middle)
    assert store.exists(newest)


def test_report_totals_are_accurate(store: CasStore) -> None:
    a = store.put_bytes(b"0123456789", kind="blob")
    store.put_bytes(b"abcdefghij", kind="blob")
    store.set_last_accessed(a, OLD)

    report = Evictor(store, EvictionPolicy(max_bytes=10)).run()

    assert report.bytes_freed == 10
    assert report.bytes_remaining == 10
    assert store.total_size() == 10


def test_database_rows_are_removed_with_the_files(store: CasStore) -> None:
    digest = store.put_bytes(b"0123456789", kind="blob")
    store.set_last_accessed(digest, OLD)

    Evictor(store, EvictionPolicy(max_bytes=0)).run()

    assert not store.exists(digest)
    with pytest.raises(ValidationError):
        store.refcount(digest)
