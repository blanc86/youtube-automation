import os
import sqlite3
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from ytauto.core.errors import ValidationError
from ytauto.core.models.content_hash import ContentHash
from ytauto.infra.cas.eviction import MAX_CEILING_BYTES, EvictionPolicy, Evictor, SweepReport
from ytauto.infra.cas.store import CasStore

# `store` comes from tests/unit/infra/conftest.py — do NOT redefine it here.

OLD = "2020-01-01T00:00:00+00:00"
NEW = "2026-01-01T00:00:00+00:00"


def _age_file(path: Path, *, seconds: float) -> None:
    """Backdate a file's mtime so age-based sweeping is deterministic."""
    past = time.time() - seconds
    os.utime(path, (past, past))


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


def test_sweep_removes_a_blob_with_no_row(store: CasStore, db_conn: sqlite3.Connection) -> None:
    digest = store.put_bytes(b"orphaned", kind="blob")
    path = store.path_for(digest)
    db_conn.execute("DELETE FROM cas_objects WHERE hash = ?", (digest,))
    _age_file(path, seconds=3600)

    report = Evictor(store, EvictionPolicy(max_bytes=10**9)).sweep_orphans()

    assert report.orphan_blobs == 1
    assert report.orphan_bytes == len(b"orphaned")
    assert not path.exists()


def test_sweep_keeps_blobs_that_have_rows(store: CasStore) -> None:
    digest = store.put_bytes(b"legitimate", kind="blob")
    _age_file(store.path_for(digest), seconds=3600)

    report = Evictor(store, EvictionPolicy(max_bytes=10**9)).sweep_orphans()

    assert report.orphan_blobs == 0
    assert store.exists(digest)


def test_sweep_spares_recently_written_blobs(store: CasStore, db_conn: sqlite3.Connection) -> None:
    """put_bytes writes the file before recording the row; a blob written
    microseconds ago is indistinguishable from an orphan."""
    digest = store.put_bytes(b"just written", kind="blob")
    path = store.path_for(digest)
    db_conn.execute("DELETE FROM cas_objects WHERE hash = ?", (digest,))

    report = Evictor(store, EvictionPolicy(max_bytes=10**9)).sweep_orphans()

    assert report.orphan_blobs == 0, "a fresh blob must not be swept"
    assert path.exists()


def test_sweep_removes_stale_staging_files(store: CasStore) -> None:
    shard = store.path_for("a" * 64).parent  # type: ignore[arg-type]
    shard.mkdir(parents=True, exist_ok=True)
    stale = shard / f"{'a' * 64}.99999.tmp"
    stale.write_bytes(b"partial")
    _age_file(stale, seconds=3600)

    report = Evictor(store, EvictionPolicy(max_bytes=10**9)).sweep_orphans()

    assert report.stale_staging == 1
    assert report.stale_staging_bytes == len(b"partial")
    assert not stale.exists()


def test_sweep_spares_recent_staging_files(store: CasStore) -> None:
    """A live worker's in-progress write must survive a concurrent sweep."""
    shard = store.path_for("b" * 64).parent  # type: ignore[arg-type]
    shard.mkdir(parents=True, exist_ok=True)
    fresh = shard / f"{'b' * 64}.{os.getpid()}.tmp"
    fresh.write_bytes(b"in progress")

    report = Evictor(store, EvictionPolicy(max_bytes=10**9)).sweep_orphans()

    assert report.stale_staging == 0
    assert fresh.exists()


def test_forget_deletes_the_row_before_unlinking(
    store: CasStore, db_conn: sqlite3.Connection
) -> None:
    """Ordering matters: a crash between the two must leave a reclaimable
    orphan, never a phantom row that makes total_size() overcount."""
    digest = store.put_bytes(b"doomed", kind="blob")
    observed: list[bool] = []
    original_unlink = Path.unlink

    def _spy(self: Path, *args: object, **kwargs: object) -> None:
        observed.append(
            db_conn.execute("SELECT 1 FROM cas_objects WHERE hash = ?", (digest,)).fetchone()
            is None
        )
        original_unlink(self, *args, **kwargs)  # type: ignore[arg-type]

    with patch.object(Path, "unlink", _spy):
        store.forget(digest)

    assert observed == [True], "row must already be gone when unlink runs"


def test_sweep_rechecks_before_unlinking_when_snapshot_is_stale(
    store: CasStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Simulates the TOCTOU race: known_digests() is a snapshot taken once at
    sweep start, but put_bytes is idempotent, so a second writer can record a
    row for an already-existing file after the snapshot was taken but before
    the loop reaches that file. A sweep that trusts the stale snapshot alone
    would delete a file whose row genuinely exists by the time it gets there -
    recreating the exact phantom-row state forget()'s reordering exists to
    prevent. The per-candidate has_row() re-check is what closes this window."""
    digest = store.put_bytes(b"raced", kind="blob")
    path = store.path_for(digest)
    _age_file(path, seconds=3600)
    monkeypatch.setattr(store, "known_digests", lambda: frozenset())

    report = Evictor(store, EvictionPolicy(max_bytes=10**9)).sweep_orphans()

    assert report.orphan_blobs == 0
    assert path.exists()


def test_sweep_reclaims_a_row_whose_file_vanished(store: CasStore) -> None:
    """The mirror of an orphan blob: a row with no backing file is never a
    legitimate transient state (both put paths write the file first), so it
    is reclaimed unconditionally - no age guard needed."""
    digest = store.put_bytes(b"vanished", kind="blob")
    store.path_for(digest).unlink()

    report = Evictor(store, EvictionPolicy(max_bytes=10**9)).sweep_orphans()

    assert report.phantom_rows == 1
    with pytest.raises(ValidationError):
        store.refcount(digest)


def test_healthy_store_sweeps_to_all_zeros(store: CasStore) -> None:
    """No false positives: a blob whose row and file both exist and are
    fresh must not be touched by any part of the sweep."""
    store.put_bytes(b"healthy", kind="blob")

    report = Evictor(store, EvictionPolicy(max_bytes=10**9)).sweep_orphans()

    assert report == SweepReport(
        orphan_blobs=0,
        orphan_bytes=0,
        stale_staging=0,
        stale_staging_bytes=0,
        phantom_rows=0,
    )


def test_a_blob_pinned_after_the_snapshot_survives_eviction(
    store: CasStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Evictor reads iter_evictable() outside any transaction and acts on it
    later, so a retain() can land in between. Deleting anyway destroys an asset a
    running job depends on - the exact loss retain() exists to prevent. Patching
    iter_evictable to pin on its way out reproduces that interleaving without
    threads."""
    digest = store.put_bytes(b"z" * 5000, kind="blob")
    real = store.iter_evictable

    def pin_after_listing() -> list[tuple[ContentHash, int]]:
        listed = real()
        store.retain(digest)
        return listed

    monkeypatch.setattr(store, "iter_evictable", pin_after_listing)
    report = Evictor(store, EvictionPolicy(max_bytes=1)).run()

    assert store.exists(digest), "a pinned blob's file must survive"
    assert store.refcount(digest) == 1, "and so must its row"
    assert report.evicted == 0
    assert report.bytes_freed == 0


def test_forget_if_unreferenced_reports_what_it_did(store: CasStore) -> None:
    unheld = store.put_bytes(b"free", kind="blob")
    held = store.put_bytes(b"pinned", kind="blob")
    store.retain(held)

    assert store.forget_if_unreferenced(unheld) is True
    assert not store.exists(unheld)

    assert store.forget_if_unreferenced(held) is False
    assert store.exists(held), "a refused delete must not touch the file"
    assert store.refcount(held) == 1


def test_an_orphan_dated_in_the_future_is_still_swept(store: CasStore) -> None:
    """A blob's timestamp can read ahead of the wall clock.

    NTFS records mtime to 100ns while the Windows system clock advances in
    ~15.6ms ticks, so a file written microseconds ago can carry an mtime later
    than ``time.time()``. The age check used to compare ``st_mtime`` against a
    cutoff directly, which made that file's age negative and skipped it - and
    with ``min_age_s=0``, which reads as "no age restriction", it was skipped
    permanently rather than merely deferred.

    This is not hypothetical clock-skew paranoia: the same test passed on one
    Windows machine and failed on another with ``assert 0 == 1``, and the
    difference was which side of the tick the two clocks landed on.
    """
    digest = store.stage_file(b"an orphan from the future", kind="blob")
    path = store.path_for(digest)

    # Explicitly dated ahead, which is the condition that has to be tolerated.
    future = time.time() + 30.0
    os.utime(path, (future, future))

    report = Evictor(store, EvictionPolicy(max_bytes=1)).sweep_orphans(min_age_s=0)

    assert report.orphan_blobs == 1, "a future-dated orphan must still be reclaimable"
    assert not path.is_file()


def test_a_minimum_age_still_protects_a_freshly_written_blob(store: CasStore) -> None:
    """The clamp must not turn min_age_s into a no-op: a future-dated file is
    brand new, so a non-zero minimum age must still leave it alone."""
    digest = store.stage_file(b"too young to reap", kind="blob")
    path = store.path_for(digest)
    future = time.time() + 30.0
    os.utime(path, (future, future))

    report = Evictor(store, EvictionPolicy(max_bytes=1)).sweep_orphans(min_age_s=300)

    assert report.orphan_blobs == 0
    assert path.is_file()
