from pathlib import Path

import pytest

from ytauto.core.errors import ValidationError
from ytauto.infra.cas.store import CasStore, hash_bytes, hash_file


def test_store_re_exports_the_hashing_primitives() -> None:
    """The primitives now live in core; this import path must keep working."""
    from ytauto.core.models.content_hash import hash_bytes as core_hash_bytes
    from ytauto.core.models.content_hash import hash_file as core_hash_file

    assert hash_bytes is core_hash_bytes
    assert hash_file is core_hash_file


def test_put_bytes_stores_and_returns_content(store: CasStore) -> None:
    digest = store.put_bytes(b"narration", kind="audio")
    assert store.exists(digest)
    assert store.read_bytes(digest) == b"narration"


def test_path_is_sharded_two_levels(store: CasStore) -> None:
    digest = store.put_bytes(b"x", kind="blob")
    path = store.path_for(digest)
    assert path.parent.name == digest[2:4]
    assert path.parent.parent.name == digest[0:2]


def test_identical_content_is_stored_once(store: CasStore) -> None:
    first = store.put_bytes(b"same", kind="audio")
    second = store.put_bytes(b"same", kind="audio")
    assert first == second
    assert store.total_size() == len(b"same")


def test_put_file_copies_by_default(store: CasStore, tmp_path: Path) -> None:
    src = tmp_path / "in.wav"
    src.write_bytes(b"wavdata")
    digest = store.put_file(src, kind="audio")
    assert src.exists(), "copy mode must leave the source in place"
    assert store.read_bytes(digest) == b"wavdata"


def test_put_file_with_move_removes_source(store: CasStore, tmp_path: Path) -> None:
    src = tmp_path / "in.wav"
    src.write_bytes(b"wavdata")
    store.put_file(src, kind="audio", move=True)
    assert not src.exists()


def test_put_file_with_move_removes_source_when_content_already_stored(
    store: CasStore, tmp_path: Path
) -> None:
    """The dedup-hit branch must still consume the source file."""
    store.put_bytes(b"wavdata", kind="audio")

    src = tmp_path / "duplicate.wav"
    src.write_bytes(b"wavdata")
    digest = store.put_file(src, kind="audio", move=True)

    assert not src.exists(), "move=True must consume the source even on a dedup hit"
    assert store.read_bytes(digest) == b"wavdata"


def test_refcount_starts_at_zero_and_tracks_retain_release(store: CasStore) -> None:
    digest = store.put_bytes(b"tracked", kind="blob")
    assert store.refcount(digest) == 0
    store.retain(digest)
    store.retain(digest)
    assert store.refcount(digest) == 2
    store.release(digest)
    assert store.refcount(digest) == 1


def test_release_never_goes_negative(store: CasStore) -> None:
    digest = store.put_bytes(b"floor", kind="blob")
    store.release(digest)
    assert store.refcount(digest) == 0


def test_total_size_sums_distinct_objects(store: CasStore) -> None:
    store.put_bytes(b"aaa", kind="blob")
    store.put_bytes(b"bbbb", kind="blob")
    assert store.total_size() == 7


def test_unknown_hash_raises_validation_error(store: CasStore) -> None:
    with pytest.raises(ValidationError):
        store.read_bytes("f" * 64)  # type: ignore[arg-type]


def test_malformed_hash_raises_validation_error(store: CasStore) -> None:
    with pytest.raises(ValidationError):
        store.path_for("not-a-hash")  # type: ignore[arg-type]


def test_refcount_mutators_reject_a_well_formed_but_unknown_digest(store: CasStore) -> None:
    """These used to UPDATE ... WHERE hash = ? and silently match nothing.

    retain() is the only thing protecting an in-flight job's assets from the
    evictor. A no-op on a wrong or already-evicted digest reports success, the
    job proceeds, and the evictor deletes an asset the render depends on - data
    loss that surfaces much later as a mid-render missing file.
    """
    unknown = "b" * 64
    for mutate in (store.retain, store.release, store.touch):
        with pytest.raises(ValidationError, match="no such object"):
            mutate(unknown)  # type: ignore[arg-type]
    with pytest.raises(ValidationError, match="no such object"):
        store.set_last_accessed(unknown, "2026-01-01T00:00:00+00:00")  # type: ignore[arg-type]


def test_refcount_mutators_reject_a_malformed_digest(store: CasStore) -> None:
    """The read side (refcount/size_of/read_bytes) already raises here; the
    write side silently accepted anything at all, including "oops".
    """
    for mutate in (store.retain, store.release, store.touch):
        with pytest.raises(ValidationError, match="not a valid sha256"):
            mutate("oops")  # type: ignore[arg-type]
    with pytest.raises(ValidationError, match="not a valid sha256"):
        store.set_last_accessed("oops", "2026-01-01T00:00:00+00:00")  # type: ignore[arg-type]


def test_a_rejected_retain_leaves_the_refcount_untouched(store: CasStore) -> None:
    """The failed UPDATE must roll back, not half-apply."""
    digest = store.put_bytes(b"real", kind="blob")
    store.retain(digest)
    with pytest.raises(ValidationError):
        store.retain("c" * 64)  # type: ignore[arg-type]
    assert store.refcount(digest) == 1


def test_iter_evictable_orders_least_recently_used_first(store: CasStore) -> None:
    recent = store.put_bytes(b"recent", kind="blob")
    stale = store.put_bytes(b"stale", kind="blob")
    store.set_last_accessed(recent, "2026-01-01T00:00:00+00:00")
    store.set_last_accessed(stale, "2020-01-01T00:00:00+00:00")

    order = [digest for digest, _size in store.iter_evictable()]

    assert order == [stale, recent]


def test_iter_evictable_excludes_retained_objects(store: CasStore) -> None:
    pinned = store.put_bytes(b"pinned", kind="blob")
    loose = store.put_bytes(b"loose", kind="blob")
    store.retain(pinned)

    assert [digest for digest, _size in store.iter_evictable()] == [loose]


def test_forget_removes_file_and_row(store: CasStore) -> None:
    digest = store.put_bytes(b"doomed", kind="blob")
    store.forget(digest)

    assert not store.exists(digest)
    with pytest.raises(ValidationError):
        store.refcount(digest)


def test_forget_is_idempotent(store: CasStore) -> None:
    digest = store.put_bytes(b"doomed", kind="blob")
    store.forget(digest)
    store.forget(digest)
    assert not store.exists(digest)
