import sqlite3

import pytest

from ytauto.core.errors import ValidationError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.infra.artifacts import ArtifactStore
from ytauto.infra.cas.store import CasStore

FP = "f" * 64


@pytest.fixture()
def artifacts(store: CasStore, db_conn: sqlite3.Connection) -> ArtifactStore:
    """Both fixtures come from tests/unit/infra/conftest.py and share a connection."""
    return ArtifactStore(cas=store, conn=db_conn)


def _put(store: CasStore, name: str, data: bytes) -> ArtifactRef:
    return ArtifactRef(name=name, kind="blob", digest=store.put_bytes(data, kind="blob"))


def test_lookup_misses_on_an_unknown_fingerprint(artifacts: ArtifactStore) -> None:
    assert artifacts.lookup(FP) is None


def test_record_then_lookup_round_trips(artifacts: ArtifactStore, store: CasStore) -> None:
    ref = _put(store, "narration", b"audio")
    assert artifacts.record(FP, "tts", [ref]) is True
    assert artifacts.lookup(FP) == (ref,)


def test_lookup_returns_several_artifacts_in_name_order(
    artifacts: ArtifactStore, store: CasStore
) -> None:
    timings = _put(store, "timings", b"json")
    narration = _put(store, "narration", b"audio")
    artifacts.record(FP, "tts", [timings, narration])
    assert [a.name for a in artifacts.lookup(FP) or ()] == ["narration", "timings"]


def test_record_retains_each_digest_once(artifacts: ArtifactStore, store: CasStore) -> None:
    """Retaining is what stops the evictor deleting a cached stage output."""
    ref = _put(store, "narration", b"audio")
    assert store.refcount(ref.digest) == 0
    artifacts.record(FP, "tts", [ref])
    assert store.refcount(ref.digest) == 1


def test_recording_the_same_fingerprint_twice_does_not_inflate_refcounts(
    artifacts: ArtifactStore, store: CasStore
) -> None:
    """A resume re-records the same fingerprint. Double-retaining would make
    the artifact permanently unevictable."""
    ref = _put(store, "narration", b"audio")
    assert artifacts.record(FP, "tts", [ref]) is True
    assert artifacts.record(FP, "tts", [ref]) is False
    assert store.refcount(ref.digest) == 1


def test_forget_releases_and_removes(artifacts: ArtifactStore, store: CasStore) -> None:
    ref = _put(store, "narration", b"audio")
    artifacts.record(FP, "tts", [ref])
    artifacts.forget(FP)
    assert artifacts.lookup(FP) is None
    assert store.refcount(ref.digest) == 0


def test_forget_is_idempotent(artifacts: ArtifactStore, store: CasStore) -> None:
    ref = _put(store, "narration", b"audio")
    artifacts.record(FP, "tts", [ref])
    artifacts.forget(FP)
    artifacts.forget(FP)
    assert store.refcount(ref.digest) == 0


def test_recording_no_artifacts_is_rejected(artifacts: ArtifactStore) -> None:
    with pytest.raises(ValidationError, match="no artifacts"):
        artifacts.record(FP, "tts", [])


def test_a_malformed_fingerprint_is_rejected(artifacts: ArtifactStore, store: CasStore) -> None:
    with pytest.raises(ValidationError, match="fingerprint"):
        artifacts.record("not-a-fingerprint", "tts", [_put(store, "n", b"x")])


def test_lookup_rejects_a_malformed_fingerprint(artifacts: ArtifactStore) -> None:
    with pytest.raises(ValidationError, match="fingerprint"):
        artifacts.lookup("nope")


def test_a_failed_record_leaves_no_partial_state(artifacts: ArtifactStore, store: CasStore) -> None:
    """The row write and the retain must land together or not at all."""
    good = _put(store, "narration", b"audio")
    missing = ArtifactRef(name="ghost", kind="blob", digest="c" * 64)  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        artifacts.record(FP, "tts", [good, missing])
    assert artifacts.lookup(FP) is None
    assert store.refcount(good.digest) == 0


def test_lookup_treats_a_vanished_blob_as_a_miss(artifacts: ArtifactStore, store: CasStore) -> None:
    """record() commits rows before retaining blobs, so a crash in that window
    leaves rows pointing at evictable blobs. Reporting a hit for artifacts that
    no longer exist would make the scheduler skip a stage whose output is gone."""
    ref = _put(store, "narration", b"audio")
    artifacts.record(FP, "tts", [ref])
    store.path_for(ref.digest).unlink()

    assert artifacts.lookup(FP) is None


def test_lookup_drops_the_stale_rows_it_finds(
    artifacts: ArtifactStore, store: CasStore, db_conn: sqlite3.Connection
) -> None:
    """Self-healing: a miss caused by a vanished blob must not be re-detected
    on every subsequent lookup."""
    ref = _put(store, "narration", b"audio")
    artifacts.record(FP, "tts", [ref])
    store.path_for(ref.digest).unlink()

    artifacts.lookup(FP)

    remaining = db_conn.execute(
        "SELECT count(*) FROM artifacts WHERE fingerprint = ?", (FP,)
    ).fetchone()[0]
    assert remaining == 0


def test_a_partially_vanished_set_is_a_miss(artifacts: ArtifactStore, store: CasStore) -> None:
    """One missing artifact invalidates the whole stage output, not just itself."""
    narration = _put(store, "narration", b"audio")
    timings = _put(store, "timings", b"json")
    artifacts.record(FP, "tts", [narration, timings])
    store.path_for(timings.digest).unlink()

    assert artifacts.lookup(FP) is None
