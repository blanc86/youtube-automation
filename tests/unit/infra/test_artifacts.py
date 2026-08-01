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


def _row_count(conn: sqlite3.Connection, fingerprint: str) -> int:
    return int(
        conn.execute(
            "SELECT count(*) FROM artifacts WHERE fingerprint = ?", (fingerprint,)
        ).fetchone()[0]
    )


class _FailsOnSecondInsert:
    """A connection stand-in that dies partway through record()'s INSERT loop.

    ``sqlite3.Connection.execute`` is a read-only C attribute, so it cannot be
    patched in place; wrapping the connection is the only way to inject a
    failure *between* two INSERTs of one batch. Everything else - including
    transaction()'s own BEGIN/COMMIT/ROLLBACK - forwards untouched, so the
    rollback under test is the real one.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._inserts = 0

    def execute(self, sql: str, *params: object) -> sqlite3.Cursor:
        if sql.startswith("INSERT INTO artifacts"):
            self._inserts += 1
            if self._inserts == 2:
                raise sqlite3.OperationalError("injected mid-batch failure")
        return self._conn.execute(sql, *params)  # type: ignore[arg-type]


class _InsertViolatesSomeOtherConstraint:
    """A connection stand-in whose INSERTs raise IntegrityError for a non-PK reason.

    Stands in for a constraint a later migration might add to ``artifacts`` (a
    CHECK, a FOREIGN KEY). Everything else forwards untouched, so record()'s
    post-collision row probe sees the real, empty table.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def execute(self, sql: str, *params: object) -> sqlite3.Cursor:
        if sql.startswith("INSERT INTO artifacts"):
            raise sqlite3.IntegrityError("CHECK constraint failed: some_future_rule")
        return self._conn.execute(sql, *params)  # type: ignore[arg-type]


def test_lookup_misses_on_an_unknown_fingerprint(artifacts: ArtifactStore) -> None:
    assert artifacts.lookup(FP) is None


def test_record_then_lookup_round_trips(artifacts: ArtifactStore, store: CasStore) -> None:
    ref = _put(store, "narration", b"audio")
    assert artifacts.record(FP, "tts", [ref]) is True
    assert artifacts.lookup(FP) == (ref,)


def test_lookup_returns_several_artifacts_in_name_order(
    artifacts: ArtifactStore, store: CasStore
) -> None:
    # Deliberate, recorded exception to the guard-pinning rule: the ORDER BY
    # name ASC clause this pins cannot be falsified by deleting it, because
    # PRIMARY KEY (fingerprint, name) indexes exactly that pair and SQLite
    # returns the rows name-ordered anyway. The clause is kept as an explicit
    # guarantee against a future schema or index change silently reordering
    # results. Non-vacuity was instead proven by mutating the clause to
    # ORDER BY rowid (insertion order), which makes this test fail.
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


def test_recording_the_same_fingerprint_twice_is_idempotent(
    artifacts: ArtifactStore, store: CasStore
) -> None:
    """A crash-resume re-records a completed stage's fingerprint. Without the
    already-recorded check that second call would hit PRIMARY KEY
    (fingerprint, name) and kill the worker on every resume; the check converts
    it into the documented False. It is not what keeps refcounts symmetric -
    the primary key rejects the duplicate INSERT before the retain loop runs
    again either way."""
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


def test_a_batch_naming_an_unknown_blob_is_rejected_before_any_write(
    artifacts: ArtifactStore, store: CasStore
) -> None:
    """The blob-existence check is pre-flight: it runs before transaction() is
    ever entered, so one unknown digest rejects the whole batch with nothing
    written and nothing retained - including for the artifacts that were fine.
    This pins rejection ordering, not rollback; rollback is pinned by
    test_a_failure_inside_the_insert_loop_rolls_back_the_whole_batch."""
    good = _put(store, "narration", b"audio")
    missing = ArtifactRef(name="ghost", kind="blob", digest="c" * 64)  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        artifacts.record(FP, "tts", [good, missing])
    assert artifacts.lookup(FP) is None
    assert store.refcount(good.digest) == 0


def test_duplicate_names_in_one_batch_are_rejected(
    artifacts: ArtifactStore, store: CasStore, db_conn: sqlite3.Connection
) -> None:
    """Two artifacts sharing a name violate PRIMARY KEY (fingerprint, name).
    That is malformed input, so it must surface as ValidationError rather than
    an undeclared IntegrityError - and it must not be swallowed as False by the
    concurrent-writer handler, which is why the check is pre-flight."""
    first = _put(store, "narration", b"audio")
    second = ArtifactRef(
        name="narration", kind="blob", digest=store.put_bytes(b"other audio", kind="blob")
    )

    with pytest.raises(ValidationError, match="duplicate artifact name"):
        artifacts.record(FP, "tts", [first, second])

    assert _row_count(db_conn, FP) == 0
    assert store.refcount(first.digest) == 0
    assert store.refcount(second.digest) == 0


def test_record_returns_false_when_a_concurrent_writer_won_the_race(
    artifacts: ArtifactStore, store: CasStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """record() reads its already-recorded check in one transaction and INSERTs
    in another, holding no lock across the gap, so a second worker can commit in
    between - normal, since cross-project dedup means shared fingerprints are
    expected. Monkeypatching lookup to miss reproduces the loser's stale
    pre-commit snapshot deterministically, without threads. The loser must
    return False, not raise, and must not retain: the winner holds the pin."""
    ref = _put(store, "narration", b"audio")
    assert artifacts.record(FP, "tts", [ref]) is True

    monkeypatch.setattr(artifacts, "lookup", lambda fingerprint: None)

    assert artifacts.record(FP, "tts", [ref]) is False
    assert store.refcount(ref.digest) == 1, "the loser must not retain a second time"


def test_an_integrity_error_that_is_not_a_collision_is_re_raised(
    artifacts: ArtifactStore, store: CasStore, db_conn: sqlite3.Connection
) -> None:
    """Reporting False means 'somebody else already recorded this'. An
    IntegrityError from any other constraint - one a future migration might add
    to this table - is not that, and swallowing it would tell the caller its
    stage output is cached when nothing was ever written. The handler proves the
    claim by checking the table instead of assuming the primary key is the only
    constraint that can fire."""
    ref = _put(store, "narration", b"audio")
    artifacts._conn = _InsertViolatesSomeOtherConstraint(db_conn)  # type: ignore[assignment]

    with pytest.raises(sqlite3.IntegrityError, match="some_future_rule"):
        artifacts.record(FP, "tts", [ref])

    assert _row_count(db_conn, FP) == 0
    assert store.refcount(ref.digest) == 0


def test_a_failure_inside_the_insert_loop_rolls_back_the_whole_batch(
    artifacts: ArtifactStore, store: CasStore, db_conn: sqlite3.Connection
) -> None:
    """The rows of one batch land together or not at all. Failing the second
    INSERT of two must leave the first one gone too - in autocommit it would
    already be committed and permanent."""
    narration = _put(store, "narration", b"audio")
    timings = _put(store, "timings", b"json")
    artifacts._conn = _FailsOnSecondInsert(db_conn)  # type: ignore[assignment]

    with pytest.raises(sqlite3.OperationalError):
        artifacts.record(FP, "tts", [narration, timings])

    assert _row_count(db_conn, FP) == 0, "the first INSERT must have rolled back too"
    assert store.refcount(narration.digest) == 0


def test_forget_tolerates_a_blob_whose_cas_row_already_vanished(
    artifacts: ArtifactStore, store: CasStore, db_conn: sqlite3.Connection
) -> None:
    """forget() commits its row deletes before releasing, so a release that
    raises leaves the caller with 'rows gone, blobs half-released' behind an
    exception that reads as 'bad input, nothing happened'. A missing
    cas_objects row is the end state forget() wants anyway, so it is tolerated."""
    ref = _put(store, "narration", b"audio")
    artifacts.record(FP, "tts", [ref])
    db_conn.execute("DELETE FROM cas_objects WHERE hash = ?", (ref.digest,))

    artifacts.forget(FP)

    assert _row_count(db_conn, FP) == 0


def test_recording_with_a_blank_stage_id_is_rejected(
    artifacts: ArtifactStore, store: CasStore
) -> None:
    """An empty artifact list is rejected two lines below; a stage_id that
    identifies no stage is no more usable to a resuming scheduler."""
    ref = _put(store, "narration", b"audio")
    with pytest.raises(ValidationError, match="stage_id"):
        artifacts.record(FP, "   ", [ref])


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
