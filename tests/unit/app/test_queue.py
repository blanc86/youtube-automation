import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from ytauto.app.scheduler.queue import JobQueue
from ytauto.infra.db.engine import connect, transaction
from ytauto.infra.db.migrations import apply_migrations


@pytest.fixture()
def queue(db_conn: sqlite3.Connection) -> JobQueue:
    return JobQueue(db_conn)


def test_claim_returns_none_on_an_empty_queue(queue: JobQueue) -> None:
    assert queue.claim("w1", lease_s=60) is None


def test_enqueue_then_claim_round_trips(queue: JobQueue) -> None:
    queue.enqueue("j1", "p1", "pipe")
    claimed = queue.claim("w1", lease_s=60)
    assert claimed is not None
    assert claimed.job_id == "j1"
    assert claimed.attempts == 1


def test_only_one_of_two_claimers_wins(queue: JobQueue) -> None:
    """Two workers racing for one job. The loser must get None, not the same job."""
    queue.enqueue("j1", "p1", "pipe")
    first = queue.claim("w1", lease_s=60)
    second = queue.claim("w2", lease_s=60)
    assert first is not None and second is None


def test_higher_priority_is_claimed_first(queue: JobQueue) -> None:
    queue.enqueue("low", "p1", "pipe", priority=0)
    queue.enqueue("high", "p1", "pipe", priority=10)
    claimed = queue.claim("w1", lease_s=60)
    assert claimed is not None
    assert claimed.job_id == "high"


def test_a_job_deferred_by_available_at_is_not_claimable(queue: JobQueue) -> None:
    """This is what makes ErrorKind.RATE_LIMITED and retry_after_s real."""
    queue.enqueue("j1", "p1", "pipe")
    queue.requeue("j1", available_in_s=3600)
    assert queue.claim("w1", lease_s=60) is None


def test_a_deferred_job_becomes_claimable_once_its_time_passes(queue: JobQueue) -> None:
    queue.enqueue("j1", "p1", "pipe")
    queue.requeue("j1", available_in_s=-1)  # already due
    assert queue.claim("w1", lease_s=60) is not None


def test_an_expired_lease_is_reaped_and_the_job_returns_to_the_queue(queue: JobQueue) -> None:
    queue.enqueue("j1", "p1", "pipe")
    queue.claim("w1", lease_s=-1)  # already expired
    assert queue.reap_expired() == ("j1",)
    assert queue.claim("w2", lease_s=60) is not None


def test_a_live_lease_is_not_reaped(queue: JobQueue) -> None:
    queue.enqueue("j1", "p1", "pipe")
    queue.claim("w1", lease_s=3600)
    assert queue.reap_expired() == ()


def test_renew_extends_only_the_owner_s_lease(queue: JobQueue) -> None:
    """A worker that lost its job to the reaper must not be able to renew it."""
    queue.enqueue("j1", "p1", "pipe")
    queue.claim("w1", lease_s=60)
    assert queue.renew("j1", "w1", lease_s=120) is True
    assert queue.renew("j1", "impostor", lease_s=120) is False


def test_attempts_increments_on_every_claim(queue: JobQueue) -> None:
    queue.enqueue("j1", "p1", "pipe")
    queue.claim("w1", lease_s=-1)
    queue.reap_expired()
    second = queue.claim("w2", lease_s=60)
    assert second is not None
    assert second.attempts == 2


def test_complete_and_fail_are_terminal(queue: JobQueue) -> None:
    queue.enqueue("j1", "p1", "pipe")
    queue.claim("w1", lease_s=60)
    queue.complete("j1")
    assert queue.claim("w2", lease_s=60) is None


# ---------------------------------------------------------------------------
# Step 5(a): immediate=True is load-bearing for claim()'s read-then-write.
#
# The eleven tests above all use a single connection, so they cannot exercise
# the race immediate=True exists to prevent - proven honestly in the task
# report by flipping claim()'s immediate=True to False and observing that
# they all still pass. The two tests below use two real connections to the
# same database file (prior art: tests/unit/infra/test_db_engine.py) to
# reproduce claim()'s exact SELECT-then-UPDATE shape and make the race
# deterministic instead of timing-dependent: the contender's busy_timeout=0
# means it fails fast rather than masking the result by waiting.
# ---------------------------------------------------------------------------


@pytest.fixture()
def two_connections(tmp_path: Path) -> Iterator[tuple[sqlite3.Connection, sqlite3.Connection]]:
    """Two live connections to the same migrated database file."""
    db = tmp_path / "two.db"
    conn_a = connect(db)
    apply_migrations(conn_a)
    conn_b = connect(db)
    conn_b.execute("PRAGMA busy_timeout=0")
    try:
        yield conn_a, conn_b
    finally:
        conn_b.close()
        conn_a.close()


def test_a_deferred_read_then_write_loses_to_a_concurrent_writer(
    two_connections: tuple[sqlite3.Connection, sqlite3.Connection],
) -> None:
    """Reproduces claim()'s SELECT-then-UPDATE shape under immediate=False.

    conn_a takes its read snapshot at the SELECT. Before its UPDATE runs,
    conn_b commits a write. SQLite refuses to let conn_a's deferred
    transaction upgrade to a writer against a now-stale snapshot -
    SQLITE_BUSY_SNAPSHOT, raised immediately, without the busy handler ever
    running. This is exactly the failure immediate=True prevents in claim().
    """
    conn_a, conn_b = two_connections
    JobQueue(conn_a).enqueue("j1", "p1", "pipe")

    with pytest.raises(sqlite3.OperationalError), transaction(conn_a, immediate=False):
        conn_a.execute(
            "SELECT id FROM jobs WHERE state = 'queued' ORDER BY priority DESC, created_at LIMIT 1"
        ).fetchone()
        conn_b.execute("BEGIN IMMEDIATE")
        conn_b.execute("UPDATE jobs SET priority = priority + 1 WHERE id = 'j1'")
        conn_b.execute("COMMIT")
        conn_a.execute("UPDATE jobs SET state = 'running' WHERE id = 'j1'")


def test_an_immediate_read_then_write_survives_the_same_concurrent_writer(
    two_connections: tuple[sqlite3.Connection, sqlite3.Connection],
) -> None:
    """The contrast that makes the previous test meaningful.

    Same shape, immediate=True: the write lock is taken at BEGIN, before the
    SELECT ever runs, so there is no read snapshot to invalidate. The
    contender simply fails fast on lock contention (busy_timeout=0) instead
    of racing a snapshot - and conn_a's own UPDATE completes normally.
    """
    conn_a, conn_b = two_connections
    JobQueue(conn_a).enqueue("j1", "p1", "pipe")

    with transaction(conn_a, immediate=True):
        conn_a.execute(
            "SELECT id FROM jobs WHERE state = 'queued' ORDER BY priority DESC, created_at LIMIT 1"
        ).fetchone()
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            conn_b.execute("UPDATE jobs SET priority = priority + 1 WHERE id = 'j1'")
        conn_a.execute("UPDATE jobs SET state = 'running' WHERE id = 'j1'")

    row = conn_a.execute("SELECT state FROM jobs WHERE id = 'j1'").fetchone()
    assert row["state"] == "running"
