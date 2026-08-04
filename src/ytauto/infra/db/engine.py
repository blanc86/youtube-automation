"""SQLite connection factory and transaction helper.

WAL mode is required, not optional: the dispatcher writes job state while the
GUI reads it, and in rollback-journal mode those readers would block writers
and visibly stall the interface during batch runs.

A connection must not be shared across threads once transactions are in
play. ``connect()`` sets ``check_same_thread=False`` so a connection can be
*handed off* between threads (opened on one, used later on another) - it does
not make concurrent use from multiple threads safe. Savepoints are a
per-connection LIFO stack; two threads each calling ``transaction()`` on the
same connection at the same time can interleave their SAVEPOINT/RELEASE
calls, so one thread's frame lands on top of another's and a RELEASE or
ROLLBACK TO issued by either can destroy the other's savepoint out from under
it. ``transaction()`` has no way to detect this - the savepoint-depth lock
only serialises name allocation, and no lock over that bookkeeping can
reconstruct which thread's block a given stack frame belongs to. Each thread
that opens transactions needs its own connection; treat single-connection,
single-thread transaction use as a caller contract, not something this module
enforces.
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from ytauto.core.errors import TransactionError

_PRAGMAS = (
    "PRAGMA journal_mode=WAL",
    "PRAGMA foreign_keys=ON",
    "PRAGMA busy_timeout=10000",
    "PRAGMA synchronous=NORMAL",
)


def connect(db_path: Path) -> sqlite3.Connection:
    """Open a connection with the pragmas this application requires.

    Raises:
        OSError: the parent directory of ``db_path`` cannot be created.
        sqlite3.Error: the database cannot be opened (missing, locked, or
            corrupt), or a required pragma is rejected.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    for pragma in _PRAGMAS:
        conn.execute(pragma)
    return conn


_SAVEPOINT_DEPTH: dict[int, int] = {}

# Guards read-modify-write access to _SAVEPOINT_DEPTH. connect() sets
# check_same_thread=False, so a connection can be handed to more than one
# thread (the dispatcher runs on its own thread); the dict itself must not be
# corrupted by concurrent mutation. The lock is held only around the get+set
# of the depth counter, never around a conn.execute() call, so SQLite's own
# (potentially slow, lock-contending) I/O never happens while this lock is
# held.
_SAVEPOINT_DEPTH_LOCK = threading.Lock()


@contextmanager
def transaction(
    conn: sqlite3.Connection, *, immediate: bool = False
) -> Iterator[sqlite3.Connection]:
    """Run a block in one transaction: commit on success, roll back on any error.

    Re-entrant. An outermost call issues BEGIN/COMMIT; a nested call issues a
    SAVEPOINT instead, so composing two modules that each open a transaction is
    safe and the whole composition still lands atomically.

    Pass ``immediate=True`` for read-then-write work such as claiming a queued
    job or acquiring a resource lease. A deferred ``BEGIN`` upgrades to a write
    lock lazily, and in WAL mode that upgrade returns SQLITE_BUSY_SNAPSHOT
    *immediately* without invoking the busy handler - so ``busy_timeout`` does
    not apply and the caller sees a spurious failure under concurrency.

    ``immediate=True`` is refused inside an existing transaction: the write-lock
    timing was already decided by the outer BEGIN, so honouring the flag is
    impossible and silently downgrading it would reintroduce the very failure it
    exists to prevent.

    Raises:
        TransactionError: if ``immediate=True`` is requested while a transaction
            is already open on ``conn``. Always a programming error, never
            retryable.
        sqlite3.OperationalError: if the write lock could not be acquired within
            ``busy_timeout`` - legitimate contention, which callers competing for
            a job or a lease must expect and handle.
        BaseException: anything raised inside the block, after rolling back.
    """
    key = id(conn)
    if conn.in_transaction:
        if immediate:
            raise TransactionError(
                "immediate=True cannot be honoured inside an open transaction; "
                "the write lock was already taken by the outer BEGIN - move the "
                "immediate transaction to the outermost call site"
            )
        with _SAVEPOINT_DEPTH_LOCK:
            depth = _SAVEPOINT_DEPTH.get(key, 0)
            _SAVEPOINT_DEPTH[key] = depth + 1
        name = f"_sp_{depth}"
        conn.execute(f"SAVEPOINT {name}")
        try:
            yield conn
        except BaseException:
            # ROLLBACK TO does not pop the savepoint; RELEASE must follow or the
            # stack leaks a frame per failure.
            conn.execute(f"ROLLBACK TO {name}")
            conn.execute(f"RELEASE {name}")
            with _SAVEPOINT_DEPTH_LOCK:
                _SAVEPOINT_DEPTH[key] = depth
            raise
        conn.execute(f"RELEASE {name}")
        with _SAVEPOINT_DEPTH_LOCK:
            _SAVEPOINT_DEPTH[key] = depth
        return

    conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        with _SAVEPOINT_DEPTH_LOCK:
            _SAVEPOINT_DEPTH.pop(key, None)
        raise
    conn.execute("COMMIT")
    with _SAVEPOINT_DEPTH_LOCK:
        _SAVEPOINT_DEPTH.pop(key, None)
