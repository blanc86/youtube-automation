"""SQLite connection factory and transaction helper.

WAL mode is required, not optional: the dispatcher writes job state while the
GUI reads it, and in rollback-journal mode those readers would block writers
and visibly stall the interface during batch runs.
"""

from __future__ import annotations

import sqlite3
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


@contextmanager
def transaction(
    conn: sqlite3.Connection, *, immediate: bool = False
) -> Iterator[sqlite3.Connection]:
    """Run a block in one transaction: commit on success, roll back on any error.

    Pass ``immediate=True`` for read-then-write work such as claiming a queued
    job or acquiring a resource lease. A deferred ``BEGIN`` upgrades to a write
    lock lazily, and in WAL mode that upgrade returns SQLITE_BUSY_SNAPSHOT
    *immediately* without invoking the busy handler - so ``busy_timeout`` does
    not apply and the caller sees a spurious failure under concurrency.

    Nesting two transactions on one connection raises TransactionError; keep
    transactions at the outermost call site. That check happens before ``BEGIN``
    is issued, so an outer transaction is left fully intact - letting the nested
    ``BEGIN`` fail instead would trip the rollback handler below and silently
    discard the caller's work.

    Raises:
        TransactionError: if a transaction is already open on ``conn``. Always a
            programming error, never retryable.
        sqlite3.OperationalError: if the write lock could not be acquired within
            ``busy_timeout`` - legitimate contention, which callers competing for
            a job or a lease must expect and handle. Distinguishing these two is
            why the nesting case has its own type.
        BaseException: anything raised inside the block, after rolling back.
    """
    if conn.in_transaction:
        raise TransactionError(
            "a transaction is already open on this connection; transaction() is "
            "not re-entrant - move the transaction to the outermost call site"
        )
    conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")
