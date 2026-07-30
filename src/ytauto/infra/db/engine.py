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
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Run a block in one transaction: commit on success, roll back on any error.

    Raises:
        sqlite3.Error: BEGIN, COMMIT or ROLLBACK fails.
        BaseException: anything the wrapped block raises is re-raised unchanged
            after the transaction has been rolled back.
    """
    conn.execute("BEGIN")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")
