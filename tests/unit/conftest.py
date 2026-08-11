import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import apply_migrations


@pytest.fixture()
def db_conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """A migrated database. Closed on teardown so Windows can delete tmp_path.

    Shared across the unit suite (not just infra) - the job queue and the CAS
    store both need a real migrated connection, and duplicating this fixture
    per-package would let the two definitions drift.
    """
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    try:
        yield conn
    finally:
        conn.close()
