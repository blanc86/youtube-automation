import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from ytauto.infra.cas.store import CasStore
from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import apply_migrations


@pytest.fixture()
def db_conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """A migrated database. Closed on teardown so Windows can delete tmp_path."""
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture()
def store(tmp_path: Path, db_conn: sqlite3.Connection) -> CasStore:
    """A CasStore sharing the migrated connection from ``db_conn``."""
    return CasStore(root=tmp_path / "cas", conn=db_conn)
