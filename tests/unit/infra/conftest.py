import sqlite3
from pathlib import Path

import pytest

from ytauto.infra.cas.store import CasStore

# db_conn is defined in tests/unit/conftest.py - shared with tests/unit/app,
# which also needs a real migrated connection (the job queue).


@pytest.fixture()
def store(tmp_path: Path, db_conn: sqlite3.Connection) -> CasStore:
    """A CasStore sharing the migrated connection from ``db_conn``."""
    return CasStore(root=tmp_path / "cas", conn=db_conn)
