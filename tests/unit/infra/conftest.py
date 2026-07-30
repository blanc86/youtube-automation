from collections.abc import Iterator
from pathlib import Path

import pytest

from ytauto.infra.cas.store import CasStore
from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import apply_migrations


@pytest.fixture()
def store(tmp_path: Path) -> Iterator[CasStore]:
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    try:
        yield CasStore(root=tmp_path / "cas", conn=conn)
    finally:
        conn.close()
