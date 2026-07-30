import sqlite3
from pathlib import Path

import pytest

from ytauto.infra.db.engine import connect, transaction


def test_connection_uses_wal(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode.lower() == "wal"
    conn.close()


def test_foreign_keys_are_enforced(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    conn.close()


def test_rows_are_accessible_by_column_name(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    conn.execute("CREATE TABLE t (a TEXT, b INTEGER)")
    conn.execute("INSERT INTO t VALUES ('x', 1)")
    row = conn.execute("SELECT * FROM t").fetchone()
    assert row["a"] == "x"
    assert row["b"] == 1
    conn.close()


def test_parent_directory_is_created(tmp_path: Path) -> None:
    conn = connect(tmp_path / "nested" / "deeper" / "t.db")
    conn.close()
    assert (tmp_path / "nested" / "deeper" / "t.db").exists()


def test_transaction_commits_on_success(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    conn.execute("CREATE TABLE t (a TEXT)")
    with transaction(conn):
        conn.execute("INSERT INTO t VALUES ('kept')")
    assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 1
    conn.close()


def test_transaction_rolls_back_on_error(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    conn.execute("CREATE TABLE t (a TEXT)")
    with pytest.raises(sqlite3.IntegrityError), transaction(conn):
        conn.execute("INSERT INTO t VALUES ('gone')")
        raise sqlite3.IntegrityError("simulated failure")
    assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 0
    conn.close()
