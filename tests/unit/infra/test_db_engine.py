import sqlite3
from pathlib import Path

import pytest

from ytauto.core.errors import TransactionError
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


def test_immediate_takes_the_write_lock_before_any_statement(tmp_path: Path) -> None:
    """An immediate transaction locks on BEGIN, so a read-then-write claim is safe."""
    db = tmp_path / "t.db"
    writer = connect(db)
    writer.execute("CREATE TABLE t (a TEXT)")
    other = connect(db)
    other.execute("PRAGMA busy_timeout=0")

    with (
        transaction(writer, immediate=True),
        pytest.raises(sqlite3.OperationalError, match="locked|busy"),
    ):
        other.execute("INSERT INTO t VALUES ('blocked')")

    other.close()
    writer.close()


def test_deferred_does_not_hold_the_lock_until_its_first_write(tmp_path: Path) -> None:
    """The contrast that makes the previous test meaningful."""
    db = tmp_path / "t.db"
    reader = connect(db)
    reader.execute("CREATE TABLE t (a TEXT)")
    other = connect(db)
    other.execute("PRAGMA busy_timeout=0")

    with transaction(reader):
        reader.execute("SELECT count(*) FROM t").fetchone()
        other.execute("INSERT INTO t VALUES ('allowed')")

    assert other.execute("SELECT count(*) FROM t").fetchone()[0] == 1
    other.close()
    reader.close()


def test_immediate_still_commits_and_rolls_back(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    conn.execute("CREATE TABLE t (a TEXT)")

    with transaction(conn, immediate=True):
        conn.execute("INSERT INTO t VALUES ('kept')")
    assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 1

    with pytest.raises(sqlite3.IntegrityError), transaction(conn, immediate=True):
        conn.execute("INSERT INTO t VALUES ('gone')")
        raise sqlite3.IntegrityError("simulated")
    assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 1
    conn.close()


def test_nesting_a_transaction_raises_a_distinct_type(tmp_path: Path) -> None:
    """Nesting is a programming error and contention is a normal race, but both
    used to surface as sqlite3.OperationalError. A claim loop that must retry one
    and crash on the other cannot tell them apart from the message alone."""
    conn = connect(tmp_path / "t.db")
    conn.execute("CREATE TABLE t (a TEXT)")
    conn.execute("BEGIN")  # raw, so the precondition under test is unambiguous

    with pytest.raises(TransactionError), transaction(conn):
        pass

    conn.execute("ROLLBACK")
    conn.close()


def test_a_refused_nested_transaction_leaves_the_outer_one_intact(tmp_path: Path) -> None:
    """The guard runs before BEGIN is issued. Letting the nested BEGIN fail
    instead would trip transaction()'s own rollback handler and silently discard
    the caller's committed-to-be work - a scheduler would lose its job claim."""
    conn = connect(tmp_path / "t.db")
    conn.execute("CREATE TABLE t (a TEXT)")

    with transaction(conn):
        conn.execute("INSERT INTO t VALUES ('claimed')")
        with pytest.raises(TransactionError), transaction(conn):
            pass
        assert conn.in_transaction, "the outer transaction must survive the refusal"

    assert [row["a"] for row in conn.execute("SELECT a FROM t")] == ["claimed"]
    conn.close()
