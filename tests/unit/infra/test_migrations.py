import sqlite3
from pathlib import Path

import pytest

from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import (
    HEAD_VERSION,
    MIGRATIONS,
    apply_migrations,
    current_version,
)


def _tables(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {row["name"] for row in rows}


def test_fresh_database_is_at_version_zero(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    assert current_version(conn) == 0
    conn.close()


def test_apply_migrations_reaches_head(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    assert apply_migrations(conn) == HEAD_VERSION
    assert current_version(conn) == HEAD_VERSION
    conn.close()


def test_expected_tables_exist_after_migration(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    assert {"cas_objects", "settings", "schema_version"} <= _tables(conn)
    conn.close()


def test_apply_migrations_is_idempotent(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    before = _tables(conn)
    assert apply_migrations(conn) == HEAD_VERSION
    assert _tables(conn) == before
    conn.close()


def test_migrations_are_uniquely_and_contiguously_versioned() -> None:
    versions = [m.version for m in MIGRATIONS]
    assert versions == sorted(versions)
    assert len(set(versions)) == len(versions)
    assert versions == list(range(1, len(versions) + 1))


def test_cas_objects_rejects_duplicate_hashes(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    insert = (
        "INSERT INTO cas_objects (hash, kind, size_bytes, created_at, last_accessed_at) "
        "VALUES (?, ?, ?, ?, ?)"
    )
    args = ("a" * 64, "audio", 10, "2026-07-30T00:00:00+00:00", "2026-07-30T00:00:00+00:00")
    conn.execute(insert, args)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(insert, args)
    conn.close()
