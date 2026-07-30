"""Ordered, idempotent schema migrations.

Migrations are append-only. Never edit a released migration; add a new one.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from ytauto.infra.clock import utc_now_iso
from ytauto.infra.db.engine import transaction


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    statements: tuple[str, ...]


_M001 = Migration(
    version=1,
    name="cas_and_settings",
    statements=(
        """
        CREATE TABLE cas_objects (
            hash             TEXT PRIMARY KEY,
            kind             TEXT NOT NULL,
            size_bytes       INTEGER NOT NULL,
            created_at       TEXT NOT NULL,
            last_accessed_at TEXT NOT NULL,
            refcount         INTEGER NOT NULL DEFAULT 0
        )
        """,
        "CREATE INDEX idx_cas_last_accessed ON cas_objects (last_accessed_at)",
        "CREATE INDEX idx_cas_refcount ON cas_objects (refcount)",
        """
        CREATE TABLE settings (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """,
    ),
)

MIGRATIONS: tuple[Migration, ...] = (_M001,)
HEAD_VERSION: int = MIGRATIONS[-1].version


def _ensure_version_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            version    INTEGER PRIMARY KEY,
            name       TEXT NOT NULL,
            applied_at TEXT NOT NULL
        )
        """
    )


def current_version(conn: sqlite3.Connection) -> int:
    """Return the highest applied migration version, or 0 on a fresh database.

    Raises:
        sqlite3.Error: the version table cannot be created or queried.
    """
    _ensure_version_table(conn)
    row = conn.execute("SELECT max(version) AS v FROM schema_version").fetchone()
    return int(row["v"]) if row["v"] is not None else 0


def apply_migrations(conn: sqlite3.Connection) -> int:
    """Apply every pending migration in order. Returns the resulting version.

    Each migration and its version row commit together, so a crash mid-migration
    can never leave the schema ahead of the recorded version.

    ``executescript`` is deliberately avoided: it implicitly commits any pending
    transaction, which would defeat that atomicity.

    Raises:
        sqlite3.Error: any statement in a migration fails. That migration is
            rolled back whole, so the recorded version still describes the
            schema actually present.
    """
    version = current_version(conn)
    for migration in MIGRATIONS:
        if migration.version <= version:
            continue
        with transaction(conn):
            for statement in migration.statements:
                conn.execute(statement)
            conn.execute(
                "INSERT INTO schema_version (version, name, applied_at) VALUES (?, ?, ?)",
                (migration.version, migration.name, utc_now_iso()),
            )
        version = migration.version
    return version
