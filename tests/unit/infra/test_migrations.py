import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from ytauto.infra.clock import utc_now_iso
from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import (
    HEAD_VERSION,
    MIGRATIONS,
    Migration,
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


def test_failed_migration_rolls_back_schema_and_version_together(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The atomicity guarantee, asserted under an actual failure.

    Without this, every other test in this file would still pass if the
    `transaction()` wrapper were deleted outright — they only ever check
    post-success state.
    """
    conn = connect(tmp_path / "t.db")
    broken = Migration(
        version=1,
        name="broken",
        statements=(
            "CREATE TABLE will_not_survive (a TEXT)",
            "THIS IS NOT VALID SQL",
        ),
    )
    monkeypatch.setattr("ytauto.infra.db.migrations.MIGRATIONS", (broken,))

    with pytest.raises(sqlite3.OperationalError):
        apply_migrations(conn)

    assert current_version(conn) == 0, "version row must not survive a failed migration"
    assert "will_not_survive" not in _tables(conn), "DDL must roll back with the version row"
    conn.close()


def test_phase_one_tables_exist(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    assert {"jobs", "job_stages", "artifacts"} <= _tables(conn)
    conn.close()


def test_job_stages_cascade_when_a_job_is_deleted(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    now = "2026-07-31T00:00:00+00:00"
    conn.execute(
        "INSERT INTO jobs (id, project_id, pipeline_id, state, created_at, updated_at) "
        "VALUES ('j1', 'p1', 'shorts', 'queued', ?, ?)",
        (now, now),
    )
    conn.execute(
        "INSERT INTO job_stages (job_id, stage_id, status) VALUES ('j1', 'rewrite', 'pending')"
    )
    conn.execute("DELETE FROM jobs WHERE id = 'j1'")
    assert conn.execute("SELECT count(*) FROM job_stages").fetchone()[0] == 0
    conn.close()


def test_artifacts_allow_several_outputs_per_fingerprint(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    insert = (
        "INSERT INTO artifacts (fingerprint, name, stage_id, kind, digest, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)"
    )
    now = "2026-07-31T00:00:00+00:00"
    conn.execute(insert, ("f" * 64, "narration", "tts", "audio", "a" * 64, now))
    conn.execute(insert, ("f" * 64, "timings", "tts", "json", "b" * 64, now))
    assert conn.execute("SELECT count(*) FROM artifacts").fetchone()[0] == 2

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(insert, ("f" * 64, "narration", "tts", "audio", "c" * 64, now))
    conn.close()


def test_migration_002_is_applied_on_top_of_an_existing_001(tmp_path: Path) -> None:
    """Upgrade path, not just a fresh create."""
    db = tmp_path / "t.db"
    conn = connect(db)
    monkeyed = MIGRATIONS[:1]
    with patch("ytauto.infra.db.migrations.MIGRATIONS", monkeyed):
        assert apply_migrations(conn) == 1
    assert "jobs" not in _tables(conn)

    with patch("ytauto.infra.db.migrations.MIGRATIONS", MIGRATIONS[:2]):
        assert apply_migrations(conn) == 2
    assert {"cas_objects", "jobs"} <= _tables(conn)
    conn.close()


def test_migration_003_adds_available_at(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    cols = {r["name"]: r for r in conn.execute("PRAGMA table_info(jobs)")}
    assert "available_at" in cols
    assert cols["available_at"]["notnull"] == 1
    conn.close()


def test_migration_003_adds_per_stage_attempts(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(job_stages)")}
    assert "attempts" in cols
    conn.close()


def test_the_claim_index_leads_with_state_then_available_at(tmp_path: Path) -> None:
    """The claim query filters on state AND available_at before ordering. An
    index that does not lead with both cannot serve it as a covering scan."""
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    cols = [r["name"] for r in conn.execute("PRAGMA index_info(idx_jobs_claimable)")]
    assert cols[:2] == ["state", "available_at"]
    conn.close()


def test_head_version_is_three(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    assert conn.execute("SELECT max(version) FROM schema_version").fetchone()[0] == 3
    assert HEAD_VERSION == 3
    conn.close()


def test_existing_jobs_are_immediately_claimable_after_upgrade(tmp_path: Path) -> None:
    """The '' default must sort before every ISO-8601 timestamp, so rows written
    under v2 need no backfill to remain claimable."""
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    now = utc_now_iso()
    conn.execute(
        "INSERT INTO jobs (id, project_id, pipeline_id, state, created_at, updated_at) "
        "VALUES ('j1', 'p1', 'pipe', 'queued', ?, ?)",
        (now, now),
    )
    row = conn.execute(
        "SELECT id FROM jobs WHERE state = 'queued' AND available_at <= ?", (now,)
    ).fetchone()
    assert row["id"] == "j1"
    conn.close()


def test_migration_003_is_applied_on_top_of_an_existing_002(tmp_path: Path) -> None:
    """Upgrade path from a v2 database, not just a fresh create."""
    db = tmp_path / "t.db"
    conn = connect(db)
    monkeyed = MIGRATIONS[:2]
    with patch("ytauto.infra.db.migrations.MIGRATIONS", monkeyed):
        assert apply_migrations(conn) == 2
    now = utc_now_iso()
    conn.execute(
        "INSERT INTO jobs (id, project_id, pipeline_id, state, created_at, updated_at) "
        "VALUES ('j1', 'p1', 'pipe', 'queued', ?, ?)",
        (now, now),
    )
    conn.execute(
        "INSERT INTO job_stages (job_id, stage_id, status) VALUES ('j1', 'rewrite', 'pending')"
    )
    assert "available_at" not in {r["name"] for r in conn.execute("PRAGMA table_info(jobs)")}

    assert apply_migrations(conn) == 3
    job_cols = {r["name"] for r in conn.execute("PRAGMA table_info(jobs)")}
    stage_cols = {r["name"] for r in conn.execute("PRAGMA table_info(job_stages)")}
    assert "available_at" in job_cols
    assert "attempts" in stage_cols

    row = conn.execute("SELECT id, available_at FROM jobs WHERE id = 'j1'").fetchone()
    assert row["id"] == "j1", "the pre-existing row must survive the upgrade"
    assert row["available_at"] == "", "existing rows get the '' default, not a backfilled value"

    stage_row = conn.execute(
        "SELECT attempts FROM job_stages WHERE job_id = 'j1' AND stage_id = 'rewrite'"
    ).fetchone()
    assert stage_row["attempts"] == 0
    conn.close()
