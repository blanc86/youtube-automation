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


def test_the_claim_index_follows_esr_order(tmp_path: Path) -> None:
    """ESR: equality, sort, range. The claim query is WHERE state=? AND
    available_at<=? ORDER BY priority DESC, created_at. state is the equality
    column; priority/created_at are the sort columns; available_at is the
    range-filtered column and must trail the sort columns, not lead them, or
    SQLite cannot use the index to satisfy ORDER BY."""
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    cols = [r["name"] for r in conn.execute("PRAGMA index_info(idx_jobs_claimable)")]
    assert cols == ["state", "priority", "created_at", "available_at"]
    conn.close()


def test_the_claim_index_avoids_a_sort_pass(tmp_path: Path) -> None:
    """ESR: equality, sort, range. Putting the range-filtered available_at
    ahead of the sort columns makes SQLite build a temp b-tree on every claim,
    which the v2 index did not need. The claim query is the dispatcher's hot
    polling path."""
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    plan = [
        row[3]
        for row in conn.execute(
            "EXPLAIN QUERY PLAN "
            "SELECT id FROM jobs WHERE state = ? AND available_at <= ? "
            "ORDER BY priority DESC, created_at LIMIT 1",
            ("queued", utc_now_iso()),
        )
    ]
    assert not any("TEMP B-TREE" in step for step in plan), plan
    conn.close()


def test_head_version_is_five(tmp_path: Path) -> None:
    conn = connect(tmp_path / "t.db")
    apply_migrations(conn)
    assert conn.execute("SELECT max(version) FROM schema_version").fetchone()[0] == 5
    assert HEAD_VERSION == 5
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

    with patch("ytauto.infra.db.migrations.MIGRATIONS", MIGRATIONS[:3]):
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


def test_a_job_inserted_under_v2_is_claimable_after_a_real_upgrade_to_v3(
    tmp_path: Path,
) -> None:
    """End-to-end version of the '' default property: build a genuine v2
    database, insert a queued job the way v2 code would (no available_at
    column exists yet to write to), upgrade to v3 for real, and confirm the
    claim predicate finds that same row with no backfill in between.

    The other two tests each cover half of this: one runs the claim predicate
    but only against a row inserted straight into an already-v3 schema; the
    other performs a genuine v2->v3 upgrade but only checks available_at ==
    "" directly, never runs the claim predicate. Neither exercises the actual
    scenario the default exists for.
    """
    conn = connect(tmp_path / "t.db")
    with patch("ytauto.infra.db.migrations.MIGRATIONS", MIGRATIONS[:2]):
        assert apply_migrations(conn) == 2
    now = utc_now_iso()
    conn.execute(
        "INSERT INTO jobs (id, project_id, pipeline_id, state, created_at, updated_at) "
        "VALUES ('j1', 'p1', 'pipe', 'queued', ?, ?)",
        (now, now),
    )

    with patch("ytauto.infra.db.migrations.MIGRATIONS", MIGRATIONS[:3]):
        assert apply_migrations(conn) == 3

    row = conn.execute(
        "SELECT id FROM jobs WHERE state = 'queued' AND available_at <= ?", (now,)
    ).fetchone()
    assert row["id"] == "j1"
    conn.close()


def test_migration_004_adds_projects_and_broll_clips(db_conn: sqlite3.Connection) -> None:
    apply_migrations(db_conn)
    assert current_version(db_conn) == 5
    tables = {
        r["name"] for r in db_conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    assert {"projects", "broll_clips"} <= tables


def test_broll_clips_records_a_licence_and_both_normalised_digests(
    db_conn: sqlite3.Connection,
) -> None:
    """Provenance is not optional - it is the DMCA defence for the channel."""
    apply_migrations(db_conn)
    cols = {r["name"] for r in db_conn.execute("PRAGMA table_info(broll_clips)")}
    assert {"source_url", "licence", "attribution", "notes"} <= cols
    assert {"normalised_landscape_digest", "normalised_vertical_digest"} <= cols
