"""CLI wiring for ``ytauto project create`` and ``ytauto run``.

Two different hermeticity strategies are used for ``run``'s tests, and the
split is deliberate. Tests that need a job to reach ``succeeded`` or
``failed`` replace ``ytauto.cli.__main__.build_pipeline`` with a fake
single-stage pipeline and either pre-record a cache hit or replace
``ytauto.app.scheduler.dispatcher.Popen`` with a spy that never spawns a real
process - the same conventions ``tests/unit/app/test_dispatcher.py`` already
established, so no genuine subprocess is ever spawned from a "unit" test (see
``tests/integration/test_resume.py``'s own docstring: spawning a real
``python -m ytauto.app.worker`` is what makes a test integration, not unit,
regardless of whether ffmpeg is involved). The poison-job test and the
max-ticks-exhausted test need no such fake: the real ``story_video`` pipeline
is cheap and side-effect-free to *construct* (its stage factories do no
network or subprocess work at construction time - only ``Stage.run`` does,
and neither test ever reaches a spawn), so they exercise the genuine
``app.registry.build_pipeline`` end to end.
"""

from __future__ import annotations

import io
import sqlite3
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path

import pytest

from ytauto.app.scheduler.worker_protocol import Error, encode
from ytauto.app.services.enqueue import story_digest_for
from ytauto.app.services.projects import ProjectService
from ytauto.cli.__main__ import main
from ytauto.core.errors import ErrorKind
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.pipeline.graph import Pipeline
from ytauto.core.pipeline.stage import JobContext, ProgressFn, StageResult
from ytauto.infra.artifacts import ArtifactStore
from ytauto.infra.cas.store import CasStore
from ytauto.infra.clock import utc_now_iso
from ytauto.infra.db.engine import connect, transaction
from ytauto.infra.db.migrations import apply_migrations
from ytauto.infra.paths import AppPaths

# -- fixtures -----------------------------------------------------------


@pytest.fixture()
def db_conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """A migrated connection to the exact database file ``main`` will itself
    open for ``--data-dir tmp_path`` - NOT the generic ``tests/unit/conftest.py``
    ``db_conn`` fixture (which points at an unrelated ``t.db``). Shadowing the
    name locally is deliberate: every test below needs to set up state (a
    project row, a pre-recorded cache hit) in the very file the CLI process
    under test will read and write, and to read it back afterwards.
    """
    paths = AppPaths.resolve(override=tmp_path)
    conn = connect(paths.db_file)
    apply_migrations(conn)
    try:
        yield conn
    finally:
        conn.close()


# -- shared helpers -------------------------------------------------------


def _project(
    conn: sqlite3.Connection, *, slug: str, settings: dict[str, object] | None = None
) -> str:
    return ProjectService(conn).create(
        slug=slug, title=slug, story_digest=None, settings={} if settings is None else settings
    )


def _job_state(conn: sqlite3.Connection, *, slug: str) -> str:
    """The state of the (only) job enqueued against the project named ``slug``."""
    row = conn.execute(
        """
        SELECT j.state FROM jobs j JOIN projects p ON j.project_id = p.id
        WHERE p.slug = ?
        """,
        (slug,),
    ).fetchone()
    assert row is not None, f"no job was ever enqueued for project slug {slug!r}"
    return str(row["state"])


# -- Step 0: the story digest must ignore line-ending convention -----------


def test_the_story_digest_ignores_line_ending_convention(tmp_path: Path) -> None:
    """A CRLF and an LF copy of one story are the same story and must
    fingerprint identically, or every Windows-authored story misses the cache.
    """
    lf = tmp_path / "lf.txt"
    crlf = tmp_path / "crlf.txt"
    lf.write_bytes(b"The train never stopped.\nIt kept going.\n")
    crlf.write_bytes(b"The train never stopped.\r\nIt kept going.\r\n")

    assert story_digest_for(lf) == story_digest_for(crlf)


# -- ``ytauto project create`` --------------------------------------------


def test_project_create_writes_story_to_disk_and_cas_and_records_settings(
    tmp_path: Path, db_conn: sqlite3.Connection
) -> None:
    story = tmp_path / "story.txt"
    story.write_text("Once upon a time.\n", encoding="utf-8")

    rc = main(
        [
            "--data-dir",
            str(tmp_path),
            "project",
            "create",
            "--slug",
            "once-upon",
            "--title",
            "Once Upon",
            "--story",
            str(story),
        ]
    )

    assert rc == 0
    paths = AppPaths.resolve(override=tmp_path)
    project_row = db_conn.execute(
        "SELECT id, story_digest, settings_json FROM projects WHERE slug = ?", ("once-upon",)
    ).fetchone()
    assert project_row is not None

    expected_digest = story_digest_for(story)
    assert project_row["story_digest"] == expected_digest

    import json

    settings = json.loads(project_row["settings_json"])
    assert settings["story_digest"] == expected_digest

    on_disk = paths.projects / "once-upon" / "story.txt"
    assert on_disk.is_file(), "the human-editable copy must exist in the project directory"
    assert on_disk.read_text(encoding="utf-8") == "Once upon a time.\n"
    assert settings["story_path"] == str(on_disk)

    cas = CasStore(root=paths.cas, conn=db_conn)
    assert cas.exists(expected_digest), "the same digest must also be staged into the CAS"


def test_project_create_rejects_a_missing_story_file(
    tmp_path: Path, db_conn: sqlite3.Connection, capsys: pytest.CaptureFixture[str]
) -> None:
    rc = main(
        [
            "--data-dir",
            str(tmp_path),
            "project",
            "create",
            "--slug",
            "ghost",
            "--title",
            "Ghost",
            "--story",
            str(tmp_path / "does-not-exist.txt"),
        ]
    )

    assert rc == 2
    assert "does-not-exist.txt" in capsys.readouterr().err
    assert db_conn.execute("SELECT count(*) FROM projects").fetchone()[0] == 0


def test_project_create_rejects_a_duplicate_slug(
    tmp_path: Path, db_conn: sqlite3.Connection, capsys: pytest.CaptureFixture[str]
) -> None:
    story = tmp_path / "story.txt"
    story.write_text("hello\n", encoding="utf-8")
    argv = [
        "--data-dir",
        str(tmp_path),
        "project",
        "create",
        "--slug",
        "dup",
        "--title",
        "Dup",
        "--story",
        str(story),
    ]

    assert main(argv) == 0
    rc = main(argv)

    assert rc == 2
    assert "dup" in capsys.readouterr().err
    assert db_conn.execute("SELECT count(*) FROM projects").fetchone()[0] == 1


# -- ``ytauto run`` fakes: a single-stage pipeline, never spawned for real --

_FIXED_FINGERPRINT = "f" * 64
_FAKE_STAGE_ID = "only"


class _FixedStage:
    """A minimal Stage double whose fingerprint is a fixed constant - the same
    convention ``tests/unit/app/test_dispatcher.py``'s ``_FixedStage`` uses,
    reused here so ``run``'s CLI wiring can be exercised without ever
    resolving the real ``story_video`` entry points or spawning a worker.
    """

    id = _FAKE_STAGE_ID
    version = 1
    depends_on: tuple[str, ...] = ()
    settings_keys: tuple[str, ...] = ()
    gpu_pool = "gpu_compute"

    def fingerprint(self, ctx: JobContext) -> str:
        return _FIXED_FINGERPRINT

    def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult:
        raise NotImplementedError(
            "not exercised directly: tests either pre-seed a cache hit (never "
            "calls run) or replace subprocess.Popen (run executes in a fake "
            "worker, never in this process)"
        )


def _fake_build_pipeline(
    pipeline_id: str, cas: CasStore, settings: Mapping[str, object]
) -> Pipeline:
    return Pipeline(id=pipeline_id, stages=(_FixedStage(),))


def _preseed_cache_hit(paths: AppPaths, conn: sqlite3.Connection) -> None:
    """Record an artifact under ``_FIXED_FINGERPRINT`` so ``tick()``'s own
    probe reports a cache hit and never spawns a worker at all."""
    cas = CasStore(root=paths.cas, conn=conn)
    artifacts = ArtifactStore(cas, conn)
    digest = cas.put_bytes(b"fake cached output", kind="blob")
    artifacts.record(
        _FIXED_FINGERPRINT, _FAKE_STAGE_ID, [ArtifactRef(name="out", kind="blob", digest=digest)]
    )


class _FakeProcess:
    """A ``subprocess.Popen`` double that is already finished, whose stdout
    yields exactly the lines it was constructed with. Mirrors
    ``test_dispatcher.py``'s ``_FakeProcess``/``SpawnSpy`` pair."""

    def __init__(self, stdout_lines: Sequence[str] = ()) -> None:
        self.pid = -1
        self.returncode = 0
        self.stdin: io.StringIO = io.StringIO()
        body = "".join(f"{line}\n" for line in stdout_lines)
        self.stdout: io.StringIO | None = io.StringIO(body)
        self.stderr: io.StringIO | None = io.StringIO("")

    def wait(self, timeout: float | None = None) -> int:
        return self.returncode

    def poll(self) -> int | None:
        return self.returncode

    def kill(self) -> None:
        pass

    def terminate(self) -> None:
        pass


class _FixedUUID:
    """A ``uuid.uuid4()`` double with a predictable ``.hex`` - lets a test
    know a job's id before ``main()`` generates it, which the fatal-error
    fake worker below needs in order to address the right job."""

    def __init__(self, hex_value: str) -> None:
        self.hex = hex_value


# -- Step 1: ``ytauto run`` ------------------------------------------------


def test_run_enqueues_one_job_and_drains_it(
    tmp_path: Path, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = AppPaths.resolve(override=tmp_path)
    _project(db_conn, slug="ghost-train")
    _preseed_cache_hit(paths, db_conn)
    monkeypatch.setattr("ytauto.cli.__main__.build_pipeline", _fake_build_pipeline)

    rc = main(["--data-dir", str(tmp_path), "run", "--project", "ghost-train", "--max-ticks", "20"])

    assert rc == 0
    assert _job_state(db_conn, slug="ghost-train") == "succeeded"


def test_run_on_an_unknown_slug_exits_nonzero_without_enqueueing(
    tmp_path: Path, db_conn: sqlite3.Connection, capsys: pytest.CaptureFixture[str]
) -> None:
    rc = main(["--data-dir", str(tmp_path), "run", "--project", "absent"])

    assert rc == 2
    assert db_conn.execute("SELECT count(*) FROM jobs").fetchone()[0] == 0
    assert "absent" in capsys.readouterr().err


def test_run_returns_1_when_the_job_fails(
    tmp_path: Path, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The real ``handle_error`` -> ``_fail_job`` path, reached through a fake
    worker that reports a FATAL error instead of a result - not a cache hit
    this time, so ``tick()`` must actually spawn (the monkeypatched ``Popen``)
    and pump a terminal message.
    """
    _project(db_conn, slug="doomed")
    monkeypatch.setattr("ytauto.cli.__main__.build_pipeline", _fake_build_pipeline)

    fixed_job_id = "f" * 32
    monkeypatch.setattr("uuid.uuid4", lambda: _FixedUUID(fixed_job_id))

    error_line = encode(
        Error(
            job_id=fixed_job_id,
            stage_id=_FAKE_STAGE_ID,
            correlation_id="c1",
            message="simulated fatal provider failure",
            kind=ErrorKind.FATAL,
        )
    )

    def _spy(argv: object, **kwargs: object) -> _FakeProcess:
        return _FakeProcess([error_line])

    monkeypatch.setattr("ytauto.app.scheduler.dispatcher.Popen", _spy)

    rc = main(["--data-dir", str(tmp_path), "run", "--project", "doomed", "--max-ticks", "20"])

    assert rc == 1
    assert _job_state(db_conn, slug="doomed") == "failed"


def test_run_returns_1_when_max_ticks_is_exhausted_before_the_job_finishes(
    tmp_path: Path, db_conn: sqlite3.Connection
) -> None:
    """``--max-ticks 0`` never calls ``tick()`` at all, so the freshly
    enqueued job never leaves ``queued`` - not a success, and distinct from a
    ``failed`` job, so it must still be reported as a failure (exit 1), never
    exit 0. No fake pipeline needed: ``build_pipeline`` is still called to
    construct the real ``story_video`` pipeline (Step 3's contract), but with
    zero ticks nothing about it is ever exercised past construction.
    """
    _project(db_conn, slug="stalled")

    rc = main(["--data-dir", str(tmp_path), "run", "--project", "stalled", "--max-ticks", "0"])

    assert rc == 1
    assert _job_state(db_conn, slug="stalled") == "queued"


def test_run_reports_failure_rather_than_success_when_a_poison_job_blocks_the_queue(
    tmp_path: Path, db_conn: sqlite3.Connection, capsys: pytest.CaptureFixture[str]
) -> None:
    """The live hazard this task creates. ``tick()`` propagates
    ``ValidationError`` when a claimed job's ``project_id`` names no row in
    ``projects`` (Task 3's review; Task 11's confirmed it also kills a
    supervisor loop). ``ytauto run`` is the first thing that ever drains a
    real queue, and ``claim()`` takes the highest-priority claimable job, not
    necessarily the one this invocation just enqueued - a poison job left
    behind by unrelated earlier breakage, given a high priority, must make a
    healthy invocation fail loudly rather than silently report success while
    skipping past it.
    """
    _project(db_conn, slug="good")
    now = utc_now_iso()
    with transaction(db_conn):
        db_conn.execute(
            """
            INSERT INTO jobs
                (id, project_id, pipeline_id, state, priority, attempts, created_at, updated_at)
            VALUES ('poison', 'no-such-project', 'story_video', 'queued', 100, 0, ?, ?)
            """,
            (now, now),
        )

    rc = main(["--data-dir", str(tmp_path), "run", "--project", "good", "--max-ticks", "20"])

    assert rc == 1
    assert "no-such-project" in capsys.readouterr().err
    # The poison job (priority 100) was claimed first and the dispatcher died
    # mid-claim on it - it never got to release the claim, so it is stuck
    # "running" until its lease expires, exactly as the brief describes.
    poison_state = db_conn.execute("SELECT state FROM jobs WHERE id = 'poison'").fetchone()
    assert poison_state["state"] == "running"
    # "good" was never even claimed - it must not be reported as succeeded.
    assert _job_state(db_conn, slug="good") == "queued"
