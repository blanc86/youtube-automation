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
import json
import sqlite3
import time
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path

import pytest

from ytauto.app.scheduler.worker_protocol import ArtifactLine, Error, Result, Staged, encode
from ytauto.app.services.enqueue import DEFAULT_SETTINGS, story_digest_for
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
    conn: sqlite3.Connection,
    tmp_path: Path,
    *,
    slug: str,
    settings: dict[str, object] | None = None,
) -> str:
    """A project ``ytauto run`` will actually accept.

    This used to create a row with ``settings={}``, which ``run`` tolerated
    only because nothing checked. The whole-branch review's Critical 1 fix
    makes ``run`` re-derive and then validate a project's settings before it
    enqueues anything (``refresh_run_settings``), so a project now needs a
    real story file on disk at ``story_path`` and the full
    ``DEFAULT_SETTINGS`` template - exactly what ``ytauto project create``
    now seeds. Building that here rather than calling ``create_project``
    keeps these tests free to invent slugs without also inventing a
    ``--data-dir`` layout, while still exercising the real precondition.

    ``broll_manifest_digest`` is deliberately *not* seeded: ``run`` derives
    it from the B-roll library on every invocation, so seeding one here would
    hide a regression in that derivation.
    """
    story = tmp_path / f"{slug}-story.txt"
    story.write_text(f"a story for {slug}\n", encoding="utf-8")
    merged: dict[str, object] = {
        **DEFAULT_SETTINGS,
        "story_digest": story_digest_for(story),
        "story_path": str(story),
    }
    if settings is not None:
        merged.update(settings)
    return ProjectService(conn).create(
        slug=slug, title=slug, story_digest=merged["story_digest"], settings=merged
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


def test_project_create_rejects_a_duplicate_slug_without_corrupting_the_existing_project(
    tmp_path: Path, db_conn: sqlite3.Connection
) -> None:
    """Review finding (Important #1): a duplicate-slug retry used to write
    ``story.txt`` and stage a CAS blob *before* the uniqueness check ran, so
    retrying with genuinely different content overwrote the existing
    project's on-disk story and left ``story_digest`` pointing at stale
    content. This drives the second call with *different* content from the
    first - the original duplicate-slug test alone cannot catch the defect,
    since it reuses identical content both times.
    """
    paths = AppPaths.resolve(override=tmp_path)
    story_v1 = tmp_path / "story_v1.txt"
    story_v1.write_text("the original story\n", encoding="utf-8")
    argv_v1 = [
        "--data-dir",
        str(tmp_path),
        "project",
        "create",
        "--slug",
        "dup2",
        "--title",
        "Dup2",
        "--story",
        str(story_v1),
    ]
    assert main(argv_v1) == 0

    on_disk = paths.projects / "dup2" / "story.txt"
    original_text = on_disk.read_text(encoding="utf-8")
    original_row = db_conn.execute(
        "SELECT story_digest FROM projects WHERE slug = 'dup2'"
    ).fetchone()
    original_digest = original_row["story_digest"]

    story_v2 = tmp_path / "story_v2.txt"
    story_v2.write_text("a completely different story\n", encoding="utf-8")
    argv_v2 = [
        "--data-dir",
        str(tmp_path),
        "project",
        "create",
        "--slug",
        "dup2",
        "--title",
        "Dup2 again",
        "--story",
        str(story_v2),
    ]

    rc = main(argv_v2)

    assert rc == 2
    assert on_disk.read_text(encoding="utf-8") == original_text, (
        "the existing project's on-disk story must be untouched"
    )
    row = db_conn.execute("SELECT story_digest FROM projects WHERE slug = 'dup2'").fetchone()
    assert row["story_digest"] == original_digest, "the DB row must be untouched"
    assert db_conn.execute("SELECT count(*) FROM projects").fetchone()[0] == 1

    cas = CasStore(root=paths.cas, conn=db_conn)
    new_digest = story_digest_for(story_v2)
    assert not cas.exists(new_digest), "the rejected second story must never reach the CAS"


def test_project_create_rejects_a_non_utf8_story_file(
    tmp_path: Path, db_conn: sqlite3.Connection, capsys: pytest.CaptureFixture[str]
) -> None:
    """Review finding (Important #2): ``story_digest_for``/``create_project``
    both document ``UnicodeDecodeError`` and ``OSError`` alongside
    ``ValidationError``, but the CLI handler used to catch only
    ``ValidationError`` - a non-UTF-8 story file (an ordinary Windows-1252
    save is entirely plausible) crashed out of ``main()`` as a raw traceback
    instead of the documented exit-2 contract.
    """
    story = tmp_path / "story.txt"
    # 0x93/0x94 are Windows-1252 curly quotes; neither is a valid UTF-8 lead
    # or continuation byte on its own, so this reliably fails utf-8 decoding.
    story.write_bytes(b"Hello \x93World\x94\n")

    rc = main(
        [
            "--data-dir",
            str(tmp_path),
            "project",
            "create",
            "--slug",
            "bad-encoding",
            "--title",
            "Bad Encoding",
            "--story",
            str(story),
        ]
    )

    assert rc == 2
    assert capsys.readouterr().err  # some diagnostic reached the operator
    assert db_conn.execute("SELECT count(*) FROM projects").fetchone()[0] == 0


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
    _project(db_conn, tmp_path, slug="ghost-train")
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


def test_run_rejects_a_project_whose_settings_are_invalid_before_enqueueing_anything(
    tmp_path: Path, db_conn: sqlite3.Connection, capsys: pytest.CaptureFixture[str]
) -> None:
    """Whole-branch review, Critical 1's second half. With no ``ytauto project
    set-setting`` verb, hand-editing ``projects.settings_json`` is the only
    way to configure a project, so an inverted bound is the expected path
    rather than an exotic one. It must be exit 2 (bad input, like an unknown
    slug) with the offending key named, and nothing enqueued - not a job that
    runs four stages and then dies FATAL on a `TypeError` deep inside
    ``plan_timeline``.
    """
    _project(db_conn, tmp_path, slug="misconfigured", settings={"words_per_group_max": 0})

    # --max-ticks 0 so that if the validation ever regresses, this test fails
    # on its own assertions rather than by driving the real story_video
    # pipeline - which would spawn genuine worker subprocesses and reach the
    # network from what is meant to be a hermetic unit test.
    rc = main(
        ["--data-dir", str(tmp_path), "run", "--project", "misconfigured", "--max-ticks", "0"]
    )

    assert rc == 2
    assert "words_per_group_max" in capsys.readouterr().err
    assert db_conn.execute("SELECT count(*) FROM jobs").fetchone()[0] == 0


def test_run_rebinds_the_story_digest_and_the_broll_manifest_digest_before_enqueueing(
    tmp_path: Path, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Whole-branch review, Critical 2 (first half) and Critical 1 (the key no
    CLI verb could set), proven through ``main()`` rather than only against
    ``refresh_run_settings`` directly.

    The project is seeded with a *stale* ``story_digest`` and no
    ``broll_manifest_digest`` at all - the state ``ytauto project create``
    leaves behind plus an edit. After one ``ytauto run``, the digest must
    match the file on disk (or an edited story is silently ignored, since
    ``ingest_story`` fingerprints the digest and reads the path) and the
    manifest digest must be present (or every compose stage dies on
    ``KeyError``).
    """
    paths = AppPaths.resolve(override=tmp_path)
    project_id = _project(db_conn, tmp_path, slug="edited", settings={"story_digest": "0" * 64})
    _preseed_cache_hit(paths, db_conn)
    monkeypatch.setattr("ytauto.cli.__main__.build_pipeline", _fake_build_pipeline)

    assert main(["--data-dir", str(tmp_path), "run", "--project", "edited"]) == 0

    settings = ProjectService(db_conn).settings_for(project_id)
    assert settings["story_digest"] == story_digest_for(tmp_path / "edited-story.txt")
    assert settings["story_digest"] != "0" * 64
    assert isinstance(settings["broll_manifest_digest"], str)


def test_run_returns_1_when_the_job_fails(
    tmp_path: Path, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The real ``handle_error`` -> ``_fail_job`` path, reached through a fake
    worker that reports a FATAL error instead of a result - not a cache hit
    this time, so ``tick()`` must actually spawn (the monkeypatched ``Popen``)
    and pump a terminal message.
    """
    _project(db_conn, tmp_path, slug="doomed")
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


def test_run_retries_a_retryable_failure_through_to_success(
    tmp_path: Path, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review finding (Important #3): a RETRYABLE worker error defers the job
    via ``jobs.available_at`` (``Dispatcher._retry_stage``), which makes the
    *next* ``tick()`` report idle immediately - nowhere near ``--max-ticks`` -
    because nothing is currently claimable. ``run_until_idle`` stops on that
    first idle tick, so a single call leaves the job neither ``succeeded``
    nor ``failed``. ``_run`` must keep polling through the backoff, within
    its wall-clock budget, rather than treating that first idle as "nothing
    left to do".

    Drives the *real* ``Dispatcher``/backoff machinery - not a fake - via a
    Popen spy that fails once (``RETRYABLE``) and then succeeds, referencing
    a real digest already staged (but not yet recorded) in the CAS so the
    eventual successful ``commit_stage`` finds a genuine blob on disk.
    ``_BASE_BACKOFF_S`` and ``_RUN_POLL_INTERVAL_S`` are both monkeypatched
    down so the test does not have to sleep through the real (>=5s) minimum
    backoff or the real (1s) poll interval.
    """
    paths = AppPaths.resolve(override=tmp_path)
    _project(db_conn, tmp_path, slug="flaky")
    monkeypatch.setattr("ytauto.cli.__main__.build_pipeline", _fake_build_pipeline)
    monkeypatch.setattr("ytauto.app.scheduler.dispatcher._BASE_BACKOFF_S", 0.05)
    monkeypatch.setattr("ytauto.cli.__main__._RUN_POLL_INTERVAL_S", 0.01)

    fixed_job_id = "a" * 32
    monkeypatch.setattr("uuid.uuid4", lambda: _FixedUUID(fixed_job_id))

    cas = CasStore(root=paths.cas, conn=db_conn)
    output = b"real worker output"
    real_digest = cas.stage_file(output, kind="blob")  # staged, deliberately not recorded

    retryable_line = encode(
        Error(
            job_id=fixed_job_id,
            stage_id=_FAKE_STAGE_ID,
            correlation_id="c1",
            message="simulated transient failure",
            kind=ErrorKind.RETRYABLE,
        )
    )
    staged_line = encode(
        Staged(
            job_id=fixed_job_id,
            stage_id=_FAKE_STAGE_ID,
            correlation_id="c2",
            digest=real_digest,
            kind="blob",
            size_bytes=len(output),
        )
    )
    result_line = encode(
        Result(
            job_id=fixed_job_id,
            stage_id=_FAKE_STAGE_ID,
            correlation_id="c2",
            artifacts=(ArtifactLine(name="out", kind="blob", digest=real_digest),),
        )
    )

    calls = {"n": 0}

    def _spy(argv: object, **kwargs: object) -> _FakeProcess:
        calls["n"] += 1
        if calls["n"] == 1:
            return _FakeProcess([retryable_line])
        return _FakeProcess([staged_line, result_line])

    monkeypatch.setattr("ytauto.app.scheduler.dispatcher.Popen", _spy)

    rc = main(["--data-dir", str(tmp_path), "run", "--project", "flaky", "--max-ticks", "20"])

    assert rc == 0
    assert _job_state(db_conn, slug="flaky") == "succeeded"
    assert calls["n"] == 2, "must have spawned exactly twice: the transient failure, then success"


def test_run_reports_a_distinct_message_when_the_retry_wait_budget_is_exhausted(
    tmp_path: Path,
    db_conn: sqlite3.Connection,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The other half of Important #3's fix: giving up on a retry backoff
    must not be reported with the same message as running out of
    ``--max-ticks`` while genuinely busy - "made no progress, still healthy"
    and "give up and go look at what failed" send an operator to different
    places.

    Review round 3 caught that the wall-clock budget must measure a
    *continuous stretch with no progress*, not elapsed time since the whole
    invocation started - a round that spawns a worker (even one that
    immediately re-defers) resets the clock (``waiting_since``), because real
    work happened. So reproducing a genuine timeout needs a backoff (5s, the
    real minimum, left unpatched) long enough relative to the tiny wall-clock
    budget (0.05s, monkeypatched from the real 600s) that every poll *after*
    the one spawn+defer finds the job simply not yet due - zero progress each
    of those rounds - until the budget is exhausted. The fake worker is
    therefore invoked exactly once: the 5s backoff never elapses within this
    test's short wall-clock lifetime, so nothing ever spawns a second time.
    """
    _project(db_conn, tmp_path, slug="stuck-retrying")
    monkeypatch.setattr("ytauto.cli.__main__.build_pipeline", _fake_build_pipeline)
    monkeypatch.setattr("ytauto.cli.__main__._RUN_WALL_CLOCK_BUDGET_S", 0.05)
    monkeypatch.setattr("ytauto.cli.__main__._RUN_POLL_INTERVAL_S", 0.01)

    fixed_job_id = "b" * 32
    monkeypatch.setattr("uuid.uuid4", lambda: _FixedUUID(fixed_job_id))

    retryable_line = encode(
        Error(
            job_id=fixed_job_id,
            stage_id=_FAKE_STAGE_ID,
            correlation_id="c1",
            message="simulated transient failure",
            kind=ErrorKind.RETRYABLE,
        )
    )

    calls = {"n": 0}

    def _spy(argv: object, **kwargs: object) -> _FakeProcess:
        calls["n"] += 1
        return _FakeProcess([retryable_line])

    monkeypatch.setattr("ytauto.app.scheduler.dispatcher.Popen", _spy)

    rc = main(
        ["--data-dir", str(tmp_path), "run", "--project", "stuck-retrying", "--max-ticks", "20"]
    )

    assert rc == 1
    err = capsys.readouterr().err
    assert "made no progress" in err
    assert "did not reach a terminal state within" not in err
    # Review finding: the message must say what a rerun actually does (starts
    # a SEPARATE new job) rather than implying it resumes the deferred one -
    # "keep draining it" was the misleading phrasing this replaced.
    assert "SEPARATE new job" in err
    assert "keep draining it" not in err
    assert calls["n"] == 1, "the real 5s backoff must not have elapsed during this test"
    assert _job_state(db_conn, slug="stuck-retrying") == "queued"


def test_run_does_not_time_out_a_retry_that_follows_a_long_busy_phase(
    tmp_path: Path,
    db_conn: sqlite3.Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review round 3's central finding: anchoring the wall-clock budget to
    invocation start (rather than to the most recent progress) meant a real,
    healthy render that legitimately spends longer than the budget doing
    genuine work - TTS, B-roll selection, ffmpeg composes - would report a
    false "gave up waiting" timeout on its very first retry, having waited
    *zero* actual idle seconds for anything.

    Reproduced with a three-stage busy phase whose ``fingerprint()`` each
    sleeps for real (0.1s x 3 = ~0.3s total - a stand-in for slow but
    legitimate work; each is still a cache hit, so no subprocess is spawned),
    chained via ``depends_on`` ahead of the real stage that then fails once
    (RETRYABLE) before succeeding on retry. ``_RUN_WALL_CLOCK_BUDGET_S`` is
    monkeypatched to 0.15s - deliberately *shorter* than the busy phase alone.
    Under the pre-fix "anchor to invocation start" code this reliably timed
    out on the very first idle check (the busy phase's own real sleep time
    already exceeded the budget before any actual backoff-waiting began);
    under the fix, real progress (the three cache-hit skips, and the spawn
    itself) resets the waiting clock, so the run must still recover.
    """
    paths = AppPaths.resolve(override=tmp_path)
    _project(db_conn, tmp_path, slug="slow-but-healthy")

    busy_stage_ids = ("busy1", "busy2", "busy3")
    fingerprints = {sid: f"{sid[-1]}" * 64 for sid in busy_stage_ids}
    fingerprints[_FAKE_STAGE_ID] = _FIXED_FINGERPRINT
    _BUSY_SLEEP_S = 0.1

    class _BusyStage:
        """A stage whose fingerprint() sleeps for real - a stand-in for the
        wall-clock time a genuine stage's own work takes, without spawning a
        real subprocess (it is always a cache hit; see the pre-recorded
        artifacts below)."""

        def __init__(self, stage_id: str, depends_on: tuple[str, ...]) -> None:
            self.id = stage_id
            self.version = 1
            self.depends_on = depends_on
            self.settings_keys: tuple[str, ...] = ()
            self.gpu_pool = "gpu_compute"

        def fingerprint(self, ctx: JobContext) -> str:
            time.sleep(_BUSY_SLEEP_S)
            return fingerprints[self.id]

        def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult:
            raise NotImplementedError("cache-hit only")

    class _FixedStageAfterBusy(_FixedStage):
        depends_on = ("busy3",)

    def _busy_then_retry_pipeline(
        pipeline_id: str, cas: CasStore, settings: Mapping[str, object]
    ) -> Pipeline:
        return Pipeline(
            id=pipeline_id,
            stages=(
                _BusyStage("busy1", ()),
                _BusyStage("busy2", ("busy1",)),
                _BusyStage("busy3", ("busy2",)),
                _FixedStageAfterBusy(),
            ),
        )

    monkeypatch.setattr("ytauto.cli.__main__.build_pipeline", _busy_then_retry_pipeline)
    # Shorter than the ~0.3s busy phase alone - the whole point of the test.
    monkeypatch.setattr("ytauto.cli.__main__._RUN_WALL_CLOCK_BUDGET_S", 0.15)
    monkeypatch.setattr("ytauto.cli.__main__._RUN_POLL_INTERVAL_S", 0.02)
    monkeypatch.setattr("ytauto.app.scheduler.dispatcher._BASE_BACKOFF_S", 0.05)

    fixed_job_id = "c" * 32
    monkeypatch.setattr("uuid.uuid4", lambda: _FixedUUID(fixed_job_id))

    # Pre-record cache hits for the three busy stages so each is a real,
    # slow-but-non-spawning round trip through tick() - genuine "made
    # progress" ticks with no worker subprocess involved.
    cas = CasStore(root=paths.cas, conn=db_conn)
    artifacts = ArtifactStore(cas, db_conn)
    for sid in busy_stage_ids:
        blob = f"{sid}-cached-output".encode()
        digest = cas.put_bytes(blob, kind="blob")
        artifacts.record(
            fingerprints[sid], sid, [ArtifactRef(name="out", kind="blob", digest=digest)]
        )

    real_output = b"real worker output"
    real_digest = cas.stage_file(real_output, kind="blob")

    retryable_line = encode(
        Error(
            job_id=fixed_job_id,
            stage_id=_FAKE_STAGE_ID,
            correlation_id="c1",
            message="simulated transient failure",
            kind=ErrorKind.RETRYABLE,
        )
    )
    staged_line = encode(
        Staged(
            job_id=fixed_job_id,
            stage_id=_FAKE_STAGE_ID,
            correlation_id="c2",
            digest=real_digest,
            kind="blob",
            size_bytes=len(real_output),
        )
    )
    result_line = encode(
        Result(
            job_id=fixed_job_id,
            stage_id=_FAKE_STAGE_ID,
            correlation_id="c2",
            artifacts=(ArtifactLine(name="out", kind="blob", digest=real_digest),),
        )
    )

    calls = {"n": 0}

    def _spy(argv: object, **kwargs: object) -> _FakeProcess:
        calls["n"] += 1
        if calls["n"] == 1:
            return _FakeProcess([retryable_line])
        return _FakeProcess([staged_line, result_line])

    monkeypatch.setattr("ytauto.app.scheduler.dispatcher.Popen", _spy)

    rc = main(
        [
            "--data-dir",
            str(tmp_path),
            "run",
            "--project",
            "slow-but-healthy",
            "--max-ticks",
            "20",
        ]
    )

    assert rc == 0, "a retry immediately after a long busy phase must still recover, not time out"
    assert _job_state(db_conn, slug="slow-but-healthy") == "succeeded"
    assert calls["n"] == 2


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
    _project(db_conn, tmp_path, slug="stalled")

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
    _project(db_conn, tmp_path, slug="good")
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
