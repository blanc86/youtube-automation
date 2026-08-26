"""Endpoint-level tests for the local web UI.

**Hermeticity comes first here, and it is not optional.** An earlier task on
this project shipped tests that resolved a real output directory and wrote
into the user's own ``Videos\\ytauto``. Two autouse fixtures below close that:
``client`` builds the app against ``tmp_path``, and ``_hermetic_output_dir``
pins ``ytauto.ui.app.resolve_output_dir`` under ``tmp_path`` so nothing in
this file can touch a real Videos or Downloads folder even if a route calls
it by accident.

No browser automation. These drive Flask's own test client, which exercises
the real routing, the real forms, the real templates and the real services
behind them - everything except the JavaScript, which is three event handlers
and a fetch loop.

The render test uses a fake pipeline (``ytauto.app.services.render.build_pipeline``
replaced, the same convention ``tests/unit/cli/test_run_command.py``
established) whose stages are pre-recorded cache hits, so no worker
subprocess is ever spawned - which is what keeps this a unit test.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterator, Mapping
from pathlib import Path

import pytest
from flask.testing import FlaskClient

from ytauto.app.services.enqueue import refresh_run_settings, story_digest_for
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.pipeline.graph import Pipeline
from ytauto.core.pipeline.stage import JobContext, ProgressFn, StageResult
from ytauto.infra.artifacts import ArtifactStore
from ytauto.infra.cas.store import CasStore
from ytauto.infra.db.engine import connect, transaction
from ytauto.infra.paths import AppPaths
from ytauto.ui.app import create_app
from ytauto.ui.tasks import TaskManager

# -- fixtures -------------------------------------------------------------


@pytest.fixture()
def paths(tmp_path: Path) -> AppPaths:
    return AppPaths.resolve(override=tmp_path)


@pytest.fixture(autouse=True)
def _hermetic_output_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Never the real Videos folder. See this module's docstring."""
    monkeypatch.setattr(
        "ytauto.ui.app.resolve_output_dir", lambda **_kwargs: tmp_path / "auto-output"
    )


@pytest.fixture()
def tasks() -> Iterator[TaskManager]:
    """A task manager whose threads are joined before the test ends.

    Not tidiness: ``filterwarnings`` promotes ``ResourceWarning`` and
    ``PytestUnraisableExceptionWarning`` to errors, so a background thread
    still holding a SQLite connection when ``tmp_path`` is torn down is a
    failing test.
    """
    manager = TaskManager()
    try:
        yield manager
    finally:
        manager.close(timeout=30)


@pytest.fixture()
def client(paths: AppPaths, tasks: TaskManager) -> Iterator[FlaskClient]:
    app = create_app(paths, tasks=tasks)
    with app.test_client() as test_client:
        yield test_client


@pytest.fixture()
def db_conn(paths: AppPaths) -> Iterator[sqlite3.Connection]:
    """A connection to the very database file the app is using.

    Opened after the app, which is what runs the migrations - the same order
    the real process uses.
    """
    conn = connect(paths.db_file)
    try:
        yield conn
    finally:
        conn.close()


# -- helpers --------------------------------------------------------------


def _create(client: FlaskClient, *, title: str, story: str = "A story.\n") -> str:
    """Create a project through the UI and return the slug it derived."""
    response = client.post("/projects/new", data={"title": title, "story": story})
    assert response.status_code == 302, response.data[:400]
    location = response.headers["Location"]
    return location.rsplit("/", 1)[-1]


def _settings(conn: sqlite3.Connection, slug: str) -> dict[str, object]:
    row = conn.execute("SELECT settings_json FROM projects WHERE slug = ?", (slug,)).fetchone()
    assert row is not None, f"no project row for {slug!r}"
    parsed: dict[str, object] = json.loads(row["settings_json"])
    return parsed


def _settings_form(**overrides: object) -> dict[str, object]:
    """A complete, valid settings form. Overrides replace individual fields.

    Every field must be present: an HTML form posts all of its inputs, and a
    partial post would exercise a shape the browser never produces.
    """
    form: dict[str, object] = {
        "voice": "en-GB-RyanNeural",
        "rate": "+10%",
        "encoder": "libx264",
        "seed": "7",
        "words_per_group_min": "3",
        "words_per_group_max": "6",
        "segment_seconds_min": "1.5",
        "segment_seconds_max": "4.0",
        "primary_colour": "#ffffff",
        "accent_colour": "#ffff00",
        "alignment": "2",
        "font_size": "",
    }
    form.update(overrides)
    return form


# -- project creation -----------------------------------------------------


def test_creating_a_project_writes_story_txt_and_the_row(
    client: FlaskClient, db_conn: sqlite3.Connection, paths: AppPaths
) -> None:
    slug = _create(client, title="The Ghost Train", story="It never stopped.\n")

    assert slug == "the-ghost-train"
    row = db_conn.execute(
        "SELECT title, story_digest FROM projects WHERE slug = ?", (slug,)
    ).fetchone()
    assert row is not None
    assert row["title"] == "The Ghost Train"

    on_disk = paths.projects / slug / "story.txt"
    assert on_disk.is_file(), "the human-editable copy must exist in the project directory"
    assert on_disk.read_text(encoding="utf-8") == "It never stopped.\n"
    assert row["story_digest"] == story_digest_for(on_disk)


def test_the_slug_is_derived_and_the_user_never_types_one(client: FlaskClient) -> None:
    """Punctuation, case and accents all fold; the form has no slug field."""
    page = client.get("/projects/new")
    assert b'name="slug"' not in page.data, "the UI must never ask for a slug"

    assert _create(client, title="  Café: The 3 A.M. Call!  ") == "cafe-the-3-a-m-call"


def test_a_colliding_title_gets_a_numeric_suffix(
    client: FlaskClient, db_conn: sqlite3.Connection
) -> None:
    """Guard-pin. Two projects may share a title; they may not share a slug,
    a directory, or each other's story file.
    """
    first = _create(client, title="Night Shift", story="one\n")
    second = _create(client, title="Night Shift", story="two\n")
    third = _create(client, title="Night Shift", story="three\n")

    assert (first, second, third) == ("night-shift", "night-shift-2", "night-shift-3")

    # The point of the suffix: three distinct rows, three distinct stories.
    stored = {
        slug: Path(str(_settings(db_conn, slug)["story_path"])).read_text(encoding="utf-8")
        for slug in (first, second, third)
    }
    assert stored == {
        "night-shift": "one\n",
        "night-shift-2": "two\n",
        "night-shift-3": "three\n",
    }


def test_a_blank_title_is_refused_with_the_story_still_in_the_form(client: FlaskClient) -> None:
    response = client.post("/projects/new", data={"title": "  ", "story": "Keep me.\n"})

    assert response.status_code == 400
    body = response.data.decode("utf-8")
    assert "A title is required" in body
    assert "Keep me." in body, "a rejected form must come back with what the user typed"


def test_the_script_prompt_panel_shows_the_docs_file_verbatim(
    client: FlaskClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The prompt is read at runtime, never duplicated into the UI source."""
    doc = tmp_path / "SCRIPT-PROMPT.md"
    doc.write_text(
        "# The script prompt\n\nBlurb nobody should paste.\n\n"
        "## The prompt\n\n```text\nWRITE THE SCRIPT.\nTARGET LENGTH: [45 seconds]\n```\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("YTAUTO_SCRIPT_PROMPT", str(doc))

    body = client.get("/projects/new").data.decode("utf-8")

    assert "WRITE THE SCRIPT." in body
    assert "TARGET LENGTH: [45 seconds]" in body
    assert "Blurb nobody should paste." not in body, "only the fenced prompt block is shown"


# -- editing the story ----------------------------------------------------


def test_editing_the_story_rewrites_story_txt_and_changes_the_next_run_digest(
    client: FlaskClient, db_conn: sqlite3.Connection, paths: AppPaths
) -> None:
    """End to end, the whole point of making the story editable.

    An edited story must invalidate the cache. It does so through
    ``refresh_run_settings``, which recomputes ``story_digest`` from disk on
    the next run - so this asserts the file changed AND that the digest the
    next run would use follows it, rather than asserting the write alone and
    hoping.
    """
    slug = _create(client, title="Rewrite Me", story="First draft.\n")
    story_path = Path(str(_settings(db_conn, slug)["story_path"]))
    original_digest = _settings(db_conn, slug)["story_digest"]

    response = client.post(f"/projects/{slug}/story", data={"story": "Second draft.\r\n"})
    assert response.status_code == 302

    assert story_path.read_text(encoding="utf-8") == "Second draft.\n"
    assert story_path.read_bytes().count(b"\r\r") == 0, (
        "a textarea posts CRLF; write_text then translates \\n again, doubling it"
    )

    project_id = str(
        db_conn.execute("SELECT id FROM projects WHERE slug = ?", (slug,)).fetchone()["id"]
    )
    fresh = refresh_run_settings(db_conn, CasStore(root=paths.cas, conn=db_conn), project_id)
    assert fresh["story_digest"] != original_digest
    assert fresh["story_digest"] == story_digest_for(story_path)


def test_an_empty_story_edit_is_refused_and_leaves_the_file_alone(
    client: FlaskClient, db_conn: sqlite3.Connection
) -> None:
    slug = _create(client, title="Keep It", story="Do not lose this.\n")
    story_path = Path(str(_settings(db_conn, slug)["story_path"]))

    response = client.post(f"/projects/{slug}/story", data={"story": "   \n"})

    assert response.status_code == 400
    assert story_path.read_text(encoding="utf-8") == "Do not lose this.\n"


# -- settings -------------------------------------------------------------


def test_settings_round_trip_through_the_form(
    client: FlaskClient, db_conn: sqlite3.Connection
) -> None:
    slug = _create(client, title="Tuned")

    response = client.post(f"/projects/{slug}/settings", data=_settings_form())
    assert response.status_code == 302

    stored = _settings(db_conn, slug)
    assert stored["voice"] == "en-GB-RyanNeural"
    assert stored["rate"] == "+10%"
    assert stored["encoder"] == "libx264"
    assert stored["seed"] == 7
    assert stored["words_per_group_max"] == 6
    assert stored["segment_seconds_max"] == 4.0

    style = stored["caption_style"]
    assert isinstance(style, Mapping)
    # &HAABBGGRR - alpha, then BLUE, GREEN, RED. Yellow is #ffff00 in HTML and
    # &H0000FFFF in ASS; getting this backwards renders blue captions.
    assert style["accent_colour"] == "&H0000FFFF"
    assert style["primary_colour"] == "&H00FFFFFF"
    assert style["alignment"] == 2
    assert "font_size" not in style, "a blank font size means automatic, per canvas"

    # And the form shows them again on the next visit.
    body = client.get(f"/projects/{slug}").data.decode("utf-8")
    assert 'value="en-GB-RyanNeural"' in body
    assert 'value="#ffff00"' in body
    assert '<option value="2" selected>' in body


def test_settings_validation_rejects_an_inverted_pair_and_writes_nothing(
    client: FlaskClient, db_conn: sqlite3.Connection
) -> None:
    """Guard-pin. ``validate_settings`` is the authority, and a rejected form
    must not have written half of itself first.
    """
    slug = _create(client, title="Bad Bounds")
    before = _settings(db_conn, slug)

    response = client.post(
        f"/projects/{slug}/settings",
        data=_settings_form(
            words_per_group_min="9", words_per_group_max="2", voice="ShouldNotStick"
        ),
    )

    assert response.status_code == 400
    assert "words_per_group_max" in response.data.decode("utf-8")
    assert _settings(db_conn, slug) == before, (
        "validation runs before any set_setting call, so nothing is written"
    )


def test_a_non_numeric_field_is_a_form_error_not_a_traceback(client: FlaskClient) -> None:
    slug = _create(client, title="Not A Number")

    response = client.post(f"/projects/{slug}/settings", data=_settings_form(seed="soon"))

    assert response.status_code == 400
    assert "whole number" in response.data.decode("utf-8")


def test_an_existing_caption_alpha_survives_the_form(
    client: FlaskClient, db_conn: sqlite3.Connection
) -> None:
    """The form has no alpha control, so it must give back what it found.

    ASS alpha is inverted (00 opaque, FF transparent) and has no HTML
    equivalent; silently forcing every colour opaque would undo a deliberate
    hand edit with no way to notice.
    """
    slug = _create(client, title="Half Faded")
    row = db_conn.execute("SELECT id FROM projects WHERE slug = ?", (slug,)).fetchone()
    from ytauto.app.services.projects import ProjectService

    ProjectService(db_conn).set_setting(row["id"], "caption_style", {"accent_colour": "&H8000FFFF"})

    client.post(f"/projects/{slug}/settings", data=_settings_form(accent_colour="#ff0000"))

    style = _settings(db_conn, slug)["caption_style"]
    assert isinstance(style, Mapping)
    assert style["accent_colour"] == "&H800000FF", "alpha 80 kept, red written in BGR order"


# -- b-roll ---------------------------------------------------------------


def test_broll_add_refuses_a_missing_licence(client: FlaskClient, tmp_path: Path) -> None:
    """Guard-pin-adjacent: the provenance record is the reason this form
    exists, so a blank licence must never reach ``BrollLibrary.add``.
    """
    clip = tmp_path / "clip.mp4"
    clip.write_bytes(b"not really a video")

    response = client.post(
        "/broll",
        data={"path": str(clip), "source_url": "https://example.com/clip", "licence": "  "},
    )

    assert response.status_code == 400
    assert "licence is required" in response.data.decode("utf-8")


def test_broll_add_refuses_a_missing_source_url(client: FlaskClient, tmp_path: Path) -> None:
    clip = tmp_path / "clip.mp4"
    clip.write_bytes(b"not really a video")

    response = client.post("/broll", data={"path": str(clip), "source_url": "", "licence": "CC0"})

    assert response.status_code == 400
    assert "source URL is required" in response.data.decode("utf-8")


def test_the_broll_table_lists_a_clip_with_its_provenance(
    client: FlaskClient, db_conn: sqlite3.Connection
) -> None:
    """The list itself, without running ffmpeg: a row inserted directly is
    exactly what ``BrollLibrary.add`` would have committed.
    """
    db_conn.execute(
        """
        INSERT INTO broll_clips
            (id, source_digest, normalised_landscape_digest, normalised_vertical_digest,
             duration_s, width, height, source_url, licence, attribution, notes, added_at)
        VALUES ('c1', 'd0', 'd1', 'd2', 12.5, 1920, 1080,
                'https://example.com/forest', 'CC0', 'Someone', '', '2026-01-01T00:00:00Z')
        """
    )
    db_conn.commit()

    body = client.get("/broll").data.decode("utf-8")

    assert "12.5s" in body
    assert "1920" in body
    assert "CC0" in body
    assert "https://example.com/forest" in body


# -- rendering ------------------------------------------------------------

_STAGE_IDS = ("compose_landscape", "compose_vertical")
_ARTIFACTS = ("master_1920x1080.mp4", "master_1080x1920.mp4")
_FINGERPRINTS = ("a" * 64, "b" * 64)


class _CachedStage:
    """A Stage double under a real compose stage id with a fixed fingerprint.

    ``run`` is never reached: every render test below pre-records the
    artifacts for both fingerprints, so ``tick()`` takes the cache-hit path
    and no worker subprocess is spawned.
    """

    def __init__(self, stage_id: str, fingerprint: str) -> None:
        self.id = stage_id
        self.version = 1
        self.depends_on: tuple[str, ...] = ()
        self.settings_keys: tuple[str, ...] = ()
        self.gpu_pool = "gpu_encode"
        self._fingerprint = fingerprint

    def fingerprint(self, ctx: JobContext) -> str:
        return self._fingerprint

    def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult:
        raise NotImplementedError("every render test pre-seeds a cache hit")


def _seed_cache_hits(paths: AppPaths) -> None:
    conn = connect(paths.db_file)
    try:
        cas = CasStore(root=paths.cas, conn=conn)
        artifacts = ArtifactStore(cas, conn)
        for fingerprint, stage_id, name in zip(_FINGERPRINTS, _STAGE_IDS, _ARTIFACTS, strict=True):
            digest = cas.put_bytes(f"fake {name} bytes".encode(), kind="video")
            artifacts.record(
                fingerprint, stage_id, [ArtifactRef(name=name, kind="video", digest=digest)]
            )
    finally:
        conn.close()


@pytest.fixture()
def gated_pipeline(monkeypatch: pytest.MonkeyPatch) -> threading.Event:
    """Replace ``build_pipeline`` with a fake that blocks until released.

    The gate is what makes "the endpoint returns before the render does"
    assertable rather than a race: while it is closed, the worker thread is
    parked on its very first call, so the POST provably returned with the
    task still running.
    """
    gate = threading.Event()

    def _build(pipeline_id: str, cas: CasStore, settings: Mapping[str, object]) -> Pipeline:
        assert gate.wait(timeout=30), "the test never released the render gate"
        return Pipeline(
            id=pipeline_id,
            stages=tuple(
                _CachedStage(stage_id, fingerprint)
                for stage_id, fingerprint in zip(_STAGE_IDS, _FINGERPRINTS, strict=True)
            ),
        )

    monkeypatch.setattr("ytauto.app.services.render.build_pipeline", _build)
    return gate


def test_the_render_endpoint_returns_immediately_and_the_status_transitions(
    client: FlaskClient,
    paths: AppPaths,
    tasks: TaskManager,
    tmp_path: Path,
    gated_pipeline: threading.Event,
) -> None:
    slug = _create(client, title="Render Me")
    _seed_cache_hits(paths)

    accepted = client.post(f"/projects/{slug}/render")

    # 202, not 200: the render has not happened yet and the page must poll.
    assert accepted.status_code == 202
    task_id = accepted.get_json()["id"]

    running = client.get(f"/api/tasks/{task_id}").get_json()
    assert running["state"] == "running"
    assert running["done"] is False

    gated_pipeline.set()
    tasks.close(timeout=30)

    finished = client.get(f"/api/tasks/{task_id}").get_json()
    assert finished["state"] == "succeeded", finished["detail"]
    assert finished["done"] is True

    # The one thing the user most needs to see.
    export_dir = tmp_path / "auto-output" / slug
    assert finished["payload"]["output_dir"] == str(export_dir)
    for name in _ARTIFACTS:
        assert (export_dir / name).read_bytes() == f"fake {name} bytes".encode()

    # ...and it is on the page, not only in the JSON.
    body = client.get(f"/projects/{slug}").data.decode("utf-8")
    assert str(export_dir) in body


def test_a_second_render_of_the_same_project_is_refused_while_one_runs(
    client: FlaskClient, paths: AppPaths, gated_pipeline: threading.Event
) -> None:
    slug = _create(client, title="Only Once")
    _seed_cache_hits(paths)

    assert client.post(f"/projects/{slug}/render").status_code == 202
    second = client.post(f"/projects/{slug}/render")

    assert second.status_code == 409
    assert "already running" in second.get_json()["error"]
    gated_pipeline.set()


def test_rendering_an_unknown_project_is_a_404(client: FlaskClient) -> None:
    response = client.post("/projects/no-such-thing/render")

    assert response.status_code == 404
    assert "no such project" in response.get_json()["error"]


def test_a_failing_render_reports_the_failure_rather_than_a_directory(
    client: FlaskClient, tasks: TaskManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A render that never reaches ``succeeded`` must not offer an output
    path - a folder named in a success message that has nothing in it is
    worse than no message.
    """

    def _explode(pipeline_id: str, cas: CasStore, settings: Mapping[str, object]) -> Pipeline:
        raise RuntimeError("no stages registered")

    monkeypatch.setattr("ytauto.app.services.render.build_pipeline", _explode)
    slug = _create(client, title="Doomed")

    task_id = client.post(f"/projects/{slug}/render").get_json()["id"]
    tasks.close(timeout=30)

    finished = client.get(f"/api/tasks/{task_id}").get_json()
    assert finished["state"] == "failed"
    assert "no stages registered" in finished["detail"]
    assert "output_dir" not in finished["payload"]


def test_an_unknown_task_id_is_a_404(client: FlaskClient) -> None:
    assert client.get("/api/tasks/deadbeef").status_code == 404


# -- listing --------------------------------------------------------------


def test_the_project_list_shows_the_last_job_state(
    client: FlaskClient, db_conn: sqlite3.Connection
) -> None:
    slug = _create(client, title="Been Run")
    project_id = db_conn.execute("SELECT id FROM projects WHERE slug = ?", (slug,)).fetchone()["id"]
    for job_id, state, created in (
        ("j1", "failed", "2026-01-01T00:00:00Z"),
        ("j2", "succeeded", "2026-01-02T00:00:00Z"),
    ):
        db_conn.execute(
            """
            INSERT INTO jobs (id, project_id, pipeline_id, state, created_at, updated_at)
            VALUES (?, ?, 'story_video', ?, ?, ?)
            """,
            (job_id, project_id, state, created, created),
        )
    db_conn.commit()

    body = client.get("/").data.decode("utf-8")

    assert "Been Run" in body
    assert "state-succeeded" in body, "the most recent job's state is the one shown"


def test_a_project_that_has_never_run_says_so(client: FlaskClient) -> None:
    _create(client, title="Fresh")

    assert "never run" in client.get("/").data.decode("utf-8")


# -- the music library --------------------------------------------------------


def _seed_track(conn: sqlite3.Connection, track_id: str = "t1", title: str = "Slow Pulse") -> None:
    with transaction(conn, immediate=True):
        conn.execute(
            """
            INSERT INTO music_tracks (id, source_digest, duration_s, title,
                                      source_url, licence, attribution, notes, added_at)
            VALUES (?, ?, ?, ?, ?, ?, '', '', ?)
            """,
            (
                track_id,
                "b" * 64,
                42.0,
                title,
                "https://example.com/t",
                "CC0",
                "2026-01-01T00:00:00Z",
            ),
        )


def test_the_music_page_lists_the_library(client: FlaskClient, db_conn: sqlite3.Connection) -> None:
    _seed_track(db_conn)
    body = client.get("/music").get_data(as_text=True)
    assert "Slow Pulse" in body
    assert "CC0" in body


def test_adding_a_track_without_a_licence_is_refused_with_a_message(
    client: FlaskClient,
) -> None:
    """The provenance record is the point, and a browser must be told why in
    words rather than by a stack trace."""
    response = client.post(
        "/music", data={"path": "x.mp3", "source_url": "https://e/x", "licence": ""}
    )
    assert response.status_code == 400
    assert "licence is required" in response.get_data(as_text=True)


def test_a_project_offers_no_music_picker_until_the_library_has_something(
    client: FlaskClient, db_conn: sqlite3.Connection
) -> None:
    """An empty dropdown reading only 'No music' is a dead control; the page
    says where tracks come from instead."""
    slug = _create(client, title="No Bed")
    body = client.get(f"/projects/{slug}").get_data(as_text=True)
    assert 'id="music_track_id"' not in body
    assert "No tracks in the library yet" in body


def test_a_project_can_select_a_track_and_the_choice_persists(
    client: FlaskClient, db_conn: sqlite3.Connection
) -> None:
    _seed_track(db_conn)
    slug = _create(client, title="With A Bed")

    form = _settings_form(music_track_id="t1", music_gain_db="-24")
    client.post(f"/projects/{slug}/settings", data=form, follow_redirects=True)

    body = client.get(f"/projects/{slug}").get_data(as_text=True)
    assert 'value="t1" selected' in body.replace("  ", " ")
    assert 'value="-24.0"' in body or 'value="-24"' in body
