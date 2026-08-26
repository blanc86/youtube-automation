"""The Flask application: routes, and nothing that belongs behind them.

Every route here is a thin adapter. It reads a form, calls one of the
services the CLI already calls, and renders a template. Where a route looks
longer than that, the extra length is error handling for a browser - a form
that comes back with its values still in it and a message at the top, rather
than a traceback.

**Why Flask, and why not an async framework.** Three properties of this
particular application decided it:

1. Everything it touches is blocking and synchronous - SQLite, ffmpeg,
   ``shutil.copyfile``. There is no I/O here that an event loop would
   overlap, so async would buy nothing and would turn every one of those
   calls into something that must be remembered to be pushed to an executor.
   With a threaded WSGI server there is no event loop to block: a slow
   request occupies its own thread and no other request notices.
2. SQLite connections must not be shared across threads (see
   ``infra.db.engine``). A synchronous, thread-per-request server maps onto
   that exactly - one connection per request, opened in the thread that uses
   it, closed by ``teardown_appcontext``.
3. Flask's test client is part of Flask. FastAPI's needs ``httpx``; the
   endpoint tests are the gate's evidence that any of this works, and paying
   a second dependency for them is worse than paying none.

**One connection per request.** ``_conn()`` opens on first use within a
request and ``teardown_appcontext`` closes it. Nothing is cached across
requests: the test suite promotes ``ResourceWarning`` to an error, and a
connection that outlives its request is exactly what that setting exists to
catch.

**No authentication, by design and by binding.** See ``ytauto.ui.HOST``. That
also decides what is *not* here: no CSRF tokens, no sessions, no login. A
page that only loopback can reach, on a single-user machine, has no
attacker-controlled origin to defend against - and adding half a security
model would be worse than being clear about having none.
"""

from __future__ import annotations

import secrets
import sqlite3
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path

from flask import (
    Flask,
    Response,
    current_app,
    flash,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)
from flask.typing import ResponseReturnValue

from ytauto.app.services.enqueue import (
    create_project,
    refresh_run_settings,
    resolve_project_id,
)
from ytauto.app.services.projects import ProjectService
from ytauto.app.services.render import RenderState, render_project
from ytauto.core.errors import ValidationError
from ytauto.infra.artifacts import ArtifactStore
from ytauto.infra.broll import BrollLibrary
from ytauto.infra.cas.store import CasStore
from ytauto.infra.db.engine import connect
from ytauto.infra.db.migrations import apply_migrations
from ytauto.infra.paths import AppPaths, resolve_output_dir
from ytauto.ui import script_prompt
from ytauto.ui.settings_form import ALIGNMENTS, FormError, form_values, parse_form
from ytauto.ui.slugs import unique_slug
from ytauto.ui.tasks import TaskBusy, TaskManager, TaskRecord

_STORY_FILENAME = "story.txt"

_RENDER_MESSAGES: Mapping[RenderState, str] = {
    RenderState.FAILED: "The render failed.",
    RenderState.EXPORT_FAILED: "The video rendered, but copying it out failed.",
    RenderState.TICK_BUDGET_EXHAUSTED: (
        "The render ran out of dispatcher budget before finishing. Nothing was lost - "
        "start it again and it resumes from the last completed stage."
    ),
    RenderState.NO_PROGRESS_TIMEOUT: (
        "The render made no progress for long enough that this page stopped waiting. "
        "The job itself is untouched and will resume on its own; starting a render "
        "again now enqueues a separate new job."
    ),
}
"""What a non-success outcome says in a browser. Deliberately not the CLI's
wording: ``ytauto run``'s stderr is a contract an operator greps, and these
are sentences for someone who has never seen a dispatcher. The two are
allowed to differ precisely because ``RenderOutcome`` carries the *state* and
lets each front end phrase it."""


def create_app(paths: AppPaths, *, tasks: TaskManager | None = None) -> Flask:
    """Build the application against one data directory.

    ``paths`` is passed in rather than resolved here so the whole UI can be
    driven against a temporary directory - which the endpoint tests do, and
    must: an earlier task's tests wrote into the real ``Videos\\ytauto``.

    Migrations run once, here, on their own connection - the same thing every
    CLI subcommand does before it touches the database.
    """
    app = Flask(__name__)
    paths.ensure()
    # flash() needs a signing key. This is a loopback-only, single-user tool
    # with no login and no session data worth forging, so a per-process
    # random key is right: it costs nothing, and the only consequence of it
    # changing is that a flash message does not survive a restart.
    app.secret_key = secrets.token_bytes(32)
    # Never cache the stylesheet or the script. Flask's twelve-hour default is
    # for servers with many users and a CDN; this serves one person off local
    # disk, where the only thing that default can do is hand them yesterday's
    # CSS after they pull. The request cost is a file read.
    app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0
    app.config["YTAUTO_PATHS"] = paths
    app.config["YTAUTO_TASKS"] = tasks if tasks is not None else TaskManager()

    bootstrap = connect(paths.db_file)
    try:
        apply_migrations(bootstrap)
    finally:
        bootstrap.close()

    _register_routes(app)
    return app


def _paths() -> AppPaths:
    value: AppPaths = current_app.config["YTAUTO_PATHS"]
    return value


def _tasks() -> TaskManager:
    value: TaskManager = current_app.config["YTAUTO_TASKS"]
    return value


def _conn() -> sqlite3.Connection:
    """This request's database connection, opened on first use.

    Stored on ``g``, which is per application context and therefore per
    request and per thread - never shared. Closed by the teardown handler
    registered in ``_register_routes``.
    """
    existing = getattr(g, "ytauto_conn", None)
    if existing is not None:
        assert isinstance(existing, sqlite3.Connection)
        return existing
    conn = connect(_paths().db_file)
    g.ytauto_conn = conn
    return conn


def _cas(conn: sqlite3.Connection) -> CasStore:
    return CasStore(root=_paths().cas, conn=conn)


def _register_routes(app: Flask) -> None:
    @app.teardown_appcontext
    def _close_conn(_exc: BaseException | None) -> None:
        conn = getattr(g, "ytauto_conn", None)
        if conn is not None:
            g.ytauto_conn = None
            conn.close()

    # -- projects ---------------------------------------------------------

    @app.get("/")
    def index() -> str:
        return render_template("projects.html", projects=_project_rows(_conn()))

    @app.get("/projects/new")
    def new_project() -> str:
        return render_template(
            "new_project.html",
            title="",
            story="",
            script_prompt=script_prompt.load(),
        )

    @app.post("/projects/new")
    def create() -> ResponseReturnValue:
        title = request.form.get("title", "").strip()
        story = _normalise_newlines(request.form.get("story", ""))
        error = None
        if not title:
            error = "A title is required - the slug is derived from it."
        elif not story.strip():
            error = "The story is empty. Paste the narration you want rendered."
        if error is None:
            conn = _conn()
            slug = unique_slug(conn, title)
            try:
                _create_project_from_text(conn, slug=slug, title=title, story=story)
            except (ValidationError, OSError, UnicodeDecodeError) as exc:
                error = str(exc)
            else:
                flash(f"Created {title} as {slug}.", "success")
                return redirect(url_for("project", slug=slug))
        return (
            render_template(
                "new_project.html",
                title=title,
                story=story,
                script_prompt=script_prompt.load(),
                error=error,
            ),
            400,
        )

    @app.get("/projects/<slug>")
    def project(slug: str) -> ResponseReturnValue:
        conn = _conn()
        try:
            project_id = resolve_project_id(conn, slug)
        except ValidationError:
            flash(f"No project called {slug}.", "error")
            return redirect(url_for("index"))
        return _render_project_page(conn, project_id, slug)

    @app.post("/projects/<slug>/story")
    def save_story(slug: str) -> ResponseReturnValue:
        conn = _conn()
        try:
            project_id = resolve_project_id(conn, slug)
        except ValidationError:
            flash(f"No project called {slug}.", "error")
            return redirect(url_for("index"))
        story = _normalise_newlines(request.form.get("story", ""))
        if not story.strip():
            return _render_project_page(
                conn, project_id, slug, error="The story is empty - nothing was saved.", status=400
            )
        settings = ProjectService(conn).settings_for(project_id)
        story_path = settings.get("story_path")
        if not isinstance(story_path, str) or not story_path.strip():
            return _render_project_page(
                conn,
                project_id,
                slug,
                error="This project has no story_path setting, so there is nowhere to save.",
                status=400,
            )
        try:
            Path(story_path).write_text(story, encoding="utf-8")
        except OSError as exc:
            return _render_project_page(conn, project_id, slug, error=str(exc), status=400)
        # The digest is NOT recomputed here. refresh_run_settings does it on
        # the next run, from whatever is on disk at that moment - which is
        # the one place it can be right, and doing it here as well would be a
        # second source of truth for the same derived value.
        flash("Story saved. The next render will pick it up.", "success")
        return redirect(url_for("project", slug=slug))

    @app.post("/projects/<slug>/settings")
    def save_settings(slug: str) -> ResponseReturnValue:
        conn = _conn()
        try:
            project_id = resolve_project_id(conn, slug)
        except ValidationError:
            flash(f"No project called {slug}.", "error")
            return redirect(url_for("index"))
        projects = ProjectService(conn)
        current = projects.settings_for(project_id)
        try:
            parsed = parse_form(request.form, current=current)
        except (FormError, ValidationError) as exc:
            return _render_project_page(
                conn,
                project_id,
                slug,
                error=str(exc),
                status=400,
                settings_override={**current, **_salvage(request.form, current)},
            )
        for key, value in parsed.items():
            projects.set_setting(project_id, key, value)
        flash("Settings saved.", "success")
        return redirect(url_for("project", slug=slug))

    # -- rendering --------------------------------------------------------

    @app.post("/projects/<slug>/render")
    def render(slug: str) -> ResponseReturnValue:
        conn = _conn()
        try:
            project_id = resolve_project_id(conn, slug)
        except ValidationError as exc:
            return _json_error(str(exc), 404)
        try:
            record = _start_render(project_id, slug)
        except TaskBusy:
            return _json_error("A render for this project is already running.", 409)
        # 202: accepted, not finished. The whole point of this endpoint is
        # that it returns before the render does.
        return jsonify(record.as_json()), 202

    @app.get("/api/tasks/<task_id>")
    def task_status(task_id: str) -> ResponseReturnValue:
        record = _tasks().get(task_id)
        if record is None:
            return _json_error("no such task", 404)
        return jsonify(record.as_json())

    # -- b-roll -----------------------------------------------------------

    @app.get("/broll")
    def broll() -> str:
        return render_template("broll.html", clips=_broll_rows(_conn()))

    @app.post("/broll")
    def add_broll() -> ResponseReturnValue:
        form = request.form
        path = form.get("path", "").strip()
        source_url = form.get("source_url", "").strip()
        licence = form.get("licence", "").strip()
        error = None
        if not path:
            error = "A path to the video file is required."
        elif not source_url:
            error = "A source URL is required - it is half of the provenance record."
        elif not licence:
            error = "A licence is required - it is the other half."
        if error is None:
            try:
                record = _start_broll_add(
                    path=Path(path).expanduser(),
                    source_url=source_url,
                    licence=licence,
                    attribution=form.get("attribution", "").strip(),
                    notes=form.get("notes", "").strip(),
                )
            except TaskBusy:
                error = "A clip is already being added. Wait for it to finish."
            else:
                return render_template(
                    "broll.html", clips=_broll_rows(_conn()), pending_task=record.as_json()
                )
        return (
            render_template("broll.html", clips=_broll_rows(_conn()), error=error, form=form),
            400,
        )

    # -- helpers that need the app context --------------------------------

    def _render_project_page(
        conn: sqlite3.Connection,
        project_id: str,
        slug: str,
        *,
        error: str | None = None,
        status: int = 200,
        settings_override: Mapping[str, object] | None = None,
    ) -> ResponseReturnValue:
        row = ProjectService(conn).get(project_id)
        settings = (
            settings_override
            if settings_override is not None
            else ProjectService(conn).settings_for(project_id)
        )
        story = _read_story(settings)
        page = render_template(
            "project.html",
            project=row,
            story=story,
            values=form_values(settings),
            alignments=ALIGNMENTS,
            last_job=_last_job(conn, project_id),
            render_task=_tasks().for_key(_render_key(project_id)),
            error=error,
        )
        return page if status == 200 else (page, status)

    def _start_render(project_id: str, slug: str) -> TaskRecord:
        paths = _paths()

        def work() -> tuple[str, dict[str, str]]:
            # A connection of this thread's own - never the request's. See
            # ui.tasks and infra.db.engine on why that is not optional.
            conn = connect(paths.db_file)
            try:
                cas = CasStore(root=paths.cas, conn=conn)
                artifacts = ArtifactStore(cas, conn)
                settings = refresh_run_settings(conn, cas, project_id)
                output_dir = resolve_output_dir()
                outcome = render_project(
                    conn,
                    cas,
                    artifacts,
                    project_id=project_id,
                    slug=slug,
                    settings=settings,
                    output_dir=output_dir,
                )
            finally:
                conn.close()
            if outcome.state is RenderState.SUCCEEDED:
                assert outcome.export_dir is not None
                return str(outcome.export_dir), {"output_dir": str(outcome.export_dir)}
            message = _RENDER_MESSAGES[outcome.state]
            raise RenderFailed(f"{message} {outcome.detail}".strip())

        return _tasks().submit(key=_render_key(project_id), kind="render", label=slug, work=work)

    def _start_broll_add(
        *, path: Path, source_url: str, licence: str, attribution: str, notes: str
    ) -> TaskRecord:
        paths = _paths()

        def work() -> tuple[str, dict[str, str]]:
            conn = connect(paths.db_file)
            try:
                library = BrollLibrary(conn, CasStore(root=paths.cas, conn=conn))
                clip_id = library.add(
                    path,
                    source_url=source_url,
                    licence=licence,
                    attribution=attribution,
                    notes=notes,
                )
                # Same as `ytauto broll add`: the manifest must never describe
                # a library older than the row just committed.
                library.write_manifest()
            finally:
                conn.close()
            return f"Added clip {clip_id}.", {"clip_id": clip_id}

        return _tasks().submit(key="broll", kind="broll", label=path.name, work=work)

    def _create_project_from_text(
        conn: sqlite3.Connection, *, slug: str, title: str, story: str
    ) -> str:
        """``create_project``, fed from a textarea instead of a file.

        ``create_project`` takes a path because that is what ``--story``
        gives it, and it is the function that decides how a story is hashed,
        staged and copied. Writing the pasted text to a temporary file and
        handing it over reuses all of that; reimplementing it against a
        string would be a second definition of what a project's story is.
        """
        with tempfile.TemporaryDirectory(prefix="ytauto-ui-") as tmp:
            staged = Path(tmp) / _STORY_FILENAME
            staged.write_text(story, encoding="utf-8")
            return create_project(
                conn,
                _cas(conn),
                _paths().projects / slug,
                slug=slug,
                title=title,
                story_path=staged,
            )


class RenderFailed(Exception):
    """A render ended in anything other than success.

    Raised inside the background task so ``TaskManager`` records it the same
    way it records an unexpected exception - one path for "this task did not
    succeed", rather than a second success/failure channel that only renders
    use.
    """


def _render_key(project_id: str) -> str:
    return f"render:{project_id}"


def _json_error(message: str, status: int) -> Response:
    response = jsonify({"error": message})
    response.status_code = status
    return response


def _normalise_newlines(text: str) -> str:
    r"""Collapse a browser's CRLF line endings to LF.

    A ``<textarea>`` submits ``\r\n`` per the HTML spec. ``Path.write_text``
    then translates every ``\n`` to ``os.linesep``, which on Windows - this
    project's own platform - turns each ``\r\n`` into ``\r\r\n``. That is not
    cosmetic: it reaches ``ingest_story`` and the narration.

    ``story_digest_for`` reads with ``read_text``, whose universal-newline
    translation makes CRLF and LF hash identically, so normalising here does
    not itself change any digest - it only stops the doubling.
    """
    return text.replace("\r\n", "\n").replace("\r", "\n")


def _read_story(settings: Mapping[str, object]) -> str:
    """The project's story, read from ``settings["story_path"]``.

    Reads the file rather than the CAS copy: the file is what the docstring
    of ``create_project`` calls "the human-readable, human-editable copy - the
    source of truth someone opens to revise the story", and the textarea is
    exactly that act. Returns an empty string if it cannot be read, so a
    project whose story file was moved still opens (and can be given a new
    one by saving).
    """
    story_path = settings.get("story_path")
    if not isinstance(story_path, str):
        return ""
    try:
        return Path(story_path).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return ""


def _salvage(form: Mapping[str, str], current: Mapping[str, object]) -> dict[str, object]:
    """Whatever of a rejected settings form can be shown back to the user.

    A form that fails validation must come back with the user's own values in
    it, not the stored ones - otherwise the field they got wrong silently
    reverts and they cannot see what they typed. But the values are, by
    definition, not all parseable, so this puts back only what is: the raw
    strings for text fields, and any number that does parse.
    """
    salvaged: dict[str, object] = {}
    for key in ("voice", "rate", "encoder"):
        if key in form:
            salvaged[key] = form[key]
    for key in ("seed", "words_per_group_min", "words_per_group_max"):
        try:
            salvaged[key] = int(form[key])
        except (KeyError, ValueError):
            continue
    for key in ("segment_seconds_min", "segment_seconds_max"):
        try:
            salvaged[key] = float(form[key])
        except (KeyError, ValueError):
            continue
    style = current.get("caption_style")
    salvaged["caption_style"] = dict(style) if isinstance(style, Mapping) else {}
    return salvaged


def _project_rows(conn: sqlite3.Connection) -> Sequence[Mapping[str, object]]:
    """Every project with the state of its most recent job.

    One query with a correlated subquery rather than N+1 lookups - the list
    is the landing page, and a personal library of a few dozen projects
    should not cost a few dozen round trips.
    """
    rows = conn.execute(
        """
        SELECT p.id, p.slug, p.title, p.created_at,
               (SELECT j.state FROM jobs j
                 WHERE j.project_id = p.id
                 ORDER BY j.created_at DESC, j.id DESC
                 LIMIT 1) AS last_state
        FROM projects p
        ORDER BY p.created_at DESC, p.id DESC
        """
    ).fetchall()
    return [dict(row) for row in rows]


def _last_job(conn: sqlite3.Connection, project_id: str) -> Mapping[str, object] | None:
    row = conn.execute(
        """
        SELECT id, state, created_at, updated_at, last_error
        FROM jobs WHERE project_id = ?
        ORDER BY created_at DESC, id DESC LIMIT 1
        """,
        (project_id,),
    ).fetchone()
    return dict(row) if row is not None else None


def _broll_rows(conn: sqlite3.Connection) -> Sequence[Mapping[str, object]]:
    rows = conn.execute(
        """
        SELECT id, duration_s, width, height, licence, source_url, attribution, added_at
        FROM broll_clips ORDER BY added_at DESC, id DESC
        """
    ).fetchall()
    return [dict(row) for row in rows]


__all__ = ["create_app"]
