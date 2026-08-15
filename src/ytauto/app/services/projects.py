"""Project persistence: identity, story linkage and per-project settings.

A project can exist before a story is attached - ``story_digest`` is nullable
for exactly that reason. ``settings_json`` holds an arbitrary JSON object,
defaulted to ``'{}'`` in the schema so a freshly created project always has
something valid to parse.

``slug`` is the human-facing, URL-safe identity; ``id`` (a `uuid4().hex`,
matching the correlation-id scheme already used in
``ytauto.infra.logging.bind_correlation_id``) is the opaque foreign key every
other table - starting with ``jobs.project_id`` - actually references. No
existing table generates its own ids; callers of ``JobQueue.enqueue`` already
pass a caller-supplied ``job_id``, so there was no established scheme to
reuse beyond that correlation-id precedent.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass

from ytauto.core.errors import ValidationError
from ytauto.infra.clock import utc_now_iso
from ytauto.infra.db.engine import transaction


@dataclass(frozen=True)
class ProjectRow:
    """A row of the ``projects`` table, as returned by ``ProjectService.get``."""

    id: str
    slug: str
    title: str
    story_digest: str | None
    settings_json: str
    created_at: str
    updated_at: str


class ProjectService:
    """CRUD over the ``projects`` table of a migrated connection."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def create(
        self,
        *,
        slug: str,
        title: str,
        story_digest: str | None,
        settings: dict[str, object],
    ) -> str:
        """Create a new project and return its generated id.

        ``settings`` is serialised to JSON immediately - ``settings_for``
        deserialises it back on read, so the round trip is exact for anything
        ``json`` can represent.

        Raises:
            ValidationError: ``slug`` is already in use by another project.
                This is a "bad input" failure - the caller supplied a slug
                that collides with existing state - distinct from a lookup
                that finds nothing (carry-forward Phase 1a §1.8: "bad input"
                and "missing state" must not be indistinguishable errors).
            sqlite3.OperationalError: the write lock could not be acquired
                within ``busy_timeout``.
        """
        project_id = uuid.uuid4().hex
        now = utc_now_iso()
        settings_json = json.dumps(settings)
        try:
            with transaction(self._conn, immediate=True):
                self._conn.execute(
                    """
                    INSERT INTO projects
                        (id, slug, title, story_digest, settings_json, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (project_id, slug, title, story_digest, settings_json, now, now),
                )
        except sqlite3.IntegrityError:
            raise ValidationError(f"slug already in use by another project: {slug!r}") from None
        return project_id

    def get(self, project_id: str) -> ProjectRow:
        """Fetch a project by id.

        Raises:
            ValidationError: no project exists with this id - "missing state",
                as opposed to ``create``'s slug collision, which is "bad
                input" (carry-forward Phase 1a §1.8).
            sqlite3.Error: the query fails.
        """
        row = self._conn.execute(
            """
            SELECT id, slug, title, story_digest, settings_json, created_at, updated_at
            FROM projects WHERE id = ?
            """,
            (project_id,),
        ).fetchone()
        if row is None:
            raise ValidationError(f"no such project: {project_id!r}")
        return ProjectRow(
            id=row["id"],
            slug=row["slug"],
            title=row["title"],
            story_digest=row["story_digest"],
            settings_json=row["settings_json"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def settings_for(self, project_id: str) -> dict[str, object]:
        """The project's settings, deserialised from ``settings_json``.

        Raises:
            ValidationError: no project exists with this id.
            sqlite3.Error: the query fails.
        """
        row = self.get(project_id)
        parsed: dict[str, object] = json.loads(row.settings_json)
        return parsed

    def set_setting(self, project_id: str, key: str, value: object) -> None:
        """Set a single key in the project's settings, leaving the rest untouched.

        Read-modify-write under one ``transaction(conn, immediate=True)`` -
        the parse-mutate-serialise round trip is not itself atomic, so the
        write lock must be held across the whole read before anyone else's
        write can interleave.

        Raises:
            ValidationError: no project exists with this id.
            sqlite3.OperationalError: the write lock could not be acquired
                within ``busy_timeout``.
        """
        now = utc_now_iso()
        with transaction(self._conn, immediate=True):
            row = self._conn.execute(
                "SELECT settings_json FROM projects WHERE id = ?", (project_id,)
            ).fetchone()
            if row is None:
                raise ValidationError(f"no such project: {project_id!r}")
            settings: dict[str, object] = json.loads(row["settings_json"])
            settings[key] = value
            self._conn.execute(
                "UPDATE projects SET settings_json = ?, updated_at = ? WHERE id = ?",
                (json.dumps(settings), now, project_id),
            )
