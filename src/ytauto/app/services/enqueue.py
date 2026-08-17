"""Turning CLI input into runnable state: story ingestion and slug lookup.

``ytauto project create`` and ``ytauto run`` (``cli/__main__.py``) are both
thin wiring over the functions here, mirroring how ``_broll_add`` is thin
wiring over ``BrollLibrary`` - the CLI module parses arguments and reports
exit codes; the actual domain behaviour lives in ``app/``.

**Hash the normalised text, not the raw file bytes.** Found by Task 4's
review: ``story_digest_for`` is the one place in this project that hashes a
story's content for the ``story_digest`` column/setting, and it hashes
``Path.read_text(encoding="utf-8")``, not raw bytes. Every CAS hashing path
elsewhere hashes raw bytes (``hash_file`` opens ``"rb"``), which is correct
for opaque blobs - but a story is text, and ``ingest_story``'s own
``PastedStorySource.fetch`` reads it the same normalised way (see that
module's docstring on why: ``read_text``'s universal-newline translation is
what makes the pinned verbatim-round-trip test pass). If this hashed raw
bytes instead, a CRLF-saved story and an LF-saved copy of the identical text
- routinely produced by a normal editor on this project's own platform,
Windows - would get two different digests and spuriously miss the cache on
every single re-run, even though ``ingest_story`` stages byte-identical
output for both. Hashing the normalised text is what makes the digest agree
with what ``ingest_story`` actually stages.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from ytauto.app.services.projects import ProjectService
from ytauto.core.errors import ValidationError
from ytauto.core.models.content_hash import ContentHash, hash_bytes
from ytauto.infra.cas.store import CasStore

_STORY_KIND = "text"


def story_digest_for(story_path: Path) -> ContentHash:
    """Hash a story file's normalised text content - see the module docstring.

    Raises:
        OSError: ``story_path`` cannot be opened or read.
        UnicodeDecodeError: ``story_path`` is not valid UTF-8.
    """
    text = story_path.read_text(encoding="utf-8")
    return hash_bytes(text.encode("utf-8"))


def create_project(
    conn: sqlite3.Connection,
    cas: CasStore,
    project_dir: Path,
    *,
    slug: str,
    title: str,
    story_path: Path,
) -> str:
    """Create a project from a story file on disk. Returns the new project id.

    Two writes make the story durable, in two different senses (Design
    Sec 6.4: a project must be reopenable from disk alone). ``story.txt``
    inside ``project_dir`` is the human-readable, human-editable copy - the
    source of truth someone opens to revise the story. The CAS copy, staged
    under the same digest recorded in ``projects.story_digest`` and
    ``settings["story_digest"]``, is what ``ingest_story`` fingerprints
    against (see that stage's own module docstring for why it fingerprints
    the digest rather than reading the file itself).

    ``settings["story_path"]`` is set to ``project_dir``'s own copy, not the
    caller's ``story_path`` argument: ``ingest_story.run`` reads that path at
    run time (``ctx.settings["story_path"]``), and the caller's original file
    may move or be deleted long before the job that reads it ever runs.

    The slug-uniqueness check runs *first*, before anything touches the CAS
    or the filesystem - found by review: this used to write ``story.txt``
    into ``project_dir`` (a path derived from ``slug`` alone) before
    ``ProjectService.create`` ever checked for a collision, so retrying
    ``project create`` against an existing slug with *different* story
    content would overwrite that project's on-disk story and stage an
    unreferenced CAS blob, then fail on the duplicate-slug error - leaving
    ``projects.story_digest``/``settings["story_digest"]`` pointing at the
    *old* digest while the file on disk held the *new*, different content.
    That divergence is not cosmetic: ``ingest_story.fingerprint()`` reads
    ``settings["story_digest"]`` alone, with no ``project_id`` component (by
    design, for cross-project cache dedup), so a later run carrying the
    stale digest could take a cache hit serving content that no longer
    matches what a human editing ``story.txt`` believes they are running.
    Checking uniqueness before any write closes that window: a rejected
    ``create_project`` call now touches neither the CAS nor the filesystem.

    Also reads ``story_path`` exactly once, hashing the same ``text`` it
    then stages and writes - not once via ``story_digest_for`` and again
    directly, which left an (admittedly narrow) window where the recorded
    digest and the bytes actually written could theoretically diverge
    between the two reads.

    Raises:
        ValidationError: ``slug`` already names another project, or
            ``story_path`` does not exist or is not a regular file.
        UnicodeDecodeError: ``story_path`` is not valid UTF-8.
        OSError: the story cannot be read, or ``project_dir``/its ``story.txt``
            cannot be written.
        sqlite3.OperationalError: the write lock could not be acquired within
            ``busy_timeout``.
    """
    if _slug_in_use(conn, slug):
        raise ValidationError(f"slug already in use by another project: {slug!r}")
    if not story_path.is_file():
        raise ValidationError(f"story file does not exist: {story_path}")

    text = story_path.read_text(encoding="utf-8")
    digest = hash_bytes(text.encode("utf-8"))
    cas.put_bytes(text.encode("utf-8"), kind=_STORY_KIND)

    project_dir.mkdir(parents=True, exist_ok=True)
    dest = project_dir / "story.txt"
    dest.write_text(text, encoding="utf-8")

    settings: dict[str, object] = {"story_digest": digest, "story_path": str(dest)}
    return ProjectService(conn).create(
        slug=slug, title=title, story_digest=digest, settings=settings
    )


def _slug_in_use(conn: sqlite3.Connection, slug: str) -> bool:
    """Whether a project already exists under this slug.

    A plain pre-check, not a substitute for ``ProjectService.create``'s own
    unique-constraint handling: this closes the window described above
    (writes reaching disk/CAS before the check ran) for the ordinary,
    non-racing CLI case; the ``ProjectService.create`` call at the end of
    ``create_project`` remains the authoritative guard against a genuine
    concurrent collision between two processes racing on the same slug.
    """
    row = conn.execute("SELECT 1 FROM projects WHERE slug = ? LIMIT 1", (slug,)).fetchone()
    return row is not None


def resolve_project_id(conn: sqlite3.Connection, slug: str) -> str:
    """Look up a project's id by its human-facing slug.

    Raises:
        ValidationError: no project has this slug - bad input, distinct from
            a project that exists but is otherwise unrunnable (carry-forward
            Phase 1a Sec 1.8: "bad input" and "missing state" must not be
            indistinguishable errors).
        sqlite3.Error: the query fails.
    """
    row = conn.execute("SELECT id FROM projects WHERE slug = ?", (slug,)).fetchone()
    if row is None:
        raise ValidationError(f"no such project: {slug!r}")
    return str(row["id"])
