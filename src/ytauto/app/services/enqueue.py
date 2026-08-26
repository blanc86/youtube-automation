"""Turning CLI input into runnable state: story ingestion, settings, slug lookup.

``ytauto project create`` and ``ytauto run`` (``cli/__main__.py``) are both
thin wiring over the functions here, mirroring how ``_broll_add`` is thin
wiring over ``BrollLibrary`` - the CLI module parses arguments and reports
exit codes; the actual domain behaviour lives in ``app/``.

**A created project must be runnable.** ``create_project`` used to write
exactly ``{story_digest, story_path}`` into ``settings``, while the pipeline
reads nine more keys (``voice``, ``rate``, ``seed``, the two
``words_per_group_*`` bounds, the two ``segment_seconds_*`` bounds,
``caption_style``, ``encoder``) plus ``broll_manifest_digest``. Every one of
them is a bare ``ctx.settings[...]`` subscript, so a project created through
the CLI and then run through the CLI failed on ``KeyError: 'voice'`` at the
second stage - FATAL, exit 1 - and there is no ``ytauto project set-setting``
verb to fix it with. ``DEFAULT_SETTINGS`` below closes that: creation seeds a
complete, documented template, and ``refresh_run_settings`` supplies the two
keys that cannot be decided at creation time because they are *derived* from
state that changes underneath the project.

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
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType

from ytauto.app.services.projects import ProjectService
from ytauto.core.errors import ValidationError
from ytauto.core.models.content_hash import ContentHash, hash_bytes
from ytauto.infra.broll import BrollLibrary
from ytauto.infra.cas.store import CasStore
from ytauto.infra.music import MusicLibrary

_STORY_KIND = "text"

DEFAULT_SETTINGS: Mapping[str, object] = MappingProxyType(
    {
        # synthesize_speech
        "voice": "en-US-AriaNeural",
        "rate": "+0%",
        # plan_timeline / select_broll
        "seed": 1,
        "words_per_group_min": 3,
        "words_per_group_max": 5,
        "segment_seconds_min": 1.5,
        "segment_seconds_max": 4.0,
        # compose_landscape / compose_vertical
        "caption_style": {},
        "encoder": "auto",
        # Music bed. "" means no music, which is the default: a video with no
        # bed is a complete video, and a track that has to be chosen before
        # anything renders would be a step in the way of the common case.
        "music_track_id": "",
        # Applied to the bed alone, never to the narration - see
        # infra.ffmpeg.compose.MusicBed. -18 dB is roughly where a bed sits
        # under speech without competing with it.
        "music_gain_db": -18.0,
    }
)
"""The settings template ``create_project`` seeds, so a project created
through the CLI is immediately runnable through the CLI.

These are exactly the values ``tests/integration/test_first_light.py`` used
to seed by hand before this existed - the four Phase 2a exit criteria were
proven against them, so they are the agreed defaults rather than fresh
guesses. Read-only (``MappingProxyType``) because a module-level mutable
default that a caller could mutate in place would silently change every
subsequent project's template; ``create_project`` copies it into a fresh
dict.

``caption_style`` intentionally starts empty: ``render_ass`` supplies every
field's own default, and ``ComposeStage`` fills in a canvas-appropriate
``font_size`` via ``setdefault``, so an empty mapping means "everything
default" rather than "unset". ``encoder="auto"`` defers to ffmpeg's own
capability probe, which is what makes the same project render on a machine
with NVENC and one without.

Deliberately NOT here: ``story_digest``/``story_path`` (``create_project``
computes both from the story it was handed) and ``broll_manifest_digest``
(derived from the global, mutable B-roll library - see
``refresh_run_settings``)."""

DERIVED_SETTINGS: tuple[str, ...] = ("broll_manifest_digest", "music_digest")
"""Required to run, but deliberately absent from a freshly created project:
both are re-derived per run by ``refresh_run_settings`` from state that
changes underneath the project - the global B-roll library, and the music
library row a project's ``music_track_id`` points at.

Named here rather than spelled out at each use because the set grows: it was
one key, is now two, and every place that has to say "everything except the
derived ones" - ``create_project``'s own tests included - was carrying its own
literal copy that had to be remembered separately."""

REQUIRED_SETTINGS: tuple[str, ...] = (
    "story_digest",
    "story_path",
    *DERIVED_SETTINGS,
    *DEFAULT_SETTINGS,
)
"""Every settings key some stage reads with a bare ``ctx.settings[...]``
subscript, plus ``story_path``, which ``IngestStory.run`` reads the same way.

Kept as one list so a missing key is reported once, up front, by name -
rather than as a ``KeyError`` inside a worker three stages later, which the
dispatcher can only report as a FATAL job failure naming a Python exception."""


def _require_str(settings: Mapping[str, object], key: str) -> str:
    value = settings[key]
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"setting {key!r} must be a non-empty string, got {value!r}")
    return value


def _require_int(settings: Mapping[str, object], key: str) -> int:
    value = settings[key]
    # bool is a subclass of int in Python; a stray True must not act as 1.
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"setting {key!r} must be an integer, got {value!r}")
    return value


def _require_number(settings: Mapping[str, object], key: str) -> float:
    value = settings[key]
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValidationError(f"setting {key!r} must be a number, got {value!r}")
    return float(value)


def validate_settings(settings: Mapping[str, object]) -> None:
    """Reject settings values the pipeline cannot act on. Checks values only.

    Presence is ``require_runnable_settings``'s job; this checks whichever of
    the known keys are actually present, so it can validate both a freshly
    seeded template (which has no ``broll_manifest_digest`` yet) and a
    complete, ready-to-run mapping.

    Ledger item T7 ("no range validation"), upgraded from Minor by the
    whole-branch review: with no ``ytauto project set-setting`` verb,
    hand-editing ``projects.settings_json`` is the *only* way to configure a
    project, which makes ``words_per_group_max = 0`` (one-word captions,
    silently) and ``segment_seconds_min > segment_seconds_max`` (every
    segment uniformly shorter than its own declared minimum, silently) the
    expected path rather than an exotic edge case. Both are caught here, by
    name, before a job is enqueued - the stage that consumes them,
    ``plan_timeline``, decides the whole edit, so a silent misconfiguration
    there is a wrong video with nothing to point at.

    Raises:
        ValidationError: a present setting has the wrong type, or a pair of
            bounds is inverted.
    """
    for key in ("voice", "rate", "encoder", "story_path"):
        if key in settings:
            _require_str(settings, key)
    for key in ("story_digest", "broll_manifest_digest"):
        if key in settings:
            _require_str(settings, key)
    # The two music keys are strings that are *legitimately* empty - "" is how
    # "no bed" is spelled, and it is the default. _require_str rejects empty
    # strings by design, so these get their own check: the type must be right,
    # the emptiness must not be an error.
    for key in ("music_track_id", "music_digest"):
        if key in settings and not isinstance(settings[key], str):
            raise ValidationError(
                f"setting {key!r} must be a string (empty means no music), got {settings[key]!r}"
            )
    if "seed" in settings:
        _require_int(settings, "seed")
    if "caption_style" in settings and not isinstance(settings["caption_style"], Mapping):
        raise ValidationError(
            f"setting 'caption_style' must be a mapping, got {settings['caption_style']!r}"
        )

    if "words_per_group_min" in settings:
        group_min = _require_int(settings, "words_per_group_min")
        if group_min < 1:
            raise ValidationError(
                f"setting 'words_per_group_min' must be at least 1, got {group_min}"
            )
    if "words_per_group_max" in settings:
        group_max = _require_int(settings, "words_per_group_max")
        if group_max < 1:
            raise ValidationError(
                f"setting 'words_per_group_max' must be at least 1, got {group_max} "
                "- a captions-per-group cap of 0 would group no words at all"
            )
        if "words_per_group_min" in settings and group_max < _require_int(
            settings, "words_per_group_min"
        ):
            raise ValidationError(
                "setting 'words_per_group_max' must be at least "
                f"'words_per_group_min', got max={group_max} < "
                f"min={settings['words_per_group_min']!r}"
            )

    if "music_gain_db" in settings:
        gain = _require_number(settings, "music_gain_db")
        # A bed above the narration is never what anyone means, and ffmpeg
        # will happily clip the mix if asked. The ceiling is 0 dB (the track
        # at its own recorded level); the floor is where it stops being
        # audible at all, and below that the honest setting is no track.
        if not (-60.0 <= gain <= 0.0):
            raise ValidationError(
                f"setting 'music_gain_db' must be between -60 and 0, got {gain} "
                "- positive gain drives the mix into clipping, and below -60 dB "
                "the bed is inaudible; to remove it, clear 'music_track_id'"
            )

    if "segment_seconds_min" in settings:
        seconds_min = _require_number(settings, "segment_seconds_min")
        if seconds_min <= 0:
            raise ValidationError(
                f"setting 'segment_seconds_min' must be greater than 0, got {seconds_min}"
            )
    if "segment_seconds_max" in settings:
        seconds_max = _require_number(settings, "segment_seconds_max")
        if seconds_max <= 0:
            raise ValidationError(
                f"setting 'segment_seconds_max' must be greater than 0, got {seconds_max}"
            )
        if "segment_seconds_min" in settings and seconds_max < _require_number(
            settings, "segment_seconds_min"
        ):
            raise ValidationError(
                "setting 'segment_seconds_max' must be at least "
                f"'segment_seconds_min', got max={seconds_max} < "
                f"min={settings['segment_seconds_min']!r}"
            )


def require_runnable_settings(settings: Mapping[str, object]) -> None:
    """Everything ``validate_settings`` checks, plus: nothing is missing.

    Raises:
        ValidationError: a key in ``REQUIRED_SETTINGS`` is absent, or a value
            fails ``validate_settings``.
    """
    missing = [key for key in REQUIRED_SETTINGS if key not in settings]
    if missing:
        raise ValidationError(
            "this project's settings are incomplete - the pipeline reads "
            f"{', '.join(repr(key) for key in missing)}, which "
            f"{'is' if len(missing) == 1 else 'are'} not set. A project "
            "created by `ytauto project create` is seeded with a complete "
            "template; a project whose settings_json has been hand-edited "
            "must keep every key."
        )
    validate_settings(settings)


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

    ``settings`` is seeded from ``DEFAULT_SETTINGS`` - see that constant for
    why, and for why ``broll_manifest_digest`` is not among them. The seeded
    template is passed through ``validate_settings`` before the row is
    written: the check costs nothing on a template that is correct by
    construction, and it means the one function that decides what a new
    project's settings look like can never drift out of agreement with the
    one function that decides which settings are legal.

    Raises:
        ValidationError: ``slug`` already names another project,
            ``story_path`` does not exist or is not a regular file, or the
            seeded settings template is not valid.
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

    settings: dict[str, object] = {
        **DEFAULT_SETTINGS,
        "story_digest": digest,
        "story_path": str(dest),
    }
    validate_settings(settings)
    return ProjectService(conn).create(
        slug=slug, title=title, story_digest=digest, settings=settings
    )


def refresh_run_settings(
    conn: sqlite3.Connection, cas: CasStore, project_id: str
) -> dict[str, object]:
    """Re-derive the two settings that go stale between runs, then check the
    whole mapping is runnable. Returns the project's fresh settings.

    Called by ``ytauto run`` immediately before it enqueues a job. Two keys
    cannot be decided once, at creation time, because both describe state
    that changes underneath a project:

    ``story_digest`` follows ``story.txt``. ``create_project``'s own
    docstring calls that file "the human-readable, human-editable copy - the
    source of truth someone opens to revise the story", and
    ``IngestStory.run`` reads whatever is at ``settings["story_path"]`` at
    run time - but ``IngestStory.fingerprint`` hashes
    ``settings["story_digest"]``. Without this refresh the two diverge the
    moment anyone edits their story, with two distinct consequences the
    whole-branch review reproduced: the edit is *silently ignored* (the
    fingerprint is unchanged, every stage is a cache hit, ``ytauto run``
    exits 0 and the old video is still the newest artifact), and, on a cache
    miss, the *new* text gets recorded under the *old* digest's fingerprint -
    which, since ``ingest_story``'s fingerprint deliberately carries no
    ``project_id`` so identical stories dedupe across projects, hands that
    stale entry to any other project whose story genuinely hashes to the old
    digest. Recomputing here makes an edited story invalidate exactly the
    stages that depend on it and nothing else.

    ``broll_manifest_digest`` follows the B-roll library, which is global and
    mutable: ``ytauto broll add`` rewrites the manifest, and every project
    shares it. Binding the digest per *run* rather than per *project* is
    correct by construction - the fingerprint then honestly reflects the
    library state this render actually saw, so adding a clip invalidates the
    next run's ``select_broll`` (a genuinely different edit was possible)
    while re-running an untouched library still hits the cache. Rewriting
    the manifest here also re-pins it against the evictor
    (``BrollLibrary.write_manifest`` retains what it writes), which is what
    keeps the blob every compose stage reads from being the first thing
    evicted under disk pressure.

    Raises:
        ValidationError: no project has this id, ``story_path`` is missing or
            not a string, or the resulting settings are not runnable (see
            ``require_runnable_settings``).
        OSError: the story file cannot be read.
        UnicodeDecodeError: the story file is not valid UTF-8.
        sqlite3.Error: a query or write fails.
    """
    projects = ProjectService(conn)
    settings = projects.settings_for(project_id)

    # Backfill any key added to DEFAULT_SETTINGS since this project was
    # created. Without this, adding a setting to the template retroactively
    # breaks every project already on disk: require_runnable_settings below
    # fails on the absent key, and `ytauto run` refuses a project that
    # rendered fine yesterday, with a message about hand-edited settings that
    # is simply untrue. Seeding the documented default is what an upgrade
    # should mean - it is the value a project created today would have.
    #
    # Only absent keys are touched, so a deliberately changed value is never
    # reverted; and the whole thing is a no-op for a project created after
    # the key existed.
    for key, default in DEFAULT_SETTINGS.items():
        if key not in settings:
            projects.set_setting(project_id, key, default)
    settings = projects.settings_for(project_id)

    story_path = settings.get("story_path")
    if not isinstance(story_path, str) or not story_path.strip():
        raise ValidationError(
            "this project has no usable 'story_path' setting, so there is no "
            f"story to render (got {story_path!r})"
        )
    projects.set_setting(project_id, "story_digest", story_digest_for(Path(story_path)))

    manifest_digest = BrollLibrary(conn, cas).write_manifest()
    projects.set_setting(project_id, "broll_manifest_digest", str(manifest_digest))

    # `music_track_id` names a row; the compose stages run in worker processes
    # that hold a CasStore and no database connection, so the id is resolved
    # to a digest here - the same shape as the B-roll manifest above, and for
    # the same reason.
    track_id = str(settings.get("music_track_id", "") or "")
    if track_id:
        digest = MusicLibrary(conn, cas).digest_for(track_id)
        if digest is None:
            raise ValidationError(
                f"this project's music_track_id ({track_id!r}) is not in the music "
                "library - it was probably removed after the project selected it. "
                "Choose another track, or clear the setting to render without a bed."
            )
        projects.set_setting(project_id, "music_digest", str(digest))
    else:
        projects.set_setting(project_id, "music_digest", "")

    fresh = projects.settings_for(project_id)
    require_runnable_settings(fresh)
    return fresh


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
