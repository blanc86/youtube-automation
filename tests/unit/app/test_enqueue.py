"""The settings half of ``app.services.enqueue``: the template a project is
seeded with, the rules that template has to satisfy, and the two derived
values ``ytauto run`` re-computes before every enqueue.

All three exist because of the whole-branch review. Critical 1: a project
created through the CLI could not be run through the CLI, because
``create_project`` wrote exactly ``{story_digest, story_path}`` while the
pipeline reads nine more keys as bare ``ctx.settings[...]`` subscripts - so
the second stage died on ``KeyError: 'voice'`` and there is no ``ytauto
project set-setting`` verb to fix it with. Critical 2: ``ingest_story``
fingerprints ``story_digest`` but reads ``story_path`` at run time, so a
story edited in place (which ``create_project``'s own docstring invites -
"the source of truth someone opens to revise the story") was silently
ignored. And the settings validation the review upgraded out of the ledger's
T7 minor: with no CLI verb, hand-editing ``settings_json`` is now the *only*
way to configure a project, which makes an inverted bound the expected path
rather than an exotic one.

``tests/unit/cli/test_run_command.py`` covers the same ground from the
outside, through ``main()``; these are the direct tests of the functions
those exit codes come from.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from ytauto.app.services.enqueue import (
    DEFAULT_SETTINGS,
    DERIVED_SETTINGS,
    REQUIRED_SETTINGS,
    create_project,
    refresh_run_settings,
    require_runnable_settings,
    story_digest_for,
    validate_settings,
)
from ytauto.app.services.projects import ProjectService
from ytauto.core.errors import ValidationError
from ytauto.infra.cas.store import CasStore
from ytauto.infra.db.engine import transaction

# db_conn is defined in tests/unit/conftest.py.


@pytest.fixture()
def cas(tmp_path: Path, db_conn: sqlite3.Connection) -> CasStore:
    return CasStore(root=tmp_path / "cas", conn=db_conn)


def _create(
    conn: sqlite3.Connection,
    cas: CasStore,
    tmp_path: Path,
    *,
    text: str = "The train never stopped.\n",
) -> tuple[str, Path]:
    """A project created exactly the way ``ytauto project create`` creates
    one. Returns its id and the path to its own editable ``story.txt``."""
    source = tmp_path / "source-story.txt"
    source.write_text(text, encoding="utf-8")
    project_dir = tmp_path / "projects" / "probe"
    project_id = create_project(
        conn, cas, project_dir, slug="probe", title="Probe", story_path=source
    )
    return project_id, project_dir / "story.txt"


# -- Critical 1: a created project carries a complete template ----------------


def test_create_project_seeds_every_setting_the_pipeline_reads(
    tmp_path: Path, db_conn: sqlite3.Connection, cas: CasStore
) -> None:
    """The one assertion that would have caught Critical 1 at creation time.
    ``DERIVED_SETTINGS`` are the exceptions, supplied per-run by
    ``refresh_run_settings`` - see its own tests below."""
    project_id, _story = _create(db_conn, cas, tmp_path)

    settings = ProjectService(db_conn).settings_for(project_id)

    missing = [
        key for key in REQUIRED_SETTINGS if key not in DERIVED_SETTINGS and key not in settings
    ]
    assert missing == [], f"a created project must be runnable; missing {missing}"
    for key, value in DEFAULT_SETTINGS.items():
        assert settings[key] == value


def test_the_seeded_template_is_itself_valid(
    tmp_path: Path, db_conn: sqlite3.Connection, cas: CasStore
) -> None:
    """The template and the rules are two halves of one decision; nothing but
    a test keeps them agreeing."""
    project_id, _story = _create(db_conn, cas, tmp_path)
    settings = ProjectService(db_conn).settings_for(project_id)
    settings["broll_manifest_digest"] = "0" * 64
    # Empty is what "no music" is spelled as, and it is the default a created
    # project carries - so the template must validate with it empty, not only
    # with a track chosen.
    settings["music_digest"] = ""

    require_runnable_settings(settings)  # must not raise


# -- the settings rules (ledger T7, upgraded by the whole-branch review) -------


def test_a_zero_words_per_group_max_is_rejected_by_name() -> None:
    """Silently yields one-word captions for the whole video - the stage that
    consumes it decides the entire edit, so a silent misconfiguration there
    is a wrong video with nothing to point at.

    *** This assertion is OVER-DETERMINED against ``DEFAULT_SETTINGS`` and
    cannot, on its own, pin the lower-bound check. *** Guard-pinning it found
    that out: deleting ``words_per_group_max``'s own ``< 1`` branch entirely
    leaves this test still passing, because the template's
    ``words_per_group_min`` is 3, so the *inversion* check (``0 < 3``) fires
    instead and raises naming the same key. Two independent rules cover this
    one input. Recorded here rather than papered over, per Phase 1a Sec 2.3
    and Task 14's precedent; the test below is the one that isolates the
    lower bound."""
    with pytest.raises(ValidationError, match="words_per_group_max"):
        validate_settings({**DEFAULT_SETTINGS, "words_per_group_max": 0})


def test_a_zero_words_per_group_max_is_rejected_even_with_no_minimum_set() -> None:
    """The assertion that isolates the lower-bound check from the inversion
    check: with no ``words_per_group_min`` present at all, nothing else can
    reject this input. Guard-pinned - deleting the ``< 1`` branch fails this
    with DID NOT RAISE, while leaving the test above green.

    Not a contrived shape, either: ``validate_settings`` checks whichever keys
    are present precisely so it can validate a partial mapping, and a
    hand-edited ``settings_json`` is exactly where a partial one shows up."""
    with pytest.raises(ValidationError, match="words_per_group_max"):
        validate_settings({"words_per_group_max": 0})


def test_an_inverted_words_per_group_range_is_rejected_by_name() -> None:
    with pytest.raises(ValidationError, match="words_per_group_max"):
        validate_settings({**DEFAULT_SETTINGS, "words_per_group_min": 6, "words_per_group_max": 4})


def test_an_inverted_segment_seconds_range_is_rejected_by_name() -> None:
    """Silently yields segments uniformly shorter than their own declared
    minimum."""
    with pytest.raises(ValidationError, match="segment_seconds_max"):
        validate_settings(
            {**DEFAULT_SETTINGS, "segment_seconds_min": 9.0, "segment_seconds_max": 2.0}
        )


def test_a_non_positive_segment_seconds_min_is_rejected() -> None:
    with pytest.raises(ValidationError, match="segment_seconds_min"):
        validate_settings({**DEFAULT_SETTINGS, "segment_seconds_min": 0.0})


def test_a_blank_voice_is_rejected() -> None:
    with pytest.raises(ValidationError, match="voice"):
        validate_settings({**DEFAULT_SETTINGS, "voice": "  "})


def test_a_boolean_seed_is_rejected_rather_than_read_as_one() -> None:
    """``bool`` is a subclass of ``int`` in Python, so ``True`` would sail
    through a bare ``isinstance(value, int)`` and act as a seed of 1."""
    with pytest.raises(ValidationError, match="seed"):
        validate_settings({**DEFAULT_SETTINGS, "seed": True})


def test_a_non_mapping_caption_style_is_rejected() -> None:
    with pytest.raises(ValidationError, match="caption_style"):
        validate_settings({**DEFAULT_SETTINGS, "caption_style": "big and red"})


def test_validate_settings_checks_values_not_presence() -> None:
    """The half-built template ``create_project`` validates has no
    ``broll_manifest_digest`` yet, so value checking and presence checking
    have to be separable."""
    validate_settings({})  # must not raise


def test_require_runnable_settings_names_every_missing_key_at_once() -> None:
    """One message listing everything, rather than a ``KeyError`` per stage
    discovered one failed job at a time."""
    with pytest.raises(ValidationError) as exc:
        require_runnable_settings({"story_digest": "0" * 64, "story_path": "/tmp/s.txt"})

    message = str(exc.value)
    assert "'voice'" in message
    assert "'encoder'" in message
    assert "'broll_manifest_digest'" in message


# -- Critical 2: an edited story must invalidate ------------------------------


def test_refresh_recomputes_the_story_digest_from_the_file_on_disk(
    tmp_path: Path, db_conn: sqlite3.Connection, cas: CasStore
) -> None:
    """The silent half of Critical 2. ``ingest_story.fingerprint`` hashes
    ``settings["story_digest"]`` while ``run`` reads ``settings["story_path"]``
    - so without this refresh, editing ``story.txt`` and re-running gives exit
    0 and the *old* video, with nothing saying so."""
    project_id, story = _create(db_conn, cas, tmp_path)
    projects = ProjectService(db_conn)
    before = projects.settings_for(project_id)["story_digest"]

    story.write_text("A completely different story.\n", encoding="utf-8")
    refreshed = refresh_run_settings(db_conn, cas, project_id)

    assert refreshed["story_digest"] != before
    assert refreshed["story_digest"] == story_digest_for(story)
    assert projects.settings_for(project_id)["story_digest"] == refreshed["story_digest"], (
        "the new digest must be persisted, not merely returned"
    )


def test_refresh_leaves_an_unedited_story_s_digest_alone(
    tmp_path: Path, db_conn: sqlite3.Connection, cas: CasStore
) -> None:
    """The other side of the same coin: re-running an untouched project must
    still be an all-cache-hits run (Phase 2a exit criterion 2), which it
    cannot be if the refresh perturbs the digest."""
    project_id, _story = _create(db_conn, cas, tmp_path)
    before = ProjectService(db_conn).settings_for(project_id)["story_digest"]

    assert refresh_run_settings(db_conn, cas, project_id)["story_digest"] == before


def test_refresh_binds_the_broll_manifest_digest_and_pins_it(
    tmp_path: Path, db_conn: sqlite3.Connection, cas: CasStore
) -> None:
    """``broll_manifest_digest`` is the key no CLI verb could ever set by
    hand: ``_broll_add`` throws away ``write_manifest()``'s return value, so
    the digest was simply unobtainable through the CLI. Binding it per *run*
    is also what makes the fingerprint honestly reflect the library state
    this render saw - the library is global and mutable."""
    project_id, _story = _create(db_conn, cas, tmp_path)

    refreshed = refresh_run_settings(db_conn, cas, project_id)

    digest = refreshed["broll_manifest_digest"]
    assert isinstance(digest, str)
    assert cas.read_bytes(digest).decode("utf-8").strip() == "[]", "an empty library, honestly"
    assert cas.refcount(digest) == 1, "the manifest every compose stage reads must be pinned"


def test_refresh_rejects_a_project_whose_settings_were_broken_by_hand(
    tmp_path: Path, db_conn: sqlite3.Connection, cas: CasStore
) -> None:
    """Hand-editing ``settings_json`` is the only way to configure a project
    today, so this is the expected path, not an edge case - and it must fail
    before a job is enqueued rather than as a FATAL worker error four stages
    in."""
    project_id, _story = _create(db_conn, cas, tmp_path)
    ProjectService(db_conn).set_setting(project_id, "segment_seconds_max", 0.5)

    with pytest.raises(ValidationError, match="segment_seconds_max"):
        refresh_run_settings(db_conn, cas, project_id)


def test_refresh_reports_a_project_with_no_story_path(
    tmp_path: Path, db_conn: sqlite3.Connection, cas: CasStore
) -> None:
    project_id = ProjectService(db_conn).create(
        slug="pathless", title="Pathless", story_digest=None, settings={}
    )

    with pytest.raises(ValidationError, match="story_path"):
        refresh_run_settings(db_conn, cas, project_id)


# -- an upgrade must not brick projects that already exist ---------------------


def test_refresh_backfills_settings_added_since_the_project_was_created(
    tmp_path: Path, db_conn: sqlite3.Connection, cas: CasStore
) -> None:
    """Adding a key to DEFAULT_SETTINGS retroactively breaks every project on
    disk unless the refresh seeds it: require_runnable_settings fails on the
    absent key and `ytauto run` refuses a project that rendered fine
    yesterday, blaming hand-edited settings that were never touched.

    Simulated by deleting the music keys, which is exactly the shape of a
    project created before they existed."""
    project_id, _story = _create(db_conn, cas, tmp_path)
    projects = ProjectService(db_conn)

    settings = projects.settings_for(project_id)
    del settings["music_track_id"]
    del settings["music_gain_db"]
    # Written straight to the column: this is literally what a project created
    # before these keys existed looks like on disk.
    with transaction(db_conn, immediate=True):
        db_conn.execute(
            "UPDATE projects SET settings_json = ? WHERE id = ?",
            (json.dumps(settings), project_id),
        )
    assert "music_track_id" not in projects.settings_for(project_id)

    refreshed = refresh_run_settings(db_conn, cas, project_id)

    assert refreshed["music_track_id"] == ""
    assert refreshed["music_gain_db"] == -18.0
    assert projects.settings_for(project_id)["music_track_id"] == "", (
        "the backfill must be persisted, not merely returned"
    )


def test_the_backfill_never_overwrites_a_value_someone_chose(
    tmp_path: Path, db_conn: sqlite3.Connection, cas: CasStore
) -> None:
    """Only absent keys are seeded. A refresh that reset deliberate settings to
    the template on every run would be far worse than the bug it fixes."""
    project_id, _story = _create(db_conn, cas, tmp_path)
    projects = ProjectService(db_conn)
    projects.set_setting(project_id, "voice", "en-GB-RyanNeural")
    projects.set_setting(project_id, "music_gain_db", -30.0)

    refreshed = refresh_run_settings(db_conn, cas, project_id)

    assert refreshed["voice"] == "en-GB-RyanNeural"
    assert refreshed["music_gain_db"] == -30.0


def test_a_music_track_that_has_been_removed_is_reported_at_enqueue_time(
    tmp_path: Path, db_conn: sqlite3.Connection, cas: CasStore
) -> None:
    """Naming the problem before the job is queued, rather than failing inside
    a worker two stages into a render that has already spent an encode."""
    project_id, _story = _create(db_conn, cas, tmp_path)
    ProjectService(db_conn).set_setting(project_id, "music_track_id", "deleted-track")

    with pytest.raises(ValidationError, match="not in the music library"):
        refresh_run_settings(db_conn, cas, project_id)
